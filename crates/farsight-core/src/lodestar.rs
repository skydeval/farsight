//! Reading identities from a lodestar instance (`backfill.lodestar_url`;
//! see `docs/design/lodestar.md`): a read-only mirror of the PLC directory
//! that also verifies every claimed handle in both directions and
//! re-checks each one weekly.
//!
//! This module knows lodestar's answers and nothing about how they are
//! fetched: the backfill process and the server each send the requests
//! through their own client and limits. Every answer is one of three
//! things ([`Answer`]): an account, lodestar saying for certain that it
//! has none, or nothing to go by. The last covers a 503 (lodestar is
//! catching up, or its verification is paused), an error, an answer that
//! says lodestar is not at the directory's tip (`source.plc.fresh` is not
//! `true`), and any answer this module does not understand. The caller
//! then asks the PLC directory and makes its own checks, as it does
//! without lodestar.

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::Did;

/// Most DIDs one `POST /accounts` asks for.
pub const BATCH: usize = 1_000;
/// Entries one page of `/account/{did}/history` is asked for.
pub const HISTORY_PAGE: u32 = 200;

/// The base URL of the lodestar instance `configured` names, without a
/// trailing slash; `None` when the setting is empty.
pub fn base(configured: &str) -> Option<&str> {
    let b = configured.trim().trim_end_matches('/');
    (!b.is_empty()).then_some(b)
}

/// `GET {base}/account/{did}`.
pub fn account_url(base: &str, did: &Did) -> Option<url::Url> {
    url::Url::parse(&format!("{base}/account/{}", did.as_str())).ok()
}

/// `GET {base}/account/{did}/history`, a page of [`HISTORY_PAGE`]
/// entries from `cursor`.
pub fn history_url(base: &str, did: &Did, cursor: Option<&str>) -> Option<url::Url> {
    let mut u = url::Url::parse(&format!("{base}/account/{}/history", did.as_str())).ok()?;
    u.query_pairs_mut()
        .append_pair("limit", &HISTORY_PAGE.to_string());
    if let Some(c) = cursor {
        u.query_pairs_mut().append_pair("cursor", c);
    }
    Some(u)
}

/// `POST {base}/accounts`.
pub fn accounts_url(base: &str) -> Option<url::Url> {
    url::Url::parse(&format!("{base}/accounts")).ok()
}

/// The body of a `POST /accounts` for `dids` (at most [`BATCH`]).
pub fn accounts_body<'a>(dids: impl IntoIterator<Item = &'a str>) -> Vec<u8> {
    let dids: Vec<&str> = dids.into_iter().collect();
    serde_json::to_vec(&serde_json::json!({ "dids": dids })).unwrap_or_default()
}

/// An identity's state in lodestar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// A live identity.
    Active,
    /// Tombstoned in the PLC directory.
    Tombstoned,
    /// A `did:web` whose document could not be read for two weeks.
    Unresolvable,
    /// A state this version does not know.
    Other,
}

/// Where lodestar's verification of the claimed handle stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleStatus {
    /// The handle resolves back to the DID: it is the account's.
    Verified,
    /// The handle resolves to another DID.
    Mismatch,
    /// The handle's domain answered that it names no DID.
    Unresolved,
    /// Checks over at least a day got no answer.
    Unreachable,
    /// The claim has not been checked since it was set.
    Pending,
    /// No usable claim.
    None,
    /// A state this version does not know.
    Other,
}

/// One account, as `/account/{did}` and `POST /accounts` give it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    /// The identity's state.
    pub status: Status,
    /// The handle the document claims, lower-cased, if it is a hostname.
    pub claimed_handle: Option<String>,
    /// The claim's verification.
    pub handle_status: HandleStatus,
    /// The handle, present only when it is verified in both directions.
    pub handle: Option<String>,
    /// When the handle was last checked.
    pub handle_checked_at: Option<DateTime<Utc>>,
    /// The PDS endpoint the document names, as written.
    pub pds: Option<String>,
    /// The identity's creation: the genesis operation of a `did:plc`,
    /// when lodestar first saw a `did:web`.
    pub created_at: Option<DateTime<Utc>>,
}

/// What a read from lodestar established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// The account.
    Account(Box<Account>),
    /// Lodestar is at the directory's tip and has no such identity.
    NotFound,
    /// Nothing to go by: ask the PLC directory and check for yourself.
    Unavailable,
}

/// What lodestar says about the handle an account may show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// This handle is the account's, verified in both directions.
    Verified(String),
    /// The account has no handle to show, for certain: it is gone, it
    /// claims none, or its claim resolves to another DID or to nothing.
    NotTheirs,
    /// It claims this handle, whose domain has not answered for a day or
    /// more.
    Unreachable(String),
    /// Lodestar has not checked the claim yet, or cannot say.
    Unknown,
}

fn time(v: &Value) -> Option<DateTime<Utc>> {
    v.as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
}

fn handle(v: &Value) -> Option<String> {
    let h = v.as_str()?.to_ascii_lowercase();
    crate::did::is_valid_hostname(&h).then_some(h)
}

/// Whether an answer says lodestar is at the directory's tip.
pub fn fresh(v: &Value) -> bool {
    v["source"]["plc"]["fresh"].as_bool() == Some(true)
}

/// Reads one account object (without its `source`). `None` when it has
/// no `status`.
pub fn parse_account(v: &Value) -> Option<Account> {
    let status = match v["status"].as_str()? {
        "active" => Status::Active,
        "tombstoned" => Status::Tombstoned,
        "unresolvable" => Status::Unresolvable,
        _ => Status::Other,
    };
    let handle_status = match v["handle_status"].as_str() {
        Some("verified") => HandleStatus::Verified,
        Some("mismatch") => HandleStatus::Mismatch,
        Some("unresolved") => HandleStatus::Unresolved,
        Some("unreachable") => HandleStatus::Unreachable,
        Some("pending") => HandleStatus::Pending,
        Some("none") | None => HandleStatus::None,
        Some(_) => HandleStatus::Other,
    };
    Some(Account {
        status,
        claimed_handle: handle(&v["claimed_handle"]),
        handle_status,
        handle: handle(&v["handle"]),
        handle_checked_at: time(&v["handle_checked_at"]),
        pds: v["pds"]
            .as_str()
            .map(|p| p.trim_end_matches('/').to_owned()),
        created_at: time(&v["created_at"]),
    })
}

/// Reads the answer of `GET /account/{did}`.
pub fn read_account(status: u16, body: &[u8]) -> Answer {
    let Ok(v) = serde_json::from_slice::<Value>(body) else {
        return Answer::Unavailable;
    };
    match status {
        200 if fresh(&v) => {
            parse_account(&v).map_or(Answer::Unavailable, |a| Answer::Account(Box::new(a)))
        }
        // Lodestar says `not_found` only while it is live and fresh.
        404 if v["error"].as_str() == Some("not_found") => Answer::NotFound,
        _ => Answer::Unavailable,
    }
}

/// Reads the answer of `POST /accounts`: each DID asked for with its
/// answer. `None` when the batch as a whole gives nothing to go by.
pub fn read_batch(status: u16, body: &[u8]) -> Option<Vec<(String, Answer)>> {
    if status != 200 {
        return None;
    }
    let v: Value = serde_json::from_slice(body).ok()?;
    if !fresh(&v) {
        return None;
    }
    let accounts = v["accounts"].as_object()?;
    Some(
        accounts
            .iter()
            .map(|(did, a)| {
                let answer = match a["error"].as_str() {
                    Some("not_found") => Answer::NotFound,
                    Some(_) => Answer::Unavailable,
                    None => parse_account(a)
                        .map_or(Answer::Unavailable, |a| Answer::Account(Box::new(a))),
                };
                (did.clone(), answer)
            })
            .collect(),
    )
}

impl Account {
    /// What the account's handle is, by lodestar's verification.
    pub fn verdict(&self) -> Verdict {
        match self.status {
            Status::Active => {}
            Status::Tombstoned => return Verdict::NotTheirs,
            Status::Unresolvable | Status::Other => return Verdict::Unknown,
        }
        match self.handle_status {
            HandleStatus::Verified => match &self.handle {
                Some(h) => Verdict::Verified(h.clone()),
                None => Verdict::Unknown,
            },
            HandleStatus::Mismatch | HandleStatus::Unresolved | HandleStatus::None => {
                Verdict::NotTheirs
            }
            HandleStatus::Unreachable => match &self.claimed_handle {
                Some(c) => Verdict::Unreachable(c.clone()),
                None => Verdict::NotTheirs,
            },
            HandleStatus::Pending | HandleStatus::Other => Verdict::Unknown,
        }
    }
}

/// One change of an account's handle claim or PDS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// When it took effect.
    pub changed_at: DateTime<Utc>,
    /// The handle claimed from then on (a claim, not a verified handle).
    pub handle_claim: Option<String>,
    /// The PDS endpoint named from then on.
    pub pds: Option<String>,
}

/// One page of `/account/{did}/history`: its changes, newest first, and
/// the cursor of the next page. `None` when it gives nothing to go by.
pub fn read_history(status: u16, body: &[u8]) -> Option<(Vec<Change>, Option<String>)> {
    if status != 200 {
        return None;
    }
    let v: Value = serde_json::from_slice(body).ok()?;
    if !fresh(&v) {
        return None;
    }
    let changes = v["entries"]
        .as_array()?
        .iter()
        .filter_map(|e| {
            Some(Change {
                changed_at: time(&e["changed_at"])?,
                handle_claim: handle(&e["handle_claim"]),
                pds: e["pds"].as_str().map(str::to_owned),
            })
        })
        .collect();
    let next = match &v["next_cursor"] {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    };
    Some((changes, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source(fresh: bool) -> Value {
        json!({"plc": {"phase": "live", "fresh": fresh, "cursor": 1}})
    }

    fn account(extra: Value, fresh: bool) -> Vec<u8> {
        let mut v = json!({
            "did": "did:plc:z72i7hdynmk6r22z27h6tvur", "status": "active",
            "pds": "https://pds.example/", "created_at": "2023-04-12T04:53:57.057Z",
            "source": source(fresh)
        });
        for (k, x) in extra.as_object().unwrap() {
            v[k] = x.clone();
        }
        serde_json::to_vec(&v).unwrap()
    }

    #[test]
    fn a_verified_handle_is_the_accounts() {
        let body = account(
            json!({"handle": "Bsky.App", "claimed_handle": "bsky.app",
                   "handle_status": "verified", "handle_checked_at": "2026-10-17T09:48:38.796Z"}),
            true,
        );
        let Answer::Account(a) = read_account(200, &body) else {
            panic!("an account");
        };
        assert_eq!(a.verdict(), Verdict::Verified("bsky.app".into()));
        assert_eq!(a.pds.as_deref(), Some("https://pds.example"));
        assert_eq!(
            a.created_at.unwrap().to_rfc3339(),
            "2023-04-12T04:53:57.057+00:00"
        );
        assert!(a.handle_checked_at.is_some());
    }

    #[test]
    fn an_answer_that_is_not_fresh_gives_nothing_to_go_by() {
        let body = account(json!({"handle_status": "none"}), false);
        assert_eq!(read_account(200, &body), Answer::Unavailable);
        let body = serde_json::to_vec(&json!({"accounts": {}, "source": source(false)})).unwrap();
        assert_eq!(read_batch(200, &body), None);
    }

    #[test]
    fn errors() {
        let e = |w: &str| serde_json::to_vec(&json!({"error": w})).unwrap();
        assert_eq!(read_account(404, &e("not_found")), Answer::NotFound);
        assert_eq!(read_account(404, &e("not_an_account")), Answer::Unavailable);
        assert_eq!(read_account(503, &e("syncing")), Answer::Unavailable);
        assert_eq!(read_account(503, &e("unavailable")), Answer::Unavailable);
        assert_eq!(read_account(500, b"<html>"), Answer::Unavailable);
        assert_eq!(read_batch(503, &e("syncing")), None);
    }

    #[test]
    fn verdicts() {
        let v = |status: &str, hs: &str, extra: Value| {
            let mut x = json!({"status": status, "handle_status": hs,
                               "claimed_handle": "alice.example"});
            for (k, y) in extra.as_object().unwrap() {
                x[k] = y.clone();
            }
            parse_account(&x).unwrap().verdict()
        };
        assert_eq!(v("active", "mismatch", json!({})), Verdict::NotTheirs);
        assert_eq!(v("active", "unresolved", json!({})), Verdict::NotTheirs);
        assert_eq!(v("active", "none", json!({})), Verdict::NotTheirs);
        assert_eq!(
            v("active", "unreachable", json!({})),
            Verdict::Unreachable("alice.example".into())
        );
        assert_eq!(v("active", "pending", json!({})), Verdict::Unknown);
        assert_eq!(v("active", "later", json!({})), Verdict::Unknown);
        // Verified needs the handle itself.
        assert_eq!(v("active", "verified", json!({})), Verdict::Unknown);
        assert_eq!(
            v("tombstoned", "verified", json!({"handle": "alice.example"})),
            Verdict::NotTheirs
        );
        assert_eq!(v("unresolvable", "none", json!({})), Verdict::Unknown);
        // A claim that is not a hostname is no claim.
        assert_eq!(
            v(
                "active",
                "unreachable",
                json!({"claimed_handle": "<script>"})
            ),
            Verdict::NotTheirs
        );
    }

    #[test]
    fn a_batch_answers_each_did() {
        let body = serde_json::to_vec(&json!({
            "accounts": {
                "did:plc:a": {"status": "active", "handle": "a.example", "handle_status": "verified"},
                "did:plc:b": {"error": "not_found"},
                "did:web:c.example": {"error": "unknown"}
            },
            "source": source(true)
        }))
        .unwrap();
        let mut got = read_batch(200, &body).unwrap();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(
            matches!(&got[0].1, Answer::Account(a) if a.verdict() == Verdict::Verified("a.example".into()))
        );
        assert_eq!(got[1].1, Answer::NotFound);
        assert_eq!(got[2].1, Answer::Unavailable);
        assert_eq!(
            accounts_body(["did:plc:a", "did:plc:b"]),
            br#"{"dids":["did:plc:a","did:plc:b"]}"#
        );
    }

    #[test]
    fn history_pages() {
        let body = serde_json::to_vec(&json!({
            "did": "did:plc:a", "status": "active",
            "entries": [
                {"changed_at": "2023-11-09T21:49:10.793Z", "handle_claim": "bsky.app", "pds": "https://new.example"},
                {"changed_at": "2023-04-12T04:53:57.057Z", "handle_claim": "Old.Example", "pds": "https://old.example"}
            ],
            "next_cursor": 2, "source": source(true)
        }))
        .unwrap();
        let (changes, next) = read_history(200, &body).unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[1].handle_claim.as_deref(), Some("old.example"));
        assert_eq!(next.as_deref(), Some("2"));
        let last = serde_json::to_vec(
            &json!({"entries": [], "next_cursor": null, "source": source(true)}),
        )
        .unwrap();
        assert_eq!(read_history(200, &last), Some((Vec::new(), None)));
        assert_eq!(read_history(503, b"{}"), None);
    }

    #[test]
    fn urls() {
        assert_eq!(base(" https://l.example/ "), Some("https://l.example"));
        assert_eq!(base(""), None);
        let did = Did::parse("did:plc:z72i7hdynmk6r22z27h6tvur").unwrap();
        assert_eq!(
            account_url("https://l.example", &did).unwrap().as_str(),
            "https://l.example/account/did:plc:z72i7hdynmk6r22z27h6tvur"
        );
        assert_eq!(
            history_url("https://l.example", &did, Some("3"))
                .unwrap()
                .as_str(),
            "https://l.example/account/did:plc:z72i7hdynmk6r22z27h6tvur/history?limit=200&cursor=3"
        );
    }
}
