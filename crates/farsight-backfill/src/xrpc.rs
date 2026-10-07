//! The XRPC and HTTP calls backfill makes: PDS repo reads, relay
//! enumeration and status, the PLC export, and a backlink index with the
//! public `/links` shape (see `docs/design/backfill.md`).

use farsight_core::{Did, Tid};
use farsight_storage::ids::Stamp;
use serde_json::Value;
use serde_json::value::RawValue;
use url::Url;

use crate::net::{Net, NetError, RECORD_NOT_FOUND, REPO_NOT_FOUND};

/// Repo-level errors that trigger the re-resolve / relay-status rule.
pub const REPO_ERRORS: [&str; 4] = [
    REPO_NOT_FOUND,
    "RepoDeactivated",
    "RepoTakendown",
    "RepoSuspended",
];

/// Whether `e` is a repo-level error (or `RecordNotFound` in a record
/// check, or a refused connection).
pub fn is_repo_level(e: &NetError) -> bool {
    match e {
        NetError::Http { name, .. } => {
            REPO_ERRORS.contains(&name.as_str()) || name == RECORD_NOT_FOUND
        }
        NetError::Transport(m) => m.contains("onnection refused"),
        _ => false,
    }
}

fn url(base: &str, path: &str, q: &[(&str, &str)]) -> Result<Url, NetError> {
    let mut u = Url::parse(base.trim_end_matches('/'))
        .and_then(|b| b.join(path))
        .map_err(|e| NetError::Transport(format!("bad URL {base}: {e}")))?;
    {
        let mut qp = u.query_pairs_mut();
        for (k, v) in q {
            qp.append_pair(k, v);
        }
    }
    if u.query() == Some("") {
        u.set_query(None);
    }
    Ok(u)
}

fn xrpc(base: &str, nsid: &str, q: &[(&str, &str)]) -> Result<Url, NetError> {
    url(base, &format!("/xrpc/{nsid}"), q)
}

fn s(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_owned)
}

/// Decodes a rev (TID) into the stored stamp. A rev whose time is past
/// the clock by more than [`farsight_core::tid::MAX_REV_AHEAD_US`] is
/// refused: a listing stamped with it would outrank every later write to
/// the records it stores.
pub fn rev_stamp(rev: &str) -> Result<Stamp, NetError> {
    rev_stamp_at(rev, chrono::Utc::now().timestamp_micros())
}

/// [`rev_stamp`] against the clock `now_us` (microseconds since the
/// epoch).
pub fn rev_stamp_at(rev: &str, now_us: i64) -> Result<Stamp, NetError> {
    let tid = Tid::parse(rev).map_err(|e| NetError::Decode(format!("rev {rev:?}: {e}")))?;
    if tid.is_ahead_of(now_us) {
        return Err(NetError::Decode(format!(
            "rev {rev:?} is ahead of the clock"
        )));
    }
    Ok(Stamp::from_tid(tid))
}

/// `com.atproto.repo.describeRepo`: the repo's collections.
pub async fn describe_repo(net: &Net, pds: &str, did: &Did) -> Result<Vec<String>, NetError> {
    let v = net
        .get_json(
            &xrpc(
                pds,
                "com.atproto.repo.describeRepo",
                &[("repo", did.as_str())],
            )?,
            "describeRepo",
        )
        .await?;
    Ok(v.get("collections")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default())
}

/// `com.atproto.sync.getLatestCommit`: the stamp `R` (decoded rev).
pub async fn latest_rev(net: &Net, pds: &str, did: &Did) -> Result<Stamp, NetError> {
    let v = net
        .get_json(
            &xrpc(
                pds,
                "com.atproto.sync.getLatestCommit",
                &[("did", did.as_str())],
            )?,
            "getLatestCommit",
        )
        .await?;
    rev_stamp(&s(&v, "rev").ok_or_else(|| NetError::Decode("no rev".into()))?)
}

/// Records asked for per `listRecords` page, and the most a page may
/// hold.
pub const PAGE_RECORDS: usize = 100;

/// One listed record: its URI and value.
#[derive(Debug, Clone)]
pub struct Listed {
    /// `uri`: the record's `at://` URI, as the PDS returned it.
    pub uri: String,
    /// `value`: the record itself, not yet checked. `None` when its text
    /// cannot be parsed as JSON (nested deeper than the parser allows,
    /// for one): the key is listed, the record is not usable.
    pub value: Option<Value>,
}

/// A `listRecords` page as it is written: the envelope is parsed, each
/// record's `value` is left as text and parsed on its own.
#[derive(serde::Deserialize)]
struct RawPage<'a> {
    #[serde(borrow)]
    records: Option<Vec<RawRecord<'a>>>,
    #[serde(borrow, default)]
    cursor: Option<&'a RawValue>,
}

#[derive(serde::Deserialize)]
struct RawRecord<'a> {
    #[serde(borrow, default)]
    uri: Option<&'a RawValue>,
    #[serde(borrow, default)]
    value: Option<&'a RawValue>,
}

/// The JSON string written as `raw`, if it is one.
fn raw_string(raw: Option<&RawValue>) -> Option<String> {
    serde_json::from_str::<String>(raw?.get()).ok()
}

/// Parses a `listRecords` body. One record that cannot be parsed costs
/// that record ([`Listed::value`] is `None`), not the page: the page's
/// other records and its cursor are read all the same. A page with more
/// than [`PAGE_RECORDS`] records is refused; an entry without a `uri` or a
/// `value` is dropped.
pub fn parse_page(body: &[u8]) -> Result<(Vec<Listed>, Option<String>), NetError> {
    let page: RawPage<'_> =
        serde_json::from_slice(body).map_err(|e| NetError::Decode(e.to_string()))?;
    let records = page
        .records
        .ok_or_else(|| NetError::Decode("no records".into()))?;
    if records.len() > PAGE_RECORDS {
        return Err(NetError::Decode(format!(
            "{} records in a page of at most {PAGE_RECORDS}",
            records.len()
        )));
    }
    let listed = records
        .into_iter()
        .filter_map(|r| {
            let uri = raw_string(r.uri)?;
            let raw = r.value?;
            Some(Listed {
                uri,
                value: serde_json::from_str::<Value>(raw.get()).ok(),
            })
        })
        .collect();
    Ok((listed, raw_string(page.cursor)))
}

/// `com.atproto.repo.listRecords` with `limit=100&reverse=true` (ascending
/// rkey).
pub async fn list_records(
    net: &Net,
    pds: &str,
    did: &Did,
    collection: &str,
    cursor: Option<&str>,
) -> Result<(Vec<Listed>, Option<String>), NetError> {
    let limit = PAGE_RECORDS.to_string();
    let mut q = vec![
        ("repo", did.as_str()),
        ("collection", collection),
        ("limit", limit.as_str()),
        ("reverse", "true"),
    ];
    if let Some(c) = cursor {
        q.push(("cursor", c));
    }
    let body = net
        .get_body(
            &xrpc(pds, "com.atproto.repo.listRecords", &q)?,
            "listRecords",
        )
        .await?;
    parse_page(&body)
}

/// `com.atproto.repo.getRecord`; `Ok(None)` on `RecordNotFound`.
pub async fn get_record(
    net: &Net,
    pds: &str,
    did: &Did,
    collection: &str,
    rkey: &str,
) -> Result<Option<Value>, NetError> {
    match net
        .get_json(
            &xrpc(
                pds,
                "com.atproto.repo.getRecord",
                &[
                    ("repo", did.as_str()),
                    ("collection", collection),
                    ("rkey", rkey),
                ],
            )?,
            "getRecord",
        )
        .await
    {
        Ok(v) => Ok(v.get("value").cloned()),
        Err(NetError::Http { name, .. }) if name == RECORD_NOT_FOUND => Ok(None),
        Err(e) => Err(e),
    }
}

/// A relay's view of a repo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoStatus {
    /// `active`; a response without the field reads as active.
    pub active: bool,
    /// Whether the response carried `active` as a boolean. Only then has
    /// the relay said that the account is active.
    pub stated: bool,
    /// `status` when inactive.
    pub status: Option<String>,
}

/// `com.atproto.sync.getRepoStatus` at the relay.
pub async fn repo_status(net: &Net, relay: &str, did: &Did) -> Result<RepoStatus, NetError> {
    let v = net
        .get_json(
            &xrpc(
                relay,
                "com.atproto.sync.getRepoStatus",
                &[("did", did.as_str())],
            )?,
            "getRepoStatus",
        )
        .await?;
    let active = v.get("active").and_then(Value::as_bool);
    Ok(RepoStatus {
        active: active.unwrap_or(true),
        stated: active.is_some(),
        status: s(&v, "status"),
    })
}

/// One `listRepos` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedRepo {
    /// `did`. An entry without one is dropped.
    pub did: String,
    /// `rev`: the repo's latest rev (a TID) as the relay knows it, if
    /// reported. A repair cycle reads its time to pick candidates.
    pub rev: Option<String>,
    /// `active`; an entry without the field reads as active.
    pub active: bool,
    /// `status` when inactive.
    pub status: Option<String>,
}

/// `com.atproto.sync.listRepos` at the relay.
pub async fn list_repos(
    net: &Net,
    relay: &str,
    cursor: Option<&str>,
    limit: u32,
) -> Result<(Vec<ListedRepo>, Option<String>), NetError> {
    let limit = limit.clamp(1, 1000).to_string();
    let mut q = vec![("limit", limit.as_str())];
    if let Some(c) = cursor {
        q.push(("cursor", c));
    }
    let v = net
        .get_json(&xrpc(relay, "com.atproto.sync.listRepos", &q)?, "listRepos")
        .await?;
    let repos = v
        .get("repos")
        .and_then(Value::as_array)
        .ok_or_else(|| NetError::Decode("no repos".into()))?
        .iter()
        .filter_map(|r| {
            Some(ListedRepo {
                did: s(r, "did")?,
                rev: s(r, "rev"),
                active: r.get("active").and_then(Value::as_bool).unwrap_or(true),
                status: s(r, "status"),
            })
        })
        .collect();
    Ok((repos, s(&v, "cursor")))
}

/// `com.atproto.sync.listReposByCollection` at the relay.
pub async fn list_repos_by_collection(
    net: &Net,
    relay: &str,
    collection: &str,
    cursor: Option<&str>,
    limit: u32,
) -> Result<(Vec<String>, Option<String>), NetError> {
    let limit = limit.clamp(1, 2000).to_string();
    let mut q = vec![("collection", collection), ("limit", limit.as_str())];
    if let Some(c) = cursor {
        q.push(("cursor", c));
    }
    let v = net
        .get_json(
            &xrpc(relay, "com.atproto.sync.listReposByCollection", &q)?,
            "listReposByCollection",
        )
        .await?;
    let repos = v
        .get("repos")
        .and_then(Value::as_array)
        .ok_or_else(|| NetError::Decode("no repos".into()))?
        .iter()
        .filter_map(|r| s(r, "did"))
        .collect();
    Ok((repos, s(&v, "cursor")))
}

/// One PLC export operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportOp {
    /// `did`: the DID the operation belongs to. A DID appears once per
    /// operation, so the same one can recur across the export.
    pub did: String,
    /// The PDS endpoint the operation sets, if any.
    pub pds: Option<String>,
    /// `createdAt` (the next page's `after`).
    pub created_at: String,
    /// A nullified operation (kept for paging; carries no membership).
    pub nullified: bool,
}

/// PLC `/export?count=…&after=…` (JSON lines; at most 1000 per page).
pub async fn plc_export(
    net: &Net,
    plc: &str,
    after: Option<&str>,
    count: u32,
) -> Result<Vec<ExportOp>, NetError> {
    let count = count.clamp(1, 1000).to_string();
    let mut q = vec![("count", count.as_str())];
    if let Some(a) = after {
        q.push(("after", a));
    }
    // The export draws on the shared half of the PLC limiter; resolution
    // keeps its reserved half.
    net.plc.acquire(crate::net::PlcUse::Export).await;
    let body = net.get_text(&url(plc, "/export", &q)?).await?;
    Ok(body
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|v| {
            let op = v.get("operation")?;
            let pds = op
                .pointer("/services/atproto_pds/endpoint")
                .and_then(Value::as_str)
                .map(str::to_owned);
            Some(ExportOp {
                did: s(&v, "did")?,
                pds,
                created_at: s(&v, "createdAt")?,
                nullified: v.get("nullified").and_then(Value::as_bool).unwrap_or(false),
            })
        })
        .collect())
}

/// One backlink reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backlink {
    /// The linking record's repo.
    pub did: String,
    /// Its collection.
    pub collection: String,
    /// Its record key, as the index reports it. Not validated here.
    pub rkey: String,
}

/// A backlink index `/links?target=&collection=&path=` page.
pub async fn backlinks(
    net: &Net,
    base: &str,
    target: &str,
    collection: &str,
    path: &str,
    cursor: Option<&str>,
) -> Result<(Vec<Backlink>, Option<String>), NetError> {
    let mut q = vec![
        ("target", target),
        ("collection", collection),
        ("path", path),
        ("limit", "100"),
    ];
    if let Some(c) = cursor {
        q.push(("cursor", c));
    }
    let v = net.get_json(&url(base, "/links", &q)?, "backlinks").await?;
    let links = v
        .get("linking_records")
        .and_then(Value::as_array)
        .ok_or_else(|| NetError::Decode("no linking_records".into()))?
        .iter()
        .filter_map(|r| {
            Some(Backlink {
                did: s(r, "did")?,
                collection: s(r, "collection")?,
                rkey: s(r, "rkey")?,
            })
        })
        .collect();
    Ok((links, s(&v, "cursor").filter(|c| !c.is_empty())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        let u = xrpc(
            "https://pds.example/",
            "com.atproto.repo.listRecords",
            &[("repo", "did:plc:x"), ("limit", "100")],
        )
        .unwrap();
        assert_eq!(
            u.as_str(),
            "https://pds.example/xrpc/com.atproto.repo.listRecords?repo=did%3Aplc%3Ax&limit=100"
        );
        assert!(is_repo_level(&NetError::Http {
            status: 400,
            name: "RepoDeactivated".into()
        }));
        assert!(!is_repo_level(&NetError::Http {
            status: 500,
            name: String::new()
        }));
    }

    #[test]
    fn one_record_that_cannot_be_parsed_costs_that_record_not_the_page() {
        let deep = format!("{}{}", "[".repeat(4_000), "]".repeat(4_000));
        let body = format!(
            r#"{{"records":[
                 {{"uri":"at://did:plc:a/app.bsky.graph.block/1","cid":"x","value":{{"subject":"did:plc:b"}}}},
                 {{"uri":"at://did:plc:a/app.bsky.graph.block/2","cid":"x","value":{{"subject":{deep}}}}},
                 {{"uri":"at://did:plc:a/app.bsky.graph.block/3","cid":"x","value":{{"subject":"did:plc:c"}}}},
                 {{"cid":"no uri","value":{{}}}},
                 {{"uri":"at://did:plc:a/app.bsky.graph.block/5"}}
               ],"cursor":"next"}}"#
        );
        // Parsed whole, the page is lost with the one record.
        assert!(serde_json::from_str::<Value>(&body).is_err());
        let (records, cursor) = parse_page(body.as_bytes()).unwrap();
        assert_eq!(cursor.as_deref(), Some("next"));
        let uris: Vec<&str> = records.iter().map(|r| r.uri.as_str()).collect();
        assert_eq!(
            uris,
            [
                "at://did:plc:a/app.bsky.graph.block/1",
                "at://did:plc:a/app.bsky.graph.block/2",
                "at://did:plc:a/app.bsky.graph.block/3"
            ]
        );
        assert!(records[0].value.is_some() && records[2].value.is_some());
        assert!(
            records[1].value.is_none(),
            "the nested record is listed without a value"
        );
        // The last page has no cursor, written or null.
        for tail in [r#"{"records":[]}"#, r#"{"records":[],"cursor":null}"#] {
            assert_eq!(parse_page(tail.as_bytes()).unwrap().1, None);
        }
        // Not a page at all.
        for bad in ["", "[]", r#"{"cursor":"x"}"#, r#"{"records":7}"#, "{"] {
            assert!(
                matches!(parse_page(bad.as_bytes()), Err(NetError::Decode(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_page_with_more_records_than_asked_for_is_refused() {
        let page = |n: usize| {
            let records: Vec<String> = (0..n)
                .map(|i| format!(r#"{{"uri":"at://did:plc:a/c/{i}","value":{{}}}}"#))
                .collect();
            format!(r#"{{"records":[{}]}}"#, records.join(","))
        };
        assert_eq!(
            parse_page(page(PAGE_RECORDS).as_bytes()).unwrap().0.len(),
            PAGE_RECORDS
        );
        assert!(matches!(
            parse_page(page(PAGE_RECORDS + 1).as_bytes()),
            Err(NetError::Decode(_))
        ));
    }

    #[test]
    fn a_listing_rev_from_the_future_is_refused() {
        use farsight_core::tid::MAX_REV_AHEAD_US;
        let now = 1_800_000_000_000_000u64;
        let rev = |us: u64| Tid::from_parts(us, 0).unwrap().encode();
        let at = |us: u64| rev_stamp_at(&rev(us), now as i64);
        assert_eq!(
            at(now - 1).unwrap(),
            Stamp::from_tid(Tid::from_parts(now - 1, 0).unwrap())
        );
        assert!(at(now + MAX_REV_AHEAD_US).is_ok());
        assert!(matches!(
            at(now + MAX_REV_AHEAD_US + 1),
            Err(NetError::Decode(_))
        ));
        assert!(matches!(at((1 << 53) - 1), Err(NetError::Decode(_))));
        assert!(matches!(
            rev_stamp_at("not-a-tid", now as i64),
            Err(NetError::Decode(_))
        ));
    }
}
