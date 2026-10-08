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
    collections_of(&v)
}

/// The `collections` of a `describeRepo` body. A body without the list,
/// or with an entry that is not a string, is not an answer: read as "no
/// collections" it would have every stored row of the repo removed.
pub fn collections_of(v: &Value) -> Result<Vec<String>, NetError> {
    v.get("collections")
        .and_then(Value::as_array)
        .ok_or_else(|| NetError::Decode("describeRepo without collections".into()))?
        .iter()
        .map(|c| {
            c.as_str()
                .map(str::to_owned)
                .ok_or_else(|| NetError::Decode("a collection that is not a string".into()))
        })
        .collect()
}

/// The `cursor` of a paged answer: absent or `null` ends the listing, a
/// string continues it, anything else is not an answer (read as the end
/// it would close a listing or an enumeration early).
pub fn cursor_of(v: &Value) -> Result<Option<String>, NetError> {
    match v.get("cursor") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(c)) => Ok(Some(c.clone())),
        Some(_) => Err(NetError::Decode("a cursor that is not a string".into())),
    }
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

/// The cursor written as `raw`: absent or `null` ends the listing, a
/// string continues it, anything else is refused.
fn raw_cursor(raw: Option<&RawValue>) -> Result<Option<String>, NetError> {
    let Some(raw) = raw else { return Ok(None) };
    match serde_json::from_str::<Option<String>>(raw.get()) {
        Ok(c) => Ok(c),
        Err(_) => Err(NetError::Decode("a cursor that is not a string".into())),
    }
}

/// Parses a `listRecords` body. One record that cannot be parsed costs
/// that record ([`Listed::value`] is `None`), not the page: the page's
/// other records and its cursor are read all the same. A page with more
/// than [`PAGE_RECORDS`] records is refused, and so is a cursor that is
/// not a string; an entry without a `uri` or a `value` is dropped.
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
    Ok((listed, raw_cursor(page.cursor)?))
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
        // Only the host's own "not found" says the record is gone. A
        // 200 without a `value` is not an answer.
        Ok(v) => match v.get("value") {
            Some(value) => Ok(Some(value.clone())),
            None => Err(NetError::Decode("getRecord without a value".into())),
        },
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
        .map(|r| {
            Ok(ListedRepo {
                did: s(r, "did").ok_or_else(|| NetError::Decode("a repo without a did".into()))?,
                rev: s(r, "rev"),
                active: r.get("active").and_then(Value::as_bool).unwrap_or(true),
                status: s(r, "status"),
            })
        })
        .collect::<Result<Vec<_>, NetError>>()?;
    Ok((repos, cursor_of(&v)?))
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
        .map(|r| s(r, "did").ok_or_else(|| NetError::Decode("a repo without a did".into())))
        .collect::<Result<Vec<_>, NetError>>()?;
    Ok((repos, cursor_of(&v)?))
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
    parse_export(&body)
}

/// Parses a PLC export page. A line that is not an operation with a
/// `did` and a `createdAt` fails the page: dropped, it would make the
/// page look shorter than it is, and a short page ends the enumeration.
pub fn parse_export(body: &str) -> Result<Vec<ExportOp>, NetError> {
    body.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let bad = |what: &str| NetError::Decode(format!("export line {what}"));
            let v: Value = serde_json::from_str(l).map_err(|_| bad("is not JSON"))?;
            let op = v.get("operation").ok_or_else(|| bad("has no operation"))?;
            let pds = op
                .pointer("/services/atproto_pds/endpoint")
                .and_then(Value::as_str)
                .map(str::to_owned);
            Ok(ExportOp {
                did: s(&v, "did").ok_or_else(|| bad("has no did"))?,
                pds,
                created_at: s(&v, "createdAt").ok_or_else(|| bad("has no createdAt"))?,
                nullified: v.get("nullified").and_then(Value::as_bool).unwrap_or(false),
            })
        })
        .collect()
}

/// Where the next export page starts, and how many operations of this
/// one to use. The directory returns operations created after `after`,
/// so a page that ends inside a run of operations with one `createdAt`
/// would lose the rest of the run. A full page therefore gives up its
/// last run and the next page starts before it. A page that is one run
/// throughout is used whole (there is nothing earlier to start from).
pub fn export_step(ops: &[ExportOp], full: bool) -> (usize, Option<&str>) {
    let Some(last) = ops.last() else {
        return (0, None);
    };
    if !full {
        return (ops.len(), Some(last.created_at.as_str()));
    }
    let keep = ops
        .iter()
        .rposition(|o| o.created_at != last.created_at)
        .map(|i| i + 1);
    match keep {
        Some(n) => (n, Some(ops[n - 1].created_at.as_str())),
        None => (ops.len(), Some(last.created_at.as_str())),
    }
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
    fn a_body_without_the_expected_field_is_not_an_empty_answer() {
        // describeRepo: no list, or a list with a non-string, is refused.
        assert!(collections_of(&serde_json::json!({"handle": "a.test"})).is_err());
        assert!(collections_of(&serde_json::json!({"collections": "x"})).is_err());
        assert!(collections_of(&serde_json::json!({"collections": ["a", 1]})).is_err());
        assert_eq!(
            collections_of(&serde_json::json!({"collections": []})).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            collections_of(&serde_json::json!({"collections": ["app.bsky.graph.block"]})).unwrap(),
            vec!["app.bsky.graph.block".to_owned()]
        );
        // A cursor: absent or null ends, a string continues, else refused.
        assert_eq!(cursor_of(&serde_json::json!({})).unwrap(), None);
        assert_eq!(
            cursor_of(&serde_json::json!({"cursor": null})).unwrap(),
            None
        );
        assert_eq!(
            cursor_of(&serde_json::json!({"cursor": "c1"})).unwrap(),
            Some("c1".to_owned())
        );
        assert!(cursor_of(&serde_json::json!({"cursor": 7})).is_err());
        assert!(parse_page(br#"{"records":[],"cursor":7}"#).is_err());
        assert!(parse_page(br#"{"records":[],"cursor":{"a":1}}"#).is_err());
        let (_, c) = parse_page(br#"{"records":[],"cursor":null}"#).unwrap();
        assert_eq!(c, None);
        let (_, c) = parse_page(br#"{"records":[],"cursor":"k"}"#).unwrap();
        assert_eq!(c.as_deref(), Some("k"));
    }

    #[test]
    fn an_export_line_that_cannot_be_read_fails_the_page() {
        let good = r#"{"did":"did:plc:a","operation":{"services":{"atproto_pds":{"endpoint":"https://p.test"}}},"createdAt":"2026-01-01T00:00:00.000Z","nullified":false}"#;
        let ops = parse_export(&format!(
            "{good}

{good}
"
        ))
        .unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].pds.as_deref(), Some("https://p.test"));
        assert!(
            parse_export(&format!(
                "{good}
not json
"
            ))
            .is_err()
        );
        assert!(parse_export(r#"{"did":"did:plc:a","createdAt":"x"}"#).is_err());
        assert!(parse_export(r#"{"operation":{},"createdAt":"x"}"#).is_err());
        assert!(parse_export(r#"{"did":"did:plc:a","operation":{}}"#).is_err());
    }

    #[test]
    fn a_full_export_page_gives_up_its_last_run_of_equal_timestamps() {
        let op = |did: &str, at: &str| ExportOp {
            did: did.to_owned(),
            pds: None,
            created_at: at.to_owned(),
            nullified: false,
        };
        let ops = vec![op("a", "t1"), op("b", "t2"), op("c", "t3"), op("d", "t3")];
        // Full: the run at t3 may go on in the next page, which starts
        // after t2 and reads all of it.
        assert_eq!(export_step(&ops, true), (2, Some("t2")));
        // Short: the page is the end of the export.
        assert_eq!(export_step(&ops, false), (4, Some("t3")));
        // One run throughout: used whole.
        let same = vec![op("a", "t1"), op("b", "t1")];
        assert_eq!(export_step(&same, true), (2, Some("t1")));
        assert_eq!(export_step(&[], true), (0, None));
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
