//! The XRPC and HTTP calls backfill makes: PDS repo reads, relay
//! enumeration and status, the PLC export, and a backlink index with the
//! public `/links` shape (see `docs/design/backfill.md`).

use farsight_core::Tid;
use serde_json::Value;
use url::Url;

use crate::net::{Net, NetError};

/// Repo-level errors that trigger the re-resolve / relay-status rule.
pub const REPO_ERRORS: [&str; 4] = [
    "RepoNotFound",
    "RepoDeactivated",
    "RepoTakendown",
    "RepoSuspended",
];

/// Whether `e` is a repo-level error (or `RecordNotFound` in a record
/// check, or a refused connection).
pub fn is_repo_level(e: &NetError) -> bool {
    match e {
        NetError::Http { name, .. } => {
            REPO_ERRORS.contains(&name.as_str()) || name == "RecordNotFound"
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

/// Decodes a rev (TID) into the stored stamp.
pub fn rev_stamp(rev: &str) -> Result<i64, NetError> {
    Tid::parse(rev)
        .map(Tid::as_i64)
        .map_err(|e| NetError::Decode(format!("rev {rev:?}: {e}")))
}

/// `com.atproto.repo.describeRepo`: the repo's collections.
pub async fn describe_repo(net: &Net, pds: &str, did: &str) -> Result<Vec<String>, NetError> {
    let v = net
        .get_json(
            &xrpc(pds, "com.atproto.repo.describeRepo", &[("repo", did)])?,
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
pub async fn latest_rev(net: &Net, pds: &str, did: &str) -> Result<i64, NetError> {
    let v = net
        .get_json(
            &xrpc(pds, "com.atproto.sync.getLatestCommit", &[("did", did)])?,
            "getLatestCommit",
        )
        .await?;
    rev_stamp(&s(&v, "rev").ok_or_else(|| NetError::Decode("no rev".into()))?)
}

/// One listed record: its URI and value.
#[derive(Debug, Clone)]
pub struct Listed {
    /// `uri`.
    pub uri: String,
    /// `value`.
    pub value: Value,
}

/// `com.atproto.repo.listRecords` with `limit=100&reverse=true` (ascending
/// rkey).
pub async fn list_records(
    net: &Net,
    pds: &str,
    did: &str,
    collection: &str,
    cursor: Option<&str>,
) -> Result<(Vec<Listed>, Option<String>), NetError> {
    let mut q = vec![
        ("repo", did),
        ("collection", collection),
        ("limit", "100"),
        ("reverse", "true"),
    ];
    if let Some(c) = cursor {
        q.push(("cursor", c));
    }
    let v = net
        .get_json(
            &xrpc(pds, "com.atproto.repo.listRecords", &q)?,
            "listRecords",
        )
        .await?;
    let records = v
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| NetError::Decode("no records".into()))?
        .iter()
        .filter_map(|r| {
            Some(Listed {
                uri: s(r, "uri")?,
                value: r.get("value")?.clone(),
            })
        })
        .collect();
    Ok((records, s(&v, "cursor")))
}

/// `com.atproto.repo.getRecord`; `Ok(None)` on `RecordNotFound`.
pub async fn get_record(
    net: &Net,
    pds: &str,
    did: &str,
    collection: &str,
    rkey: &str,
) -> Result<Option<Value>, NetError> {
    match net
        .get_json(
            &xrpc(
                pds,
                "com.atproto.repo.getRecord",
                &[("repo", did), ("collection", collection), ("rkey", rkey)],
            )?,
            "getRecord",
        )
        .await
    {
        Ok(v) => Ok(v.get("value").cloned()),
        Err(NetError::Http { name, .. }) if name == "RecordNotFound" => Ok(None),
        Err(e) => Err(e),
    }
}

/// A relay's view of a repo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoStatus {
    /// `active`.
    pub active: bool,
    /// `status` when inactive.
    pub status: Option<String>,
}

/// `com.atproto.sync.getRepoStatus` at the relay.
pub async fn repo_status(net: &Net, relay: &str, did: &str) -> Result<RepoStatus, NetError> {
    let v = net
        .get_json(
            &xrpc(relay, "com.atproto.sync.getRepoStatus", &[("did", did)])?,
            "getRepoStatus",
        )
        .await?;
    Ok(RepoStatus {
        active: v.get("active").and_then(Value::as_bool).unwrap_or(true),
        status: s(&v, "status"),
    })
}

/// One `listRepos` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedRepo {
    /// DID.
    pub did: String,
    /// Latest rev, if reported.
    pub rev: Option<String>,
    /// `active`.
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
    /// DID.
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
    /// Its record key.
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
}
