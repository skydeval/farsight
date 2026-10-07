//! A fake ATProto world on loopback: one PDS (also serving the relay
//! endpoints) and a PLC directory, both driven by a shared [`World`] the
//! checks script. Every request is logged so checks can assert what the
//! backfill actually asked for.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};

/// One repo on the fake PDS.
#[derive(Debug, Clone, Default)]
pub struct Repo {
    /// `(collection, rkey) → value`.
    pub records: BTreeMap<(String, String), Value>,
    /// Current rev (TID).
    pub rev: String,
    /// Every PDS call for this repo answers 400 with this error name.
    pub repo_error: Option<String>,
    /// `listRecords` of this collection answers 500.
    pub fail_collection: Option<String>,
}

/// The scripted world.
#[derive(Debug, Default)]
pub struct World {
    /// Repos on the PDS.
    pub repos: HashMap<String, Repo>,
    /// PLC: DID → PDS endpoint.
    pub plc: HashMap<String, String>,
    /// Relay `getRepoStatus` overrides: DID → (active, status).
    pub status: HashMap<String, (bool, Option<String>)>,
    /// Relay `getRepoStatus` failures: for these DIDs the relay answers
    /// 500 instead of a status.
    pub status_down: HashSet<String>,
    /// Relay `listReposByCollection`: collection → DIDs (sorted).
    pub by_collection: BTreeMap<String, BTreeSet<String>>,
    /// Relay `listRepos`: `(did, rev, active)`, in order.
    pub listed: Vec<(String, String, bool)>,
    /// Relay supports `listReposByCollection`.
    pub collections_supported: bool,
    /// Backlink index: `(target, collection, did, rkey)`.
    pub backlinks: Vec<(String, String, String, String)>,
    /// Request log: `(method, did, collection)`.
    pub hits: Vec<(String, String, String)>,
}

/// Shared handle.
pub type Shared = Arc<Mutex<World>>;

/// Locks the world.
pub fn w(s: &Shared) -> std::sync::MutexGuard<'_, World> {
    s.lock().unwrap_or_else(|e| e.into_inner())
}

fn err(status: StatusCode, name: &str) -> Response {
    (status, Json(json!({"error": name, "message": name}))).into_response()
}

/// How reference PDSes answer for a repo they do not host.
fn no_repo(did: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "InvalidRequest", "message": format!("Could not find repo: {did}")})),
    )
        .into_response()
}

type Q = Query<HashMap<String, String>>;

async fn xrpc(State(s): State<Shared>, Path(method): Path<String>, Query(q): Q) -> Response {
    let mut world = w(&s);
    let did = q
        .get("repo")
        .or_else(|| q.get("did"))
        .cloned()
        .unwrap_or_default();
    let coll = q.get("collection").cloned().unwrap_or_default();
    let short = method.rsplit('.').next().unwrap_or("").to_owned();
    world.hits.push((short.clone(), did.clone(), coll.clone()));
    match short.as_str() {
        "getRepoStatus" => {
            if world.status_down.contains(&did) {
                return err(StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError");
            }
            if let Some((active, status)) = world.status.get(&did).cloned() {
                return Json(json!({"did": did, "active": active, "status": status}))
                    .into_response();
            }
            match world.repos.get(&did) {
                Some(_) => Json(json!({"did": did, "active": true})).into_response(),
                None => err(StatusCode::BAD_REQUEST, "RepoNotFound"),
            }
        }
        "listRepos" => {
            let limit: usize = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(500);
            let start: usize = q.get("cursor").and_then(|c| c.parse().ok()).unwrap_or(0);
            let page: Vec<Value> = world
                .listed
                .iter()
                .skip(start)
                .take(limit)
                .map(|(d, rev, active)| {
                    let mut v = json!({"did": d, "head": "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm", "rev": rev, "active": active});
                    if !active {
                        v["status"] = json!("deactivated");
                    }
                    v
                })
                .collect();
            let end = start + page.len();
            let cursor = (end < world.listed.len()).then(|| end.to_string());
            Json(json!({"repos": page, "cursor": cursor})).into_response()
        }
        "listReposByCollection" => {
            if !world.collections_supported {
                return err(StatusCode::NOT_IMPLEMENTED, "MethodNotImplemented");
            }
            let limit: usize = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(500);
            let cursor = q.get("cursor").cloned().unwrap_or_default();
            let all = world.by_collection.get(&coll).cloned().unwrap_or_default();
            let page: Vec<String> = all
                .range::<String, _>((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .take(limit)
                .cloned()
                .collect();
            let next = page
                .last()
                .filter(|l| {
                    all.range::<String, _>((
                        std::ops::Bound::Excluded((*l).clone()),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .is_some()
                })
                .cloned();
            let repos: Vec<Value> = page.iter().map(|d| json!({"did": d})).collect();
            Json(json!({"repos": repos, "cursor": next})).into_response()
        }
        _ => {
            let Some(repo) = world.repos.get(&did).cloned() else {
                return no_repo(&did);
            };
            if let Some(e) = &repo.repo_error {
                if e == "RepoNotFound" {
                    return no_repo(&did);
                }
                return err(StatusCode::BAD_REQUEST, e);
            }
            match short.as_str() {
                "describeRepo" => {
                    let colls: BTreeSet<&String> = repo.records.keys().map(|(c, _)| c).collect();
                    Json(json!({"did": did, "handle": "handle.invalid", "collections": colls}))
                        .into_response()
                }
                "getLatestCommit" => Json(json!({
                    "cid": "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm",
                    "rev": repo.rev
                }))
                .into_response(),
                "listRecords" => {
                    if repo.fail_collection.as_deref() == Some(coll.as_str()) {
                        return err(StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError");
                    }
                    let limit: usize = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(50);
                    let cursor = q.get("cursor").cloned().unwrap_or_default();
                    let recs: Vec<(&String, &Value)> = repo
                        .records
                        .iter()
                        .filter(|((c, r), _)| *c == coll && *r > cursor)
                        .map(|((_, r), v)| (r, v))
                        .collect();
                    let page: Vec<Value> = recs
                        .iter()
                        .take(limit)
                        .map(|(r, v)| {
                            json!({"uri": format!("at://{did}/{coll}/{r}"),
                                   "cid": "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm",
                                   "value": v})
                        })
                        .collect();
                    let next = (recs.len() > limit).then(|| recs[limit - 1].0.clone());
                    Json(json!({"records": page, "cursor": next})).into_response()
                }
                "getRecord" => {
                    let rkey = q.get("rkey").cloned().unwrap_or_default();
                    match repo.records.get(&(coll.clone(), rkey.clone())) {
                        Some(v) => {
                            Json(json!({"uri": format!("at://{did}/{coll}/{rkey}"), "value": v}))
                                .into_response()
                        }
                        None => err(StatusCode::BAD_REQUEST, "RecordNotFound"),
                    }
                }
                _ => err(StatusCode::NOT_IMPLEMENTED, "MethodNotImplemented"),
            }
        }
    }
}

async fn links(State(s): State<Shared>, Query(q): Q) -> Response {
    let mut world = w(&s);
    let target = q.get("target").cloned().unwrap_or_default();
    let coll = q.get("collection").cloned().unwrap_or_default();
    world
        .hits
        .push(("links".into(), target.clone(), coll.clone()));
    let limit: usize = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(16);
    let start: usize = q.get("cursor").and_then(|c| c.parse().ok()).unwrap_or(0);
    let all: Vec<Value> = world
        .backlinks
        .iter()
        .filter(|(t, c, _, _)| *t == target && *c == coll)
        .map(|(_, c, d, r)| json!({"did": d, "collection": c, "rkey": r}))
        .collect();
    let page: Vec<Value> = all.iter().skip(start).take(limit).cloned().collect();
    let end = start + page.len();
    let cursor = (end < all.len()).then(|| end.to_string());
    Json(json!({"total": all.len(), "linking_records": page, "cursor": cursor})).into_response()
}

async fn plc_doc(State(s): State<Shared>, Path(did): Path<String>) -> Response {
    let mut world = w(&s);
    world.hits.push(("plc".into(), did.clone(), String::new()));
    match world.plc.get(&did) {
        Some(ep) => Json(json!({
            "id": did,
            "alsoKnownAs": [],
            "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": ep}]
        }))
        .into_response(),
        None => (StatusCode::NOT_FOUND, "DID not registered").into_response(),
    }
}

/// Starts the PDS (+ relay) and PLC servers; returns their base URLs.
pub async fn start(world: Shared) -> Result<(String, String), String> {
    let pds = Router::new()
        .route("/xrpc/{method}", get(xrpc))
        .route("/links", get(links))
        .with_state(world.clone());
    let plc = Router::new()
        .route("/{did}", get(plc_doc))
        .with_state(world);
    let l1 = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let l2 = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let a1 = l1.local_addr().map_err(|e| e.to_string())?;
    let a2 = l2.local_addr().map_err(|e| e.to_string())?;
    tokio::spawn(async move {
        let _ = axum::serve(l1, pds).await;
    });
    tokio::spawn(async move {
        let _ = axum::serve(l2, plc).await;
    });
    Ok((format!("http://{a1}"), format!("http://{a2}")))
}
