//! A stand-in PLC directory for the stage-8 harness: it answers every
//! `did:plc` with a document that names no handle and an audit log with a
//! creation date, and counts what it was asked for. Handle warming against
//! it ends as `failed` for every account without leaving the machine, and
//! the counts show which accounts the server asked about, and how often.
//!
//! For the accounts a probe names it also stands in for their own server:
//! their audit log names it as the PDS, and it answers `getRecord` for
//! the profile records it was given (an error for any other).
//!
//! It listens on a TEST-NET-2 address (a bridge the harness creates), an
//! address the safe outbound client treats as public, so no rule of that
//! client is bypassed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::json;

#[derive(Debug, Default)]
struct Inner {
    documents: HashMap<String, u32>,
    audits: HashMap<String, u32>,
    /// Accounts whose audit log names this stand-in as their PDS.
    hosted: Vec<String>,
    /// Profile records by account.
    profiles: HashMap<String, serde_json::Value>,
    /// Accounts whose audit log has a history: the handle they claim now.
    claims: HashMap<String, String>,
    records: u32,
}

/// The running stand-in.
#[derive(Debug)]
pub struct Plc {
    /// `http://{addr}:{port}`.
    pub base: String,
    inner: Mutex<Inner>,
}

impl Plc {
    /// Starts it on `addr`, any port.
    pub async fn start(addr: &str) -> Result<Arc<Plc>, String> {
        let listener = tokio::net::TcpListener::bind((addr, 0))
            .await
            .map_err(|e| format!("binding the stand-in PLC on {addr}: {e}"))?;
        let port = listener.local_addr().map_err(|e| e.to_string())?.port();
        let plc = Arc::new(Plc {
            base: format!("http://{addr}:{port}"),
            inner: Mutex::new(Inner::default()),
        });
        let app = Router::new()
            .route("/{did}", get(document))
            .route("/{did}/log/audit", get(audit))
            .route("/xrpc/com.atproto.repo.getRecord", get(record))
            .with_state(plc.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(plc)
    }

    /// How often the document of `did` was asked for.
    pub fn documents(&self, did: &str) -> u32 {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.documents.get(did).copied().unwrap_or(0)
    }

    /// From now on the audit log of `did` names this stand-in as its PDS.
    pub fn host(&self, did: &str) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.hosted.push(did.to_owned());
    }

    /// From now on the audit log of `did` has two operations: an earlier
    /// handle on an earlier host, then `handle` on this stand-in.
    pub fn claim(&self, did: &str, handle: &str) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.claims.insert(did.to_owned(), handle.to_owned());
        g.hosted.push(did.to_owned());
    }

    /// The profile record `getRecord` answers with for `did`.
    pub fn set_profile(&self, did: &str, value: serde_json::Value) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.profiles.insert(did.to_owned(), value);
    }

    /// `getRecord` requests in total.
    pub fn record_total(&self) -> u32 {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.records
    }

    /// Document requests in total.
    pub fn document_total(&self) -> u32 {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.documents.values().sum()
    }

    /// Audit-log requests in total (one per card that reached the
    /// fetches).
    pub fn audit_total(&self) -> u32 {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.audits.values().sum()
    }
}

async fn document(State(p): State<Arc<Plc>>, Path(did): Path<String>) -> Response {
    if !did.starts_with("did:plc:") {
        return StatusCode::NOT_FOUND.into_response();
    }
    {
        let mut g = p.inner.lock().unwrap_or_else(|e| e.into_inner());
        *g.documents.entry(did.clone()).or_default() += 1;
    }
    axum::Json(json!({"id": did, "alsoKnownAs": [], "service": []})).into_response()
}

async fn audit(State(p): State<Arc<Plc>>, Path(did): Path<String>) -> Response {
    if !did.starts_with("did:plc:") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let (hosted, claim) = {
        let mut g = p.inner.lock().unwrap_or_else(|e| e.into_inner());
        *g.audits.entry(did.clone()).or_default() += 1;
        (g.hosted.contains(&did), g.claims.get(&did).cloned())
    };
    if let Some(handle) = claim {
        return axum::Json(json!([
            {
                "did": did,
                "createdAt": "2023-04-12T04:53:57.057Z",
                "nullified": false,
                "operation": {
                    "type": "plc_operation",
                    "alsoKnownAs": [format!("at://earlier-{handle}")],
                    "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": "https://earlier-host.example"}}
                }
            },
            {
                "did": did,
                "createdAt": "2024-06-01T12:00:00.000Z",
                "nullified": false,
                "operation": {
                    "type": "plc_operation",
                    "alsoKnownAs": [format!("at://{handle}")],
                    "services": {"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": p.base}}
                }
            }
        ]))
        .into_response();
    }
    let services = if hosted {
        json!({"atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": p.base}})
    } else {
        json!({})
    };
    axum::Json(json!([{
        "did": did,
        "createdAt": "2023-04-12T04:53:57.057Z",
        "nullified": false,
        "operation": {"type": "plc_operation", "alsoKnownAs": [], "services": services}
    }]))
    .into_response()
}

async fn record(State(p): State<Arc<Plc>>, Query(q): Query<HashMap<String, String>>) -> Response {
    let get = |k: &str| q.get(k).cloned().unwrap_or_default();
    let (repo, collection, rkey) = (get("repo"), get("collection"), get("rkey"));
    let found = {
        let mut g = p.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.records += 1;
        match collection.as_str() {
            "app.bsky.actor.profile" if rkey == "self" => g.profiles.get(&repo).cloned(),
            _ => None,
        }
    };
    match found {
        Some(value) => axum::Json(json!({
            "uri": format!("at://{repo}/{collection}/{rkey}"),
            "value": value
        }))
        .into_response(),
        None => (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "RecordNotFound"})),
        )
            .into_response(),
    }
}
