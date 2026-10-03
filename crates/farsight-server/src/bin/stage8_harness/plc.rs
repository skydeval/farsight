//! A stand-in PLC directory for the stage-8 harness: it answers every
//! `did:plc` with a document that names no handle and an audit log with a
//! creation date, and counts what it was asked for. Handle warming against
//! it ends as `failed` for every account without leaving the machine, and
//! the counts show which accounts the server asked about, and how often.
//!
//! It listens on a TEST-NET-2 address (a bridge the harness creates), an
//! address the safe outbound client treats as public, so no rule of that
//! client is bypassed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::json;

#[derive(Debug, Default)]
struct Inner {
    documents: HashMap<String, u32>,
    audits: HashMap<String, u32>,
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
    {
        let mut g = p.inner.lock().unwrap_or_else(|e| e.into_inner());
        *g.audits.entry(did.clone()).or_default() += 1;
    }
    axum::Json(json!([{
        "did": did,
        "createdAt": "2023-04-12T04:53:57.057Z",
        "nullified": false,
        "operation": {"type": "plc_operation", "alsoKnownAs": [], "services": {}}
    }]))
    .into_response()
}
