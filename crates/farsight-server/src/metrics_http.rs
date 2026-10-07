//! The Prometheus endpoint (see `docs/design/operations.md`):
//! `metrics.bind`, compose-network only by default, with an optional
//! bearer token (`metrics.bearer_token_sha256`).

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use subtle::ConstantTimeEq;
use tokio::sync::watch;

/// Installs the process-wide recorder once; every mode reuses it.
pub fn install() -> Option<PrometheusHandle> {
    match PrometheusBuilder::new().install_recorder() {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!(error = %e, "metrics recorder not installed");
            None
        }
    }
}

#[derive(Clone)]
struct MetricsState {
    handle: PrometheusHandle,
    bearer_sha256: Option<[u8; 32]>,
}

fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

async fn render(State(st): State<Arc<MetricsState>>, headers: HeaderMap) -> Response {
    if let Some(want) = st.bearer_sha256 {
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(farsight_api::auth::sha256);
        let ok = presented.is_some_and(|h| bool::from(h.ct_eq(&want)));
        if !ok {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "unauthorized",
            )
                .into_response();
        }
    }
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        st.handle.render(),
    )
        .into_response()
}

/// Serves `/metrics` on `bind` until `stop` flips.
pub async fn serve(
    handle: PrometheusHandle,
    bind: String,
    bearer_hex: String,
    mut stop: watch::Receiver<bool>,
) {
    let state = Arc::new(MetricsState {
        handle,
        bearer_sha256: unhex32(&bearer_hex),
    });
    let app = Router::new()
        .route("/metrics", get(render))
        .with_state(state);
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, bind, "metrics listener not started");
            return;
        }
    };
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !*stop.borrow() {
                if stop.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
}
