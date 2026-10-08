//! The Prometheus endpoint (see `docs/design/operations.md`):
//! `metrics.bind`, compose-network only by default, with an optional
//! bearer token (`metrics.bearer_token_sha256`).

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use farsight_core::listen;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use subtle::ConstantTimeEq;
use tokio::sync::watch;

/// How often the recorder's upkeep runs.
const UPKEEP_EVERY: Duration = Duration::from_secs(5);

/// How long a stopping metrics listener waits for a scrape in flight.
const METRICS_DRAIN: Duration = Duration::from_secs(3);

/// Pause before a metrics listener that could not bind tries again.
const BIND_RETRY: Duration = Duration::from_secs(30);

/// Installs the process-wide recorder once; every mode reuses it. Must
/// run inside the runtime: the recorder's upkeep is a task. Histogram
/// samples are held until a scrape or the upkeep drains them, so
/// without it an instance nobody scrapes grows without bound.
pub fn install() -> Option<PrometheusHandle> {
    match PrometheusBuilder::new().install_recorder() {
        Ok(h) => {
            let upkeep = h.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(UPKEEP_EVERY);
                loop {
                    tick.tick().await;
                    upkeep.run_upkeep();
                }
            });
            Some(h)
        }
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
    // An address that cannot be bound now (still held by a process that
    // is on its way out, say) is tried again until it can.
    let listener = loop {
        match tokio::net::TcpListener::bind(&bind).await {
            Ok(l) => break l,
            Err(e) => {
                tracing::warn!(error = %e, bind, "metrics listener not started; trying again");
                tokio::select! {
                    () = tokio::time::sleep(BIND_RETRY) => {}
                    r = stop.changed() => if r.is_err() { return; },
                }
                if *stop.borrow() {
                    return;
                }
            }
        }
    };
    let (stopping_tx, stopping_rx) = tokio::sync::oneshot::channel::<()>();
    let served = axum::serve(
        listen::guarded(listener, listen::MAX_METRICS_CONNECTIONS),
        app,
    )
    .with_graceful_shutdown(async move {
        while !*stop.borrow() {
            if stop.changed().await.is_err() {
                break;
            }
        }
        let _ = stopping_tx.send(());
    })
    .into_future();
    let _ = listen::drain_within(served, stopping_rx, METRICS_DRAIN).await;
}
