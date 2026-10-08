//! `/health` and `/livez` (see `docs/design/operations.md`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use farsight_api::config_store::ConfigStore;
use serde_json::json;
use sqlx::PgPool;

fn no_store(mut r: Response) -> Response {
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// `/livez`: 200 while serving HTTP.
pub async fn livez() -> Response {
    no_store((StatusCode::OK, axum::Json(json!({ "status": "ok" }))).into_response())
}

/// `/health` in setup mode: 503 `{"status":"setup"}`.
pub async fn setup_health() -> Response {
    no_store(
        (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({ "status": "setup" })),
        )
            .into_response(),
    )
}

/// What `/health` in normal mode reads.
#[derive(Clone)]
pub struct HealthState {
    /// The pool the checks run on.
    pub pool: PgPool,
    /// The live configuration (`firehose.tuning.synthetic_gap_lag`).
    pub config: Arc<ConfigStore>,
    /// The last answer and when it was read. The endpoint is open to
    /// anyone and not rate limited: however often it is asked, the
    /// database is asked at most once per [`HEALTH_MEMO`], by one
    /// request at a time.
    pub last: Arc<tokio::sync::Mutex<Option<(Instant, StatusCode, serde_json::Value)>>>,
}

/// How long an answer of `/health` is given again without asking the
/// database.
pub const HEALTH_MEMO: Duration = Duration::from_millis(500);

/// Whether the instance is healthy: the database answers, the firehose is
/// connected, and what was applied is not further behind than
/// `max_lag`. `lag` is `None` before the first applied event, which is
/// not a lag. The connected flag alone would stay true while the writer
/// is held on one batch; coverage reports `firehose_lagging` past the
/// same bound.
pub fn healthy(db_ok: bool, connected: bool, lag: Option<f64>, max_lag: Duration) -> bool {
    db_ok && connected && lag.is_none_or(|l| l <= max_lag.as_secs_f64())
}

/// `/health` in normal mode: 200 iff `SELECT 1` answers within 1 s, the
/// firehose is connected and `applied_through` is at most
/// `firehose.tuning.synthetic_gap_lag` behind; else 503.
pub async fn health(State(st): State<HealthState>) -> Response {
    // Held across the check: requests that arrive meanwhile wait for
    // this one's answer and do not take connections of their own.
    let mut last = st.last.lock().await;
    if let Some((at, status, body)) = last.as_ref()
        && at.elapsed() < HEALTH_MEMO
    {
        return no_store((*status, axum::Json(body.clone())).into_response());
    }
    let pool = &st.pool;
    let db_ok = tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool),
    )
    .await
    .is_ok_and(|r| r.is_ok());
    let fh = if db_ok {
        tokio::time::timeout(
            Duration::from_secs(1),
            farsight_storage::firehose::read_state(pool),
        )
        .await
        .ok()
        .and_then(Result::ok)
    } else {
        None
    };
    let connected = fh.as_ref().is_some_and(|s| s.connected);
    let lag = fh
        .as_ref()
        .and_then(|s| s.applied_through)
        .map(|a| ((chrono::Utc::now() - a).num_milliseconds() as f64 / 1000.0).max(0.0));
    let max_lag = st
        .config
        .current()
        .config
        .firehose
        .tuning
        .synthetic_gap_lag
        .get();
    let ok = healthy(db_ok, connected, lag, max_lag);
    let body = json!({
        "status": if ok { "ok" } else { "unhealthy" },
        "firehose": { "connected": connected, "lagSeconds": lag },
        "db": if db_ok { "ok" } else { "error" },
    });
    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    *last = Some((Instant::now(), status, body.clone()));
    no_store((status, axum::Json(body)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_connected_stream_that_falls_behind_is_not_healthy() {
        let max = Duration::from_secs(300);
        assert!(healthy(true, true, Some(1.4), max));
        assert!(healthy(true, true, Some(300.0), max));
        // Connected, and nothing applied for longer than coverage allows.
        assert!(!healthy(true, true, Some(300.5), max));
        assert!(!healthy(true, true, Some(86_400.0), max));
        // Connected before the first event: nothing to be behind.
        assert!(healthy(true, true, None, max));
        assert!(!healthy(true, false, Some(1.0), max));
        assert!(!healthy(false, true, Some(1.0), max));
        assert!(!healthy(false, false, None, max));
    }
}
