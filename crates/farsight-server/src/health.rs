//! `/health` and `/livez` (see `docs/design/operations.md`).

use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
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

/// `/health` in normal mode: 200 iff the firehose is connected and
/// `SELECT 1` answers within 1 s; else 503.
pub async fn health(State(pool): State<PgPool>) -> Response {
    let db_ok = tokio::time::timeout(
        Duration::from_secs(1),
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&pool),
    )
    .await
    .is_ok_and(|r| r.is_ok());
    let fh = if db_ok {
        tokio::time::timeout(
            Duration::from_secs(1),
            farsight_storage::firehose::read_state(&pool),
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
    let ok = db_ok && connected;
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
    no_store((status, axum::Json(body)).into_response())
}
