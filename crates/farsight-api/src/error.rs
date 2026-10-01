//! The XRPC error shape (design §3.1): `{ "error", "message" }` with a
//! fixed set of names. Errors are never cached (`Cache-Control:
//! no-store`, §9.4); `AuthRequired` adds `WWW-Authenticate: Bearer`.

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::ratelimit::RateHeaders;

/// SQLSTATE of a statement cancelled by `statement_timeout`.
pub const QUERY_CANCELED: &str = "57014";

/// An XRPC error response.
#[derive(Debug, Clone)]
pub struct XrpcError {
    /// HTTP status.
    pub status: StatusCode,
    /// Stable error name.
    pub name: &'static str,
    /// Human-readable message.
    pub message: String,
    /// `Retry-After` seconds.
    pub retry_after: Option<u64>,
    /// Rate-limit headers to attach.
    pub rate: Option<RateHeaders>,
}

impl XrpcError {
    fn new(status: StatusCode, name: &'static str, message: impl Into<String>) -> XrpcError {
        XrpcError {
            status,
            name,
            message: message.into(),
            retry_after: None,
            rate: None,
        }
    }

    /// `400 InvalidRequest`.
    pub fn invalid(message: impl Into<String>) -> XrpcError {
        XrpcError::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
    }

    /// `401 AuthRequired` (+ `WWW-Authenticate: Bearer`).
    pub fn auth_required(message: impl Into<String>) -> XrpcError {
        XrpcError::new(StatusCode::UNAUTHORIZED, "AuthRequired", message)
    }

    /// `403 Forbidden`.
    pub fn forbidden(message: impl Into<String>) -> XrpcError {
        XrpcError::new(StatusCode::FORBIDDEN, "Forbidden", message)
    }

    /// `429 RateLimitExceeded` with `Retry-After`.
    pub fn rate_limited(retry_after: u64, rate: RateHeaders) -> XrpcError {
        XrpcError {
            retry_after: Some(retry_after.max(1)),
            rate: Some(rate),
            ..XrpcError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "RateLimitExceeded",
                "rate limit exceeded",
            )
        }
    }

    /// `429 QueueFull`.
    pub fn queue_full(message: impl Into<String>) -> XrpcError {
        XrpcError {
            retry_after: Some(60),
            ..XrpcError::new(StatusCode::TOO_MANY_REQUESTS, "QueueFull", message)
        }
    }

    /// `503 Overloaded` with `Retry-After`.
    pub fn overloaded(message: impl Into<String>) -> XrpcError {
        XrpcError {
            retry_after: Some(1),
            ..XrpcError::new(StatusCode::SERVICE_UNAVAILABLE, "Overloaded", message)
        }
    }

    /// `503 SetupRequired` (every `/xrpc/*` while in setup mode, §8.2).
    pub fn setup_required() -> XrpcError {
        XrpcError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "SetupRequired",
            "this Farsight instance has not been set up yet",
        )
    }

    /// `500 InternalError`. The detail is logged, not returned.
    pub fn internal(detail: impl std::fmt::Display) -> XrpcError {
        tracing::error!(error = %detail, "internal error");
        XrpcError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "internal error",
        )
    }
}

impl From<farsight_storage::StorageError> for XrpcError {
    fn from(e: farsight_storage::StorageError) -> XrpcError {
        match &e {
            farsight_storage::StorageError::Db(sqlx::Error::Database(d))
                if d.code().as_deref() == Some(QUERY_CANCELED) =>
            {
                XrpcError::overloaded("query timed out")
            }
            _ => XrpcError::internal(e),
        }
    }
}

impl From<sqlx::Error> for XrpcError {
    fn from(e: sqlx::Error) -> XrpcError {
        XrpcError::from(farsight_storage::StorageError::from(e))
    }
}

/// Appends `RateLimit-Policy` / `RateLimit` (IETF draft) headers.
pub fn put_rate_headers(h: &mut HeaderMap, r: &RateHeaders) {
    if let Ok(v) = HeaderValue::from_str(&r.policy()) {
        h.insert("ratelimit-policy", v);
    }
    if let Ok(v) = HeaderValue::from_str(&r.state()) {
        h.insert("ratelimit", v);
    }
}

impl IntoResponse for XrpcError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "error": self.name, "message": self.message });
        let mut resp = (self.status, axum::Json(body)).into_response();
        let h = resp.headers_mut();
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if self.name == "AuthRequired" {
            h.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        if let Some(s) = self.retry_after {
            h.insert(header::RETRY_AFTER, HeaderValue::from(s));
        }
        if let Some(r) = &self.rate {
            put_rate_headers(h, r);
        }
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_required_shape() {
        let r = XrpcError::auth_required("no token").into_response();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(r.headers()[header::WWW_AUTHENTICATE], "Bearer");
        assert_eq!(r.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[test]
    fn overloaded_retry_after() {
        let r = XrpcError::overloaded("busy").into_response();
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(r.headers()[header::RETRY_AFTER], "1");
    }
}
