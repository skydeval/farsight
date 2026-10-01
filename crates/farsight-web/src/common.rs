//! Shared web helpers: rendering, static assets, cookies, the proxy-safety
//! rules of design §8.5 (relative redirects, cookie flags, CSRF and
//! same-origin checks) and random ids.

use askama::Template;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// The stylesheet.
pub const CSS: &str = include_str!("../static/farsight.css");
/// Vendored htmx (see `static/NOTICE.md`).
pub const HTMX: &str = include_str!("../static/htmx.min.js");

/// `Cache-Control` of UI pages that show admin or setup state (§9.4).
pub const NO_STORE: &str = "no-store, private";

/// Renders a template; a render failure is a 500.
pub fn render<T: Template>(t: &T) -> Response {
    match t.render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "template render failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

/// [`render`] with `Cache-Control: no-store, private`.
pub fn render_private<T: Template>(t: &T) -> Response {
    let mut r = render(t);
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    r
}

/// A static asset response.
pub fn asset(body: &'static str, content_type: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
        .into_response()
}

/// `GET /static/farsight.css`.
pub async fn css() -> Response {
    asset(CSS, "text/css; charset=utf-8")
}

/// `GET /static/htmx.min.js`.
pub async fn htmx() -> Response {
    asset(HTMX, "text/javascript; charset=utf-8")
}

/// A relative redirect (§8.5: never absolute, never to another scheme).
pub fn redirect(path: &str) -> Response {
    debug_assert!(path.starts_with('/') && !path.starts_with("//"));
    let mut r = Redirect::to(path).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    r
}

/// 256 random bits, base64url.
pub fn random_id() -> String {
    URL_SAFE_NO_PAD.encode(farsight_api::auth::random_bytes::<32>())
}

/// SHA-256 of a string.
pub fn sha256(s: &str) -> [u8; 32] {
    Sha256::digest(s.as_bytes()).into()
}

/// Constant-time string equality.
pub fn ct_eq(a: &str, b: &str) -> bool {
    a.len() == b.len() && bool::from(a.as_bytes().ct_eq(b.as_bytes()))
}

/// A `Set-Cookie` value: host-only (no `Domain`), `Path`-scoped,
/// `HttpOnly`, `SameSite=Strict`, `Secure` when the request was HTTPS
/// (§8.5). `max_age = Some(0)` deletes.
pub fn cookie(
    name: &str,
    value: &str,
    path: &str,
    secure: bool,
    max_age: Option<u64>,
) -> HeaderValue {
    let mut c = format!("{name}={value}; Path={path}; HttpOnly; SameSite=Strict");
    if secure {
        c.push_str("; Secure");
    }
    if let Some(a) = max_age {
        c.push_str(&format!("; Max-Age={a}"));
    }
    HeaderValue::from_str(&c).expect("cookie is ASCII")
}

/// The value of cookie `name`.
pub fn read_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_owned())
}

/// Same-origin check for state-changing requests (§8.5): when the browser
/// sends `Sec-Fetch-Site`, it must be `same-origin`; when it sends
/// `Origin`, its host must equal `Host`. Requests carrying neither (non-
/// browser clients) pass; the CSRF token still applies.
pub fn same_origin(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()) {
        if site != "same-origin" {
            return false;
        }
    }
    if let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
        let host = headers
            .get(header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let origin_host = url::Url::parse(origin).ok().and_then(|u| {
            u.host_str().map(|h| match u.port() {
                Some(p) => format!("{h}:{p}"),
                None => h.to_owned(),
            })
        });
        if origin_host.as_deref() != Some(host) {
            return false;
        }
    }
    true
}

/// `403` for a failed CSRF or origin check.
pub fn forbidden(msg: &str) -> Response {
    let mut r = (StatusCode::FORBIDDEN, msg.to_owned()).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    r
}

/// Formats a byte count.
pub fn human_bytes(b: u64) -> String {
    const U: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1000.0 && i + 1 < U.len() {
        v /= 1000.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

/// Formats a duration in seconds as `2d 3h`, `5h 10m` or `42s`.
pub fn human_secs(s: i64) -> String {
    let s = s.max(0);
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3600, (s % 3600) / 60);
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {}s", s % 60)
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_rules() {
        let mut h = HeaderMap::new();
        h.insert(header::HOST, "farsight.example".parse().unwrap());
        assert!(same_origin(&h));
        h.insert(header::ORIGIN, "https://farsight.example".parse().unwrap());
        assert!(same_origin(&h));
        h.insert(header::ORIGIN, "https://evil.example".parse().unwrap());
        assert!(!same_origin(&h));
        let mut h = HeaderMap::new();
        h.insert("sec-fetch-site", "cross-site".parse().unwrap());
        assert!(!same_origin(&h));
    }

    #[test]
    fn cookies() {
        let c = cookie("farsight_setup", "abc", "/setup", true, None);
        assert_eq!(
            c,
            "farsight_setup=abc; Path=/setup; HttpOnly; SameSite=Strict; Secure"
        );
        let mut h = HeaderMap::new();
        h.insert(header::COOKIE, "a=1; farsight_setup=abc".parse().unwrap());
        assert_eq!(read_cookie(&h, "farsight_setup").as_deref(), Some("abc"));
    }

    #[test]
    fn humans() {
        assert_eq!(human_bytes(70_000_000_000), "70.0 GB");
        assert_eq!(human_secs(90_061), "1d 1h");
    }
}
