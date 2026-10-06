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

/// A short fingerprint of the stylesheets and scripts this build serves.
/// Every page names them as `/static/<file>?v=<this>`: the files may be
/// cached for an hour, and a new build's pages ask for them under a new
/// address, so a change shows on the next page view instead of an hour
/// later. The path alone still serves the file.
pub fn asset_version() -> &'static str {
    static VERSION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        let mut h = Sha256::new();
        for file in [
            CSS,
            HTMX,
            crate::public::PUBLIC_CSS,
            crate::public::PUBLIC_JS,
            crate::public::ADMIN_JS,
            crate::public::FAVICON,
        ] {
            h.update(file.as_bytes());
            h.update([0]);
        }
        h.finalize()
            .iter()
            .take(5)
            .map(|b| format!("{b:02x}"))
            .collect()
    });
    &VERSION
}

/// `Cache-Control` of UI pages that show admin or setup state (§9.4).
pub const NO_STORE: &str = "no-store, private";

/// Renders a template; a render failure is a 500.
pub fn render<T: Template>(t: &T) -> Response {
    match t.render() {
        Ok(html) => {
            let mut r = Html(html).into_response();
            r.headers_mut().insert(
                header::CONTENT_SECURITY_POLICY,
                HeaderValue::from_static(PAGE_CSP),
            );
            r
        }
        Err(e) => {
            tracing::error!(error = %e, "template render failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
        }
    }
}

/// `Content-Security-Policy` of every page rendered here: the admin
/// pages, the setup wizard and the sign-in. No inline script or style,
/// nothing loaded from another origin but images over `https` (profile
/// cards show avatars from the accounts' own servers), no framing, forms
/// that post only to this origin. A public page replaces it with its own
/// (`public::csp`).
pub const PAGE_CSP: &str = "default-src 'none'; style-src 'self'; script-src 'self'; \
                            img-src 'self' https:; connect-src 'self'; base-uri 'none'; \
                            form-action 'self'; frame-ancestors 'none'";
/// The same for the sign-in page, whose form is answered with a redirect
/// to the account's own authorization server: browsers apply
/// `form-action` to that redirect too, so it allows any `https` origin,
/// and plain `http` to the hosts the operator has allowed it for
/// (`net.allow_http_hosts`, which exists for development and tests).
pub fn sign_in_csp(allow_http_hosts: &[String]) -> HeaderValue {
    let mut form = String::from("'self' https:");
    for h in allow_http_hosts {
        // A host name or address and nothing else goes into the header.
        if !h.is_empty()
            && h.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']'))
        {
            form.push_str(&format!(" http://{h} http://{h}:*"));
        }
    }
    let policy = PAGE_CSP.replace("form-action 'self'", &format!("form-action {form}"));
    HeaderValue::from_str(&policy).unwrap_or_else(|_| HeaderValue::from_static(PAGE_CSP))
}

/// [`render`] with `Cache-Control: no-store, private`.
pub fn render_private<T: Template>(t: &T) -> Response {
    let mut r = render(t);
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    r
}

/// A static asset response (§9.4): the same in every configuration,
/// with no gate and no rate class.
pub fn asset(body: impl IntoResponse, content_type: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "public, max-age=3600"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}

/// Where the link-preview image is served.
pub const OG_IMAGE_PATH: &str = "/static/og-default.png";

/// `GET /static/farsight.css`: the stylesheet of the admin pages and the
/// wizard.
pub async fn css() -> Response {
    asset(CSS, "text/css; charset=utf-8")
}

/// `GET /static/public.css`: the stylesheet of the public pages.
pub async fn public_css() -> Response {
    asset(crate::public::PUBLIC_CSS, "text/css; charset=utf-8")
}

/// `GET /static/public.js`: the script of the public and the admin pages.
/// It does nothing where its elements are absent and names no admin
/// path.
pub async fn js() -> Response {
    asset(crate::public::PUBLIC_JS, "text/javascript; charset=utf-8")
}

/// `GET /static/admin.js`: the script of the admin pages, sign-in and
/// setup.
pub async fn admin_js() -> Response {
    asset(crate::public::ADMIN_JS, "text/javascript; charset=utf-8")
}

/// `GET /static/htmx.min.js`.
pub async fn htmx() -> Response {
    asset(HTMX, "text/javascript; charset=utf-8")
}

/// `GET /static/favicon.svg`: the icon of every page's browser tab.
pub async fn favicon() -> Response {
    asset(crate::public::FAVICON, "image/svg+xml")
}

/// `GET /static/og-default.png`.
pub async fn og_image() -> Response {
    asset(crate::public::OG_IMAGE, "image/png")
}

/// The answer for a route that does not exist — or does not exist for
/// this configuration, which must look the same: a disabled feature's
/// response does not say what the instance could serve if configured
/// otherwise.
pub fn not_found() -> Response {
    let mut r = (StatusCode::NOT_FOUND, "not found").into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

/// The router fallback: [`not_found`] for every unmatched path.
pub async fn fallback() -> Response {
    not_found()
}

/// A relative redirect (§8.5: never absolute, never to another scheme).
pub fn redirect(path: &str) -> Response {
    debug_assert!(path.starts_with('/') && !path.starts_with("//"));
    let mut r = Redirect::to(path).into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    r
}

/// A permanent redirect from an address a page had before v2.5 (§8.6),
/// kept for one release. `to` is built by the caller from a fixed prefix
/// and re-encoded path parameters — never from the request's raw path,
/// which could name another host (`//host/…`). The request's query is
/// carried over. A target that is not a path on this host, or not a valid
/// header value, is answered with [`not_found`].
pub fn moved(to: &str, query: Option<&str>) -> Response {
    if !to.starts_with('/') || to.starts_with("//") || to.contains('\\') {
        return not_found();
    }
    let location = match query {
        Some(q) if !q.is_empty() => format!("{to}?{q}"),
        _ => to.to_owned(),
    };
    let Ok(location) = HeaderValue::from_str(&location) else {
        return not_found();
    };
    (
        StatusCode::MOVED_PERMANENTLY,
        [
            (header::LOCATION, location),
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=3600"),
            ),
        ],
    )
        .into_response()
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

/// Whether `b` starts with `YYYY-MM-DD HH:MM`, digits in the digit
/// places.
fn minute_stamp(b: &[u8]) -> bool {
    b.len() >= 16
        && b[..16].iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            10 => *c == b' ',
            13 => *c == b':',
            _ => c.is_ascii_digit(),
        })
}

/// `text` as HTML for an admin page: escaped, with every `YYYY-MM-DD
/// HH:MM[:SS] UTC` in it as a `<time>` that the page's script rewrites
/// in the browser's timezone. Without script the UTC text stays.
pub fn local_times(text: impl AsRef<str>) -> String {
    let text = text.as_ref();
    let b = text.as_bytes();
    let esc = |s: &str| {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    };
    let (mut out, mut from, mut i) = (String::new(), 0, 0);
    while i < b.len() {
        if !b[i].is_ascii_digit() || !minute_stamp(&b[i..]) {
            i += 1;
            continue;
        }
        let rest = &b[i + 16..];
        let secs = rest.len() >= 3
            && rest[0] == b':'
            && rest[1].is_ascii_digit()
            && rest[2].is_ascii_digit();
        let end = i + if secs { 19 } else { 16 };
        if !b[end..].starts_with(b" UTC") {
            i += 1;
            continue;
        }
        // All of the match is ASCII, so these are character boundaries.
        let shown = &text[i..end + 4];
        out.push_str(&esc(&text[from..i]));
        out.push_str(&format!(
            "<time datetime=\"{}T{}{}Z\" data-plain>{shown}</time>",
            &text[i..i + 10],
            &text[i + 11..end],
            if secs { "" } else { ":00" },
        ));
        i = end + 4;
        from = i;
    }
    out.push_str(&esc(&text[from..]));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sign_in_policy_lets_its_form_reach_an_authorization_server() {
        let p = sign_in_csp(&[]);
        let p = p.to_str().unwrap();
        assert!(p.contains("form-action 'self' https:;"));
        assert!(p.contains("script-src 'self'") && !p.contains("unsafe"));
        // Plain http only where the operator allowed it, and never from
        // a value that is more than a host.
        let p = sign_in_csp(&["198.51.100.1".into(), "bad host; script-src *".into()]);
        let p = p.to_str().unwrap();
        assert!(p.contains("form-action 'self' https: http://198.51.100.1 http://198.51.100.1:*;"));
        assert!(!p.contains("bad host"));
    }

    #[test]
    fn utc_times_in_a_sentence_become_time_elements() {
        assert_eq!(
            local_times("up to 2026-10-05 23:43:08 UTC."),
            "up to <time datetime=\"2026-10-05T23:43:08Z\" data-plain>2026-10-05 23:43:08 UTC</time>."
        );
        assert_eq!(
            local_times("2026-10-05 23:43 UTC"),
            "<time datetime=\"2026-10-05T23:43:00Z\" data-plain>2026-10-05 23:43 UTC</time>"
        );
        // Nothing else is touched, and the text is escaped.
        assert_eq!(
            local_times("a <b> & 2026-10-05 23:43"),
            "a &lt;b&gt; &amp; 2026-10-05 23:43"
        );
        assert_eq!(
            local_times("é 12026-10-05 23:43 UTC"),
            "é 1<time datetime=\"2026-10-05T23:43:00Z\" data-plain>2026-10-05 23:43 UTC</time>"
        );
    }

    #[test]
    fn the_asset_version_is_ten_hex_digits_and_stable() {
        let v = asset_version();
        assert_eq!(v.len(), 10);
        assert!(v.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(v, asset_version());
    }

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
    fn moved_stays_on_this_host() {
        let r = moved("/did/did:plc:abc", Some("bc=x&nc=y"));
        assert_eq!(r.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(r.headers()[header::LOCATION], "/did/did:plc:abc?bc=x&nc=y");
        assert_eq!(r.headers()[header::CACHE_CONTROL], "public, max-age=3600");
        assert_eq!(moved("/", None).headers()[header::LOCATION], "/");
        assert_eq!(
            moved("/search", Some("")).headers()[header::LOCATION],
            "/search"
        );
        for bad in [
            "//evil.example/x",
            "https://evil.example/",
            "/\\evil.example",
            "x",
        ] {
            let r = moved(bad, None);
            assert_eq!(r.status(), StatusCode::NOT_FOUND, "{bad}");
            assert!(r.headers().get(header::LOCATION).is_none());
        }
        assert_eq!(moved("/a\nb", None).status(), StatusCode::NOT_FOUND);
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
