//! Admin sessions: the session cookie's key, the access rule of the
//! pages under `/admin`, the form check, the fresh sign-in that sensitive
//! actions ask for, and logout.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Form, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use farsight_api::auth::hex;
use farsight_api::ratelimit::Class;
use farsight_core::config::{AdminAuth, LoadedConfig};

use super::{ADMIN_COOKIE, ADMIN_COOKIE_HOST, SESSION_ABSOLUTE, SESSION_IDLE, WebState};
use crate::common::{self, cookie, ct_eq, read_cookie};

/// A logged-in admin.
#[derive(Debug, Clone)]
pub struct Admin {
    /// The session's form token in hex: every state-changing form posts
    /// it back as `csrf`, and `check_form` compares it in constant time.
    pub csrf: String,
    /// How long ago the sign-in that made this session completed.
    pub signed_in: std::time::Duration,
}

impl Admin {
    /// Whether the sign-in is recent enough for a sensitive action
    /// ([`super::STEP_UP_WINDOW`]).
    pub fn fresh(&self) -> bool {
        self.signed_in <= super::step_up_window()
    }
}

/// The admin page a repeated sign-in returns to. The sign-in carries one
/// of these names, never a path, so it cannot be sent anywhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Return {
    /// `/admin/settings`.
    Settings,
    /// `/admin/ops`.
    Ops,
}

impl Return {
    /// The name in `/enter?again=<name>` and in the sign-in form.
    pub fn name(self) -> &'static str {
        match self {
            Return::Settings => "settings",
            Return::Ops => "ops",
        }
    }

    /// The page's path.
    pub fn path(self) -> &'static str {
        match self {
            Return::Settings => "/admin/settings",
            Return::Ops => "/admin/ops",
        }
    }

    /// What the sign-in page calls the page.
    pub fn title(self) -> &'static str {
        match self {
            Return::Settings => "Settings",
            Return::Ops => "Operations",
        }
    }

    /// The page with this name; `None` for anything else.
    pub fn parse(name: &str) -> Option<Return> {
        [Return::Settings, Return::Ops]
            .into_iter()
            .find(|r| r.name() == name)
    }
}

/// The rule in front of a sensitive action (see `docs/design/web-ui.md`):
/// it runs only in a session whose sign-in is fresh. Otherwise nothing is
/// done and the answer sends the browser to the sign-in page, which says
/// why and returns to `back` afterwards. The posted form is not carried
/// along: the admin repeats the action on the page.
pub(crate) fn step_up(admin: &Admin, back: Return) -> Result<(), Response> {
    if admin.fresh() {
        return Ok(());
    }
    Err(common::redirect(&format!("/enter?again={}", back.name())))
}

/// The stored key of an OAuth session: SHA-256 of the cookie value, a
/// zero byte and the admin DID it was created for. A session is
/// therefore found only while that DID is the configured one.
pub fn oauth_session_key(cookie: &str, did: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(cookie.as_bytes());
    h.update([0u8]);
    h.update(did.as_bytes());
    h.finalize().into()
}

/// The key a session cookie is looked up under with this config: bound
/// to the admin DID when one is configured; none otherwise.
pub fn session_key(cfg: &LoadedConfig, cookie: &str) -> Option<[u8; 32]> {
    match cfg.admin_auth() {
        AdminAuth::Configured(did) => Some(oauth_session_key(cookie, &did)),
        AdminAuth::Unconfigured | AdminAuth::Disabled => None,
    }
}

/// The session cookie's value: the `__Host-` cookie when the browser
/// sent one, otherwise the plain one.
pub(crate) fn admin_cookie_value(headers: &HeaderMap) -> Option<String> {
    read_cookie(headers, ADMIN_COOKIE_HOST).or_else(|| read_cookie(headers, ADMIN_COOKIE))
}

/// Sets the session cookie on `r`: `Path=/`, `HttpOnly`,
/// `SameSite=Strict`; over HTTPS also `Secure`, under the `__Host-` name.
/// Whether the request was HTTPS is what the listener or a trusted proxy
/// said (`ClientIp::https`): behind a proxy that is not in
/// `proxy.trusted` the cookie is set without `Secure`.
pub(crate) fn set_admin_cookie(r: &mut Response, value: &str, secure: bool) {
    let name = if secure {
        ADMIN_COOKIE_HOST
    } else {
        ADMIN_COOKIE
    };
    r.headers_mut()
        .append(header::SET_COOKIE, cookie(name, value, "/", secure, None));
}

/// Removes the session cookie under both of its names. The `__Host-` one
/// is cleared with `Secure`, without which a browser ignores the header.
pub(crate) fn clear_admin_cookie(r: &mut Response) {
    let h = r.headers_mut();
    h.append(
        header::SET_COOKIE,
        cookie(ADMIN_COOKIE, "", "/", false, Some(0)),
    );
    h.append(
        header::SET_COOKIE,
        cookie(ADMIN_COOKIE_HOST, "", "/", true, Some(0)),
    );
}

pub(crate) async fn admin(st: &WebState, headers: &HeaderMap) -> Option<Admin> {
    let raw = admin_cookie_value(headers)?;
    let id = session_key(&st.api.config.current(), &raw)?;
    let s =
        farsight_storage::auth::touch_session(&st.api.pool, &id, SESSION_IDLE, SESSION_ABSOLUTE)
            .await
            .ok()??;
    Some(Admin {
        csrf: hex(&s.csrf),
        signed_in: std::time::Duration::from_secs(s.age_secs),
    })
}

pub(crate) fn login_redirect() -> Response {
    common::redirect("/enter")
}

/// Whether htmx sent the request: the dashboard's poll and the history
/// tables' "Next" links. htmx follows a redirect and swaps what it gets,
/// so such a request must not be sent to the sign-in page.
fn is_htmx(headers: &HeaderMap) -> bool {
    headers.contains_key("hx-request")
}

/// The admin UI's access rule: every page under `/admin` needs a session.
/// With the admin UI off the pages do not exist; without a session a
/// navigation is sent to the sign-in page and a request made by htmx gets
/// the bare 404, which leaves the page it came from as it is.
pub(crate) async fn gate(st: &WebState, headers: &HeaderMap) -> Result<Admin, Response> {
    if st.api.config.current().admin_auth() == AdminAuth::Disabled {
        return Err(common::not_found());
    }
    match admin(st, headers).await {
        Some(a) => Ok(a),
        None if is_htmx(headers) => Err(common::not_found()),
        None => Err(login_redirect()),
    }
}

pub(crate) fn check_form(
    admin: &Admin,
    headers: &HeaderMap,
    form: &HashMap<String, String>,
) -> Result<(), Response> {
    if !common::same_origin(headers) {
        return Err(common::forbidden("cross-origin request refused"));
    }
    match form.get("csrf") {
        Some(c) if ct_eq(c, &admin.csrf) => Ok(()),
        _ => Err(common::forbidden("invalid form token; reload the page")),
    }
}

// ---------------------------------------------------------------------------
// Logout (sign-in is in `crate::enter`)

pub(crate) fn metrics_rate_limited(c: Class) {
    farsight_api::metrics::rate_limited(c);
}

/// `POST /admin/logout`: ends the session of a request that is
/// same-origin and carries the session's form token, like every other
/// state-changing request. Without a session there is nothing to end and
/// the answer is the redirect to the sign-in page; the cookie is cleared
/// only together with the session, so a request forged from another site
/// can neither end a session nor remove its cookie.
pub(super) async fn logout(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    form: Result<Form<HashMap<String, String>>, axum::extract::rejection::FormRejection>,
) -> Response {
    if st.api.config.current().admin_auth() == AdminAuth::Disabled {
        return common::not_found();
    }
    let Some(session) = admin(&st, &headers).await else {
        return common::redirect("/enter");
    };
    // A body that is not a form carries no token.
    let form = form.map(|f| f.0).unwrap_or_default();
    if let Err(r) = check_form(&session, &headers, &form) {
        return r;
    }
    if let Some(raw) = admin_cookie_value(&headers)
        && let Some(key) = session_key(&st.api.config.current(), &raw)
    {
        let _ = farsight_storage::auth::delete_session(&st.api.pool, &key).await;
    }
    let mut r = common::redirect("/enter");
    clear_admin_cookie(&mut r);
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::STEP_UP_WINDOW;

    fn admin(secs: u64) -> Admin {
        Admin {
            csrf: String::new(),
            signed_in: std::time::Duration::from_secs(secs),
        }
    }

    #[test]
    fn a_sensitive_action_needs_a_sign_in_inside_the_window() {
        let window = STEP_UP_WINDOW.as_secs();
        assert!(step_up(&admin(0), Return::Settings).is_ok());
        assert!(step_up(&admin(window), Return::Settings).is_ok());
        for (back, to) in [
            (Return::Settings, "/enter?again=settings"),
            (Return::Ops, "/enter?again=ops"),
        ] {
            let r = step_up(&admin(window + 1), back).unwrap_err();
            assert_eq!(r.status(), axum::http::StatusCode::SEE_OTHER);
            assert_eq!(r.headers()[header::LOCATION], to);
        }
    }

    #[test]
    fn a_sign_in_returns_only_to_a_named_page() {
        assert_eq!(Return::parse("settings"), Some(Return::Settings));
        assert_eq!(Return::parse("ops"), Some(Return::Ops));
        for bad in [
            "",
            "/admin",
            "//evil.example",
            "https://evil.example",
            "Settings",
        ] {
            assert_eq!(Return::parse(bad), None, "{bad}");
        }
        assert_eq!(Return::Settings.path(), "/admin/settings");
        assert_eq!(Return::Ops.path(), "/admin/ops");
    }
}
