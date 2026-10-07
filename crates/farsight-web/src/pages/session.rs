//! Admin sessions: the session cookie's key, the access rule of the
//! pages under `/admin`, the form check and logout.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Form, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use farsight_api::auth::hex;
use farsight_api::ratelimit::Class;
use farsight_core::config::{AdminAuth, LoadedConfig};

use super::{ADMIN_COOKIE, SESSION_ABSOLUTE, SESSION_IDLE, WebState};
use crate::common::{self, cookie, ct_eq, read_cookie};

/// A logged-in admin.
#[derive(Debug, Clone)]
pub struct Admin {
    /// The session's form token in hex: every state-changing form posts
    /// it back as `csrf`, and `check_form` compares it in constant time.
    pub csrf: String,
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

pub(crate) async fn admin(st: &WebState, headers: &HeaderMap) -> Option<Admin> {
    let raw = read_cookie(headers, ADMIN_COOKIE)?;
    let id = session_key(&st.api.config.current(), &raw)?;
    let s =
        farsight_storage::auth::touch_session(&st.api.pool, &id, SESSION_IDLE, SESSION_ABSOLUTE)
            .await
            .ok()??;
    Some(Admin { csrf: hex(&s.csrf) })
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
    if let Some(raw) = read_cookie(&headers, ADMIN_COOKIE)
        && let Some(key) = session_key(&st.api.config.current(), &raw)
    {
        let _ = farsight_storage::auth::delete_session(&st.api.pool, &key).await;
    }
    let mut r = common::redirect("/enter");
    r.headers_mut().append(
        header::SET_COOKIE,
        cookie(ADMIN_COOKIE, "", "/", false, Some(0)),
    );
    r
}
