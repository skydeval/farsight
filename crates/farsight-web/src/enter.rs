//! Admin sign-in (design §8.6): `/enter`, the OAuth callback, the client
//! metadata document and the one-time migration from password sign-in.
//!
//! Sign-in authenticates one account, `access.admin_did`, at that
//! account's own authorization server (see [`crate::oauth`]). What
//! `/enter` serves depends on the config's [`AdminAuth`] state: the
//! sign-in page, the migration page (a pre-OAuth config with a password
//! and no admin DID), or a note that no admin is configured. The only
//! place a password is still read is the migration page.
//!
//! Logged: a successful sign-in (INFO, with the DID and address), a
//! completed flow for another account (WARN, at most one a minute).
//! Never logged: codes, states, tokens, cookies, keys.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use askama::Template;
use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use farsight_api::clientip::ClientIp;
use farsight_api::config_store::EditError;
use farsight_api::ratelimit::{Class, ip_key};
use farsight_core::Did;
use farsight_core::config::{AdminAuth, LoadedConfig, valid_admin_did};

use crate::common::{self, NO_STORE, cookie, random_id, read_cookie, render_private};
use crate::oauth::{self, Taken};
use crate::pages::{
    ADMIN_COOKIE, LOGIN_WAIT, LOGIN_WAIT_KNOWN, MessagePage, Nav, SESSION_ABSOLUTE, WebState,
    admin, metrics_rate_limited, oauth_session_key,
};

/// The flow cookie: ties an OAuth callback to the browser that started
/// the flow. It authorizes nothing by itself.
pub const FLOW_COOKIE: &str = "farsight_flow";
/// Addresses remembered as having signed in (the process-wide bucket's
/// exemption list).
pub const MAX_RECENT_LOGINS: usize = 1000;
/// The process-wide bucket's key.
const PROCESS_KEY: &str = "process";
/// What every refused callback says.
const CALLBACK_REFUSED: &str = "Sign-in did not complete. Start again.";

/// The sign-in page and its two stand-ins.
#[derive(Template)]
#[template(path = "enter.html")]
pub struct EnterPage {
    /// Navigation.
    pub nav: Nav,
    /// `signin`, `unconfigured` or `elsewhere`.
    pub kind: &'static str,
    /// `server.hostname`.
    pub hostname: String,
    /// Where hosted sign-in is, when the hostname allows it.
    pub hosted_url: Option<String>,
    /// Error.
    pub error: Option<String>,
    /// Notice.
    pub notice: Option<String>,
}

/// The migration page.
#[derive(Template)]
#[template(path = "enter_migrate.html")]
pub struct MigratePage {
    /// Navigation.
    pub nav: Nav,
    /// Error.
    pub error: Option<String>,
    /// The DID entered so far.
    pub did: String,
    /// The DID did not resolve: offer "use anyway".
    pub unresolved: bool,
    /// Where hosted sign-in will be, when the hostname allows it.
    pub hosted_url: Option<String>,
}

/// The page that ends a successful sign-in.
#[derive(Template)]
#[template(path = "enter_done.html")]
pub struct DonePage {}

fn host_header(headers: &HeaderMap) -> &str {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

fn hosted_url(cfg: &LoadedConfig) -> Option<String> {
    let h = cfg.config.server.hostname.to_ascii_lowercase();
    oauth::hosted_possible(&h).then(|| format!("https://{h}/enter"))
}

fn status(mut r: Response, s: StatusCode) -> Response {
    *r.status_mut() = s;
    r
}

/// The sign-in page for this request's `Host`: the button where a client
/// mode applies, otherwise where to go instead.
fn sign_in_page(
    cfg: &LoadedConfig,
    headers: &HeaderMap,
    error: Option<String>,
    notice: Option<String>,
) -> Response {
    let hostname = cfg.config.server.hostname.clone();
    let kind = if oauth::client_for(&hostname, host_header(headers)).is_some() {
        "signin"
    } else {
        "elsewhere"
    };
    let mut r = render_private(&EnterPage {
        nav: Nav::default(),
        kind,
        hostname,
        hosted_url: hosted_url(cfg),
        error,
        notice,
    });
    // The form on this page is answered with a redirect to the account's
    // authorization server.
    r.headers_mut().insert(
        axum::http::header::CONTENT_SECURITY_POLICY,
        common::sign_in_csp(&cfg.config.net.allow_http_hosts),
    );
    r
}

fn unconfigured_page(cfg: &LoadedConfig) -> Response {
    render_private(&EnterPage {
        nav: Nav::default(),
        kind: "unconfigured",
        hostname: cfg.config.server.hostname.clone(),
        hosted_url: hosted_url(cfg),
        error: None,
        notice: None,
    })
}

fn migrate_page(
    cfg: &LoadedConfig,
    did: &str,
    error: Option<String>,
    unresolved: bool,
) -> Response {
    render_private(&MigratePage {
        nav: Nav::default(),
        error,
        did: did.to_owned(),
        unresolved,
        hosted_url: hosted_url(cfg),
    })
}

/// `GET /enter`.
pub async fn page(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let cfg = st.api.config.current();
    match cfg.admin_auth() {
        AdminAuth::Disabled => common::not_found(),
        AdminAuth::Migration => migrate_page(&cfg, "", None, false),
        AdminAuth::Unconfigured => unconfigured_page(&cfg),
        AdminAuth::Configured(_) => {
            if admin(&st, &headers).await.is_some() {
                return common::redirect("/admin");
            }
            sign_in_page(&cfg, &headers, None, None)
        }
    }
}

fn too_many(page: Response, class: Class, retry: u64) -> Response {
    metrics_rate_limited(class);
    let mut r = status(page, StatusCode::TOO_MANY_REQUESTS);
    r.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(retry));
    r
}

fn recently_signed_in(st: &WebState, ip: IpAddr) -> bool {
    st.recent_logins
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&ip)
        .is_some_and(|t| t.elapsed() < SESSION_ABSOLUTE)
}

fn remember_sign_in(st: &WebState, ip: IpAddr) {
    let mut m = st.recent_logins.lock().unwrap_or_else(|e| e.into_inner());
    if m.len() >= MAX_RECENT_LOGINS && !m.contains_key(&ip) {
        m.retain(|_, t| t.elapsed() < SESSION_ABSOLUTE);
        if m.len() >= MAX_RECENT_LOGINS {
            if let Some(oldest) = m.iter().min_by_key(|(_, t)| **t).map(|(k, _)| *k) {
                m.remove(&oldest);
            }
        }
    }
    m.insert(ip, Instant::now());
}

/// The flow cookie: `Path=/enter`, `HttpOnly`, **`SameSite=Lax`** — the
/// one exception to §8.5's `Strict`, because the callback is a cross-site
/// top-level navigation and must carry it. `value = None` clears it.
fn flow_cookie(value: Option<&str>, secure: bool) -> HeaderValue {
    let mut c = match value {
        Some(v) => format!(
            "{FLOW_COOKIE}={v}; Path=/enter; HttpOnly; SameSite=Lax; Max-Age={}",
            oauth::FLOW_TTL.as_secs()
        ),
        None => format!("{FLOW_COOKIE}=; Path=/enter; HttpOnly; SameSite=Lax; Max-Age=0"),
    };
    if secure {
        c.push_str("; Secure");
    }
    HeaderValue::from_str(&c).expect("cookie is ASCII")
}

/// `POST /enter`: starts a sign-in (or, in the migration state, takes the
/// migration form).
pub async fn submit(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    client: Option<axum::Extension<ClientIp>>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let cfg = st.api.config.current();
    let client = client.map(|c| c.0);
    let did = match cfg.admin_auth() {
        AdminAuth::Disabled => return common::not_found(),
        AdminAuth::Migration => return migrate(&st, &cfg, &headers, client, &form).await,
        AdminAuth::Unconfigured => {
            return status(unconfigured_page(&cfg), StatusCode::BAD_REQUEST);
        }
        AdminAuth::Configured(did) => did,
    };
    if !common::same_origin(&headers) {
        return common::forbidden("cross-origin request refused");
    }
    let Some(oauth_client) = oauth::client_for(&cfg.config.server.hostname, host_header(&headers))
    else {
        return status(
            sign_in_page(&cfg, &headers, None, None),
            StatusCode::BAD_REQUEST,
        );
    };
    let ip = client.map_or(IpAddr::from([0, 0, 0, 0]), |c| c.ip);
    if let Err((_, retry)) = st.api.limiter.check(
        Class::UiLogin,
        &ip_key(ip),
        Class::UiLogin.limit(&cfg.config, None),
    ) {
        let page = sign_in_page(
            &cfg,
            &headers,
            Some("Too many sign-in attempts; wait a minute.".into()),
            None,
        );
        return too_many(page, Class::UiLogin, retry);
    }
    // One bucket for the whole process bounds what anonymous callers can
    // make Farsight send; an address that signed in recently is outside
    // it, so that the admin is not kept out by other people's starts.
    if !recently_signed_in(&st, ip) {
        if let Err((_, retry)) = st.api.limiter.check(
            Class::UiLoginStart,
            PROCESS_KEY,
            Class::UiLoginStart.limit(&cfg.config, None),
        ) {
            let page = sign_in_page(
                &cfg,
                &headers,
                Some("Sign-in is busy; try again in a few seconds.".into()),
                None,
            );
            return too_many(page, Class::UiLoginStart, retry);
        }
    }
    let unreachable = |why: &str| {
        tracing::warn!(reason = why, "admin sign-in could not be started");
        status(
            sign_in_page(
                &cfg,
                &headers,
                Some("The admin account's server could not be reached. Try again shortly.".into()),
                None,
            ),
            StatusCode::BAD_GATEWAY,
        )
    };
    let Ok(parsed) = Did::parse(&did) else {
        return unreachable("access.admin_did does not parse");
    };
    let server = match st.oauth.server(&st.safe, &cfg.config, &parsed).await {
        Ok(s) => s,
        Err(e) => return unreachable(&e),
    };
    let state = oauth::new_secret();
    let cookie_value = oauth::new_secret();
    let (flow, to) = match oauth::start(
        &st.safe,
        &server,
        oauth_client,
        &did,
        &state,
        common::sha256(&cookie_value),
    )
    .await
    {
        Ok(x) => x,
        Err(e) => return unreachable(&e),
    };
    st.oauth.flows.insert(state, flow);
    let Ok(location) = HeaderValue::from_str(to.as_str()) else {
        return unreachable("the authorization URL is not a header value");
    };
    let mut r = StatusCode::SEE_OTHER.into_response();
    let h = r.headers_mut();
    h.insert(header::LOCATION, location);
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    h.append(
        header::SET_COOKIE,
        flow_cookie(Some(&cookie_value), client.is_some_and(|c| c.https)),
    );
    r
}

fn refused(st: StatusCode) -> Response {
    let mut r = status(
        render_private(&MessagePage {
            nav: Nav::default(),
            title: "Sign in".into(),
            message: CALLBACK_REFUSED.into(),
            link: Some(("/enter".into(), "Sign in".into())),
        }),
        st,
    );
    no_referrer(&mut r);
    r
}

fn no_referrer(r: &mut Response) {
    r.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
}

/// `GET /enter/callback`: where the authorization server sends the
/// browser back. The checks run in this order (design §8.6): rate limit;
/// flow cookie and `state` present; the flow exists and is young enough;
/// the cookie is the one the flow was started with (otherwise the flow is
/// kept); the flow is then removed — its `state` is spent whatever
/// follows; `iss`; `error`/`code`; the token request; `sub`. Every refusal
/// looks the same to the browser.
pub async fn callback(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    client: Option<axum::Extension<ClientIp>>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let cfg = st.api.config.current();
    let admin_did = match cfg.admin_auth() {
        AdminAuth::Disabled => return common::not_found(),
        AdminAuth::Configured(did) => Some(did),
        AdminAuth::Migration | AdminAuth::Unconfigured => None,
    };
    let client = client.map(|c| c.0);
    let ip = client.map_or(IpAddr::from([0, 0, 0, 0]), |c| c.ip);
    if let Err((_, retry)) = st.api.limiter.check(
        Class::UiLogin,
        &ip_key(ip),
        Class::UiLogin.limit(&cfg.config, None),
    ) {
        return too_many(
            refused(StatusCode::TOO_MANY_REQUESTS),
            Class::UiLogin,
            retry,
        );
    }
    // No flow can exist without a configured admin DID.
    let Some(admin_did) = admin_did else {
        return refused(StatusCode::BAD_REQUEST);
    };
    let (Some(cookie_value), Some(state)) = (read_cookie(&headers, FLOW_COOKIE), q.get("state"))
    else {
        tracing::debug!("sign-in callback without a flow cookie or state");
        return refused(StatusCode::BAD_REQUEST);
    };
    let flow = match st
        .oauth
        .flows
        .take(state, &common::sha256(&cookie_value), Instant::now())
    {
        Taken::Flow(f) => *f,
        Taken::Absent => {
            tracing::debug!("sign-in callback for an unknown or expired flow");
            return refused(StatusCode::BAD_REQUEST);
        }
        Taken::WrongCookie => {
            tracing::debug!("sign-in callback from a browser that did not start the flow");
            return refused(StatusCode::BAD_REQUEST);
        }
    };
    // From here the flow is spent; its key and verifier die with `flow`.
    if q.get("iss").map(String::as_str) != Some(flow.issuer.as_str()) {
        tracing::debug!("sign-in callback with a missing or foreign issuer");
        return refused(StatusCode::BAD_REQUEST);
    }
    let code = match (q.get("error"), q.get("code")) {
        (None, Some(code)) if !code.is_empty() => code,
        (error, _) => {
            let error = error
                .filter(|e| {
                    e.len() <= 64 && e.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                })
                .map_or("none", String::as_str);
            tracing::debug!(
                error,
                "sign-in was not completed at the authorization server"
            );
            return refused(StatusCode::BAD_REQUEST);
        }
    };
    let expected = flow.did.clone();
    let sub = match oauth::redeem(&st.safe, flow, code).await {
        Ok(sub) => sub,
        Err(e) => {
            tracing::warn!(reason = %e, "admin sign-in: the token request failed");
            return refused(StatusCode::BAD_GATEWAY);
        }
    };
    // The account that authenticated must be the one the flow was started
    // for, and that must still be the configured admin.
    if sub != expected || expected != admin_did {
        if st.oauth.may_warn_mismatch() {
            let sub = Did::parse(&sub).map_or_else(|_| "(not a DID)".to_owned(), Did::into_string);
            tracing::warn!(
                sub,
                "admin sign-in refused: the flow completed for another account"
            );
        }
        return refused(StatusCode::FORBIDDEN);
    }
    let raw = random_id();
    let csrf = farsight_api::auth::random_bytes::<32>();
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.chars().take(300).collect::<String>());
    if let Err(e) = farsight_storage::auth::create_session(
        &st.api.pool,
        &oauth_session_key(&raw, &admin_did),
        &csrf,
        Some(ip),
        ua.as_deref(),
    )
    .await
    {
        tracing::error!(error = %e, "creating admin session failed");
        return refused(StatusCode::INTERNAL_SERVER_ERROR);
    }
    remember_sign_in(&st, ip);
    tracing::info!(did = admin_did, ip = %ip, "admin signed in");
    let secure = client.is_some_and(|c| c.https);
    // 200, not a redirect: this response ends a cross-site redirect
    // chain, and a `SameSite=Strict` cookie set here would not be sent on
    // a redirect that continues it. The page starts a same-site
    // navigation to `/admin`.
    let mut r = render_private(&DonePage {});
    no_referrer(&mut r);
    let h = r.headers_mut();
    h.append(
        header::SET_COOKIE,
        cookie(ADMIN_COOKIE, &raw, "/", secure, None),
    );
    h.append(header::SET_COOKIE, flow_cookie(None, secure));
    r
}

/// `GET /.well-known/atproto-oauth-client-metadata`: the hosted client's
/// metadata, served only on the hostname it describes; 404 when the UI is
/// disabled or the hostname cannot be a hosted client.
pub async fn client_metadata(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let cfg = st.api.config.current();
    if cfg.admin_auth() == AdminAuth::Disabled {
        return common::not_found();
    }
    let hostname = &cfg.config.server.hostname;
    match oauth::client_for(hostname, host_header(&headers)) {
        Some(c) if !c.loopback => (
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "public, max-age=3600"),
            ],
            oauth::client_metadata(hostname).to_string(),
        )
            .into_response(),
        _ => common::not_found(),
    }
}

/// The migration form (design §8.6): the existing admin password, one
/// last time, and the new admin DID. One submit, no confirmation step —
/// the password is never written into a page. On success the config is
/// rewritten (after the pre-OAuth backup), every password session ends,
/// and the sign-in page is shown. No session is created here.
async fn migrate(
    st: &WebState,
    cfg: &LoadedConfig,
    headers: &HeaderMap,
    client: Option<ClientIp>,
    form: &HashMap<String, String>,
) -> Response {
    if !common::same_origin(headers) {
        return common::forbidden("cross-origin request refused");
    }
    let did = form
        .get("admin_did")
        .map(|d| d.trim().to_owned())
        .unwrap_or_default();
    let ip = client.map_or(IpAddr::from([0, 0, 0, 0]), |c| c.ip);
    if let Err((_, retry)) = st.api.limiter.check(
        Class::UiLogin,
        &ip_key(ip),
        Class::UiLogin.limit(&cfg.config, None),
    ) {
        let page = migrate_page(
            cfg,
            &did,
            Some("Too many attempts; wait a minute.".into()),
            false,
        );
        return too_many(page, Class::UiLogin, retry);
    }
    let wait = if recently_signed_in(st, ip) {
        LOGIN_WAIT_KNOWN
    } else {
        LOGIN_WAIT
    };
    let permit = match tokio::time::timeout(wait, st.bcrypt_permits.clone().acquire_owned()).await {
        Ok(Ok(p)) => p,
        _ => {
            return status(
                migrate_page(
                    cfg,
                    &did,
                    Some("The server is busy; try again shortly.".into()),
                    false,
                ),
                StatusCode::SERVICE_UNAVAILABLE,
            );
        }
    };
    let password = form.get("password").cloned().unwrap_or_default();
    let hash = cfg.config.auth.admin_password_bcrypt.clone();
    let ok = tokio::task::spawn_blocking(move || {
        !hash.is_empty() && bcrypt::verify(password, &hash).unwrap_or(false)
    })
    .await
    .unwrap_or(false);
    drop(permit);
    if !ok {
        return status(
            migrate_page(cfg, &did, Some("Wrong password.".into()), false),
            StatusCode::UNAUTHORIZED,
        );
    }
    // Only a caller who knows the password gets as far as an outbound
    // request for a DID of their choosing.
    if !valid_admin_did(&did) {
        return status(
            migrate_page(
                cfg,
                &did,
                Some(
                    "Enter a DID: did:plc: followed by 24 characters, or did:web: followed by a \
                     hostname. A handle will not do."
                        .into(),
                ),
                false,
            ),
            StatusCode::BAD_REQUEST,
        );
    }
    let identity = match Did::parse(&did) {
        Ok(parsed) => oauth::identity(&st.safe, &cfg.config, &parsed).await,
        Err(e) => Err(e.to_string()),
    };
    let use_anyway = form.get("use_anyway").is_some_and(|v| !v.is_empty());
    let handle = match identity {
        Ok(i) => i.handle,
        Err(_) if use_anyway => None,
        Err(e) => {
            return migrate_page(
                cfg,
                &did,
                Some(format!(
                    "{did} could not be resolved ({e}). Check it, or tick \"Use this DID anyway\" \
                     and enter the password again."
                )),
                true,
            );
        }
    };
    if let Err(e) = st.api.config.migrate_admin_did(&did).await {
        let code = match e {
            EditError::AdminDid(_) | EditError::AdminUi(_) => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        return status(
            migrate_page(
                cfg,
                &did,
                Some(format!(
                    "{e}. Nothing was changed; the password still works here."
                )),
                false,
            ),
            code,
        );
    }
    // Password sessions could no longer be found anyway (their keys are
    // plain hashes, no longer looked up).
    let _ = farsight_storage::auth::delete_all_sessions(&st.api.pool).await;
    let _ = farsight_api::config_store::notify_config(&st.api.pool).await;
    st.oauth.remember_handle(&did, handle.clone());
    tracing::warn!(did, ip = %ip, "admin sign-in migrated from a password to an ATProto account");
    let who = match handle {
        Some(h) => format!("{did} (@{h})"),
        None => did.clone(),
    };
    let mut r = sign_in_page(
        &st.api.config.current(),
        headers,
        None,
        Some(format!(
            "Admin DID set to {who}. The password no longer works; sign in with that account."
        )),
    );
    // Any password-session cookie the browser holds is dead.
    r.headers_mut().append(
        header::SET_COOKIE,
        cookie(ADMIN_COOKIE, "", "/", false, Some(0)),
    );
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_cookie_attributes() {
        assert_eq!(
            flow_cookie(Some("abc"), true),
            "farsight_flow=abc; Path=/enter; HttpOnly; SameSite=Lax; Max-Age=600; Secure"
        );
        assert_eq!(
            flow_cookie(Some("abc"), false),
            "farsight_flow=abc; Path=/enter; HttpOnly; SameSite=Lax; Max-Age=600"
        );
        assert_eq!(
            flow_cookie(None, false),
            "farsight_flow=; Path=/enter; HttpOnly; SameSite=Lax; Max-Age=0"
        );
    }

    #[test]
    fn session_keys_are_bound_to_the_did() {
        let a = oauth_session_key("cookie", "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
        let b = oauth_session_key("cookie", "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb");
        assert_ne!(a, b);
        assert_ne!(a, common::sha256("cookie"));
        assert_eq!(
            a,
            oauth_session_key("cookie", "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
        );
        // The separator keeps (cookie, did) pairs apart.
        assert_ne!(oauth_session_key("ab", "c"), oauth_session_key("a", "bc"));
    }
}
