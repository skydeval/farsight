//! Admin sign-in (see `docs/design/web-ui.md`): `/enter`, the OAuth
//! callback and the client metadata document.
//!
//! Sign-in authenticates one account, `access.admin_did`, at that
//! account's own authorization server (see [`crate::oauth`]). What
//! `/enter` serves depends on the config's [`AdminAuth`] state: the
//! sign-in page, or a note that no admin is configured.
//!
//! Logged: a successful sign-in (INFO, with the DID and address), a
//! completed flow for another account (WARN, at most one a minute).
//! Never logged: codes, states, tokens, cookies, keys.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use askama::Template;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use farsight_api::clientip::ClientIp;
use farsight_api::ratelimit::{Class, ip_key};
use farsight_core::Did;
use farsight_core::config::{AdminAuth, LoadedConfig};

use crate::common::{self, NO_STORE, cookie, random_id, read_cookie, render_private};
use crate::oauth::{self, Taken};
use crate::pages::{
    ADMIN_COOKIE, MessagePage, Nav, SESSION_ABSOLUTE, WebState, admin, metrics_rate_limited,
    oauth_session_key,
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
    /// Why the last attempt did not complete, shown in a banner above
    /// the card; `None` on a plain view.
    pub error: Option<String>,
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

/// Whether the request comes from a local client (see
/// [`oauth::is_local_client`]): judged by the resolved client address,
/// which is the TCP peer unless a trusted proxy forwarded another. A
/// request without one is not local.
fn is_local(client: Option<&ClientIp>) -> bool {
    client.is_some_and(|c| oauth::is_local_client(c.ip))
}

/// The sign-in page for this request's `Host` and client: the button
/// where a client mode applies, otherwise where to go instead.
fn sign_in_page(
    cfg: &LoadedConfig,
    headers: &HeaderMap,
    local: bool,
    error: Option<String>,
) -> Response {
    let hostname = cfg.config.server.hostname.clone();
    let kind = if oauth::client_for(&hostname, host_header(headers), local).is_some() {
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
    })
}

/// `GET /enter`.
pub async fn page(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    client: Option<axum::Extension<ClientIp>>,
) -> Response {
    let cfg = st.api.config.current();
    let local = is_local(client.as_ref().map(|c| &c.0));
    match cfg.admin_auth() {
        AdminAuth::Disabled => common::not_found(),
        AdminAuth::Unconfigured => unconfigured_page(&cfg),
        AdminAuth::Configured(_) => {
            if admin(&st, &headers).await.is_some() {
                return common::redirect("/admin");
            }
            sign_in_page(&cfg, &headers, local, None)
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
        if m.len() >= MAX_RECENT_LOGINS
            && let Some(oldest) = m.iter().min_by_key(|(_, t)| **t).map(|(k, _)| *k)
        {
            m.remove(&oldest);
        }
    }
    m.insert(ip, Instant::now());
}

/// The flow cookie: `Path=/enter`, `HttpOnly`, **`SameSite=Lax`** — the
/// one exception to `Strict`, because the callback is a cross-site
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

/// `POST /enter`: starts a sign-in.
pub async fn submit(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    client: Option<axum::Extension<ClientIp>>,
) -> Response {
    let cfg = st.api.config.current();
    let client = client.map(|c| c.0);
    let did = match cfg.admin_auth() {
        AdminAuth::Disabled => return common::not_found(),
        AdminAuth::Unconfigured => {
            return status(unconfigured_page(&cfg), StatusCode::BAD_REQUEST);
        }
        AdminAuth::Configured(did) => did,
    };
    if !common::same_origin(&headers) {
        return common::forbidden("cross-origin request refused");
    }
    let local = is_local(client.as_ref());
    let Some(oauth_client) =
        oauth::client_for(&cfg.config.server.hostname, host_header(&headers), local)
    else {
        return status(
            sign_in_page(&cfg, &headers, local, None),
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
            local,
            Some("Too many sign-in attempts; wait a minute.".into()),
        );
        return too_many(page, Class::UiLogin, retry);
    }
    // One bucket for the whole process bounds what anonymous callers can
    // make Farsight send; an address that signed in recently is outside
    // it, so that the admin is not kept out by other people's starts.
    if !recently_signed_in(&st, ip)
        && let Err((_, retry)) = st.api.limiter.check(
            Class::UiLoginStart,
            PROCESS_KEY,
            Class::UiLoginStart.limit(&cfg.config, None),
        )
    {
        let page = sign_in_page(
            &cfg,
            &headers,
            local,
            Some("Sign-in is busy; try again in a few seconds.".into()),
        );
        return too_many(page, Class::UiLoginStart, retry);
    }
    let unreachable = |why: &str| {
        tracing::warn!(reason = why, "admin sign-in could not be started");
        status(
            sign_in_page(
                &cfg,
                &headers,
                local,
                Some("The admin account's server could not be reached. Try again shortly.".into()),
            ),
            StatusCode::BAD_GATEWAY,
        )
    };
    let Ok(parsed) = Did::parse(&did) else {
        return unreachable("access.admin_did does not parse");
    };
    let server = match st.oauth.server(&st.safe, &cfg.config, &parsed).await {
        Ok(s) => s,
        Err(e) => return unreachable(&e.to_string()),
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
        Err(e) => return unreachable(&e.to_string()),
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
/// browser back. The checks run in this order: rate limit; flow cookie and
/// `state` present; the flow exists and is young enough; the cookie is the
/// one the flow was started with (otherwise the flow is kept); the flow is
/// then removed — its `state` is spent whatever follows; `iss`;
/// `error`/`code`; the token request; `sub`. Every refusal looks the same
/// to the browser.
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
        AdminAuth::Unconfigured => None,
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
    // Only the hosted client has a metadata document, so whether the
    // client is local does not matter here.
    match oauth::client_for(hostname, host_header(&headers), false) {
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
