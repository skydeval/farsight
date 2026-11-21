//! The account holders' sign-in (see `docs/design/web-ui.md`): `/account`,
//! where the holder of an account signs in with it and chooses whether
//! this instance's public UI shows the account's own page and the pages of
//! the lists it owns.
//!
//! A second flow beside the admin's `/enter`, sharing only the pure parts
//! of [`crate::oauth`]: discovery, the pushed request, the token request
//! and a [`FlowStore`] of its own. It uses none of `OAuthState`'s caches,
//! which hold the admin DID's answers: discovery runs uncached, through
//! the PLC directory pinned at start. The scope is `atproto` only; the
//! tokens are dropped and the account's server is never called with them.
//!
//! There is no session. A completed sign-in issues a *change grant*: a
//! token in the page's form and a `SameSite=Strict` cookie, held in memory
//! for ten minutes, spent by the one change it allows. The change writes
//! one `public_optouts` row for the grant's DID, or removes it, and
//! nothing else.
//!
//! The routes exist only while [`optout_live`] holds. Their responses read
//! and set cookies, so they are `no-store, private` and never indexed;
//! every other public response stays the same for every caller.
//!
//! Logged: failures, at debug, as a fixed reason word. Never logged: a
//! DID, a handle, a host or a URL of the account (for a did:web account
//! the host is the DID), a code, a state, a token or a cookie. The
//! metrics carry no DID either.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use askama::Template;
use axum::extract::rejection::FormRejection;
use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use farsight_api::clientip::ClientIp;
use farsight_api::ratelimit::Class;
use farsight_core::Did;
use farsight_core::config::{Config, LoadedConfig};
use url::Url;

use crate::common::{self, NO_STORE, random_id, read_cookie, sha256};
use crate::oauth::{self, FlowStore, Taken};
use crate::pages::WebState;
use crate::public::text::clean;
use crate::public::{Chrome, OG_INSTANCE, chrome};

/// The flow cookie: ties the callback to the browser that started the
/// flow. It authorizes nothing by itself.
pub const FLOW_COOKIE: &str = "farsight_account_flow";
/// The change grant's cookie.
pub const GRANT_COOKIE: &str = "farsight_account";
/// The path both cookies are scoped to.
pub const COOKIE_PATH: &str = "/account";
/// How long one start may take: the handle's resolution, discovery, and
/// the pushed request with its nonce retry, together.
pub const START_DEADLINE: Duration = Duration::from_secs(10);
/// How long one callback's token request may take, with its retry.
pub const TOKEN_DEADLINE: Duration = Duration::from_secs(10);
/// How long a change grant may be used.
pub const GRANT_TTL: Duration = Duration::from_secs(600);
/// Change grants held at once; a new one beyond it evicts the oldest.
pub const MAX_GRANTS: usize = 256;
/// How long the dashboard's count of opt-outs is reused.
pub const COUNT_TTL: Duration = Duration::from_secs(60);
/// Longest text the account field accepts, in bytes.
pub const MAX_INPUT: usize = 512;
/// The process-wide bucket's key.
const PROCESS_KEY: &str = "process";
/// What every refused callback and change says.
const REFUSED: &str = "Sign-in did not complete. Start again.";
/// What a start that failed says, whatever failed.
const NOT_STARTED: &str =
    "Your account's server could not be reached to sign in. Check the handle or DID and try again.";

/// Whether the account holders' sign-in exists: the public UI is on,
/// `public_ui.self_optout` is on, and `server.hostname` is a domain name
/// (the flow has no loopback mode). A function of the config alone.
pub fn optout_live(cfg: &Config) -> bool {
    cfg.access.public_ui
        && cfg.public_ui.self_optout
        && oauth::hosted_possible(&cfg.server.hostname.to_ascii_lowercase())
}

// ---------------------------------------------------------------------------
// State

/// A change grant: what a completed sign-in allows, once.
#[derive(Debug, Clone)]
pub struct Grant {
    /// The account that signed in. The change is written for it alone;
    /// the form never names the account.
    pub did: Did,
    /// SHA-256 of the grant's cookie.
    pub cookie_sha256: [u8; 32],
    /// When the grant was issued.
    pub created: Instant,
}

/// Change grants, in memory only, keyed by SHA-256 of their token.
#[derive(Debug)]
pub struct GrantStore {
    grants: Mutex<HashMap<[u8; 32], Grant>>,
    ttl: Duration,
    cap: usize,
}

impl Default for GrantStore {
    fn default() -> Self {
        GrantStore::new(GRANT_TTL, MAX_GRANTS)
    }
}

impl GrantStore {
    /// A store with the given lifetime and size.
    pub fn new(ttl: Duration, cap: usize) -> GrantStore {
        GrantStore {
            grants: Mutex::new(HashMap::new()),
            ttl,
            cap: cap.max(1),
        }
    }

    /// Holds `grant` under `token`, evicting the oldest when full.
    pub fn insert(&self, token: &str, grant: Grant) {
        let mut m = self.grants.lock().unwrap_or_else(|e| e.into_inner());
        while m.len() >= self.cap {
            let Some(oldest) = m.iter().min_by_key(|(_, g)| g.created).map(|(k, _)| *k) else {
                break;
            };
            m.remove(&oldest);
        }
        m.insert(sha256(token), grant);
    }

    /// The grant of `token` at `now`, if `cookie` is the one it was issued
    /// with: removed, so it is spent. A grant older than the lifetime is
    /// absent; one asked for with another cookie stays, so that a caller
    /// without the cookie cannot spend it.
    pub fn take(&self, token: &str, cookie: &str, now: Instant) -> Option<Grant> {
        use subtle::ConstantTimeEq;
        let key = sha256(token);
        let mut m = self.grants.lock().unwrap_or_else(|e| e.into_inner());
        let g = m.get(&key)?;
        if now.saturating_duration_since(g.created) > self.ttl {
            m.remove(&key);
            return None;
        }
        if !bool::from(g.cookie_sha256.ct_eq(&sha256(cookie))) {
            return None;
        }
        m.remove(&key)
    }

    /// Grants held now, expired ones not yet taken included.
    pub fn len(&self) -> usize {
        self.grants.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether no grant is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The account holders' sign-in's state, in memory: its own flows (so
/// that a flood of starts never evicts the admin's), the change grants,
/// and the dashboard's count.
#[derive(Debug, Default)]
pub struct PrivacyState {
    /// Flows in progress, keyed by `state`. Every flow here is `known =
    /// false`; the admin's reserved places are in the admin's store.
    pub flows: FlowStore,
    /// Change grants issued by completed sign-ins.
    pub grants: GrantStore,
    count: Mutex<Option<(Instant, i64)>>,
}

impl PrivacyState {
    /// How many accounts have opted out, for the admin dashboard: read at
    /// most once a [`COUNT_TTL`]. `None` when it cannot be read.
    pub async fn count(&self, pool: &sqlx::PgPool) -> Option<i64> {
        if let Some((at, n)) = *self.count.lock().unwrap_or_else(|e| e.into_inner())
            && at.elapsed() < COUNT_TTL
        {
            return Some(n);
        }
        let mut conn = pool.acquire().await.ok()?;
        let n = farsight_storage::optouts::count(&mut conn).await.ok()?;
        *self.count.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), n));
        Some(n)
    }
}

// ---------------------------------------------------------------------------
// Pages

/// `GET /account`: what the sign-in is for and what opting out does and
/// does not do, and the form. Without it when the request's `Host` is not
/// `server.hostname`, which is the only host the sign-in completes on.
#[derive(Template)]
#[template(path = "privacy.html")]
struct StartPage {
    c: Chrome,
    /// The form is shown: the request came to `server.hostname`.
    form: bool,
    /// `server.hostname`, where to sign in otherwise.
    hostname: String,
    /// What the visitor typed, shown again after a failure.
    typed: String,
    /// Why the last attempt did not go through.
    error: Option<&'static str>,
}

/// The page after a completed sign-in: the account, its choice, and one
/// button for the other choice.
#[derive(Template)]
#[template(path = "privacy_state.html")]
struct StatePage {
    c: Chrome,
    did: String,
    /// The handle the visitor typed, when it resolved to the account.
    handle: Option<String>,
    /// The account has asked this site to hide its pages.
    hidden: bool,
    /// The change grant's token.
    token: String,
}

/// The page after a change.
#[derive(Template)]
#[template(path = "privacy_done.html")]
struct DonePage {
    c: Chrome,
    /// The choice now stored.
    hidden: bool,
}

fn layout(cfg: &Config) -> Chrome {
    chrome(
        cfg,
        &format!("Farsight at {}", cfg.server.hostname),
        OG_INSTANCE,
        COOKIE_PATH,
    )
}

/// Finishes a `/account*` response: `no-store, private`, never indexed,
/// the sign-in policy where the page holds the sign-in form.
fn finish(mut r: Response, status: StatusCode, sign_in_csp: Option<HeaderValue>) -> Response {
    if r.status() == StatusCode::OK {
        *r.status_mut() = status;
    }
    let h = r.headers_mut();
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    h.insert(
        "x-robots-tag",
        HeaderValue::from_static("noindex, nofollow"),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    if let Some(csp) = sign_in_csp {
        h.insert(header::CONTENT_SECURITY_POLICY, csp);
    }
    r
}

fn host_header(headers: &HeaderMap) -> &str {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// The hosted client for this request: only on `server.hostname`.
fn client_here(cfg: &Config, headers: &HeaderMap) -> Option<oauth::Client> {
    oauth::client_for(
        &cfg.server.hostname,
        host_header(headers),
        false,
        oauth::PRIVACY_CALLBACK,
    )
    .filter(|c| !c.loopback)
}

/// The start page, with the sign-in policy: its form is answered with a
/// redirect to the account's own authorization server, and browsers
/// apply `form-action` to that redirect too.
fn start_page(
    cfg: &Config,
    headers: &HeaderMap,
    typed: String,
    error: Option<&'static str>,
    status: StatusCode,
) -> Response {
    let r = common::render(&StartPage {
        c: layout(cfg),
        form: client_here(cfg, headers).is_some(),
        hostname: cfg.server.hostname.to_ascii_lowercase(),
        typed,
        error,
    });
    finish(
        r,
        status,
        Some(common::sign_in_csp(&cfg.net.allow_http_hosts)),
    )
}

/// What every refused callback or change gets: the start page with one
/// fixed line, whatever was refused.
fn refused(cfg: &Config, headers: &HeaderMap, status: StatusCode) -> Response {
    let mut r = start_page(cfg, headers, String::new(), Some(REFUSED), status);
    // Not `no-referrer`: under it a browser sends `Origin: null` with
    // the page's form, which the origin check refuses.
    r.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    r
}

fn retry_after(mut r: Response, retry: u64) -> Response {
    r.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(retry.max(1)));
    r
}

/// The flow cookie: `Path=/account`, `HttpOnly`, **`SameSite=Lax`** —
/// the callback is a cross-site top-level navigation and must carry it.
/// `value = None` clears it.
fn flow_cookie(value: Option<&str>, secure: bool) -> HeaderValue {
    let mut c = match value {
        Some(v) => format!(
            "{FLOW_COOKIE}={v}; Path={COOKIE_PATH}; HttpOnly; SameSite=Lax; Max-Age={}",
            oauth::FLOW_TTL.as_secs()
        ),
        None => format!("{FLOW_COOKIE}=; Path={COOKIE_PATH}; HttpOnly; SameSite=Lax; Max-Age=0"),
    };
    if secure {
        c.push_str("; Secure");
    }
    HeaderValue::from_str(&c).expect("cookie is ASCII")
}

/// The change grant's cookie: `SameSite=Strict`, like every cookie but
/// the two flow cookies.
fn grant_cookie(value: Option<&str>, secure: bool) -> HeaderValue {
    match value {
        Some(v) => common::cookie(
            GRANT_COOKIE,
            v,
            COOKIE_PATH,
            secure,
            Some(GRANT_TTL.as_secs()),
        ),
        None => common::cookie(GRANT_COOKIE, "", COOKIE_PATH, secure, Some(0)),
    }
}

// ---------------------------------------------------------------------------
// Handlers

type St = State<Arc<WebState>>;
type Client = Option<axum::Extension<ClientIp>>;

/// The config in force, if the routes exist under it.
fn live(st: &WebState) -> Option<Arc<LoadedConfig>> {
    let cfg = st.api.config.current();
    optout_live(&cfg.config).then_some(cfg)
}

/// Takes one token of `class` for `ip`; `Err` carries `Retry-After`.
fn charge(st: &WebState, cfg: &Config, class: Class, ip: IpAddr) -> Result<(), u64> {
    st.api
        .limiter
        .check_ip(class, ip, class.limit(cfg, None))
        .map(|_| ())
        .map_err(|(_, retry)| {
            farsight_api::metrics::rate_limited(class);
            retry
        })
}

/// What the account field names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// A DID, in canonical form.
    Did(Did),
    /// A handle, in lower case, to be resolved.
    Handle(String),
}

/// Reads the account field as search does: a DID is parsed to canonical
/// form, anything else must be a whole handle (a leading `@` is dropped).
pub fn parse_input(s: &str) -> Option<Input> {
    let s = s.trim();
    if s.len() > MAX_INPUT {
        return None;
    }
    let s = s.strip_prefix('@').unwrap_or(s);
    if s.starts_with("did:") {
        return Did::parse(s).ok().map(Input::Did);
    }
    let h = s.to_ascii_lowercase();
    farsight_core::did::is_valid_hostname(&h).then_some(Input::Handle(h))
}

/// Resolves the input, discovers the account's authorization server and
/// pushes the request. The error is a fixed reason word for the log.
async fn begin(
    st: &WebState,
    input: Input,
    client: oauth::Client,
    state: &str,
    cookie_sha256: [u8; 32],
) -> Result<(oauth::Flow, Url), &'static str> {
    let (did, handle) = match input {
        Input::Did(d) => (d, None),
        Input::Handle(h) => {
            // Through the safe client; never from or into `handle_cache`,
            // and never charged to the pages' handle budget.
            let d = crate::pages::handle_to_did(&st.safe, &h)
                .await
                .map_err(|_| "resolution")?;
            (d, Some(h))
        }
    };
    // Uncached, through the directory pinned at start: the admin's
    // discovery cache is not touched.
    let server = oauth::discover(&st.safe, st.oauth.plc_url(), &did)
        .await
        .map_err(|_| "discovery")?;
    let (mut flow, to) = oauth::start(
        &st.safe,
        &server,
        client,
        did.as_str(),
        state,
        cookie_sha256,
    )
    .await
    .map_err(|_| "par")?;
    flow.handle = handle;
    flow.known = false;
    Ok((flow, to))
}

/// `GET /account`.
pub async fn page(State(st): St, headers: HeaderMap, client: Client) -> Response {
    let Some(loaded) = live(&st) else {
        return common::not_found();
    };
    let cfg = &loaded.config;
    let ip = crate::public::client_addr(client.map(|c| c.0));
    if let Err(retry) = charge(&st, cfg, Class::PublicUi, ip) {
        return retry_after(
            start_page(
                cfg,
                &headers,
                String::new(),
                Some("Too many requests from your address. Wait a few seconds and try again."),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            retry,
        );
    }
    start_page(cfg, &headers, String::new(), None, StatusCode::OK)
}

/// `POST /account`: starts a sign-in for the account the form names.
pub async fn start(
    State(st): St,
    headers: HeaderMap,
    client: Client,
    form: Result<Form<HashMap<String, String>>, FormRejection>,
) -> Response {
    let Some(loaded) = live(&st) else {
        return common::not_found();
    };
    let cfg = &loaded.config;
    // Another host cannot complete a sign-in: refused before any bucket
    // is charged or any request sent.
    let Some(oauth_client) = client_here(cfg, &headers) else {
        return start_page(cfg, &headers, String::new(), None, StatusCode::BAD_REQUEST);
    };
    if !common::same_origin(&headers) {
        return finish(
            common::forbidden("cross-origin request refused"),
            StatusCode::FORBIDDEN,
            None,
        );
    }
    let client = client.map(|c| c.0);
    let ip = crate::public::client_addr(client);
    let busy = |retry: u64| {
        retry_after(
            start_page(
                cfg,
                &headers,
                String::new(),
                Some("Sign-in is busy; try again in a few seconds."),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            retry,
        )
    };
    if let Err(retry) = charge(&st, cfg, Class::Optout, ip) {
        return busy(retry);
    }
    let typed = form
        .map(|f| f.0)
        .unwrap_or_default()
        .get("account")
        .map(|s| s.trim().to_owned())
        .unwrap_or_default();
    let Some(input) = parse_input(&typed) else {
        let shown: String = typed.chars().take(100).collect();
        return start_page(
            cfg,
            &headers,
            clean(&shown),
            Some("Enter your account's handle (such as alice.example.com) or its DID."),
            StatusCode::BAD_REQUEST,
        );
    };
    // One bucket for the whole process bounds what anonymous callers can
    // make this instance send. No address is exempt.
    if let Err((_, retry)) = st.api.limiter.check(
        Class::OptoutStart,
        PROCESS_KEY,
        Class::OptoutStart.limit(cfg, None),
    ) {
        farsight_api::metrics::rate_limited(Class::OptoutStart);
        return busy(retry);
    }
    let state = oauth::new_secret();
    let cookie_value = oauth::new_secret();
    let begun = tokio::time::timeout(
        START_DEADLINE,
        begin(&st, input, oauth_client, &state, sha256(&cookie_value)),
    )
    .await;
    let (flow, to) = match begun {
        Ok(Ok(x)) => x,
        Ok(Err(reason)) => {
            tracing::debug!(reason, "account holder's sign-in could not be started");
            return start_page(
                cfg,
                &headers,
                clean(&typed),
                Some(NOT_STARTED),
                StatusCode::BAD_GATEWAY,
            );
        }
        Err(_) => {
            tracing::debug!(
                reason = "timeout",
                "account holder's sign-in could not be started"
            );
            return start_page(
                cfg,
                &headers,
                clean(&typed),
                Some(NOT_STARTED),
                StatusCode::BAD_GATEWAY,
            );
        }
    };
    let Ok(location) = HeaderValue::from_str(to.as_str()) else {
        tracing::debug!(
            reason = "location",
            "account holder's sign-in could not be started"
        );
        return start_page(
            cfg,
            &headers,
            clean(&typed),
            Some(NOT_STARTED),
            StatusCode::BAD_GATEWAY,
        );
    };
    st.public.privacy.flows.insert(state, flow);
    let mut r = StatusCode::SEE_OTHER.into_response();
    let h = r.headers_mut();
    h.insert(header::LOCATION, location);
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    h.insert(
        "x-robots-tag",
        HeaderValue::from_static("noindex, nofollow"),
    );
    h.append(
        header::SET_COOKIE,
        flow_cookie(Some(&cookie_value), client.is_some_and(|c| c.https)),
    );
    r
}

/// `GET /account/callback`: the admin callback's checks, in its order:
/// rate limit; flow cookie and `state` present; the flow exists and is
/// young enough; the cookie is the one it was started with (otherwise the
/// flow is kept); the flow is removed; `iss`; `error`/`code`; the token
/// request; `sub`, which must name the flow's account. Every refusal looks
/// the same to the browser.
pub async fn callback(
    State(st): St,
    headers: HeaderMap,
    client: Client,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Some(loaded) = live(&st) else {
        return common::not_found();
    };
    let cfg = &loaded.config;
    let client = client.map(|c| c.0);
    let ip = crate::public::client_addr(client);
    let secure = client.is_some_and(|c| c.https);
    let refuse = |reason: &'static str, status: StatusCode| {
        tracing::debug!(reason, "account holder's sign-in refused");
        refused(cfg, &headers, status)
    };
    if let Err(retry) = charge(&st, cfg, Class::Optout, ip) {
        return retry_after(refused(cfg, &headers, StatusCode::TOO_MANY_REQUESTS), retry);
    }
    let (Some(cookie_value), Some(state)) = (read_cookie(&headers, FLOW_COOKIE), q.get("state"))
    else {
        return refuse("no_flow", StatusCode::BAD_REQUEST);
    };
    let flow = match st
        .public
        .privacy
        .flows
        .take(state, &sha256(&cookie_value), Instant::now())
    {
        Taken::Flow(f) => *f,
        Taken::Absent => return refuse("unknown_flow", StatusCode::BAD_REQUEST),
        Taken::WrongCookie => return refuse("wrong_cookie", StatusCode::BAD_REQUEST),
    };
    // From here the flow is spent; its key and verifier die with `flow`.
    if q.get("iss").map(String::as_str) != Some(flow.issuer.as_str()) {
        return refuse("issuer", StatusCode::BAD_REQUEST);
    }
    let code: &String = match (q.get("error"), q.get("code")) {
        (None, Some(code)) if !code.is_empty() => code,
        _ => return refuse("not_completed", StatusCode::BAD_REQUEST),
    };
    let Ok(did) = Did::parse(&flow.did) else {
        return refuse("flow", StatusCode::BAD_REQUEST);
    };
    let handle = flow.handle.clone();
    let sub = match tokio::time::timeout(TOKEN_DEADLINE, oauth::redeem(&st.safe, flow, code)).await
    {
        Ok(Ok(sub)) => sub,
        Ok(Err(_)) => return refuse("token", StatusCode::BAD_GATEWAY),
        Err(_) => return refuse("timeout", StatusCode::BAD_GATEWAY),
    };
    // The account that authenticated must be the one the flow was
    // started for, compared in canonical form.
    if !oauth::sub_is(&sub, &did) {
        return refuse("sub_mismatch", StatusCode::FORBIDDEN);
    }
    let hidden = async {
        let mut conn = st.api.pool.acquire().await.ok()?;
        farsight_storage::optouts::opted_out(&mut conn, &did)
            .await
            .ok()
    }
    .await;
    let Some(hidden) = hidden else {
        return refuse("database", StatusCode::SERVICE_UNAVAILABLE);
    };
    let token = random_id();
    let grant_value = random_id();
    st.public.privacy.grants.insert(
        &token,
        Grant {
            did: did.clone(),
            cookie_sha256: sha256(&grant_value),
            created: Instant::now(),
        },
    );
    // 200, not a redirect: this response ends a cross-site redirect
    // chain, and the `Strict` cookie set here would not be kept on a
    // redirect that continued it.
    let mut r = finish(
        common::render(&StatePage {
            c: layout(cfg),
            did: did.into_string(),
            handle: handle.map(|h| clean(&h)),
            hidden,
            token,
        }),
        StatusCode::OK,
        None,
    );
    let h = r.headers_mut();
    // Its address carried the code, which is spent by now. `same-origin`
    // keeps that address from other sites; `no-referrer` would make a
    // browser send `Origin: null` with the change form, which the origin
    // check refuses.
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    h.append(header::SET_COOKIE, grant_cookie(Some(&grant_value), secure));
    h.append(header::SET_COOKIE, flow_cookie(None, secure));
    r
}

/// `POST /account/set`: the one change a grant allows, for the grant's
/// account only. The form names the wanted state, so a repeated submit
/// changes nothing.
pub async fn set(
    State(st): St,
    headers: HeaderMap,
    client: Client,
    form: Result<Form<HashMap<String, String>>, FormRejection>,
) -> Response {
    let Some(loaded) = live(&st) else {
        return common::not_found();
    };
    let cfg = &loaded.config;
    let client = client.map(|c| c.0);
    let ip = crate::public::client_addr(client);
    let secure = client.is_some_and(|c| c.https);
    let refuse = |reason: &'static str, status: StatusCode| {
        tracing::debug!(reason, "account holder's change refused");
        refused(cfg, &headers, status)
    };
    if let Err(retry) = charge(&st, cfg, Class::Optout, ip) {
        return retry_after(refused(cfg, &headers, StatusCode::TOO_MANY_REQUESTS), retry);
    }
    if !common::same_origin(&headers) {
        return finish(
            common::forbidden("cross-origin request refused"),
            StatusCode::FORBIDDEN,
            None,
        );
    }
    let form = form.map(|f| f.0).unwrap_or_default();
    let hidden = match form.get("to").map(String::as_str) {
        Some("hidden") => true,
        Some("shown") => false,
        _ => return refuse("form", StatusCode::BAD_REQUEST),
    };
    let (Some(token), Some(cookie)) = (form.get("token"), read_cookie(&headers, GRANT_COOKIE))
    else {
        return refuse("no_grant", StatusCode::BAD_REQUEST);
    };
    let Some(grant) = st
        .public
        .privacy
        .grants
        .take(token, &cookie, Instant::now())
    else {
        return refuse("grant", StatusCode::BAD_REQUEST);
    };
    let written = async {
        let mut conn = st.api.pool.acquire().await.ok()?;
        farsight_storage::optouts::set(&mut conn, &grant.did, hidden)
            .await
            .ok()
    }
    .await;
    if written.is_none() {
        return refuse("database", StatusCode::SERVICE_UNAVAILABLE);
    }
    crate::public::metrics::optout_change(hidden);
    let mut r = finish(
        common::render(&DonePage {
            c: layout(cfg),
            hidden,
        }),
        StatusCode::OK,
        None,
    );
    r.headers_mut()
        .append(header::SET_COOKIE, grant_cookie(None, secure));
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(did: &str, cookie: &str, created: Instant) -> Grant {
        Grant {
            did: Did::parse(did).unwrap(),
            cookie_sha256: sha256(cookie),
            created,
        }
    }

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn a_grant_is_spent_once_and_bound_to_its_cookie_and_account() {
        let store = GrantStore::new(GRANT_TTL, 4);
        let t0 = Instant::now();
        store.insert("token-a", grant(A, "cookie-a", t0));
        store.insert("token-b", grant(B, "cookie-b", t0));
        // Another browser's cookie does not spend it, and gets nothing.
        assert!(store.take("token-a", "cookie-b", t0).is_none());
        assert!(store.take("token-a", "", t0).is_none());
        assert_eq!(store.len(), 2);
        // The right pair gives the grant's own account, once.
        let g = store.take("token-a", "cookie-a", t0).unwrap();
        assert_eq!(g.did.as_str(), A);
        assert!(store.take("token-a", "cookie-a", t0).is_none());
        // A token names one grant: B's cookie with A's token is nothing,
        // and B's grant is still B's.
        assert!(store.take("token-b", "cookie-a", t0).is_none());
        assert_eq!(
            store.take("token-b", "cookie-b", t0).unwrap().did.as_str(),
            B
        );
        // An unknown token is nothing.
        assert!(store.take("nope", "cookie-a", t0).is_none());
        assert!(store.is_empty());
    }

    #[test]
    fn a_grant_lives_ten_minutes() {
        let store = GrantStore::default();
        let t0 = Instant::now();
        store.insert("t1", grant(A, "c", t0));
        store.insert("t2", grant(A, "c", t0));
        assert!(store.take("t1", "c", t0 + GRANT_TTL).is_some());
        assert!(
            store
                .take("t2", "c", t0 + GRANT_TTL + Duration::from_secs(1))
                .is_none()
        );
        assert!(store.is_empty());
    }

    #[test]
    fn the_grant_store_is_capped_oldest_out() {
        let store = GrantStore::default();
        let t0 = Instant::now();
        for i in 0..MAX_GRANTS + 10 {
            store.insert(
                &format!("t{i}"),
                grant(A, "c", t0 + Duration::from_millis(i as u64)),
            );
        }
        assert_eq!(store.len(), MAX_GRANTS);
        let now = t0 + Duration::from_secs(1);
        for i in 0..10 {
            assert!(store.take(&format!("t{i}"), "c", now).is_none(), "t{i}");
        }
        assert!(store.take("t10", "c", now).is_some());
        assert!(
            store
                .take(&format!("t{}", MAX_GRANTS + 9), "c", now)
                .is_some()
        );
    }

    #[test]
    fn cookie_attributes() {
        assert_eq!(
            flow_cookie(Some("abc"), true),
            "farsight_account_flow=abc; Path=/account; HttpOnly; SameSite=Lax; Max-Age=600; Secure"
        );
        assert_eq!(
            flow_cookie(None, false),
            "farsight_account_flow=; Path=/account; HttpOnly; SameSite=Lax; Max-Age=0"
        );
        assert_eq!(
            grant_cookie(Some("xyz"), true),
            "farsight_account=xyz; Path=/account; HttpOnly; SameSite=Strict; Secure; Max-Age=600"
        );
        assert_eq!(
            grant_cookie(None, false),
            "farsight_account=; Path=/account; HttpOnly; SameSite=Strict; Max-Age=0"
        );
    }

    #[test]
    fn the_field_takes_a_handle_or_a_did() {
        assert_eq!(
            parse_input(" @Alice.Example.com "),
            Some(Input::Handle("alice.example.com".into()))
        );
        assert_eq!(
            parse_input("did:web:Alice.Example"),
            Some(Input::Did(Did::parse("did:web:alice.example").unwrap()))
        );
        assert_eq!(parse_input(A), Some(Input::Did(Did::parse(A).unwrap())));
        for bad in [
            "",
            "alice",
            "did:plc:short",
            "https://alice.example",
            "a b.example",
        ] {
            assert_eq!(parse_input(bad), None, "{bad}");
        }
        assert_eq!(parse_input(&"a.".repeat(400)), None);
    }

    fn live_config() -> Config {
        let mut c = Config::default();
        c.access.public_ui = true;
        c.server.hostname = "farsight.example".into();
        c
    }

    #[test]
    fn the_sign_in_exists_only_where_it_can_complete() {
        let c = live_config();
        assert!(optout_live(&c));
        let mut off = c.clone();
        off.public_ui.self_optout = false;
        assert!(!optout_live(&off));
        let mut no_ui = c.clone();
        no_ui.access.public_ui = false;
        assert!(!optout_live(&no_ui));
        for host in ["127.0.0.1", "farsight", "farsight.example:8080", "[::1]"] {
            let mut h = c.clone();
            h.server.hostname = host.into();
            assert!(!optout_live(&h), "{host}");
        }
    }

    #[test]
    fn privacy_pages_carry_the_sign_in_policy() {
        let c = live_config();
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "farsight.example".parse().unwrap());
        let r = start_page(&c, &headers, String::new(), None, StatusCode::OK);
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            r.headers()[header::CONTENT_SECURITY_POLICY],
            common::sign_in_csp(&c.net.allow_http_hosts)
        );
        assert!(
            r.headers()[header::CONTENT_SECURITY_POLICY]
                .to_str()
                .unwrap()
                .contains("form-action 'self' https:;")
        );
        assert_eq!(r.headers()[header::CACHE_CONTROL], NO_STORE);
        assert_eq!(r.headers()["x-robots-tag"], "noindex, nofollow");
        // Another Host: the page without the form.
        let mut other = HeaderMap::new();
        other.insert(header::HOST, "other.example".parse().unwrap());
        assert!(client_here(&c, &other).is_none());
        assert_eq!(
            client_here(&c, &headers).unwrap().redirect_uri,
            "https://farsight.example/account/callback"
        );
    }
}
