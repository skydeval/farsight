//! The XRPC API of Farsight (see `docs/design/api.md`): handlers for the
//! stable `app.nearhorizon.farsight.*` queries and procedures, auth and
//! access modes, rate limits, the query semaphore and timeout, client-IP
//! resolution, cache headers, the error shape, coverage (`freshness`) and API
//! metrics.

#![warn(missing_docs)]

pub mod admin;
pub mod auth;
pub mod clientip;
pub mod config_store;
pub mod cursor;
pub mod error;
pub mod freshness;
pub mod handlers;
#[cfg(any(test, feature = "harness"))]
pub mod lexicon;
pub mod metrics;
pub mod params;
pub mod public_ui;
pub mod ratelimit;
pub mod snapshot;
pub mod usage;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, RawQuery, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use farsight_core::config::ReadsMode;
use farsight_ingest::{Control, IngestStats};
use farsight_storage::counters::CounterSink;
use farsight_storage::gates::SharedGates;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::{Semaphore, mpsc};

use crate::auth::{Caller, KeyTable, SCOPE_BACKFILL, SCOPE_READ};
use crate::clientip::{CfTracker, ClientIp, ProxyTrust};
use crate::config_store::ConfigStore;
use crate::error::{XrpcError, put_rate_headers};
use crate::params::Params;
use crate::ratelimit::{Class, RateHeaders, RateLimiter};
use crate::snapshot::SnapshotHolder;

/// NSID prefix of every Farsight method.
pub const NSID_PREFIX: &str = "app.nearhorizon.farsight.";

/// Longest a read waits for a query slot before `503 Overloaded`.
pub const PERMIT_WAIT: Duration = Duration::from_secs(2);

/// Query slots anonymous callers can never hold: a quarter of
/// `rate_limit.query_concurrency`, at least one (none when there is only
/// one slot). Callers with a token always find these free of anonymous
/// load.
pub fn reserved_slots(query_concurrency: u32) -> usize {
    let n = query_concurrency.max(1) as usize;
    if n < 2 { 0 } else { (n / 4).max(1) }
}

/// Query slots open to anonymous callers.
pub fn anon_slots(query_concurrency: u32) -> usize {
    query_concurrency.max(1) as usize - reserved_slots(query_concurrency)
}

/// Requests one anonymous address (IPv6: one /48) may have in flight: a
/// quarter of the anonymous slots, at least one.
pub fn anon_in_flight(query_concurrency: u32) -> usize {
    (anon_slots(query_concurrency) / 4).max(1)
}

/// Requests one API key may have in flight: half of the slots, at least
/// one.
pub fn key_in_flight(query_concurrency: u32) -> usize {
    (query_concurrency.max(1) as usize / 2).max(1)
}

/// The link to the running ingest.
#[derive(Debug, Clone)]
pub struct IngestLink {
    /// Live statistics (source lag).
    pub stats: Arc<IngestStats>,
    /// Commands (restart).
    pub control: mpsc::Sender<Control>,
}

/// Everything a request needs.
#[derive(Debug)]
pub struct ApiState {
    /// API pool (separate from ingest's).
    pub pool: PgPool,
    /// Live configuration, read once per request so hot keys apply at
    /// once.
    pub config: Arc<ConfigStore>,
    /// The global coverage snapshot every `freshness` object is composed
    /// on. Empty until the first read succeeds; reads answer `503
    /// Overloaded` until then.
    pub snapshot: Arc<SnapshotHolder>,
    /// The live API keys by token hash, mirrored from `api_tokens`.
    pub keys: Arc<KeyTable>,
    /// The token buckets of every rate-limit class, the web UI's
    /// included.
    pub limiter: Arc<RateLimiter>,
    /// The global read-query semaphore.
    pub query_permits: Arc<Semaphore>,
    /// The part of it open to anonymous callers ([`anon_slots`]): an
    /// anonymous read takes a place here first, then a query slot.
    pub anon_permits: Arc<Semaphore>,
    /// Requests in flight per caller ([`anon_in_flight`],
    /// [`key_in_flight`]).
    pub in_flight: ratelimit::InFlight,
    /// The refreshed Cloudflare ranges that extend `proxy.trusted`.
    pub trust: Arc<ProxyTrust>,
    /// Cloudflare-share detector.
    pub cf: Arc<CfTracker>,
    /// The running ingest. `None` when this process runs none: source lag
    /// is then left out of responses and `restartFirehose` has nothing to
    /// restart.
    pub ingest: Option<IngestLink>,
    /// Approximate counters (interning by `requestBackfill`).
    pub counters: Arc<CounterSink>,
    /// Write gates (budget monitor).
    pub gates: Arc<SharedGates>,
    /// Version of the running binary, as `getStats` reports it.
    pub version: &'static str,
    /// Requests answered per endpoint since start, for the dashboard.
    pub usage: Arc<usage::Usage>,
}

/// Endpoint classes for access, limits and caching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Stable read query (public by default).
    Read,
    /// `getStats`.
    Stats,
    /// `getBackfillStatus` (`backfill` scope, never cached).
    BackfillStatus,
    /// `requestBackfill` (`backfill` scope).
    RequestBackfill,
    /// Unstable admin procedures (admin token only).
    Admin,
}

macro_rules! endpoints {
    ($($var:ident => $name:literal, $kind:ident, $method:ident;)+) => {
        /// Every method Farsight serves.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Endpoint { $(#[doc = $name] $var),+ }
        impl Endpoint {
            /// All endpoints.
            pub const ALL: &'static [Endpoint] = &[$(Endpoint::$var),+];
            /// The NSID suffix after `app.nearhorizon.farsight.`.
            pub fn name(self) -> &'static str { match self { $(Endpoint::$var => $name),+ } }
            /// The class that decides who may call it, which rate limit
            /// it draws on and how its responses are cached.
            pub fn kind(self) -> Kind { match self { $(Endpoint::$var => Kind::$kind),+ } }
            /// Expected HTTP method (queries GET, procedures POST).
            pub fn method(self) -> Method { match self { $(Endpoint::$var => Method::$method),+ } }
        }
    };
}

endpoints! {
    GetIncomingBlocks => "query.getIncomingBlocks", Read, GET;
    GetIncomingListBlocks => "query.getIncomingListBlocks", Read, GET;
    GetListsNaming => "query.getListsNaming", Read, GET;
    GetListMembers => "query.getListMembers", Read, GET;
    CheckBlocks => "query.checkBlocks", Read, GET;
    GetStats => "query.getStats", Stats, GET;
    GetBackfillStatus => "query.getBackfillStatus", BackfillStatus, GET;
    RequestBackfill => "admin.requestBackfill", RequestBackfill, POST;
    ListErrors => "admin.listErrors", Admin, GET;
    RestartFirehose => "admin.restartFirehose", Admin, POST;
    PauseSweep => "admin.pauseSweep", Admin, POST;
    StartRepair => "admin.startRepair", Admin, POST;
    PauseRepair => "admin.pauseRepair", Admin, POST;
    CancelRepair => "admin.cancelRepair", Admin, POST;
    CreateApiKey => "admin.createApiKey", Admin, POST;
    RevokeApiKey => "admin.revokeApiKey", Admin, POST;
}

impl Endpoint {
    /// The endpoint with this full NSID; `None` for an unknown method or
    /// another namespace.
    pub fn from_nsid(nsid: &str) -> Option<Endpoint> {
        let rest = nsid.strip_prefix(NSID_PREFIX)?;
        Endpoint::ALL.iter().copied().find(|e| e.name() == rest)
    }

    /// The full NSID.
    pub fn nsid(self) -> String {
        format!("{NSID_PREFIX}{}", self.name())
    }

    /// The metric label (method name without the group).
    pub fn label(self) -> &'static str {
        self.name().rsplit('.').next().unwrap_or("unknown")
    }

    /// Whether this is a read endpoint for CORS.
    pub fn is_read(self) -> bool {
        matches!(self.kind(), Kind::Read | Kind::Stats)
    }
}

/// How a successful response may be cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheClass {
    /// `public, max-age=<s>` (`private` when anonymous callers could not get
    /// the response).
    Public(u32),
    /// `no-store, private`.
    NoStore,
}

/// A successful handler result.
#[derive(Debug)]
pub struct Reply {
    /// Status (200, or 202 for `requestBackfill`).
    pub status: StatusCode,
    /// The response body, serialized as `application/json`.
    pub body: serde_json::Value,
}

impl Reply {
    /// `200 OK` with `body`.
    pub fn ok(body: serde_json::Value) -> Reply {
        Reply {
            status: StatusCode::OK,
            body,
        }
    }
}

/// The API router: every method under `/xrpc/{nsid}`.
pub fn router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/xrpc/{nsid}", any(dispatch))
        .with_state(state)
}

/// The setup-mode stand-in: every `/xrpc/*` is `503 SetupRequired`.
pub fn setup_router() -> Router {
    Router::new().route(
        "/xrpc/{*rest}",
        any(|| async { XrpcError::setup_required().into_response() }),
    )
}

/// State of the client-IP middleware.
#[derive(Debug, Clone)]
pub struct IpLayer {
    /// Live config (`None` in setup mode: no proxy is trusted yet).
    pub config: Option<Arc<ConfigStore>>,
    /// The refreshed Cloudflare ranges that extend `proxy.trusted`.
    pub trust: Arc<ProxyTrust>,
    /// Cloudflare-share detector.
    pub cf: Arc<CfTracker>,
}

/// Middleware: resolves the client IP, strips forwarding headers of
/// untrusted peers, counts Cloudflare-edge traffic and attaches
/// [`ClientIp`].
pub async fn client_ip_middleware(
    State(layer): State<IpLayer>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    mut req: Request,
    next: Next,
) -> Response {
    let (proxy, setup) = match &layer.config {
        Some(c) => (c.current().config.proxy.clone(), false),
        None => (farsight_core::config::ProxyConfig::default(), true),
    };
    let trusted = layer.trust.trusted(&proxy);
    let original: Vec<(String, String)> = clientip::FORWARDING_HEADERS
        .iter()
        .flat_map(|h| {
            req.headers()
                .get_all(*h)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .map(|v| ((*h).to_owned(), v.to_owned()))
                .collect::<Vec<_>>()
        })
        .collect();
    req.extensions_mut()
        .insert(clientip::OriginalForwarding(original));
    let client = clientip::resolve_request(&mut req, peer, &proxy, &trusted, setup);
    layer.cf.record(&client);
    req.extensions_mut().insert(client);
    next.run(req).await
}

/// Whose bucket a request draws on.
enum LimitKey {
    /// An anonymous caller's address.
    Ip(std::net::IpAddr),
    /// A token's own key.
    Named(String),
}

fn caller_limit(
    ep: Endpoint,
    caller: &Caller,
    client: &ClientIp,
) -> Option<(Class, LimitKey, Option<f32>)> {
    match (ep.kind(), caller) {
        (Kind::Read | Kind::Stats, Caller::Anonymous) => {
            Some((Class::AnonRead, LimitKey::Ip(client.ip), None))
        }
        (Kind::Read | Kind::Stats | Kind::BackfillStatus, Caller::Key(k)) => Some((
            Class::KeyRead,
            LimitKey::Named(format!("key:{}", k.id)),
            k.read_rps,
        )),
        (Kind::RequestBackfill, Caller::Key(k)) => Some((
            Class::KeyBackfill,
            LimitKey::Named(format!("key:{}", k.id)),
            None,
        )),
        (Kind::RequestBackfill, Caller::Admin) => Some((
            Class::AdminBackfill,
            LimitKey::Named("admin".to_owned()),
            None,
        )),
        _ => None,
    }
}

/// The count a caller's requests in flight are kept under, and its
/// bound. The admin token has none.
fn flight_bound(
    caller: &Caller,
    client: &ClientIp,
    query_concurrency: u32,
) -> Option<(String, usize)> {
    match caller {
        Caller::Anonymous => Some((
            format!("anon:{}", ratelimit::site_key(client.ip)),
            anon_in_flight(query_concurrency),
        )),
        Caller::Key(k) => Some((format!("key:{}", k.id), key_in_flight(query_concurrency))),
        Caller::Admin => None,
    }
}

/// Whether `caller` may call an endpoint of `kind` under `access.reads`.
pub fn authorize(kind: Kind, caller: &Caller, reads: ReadsMode) -> Result<(), XrpcError> {
    match kind {
        Kind::Read | Kind::Stats => match (reads, caller) {
            (_, Caller::Admin) => Ok(()),
            (ReadsMode::Public, Caller::Anonymous) => Ok(()),
            (ReadsMode::ApiKey, Caller::Anonymous) => {
                Err(XrpcError::auth_required("reads require an API key"))
            }
            (ReadsMode::Disabled, Caller::Anonymous) => Err(XrpcError::auth_required(
                "read queries are disabled on this instance",
            )),
            (ReadsMode::Disabled, Caller::Key(_)) => Err(XrpcError::forbidden(
                "read queries are disabled on this instance",
            )),
            (_, Caller::Key(k)) if k.has(SCOPE_READ) => Ok(()),
            (_, Caller::Key(_)) => Err(XrpcError::forbidden("API key lacks the `read` scope")),
        },
        Kind::BackfillStatus | Kind::RequestBackfill => match caller {
            Caller::Admin => Ok(()),
            Caller::Key(k) if k.has(SCOPE_BACKFILL) => Ok(()),
            Caller::Key(_) => Err(XrpcError::forbidden("API key lacks the `backfill` scope")),
            Caller::Anonymous => Err(XrpcError::auth_required(
                "a token with the `backfill` scope is required",
            )),
        },
        Kind::Admin => match caller {
            Caller::Admin => Ok(()),
            Caller::Key(_) => Err(XrpcError::forbidden("admin token required")),
            Caller::Anonymous => Err(XrpcError::auth_required("admin token required")),
        },
    }
}

impl ApiState {
    /// Begins a read transaction with `statement_timeout =
    /// rate_limit.query_timeout`.
    pub async fn read_tx(&self) -> Result<Transaction<'static, Postgres>, XrpcError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await?;
        let ms = self
            .config
            .current()
            .config
            .rate_limit
            .query_timeout
            .get()
            .as_millis()
            .max(1);
        sqlx::query(&format!("SET LOCAL statement_timeout = {ms}"))
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }

    /// Source lag from the running ingest.
    pub fn source_lag(&self) -> Option<f64> {
        let s = self.ingest.as_ref()?;
        let ms = s
            .stats
            .source_lag_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        (ms >= 0).then(|| ms as f64 / 1000.0)
    }
}

fn cors(h: &mut HeaderMap) {
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
}

async fn dispatch(
    State(st): State<Arc<ApiState>>,
    Path(nsid): Path<String>,
    method: Method,
    client: Option<axum::Extension<ClientIp>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let Some(ep) = Endpoint::from_nsid(&nsid) else {
        let mut r = XrpcError::invalid(format!("unknown method {nsid}")).into_response();
        *r.status_mut() = StatusCode::NOT_FOUND;
        return r;
    };
    let cfg = st.config.current();
    let cors_on = cfg.config.access.cors && ep.is_read();
    if method == Method::OPTIONS {
        let mut r = StatusCode::NO_CONTENT.into_response();
        if cors_on {
            let h = r.headers_mut();
            cors(h);
            h.insert(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_static("authorization, content-type"),
            );
            h.insert(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET, POST, OPTIONS"),
            );
            h.insert(
                header::ACCESS_CONTROL_MAX_AGE,
                HeaderValue::from_static("86400"),
            );
        }
        return r;
    }
    let client = client.map(|c| c.0).unwrap_or(ClientIp {
        ip: std::net::IpAddr::from([0, 0, 0, 0]),
        peer: std::net::IpAddr::from([0, 0, 0, 0]),
        trusted_peer: false,
        https: false,
    });
    let mut rate: Option<RateHeaders> = None;
    let result = run(
        &st,
        ep,
        &method,
        &client,
        &headers,
        query.as_deref(),
        &body,
        &mut rate,
    )
    .await;
    let reads_public = cfg.config.access.reads == ReadsMode::Public;
    let mut resp = match result {
        Ok(reply) => {
            let cache = match ep.kind() {
                Kind::Stats => CacheClass::Public(60),
                Kind::Read => CacheClass::Public(30),
                _ => CacheClass::NoStore,
            };
            let mut r = (reply.status, axum::Json(reply.body)).into_response();
            let h = r.headers_mut();
            let shared = match cache {
                CacheClass::Public(age) if reads_public => {
                    let v = format!("public, max-age={age}");
                    h.insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_str(&v).expect("ascii"),
                    );
                    true
                }
                CacheClass::Public(_) => {
                    h.insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_static("private, max-age=30"),
                    );
                    false
                }
                CacheClass::NoStore => {
                    h.insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_static("no-store, private"),
                    );
                    false
                }
            };
            // Per-caller headers only on responses edge caches never
            // replay.
            if !shared && let Some(r) = &rate {
                put_rate_headers(h, r);
            }
            r
        }
        Err(e) => {
            let e = XrpcError {
                rate: e.rate.or(rate),
                ..e
            };
            e.into_response()
        }
    };
    if cors_on {
        cors(resp.headers_mut());
    }
    st.usage.record(ep, resp.status().as_u16());
    let status = resp.status().as_u16().to_string();
    ::metrics::counter!(m::QUERY_REQUESTS, "endpoint" => ep.label(), "status" => status)
        .increment(1);
    ::metrics::histogram!(m::QUERY_DURATION, "endpoint" => ep.label())
        .record(started.elapsed().as_secs_f64());
    resp
}

use crate::metrics as m;

#[allow(clippy::too_many_arguments)]
async fn run(
    st: &Arc<ApiState>,
    ep: Endpoint,
    method: &Method,
    client: &ClientIp,
    headers: &HeaderMap,
    query: Option<&str>,
    body: &Bytes,
    rate: &mut Option<RateHeaders>,
) -> Result<Reply, XrpcError> {
    let cfg = st.config.current();
    // `requestBackfill` also accepts the query-parameter form; other
    // methods take only their own verb.
    if *method != ep.method() {
        return Err(XrpcError::invalid(format!(
            "{} expects {}",
            ep.nsid(),
            ep.method()
        )));
    }
    let caller = auth::authenticate(headers, &cfg.config, &st.keys)?;
    authorize(ep.kind(), &caller, cfg.config.access.reads)?;
    if let Some((class, key, rps)) = caller_limit(ep, &caller, client) {
        let limit = class.limit(&cfg.config, rps);
        let checked = match &key {
            LimitKey::Ip(ip) => st.limiter.check_ip(class, *ip, limit),
            LimitKey::Named(k) => st.limiter.check(class, k, limit),
        };
        match checked {
            Ok(h) => *rate = Some(h),
            Err((h, retry)) => {
                ::metrics::counter!(m::RATE_LIMITED, "class" => class.label()).increment(1);
                return Err(XrpcError::rate_limited(retry, h));
            }
        }
    }
    let params = Params::parse(query.unwrap_or(""));
    if ep == Endpoint::RestartFirehose {
        return admin::restart_firehose(st).await;
    }
    // One caller holds a bounded number of slots, so a burst from one
    // address or one key cannot occupy them all: its further requests
    // wait for one of its own, as long as any request waits for a slot.
    let slots = cfg.config.rate_limit.query_concurrency;
    let _flight = match flight_bound(&caller, client, slots) {
        Some((key, max)) => match st.in_flight.enter(&key, max, PERMIT_WAIT).await {
            Some(f) => Some(f),
            None => return Err(XrpcError::overloaded("too many concurrent queries")),
        },
        None => None,
    };
    let wait = |sem: &Arc<Semaphore>| {
        let sem = sem.clone();
        async move {
            match tokio::time::timeout(PERMIT_WAIT, sem.acquire_owned()).await {
                Ok(Ok(p)) => Ok(p),
                Ok(Err(_)) => Err(XrpcError::overloaded("shutting down")),
                Err(_) => Err(XrpcError::overloaded("too many concurrent queries")),
            }
        }
    };
    // Anonymous callers share a smaller pool, so that they cannot take
    // the slots kept for callers with a token.
    let _anon = match caller {
        Caller::Anonymous => Some(wait(&st.anon_permits).await?),
        _ => None,
    };
    let permit = wait(&st.query_permits).await?;
    // The handler runs in a task of its own, which holds the caller's
    // slots until it has ended. A client that goes away drops this
    // request, not the statement it started: the database goes on with
    // it until its timeout, and for that long the slots stay taken.
    // Given back at the disconnect, they would let one caller start far
    // more statements than its bound allows.
    let (st, body) = (st.clone(), body.clone());
    let held = (_flight, _anon, permit);
    let task = tokio::spawn(async move {
        let _held = held;
        let (st, body) = (&st, &body);
        match ep {
            Endpoint::GetIncomingBlocks => handlers::get_incoming_blocks(st, &params).await,
            Endpoint::GetIncomingListBlocks => {
                handlers::get_incoming_list_blocks(st, &params).await
            }
            Endpoint::GetListsNaming => handlers::get_lists_naming(st, &params).await,
            Endpoint::GetListMembers => handlers::get_list_members(st, &params).await,
            Endpoint::CheckBlocks => handlers::check_blocks(st, &params).await,
            Endpoint::GetStats => handlers::get_stats(st, &params).await,
            Endpoint::GetBackfillStatus => admin::get_backfill_status(st, &params).await,
            Endpoint::RequestBackfill => admin::request_backfill(st, &caller, &params, body).await,
            Endpoint::ListErrors => admin::list_errors(st, &params).await,
            Endpoint::RestartFirehose => admin::restart_firehose(st).await,
            Endpoint::PauseSweep => admin::pause_sweep(st, body).await,
            Endpoint::StartRepair => admin::start_repair(st).await,
            Endpoint::PauseRepair => admin::pause_repair(st, body).await,
            Endpoint::CancelRepair => admin::cancel_repair(st).await,
            Endpoint::CreateApiKey => admin::create_api_key(st, body).await,
            Endpoint::RevokeApiKey => admin::revoke_api_key(st, body).await,
        }
    });
    match task.await {
        Ok(reply) => reply,
        Err(e) => Err(XrpcError::internal(format!("the handler ended early: {e}"))),
    }
}

/// The query slots of a read made for an anonymous visitor, by a caller
/// that is not the XRPC router (a public page): one of the slots open to
/// anonymous callers, then one of the read semaphore, each waited for
/// [`PERMIT_WAIT`] at most. Taken through here, public pages share the
/// anonymous pool with anonymous API calls and never hold a slot kept
/// for callers with a token. `None` when no slot came free in time.
pub async fn anonymous_query_slots(
    st: &ApiState,
) -> Option<(
    tokio::sync::OwnedSemaphorePermit,
    tokio::sync::OwnedSemaphorePermit,
)> {
    anonymous_slots_of(&st.anon_permits, &st.query_permits, PERMIT_WAIT).await
}

async fn anonymous_slots_of(
    anon: &Arc<Semaphore>,
    query: &Arc<Semaphore>,
    wait: Duration,
) -> Option<(
    tokio::sync::OwnedSemaphorePermit,
    tokio::sync::OwnedSemaphorePermit,
)> {
    let take = |sem: &Arc<Semaphore>| {
        let sem = sem.clone();
        async move {
            tokio::time::timeout(wait, sem.acquire_owned())
                .await
                .ok()?
                .ok()
        }
    };
    let anon = take(anon).await?;
    let query = take(query).await?;
    Some((anon, query))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::KeyInfo;

    #[tokio::test]
    async fn public_pages_leave_the_reserved_slots_to_callers_with_a_token() {
        let slots = 8;
        let query = Arc::new(Semaphore::new(slots as usize));
        let anon = Arc::new(Semaphore::new(anon_slots(slots)));
        let wait = Duration::from_millis(50);
        // Pages take every slot open to anonymous callers.
        let mut held = Vec::new();
        for _ in 0..anon_slots(slots) {
            held.push(
                anonymous_slots_of(&anon, &query, wait)
                    .await
                    .expect("a slot"),
            );
        }
        // The next one waits and gives up; the reserved slots are free.
        assert!(anonymous_slots_of(&anon, &query, wait).await.is_none());
        assert_eq!(query.available_permits(), reserved_slots(slots));
        assert!(reserved_slots(slots) >= 1);
        // A page that ends gives both of its slots back.
        held.pop();
        assert!(anonymous_slots_of(&anon, &query, wait).await.is_some());
    }

    fn key(scopes: &[&str]) -> Caller {
        Caller::Key(KeyInfo {
            id: 1,
            name: "k".into(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            read_rps: None,
        })
    }

    #[test]
    fn slots_are_divided_between_anonymous_callers_and_tokens() {
        // (slots, reserved, anonymous, per address, per key)
        for (n, reserved, anon, per_addr, per_key) in [
            (32, 8, 24, 6, 16),
            (8, 2, 6, 1, 4),
            (4, 1, 3, 1, 2),
            (2, 1, 1, 1, 1),
            (1, 0, 1, 1, 1),
            (0, 0, 1, 1, 1),
        ] {
            assert_eq!(reserved_slots(n), reserved, "{n}");
            assert_eq!(anon_slots(n), anon, "{n}");
            assert_eq!(anon_in_flight(n), per_addr, "{n}");
            assert_eq!(key_in_flight(n), per_key, "{n}");
        }
        let client = |ip: &str| ClientIp {
            ip: ip.parse().unwrap(),
            peer: ip.parse().unwrap(),
            trusted_peer: false,
            https: false,
        };
        // An IPv6 caller is counted by its /48.
        assert_eq!(
            flight_bound(&Caller::Anonymous, &client("2001:db8:1:2::9"), 32),
            Some(("anon:2001:db8:1::/48".to_owned(), 6))
        );
        assert_eq!(
            flight_bound(&key(&["read"]), &client("192.0.2.1"), 32),
            Some(("key:1".to_owned(), 16))
        );
        assert_eq!(flight_bound(&Caller::Admin, &client("192.0.2.1"), 32), None);
    }

    #[test]
    fn nsid_table() {
        assert_eq!(
            Endpoint::from_nsid("app.nearhorizon.farsight.query.checkBlocks"),
            Some(Endpoint::CheckBlocks)
        );
        assert_eq!(
            Endpoint::from_nsid("app.nearhorizon.farsight.query.nope"),
            None
        );
        assert_eq!(Endpoint::RequestBackfill.method(), Method::POST);
        assert_eq!(Endpoint::GetStats.label(), "getStats");
    }

    #[test]
    fn access_matrix() {
        use ReadsMode::*;
        assert!(authorize(Kind::Read, &Caller::Anonymous, Public).is_ok());
        assert_eq!(
            authorize(Kind::Read, &Caller::Anonymous, ApiKey)
                .unwrap_err()
                .name,
            "AuthRequired"
        );
        assert!(authorize(Kind::Read, &key(&["read"]), ApiKey).is_ok());
        assert_eq!(
            authorize(Kind::Read, &key(&["backfill"]), ApiKey)
                .unwrap_err()
                .name,
            "Forbidden"
        );
        assert_eq!(
            authorize(Kind::Read, &key(&["read"]), Disabled)
                .unwrap_err()
                .name,
            "Forbidden"
        );
        assert!(authorize(Kind::Read, &Caller::Admin, Disabled).is_ok());
        // Backfill endpoints always need a token.
        assert_eq!(
            authorize(Kind::RequestBackfill, &Caller::Anonymous, Public)
                .unwrap_err()
                .name,
            "AuthRequired"
        );
        assert_eq!(
            authorize(Kind::RequestBackfill, &key(&["read"]), Public)
                .unwrap_err()
                .name,
            "Forbidden"
        );
        assert!(authorize(Kind::BackfillStatus, &key(&["backfill"]), Public).is_ok());
        assert_eq!(
            authorize(Kind::Admin, &key(&["read", "backfill"]), Public)
                .unwrap_err()
                .name,
            "Forbidden"
        );
    }
}
