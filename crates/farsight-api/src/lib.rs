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
    /// Live configuration.
    pub config: Arc<ConfigStore>,
    /// Coverage snapshot.
    pub snapshot: Arc<SnapshotHolder>,
    /// API keys.
    pub keys: Arc<KeyTable>,
    /// Rate limits.
    pub limiter: Arc<RateLimiter>,
    /// The global read-query semaphore.
    pub query_permits: Arc<Semaphore>,
    /// Proxy trust.
    pub trust: Arc<ProxyTrust>,
    /// Cloudflare-share detector.
    pub cf: Arc<CfTracker>,
    /// The running ingest.
    pub ingest: Option<IngestLink>,
    /// Approximate counters (interning by `requestBackfill`).
    pub counters: Arc<CounterSink>,
    /// Write gates (budget monitor).
    pub gates: Arc<SharedGates>,
    /// Binary version.
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
            /// Access class.
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
    /// Looks up a full NSID.
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
    /// JSON body.
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
    /// Proxy trust.
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

fn caller_limit(
    st: &ApiState,
    ep: Endpoint,
    caller: &Caller,
    client: &ClientIp,
) -> Option<(Class, String, Option<f32>)> {
    match (ep.kind(), caller) {
        (Kind::Read | Kind::Stats, Caller::Anonymous) => {
            Some((Class::AnonRead, ratelimit::ip_key(client.ip), None))
        }
        (Kind::Read | Kind::Stats | Kind::BackfillStatus, Caller::Key(k)) => {
            Some((Class::KeyRead, format!("key:{}", k.id), k.read_rps))
        }
        (Kind::RequestBackfill, Caller::Key(k)) => {
            Some((Class::KeyBackfill, format!("key:{}", k.id), None))
        }
        (Kind::RequestBackfill, Caller::Admin) => {
            Some((Class::AdminBackfill, "admin".to_owned(), None))
        }
        _ => {
            let _ = st;
            None
        }
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
            if !shared {
                if let Some(r) = &rate {
                    put_rate_headers(h, r);
                }
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
    if let Some((class, key, rps)) = caller_limit(st, ep, &caller, client) {
        let limit = class.limit(&cfg.config, rps);
        match st.limiter.check(class, &key, limit) {
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
    let _permit =
        match tokio::time::timeout(PERMIT_WAIT, st.query_permits.clone().acquire_owned()).await {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => return Err(XrpcError::overloaded("shutting down")),
            Err(_) => return Err(XrpcError::overloaded("too many concurrent queries")),
        };
    match ep {
        Endpoint::GetIncomingBlocks => handlers::get_incoming_blocks(st, &params).await,
        Endpoint::GetIncomingListBlocks => handlers::get_incoming_list_blocks(st, &params).await,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::KeyInfo;

    fn key(scopes: &[&str]) -> Caller {
        Caller::Key(KeyInfo {
            id: 1,
            name: "k".into(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            read_rps: None,
        })
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
