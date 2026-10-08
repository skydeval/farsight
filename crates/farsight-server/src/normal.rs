//! Normal mode (see `docs/design/README.md` and `docs/design/web-ui.md`):
//! migrations, the API pool (separate from ingest's 4 connections), the
//! ingest task, the coverage snapshot and its NOTIFY listener, the API and
//! UI routers, `/health` and `/livez`, the metrics listener, the periodic
//! tasks, the UI sort-index builder and the handle-warming worker. Ends on
//! shutdown or on a config reset (then the caller enters setup mode
//! in-process).
//!
//! Every background task here keeps its state outside itself and runs
//! under `farsight_core::task::supervise`: a panic is logged, counted and
//! the task started again. Ingest is the exception: its reader and writer
//! are not started again in place, so a panic in either ends normal mode
//! with an error and the process exits (see `farsight_ingest`).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use farsight_api::clientip::{CfTracker, ProxyTrust};
use farsight_api::config_store::ConfigStore;
use farsight_api::snapshot::SnapshotHolder;
use farsight_api::{ApiState, IngestLink, IpLayer, client_ip_middleware};
use farsight_core::config::LoadedConfig;
use farsight_core::listen;
use farsight_core::net::{SafeClient, SafeClientConfig};
use farsight_ingest::{Ingest, IngestConfig};
use farsight_storage::counters::{CounterSink, FLUSH_INTERVAL};
use farsight_storage::gates::SharedGates;
use farsight_storage::keys::Limits;
use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::PgPool;
use tokio::sync::{Semaphore, watch};

use farsight_core::task::supervise;

use crate::error::{CloudflareError, ServerError};
use crate::tasks::{self, TaskCtx};
use crate::{ModeEnd, VERSION, health, metrics_http, sort_indexes};

/// Extra API-pool connections beyond the query semaphore (writes,
/// lookups, health).
pub const API_POOL_EXTRA: u32 = 8;
/// Connections of the pool shared by the periodic tasks, the counter
/// flusher and the sort-index builder's short checks: apart from the API
/// pool and from ingest's, so neither can starve them.
pub const TASKS_POOL: u32 = 4;
/// How long the teardown waits for the tasks it stopped.
const TASKS_STOP: Duration = Duration::from_secs(5);
/// How long the teardown waits for ingest to drain its writer.
const INGEST_STOP: Duration = Duration::from_secs(20);
/// How long the teardown waits for the last counter flushes.
const LAST_FLUSH: Duration = Duration::from_secs(5);

/// Counter sink shard of the server's own writes (ingest uses 0).
pub const TASKS_SHARD: i16 = 1;

/// The supervised tasks of normal mode, by the name their panics are
/// counted under (the periodic jobs are counted under their own names).
pub const TASKS: [&str; 10] = [
    "counter_flush",
    "coverage_snapshot",
    "housekeeping",
    "periodic_scheduler",
    "metrics_listener",
    "handle_warming",
    "handle_pass",
    "top_lists",
    "sort_index_builder",
    farsight_web::public::warming::CHECK_TASK,
];

async fn connect_retry(
    url: &str,
    max: u32,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<PgPool> {
    let mut wait = Duration::from_millis(500);
    loop {
        match farsight_storage::connect(url, max).await {
            Ok(p) => return Some(p),
            Err(e) => {
                tracing::warn!(error = %e, "database not reachable yet; retrying");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() { return None; }
            }
        }
        wait = (wait * 2).min(Duration::from_secs(10));
    }
}

async fn stopped(mut rx: watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Runs normal mode until `shutdown` flips ([`ModeEnd::Shutdown`]) or the
/// configuration is reset from the admin UI ([`ModeEnd::Switch`], and the
/// caller enters setup mode). `env` is the environment captured at
/// start-up, which config edits are validated against. An error is a
/// failed start-up step, a listener failure or a panicked ingest task.
pub async fn run(
    loaded: LoadedConfig,
    config_path: PathBuf,
    env: Vec<(String, String)>,
    mut shutdown: watch::Receiver<bool>,
    metrics_handle: Option<PrometheusHandle>,
) -> Result<ModeEnd, ServerError> {
    for w in &loaded.warnings {
        tracing::warn!(warning = %w, "configuration");
    }
    let cfg = loaded.config.clone();
    let api_pool_size = cfg.rate_limit.query_concurrency + API_POOL_EXTRA;
    let Some(api_pool) =
        connect_retry(&cfg.storage.database_url, api_pool_size, &mut shutdown).await
    else {
        return Ok(ModeEnd::Shutdown);
    };
    farsight_storage::migrate(&api_pool)
        .await
        .map_err(ServerError::step("migrations"))?;
    tracing::info!(
        schema = farsight_storage::SCHEMA_VERSION,
        "migrations applied"
    );
    // The recording window opens or closes here and only here: the
    // ingest writer takes `block_history_enabled` at start.
    let recording =
        farsight_storage::history::sync_window(&api_pool, cfg.storage.block_history_enabled)
            .await
            .map_err(ServerError::step("history window"))?;
    tracing::info!(recording, "block and list-membership history");
    let ingest_pool =
        farsight_storage::connect(&cfg.storage.database_url, farsight_ingest::POOL_SIZE).await?;
    let tasks_pool = farsight_storage::connect(&cfg.storage.database_url, TASKS_POOL).await?;

    let config = Arc::new(ConfigStore::new(config_path.clone(), env, loaded));
    let gates = Arc::new(SharedGates::default());
    let (stop_tx, stop_rx) = watch::channel(false);

    // Ingest (its own 4-connection pool).
    let mut icfg = IngestConfig::from_config(&cfg);
    icfg.gates = gates.clone();
    let ingest = Ingest::start(icfg, ingest_pool)
        .await
        .map_err(ServerError::Ingest)?;

    // The server's own counter sink (janitors, requestBackfill interning).
    let counters = Arc::new(CounterSink::new(TASKS_SHARD));
    let flusher = {
        let counters = counters.clone();
        let pool = tasks_pool.clone();
        let config = config.clone();
        tokio::spawn(supervise("counter_flush", move || {
            let (counters, pool, config) = (counters.clone(), pool.clone(), config.clone());
            async move {
                let mut tick = tokio::time::interval(FLUSH_INTERVAL);
                loop {
                    tick.tick().await;
                    let limits = Limits::from_config(&config.current().config);
                    if let Err(e) = counters.flush(&pool, &limits).await {
                        tracing::warn!(error = %e, "counter flush failed");
                    }
                }
            }
        }))
    };

    // Coverage snapshot: one read before serving, then the refresher.
    let snapshot = Arc::new(SnapshotHolder::default());
    loop {
        match snapshot
            .refresh(&api_pool, &Limits::from_config(&cfg))
            .await
        {
            Ok(()) => break,
            Err(e) => {
                tracing::warn!(error = %e, "first coverage snapshot failed; retrying");
                // A stop asked for while the snapshot cannot be read is
                // obeyed: ingest is shut down and the process exits.
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    () = stopped(shutdown.clone()) => {
                        ingest.shutdown(INGEST_STOP).await;
                        return Ok(ModeEnd::Shutdown);
                    }
                }
            }
        }
    }
    let refresher = tokio::spawn(supervise("coverage_snapshot", {
        let (snapshot, pool, config, stop) = (
            snapshot.clone(),
            api_pool.clone(),
            config.clone(),
            stop_rx.clone(),
        );
        move || {
            farsight_api::snapshot::run_refresher(
                snapshot.clone(),
                pool.clone(),
                config.clone(),
                stop.clone(),
            )
        }
    }));

    let keys = Arc::new(farsight_api::auth::KeyTable::default());
    keys.refresh(&api_pool)
        .await
        .map_err(ServerError::step("loading API keys"))?;
    let limiter = Arc::new(farsight_api::ratelimit::RateLimiter::default());
    let trust = Arc::new(ProxyTrust::default());
    let cf = Arc::new(CfTracker::default());
    farsight_api::metrics::register();
    farsight_storage::history::register_metrics();
    farsight_web::public::metrics::register();
    farsight_web::public::warming::register();
    farsight_web::public::pass::register();
    farsight_core::task::register(&TASKS);
    farsight_core::task::register(&tasks::job_names());
    // Which UI sections sort by shown time: read once before serving, then
    // kept by the index builder.
    let sort = Arc::new(farsight_storage::ui_rows::SortIndexes::default());
    sort_indexes::load(&api_pool, &sort)
        .await
        .map_err(ServerError::step("reading the sort indexes"))?;

    let api = Arc::new(ApiState {
        pool: api_pool.clone(),
        config: config.clone(),
        snapshot: snapshot.clone(),
        keys: keys.clone(),
        limiter: limiter.clone(),
        query_permits: Arc::new(Semaphore::new(
            cfg.rate_limit.query_concurrency.max(1) as usize
        )),
        anon_permits: Arc::new(Semaphore::new(farsight_api::anon_slots(
            cfg.rate_limit.query_concurrency,
        ))),
        in_flight: Default::default(),
        trust: trust.clone(),
        cf: cf.clone(),
        ingest: Some(IngestLink {
            stats: ingest.stats.clone(),
            control: ingest.control.clone(),
        }),
        counters: counters.clone(),
        gates: gates.clone(),
        version: VERSION,
        usage: Arc::default(),
    });
    let status = Arc::new(farsight_web::ServerStatus::default());
    let (reset_tx, mut reset_rx) = watch::channel(false);
    let web = Arc::new(farsight_web::WebState {
        api: api.clone(),
        safe: SafeClient::new(SafeClientConfig::from_config(&cfg, VERSION)),
        recent_logins: Mutex::new(Default::default()),
        // The directory the admin DID is resolved through is the one
        // named at start, whatever a later settings save says.
        oauth: farsight_web::oauth::OAuthState::new(&cfg.backfill.plc_url),
        reset: reset_tx,
        token_path: farsight_web::setup_token::token_path(&config_path),
        status: status.clone(),
        public: Default::default(),
        sort: sort.clone(),
    });

    // Housekeeping of in-memory API state.
    let housekeeping = {
        let keys = keys.clone();
        let limiter = limiter.clone();
        let pool = api_pool.clone();
        let trust = trust.clone();
        let config = config.clone();
        let status = status.clone();
        let web = web.clone();
        tokio::spawn(supervise("housekeeping", move || {
            let (keys, limiter, pool, trust, config, status, web) = (
                keys.clone(),
                limiter.clone(),
                pool.clone(),
                trust.clone(),
                config.clone(),
                status.clone(),
                web.clone(),
            );
            async move {
                let mut tick = tokio::time::interval(Duration::from_secs(30));
                let mut n: u64 = 0;
                loop {
                    tick.tick().await;
                    n += 1;
                    // Sign-in flows older than their lifetime (they are already
                    // refused at lookup; this frees the memory).
                    if n.is_multiple_of(2) {
                        web.oauth.flows.sweep(std::time::Instant::now());
                    }
                    if let Err(e) = keys.refresh(&pool).await {
                        tracing::warn!(error = %e, "API key refresh failed");
                    }
                    if n.is_multiple_of(2) {
                        let _ = keys.flush_usage(&pool).await;
                    }
                    if n.is_multiple_of(20) {
                        limiter.sweep(Duration::from_secs(600));
                    }
                    // Opt-in daily refresh of the Cloudflare ranges.
                    let proxy = config.current().config.proxy.clone();
                    if proxy.cloudflare_refresh
                        && proxy.mode == farsight_core::config::ProxyMode::Cloudflare
                        && (n == 1 || n.is_multiple_of(2880))
                    {
                        match refresh_cloudflare().await {
                            Ok(nets) => {
                                tracing::info!(ranges = nets.len(), "Cloudflare ranges refreshed");
                                trust.set_refreshed(nets);
                                status.update(|s| s.cf_refreshed_at = Some(chrono::Utc::now()));
                            }
                            Err(e) => tracing::warn!(error = %e, "Cloudflare range refresh failed"),
                        }
                    }
                }
            }
        }))
    };

    let task_ctx = Arc::new(TaskCtx::new(
        tasks_pool.clone(),
        config.clone(),
        counters.clone(),
        gates.clone(),
        status.clone(),
    ));
    let scheduler = tokio::spawn(supervise("periodic_scheduler", {
        let stop = stop_rx.clone();
        move || tasks::run_scheduler(task_ctx.clone(), stop.clone())
    }));
    let metrics_task = metrics_handle.map(|h| {
        let (bind, token, stop) = (
            cfg.metrics.bind.clone(),
            cfg.metrics.bearer_token_sha256.clone(),
            stop_rx.clone(),
        );
        tokio::spawn(supervise("metrics_listener", move || {
            metrics_http::serve(h.clone(), bind.clone(), token.clone(), stop.clone())
        }))
    });

    let layer = IpLayer {
        config: Some(config.clone()),
        trust: trust.clone(),
        cf,
    };
    let app = Router::new()
        .merge(farsight_api::router(api.clone()))
        .merge(farsight_web::pages::router(web.clone()))
        .route(
            "/health",
            get(health::health).with_state(health::HealthState {
                pool: api_pool.clone(),
                config: config.clone(),
                last: Arc::default(),
            }),
        )
        .route("/livez", get(health::livez))
        // One answer for every path that does not exist, and for the
        // routes of a feature that is switched off.
        .fallback(farsight_web::common::fallback)
        .layer(axum::middleware::from_fn_with_state(
            layer,
            client_ip_middleware,
        ));

    let bind = cfg.server.bind.clone();
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(ServerError::io(format!("binding {bind}")))?;
    tracing::info!(bind, hostname = %cfg.server.hostname, "normal mode: serving");

    // Not periodic jobs: the warming worker waits on its queue, and the
    // index builder runs until the four indexes are valid. Both start
    // once normal mode is serving (after a wizard run too): a long index
    // build never stands between a start and a healthy instance.
    let warming = tokio::spawn(supervise("handle_warming", {
        let (web, stop) = (web.clone(), stop_rx.clone());
        move || farsight_web::public::warming::run(web.clone(), stop.clone())
    }));
    let pass = tokio::spawn(supervise("handle_pass", {
        let (web, stop) = (web.clone(), stop_rx.clone());
        move || farsight_web::public::pass::run(web.clone(), stop.clone())
    }));
    let top_lists = tokio::spawn(supervise("top_lists", {
        let (web, stop) = (web.clone(), stop_rx.clone());
        move || farsight_web::public::top::run(web.clone(), stop.clone())
    }));
    let index_builder = tokio::spawn(supervise("sort_index_builder", {
        let (pool, database_url, config, status, stop) = (
            tasks_pool.clone(),
            cfg.storage.database_url.clone(),
            config.clone(),
            status.clone(),
            stop_rx.clone(),
        );
        move || {
            sort_indexes::Builder {
                pool: pool.clone(),
                database_url: database_url.clone(),
                config: config.clone(),
                sort: sort.clone(),
                status: status.clone(),
            }
            .run(stop.clone())
        }
    }));

    // A panic in ingest's reader or writer ends normal mode: the listener
    // stops, everything is torn down in order, and the process exits with
    // an error for its supervisor to start it again.
    let ingest_failed = Arc::new(Mutex::new(None::<&'static str>));
    let ingest_wait = {
        let failed = ingest.failed();
        let seen = ingest_failed.clone();
        async move {
            let name = failed.await;
            *seen.lock().unwrap_or_else(|e| e.into_inner()) = Some(name);
        }
    };
    let shutdown_wait = shutdown.clone();
    let mut reset_wait = reset_rx.clone();
    let (stopping_tx, stopping_rx) = tokio::sync::oneshot::channel::<()>();
    // The proxies the configuration trusts hold the connections of all
    // their clients: the per-peer bound is not for them.
    let trusted_proxy: listen::PeerFilter = {
        let (config, trust) = (config.clone(), trust.clone());
        Arc::new(move |ip| {
            trust
                .trusted(&config.current().config.proxy)
                .iter()
                .any(|net| net.contains(&ip))
        })
    };
    let served = axum::serve(
        listen::guarded_per_peer(
            listener,
            listen::MAX_CONNECTIONS,
            listen::MAX_PER_PEER,
            trusted_proxy,
        ),
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        tokio::select! {
            _ = stopped(shutdown_wait) => {}
            _ = ingest_wait => {}
            _ = async { while !*reset_wait.borrow() { if reset_wait.changed().await.is_err() { break; } } } => {
                // Let the reset page reach the browser first.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        let _ = stopping_tx.send(());
    })
    .into_future();
    // Open connections get a bounded time to finish. One that is still
    // open then (a client that stalled in the middle of a request) is
    // left behind: the teardown and the exit do not wait for it.
    let served = match listen::drain_within(served, stopping_rx, listen::DRAIN).await {
        Some(r) => r,
        None => {
            tracing::warn!("connections still open after the drain time; going on without them");
            Ok(())
        }
    };

    // Teardown, in dependency order. Every wait has a deadline and the
    // deadlines add up to less than the 45 seconds a supervisor gives
    // (`stop_grace_period`): 10 for the drain above, 5 for the tasks, 20
    // for ingest, 5 for the last flushes.
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(TASKS_STOP, async {
        let _ = refresher.await;
        let _ = scheduler.await;
        if let Some(m) = metrics_task {
            let _ = m.await;
        }
    })
    .await;
    housekeeping.abort();
    warming.abort();
    pass.abort();
    top_lists.abort();
    // An interrupted build leaves an invalid index; the next start drops
    // it and builds again.
    index_builder.abort();
    ingest.shutdown(INGEST_STOP).await;
    flusher.abort();
    let limits = Limits::from_config(&config.current().config);
    let flushed = tokio::time::timeout(LAST_FLUSH, async {
        if let Err(e) = counters.flush(&tasks_pool, &limits).await {
            tracing::warn!(error = %e, "final counter flush failed");
        }
        let _ = keys.flush_usage(&api_pool).await;
    })
    .await;
    if flushed.is_err() {
        tracing::warn!("final counter flush did not finish in time");
    }
    // Closing a pool waits for its connections to come back; a
    // connection held by a request that was left behind does not.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        api_pool.close().await;
        tasks_pool.close().await;
    })
    .await;
    served.map_err(ServerError::io("listener"))?;
    if let Some(task) = *ingest_failed.lock().unwrap_or_else(|e| e.into_inner()) {
        return Err(ServerError::IngestPanicked(task));
    }
    if *reset_rx.borrow_and_update() {
        tracing::info!("configuration reset; entering setup mode in-process");
        return Ok(ModeEnd::Switch);
    }
    Ok(ModeEnd::Shutdown)
}

/// Largest Cloudflare range list accepted.
const CLOUDFLARE_LIST_MAX_BYTES: usize = 256 * 1024;

async fn refresh_cloudflare() -> Result<Vec<ipnet::IpNet>, CloudflareError> {
    let unusable = |url: &'static str, problem: String| CloudflareError::List { url, problem };
    // Direct, like every other outbound request: a proxy named in the
    // environment is not used.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .no_proxy()
        .build()?;
    let mut all = Vec::new();
    for url in farsight_core::cloudflare::REFRESH_URLS {
        let mut resp = client
            .get(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)?;
        // The lists are a few hundred bytes; the cap is what a response
        // may cost at most.
        let mut body = Vec::new();
        while let Some(chunk) = resp.chunk().await? {
            if body.len() + chunk.len() > CLOUDFLARE_LIST_MAX_BYTES {
                return Err(unusable(
                    url,
                    format!("larger than {CLOUDFLARE_LIST_MAX_BYTES} bytes"),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let text = String::from_utf8(body).map_err(|e| unusable(url, e.to_string()))?;
        let nets = farsight_core::cloudflare::parse_list(&text)
            .ok_or_else(|| unusable(url, "unexpected content".into()))?;
        if nets.is_empty() {
            return Err(unusable(url, "empty list".into()));
        }
        all.extend(nets);
    }
    Ok(all)
}
