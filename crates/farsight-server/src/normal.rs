//! Normal mode (design §2, §8.2): migrations, the API pool (separate from
//! ingest's 4 connections), the ingest task, the coverage snapshot and its
//! NOTIFY listener, the API and UI routers, `/health` and `/livez`, the
//! metrics listener and the periodic tasks. Ends on shutdown or on a
//! config reset (then the caller enters setup mode in-process).

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
use farsight_core::net::{SafeClient, SafeClientConfig};
use farsight_ingest::{Ingest, IngestConfig};
use farsight_storage::counters::{CounterSink, FLUSH_INTERVAL};
use farsight_storage::gates::SharedGates;
use farsight_storage::keys::Limits;
use metrics_exporter_prometheus::PrometheusHandle;
use sqlx::PgPool;
use tokio::sync::{Semaphore, watch};

use crate::tasks::{self, TaskCtx};
use crate::{ModeEnd, VERSION, health, metrics_http};

/// Extra API-pool connections beyond the query semaphore (writes,
/// lookups, health).
pub const API_POOL_EXTRA: u32 = 8;
/// Tasks pool size.
pub const TASKS_POOL: u32 = 4;
/// Counter sink shard of the server's own writes (ingest uses 0).
pub const TASKS_SHARD: i16 = 1;

async fn connect_retry(
    url: &str,
    max: u32,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<PgPool>, String> {
    let mut wait = Duration::from_millis(500);
    loop {
        match farsight_storage::connect(url, max).await {
            Ok(p) => return Ok(Some(p)),
            Err(e) => {
                tracing::warn!(error = %e, "database not reachable yet; retrying");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() { return Ok(None); }
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

/// Runs normal mode.
pub async fn run(
    loaded: LoadedConfig,
    config_path: PathBuf,
    env: Vec<(String, String)>,
    mut shutdown: watch::Receiver<bool>,
    metrics_handle: Option<PrometheusHandle>,
) -> Result<ModeEnd, String> {
    for w in &loaded.warnings {
        tracing::warn!(warning = %w, "configuration");
    }
    let cfg = loaded.config.clone();
    let api_pool_size = cfg.rate_limit.query_concurrency + API_POOL_EXTRA;
    let Some(api_pool) =
        connect_retry(&cfg.storage.database_url, api_pool_size, &mut shutdown).await?
    else {
        return Ok(ModeEnd::Shutdown);
    };
    farsight_storage::migrate(&api_pool)
        .await
        .map_err(|e| format!("migrations: {e}"))?;
    tracing::info!(
        schema = farsight_storage::SCHEMA_VERSION,
        "migrations applied"
    );
    // The recording window (§7.7) opens or closes here and only here: the
    // ingest writer takes `block_history_enabled` at start.
    let recording =
        farsight_storage::history::sync_window(&api_pool, cfg.storage.block_history_enabled)
            .await
            .map_err(|e| format!("history window: {e}"))?;
    tracing::info!(recording, "block and list-membership history");
    let ingest_pool =
        farsight_storage::connect(&cfg.storage.database_url, farsight_ingest::POOL_SIZE)
            .await
            .map_err(|e| e.to_string())?;
    let tasks_pool = farsight_storage::connect(&cfg.storage.database_url, TASKS_POOL)
        .await
        .map_err(|e| e.to_string())?;

    let config = Arc::new(ConfigStore::new(config_path.clone(), env, loaded));
    let gates = Arc::new(SharedGates::default());
    let (stop_tx, stop_rx) = watch::channel(false);

    // Ingest (its own 4-connection pool, §6.2).
    let mut icfg = IngestConfig::from_config(&cfg);
    icfg.gates = gates.clone();
    let ingest = Ingest::start(icfg, ingest_pool)
        .await
        .map_err(|e| format!("starting ingest: {e}"))?;

    // The server's own counter sink (janitors, requestBackfill interning).
    let counters = Arc::new(CounterSink::new(TASKS_SHARD));
    let flusher = {
        let counters = counters.clone();
        let pool = tasks_pool.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            loop {
                tick.tick().await;
                let limits = Limits::from_config(&config.current().config);
                if let Err(e) = counters.flush(&pool, &limits).await {
                    tracing::warn!(error = %e, "counter flush failed");
                }
            }
        })
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
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    let refresher = tokio::spawn(farsight_api::snapshot::run_refresher(
        snapshot.clone(),
        api_pool.clone(),
        config.clone(),
        stop_rx.clone(),
    ));

    let keys = Arc::new(farsight_api::auth::KeyTable::default());
    keys.refresh(&api_pool)
        .await
        .map_err(|e| format!("loading API keys: {e}"))?;
    let limiter = Arc::new(farsight_api::ratelimit::RateLimiter::default());
    let trust = Arc::new(ProxyTrust::default());
    let cf = Arc::new(CfTracker::default());
    farsight_api::metrics::register();
    farsight_storage::history::register_metrics();
    farsight_web::public::metrics::register();

    let api = Arc::new(ApiState {
        pool: api_pool.clone(),
        config: config.clone(),
        snapshot: snapshot.clone(),
        keys: keys.clone(),
        limiter: limiter.clone(),
        query_permits: Arc::new(Semaphore::new(
            cfg.rate_limit.query_concurrency.max(1) as usize
        )),
        trust: trust.clone(),
        cf: cf.clone(),
        ingest: Some(IngestLink {
            stats: ingest.stats.clone(),
            control: ingest.control.clone(),
        }),
        counters: counters.clone(),
        gates: gates.clone(),
        version: VERSION,
    });
    let status = Arc::new(farsight_web::ServerStatus::default());
    let (reset_tx, mut reset_rx) = watch::channel(false);
    let web = Arc::new(farsight_web::WebState {
        api: api.clone(),
        safe: SafeClient::new(SafeClientConfig::from_config(&cfg, VERSION)),
        bcrypt_permits: Arc::new(Semaphore::new(
            cfg.rate_limit.bcrypt_concurrency.max(1) as usize
        )),
        recent_logins: Mutex::new(Default::default()),
        reset: reset_tx,
        token_path: farsight_web::setup_token::token_path(&config_path),
        status: status.clone(),
        public: Default::default(),
    });

    // Housekeeping of in-memory API state.
    let housekeeping = {
        let keys = keys.clone();
        let limiter = limiter.clone();
        let pool = api_pool.clone();
        let trust = trust.clone();
        let config = config.clone();
        let status = status.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            let mut n: u64 = 0;
            loop {
                tick.tick().await;
                n += 1;
                if let Err(e) = keys.refresh(&pool).await {
                    tracing::warn!(error = %e, "API key refresh failed");
                }
                if n % 2 == 0 {
                    let _ = keys.flush_usage(&pool).await;
                }
                if n % 20 == 0 {
                    limiter.sweep(Duration::from_secs(600));
                }
                // Opt-in daily refresh of the Cloudflare ranges (§9.3).
                let proxy = config.current().config.proxy.clone();
                if proxy.cloudflare_refresh
                    && proxy.mode == farsight_core::config::ProxyMode::Cloudflare
                    && (n == 1 || n % 2880 == 0)
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
        })
    };

    let task_ctx = Arc::new(TaskCtx::new(
        tasks_pool.clone(),
        config.clone(),
        counters.clone(),
        gates.clone(),
        status.clone(),
    ));
    let scheduler = tokio::spawn(tasks::run_scheduler(task_ctx, stop_rx.clone()));

    let metrics_task = metrics_handle.map(|h| {
        tokio::spawn(metrics_http::serve(
            h,
            cfg.metrics.bind.clone(),
            cfg.metrics.bearer_token_sha256.clone(),
            stop_rx.clone(),
        ))
    });

    let layer = IpLayer {
        config: Some(config.clone()),
        trust,
        cf,
    };
    let app = Router::new()
        .merge(farsight_api::router(api.clone()))
        .merge(farsight_web::pages::router(web.clone()))
        .route("/health", get(health::health).with_state(api_pool.clone()))
        .route("/livez", get(health::livez))
        // One answer for every path that does not exist, and for the
        // routes of a feature that is switched off (§8.6).
        .fallback(farsight_web::common::fallback)
        .layer(axum::middleware::from_fn_with_state(
            layer,
            client_ip_middleware,
        ));

    let bind = cfg.server.bind.clone();
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("binding {bind}: {e}"))?;
    tracing::info!(bind, hostname = %cfg.server.hostname, "normal mode: serving");

    let shutdown_wait = shutdown.clone();
    let mut reset_wait = reset_rx.clone();
    let served = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        tokio::select! {
            _ = stopped(shutdown_wait) => {}
            _ = async { while !*reset_wait.borrow() { if reset_wait.changed().await.is_err() { break; } } } => {
                // Let the reset page reach the browser first.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    })
    .await;

    // Teardown, in dependency order.
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), refresher).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), scheduler).await;
    if let Some(m) = metrics_task {
        let _ = tokio::time::timeout(Duration::from_secs(5), m).await;
    }
    housekeeping.abort();
    ingest.shutdown().await;
    flusher.abort();
    let limits = Limits::from_config(&config.current().config);
    if let Err(e) = counters.flush(&tasks_pool, &limits).await {
        tracing::warn!(error = %e, "final counter flush failed");
    }
    let _ = keys.flush_usage(&api_pool).await;
    api_pool.close().await;
    tasks_pool.close().await;
    served.map_err(|e| format!("listener: {e}"))?;
    if *reset_rx.borrow_and_update() {
        tracing::info!("configuration reset; entering setup mode in-process");
        return Ok(ModeEnd::Switch);
    }
    Ok(ModeEnd::Shutdown)
}

async fn refresh_cloudflare() -> Result<Vec<ipnet::IpNet>, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|e| e.to_string())?;
    let mut all = Vec::new();
    for url in farsight_core::cloudflare::REFRESH_URLS {
        let text = client
            .get(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| e.to_string())?
            .text()
            .await
            .map_err(|e| e.to_string())?;
        let nets = farsight_core::cloudflare::parse_list(&text)
            .ok_or_else(|| format!("{url}: unexpected content"))?;
        if nets.is_empty() {
            return Err(format!("{url}: empty list"));
        }
        all.extend(nets);
    }
    Ok(all)
}
