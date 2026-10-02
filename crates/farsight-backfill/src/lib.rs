//! Background backfill for Farsight: sweep sources, the tiered scheduler
//! with per-host limits, repo listing, list jobs and optional subject
//! discovery (design §5).
//!
//! The process idles while no config exists (setup mode, or after a
//! config reset), waits for the server to migrate the schema, then runs one
//! scheduler, the sweep, the debt feeder and its own budget monitor on its
//! own pool. Config reloads on `NOTIFY farsight_config` and every 60 s by
//! mtime.

#![warn(missing_docs)]
#![allow(clippy::result_large_err)]

pub mod ctx;
pub mod feeder;
pub mod jobs;
pub mod lanes;
pub mod metrics;
pub mod net;
pub mod resolve;
pub mod scheduler;
pub mod sweep;
pub mod xrpc;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use farsight_core::Config;
use farsight_core::config::{self, StartMode};
use farsight_core::net::{SafeClient, SafeClientConfig};
use farsight_storage::gates::{self, Budget};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::watch;

use crate::ctx::Ctx;
use crate::net::{Client, Net};
use crate::resolve::Resolver;
use crate::scheduler::Scheduler;

/// Binary version (User-Agent).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Overrides the config path, as for the server.
pub const CONFIG_PATH_ENV: &str = "FARSIGHT_CONFIG";
/// Harness builds: `1` makes outbound requests over plain HTTP to the
/// loopback fakes.
pub const HARNESS_PLAIN_ENV: &str = "FARSIGHT_HARNESS_PLAIN_HTTP";

const IDLE_POLL: Duration = Duration::from_secs(5);
const RELOAD_EVERY: Duration = Duration::from_secs(60);
const FEEDER_EVERY: Duration = Duration::from_secs(10);
const BUDGET_EVERY: Duration = Duration::from_secs(60);
const PERIODIC_EVERY: Duration = Duration::from_secs(60);
const GAUGES_EVERY: Duration = Duration::from_secs(15);
const COUNTER_FLUSH: Duration = Duration::from_secs(5);
/// Series not updated for this long are dropped (bounded host labels).
const METRIC_IDLE: Duration = Duration::from_secs(900);

/// The config file path.
pub fn config_path() -> PathBuf {
    std::env::var_os(CONFIG_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(config::DEFAULT_CONFIG_PATH))
}

/// Installs the process-wide recorder (host series expire when idle).
pub fn install_metrics() -> Option<PrometheusHandle> {
    let b =
        PrometheusBuilder::new().idle_timeout(metrics_util::MetricKindMask::ALL, Some(METRIC_IDLE));
    match b.install_recorder() {
        Ok(h) => {
            metrics::register();
            // Reconciles write history rows in this process (§7.7).
            farsight_storage::history::register_metrics();
            Some(h)
        }
        Err(e) => {
            tracing::warn!(error = %e, "metrics recorder not installed");
            None
        }
    }
}

#[derive(Clone)]
struct MetricsState {
    handle: PrometheusHandle,
    bearer_sha256: Option<[u8; 32]>,
}

fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

async fn render(State(st): State<Arc<MetricsState>>, headers: HeaderMap) -> Response {
    if let Some(want) = st.bearer_sha256 {
        let presented: Option<[u8; 32]> = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|t| Sha256::digest(t.as_bytes()).into());
        if !presented.is_some_and(|h| bool::from(h.ct_eq(&want))) {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "unauthorized",
            )
                .into_response();
        }
    }
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        st.handle.render(),
    )
        .into_response()
}

/// Serves `/metrics` on `metrics.backfill_bind` until `stop` flips.
async fn serve_metrics(handle: PrometheusHandle, cfg: &Config, mut stop: watch::Receiver<bool>) {
    let state = Arc::new(MetricsState {
        handle,
        bearer_sha256: unhex32(&cfg.metrics.bearer_token_sha256),
    });
    let app = Router::new()
        .route("/metrics", get(render))
        .with_state(state);
    let bind = cfg.metrics.backfill_bind.clone();
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, bind, "metrics listener not started");
            return;
        }
    };
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !*stop.borrow() {
                if stop.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
}

/// Why a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// Signal.
    Shutdown,
    /// Config gone (reset): idle until one exists again.
    Idle,
    /// A restart-only key changed (database URL, binds): rebuild.
    Restart,
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Runs until `shutdown` flips. Errors are fatal (invalid config).
pub async fn run(
    path: PathBuf,
    shutdown: watch::Receiver<bool>,
    metrics: Option<PrometheusHandle>,
) -> Result<(), String> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let env: Vec<(String, String)> = std::env::vars().collect();
        let mode = config::load(&path, &env).map_err(|e| {
            let source = if path.exists() {
                path.display().to_string()
            } else {
                "the environment (FARSIGHT_SKIP_WIZARD)".to_owned()
            };
            format!("invalid configuration in {source}: {e}")
        })?;
        let end = match mode {
            StartMode::Setup => idle(&path, shutdown.clone()).await,
            StartMode::Normal(loaded) => {
                match run_normal(
                    Arc::new(loaded.config),
                    &path,
                    shutdown.clone(),
                    metrics.clone(),
                )
                .await
                {
                    Ok(end) => end,
                    Err(e) => {
                        tracing::error!(error = %e, "backfill stopped; retrying");
                        let mut s = shutdown.clone();
                        tokio::select! {
                            _ = tokio::time::sleep(IDLE_POLL) => {}
                            _ = s.changed() => {}
                        }
                        End::Restart
                    }
                }
            }
        };
        if end == End::Shutdown {
            return Ok(());
        }
    }
}

/// Setup mode (no config): do nothing until a config appears.
async fn idle(path: &Path, mut shutdown: watch::Receiver<bool>) -> End {
    tracing::info!("no configuration yet: idle until setup finishes");
    loop {
        tokio::select! {
            _ = tokio::time::sleep(IDLE_POLL) => {}
            _ = shutdown.changed() => {}
        }
        if *shutdown.borrow() {
            return End::Shutdown;
        }
        let env: Vec<(String, String)> = std::env::vars().collect();
        if !matches!(config::load(path, &env), Ok(StartMode::Setup)) {
            return End::Restart;
        }
    }
}

fn client(cfg: &Config) -> Client {
    #[cfg(feature = "harness")]
    if std::env::var(HARNESS_PLAIN_ENV).is_ok_and(|v| v == "1") {
        tracing::warn!("harness build: outbound requests over plain HTTP");
        return Client::Plain(reqwest::Client::new());
    }
    Client::Safe(SafeClient::new(SafeClientConfig::from_config(cfg, VERSION)))
}

async fn run_normal(
    cfg: Arc<Config>,
    path: &Path,
    mut shutdown: watch::Receiver<bool>,
    metrics_handle: Option<PrometheusHandle>,
) -> Result<End, String> {
    let max_conn = cfg.backfill.concurrency.saturating_add(8);
    let pool = farsight_storage::connect(&cfg.storage.database_url, max_conn)
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        schema = farsight_storage::SCHEMA_VERSION,
        "waiting for the schema"
    );
    tokio::select! {
        r = farsight_storage::wait_for_schema(&pool) => r.map_err(|e| e.to_string())?,
        _ = shutdown.changed() => return Ok(End::Shutdown),
    }
    let net = Arc::new(Net::new(
        client(&cfg),
        cfg.backfill.per_host_rps,
        cfg.backfill.per_host_concurrency,
        cfg.backfill.plc_rps,
        &cfg.backfill.plc_url,
    ));
    let dns = match client(&cfg) {
        Client::Safe(c) => Some(c),
        #[cfg(feature = "harness")]
        Client::Plain(_) => None,
    };
    let resolver = Resolver::new(net.clone(), pool.clone(), &cfg, dns);
    let ctx = Arc::new(Ctx::new(pool.clone(), cfg.clone(), net, resolver, VERSION));
    let (stop_tx, stop) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    if let Some(h) = metrics_handle {
        let c = cfg.clone();
        let s = stop.clone();
        tasks.spawn(async move { serve_metrics(h, &c, s).await });
    }
    // The gates first: no job runs before the budget state is known.
    budget_monitor(&ctx).await;
    let sched = Scheduler::new(ctx.clone());
    tasks.spawn(sched.clone().run(stop.clone()));
    let sweep = Arc::new(sweep::Sweep::default());
    tasks.spawn(sweep::run(ctx.clone(), sweep, stop.clone()));
    tasks.spawn(every(FEEDER_EVERY, stop.clone(), {
        let ctx = ctx.clone();
        move || {
            let ctx = ctx.clone();
            async move {
                match feeder::pass(&ctx).await {
                    Ok(n) if n > 0 => tracing::debug!(fed = n, "feeder pass"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "feeder pass failed"),
                }
            }
        }
    }));
    tasks.spawn(every(BUDGET_EVERY, stop.clone(), {
        let ctx = ctx.clone();
        move || {
            let ctx = ctx.clone();
            async move { budget_monitor(&ctx).await }
        }
    }));
    tasks.spawn(every(PERIODIC_EVERY, stop.clone(), {
        let ctx = ctx.clone();
        move || {
            let ctx = ctx.clone();
            async move {
                if let Ok(now) = jobs::db_now(&ctx.pool).await {
                    if let Err(e) = jobs::list_phase1::pending_timeouts(&ctx, now).await {
                        tracing::warn!(error = %e, "pending_max_age pass failed");
                    }
                }
            }
        }
    }));
    tasks.spawn(every(GAUGES_EVERY, stop.clone(), {
        let sched = sched.clone();
        move || {
            let sched = sched.clone();
            async move { sched.publish_gauges().await }
        }
    }));
    tasks.spawn(every(COUNTER_FLUSH, stop.clone(), {
        let ctx = ctx.clone();
        move || {
            let ctx = ctx.clone();
            async move {
                if let Err(e) = ctx.counters.flush(&ctx.pool, &ctx.limits()).await {
                    tracing::warn!(error = %e, "counter flush failed");
                }
            }
        }
    }));
    tracing::info!(concurrency = cfg.backfill.concurrency, "backfill running");
    let end = watch_config(&ctx, path, &mut shutdown).await;
    let _ = stop_tx.send(true);
    // Jobs stop at their next await; leases expire if one is cut short and
    // resumable state (cursors, runs) is persisted as it goes.
    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .is_err()
    {
        tasks.abort_all();
    }
    let _ = ctx.counters.flush(&ctx.pool, &ctx.limits()).await;
    tracing::info!(?end, "backfill stopped");
    Ok(end)
}

/// Runs `f` every `period` until `stop` flips (never overlapping itself).
async fn every<F, Fut>(period: Duration, mut stop: watch::Receiver<bool>, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        f().await;
        tokio::select! {
            _ = tokio::time::sleep(period) => {}
            _ = stop.changed() => {}
        }
        if *stop.borrow() {
            return;
        }
    }
}

/// Reloads the config on `NOTIFY farsight_config` and every 60 s when the
/// file's mtime changed. Returns when the process must stop, idle or
/// restart.
async fn watch_config(ctx: &Ctx, path: &Path, shutdown: &mut watch::Receiver<bool>) -> End {
    let mut listener = sqlx::postgres::PgListener::connect_with(&ctx.pool)
        .await
        .ok();
    if let Some(l) = listener.as_mut() {
        if let Err(e) = l.listen("farsight_config").await {
            tracing::warn!(error = %e, "LISTEN farsight_config failed; polling only");
            listener = None;
        }
    }
    let mut seen = mtime(path);
    loop {
        let notified = async {
            match listener.as_mut() {
                Some(l) => l.recv().await.is_ok(),
                None => std::future::pending::<bool>().await,
            }
        };
        tokio::select! {
            _ = shutdown.changed() => return End::Shutdown,
            ok = notified => {
                if !ok {
                    tokio::time::sleep(IDLE_POLL).await;
                }
            }
            _ = tokio::time::sleep(RELOAD_EVERY) => {
                let now = mtime(path);
                if now == seen && now.is_some() {
                    continue;
                }
            }
        }
        if *shutdown.borrow() {
            return End::Shutdown;
        }
        seen = mtime(path);
        let env: Vec<(String, String)> = std::env::vars().collect();
        match config::load(path, &env) {
            Ok(StartMode::Setup) => {
                tracing::info!("configuration removed: idling");
                return End::Idle;
            }
            Ok(StartMode::Normal(l)) => {
                let old = ctx.cfg();
                let new = l.config;
                if restart_needed(&old, &new) {
                    tracing::info!("restart-only setting changed: restarting backfill");
                    return End::Restart;
                }
                if *old != new {
                    tracing::info!("configuration reloaded");
                    ctx.set_cfg(Arc::new(new));
                }
            }
            // An invalid edit keeps the running config (the server's
            // settings page validates before writing).
            Err(e) => {
                tracing::warn!(error = %e, "config reload failed; keeping the running config")
            }
        }
    }
}

/// Keys the running process cannot apply in place.
fn restart_needed(old: &Config, new: &Config) -> bool {
    old.storage.database_url != new.storage.database_url
        || old.metrics != new.metrics
        || old.backfill.per_host_rps != new.backfill.per_host_rps
        || old.backfill.per_host_concurrency != new.backfill.per_host_concurrency
        || old.backfill.plc_rps != new.backfill.plc_rps
        || old.backfill.plc_url != new.backfill.plc_url
        || old.net != new.net
        || old.limits.large_hosts != new.limits.large_hosts
        || old.limits.cdn_ranges_extra != new.limits.cdn_ranges_extra
}

/// Backfill's own budget monitor (§11.2): measures the database and
/// publishes the gates to its writers and jobs. Refusal intervals and
/// GF/GO on lists are the server's (one owner each).
pub async fn budget_monitor(ctx: &Ctx) {
    let cfg = ctx.cfg();
    let budget = Budget {
        budget_bytes: cfg.storage.budget_bytes,
        ceiling_bytes: cfg.storage.effective_hard_ceiling(),
    };
    match gates::measure_database_bytes(&ctx.pool).await {
        Ok(bytes) => {
            let prev = *ctx.gate_state.lock().unwrap_or_else(|e| e.into_inner());
            let next = gates::next_gate_state(prev, bytes, budget);
            ctx.gates.store(next.gates);
            *ctx.gate_state.lock().unwrap_or_else(|e| e.into_inner()) = next;
            if prev != next {
                tracing::info!(gates = ?next.gates, sweep_paused = next.sweep_paused, "storage gates changed");
            }
        }
        Err(e) => tracing::warn!(error = %e, "measuring the database failed"),
    }
}
