//! Background backfill for Farsight: sweep sources, the tiered scheduler
//! with per-host limits, repo listing, list jobs and optional subject
//! discovery (see `docs/design/backfill.md`).
//!
//! The process idles while no config exists (setup mode, or after a
//! config reset), waits for the server to migrate the schema, then runs one
//! scheduler, the sweep, the debt feeder and its own budget monitor on its
//! own pool. Config reloads on `NOTIFY farsight_config` and every 60 s by
//! mtime.
//!
//! A change that the running tasks cannot take in place rebuilds them:
//! the scheduler stops its jobs and gives their work back to the queue,
//! and new tasks start on the new config. One network layer serves every
//! rebuild, so what it knows of each host (cooldowns, the breaker,
//! requests in flight) is kept, and no host is ever asked by two sets of
//! jobs at once.
//!
//! Every task is supervised (`farsight_core::task`): a panic in the
//! scheduler loop, the sweep or the metrics listener is logged, counted
//! and the task started again; a panic in one pass of a periodic task
//! costs that pass; a panic in a job costs that job (see `scheduler`).

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
use farsight_core::listen;
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
/// How often the recorder's upkeep runs.
const UPKEEP_EVERY: Duration = Duration::from_secs(5);
/// Pause before a metrics listener that could not bind tries again.
const BIND_RETRY: Duration = Duration::from_secs(30);
/// How long a stopping metrics listener waits for a scrape in flight.
const METRICS_DRAIN: Duration = Duration::from_secs(3);

/// The supervised tasks, by the name their panics are counted under.
pub const TASKS: [&str; 9] = [
    "backfill_scheduler",
    scheduler::JOB_TASK,
    "backfill_sweep",
    "backfill_feeder",
    "backfill_budget_monitor",
    "backfill_pending_timeouts",
    "backfill_gauges",
    "backfill_counter_flush",
    "backfill_metrics_listener",
];

/// The config file path: `FARSIGHT_CONFIG` if set, else the default the
/// server uses.
pub fn config_path() -> PathBuf {
    std::env::var_os(CONFIG_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(config::DEFAULT_CONFIG_PATH))
}

/// Installs the process-wide recorder (host series expire when idle).
/// Must run inside the runtime: the recorder's upkeep is a task.
pub fn install_metrics() -> Option<PrometheusHandle> {
    let b =
        PrometheusBuilder::new().idle_timeout(metrics_util::MetricKindMask::ALL, Some(METRIC_IDLE));
    match b.install_recorder() {
        Ok(h) => {
            // Histogram samples are held until a scrape or the upkeep
            // drains them: without it a process nobody scrapes grows
            // without bound.
            let upkeep = h.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(UPKEEP_EVERY);
                loop {
                    tick.tick().await;
                    upkeep.run_upkeep();
                }
            });
            metrics::register();
            farsight_core::task::register(&TASKS);
            // Reconciles write history rows in this process.
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
    // An address that cannot be bound now (still held by a process that
    // is on its way out, say) is tried again until it can.
    let listener = loop {
        match tokio::net::TcpListener::bind(&bind).await {
            Ok(l) => break l,
            Err(e) => {
                tracing::warn!(error = %e, bind, "metrics listener not started; trying again");
                tokio::select! {
                    () = tokio::time::sleep(BIND_RETRY) => {}
                    r = stop.changed() => if r.is_err() { return; },
                }
                if *stop.borrow() {
                    return;
                }
            }
        }
    };
    let (stopping_tx, stopping_rx) = tokio::sync::oneshot::channel::<()>();
    let served = axum::serve(
        listen::guarded(listener, listen::MAX_METRICS_CONNECTIONS),
        app,
    )
    .with_graceful_shutdown(async move {
        while !*stop.borrow() {
            if stop.changed().await.is_err() {
                break;
            }
        }
        let _ = stopping_tx.send(());
    })
    .into_future();
    let _ = listen::drain_within(served, stopping_rx, METRICS_DRAIN).await;
}

/// Why a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// The shutdown signal arrived: the process exits.
    Shutdown,
    /// Config gone (reset): idle until one exists again.
    Idle,
    /// A restart-only key changed (database URL, binds): rebuild.
    Restart,
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Why the backfill process cannot run.
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    /// The configuration does not load. `origin` names where it was read
    /// from: the file's path, or the environment.
    #[error("invalid configuration in {origin}: {error}")]
    Config {
        /// Where the configuration came from.
        origin: String,
        /// Why it does not load.
        #[source]
        error: config::ConfigError,
    },
    /// The database could not be reached, or its schema not read.
    #[error(transparent)]
    Storage(#[from] farsight_storage::StorageError),
}

/// Runs until `shutdown` flips. Errors are fatal (invalid config).
pub async fn run(
    path: PathBuf,
    shutdown: watch::Receiver<bool>,
    metrics: Option<PrometheusHandle>,
) -> Result<(), BackfillError> {
    // The network layer outlives every rebuild.
    let mut net: Option<Arc<Net>> = None;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let env: Vec<(String, String)> = std::env::vars().collect();
        let mode = config::load(&path, &env).map_err(|error| {
            let origin = if path.exists() {
                path.display().to_string()
            } else {
                "the environment (FARSIGHT_SKIP_WIZARD)".to_owned()
            };
            BackfillError::Config { origin, error }
        })?;
        let end = match mode {
            StartMode::Setup => idle(&path, shutdown.clone()).await,
            StartMode::Normal(loaded) => {
                match run_normal(
                    Arc::new(loaded.config),
                    &path,
                    shutdown.clone(),
                    metrics.clone(),
                    &mut net,
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
        // The fakes are on loopback: a proxy named in the environment
        // has no part in reaching them.
        if let Ok(plain) = reqwest::Client::builder().no_proxy().build() {
            return Client::Plain(plain);
        }
    }
    // No redirect is followed: a request is counted against the limits
    // of the host it was made to, and a redirect would take it to a host
    // whose slot and breaker it never touched.
    let mut settings = SafeClientConfig::from_config(cfg, VERSION);
    settings.max_redirects = 0;
    Client::Safe(SafeClient::new(settings))
}

async fn run_normal(
    cfg: Arc<Config>,
    path: &Path,
    mut shutdown: watch::Receiver<bool>,
    metrics_handle: Option<PrometheusHandle>,
    shared_net: &mut Option<Arc<Net>>,
) -> Result<End, BackfillError> {
    let max_conn = cfg.backfill.concurrency.saturating_add(8);
    let pool = farsight_storage::connect(&cfg.storage.database_url, max_conn).await?;
    tracing::info!(
        schema = farsight_storage::SCHEMA_VERSION,
        "waiting for the schema"
    );
    tokio::select! {
        r = farsight_storage::wait_for_schema(&pool) => r?,
        _ = shutdown.changed() => return Ok(End::Shutdown),
    }
    let net = match shared_net {
        // A rebuild: the new client and limits, and everything the layer
        // has learned of the hosts it talked to.
        Some(net) => {
            net.reconfigure(client(&cfg), &cfg);
            net.clone()
        }
        None => shared_net
            .insert(Arc::new(Net::new(client(&cfg), &cfg)))
            .clone(),
    };
    let dns = match client(&cfg) {
        Client::Safe(c) => Some(c),
        #[cfg(feature = "harness")]
        Client::Plain(_) => None,
    };
    let counters = Ctx::counter_sink();
    let resolver = Resolver::new(net.clone(), pool.clone(), &cfg, dns, counters.clone());
    let ctx = Arc::new(Ctx::new(
        pool.clone(),
        cfg.clone(),
        net,
        resolver,
        counters,
        VERSION,
    ));
    let (stop_tx, stop) = watch::channel(false);
    let mut tasks = tokio::task::JoinSet::new();
    if let Some(h) = metrics_handle {
        let c = cfg.clone();
        let s = stop.clone();
        tasks.spawn(farsight_core::task::supervise(
            "backfill_metrics_listener",
            move || {
                let (h, c, s) = (h.clone(), c.clone(), s.clone());
                async move { serve_metrics(h, &c, s).await }
            },
        ));
    }
    // The gates first: no job runs before the budget state is known.
    budget_monitor(&ctx).await;
    let sched = Scheduler::new(ctx.clone());
    tasks.spawn(farsight_core::task::supervise("backfill_scheduler", {
        let (sched, stop) = (sched.clone(), stop.clone());
        move || sched.clone().run(stop.clone())
    }));
    let sweep = Arc::new(sweep::Sweep::default());
    tasks.spawn(farsight_core::task::supervise("backfill_sweep", {
        let (ctx, stop) = (ctx.clone(), stop.clone());
        move || sweep::run(ctx.clone(), sweep.clone(), stop.clone())
    }));
    tasks.spawn(every("backfill_feeder", FEEDER_EVERY, stop.clone(), {
        let ctx = ctx.clone();
        move || {
            let ctx = ctx.clone();
            async move {
                // First what a stopped process left behind, then the
                // debts.
                match feeder::recover(&ctx).await {
                    Ok(r) if r.total() > 0 => tracing::info!(
                        entries = r.entries,
                        repo_jobs = r.repo_jobs,
                        fetch_runs = r.fetch_runs,
                        "work left by a stopped process taken up"
                    ),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "taking up left work failed"),
                }
                match feeder::pass(&ctx).await {
                    Ok(n) if n > 0 => tracing::debug!(fed = n, "feeder pass"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "feeder pass failed"),
                }
            }
        }
    }));
    tasks.spawn(every(
        "backfill_budget_monitor",
        BUDGET_EVERY,
        stop.clone(),
        {
            let ctx = ctx.clone();
            move || {
                let ctx = ctx.clone();
                async move { budget_monitor(&ctx).await }
            }
        },
    ));
    tasks.spawn(every(
        "backfill_pending_timeouts",
        PERIODIC_EVERY,
        stop.clone(),
        {
            let ctx = ctx.clone();
            move || {
                let ctx = ctx.clone();
                async move {
                    if let Ok(now) = jobs::db_now(&ctx.pool).await
                        && let Err(e) = jobs::list_phase1::pending_timeouts(&ctx, now).await
                    {
                        tracing::warn!(error = %e, "pending_max_age pass failed");
                    }
                }
            }
        },
    ));
    tasks.spawn(every("backfill_gauges", GAUGES_EVERY, stop.clone(), {
        let sched = sched.clone();
        move || {
            let sched = sched.clone();
            async move { sched.publish_gauges().await }
        }
    }));
    tasks.spawn(every(
        "backfill_counter_flush",
        COUNTER_FLUSH,
        stop.clone(),
        {
            let ctx = ctx.clone();
            move || {
                let ctx = ctx.clone();
                async move {
                    if let Err(e) = ctx.counters.flush(&ctx.pool, &ctx.limits()).await {
                        tracing::warn!(error = %e, "counter flush failed");
                    }
                }
            }
        },
    ));
    tracing::info!(concurrency = cfg.backfill.concurrency, "backfill running");
    let end = watch_config(&ctx, path, &mut shutdown).await;
    let _ = stop_tx.send(true);
    // The scheduler stops its jobs at their next await and gives back the
    // queue entries and leases they held; resumable state (cursors, runs)
    // is persisted as it goes. What a task cut short here still holds
    // runs out and is taken up by the feeder.
    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .is_err()
    {
        tasks.abort_all();
    }
    // 30 seconds for the jobs and 5 for the last flush stay under the 45
    // a supervisor gives.
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        ctx.counters.flush(&ctx.pool, &ctx.limits()),
    )
    .await;
    tracing::info!(?end, "backfill stopped");
    Ok(end)
}

/// Runs `f` every `period` until `stop` flips (never overlapping itself).
/// A pass that panics is reported under `task` and the next one runs as
/// scheduled.
async fn every<F, Fut>(task: &'static str, period: Duration, mut stop: watch::Receiver<bool>, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    loop {
        if let Err(message) = farsight_core::task::catch(f()).await {
            farsight_core::task::report_panic(task, &message);
        }
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
    if let Some(l) = listener.as_mut()
        && let Err(e) = l.listen("farsight_config").await
    {
        tracing::warn!(error = %e, "LISTEN farsight_config failed; polling only");
        listener = None;
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
                    // The request limits apply to the next request.
                    ctx.net.set_limits(&new);
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

/// Keys the running tasks cannot apply in place: the database, the
/// listeners, the outbound client, what the resolver was built with, and
/// `backfill.concurrency`, which sizes the database pool.
pub fn restart_needed(old: &Config, new: &Config) -> bool {
    old.storage.database_url != new.storage.database_url
        || old.metrics != new.metrics
        || old.backfill.concurrency != new.backfill.concurrency
        || old.backfill.plc_url != new.backfill.plc_url
        || old.net != new.net
        || old.limits.large_hosts != new.limits.large_hosts
        || old.limits.cdn_ranges_extra != new.limits.cdn_ranges_extra
}

/// Backfill's own budget monitor: measures the database and publishes
/// the gates to its writers and jobs. Refusal intervals and GF/GO on
/// lists are the server's (one owner each).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_what_cannot_change_in_place_rebuilds() {
        let base = Config::default();
        let with = |f: fn(&mut Config)| {
            let mut c = Config::default();
            f(&mut c);
            c
        };
        // The pool is sized by the worker count: a change rebuilds, so
        // the pool always has a connection for every worker.
        assert!(restart_needed(
            &base,
            &with(|c| c.backfill.concurrency = 64)
        ));
        assert!(restart_needed(
            &base,
            &with(|c| c.backfill.plc_url = "https://plc.example".into())
        ));
        assert!(restart_needed(
            &base,
            &with(|c| c.storage.database_url = "postgres://other".into())
        ));
        // Request limits are taken by the next request.
        for change in [
            (|c| c.backfill.per_host_rps = 1) as fn(&mut Config),
            |c| c.backfill.per_host_concurrency = 1,
            |c| c.backfill.per_domain_rps = 2,
            |c| c.backfill.per_domain_concurrency = 2,
            |c| c.backfill.plc_rps = 3,
            |c| c.backfill.on_demand_reserved = 0,
            |c| c.backfill.sweep.enabled = !c.backfill.sweep.enabled,
        ] {
            assert!(!restart_needed(&base, &with(change)));
        }
    }
}
