//! Periodic tasks (design §3.7.3, §4.2, §4.4, §7.1, §7.3, §11.2): one
//! process-wide scheduler with jitter. Each job runs in its own task and
//! never overlaps itself; a slow nightly job does not delay the budget
//! monitor.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use farsight_api::config_store::ConfigStore;
use farsight_storage::codes::{DeferCause, TrackState};
use farsight_storage::counters::CounterSink;
use farsight_storage::gates::{self, Budget, GateState, SharedGates};
use farsight_storage::keys::Limits;
use farsight_storage::tracking::FireArgs;
use farsight_storage::transition::Event;
use farsight_storage::{debts, firehose, history, janitor, recount};
use farsight_web::ServerStatus;
use sqlx::PgPool;
use tokio::sync::watch;

/// `farsight_storage_db_bytes`.
pub const STORAGE_DB_BYTES: &str = "farsight_storage_db_bytes";
/// `farsight_storage_budget_ratio`.
pub const STORAGE_BUDGET_RATIO: &str = "farsight_storage_budget_ratio";
/// `farsight_storage_table_bytes{table}`.
pub const STORAGE_TABLE_BYTES: &str = "farsight_storage_table_bytes";
/// `farsight_records{collection}`.
pub const RECORDS: &str = "farsight_records";
/// `farsight_abuse_capped_total{kind}` is counted where batches are
/// applied (ingest writer, backfill jobs); the monitor only publishes
/// gauges.
const TABLES: [&str; 11] = [
    "blocks_history",
    "list_blocks_history",
    "list_items_history",
    "blocks",
    "list_items",
    "actors",
    "list_blocks",
    "lists",
    "backfill_state",
    "tombstones",
    "firehose_clock",
];

/// Resync debts older than this become `unreachable` (§3.7.3, §6.2).
pub const RESYNC_TERMINAL: Duration = Duration::from_secs(7 * 24 * 3600);
/// Lists fired GO per budget-monitor pass after reopening (rate-limited,
/// oldest first; §11.2).
pub const GO_PER_PASS: i64 = 200;
/// Batch size of the nightly recount.
pub const RECOUNT_BATCH: i64 = 1000;

/// What the jobs share.
pub struct TaskCtx {
    /// Tasks pool.
    pub pool: PgPool,
    /// Live config.
    pub config: Arc<ConfigStore>,
    /// Counter sink for janitor writes.
    pub counters: Arc<CounterSink>,
    /// Gates published to every writer.
    pub gates: Arc<SharedGates>,
    /// Dashboard status.
    pub status: Arc<ServerStatus>,
    /// `storage.block_history_enabled` as it was at start: the server
    /// applies a change of the flag at restart (§7.7).
    pub history_enabled: bool,
    gate_state: Mutex<GateState>,
    samples: Mutex<VecDeque<(DateTime<Utc>, u64)>>,
}

impl TaskCtx {
    /// A context.
    pub fn new(
        pool: PgPool,
        config: Arc<ConfigStore>,
        counters: Arc<CounterSink>,
        gates: Arc<SharedGates>,
        status: Arc<ServerStatus>,
    ) -> TaskCtx {
        let history_enabled = config.current().config.storage.block_history_enabled;
        TaskCtx {
            pool,
            config,
            counters,
            gates,
            status,
            history_enabled,
            gate_state: Mutex::new(GateState::default()),
            samples: Mutex::new(VecDeque::new()),
        }
    }

    fn limits(&self) -> Limits {
        let mut l = Limits::from_config(&self.config.current().config);
        l.history_enabled = self.history_enabled;
        l
    }
}

type JobFn =
    fn(
        Arc<TaskCtx>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send>>;

struct Job {
    name: &'static str,
    period: Duration,
    first_after: Duration,
    run: JobFn,
}

fn jitter(max: Duration) -> Duration {
    let mut b = [0u8; 8];
    let _ = getrandom::getrandom(&mut b);
    let x = u64::from_le_bytes(b);
    if max.is_zero() {
        Duration::ZERO
    } else {
        Duration::from_millis(x % (max.as_millis() as u64).max(1))
    }
}

macro_rules! job {
    ($name:literal, $period:expr, $first:expr, $f:path) => {
        Job {
            name: $name,
            period: $period,
            first_after: $first,
            run: |c| Box::pin($f(c)),
        }
    };
}

const MIN: Duration = Duration::from_secs(60);
const HOUR: Duration = Duration::from_secs(3600);
const DAY: Duration = Duration::from_secs(24 * 3600);

fn jobs() -> Vec<Job> {
    vec![
        job!("budget_monitor", MIN, Duration::ZERO, budget_monitor),
        job!(
            "purges",
            Duration::from_secs(10),
            Duration::from_secs(5),
            purges
        ),
        job!("grace_expiry", HOUR, Duration::from_secs(120), grace_expiry),
        job!("tombstones", HOUR, Duration::from_secs(180), tombstones),
        job!("firehose_clock", HOUR, Duration::from_secs(240), clock),
        job!(
            "resync_expiry",
            HOUR,
            Duration::from_secs(300),
            resync_expiry
        ),
        job!(
            "admin_sessions",
            HOUR,
            Duration::from_secs(360),
            admin_sessions
        ),
        job!(
            "storage_metrics",
            Duration::from_secs(300),
            Duration::from_secs(30),
            storage_metrics
        ),
        job!(
            "deferred_retry",
            DAY,
            Duration::from_secs(600),
            deferred_retry
        ),
        job!(
            "placeholder_lists",
            DAY,
            Duration::from_secs(900),
            placeholder_lists
        ),
        job!("rate_tables", DAY, Duration::from_secs(960), rate_tables),
        job!(
            "history_retention",
            DAY,
            Duration::from_secs(1080),
            history_retention
        ),
        job!(
            "orphaned_cursors",
            DAY,
            Duration::from_secs(1020),
            orphaned_cursors
        ),
        job!(
            "counter_recount",
            DAY,
            Duration::from_secs(1200),
            counter_recount
        ),
        job!(
            "counter_rebuild",
            DAY,
            Duration::from_secs(1800),
            counter_rebuild
        ),
    ]
}

/// Runs the scheduler until `stop` flips.
pub async fn run_scheduler(ctx: Arc<TaskCtx>, mut stop: watch::Receiver<bool>) {
    let jobs = jobs();
    let start = Instant::now();
    let mut due: Vec<Instant> = jobs
        .iter()
        .map(|j| start + j.first_after + jitter(j.first_after / 4))
        .collect();
    let running: Vec<Arc<std::sync::atomic::AtomicBool>> = jobs
        .iter()
        .map(|_| Arc::new(std::sync::atomic::AtomicBool::new(false)))
        .collect();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = stop.changed() => {
                if *stop.borrow() { return; }
            }
        }
        let now = Instant::now();
        for (i, j) in jobs.iter().enumerate() {
            if now < due[i] || running[i].load(std::sync::atomic::Ordering::Relaxed) {
                continue;
            }
            due[i] = now + j.period + jitter(j.period / 20);
            running[i].store(true, std::sync::atomic::Ordering::Relaxed);
            let flag = running[i].clone();
            let ctx = ctx.clone();
            let name = j.name;
            let f = j.run;
            tokio::spawn(async move {
                let started = Instant::now();
                match f(ctx.clone()).await {
                    Ok(summary) if !summary.is_empty() => {
                        tracing::info!(
                            task = name,
                            took_ms = started.elapsed().as_millis() as u64,
                            "{summary}"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(task = name, error = %e, "periodic task failed");
                        let _ = farsight_storage::auth::record_op_error(
                            &ctx.pool,
                            &format!("task:{name}"),
                            None,
                            &e,
                        )
                        .await;
                    }
                }
                flag.store(false, std::sync::atomic::Ordering::Relaxed);
            });
        }
    }
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// Lists to fire GF on while a refusal is active: `pending`, not yet
/// claimed by a run, owned by a non-large (or unresolved) owner; at the
/// ceiling, every owner (§5.5, §11.2).
async fn gf_candidates(pool: &PgPool, include_large: bool) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT l.id FROM lists l
         JOIN actors o ON o.id = l.owner_id
         LEFT JOIN pds_hosts h ON h.id = o.pds_host_id
         WHERE l.track_state = $1 AND l.fetch_run_id IS NULL
           AND ($2 OR NOT COALESCE(h.large, false))
         ORDER BY l.admitted_at NULLS LAST, l.id LIMIT 1000",
    )
    .bind(TrackState::Pending.code())
    .bind(include_large)
    .fetch_all(pool)
    .await
}

/// `deferred` lists deferred by `cause`, oldest first.
async fn go_candidates(
    pool: &PgPool,
    cause: DeferCause,
    limit: i64,
) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT id FROM lists WHERE track_state = $1 AND deferred_by = $2 AND listblock_count > 0
         ORDER BY admitted_at NULLS LAST, id LIMIT $3",
    )
    .bind(TrackState::Deferred.code())
    .bind(cause.code())
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// The budget monitor (§11.2, every minute): measures `pg_database_size`,
/// drives the gate state machine, publishes gates to every writer, records
/// global refusal intervals, fires **GF** on unclaimed pending lists while
/// refusing and **GO** on lists the gate deferred once it reopens.
async fn budget_monitor(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let cfg = ctx.config.current();
    let budget = Budget {
        budget_bytes: cfg.config.storage.budget_bytes,
        ceiling_bytes: cfg.config.storage.effective_hard_ceiling(),
    };
    let bytes = gates::measure_database_bytes(&ctx.pool)
        .await
        .map_err(err)?;
    let prev = *ctx.gate_state.lock().unwrap_or_else(|e| e.into_inner());
    let next = gates::next_gate_state(prev, bytes, budget);
    ctx.gates.store(next.gates);
    *ctx.gate_state.lock().unwrap_or_else(|e| e.into_inner()) = next;
    let witness = firehose::read_state(&ctx.pool)
        .await
        .map_err(err)?
        .applied_through
        .unwrap_or_else(Utc::now);
    gates::record_refusal_transition(&ctx.pool, prev, next, witness)
        .await
        .map_err(err)?;
    let ratio = if budget.budget_bytes > 0 {
        bytes as f64 / budget.budget_bytes as f64
    } else {
        0.0
    };
    metrics::gauge!(STORAGE_DB_BYTES).set(bytes as f64);
    metrics::gauge!(STORAGE_BUDGET_RATIO).set(ratio);
    let limits = ctx.limits();
    let mut fired = 0usize;
    let refusing = next.gates.budget_refusing || next.gates.ceiling_refusing;
    if refusing {
        let cause = if next.gates.ceiling_refusing {
            DeferCause::Ceiling
        } else {
            DeferCause::Budget
        };
        for id in gf_candidates(&ctx.pool, next.gates.ceiling_refusing)
            .await
            .map_err(err)?
        {
            janitor::fire_event(
                &ctx.pool,
                &limits,
                &ctx.counters,
                id,
                Event::GateFail(cause),
                FireArgs::default(),
            )
            .await
            .map_err(err)?;
            fired += 1;
        }
    }
    let mut reopened = 0usize;
    for (cause, open) in [
        (DeferCause::Budget, !next.gates.budget_refusing),
        (DeferCause::Ceiling, !next.gates.ceiling_refusing),
    ] {
        if !open {
            continue;
        }
        for id in go_candidates(&ctx.pool, cause, GO_PER_PASS)
            .await
            .map_err(err)?
        {
            janitor::fire_event(
                &ctx.pool,
                &limits,
                &ctx.counters,
                id,
                Event::GateOpen,
                FireArgs::default(),
            )
            .await
            .map_err(err)?;
            reopened += 1;
        }
    }
    let growth = growth_warning(&ctx, bytes);
    ctx.status.update(|s| {
        s.db_bytes = Some(bytes);
        s.budget_bytes = budget.budget_bytes;
        s.ceiling_bytes = budget.ceiling_bytes;
        s.gate = next;
        s.measured_at = Some(Utc::now());
        s.growth_warning = growth;
    });
    if ratio >= 0.8 && prev != next {
        tracing::warn!(ratio, bytes, "storage budget ratio");
    }
    Ok(if fired + reopened > 0 {
        format!("gates {:?}: {fired} GF, {reopened} GO", next.gates)
    } else {
        String::new()
    })
}

/// Sustained growth above 2× the trailing average (§11.2). Samples are
/// hourly and in memory, so the check needs three days of uptime.
fn growth_warning(ctx: &TaskCtx, bytes: u64) -> Option<String> {
    let now = Utc::now();
    let mut s = ctx.samples.lock().unwrap_or_else(|e| e.into_inner());
    if s.back()
        .is_none_or(|(t, _)| now - *t >= chrono::Duration::hours(1))
    {
        s.push_back((now, bytes));
    }
    while s
        .front()
        .is_some_and(|(t, _)| now - *t > chrono::Duration::days(30))
    {
        s.pop_front();
    }
    let (first_t, first_b) = *s.front()?;
    let span_days = (now - first_t).num_hours() as f64 / 24.0;
    if span_days < 3.0 {
        return None;
    }
    let day_ago = s
        .iter()
        .find(|(t, _)| now - *t <= chrono::Duration::hours(25))?;
    let last_day = bytes.saturating_sub(day_ago.1) as f64;
    let avg = bytes.saturating_sub(first_b) as f64 / span_days;
    (avg > 0.0 && last_day > 2.0 * avg).then(|| {
        format!(
            "Storage grew {} in the last day, more than twice the trailing average ({}/day).",
            farsight_web::common::human_bytes(last_day as u64),
            farsight_web::common::human_bytes(avg as u64)
        )
    })
}

async fn purges(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let r = janitor::process_purges(&ctx.pool, &ctx.limits(), &ctx.counters, 100)
        .await
        .map_err(err)?;
    Ok(if r.items_deleted > 0 || !r.finished.is_empty() {
        format!(
            "purged {} items; {} lists finished",
            r.items_deleted,
            r.finished.len()
        )
    } else {
        String::new()
    })
}

async fn grace_expiry(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let r = janitor::expire_grace(&ctx.pool, &ctx.limits(), &ctx.counters, Utc::now())
        .await
        .map_err(err)?;
    Ok(if r.transitions.is_empty() {
        String::new()
    } else {
        format!("{} retained lists expired", r.transitions.len())
    })
}

async fn tombstones(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let ttl = ctx.config.current().config.storage.tombstone_ttl.get();
    let n = janitor::purge_tombstones(&ctx.pool, Utc::now(), ttl)
        .await
        .map_err(err)?;
    Ok(format!("{n} tombstones past TTL deleted"))
}

async fn clock(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let n = firehose::maintain_clock(&ctx.pool, Utc::now())
        .await
        .map_err(err)?;
    Ok(format!("{n} firehose_clock rows thinned or expired"))
}

async fn resync_expiry(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let n = debts::expire_resyncs(&ctx.pool, Utc::now(), RESYNC_TERMINAL)
        .await
        .map_err(err)?;
    Ok(if n > 0 {
        format!("{n} resync debts became unreachable")
    } else {
        String::new()
    })
}

async fn admin_sessions(ctx: Arc<TaskCtx>) -> Result<String, String> {
    farsight_storage::auth::expire_sessions(
        &ctx.pool,
        farsight_web::pages::SESSION_IDLE,
        farsight_web::pages::SESSION_ABSOLUTE,
    )
    .await
    .map_err(err)?;
    Ok(String::new())
}

async fn storage_metrics(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let mut history_bytes = 0u64;
    for t in TABLES {
        let n: Option<i64> = sqlx::query_scalar("SELECT pg_total_relation_size(to_regclass($1))")
            .bind(t)
            .fetch_one(&ctx.pool)
            .await
            .map_err(err)?;
        metrics::gauge!(STORAGE_TABLE_BYTES, "table" => t).set(n.unwrap_or(0) as f64);
        if t.ends_with("_history") {
            history_bytes += n.unwrap_or(0).max(0) as u64;
        }
    }
    // The dashboard shows history next to the budget (§11.2).
    ctx.status.update(|s| s.history_bytes = Some(history_bytes));
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT name, COALESCE(sum(value), 0)::bigint FROM stats_counters GROUP BY name",
    )
    .fetch_all(&ctx.pool)
    .await
    .map_err(err)?;
    for (name, v) in rows {
        let collection = match name.as_str() {
            "blocks" => "block",
            "list_blocks" => "listblock",
            "lists" => "list",
            "list_items" => "listitem",
            _ => continue,
        };
        metrics::gauge!(RECORDS, "collection" => collection).set(v as f64);
    }
    Ok(String::new())
}

async fn deferred_retry(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let r = janitor::retry_deferred(&ctx.pool, &ctx.limits(), &ctx.counters, Utc::now())
        .await
        .map_err(err)?;
    Ok(format!("{} deferred lists retried", r.transitions.len()))
}

async fn placeholder_lists(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let mut total = 0;
    loop {
        let n = janitor::cleanup_placeholder_lists(&ctx.pool, 10_000)
            .await
            .map_err(err)?;
        total += n;
        if n == 0 {
            break;
        }
    }
    Ok(format!("{total} placeholder lists deleted"))
}

async fn rate_tables(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let n = janitor::drop_old_rates(&ctx.pool, Utc::now().date_naive())
        .await
        .map_err(err)?;
    Ok(format!("{n} rate rows older than 2 days deleted"))
}

/// The daily history retention pass (§7.7). With `"0s"` it does not run.
async fn history_retention(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let retention = ctx
        .config
        .current()
        .config
        .storage
        .block_history_retention
        .get();
    if retention.is_zero() {
        return Ok(String::new());
    }
    let r = history::prune(&ctx.pool, Utc::now(), retention)
        .await
        .map_err(err)?;
    Ok(format!(
        "history past retention deleted: {} blocks, {} listblocks, {} listitems, {} windows",
        r.rows[0], r.rows[1], r.rows[2], r.windows
    ))
}

async fn orphaned_cursors(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let n = janitor::drop_orphaned_cursors(&ctx.pool)
        .await
        .map_err(err)?;
    Ok(format!("{n} orphaned cursor rows deleted"))
}

/// Nightly exact recount (§4.2, §7.1): batched, each list under its list
/// lock; repairs drift, re-runs the transition function where a repair
/// crosses zero, and alerts on any drift.
async fn counter_recount(ctx: Arc<TaskCtx>) -> Result<String, String> {
    let limits = ctx.limits();
    let mut drift = Vec::new();
    let mut checked = 0u64;
    let mut after = 0i64;
    loop {
        let (r, last) =
            recount::recount_lists(&ctx.pool, &limits, &ctx.counters, after, RECOUNT_BATCH)
                .await
                .map_err(err)?;
        checked += r.checked;
        drift.extend(r.drift);
        match last {
            Some(l) => after = l,
            None => break,
        }
    }
    let mut after = 0i64;
    loop {
        let (r, last) = recount::recount_actors(&ctx.pool, after, RECOUNT_BATCH)
            .await
            .map_err(err)?;
        checked += r.checked;
        drift.extend(r.drift);
        match last {
            Some(l) => after = l,
            None => break,
        }
    }
    if !drift.is_empty() {
        let sample: Vec<String> = drift
            .iter()
            .take(10)
            .map(|d| format!("{}#{} {}→{}", d.column, d.id, d.stored, d.actual))
            .collect();
        let msg = format!(
            "counter drift repaired in {} rows: {}",
            drift.len(),
            sample.join(", ")
        );
        tracing::warn!(drift = drift.len(), "{msg}");
        let _ = farsight_storage::auth::record_op_error(&ctx.pool, "recount", None, &msg).await;
    }
    Ok(format!("{checked} rows recounted, {} drifted", drift.len()))
}

/// Nightly exact rebuild of the approximate counters (§7.1).
async fn counter_rebuild(ctx: Arc<TaskCtx>) -> Result<String, String> {
    recount::rebuild_approximate_counters(&ctx.pool, 100_000)
        .await
        .map_err(err)?;
    Ok("approximate counters rebuilt".into())
}
