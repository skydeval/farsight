//! The per-DID jobs (design §5.2, §5.5, §5.6) and what they share: the
//! one-per-DID lease, run outcomes (§5.2.1) and their bookkeeping.

pub mod discovery;
pub mod list_fetch;
pub mod list_phase1;
pub mod repo;

use chrono::{DateTime, Utc};
use farsight_core::Did;
use farsight_storage::codes::{DebtReason, RunOutcome};
use farsight_storage::keys;
use farsight_storage::txn::{Gates, Txn};
use sqlx::PgPool;

use crate::ctx::Ctx;
use crate::metrics as m;

/// Lease length; renewed while a job runs.
pub const LEASE: std::time::Duration = std::time::Duration::from_secs(600);

/// What a job is for (who asked, which tier).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobReq {
    /// The repo.
    pub did: Did,
    /// Tier 1–3 (§5.3).
    pub tier: i16,
    /// `token:<id>`, `admin`, `system:lists`, `system:resync`,
    /// `system:firehose`, `system:sweep`, `system:repair`.
    pub requester: String,
}

impl JobReq {
    /// Admin-requested jobs run normally under the budget gate (§5.3).
    pub fn admin(&self) -> bool {
        self.requester == "admin"
    }
}

/// How a job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// §5.2.1 clean.
    Clean,
    /// §5.2.1 complete-with-debts.
    CompleteWithDebts,
    /// §5.2.1 inactive.
    Inactive,
    /// §5.2.1 failed (terminal after `backfill.terminal_after`).
    Failed {
        /// The error.
        error: String,
        /// Terminal now.
        terminal: bool,
    },
    /// The page bound was reached: continue from the cursor later (not a
    /// failure, §5.2 bounds).
    Yielded,
    /// Another job holds the DID's lease: retried later.
    Busy,
}

impl Outcome {
    /// Metric label.
    pub fn label(&self) -> &'static str {
        match self {
            Outcome::Clean => "clean",
            Outcome::CompleteWithDebts => "complete_with_debts",
            Outcome::Inactive => "inactive",
            Outcome::Failed { .. } => "failed",
            Outcome::Yielded => "yielded",
            Outcome::Busy => "busy",
        }
    }
}

/// A finished job: its outcome and cost in outbound requests (the DRR
/// charge, §5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobResult {
    /// Outcome.
    pub outcome: Outcome,
    /// Outbound requests made.
    pub cost: u64,
}

/// Takes the DID's lease; `false` if another job holds it (§5.2).
pub async fn acquire_lease(pool: &PgPool, did: &str, owner: &str) -> Result<bool, sqlx::Error> {
    let got: Option<String> = sqlx::query_scalar(
        "INSERT INTO job_leases (did, lease_owner, lease_until)
         VALUES ($1, $2, now() + make_interval(secs => $3))
         ON CONFLICT (did) DO UPDATE SET lease_owner = EXCLUDED.lease_owner,
           lease_until = EXCLUDED.lease_until
         WHERE job_leases.lease_until < now() OR job_leases.lease_owner = EXCLUDED.lease_owner
         RETURNING did",
    )
    .bind(did)
    .bind(owner)
    .bind(LEASE.as_secs_f64())
    .fetch_optional(pool)
    .await?;
    Ok(got.is_some())
}

/// Extends the lease.
pub async fn renew_lease(pool: &PgPool, did: &str, owner: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE job_leases SET lease_until = now() + make_interval(secs => $3)
         WHERE did = $1 AND lease_owner = $2",
    )
    .bind(did)
    .bind(owner)
    .bind(LEASE.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

/// Releases the lease.
pub async fn release_lease(pool: &PgPool, did: &str, owner: &str) {
    let _ = sqlx::query("DELETE FROM job_leases WHERE did = $1 AND lease_owner = $2")
        .bind(did)
        .bind(owner)
        .execute(pool)
        .await;
}

/// The DID's `actors.id`, creating the row (under its intern lock, charged
/// to its own key and never refused, §11.2) when a job needs it: an
/// inactive outcome, a failure that must be queued, a debt.
pub async fn intern(ctx: &Ctx, did: &Did) -> Result<i64, farsight_storage::StorageError> {
    let limits = ctx.limits();
    let mut tx = ctx.pool.begin().await?;
    let (id, deltas) = {
        let mut t = Txn::start(&mut tx, &limits, Gates::default()).await?;
        t.lock_authors(&[keys::author_lock_key(did.as_str())].into_iter().collect())
            .await?;
        t.lock_new_dids(&[did.as_str()]).await?;
        let a = t.author(did).await?;
        let (_, d) = t.finish();
        (a.id, d)
    };
    tx.commit().await?;
    ctx.counters.add(deltas);
    Ok(id)
}

/// `actors.id` of `did` without creating it.
pub async fn actor_id(pool: &PgPool, did: &str) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar("SELECT id FROM actors WHERE did = $1")
        .bind(did)
        .fetch_optional(pool)
        .await
}

/// Database time (every coverage instant comes from the database, §3.7.1).
pub async fn db_now(pool: &PgPool) -> Result<DateTime<Utc>, sqlx::Error> {
    sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
}

/// The backoff before retry `attempts` (1-based) from
/// `backfill.retry_schedule` (the last entry repeats).
pub fn retry_delay(
    schedule: &[farsight_core::ConfigDuration],
    attempts: i32,
) -> std::time::Duration {
    let i = (attempts.max(1) as usize - 1).min(schedule.len().saturating_sub(1));
    schedule
        .get(i)
        .map(|d| d.get())
        .unwrap_or(std::time::Duration::from_secs(3600))
}

/// Cycle membership (§5.4): a clean, complete-with-debts or inactive job
/// that **started** after a running cycle's start deletes the DID's
/// outstanding row there; a terminal failure marks it terminal.
pub async fn settle_membership(
    pool: &PgPool,
    did: &str,
    job_start: DateTime<Utc>,
    terminal: bool,
) -> Result<(), sqlx::Error> {
    if terminal {
        let cycles: Vec<i64> = sqlx::query_scalar(
            "UPDATE cycle_outstanding o SET state = 2 FROM sweep_cycles c
             WHERE o.did = $1 AND o.state = 1 AND c.id = o.cycle_id
               AND c.completed_at IS NULL AND c.started_at < $2
             RETURNING o.cycle_id",
        )
        .bind(did)
        .bind(job_start)
        .fetch_all(pool)
        .await?;
        if !cycles.is_empty() {
            sqlx::query(
                "UPDATE sweep_cycles SET failed_terminal = failed_terminal + 1 WHERE id = ANY($1)",
            )
            .bind(&cycles)
            .execute(pool)
            .await?;
        }
    } else {
        let cycles: Vec<i64> = sqlx::query_scalar(
            "DELETE FROM cycle_outstanding o USING sweep_cycles c
             WHERE o.did = $1 AND c.id = o.cycle_id
               AND c.completed_at IS NULL AND c.started_at < $2
             RETURNING o.cycle_id",
        )
        .bind(did)
        .bind(job_start)
        .fetch_all(pool)
        .await?;
        if !cycles.is_empty() {
            sqlx::query("UPDATE sweep_cycles SET done = done + 1 WHERE id = ANY($1)")
                .bind(&cycles)
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}

/// What `finish_repo` writes.
#[derive(Debug, Clone)]
pub struct Finish<'a> {
    /// The job.
    pub req: &'a JobReq,
    /// Its coverage point `clock(job start)` (§3.7.1).
    pub point: Option<DateTime<Utc>>,
    /// Server time of the job start (cycle membership).
    pub job_start: DateTime<Utc>,
    /// The listing stamp `R`, if one was read.
    pub stamp: Option<i64>,
    /// The outcome.
    pub outcome: &'a Outcome,
}

/// The §5.2 step 7 / §5.2.1 bookkeeping of a repo-kind job.
pub async fn finish_repo(
    ctx: &Ctx,
    f: &Finish<'_>,
) -> Result<Outcome, farsight_storage::StorageError> {
    let pool = &ctx.pool;
    let did = f.req.did.as_str();
    let mut outcome = f.outcome.clone();
    // D4: only DIDs Farsight holds data for, or that were requested (or
    // that need a row: inactive, failed), get `backfill_state`.
    let id = match (&outcome, actor_id(pool, did).await?) {
        (_, Some(id)) => Some(id),
        (Outcome::Inactive | Outcome::Failed { .. }, None) => Some(intern(ctx, &f.req.did).await?),
        _ => None,
    };
    match &mut outcome {
        Outcome::Clean | Outcome::CompleteWithDebts => {
            let clean = outcome == Outcome::Clean;
            if let Some(id) = id {
                sqlx::query(
                    "INSERT INTO backfill_state (actor_id, state, backfilled_at, backfilled_witness,
                        clean_witness, last_outcome, backfill_rev, attempts, inactive_at_listing)
                     VALUES ($1, 3, now(), $2, CASE WHEN $3 THEN $2 END, $4, $5, 0, false)
                     ON CONFLICT (actor_id) DO UPDATE SET state = 3, backfilled_at = now(),
                       backfilled_witness = $2,
                       clean_witness = CASE WHEN $3 THEN $2 ELSE backfill_state.clean_witness END,
                       last_outcome = $4, backfill_rev = COALESCE($5, backfill_state.backfill_rev),
                       attempts = 0, next_attempt_at = NULL, first_failed_at = NULL,
                       last_error = NULL, current_run_id = NULL, current_run_point = NULL,
                       inactive_at_listing = false",
                )
                .bind(id)
                .bind(f.point)
                .bind(clean)
                .bind(if clean { RunOutcome::Clean } else { RunOutcome::CompleteWithDebts }.code())
                .bind(f.stamp)
                .execute(pool)
                .await?;
                if clean {
                    if let Some(p) = f.point {
                        farsight_storage::debts::clear_for_clean_run(pool, id, p).await?;
                    }
                }
                sqlx::query("DELETE FROM backfill_cursors WHERE actor_id = $1 AND job_kind = 1")
                    .bind(id)
                    .execute(pool)
                    .await?;
            }
            settle_membership(pool, did, f.job_start, false).await?;
        }
        Outcome::Inactive => {
            let id = id.expect("interned above");
            sqlx::query(
                "INSERT INTO backfill_state (actor_id, state, backfilled_at, backfilled_witness,
                    last_outcome, attempts, inactive_at_listing)
                 VALUES ($1, 3, now(), $2, 3, 0, true)
                 ON CONFLICT (actor_id) DO UPDATE SET state = 3, backfilled_at = now(),
                   backfilled_witness = $2, last_outcome = 3, attempts = 0,
                   next_attempt_at = NULL, first_failed_at = NULL, last_error = NULL,
                   current_run_id = NULL, current_run_point = NULL, inactive_at_listing = true",
            )
            .bind(id)
            .bind(f.point)
            .execute(pool)
            .await?;
            settle_membership(pool, did, f.job_start, false).await?;
        }
        Outcome::Failed { error, terminal } => {
            let id = id.expect("interned above");
            let cfg = ctx.cfg();
            let (attempts, first): (i32, DateTime<Utc>) = sqlx::query_as(
                "INSERT INTO backfill_state (actor_id, state, attempts, first_failed_at, last_error,
                    last_outcome)
                 VALUES ($1, 4, 1, now(), $2, 4)
                 ON CONFLICT (actor_id) DO UPDATE SET state = 4,
                   attempts = backfill_state.attempts + 1,
                   first_failed_at = COALESCE(backfill_state.first_failed_at, now()),
                   last_error = $2, last_outcome = 4, current_run_point = NULL
                 RETURNING attempts, first_failed_at",
            )
            .bind(id)
            .bind(&*error)
            .fetch_one(pool)
            .await?;
            let now = db_now(pool).await?;
            let failing_for = (now - first).to_std().unwrap_or_default();
            *terminal = failing_for >= cfg.backfill.terminal_after.get();
            let delay = retry_delay(&cfg.backfill.retry_schedule, attempts);
            sqlx::query(
                "UPDATE backfill_state SET next_attempt_at = now() + make_interval(secs => $2)
                 WHERE actor_id = $1",
            )
            .bind(id)
            .bind(delay.as_secs_f64())
            .execute(pool)
            .await?;
            if *terminal {
                let witness = f.point.unwrap_or(now);
                farsight_storage::debts::add_debt(pool, id, DebtReason::Unreachable, None, witness)
                    .await?;
                settle_membership(pool, did, f.job_start, true).await?;
            } else {
                // Retried with backoff through the queue (§5.2, §5.4: failed
                // cycle members are queued, which interns them).
                let mut conn = pool.acquire().await?;
                farsight_storage::queue::enqueue(
                    &mut conn,
                    id,
                    farsight_storage::queue::JobKind::Repo,
                    f.req.tier,
                    farsight_storage::repo_events::priority::NORMAL,
                    &f.req.requester,
                    None,
                )
                .await?;
                sqlx::query(
                    "UPDATE backfill_queue SET not_before = now() + make_interval(secs => $2)
                     WHERE actor_id = $1 AND kind = 1",
                )
                .bind(id)
                .bind(delay.as_secs_f64())
                .execute(&mut *conn)
                .await?;
            }
        }
        Outcome::Yielded | Outcome::Busy => {}
    }
    metrics::counter!(m::REPOS_TOTAL, "tier" => f.req.tier.to_string(), "outcome" => outcome.label())
        .increment(1);
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use farsight_core::ConfigDuration;

    #[test]
    fn retry_schedule_repeats_last() {
        let s = [
            ConfigDuration::secs(3600),
            ConfigDuration::secs(6 * 3600),
            ConfigDuration::secs(86_400),
        ];
        assert_eq!(retry_delay(&s, 1).as_secs(), 3600);
        assert_eq!(retry_delay(&s, 2).as_secs(), 6 * 3600);
        assert_eq!(retry_delay(&s, 9).as_secs(), 86_400);
    }
}
