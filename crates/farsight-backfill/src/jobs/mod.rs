//! The per-DID jobs (see `docs/design/backfill.md`) and what they share: the
//! one-per-DID lease, run outcomes and their bookkeeping.

pub mod discovery;
pub mod list_fetch;
pub mod list_phase1;
pub mod repo;

use chrono::{DateTime, Utc};
use farsight_core::Did;
use farsight_storage::codes::sql::{
    JOB_REPO, MEMBER_OUTSTANDING, MEMBER_TERMINAL, REPO_DONE, REPO_FAILED, RUN_FAILED, RUN_INACTIVE,
};
use farsight_storage::codes::{DebtReason, JobKind, Priority, RequesterKey, RunOutcome, Tier};
use farsight_storage::ids::{ActorId, Stamp};
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
    /// The account whose repo the job lists; also the key of the job's
    /// lease.
    pub did: Did,
    /// The tier the job was dispatched from; a retry is queued in it.
    pub tier: Tier,
    /// Who asked for the job.
    pub requester: RequesterKey,
}

impl JobReq {
    /// Admin-requested jobs run normally under the budget gate.
    pub fn admin(&self) -> bool {
        self.requester == RequesterKey::Admin
    }
}

/// Why a job's step failed. Its text is what the job's failure records
/// (`Outcome::Failed`).
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// A statement failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// The storage layer refused or failed.
    #[error(transparent)]
    Storage(#[from] farsight_storage::StorageError),
    /// A PDS, the relay or the backlink index could not be read.
    #[error(transparent)]
    Net(#[from] crate::net::NetError),
    /// The DID did not resolve.
    #[error(transparent)]
    Resolve(#[from] crate::resolve::ResolveError),
    /// Recording an account's status stopped. The text is the stop in
    /// its debug form.
    #[error("{0:?}")]
    Status(repo::Stop),
    /// A fetched record does not parse.
    #[error(transparent)]
    Record(#[from] farsight_core::record::RecordError),
    /// A record key is not valid.
    #[error(transparent)]
    RecordKey(#[from] farsight_core::aturi::RecordKeyError),
}

/// How a job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Listed and reconciled to the end with nothing refused: clears the
    /// debts raised up to the job's coverage point and sets
    /// `clean_witness`.
    Clean,
    /// Listed and reconciled to the end, but an insert was refused, a
    /// reconcile was skipped or listblocks stay uncounted; each shortfall
    /// is recorded as a debt and none is cleared.
    CompleteWithDebts,
    /// The relay confirms the account inactive; nothing was listed.
    Inactive,
    /// Failed (terminal after `backfill.terminal_after`).
    Failed {
        /// The failure's text, stored in `backfill_state.last_error`.
        error: String,
        /// Whether the DID has been failing for `backfill.terminal_after`
        /// or longer. Jobs return `false`; [`finish_repo`] sets it.
        terminal: bool,
    },
    /// The page bound was reached: continue from the cursor later (not a
    /// failure).
    Yielded,
    /// Another job holds the DID's lease: retried later.
    Busy,
}

impl Outcome {
    /// The `outcome` label of `farsight_backfill_repos_total`.
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
/// charge).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobResult {
    /// How the job ended.
    pub outcome: Outcome,
    /// Outbound requests the job made, whatever its outcome.
    pub cost: u64,
}

/// Takes the DID's lease; `false` if another job holds it.
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

/// Extends the lease to [`LEASE`] from now. Does nothing if `owner` no
/// longer holds it.
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

/// Deletes the lease if `owner` holds it. A failure is ignored: the lease
/// then runs out by itself.
pub async fn release_lease(pool: &PgPool, did: &str, owner: &str) {
    let _ = sqlx::query("DELETE FROM job_leases WHERE did = $1 AND lease_owner = $2")
        .bind(did)
        .bind(owner)
        .execute(pool)
        .await;
}

/// The DID's `actors.id`, creating the row (under its intern lock, charged
/// to its own key and never refused) when a job needs it: an inactive
/// outcome, a failure that must be queued, a debt.
pub async fn intern(ctx: &Ctx, did: &Did) -> Result<ActorId, farsight_storage::StorageError> {
    let limits = ctx.limits();
    let mut tx = ctx.pool.begin().await?;
    let (id, deltas) = {
        let mut t = Txn::start(&mut tx, &limits, Gates::default()).await?;
        t.lock_authors(&[keys::author_lock_key(did.as_str())].into_iter().collect())
            .await?;
        t.lock_new_dids(&[did]).await?;
        let a = t.author(did).await?;
        let (_, d) = t.finish();
        (a.id, d)
    };
    tx.commit().await?;
    ctx.counters.add(deltas);
    Ok(id)
}

/// `actors.id` of `did` without creating it.
pub async fn actor_id(pool: &PgPool, did: &str) -> Result<Option<ActorId>, sqlx::Error> {
    sqlx::query_scalar("SELECT id FROM actors WHERE did = $1")
        .bind(did)
        .fetch_optional(pool)
        .await
}

/// Database time (every coverage instant comes from the database).
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

/// Cycle membership: a clean, complete-with-debts or inactive job that
/// **started** after a running cycle's start deletes the DID's
/// outstanding row there; a terminal failure marks it terminal.
///
/// The row and the cycle's counter change in one statement, so a job cut
/// off at shutdown cannot leave a member settled and not counted.
pub async fn settle_membership(
    pool: &PgPool,
    did: &Did,
    job_start: DateTime<Utc>,
    terminal: bool,
) -> Result<(), sqlx::Error> {
    let sql = if terminal {
        format!(
            "WITH settled AS (
               UPDATE cycle_outstanding o SET state = {MEMBER_TERMINAL} FROM sweep_cycles c
               WHERE o.did = $1 AND o.state = {MEMBER_OUTSTANDING} AND c.id = o.cycle_id
                 AND c.completed_at IS NULL AND c.started_at < $2
               RETURNING o.cycle_id)
             UPDATE sweep_cycles SET failed_terminal = failed_terminal + 1
             WHERE id IN (SELECT cycle_id FROM settled)"
        )
    } else {
        "WITH settled AS (
           DELETE FROM cycle_outstanding o USING sweep_cycles c
           WHERE o.did = $1 AND c.id = o.cycle_id
             AND c.completed_at IS NULL AND c.started_at < $2
           RETURNING o.cycle_id)
         UPDATE sweep_cycles SET done = done + 1
         WHERE id IN (SELECT cycle_id FROM settled)"
            .to_owned()
    };
    sqlx::query(&sql)
        .bind(did.as_str())
        .bind(job_start)
        .execute(pool)
        .await?;
    Ok(())
}

/// What `finish_repo` writes.
#[derive(Debug, Clone)]
pub struct Finish<'a> {
    /// The job that ran: its DID, tier and requester.
    pub req: &'a JobReq,
    /// Its coverage point `clock(job start)`.
    pub point: Option<DateTime<Utc>>,
    /// Server time of the job start (cycle membership).
    pub job_start: DateTime<Utc>,
    /// The listing stamp `R`, if one was read.
    pub stamp: Option<Stamp>,
    /// How the job says it ended. [`finish_repo`] returns it with
    /// `terminal` decided for a failure.
    pub outcome: &'a Outcome,
}

/// The bookkeeping at the end of a repo-kind job, by outcome.
pub async fn finish_repo(
    ctx: &Ctx,
    f: &Finish<'_>,
) -> Result<Outcome, farsight_storage::StorageError> {
    let pool = &ctx.pool;
    let did = f.req.did.as_str();
    let mut outcome = f.outcome.clone();
    // Only DIDs Farsight holds data for, or that were requested (or
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
                    &format!("INSERT INTO backfill_state (actor_id, state, backfilled_at, backfilled_witness,
                        clean_witness, last_outcome, backfill_rev, attempts, inactive_at_listing)
                     VALUES ($1, {REPO_DONE}, now(), $2, CASE WHEN $3 THEN $2 END, $4, $5, 0, false)
                     ON CONFLICT (actor_id) DO UPDATE SET state = {REPO_DONE}, backfilled_at = now(),
                       backfilled_witness = $2,
                       clean_witness = CASE WHEN $3 THEN $2 ELSE backfill_state.clean_witness END,
                       last_outcome = $4, backfill_rev = COALESCE($5, backfill_state.backfill_rev),
                       attempts = 0, next_attempt_at = NULL, first_failed_at = NULL,
                       last_error = NULL, current_run_id = NULL, current_run_point = NULL,
                       inactive_at_listing = false"),
                )
                .bind(id)
                .bind(f.point)
                .bind(clean)
                .bind(if clean { RunOutcome::Clean } else { RunOutcome::CompleteWithDebts }.code())
                .bind(f.stamp)
                .execute(pool)
                .await?;
                if clean && let Some(p) = f.point {
                    farsight_storage::debts::clear_for_clean_run(pool, id, p).await?;
                }
                sqlx::query(&format!(
                    "DELETE FROM backfill_cursors WHERE actor_id = $1 AND job_kind = {JOB_REPO}"
                ))
                .bind(id)
                .execute(pool)
                .await?;
            }
            settle_membership(pool, &f.req.did, f.job_start, false).await?;
        }
        Outcome::Inactive => {
            let id = id.expect("interned above");
            sqlx::query(&format!(
                "INSERT INTO backfill_state (actor_id, state, backfilled_at, backfilled_witness,
                    last_outcome, attempts, inactive_at_listing)
                 VALUES ($1, {REPO_DONE}, now(), $2, {RUN_INACTIVE}, 0, true)
                 ON CONFLICT (actor_id) DO UPDATE SET state = {REPO_DONE}, backfilled_at = now(),
                   backfilled_witness = $2, last_outcome = {RUN_INACTIVE}, attempts = 0,
                   next_attempt_at = NULL, first_failed_at = NULL, last_error = NULL,
                   current_run_id = NULL, current_run_point = NULL, inactive_at_listing = true"
            ))
            .bind(id)
            .bind(f.point)
            .execute(pool)
            .await?;
            settle_membership(pool, &f.req.did, f.job_start, false).await?;
        }
        Outcome::Failed { error, terminal } => {
            let id = id.expect("interned above");
            let cfg = ctx.cfg();
            let (attempts, first): (i32, DateTime<Utc>) = sqlx::query_as(
                &format!("INSERT INTO backfill_state (actor_id, state, attempts, first_failed_at, last_error,
                    last_outcome)
                 VALUES ($1, {REPO_FAILED}, 1, now(), $2, {RUN_FAILED})
                 ON CONFLICT (actor_id) DO UPDATE SET state = {REPO_FAILED},
                   attempts = backfill_state.attempts + 1,
                   first_failed_at = COALESCE(backfill_state.first_failed_at, now()),
                   last_error = $2, last_outcome = {RUN_FAILED}, current_run_point = NULL
                 RETURNING attempts, first_failed_at"),
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
                settle_membership(pool, &f.req.did, f.job_start, true).await?;
            } else {
                // Retried with backoff through the queue (failed cycle
                // members are queued, which interns them).
                let mut conn = pool.acquire().await?;
                farsight_storage::queue::enqueue(
                    &mut conn,
                    id,
                    JobKind::Repo,
                    f.req.tier,
                    Priority::Normal,
                    f.req.requester,
                    None,
                )
                .await?;
                sqlx::query(&format!(
                    "UPDATE backfill_queue SET not_before = now() + make_interval(secs => $2)
                     WHERE actor_id = $1 AND kind = {JOB_REPO}"
                ))
                .bind(id)
                .bind(delay.as_secs_f64())
                .execute(&mut *conn)
                .await?;
            }
        }
        Outcome::Yielded | Outcome::Busy => {}
    }
    metrics::counter!(m::REPOS_TOTAL, "tier" => f.req.tier.label(), "outcome" => outcome.label())
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
