//! `backfill_queue`: enqueueing with the collapse rule, and the claims
//! the scheduler holds on the entries whose jobs run (see
//! `docs/design/backfill.md`).
//!
//! An entry waits (`claimed_by` NULL) or is claimed by the process that
//! runs its job. At most one entry per account and kind waits; a claimed
//! one is apart from it, so a request made while a job runs adds a
//! waiting entry of its own. A claim lasts [`CLAIM`] and is renewed while
//! the job runs; the entry is deleted when the job has ended. A claim
//! that runs out (the process was killed) is taken off by
//! [`release_expired`], and the entry waits again.
//!
//! Ingest only adds or upgrades waiting entries, which it needs for
//! `#sync`, poisoned events and newly active unknown DIDs (see
//! `docs/design/firehose.md`).

use std::time::Duration;

use crate::codes::sql::{JOB_REPO, REPO_QUEUED, REPO_RUNNING};
use crate::ids::{ActorId, QueueId};
use sqlx::{PgConnection, PgPool};

use crate::codes::{Priority, RequesterKey, Tier};
use crate::error::Result;

pub use crate::codes::JobKind;

/// Tier-2 cap (overflow dropped; the sweep covers it). Checked against
/// the planner's estimate of the queue's size, so it is approximate.
pub const TIER2_CAP: i64 = 1_000_000;

/// How long a claim lasts unless it is renewed.
pub const CLAIM: Duration = Duration::from_secs(600);

/// Jobs found stopped and queued again per [`requeue_stopped`] call.
pub const REQUEUE_BATCH: i64 = 500;

/// What happened to an enqueue request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enqueued {
    /// A waiting entry now exists (inserted or upgraded).
    Waiting,
    /// Dropped: the requester's or tier's cap is reached. For system
    /// requesters the durable source of truth (a debt) remains.
    CapReached,
}

/// Whether the planner's estimate of the rows in `backfill_queue` puts
/// tier 2 at its cap. `None` for a table that was never analysed (the
/// estimate is then negative): it has no estimate to go by.
pub fn tier2_full_by_estimate(estimate: f32) -> Option<bool> {
    (estimate >= 0.0).then(|| f64::from(estimate) >= TIER2_CAP as f64)
}

/// Adds or upgrades the waiting entry for `(actor, kind)` (the collapse
/// rule): tier := most urgent, `not_before` := earliest (NULL = now),
/// requester := that of the more urgent request, priority := higher.
/// `delay` is how long from now the request may first run; `None` is at
/// once. `cap` bounds the number of entries of `requester` (system
/// requesters, `backfill.system_queue_cap`), checked only when a new
/// entry would be inserted.
#[allow(clippy::too_many_arguments)]
pub async fn enqueue(
    conn: &mut PgConnection,
    actor_id: ActorId,
    kind: JobKind,
    tier: Tier,
    priority: Priority,
    requester: RequesterKey,
    cap: Option<i64>,
    delay: Option<Duration>,
) -> Result<Enqueued> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM backfill_queue
                        WHERE actor_id = $1 AND kind = $2 AND claimed_by IS NULL)",
    )
    .bind(actor_id)
    .bind(kind)
    .fetch_one(&mut *conn)
    .await?;
    if !exists {
        if let Some(cap) = cap {
            let n: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM (SELECT 1 FROM backfill_queue WHERE requester = $1
                                       LIMIT $2) x",
            )
            .bind(requester)
            .bind(cap)
            .fetch_one(&mut *conn)
            .await?;
            if n >= cap {
                return Ok(Enqueued::CapReached);
            }
        }
        if tier == Tier::Active {
            // Every new author on the firehose comes through here, inside
            // its batch's transaction and under that batch's locks: the
            // queue is not counted there. The planner's estimate is one
            // row read.
            let estimate: Option<f32> = sqlx::query_scalar(
                "SELECT reltuples FROM pg_class WHERE oid = 'backfill_queue'::regclass",
            )
            .fetch_optional(&mut *conn)
            .await?;
            let full = match estimate.and_then(tier2_full_by_estimate) {
                Some(full) => full,
                // Never analysed: the table has seen little, and counting
                // it is cheap.
                None => {
                    let n: i64 = sqlx::query_scalar(
                        "SELECT count(*) FROM (SELECT 1 FROM backfill_queue LIMIT $1) x",
                    )
                    .bind(TIER2_CAP)
                    .fetch_one(&mut *conn)
                    .await?;
                    n >= TIER2_CAP
                }
            };
            if full {
                return Ok(Enqueued::CapReached);
            }
        }
    }
    sqlx::query(
        "INSERT INTO backfill_queue (actor_id, kind, tier, priority, requester, not_before)
         VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
         ON CONFLICT (actor_id, kind) WHERE claimed_by IS NULL DO UPDATE SET
           requester = CASE
             WHEN EXCLUDED.tier < backfill_queue.tier
               OR (EXCLUDED.tier = backfill_queue.tier
                   AND EXCLUDED.priority > backfill_queue.priority)
             THEN EXCLUDED.requester ELSE backfill_queue.requester END,
           tier = LEAST(backfill_queue.tier, EXCLUDED.tier),
           priority = GREATEST(backfill_queue.priority, EXCLUDED.priority),
           not_before = CASE
             WHEN backfill_queue.not_before IS NULL OR EXCLUDED.not_before IS NULL THEN NULL
             ELSE LEAST(backfill_queue.not_before, EXCLUDED.not_before) END",
    )
    .bind(actor_id)
    .bind(kind)
    .bind(tier)
    .bind(priority)
    .bind(requester)
    .bind(delay.map(|d| d.as_secs_f64()))
    .execute(&mut *conn)
    .await?;
    Ok(Enqueued::Waiting)
}

/// Claims the waiting entry `id` for `owner`; `false` if it is gone or
/// claimed.
pub async fn claim(pool: &PgPool, id: QueueId, owner: &str) -> Result<bool> {
    let n = sqlx::query(
        "UPDATE backfill_queue SET claimed_by = $2,
           claimed_until = now() + make_interval(secs => $3)
         WHERE id = $1 AND claimed_by IS NULL",
    )
    .bind(id)
    .bind(owner)
    .bind(CLAIM.as_secs_f64())
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n > 0)
}

/// Extends `owner`'s claims on `ids` to [`CLAIM`] from now.
pub async fn renew(pool: &PgPool, ids: &[QueueId], owner: &str) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE backfill_queue SET claimed_until = now() + make_interval(secs => $3)
         WHERE id = ANY($1) AND claimed_by = $2",
    )
    .bind(ids)
    .bind(owner)
    .bind(CLAIM.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

/// Deletes the entry `id` if `owner` holds its claim: its job has ended.
pub async fn finish(pool: &PgPool, id: QueueId, owner: &str) -> Result<()> {
    sqlx::query("DELETE FROM backfill_queue WHERE id = $1 AND claimed_by = $2")
        .bind(id)
        .bind(owner)
        .execute(pool)
        .await?;
    Ok(())
}

/// Takes claims off entries so that they wait again: with an `owner`,
/// the entries it holds (only `id`, if one is named); without, every
/// entry whose claim has run out. With a `delay` an entry is not served
/// before that long from now. An entry whose account and kind already
/// have a waiting entry is deleted instead: the waiting one stands for
/// both. Returns how many wait again.
async fn unclaim(
    pool: &PgPool,
    owner: Option<&str>,
    id: Option<QueueId>,
    delay: Option<Duration>,
) -> Result<u64> {
    let n = sqlx::query(
        "WITH held AS (
           SELECT q.id, q.actor_id, q.kind FROM backfill_queue q
           WHERE q.claimed_by IS NOT NULL
             AND CASE WHEN $1::TEXT IS NULL THEN q.claimed_until < now()
                      ELSE q.claimed_by = $1 AND ($3::BIGINT IS NULL OR q.id = $3) END
           FOR UPDATE SKIP LOCKED),
         doubled AS (
           DELETE FROM backfill_queue q USING held h
           WHERE q.id = h.id AND EXISTS (
             SELECT 1 FROM backfill_queue w
             WHERE w.actor_id = h.actor_id AND w.kind = h.kind AND w.claimed_by IS NULL)
           RETURNING q.id)
         UPDATE backfill_queue q SET claimed_by = NULL, claimed_until = NULL,
           not_before = CASE WHEN $2::FLOAT8 IS NULL THEN q.not_before
                             ELSE now() + make_interval(secs => $2) END
         WHERE q.id IN (SELECT id FROM held) AND q.id NOT IN (SELECT id FROM doubled)",
    )
    .bind(owner)
    .bind(delay.map(|d| d.as_secs_f64()))
    .bind(id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n)
}

/// Gives the entry `id` back to the queue (`owner` holds its claim): it
/// waits again, `delay` from now at the earliest.
pub async fn release(pool: &PgPool, id: QueueId, owner: &str, delay: Duration) -> Result<()> {
    unclaim(pool, Some(owner), Some(id), Some(delay)).await?;
    Ok(())
}

/// Gives back every entry `owner` holds (the process is stopping). Returns
/// how many wait again.
pub async fn release_owned(pool: &PgPool, owner: &str) -> Result<u64> {
    unclaim(pool, Some(owner), None, None).await
}

/// Gives back the entries whose claim ran out: the process that held them
/// is gone, or lost its database for longer than a claim lasts. Returns
/// how many wait again.
pub async fn release_expired(pool: &PgPool) -> Result<u64> {
    unclaim(pool, None, None, None).await
}

/// Queues again the repo jobs a stopped process left behind: a
/// `backfill_state` row says `running`, yet no job holds the account's
/// lease, no queue entry exists for it and no open cycle has it as an
/// outstanding member (the scheduler dispatches those itself). The row
/// becomes `queued` and a tier-2 `system:resync` entry is added. Returns
/// how many were queued.
pub async fn requeue_stopped(pool: &PgPool) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let stopped: Vec<ActorId> = sqlx::query_scalar(&format!(
        "SELECT s.actor_id FROM backfill_state s JOIN actors a ON a.id = s.actor_id
         WHERE s.state = {REPO_RUNNING}
           AND NOT EXISTS (SELECT 1 FROM job_leases j
                           WHERE j.did = a.did AND j.lease_until > now())
           AND NOT EXISTS (SELECT 1 FROM backfill_queue q
                           WHERE q.actor_id = s.actor_id AND q.kind = {JOB_REPO})
           AND NOT EXISTS (SELECT 1 FROM cycle_outstanding o JOIN sweep_cycles c
                             ON c.id = o.cycle_id
                           WHERE o.did = a.did AND c.completed_at IS NULL)
         ORDER BY s.actor_id LIMIT $1
         FOR UPDATE OF s SKIP LOCKED"
    ))
    .bind(REQUEUE_BATCH)
    .fetch_all(&mut *tx)
    .await?;
    let mut n = 0;
    for id in stopped {
        sqlx::query(&format!(
            "UPDATE backfill_state SET state = {REPO_QUEUED} WHERE actor_id = $1"
        ))
        .bind(id)
        .execute(&mut *tx)
        .await?;
        enqueue(
            &mut tx,
            id,
            JobKind::Repo,
            Tier::Active,
            Priority::Normal,
            RequesterKey::Resync,
            None,
            None,
        )
        .await?;
        n += 1;
    }
    tx.commit().await?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_two_is_full_by_the_estimate_when_there_is_one() {
        assert_eq!(tier2_full_by_estimate(0.0), Some(false));
        assert_eq!(tier2_full_by_estimate(999_999.0), Some(false));
        assert_eq!(tier2_full_by_estimate(1_000_000.0), Some(true));
        assert_eq!(tier2_full_by_estimate(5.0e9), Some(true));
        // A table never analysed has no estimate: it is counted.
        assert_eq!(tier2_full_by_estimate(-1.0), None);
    }
}
