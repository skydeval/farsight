//! `backfill_queue` enqueueing with the collapse rule (see
//! `docs/design/backfill.md`).
//!
//! The backfill stage owns picking and running jobs; this module only
//! adds or upgrades *waiting* entries, which ingest needs for `#sync`,
//! poisoned events and newly active unknown DIDs (see
//! `docs/design/firehose.md`).

use sqlx::PgConnection;

use crate::error::Result;

/// `backfill_queue.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobKind {
    /// Full repo job.
    Repo,
    /// List fetch run for an owner.
    ListFetch,
    /// Subject discovery.
    Discovery,
}

impl JobKind {
    /// Stored code.
    pub fn code(self) -> i16 {
        match self {
            JobKind::Repo => 1,
            JobKind::ListFetch => 2,
            JobKind::Discovery => 3,
        }
    }
}

/// Requester of `system:resync` re-lists.
pub const SYSTEM_RESYNC: &str = "system:resync";
/// Requester of tier-2 active-author jobs enqueued by ingest.
pub const SYSTEM_FIREHOSE: &str = "system:firehose";

/// Tier-2 cap (overflow dropped; the sweep covers it).
pub const TIER2_CAP: i64 = 1_000_000;

/// What happened to an enqueue request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enqueued {
    /// A waiting entry now exists (inserted or upgraded).
    Waiting,
    /// Dropped: the requester's or tier's cap is reached. For system
    /// requesters the durable source of truth (a debt) remains.
    CapReached,
}

/// Adds or upgrades the waiting entry for `(actor, kind)` (the collapse
/// rule): tier := most urgent, `not_before` := earliest (NULL = now),
/// requester := that of the more urgent request, priority := higher.
/// `cap` bounds the number of waiting entries of `requester` (system
/// requesters, `backfill.system_queue_cap`) — checked only when a new
/// entry would be inserted.
pub async fn enqueue(
    conn: &mut PgConnection,
    actor_id: i64,
    kind: JobKind,
    tier: i16,
    priority: i16,
    requester: &str,
    cap: Option<i64>,
) -> Result<Enqueued> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM backfill_queue WHERE actor_id = $1 AND kind = $2)",
    )
    .bind(actor_id)
    .bind(kind.code())
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
        if tier == 2 {
            let n: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM (SELECT 1 FROM backfill_queue WHERE tier = 2 LIMIT $1) x",
            )
            .bind(TIER2_CAP)
            .fetch_one(&mut *conn)
            .await?;
            if n >= TIER2_CAP {
                return Ok(Enqueued::CapReached);
            }
        }
    }
    sqlx::query(
        "INSERT INTO backfill_queue (actor_id, kind, tier, priority, requester, not_before)
         VALUES ($1, $2, $3, $4, $5, NULL)
         ON CONFLICT (actor_id, kind) DO UPDATE SET
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
    .bind(kind.code())
    .bind(tier)
    .bind(priority)
    .bind(requester)
    .execute(&mut *conn)
    .await?;
    Ok(Enqueued::Waiting)
}
