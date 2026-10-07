//! On-demand backfill at the API layer (see `docs/design/api.md` and
//! `docs/design/backfill.md`): `requestBackfill` writes `backfill_queue`
//! (the backfill process drains it) and `getBackfillStatus` reads job
//! state without ever enqueueing.

use crate::codes::sql::{
    CYCLE_FULL, DISCOVERY_QUEUED, DISCOVERY_RUNNING, JOB_REPO, REPO_DONE, REPO_QUEUED, REPO_RUNNING,
};
use crate::ids::{ActorId, QueueId};
use chrono::{DateTime, Utc};
use farsight_core::Did;
use sqlx::{PgConnection, PgPool};

use crate::codes::{BackfillState, DiscoveryRun, JobKind, Priority, RequesterKey, Tier};
use crate::counters::{CounterSink, Deltas};
use crate::error::Result;
use crate::keys::Limits;
use crate::queue::{self, Enqueued};
use crate::txn::{Cause, Gates, Refusal, Txn};

/// Waiting entries one requester may hold (beyond it `QueueFull`).
pub const REQUESTER_QUEUE_CAP: i64 = 10_000;
/// Waiting `high` entries one requester may hold; beyond it a `high`
/// request is downgraded.
pub const REQUESTER_HIGH_CAP: i64 = 100;
/// Repo job state as reported (open enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoState {
    /// `never`.
    Never,
    /// `queued`.
    Queued,
    /// `running`.
    Running,
    /// `done`.
    Done,
    /// `failed`.
    Failed,
    /// `covered_by_sweep`: no per-DID row but a completed baseline
    /// covers the repo.
    CoveredBySweep,
}

impl RepoState {
    /// The `repo.state` string of `getBackfillStatus`.
    pub fn api_name(self) -> &'static str {
        match self {
            RepoState::Never => "never",
            RepoState::Queued => "queued",
            RepoState::Running => "running",
            RepoState::Done => "done",
            RepoState::Failed => "failed",
            RepoState::CoveredBySweep => "covered_by_sweep",
        }
    }
}

/// `getBackfillStatus.repo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoStatus {
    /// Where the repo's job stands. A waiting queue entry outranks the
    /// stored state.
    pub state: RepoState,
    /// Waiting entries of the same tier ahead of this one (queued only).
    pub position: Option<i64>,
    /// Last finish, or the covering cycle's completion.
    pub last_backfilled_at: Option<DateTime<Utc>>,
    /// `backfill_state.last_error`, as stored.
    pub last_error: Option<String>,
}

/// Discovery state as reported (open enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryState {
    /// No backlink source configured.
    Disabled,
    /// No discovery was ever requested for the account.
    Never,
    /// Waiting.
    Queued,
    /// Running.
    Running,
    /// Finished.
    Done,
    /// Failed.
    Failed,
}

impl DiscoveryState {
    /// The `discovery.state` string of `getBackfillStatus`.
    pub fn api_name(self) -> &'static str {
        match self {
            DiscoveryState::Disabled => "disabled",
            DiscoveryState::Never => "never",
            DiscoveryState::Queued => "queued",
            DiscoveryState::Running => "running",
            DiscoveryState::Done => "done",
            DiscoveryState::Failed => "failed",
        }
    }
}

/// `getBackfillStatus.discovery`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryStatus {
    /// Where the account's discovery stands. Without a backlink source it
    /// is `Disabled`, whatever is stored.
    pub state: DiscoveryState,
    /// `discovery_state.completed_at`, as stored.
    pub completed_at: Option<DateTime<Utc>>,
    /// The last run stopped at `backfill.backlinks.max_refs` references and
    /// may have missed some.
    pub truncated: bool,
    /// Backlink source of the last run.
    pub source: Option<String>,
}

/// The `getBackfillStatus` body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillStatus {
    /// The listing of the account's own repository.
    pub repo: RepoStatus,
    /// The search for records in other repositories that name the account.
    pub discovery: DiscoveryStatus,
}

/// Reads the status of `did` (pure read). `discovery_enabled` is
/// whether `backfill.backlinks.url` is set.
pub async fn status(
    conn: &mut PgConnection,
    did: &Did,
    discovery_enabled: bool,
) -> Result<BackfillStatus> {
    let baseline: Option<DateTime<Utc>> = sqlx::query_scalar(
        &format!("SELECT completed_at FROM sweep_cycles WHERE kind = {CYCLE_FULL} AND completed_at IS NOT NULL
         ORDER BY completed_at DESC LIMIT 1"),
    )
    .fetch_optional(&mut *conn)
    .await?;
    type Row = (
        ActorId,
        Option<BackfillState>,
        Option<DateTime<Utc>>,
        Option<String>,
        bool,
        bool,
        bool,
    );
    let row: Option<Row> =
        sqlx::query_as(
            &format!("SELECT a.id, s.state, s.backfilled_at, s.last_error,
                    EXISTS (SELECT 1 FROM backfill_queue q WHERE q.actor_id = a.id AND q.kind = {JOB_REPO}
                              AND q.claimed_by IS NULL),
                    EXISTS (SELECT 1 FROM job_leases j WHERE j.did = a.did AND j.lease_until > now()),
                    EXISTS (SELECT 1 FROM backfill_queue q WHERE q.actor_id = a.id AND q.kind = {JOB_REPO}
                              AND q.claimed_until > now())
             FROM actors a LEFT JOIN backfill_state s ON s.actor_id = a.id
             WHERE a.did = $1"),
        )
        .bind(did.as_str())
        .fetch_optional(&mut *conn)
        .await?;
    let (repo, actor_id) = match row {
        None => (
            RepoStatus {
                state: if baseline.is_some() {
                    RepoState::CoveredBySweep
                } else {
                    RepoState::Never
                },
                position: None,
                last_backfilled_at: baseline,
                last_error: None,
            },
            None,
        ),
        Some((id, st, at, err, queued, leased, claimed)) => {
            // A job runs while the scheduler holds its queue entry, or
            // (a cycle member has no entry) while the stored state says
            // so and the account's lease is held.
            let running = claimed || (st == Some(BackfillState::Running) && leased);
            let state = if running {
                RepoState::Running
            } else if queued {
                RepoState::Queued
            } else {
                match st {
                    // `running` without a lease or a claim: the process
                    // that ran the job is gone, and the job waits to be
                    // taken up again.
                    Some(BackfillState::Queued | BackfillState::Running) => RepoState::Queued,
                    Some(BackfillState::Done) => RepoState::Done,
                    Some(BackfillState::Failed) => RepoState::Failed,
                    Some(BackfillState::Never) => RepoState::Never,
                    None if baseline.is_some() => RepoState::CoveredBySweep,
                    None => RepoState::Never,
                }
            };
            let position = if state == RepoState::Queued && queued {
                queue_position(conn, id).await?
            } else {
                None
            };
            let last = if st.is_none() && state == RepoState::CoveredBySweep {
                baseline
            } else {
                at
            };
            (
                RepoStatus {
                    state,
                    position,
                    last_backfilled_at: last,
                    last_error: err,
                },
                Some(id),
            )
        }
    };
    let discovery = if !discovery_enabled {
        DiscoveryStatus {
            state: DiscoveryState::Disabled,
            completed_at: None,
            truncated: false,
            source: None,
        }
    } else {
        let d: Option<(DiscoveryRun, Option<DateTime<Utc>>, bool, String)> = match actor_id {
            None => None,
            Some(id) => {
                sqlx::query_as(
                    "SELECT state, completed_at, truncated, source FROM discovery_state
                     WHERE actor_id = $1",
                )
                .bind(id)
                .fetch_optional(&mut *conn)
                .await?
            }
        };
        match d {
            None => DiscoveryStatus {
                state: DiscoveryState::Never,
                completed_at: None,
                truncated: false,
                source: None,
            },
            Some((st, completed_at, truncated, source)) => DiscoveryStatus {
                state: match st {
                    DiscoveryRun::Queued => DiscoveryState::Queued,
                    DiscoveryRun::Running => DiscoveryState::Running,
                    DiscoveryRun::Done => DiscoveryState::Done,
                    DiscoveryRun::Failed => DiscoveryState::Failed,
                },
                completed_at,
                truncated,
                source: Some(source),
            },
        }
    };
    Ok(BackfillStatus { repo, discovery })
}

/// Who asked: charged as a cause (interning) and as a requester
/// (fairness and caps).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requester {
    /// `token:<id>` or `admin` (`backfill_queue.requester` and the intern
    /// cause key).
    pub key: RequesterKey,
    /// May request `high` (`backfill:high` scope or the admin token).
    pub may_high: bool,
}

/// A `requestBackfill` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request<'a> {
    /// The repo to back fill.
    pub actor: &'a Did,
    /// `priority = high` was asked for.
    pub high: bool,
    /// The call's `force`: add work even if the repo is running or was done
    /// within the fresh window.
    pub force: bool,
    /// Who asks: charged for interning the account, and held to the queue
    /// caps.
    pub requester: &'a Requester,
    /// `backfill.request_fresh_window`.
    pub fresh_window: std::time::Duration,
    /// `backfill.backlinks.url` when discovery is enabled.
    pub discovery_source: Option<&'a str>,
}

/// What `requestBackfill` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    /// Done; `enqueued` = new work was added, `downgraded` = `high` was
    /// lowered to `normal`.
    Ok {
        /// A repo job was added to the queue. False when one was already
        /// waiting, or the rules found no new work to do.
        enqueued: bool,
        /// `high` was asked for and not granted: the requester may not ask
        /// for it, or already holds [`REQUESTER_HIGH_CAP`] `high` entries.
        downgraded: bool,
    },
    /// The requester holds [`REQUESTER_QUEUE_CAP`] waiting entries.
    QueueFull,
    /// Interning the actor was refused by the requester's daily intern
    /// rate.
    InternRefused,
}

/// Applies the four rules of `requestBackfill` in one transaction:
/// already queued ⇒ no new work (the waiting entry is upgraded by the
/// collapse rule); running ⇒ no new work unless `force`, which adds a
/// waiting entry; done within the fresh window and not `force` ⇒ no new
/// work; otherwise enqueue a tier-1 repo job (plus a discovery job when a
/// backlink source is configured).
pub async fn request(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    gates: Gates,
    req: &Request<'_>,
) -> Result<RequestOutcome> {
    let mut tx = pool.begin().await?;
    let mut deltas = Deltas::default();
    let outcome = {
        let mut t = Txn::start(&mut tx, limits, gates).await?;
        let out = request_in(&mut t, req).await?;
        let (_, d) = t.finish();
        deltas.merge(d);
        out
    };
    tx.commit().await?;
    counters.add(deltas);
    Ok(outcome)
}

async fn request_in(t: &mut Txn<'_>, req: &Request<'_>) -> Result<RequestOutcome> {
    let did = req.actor.as_str();
    t.lock_new_dids(&[req.actor]).await?;
    let cause = Cause {
        key: req.requester.key.to_string(),
        buckets: Vec::new(),
        large: true,
        mask: 0,
    };
    let actor_id = match t.intern_actor(req.actor, &cause).await? {
        Ok(id) => id,
        Err(Refusal::Capped(_) | Refusal::Refused(_)) => return Ok(RequestOutcome::InternRefused),
    };
    let conn: &mut PgConnection = t.conn;
    type Standing = (
        Option<BackfillState>,
        Option<DateTime<Utc>>,
        bool,
        bool,
        bool,
    );
    let (st, done_at, queued, leased, claimed): Standing =
        sqlx::query_as(
            &format!("SELECT (SELECT state FROM backfill_state WHERE actor_id = $1),
                    (SELECT backfilled_at FROM backfill_state WHERE actor_id = $1 AND state = {REPO_DONE}),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE actor_id = $1 AND kind = {JOB_REPO}
                              AND claimed_by IS NULL),
                    EXISTS (SELECT 1 FROM job_leases WHERE did = $2 AND lease_until > now()),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE actor_id = $1 AND kind = {JOB_REPO}
                              AND claimed_until > now())"),
        )
        .bind(actor_id)
        .bind(did)
        .fetch_one(&mut *conn)
        .await?;
    let running = claimed || (st == Some(BackfillState::Running) && leased);
    let fresh = match done_at {
        Some(at) => {
            let now: DateTime<Utc> = sqlx::query_scalar("SELECT now()")
                .fetch_one(&mut *conn)
                .await?;
            (now - at).to_std().unwrap_or_default() < req.fresh_window
        }
        None => false,
    };
    let mut downgraded = false;
    let mut prio = Priority::Normal;
    if req.high {
        if !req.requester.may_high {
            downgraded = true;
        } else {
            let highs: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM (SELECT 1 FROM backfill_queue
                   WHERE requester = $1 AND priority = $2 LIMIT $3) x",
            )
            .bind(req.requester.key)
            .bind(Priority::High)
            .bind(REQUESTER_HIGH_CAP)
            .fetch_one(&mut *conn)
            .await?;
            if highs >= REQUESTER_HIGH_CAP && !queued {
                downgraded = true;
            } else {
                prio = Priority::High;
            }
        }
    }
    let new_work = if queued {
        // Rule 1: already queued. The collapse rule may still raise the
        // waiting entry's tier or priority.
        false
    } else if running {
        // Rule 2: a repo job holds the lease.
        req.force
    } else {
        // Rules 3 and 4.
        req.force || !fresh
    };
    if !(queued || new_work) {
        return Ok(RequestOutcome::Ok {
            enqueued: false,
            downgraded,
        });
    }
    let r = queue::enqueue(
        conn,
        actor_id,
        JobKind::Repo,
        Tier::OnDemand,
        prio,
        req.requester.key,
        Some(REQUESTER_QUEUE_CAP),
        None,
    )
    .await?;
    if r == Enqueued::CapReached {
        return Ok(RequestOutcome::QueueFull);
    }
    if !new_work {
        return Ok(RequestOutcome::Ok {
            enqueued: false,
            downgraded,
        });
    }
    // Requested DIDs get a backfill_state row.
    sqlx::query(
        &format!("INSERT INTO backfill_state (actor_id, state) VALUES ($1, {REPO_QUEUED})
         ON CONFLICT (actor_id) DO UPDATE SET state = {REPO_QUEUED} WHERE backfill_state.state <> {REPO_RUNNING}"),
    )
    .bind(actor_id)
    .execute(&mut *conn)
    .await?;
    if let Some(source) = req.discovery_source {
        let r = queue::enqueue(
            conn,
            actor_id,
            JobKind::Discovery,
            Tier::OnDemand,
            prio,
            req.requester.key,
            Some(REQUESTER_QUEUE_CAP),
            None,
        )
        .await?;
        if r == Enqueued::Waiting {
            sqlx::query(
                &format!("INSERT INTO discovery_state (actor_id, state, source) VALUES ($1, {DISCOVERY_QUEUED}, $2)
                 ON CONFLICT (actor_id) DO UPDATE SET state = {DISCOVERY_QUEUED}, source = EXCLUDED.source
                   WHERE discovery_state.state <> {DISCOVERY_RUNNING}"),
            )
            .bind(actor_id)
            .bind(source)
            .execute(&mut *conn)
            .await?;
        }
    }
    Ok(RequestOutcome::Ok {
        enqueued: true,
        downgraded,
    })
}

/// Waiting entries of the same tier served before `actor_id`'s repo entry
/// under the scheduler's order: within its requester, `high` before
/// `normal` 4:1, each kind oldest first; across requesters, deficit
/// round-robin, estimated with equal per-job cost (each other requester
/// is served about as many entries as this one's rank). Entries not yet
/// due (`not_before` in the future) are not ahead.
async fn queue_position(conn: &mut PgConnection, actor_id: ActorId) -> Result<Option<i64>> {
    let me: Option<(Tier, RequesterKey, Priority, DateTime<Utc>, QueueId)> =
        sqlx::query_as(&format!(
            "SELECT tier, requester, priority, enqueued_at, id FROM backfill_queue
         WHERE actor_id = $1 AND kind = {JOB_REPO} AND claimed_by IS NULL"
        ))
        .bind(actor_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some((tier, requester, prio, at, qid)) = me else {
        return Ok(None);
    };
    let (same_ahead, other_kind): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE priority = $3 AND (enqueued_at, id) < ($4, $5)),
                count(*) FILTER (WHERE priority <> $3)
         FROM backfill_queue
         WHERE tier = $1 AND requester = $2 AND id <> $5 AND claimed_by IS NULL
           AND (not_before IS NULL OR not_before <= now())",
    )
    .bind(tier)
    .bind(requester)
    .bind(prio)
    .bind(at)
    .bind(qid)
    .fetch_one(&mut *conn)
    .await?;
    let rank = rank_in_requester(prio == Priority::High, same_ahead, other_kind);
    let others: Vec<i64> = sqlx::query_scalar(
        "SELECT count(*) FROM backfill_queue
         WHERE tier = $1 AND requester <> $2 AND claimed_by IS NULL
           AND (not_before IS NULL OR not_before <= now())
         GROUP BY requester",
    )
    .bind(tier)
    .bind(requester)
    .fetch_all(&mut *conn)
    .await?;
    Ok(Some(
        rank + others.iter().map(|n| (*n).min(rank)).sum::<i64>(),
    ))
}

/// Entries of its own requester served before an entry with `same_ahead`
/// entries of its priority ahead and `other_kind` of the other priority
/// waiting (four `high` per `normal`).
fn rank_in_requester(high: bool, same_ahead: i64, other_kind: i64) -> i64 {
    let other_first = if high {
        same_ahead / 4
    } else {
        4 * (same_ahead + 1)
    };
    same_ahead + other_kind.min(other_first)
}

#[cfg(test)]
mod position_tests {
    use super::*;

    #[test]
    fn rank_follows_four_high_per_normal() {
        // Sequence HHHHN HHHHN …: the 1st normal is the 5th pick.
        assert_eq!(rank_in_requester(false, 0, 10), 4);
        assert_eq!(rank_in_requester(false, 0, 2), 2);
        assert_eq!(rank_in_requester(false, 1, 10), 9);
        // The 6th high comes after one normal.
        assert_eq!(rank_in_requester(true, 5, 3), 6);
        assert_eq!(rank_in_requester(true, 3, 3), 3);
    }
}
