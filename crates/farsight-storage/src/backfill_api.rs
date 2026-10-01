//! On-demand backfill at the API layer (design §3.3, §5.3, §5.6):
//! `requestBackfill` writes `backfill_queue` (the backfill process drains
//! it) and `getBackfillStatus` reads job state without ever enqueueing.

use chrono::{DateTime, Utc};
use farsight_core::Did;
use sqlx::{PgConnection, PgPool};

use crate::counters::{CounterSink, Deltas};
use crate::error::Result;
use crate::keys::Limits;
use crate::queue::{self, Enqueued, JobKind};
use crate::repo_events::priority;
use crate::txn::{Cause, Gates, Refusal, Txn};

/// Waiting entries one requester may hold (§5.3; beyond it `QueueFull`).
pub const REQUESTER_QUEUE_CAP: i64 = 10_000;
/// Waiting `high` entries one requester may hold (§5.3); beyond it a
/// `high` request is downgraded.
pub const REQUESTER_HIGH_CAP: i64 = 100;
/// Tier of on-demand jobs (§5.3).
pub const TIER_ON_DEMAND: i16 = 1;

/// `backfill_state.state` codes.
pub mod state {
    /// Never backfilled.
    pub const NEVER: i16 = 0;
    /// Waiting in the queue.
    pub const QUEUED: i16 = 1;
    /// A repo job holds the lease.
    pub const RUNNING: i16 = 2;
    /// Finished (clean, complete-with-debts or inactive).
    pub const DONE: i16 = 3;
    /// Failed.
    pub const FAILED: i16 = 4;
}

/// Repo job state as reported (§3.3, open enum).
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
    /// `covered_by_sweep`: no per-DID row (D4) but a completed baseline
    /// covers the repo.
    CoveredBySweep,
}

impl RepoState {
    /// Wire name.
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
    /// State.
    pub state: RepoState,
    /// Waiting entries of the same tier ahead of this one (queued only).
    pub position: Option<i64>,
    /// Last finish, or the covering cycle's completion.
    pub last_backfilled_at: Option<DateTime<Utc>>,
    /// Last error.
    pub last_error: Option<String>,
}

/// Discovery state as reported (§3.3, open enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryState {
    /// No backlink source configured.
    Disabled,
    /// Never run.
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
    /// Wire name.
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
    /// State.
    pub state: DiscoveryState,
    /// Completion time.
    pub completed_at: Option<DateTime<Utc>>,
    /// Truncated at `max_refs`.
    pub truncated: bool,
    /// Backlink source of the last run.
    pub source: Option<String>,
}

/// The `getBackfillStatus` body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillStatus {
    /// Repo job.
    pub repo: RepoStatus,
    /// Subject discovery.
    pub discovery: DiscoveryStatus,
}

/// Reads the status of `did` (pure read, §3.3). `discovery_enabled` is
/// whether `backfill.backlinks.url` is set.
pub async fn status(
    conn: &mut PgConnection,
    did: &str,
    discovery_enabled: bool,
) -> Result<BackfillStatus> {
    let baseline: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT completed_at FROM sweep_cycles WHERE kind = 1 AND completed_at IS NOT NULL
         ORDER BY completed_at DESC LIMIT 1",
    )
    .fetch_optional(&mut *conn)
    .await?;
    type Row = (
        i64,
        Option<i16>,
        Option<DateTime<Utc>>,
        Option<String>,
        bool,
        bool,
    );
    let row: Option<Row> =
        sqlx::query_as(
            "SELECT a.id, s.state, s.backfilled_at, s.last_error,
                    EXISTS (SELECT 1 FROM backfill_queue q WHERE q.actor_id = a.id AND q.kind = 1),
                    EXISTS (SELECT 1 FROM job_leases j WHERE j.did = a.did AND j.lease_until > now())
             FROM actors a LEFT JOIN backfill_state s ON s.actor_id = a.id
             WHERE a.did = $1",
        )
        .bind(did)
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
        Some((id, st, at, err, queued, leased)) => {
            let running = st == Some(state::RUNNING) && leased;
            let state = if running {
                RepoState::Running
            } else if queued {
                RepoState::Queued
            } else {
                match st {
                    Some(state::QUEUED) => RepoState::Queued,
                    Some(state::RUNNING) => RepoState::Running,
                    Some(state::DONE) => RepoState::Done,
                    Some(state::FAILED) => RepoState::Failed,
                    Some(_) => RepoState::Never,
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
        let d: Option<(i16, Option<DateTime<Utc>>, bool, String)> = match actor_id {
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
                    1 => DiscoveryState::Queued,
                    2 => DiscoveryState::Running,
                    3 => DiscoveryState::Done,
                    4 => DiscoveryState::Failed,
                    _ => DiscoveryState::Never,
                },
                completed_at,
                truncated,
                source: Some(source),
            },
        }
    };
    Ok(BackfillStatus { repo, discovery })
}

/// Who asked: charged as a cause (interning, §11.2) and as a requester
/// (fairness and caps, §5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requester {
    /// `token:<id>` or `admin` (`backfill_queue.requester` and the intern
    /// cause key).
    pub key: String,
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
    /// `force`.
    pub force: bool,
    /// The requester.
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
        /// New work was added.
        enqueued: bool,
        /// `high` was downgraded.
        downgraded: bool,
    },
    /// The requester holds [`REQUESTER_QUEUE_CAP`] waiting entries.
    QueueFull,
    /// Interning the actor was refused by the requester's daily intern
    /// rate (§11.2).
    InternRefused,
}

/// Applies the four rules of §3.3 in one transaction:
/// already queued ⇒ no new work (the waiting entry is upgraded by the
/// collapse rule); running ⇒ no new work unless `force`, which adds a
/// waiting entry; done within the fresh window and not `force` ⇒ no new
/// work; otherwise enqueue a tier-1 repo job (plus a discovery job when a
/// backlink source is configured, §5.6).
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
    t.lock_new_dids(&[did]).await?;
    let cause = Cause {
        key: req.requester.key.clone(),
        buckets: Vec::new(),
        large: true,
        mask: 0,
    };
    let actor_id = match t.intern_actor(req.actor, &cause).await? {
        Ok(id) => id,
        Err(Refusal::Capped(_) | Refusal::Refused(_)) => return Ok(RequestOutcome::InternRefused),
    };
    let conn: &mut PgConnection = t.conn;
    let (st, done_at, queued, leased): (Option<i16>, Option<DateTime<Utc>>, bool, bool) =
        sqlx::query_as(
            "SELECT (SELECT state FROM backfill_state WHERE actor_id = $1),
                    (SELECT backfilled_at FROM backfill_state WHERE actor_id = $1 AND state = 3),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE actor_id = $1 AND kind = 1),
                    EXISTS (SELECT 1 FROM job_leases WHERE did = $2 AND lease_until > now())",
        )
        .bind(actor_id)
        .bind(did)
        .fetch_one(&mut *conn)
        .await?;
    let running = st == Some(state::RUNNING) && leased;
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
    let mut prio = priority::NORMAL;
    if req.high {
        if !req.requester.may_high {
            downgraded = true;
        } else {
            let highs: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM (SELECT 1 FROM backfill_queue
                   WHERE requester = $1 AND priority = $2 LIMIT $3) x",
            )
            .bind(&req.requester.key)
            .bind(priority::HIGH)
            .bind(REQUESTER_HIGH_CAP)
            .fetch_one(&mut *conn)
            .await?;
            if highs >= REQUESTER_HIGH_CAP && !queued {
                downgraded = true;
            } else {
                prio = priority::HIGH;
            }
        }
    }
    let new_work = if queued {
        // Rule 1: already queued. The collapse rule may still raise the
        // waiting entry's tier or priority (§5.3).
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
        TIER_ON_DEMAND,
        prio,
        &req.requester.key,
        Some(REQUESTER_QUEUE_CAP),
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
    // D4: requested DIDs get a backfill_state row.
    sqlx::query(
        "INSERT INTO backfill_state (actor_id, state) VALUES ($1, 1)
         ON CONFLICT (actor_id) DO UPDATE SET state = 1 WHERE backfill_state.state <> 2",
    )
    .bind(actor_id)
    .execute(&mut *conn)
    .await?;
    if let Some(source) = req.discovery_source {
        let r = queue::enqueue(
            conn,
            actor_id,
            JobKind::Discovery,
            TIER_ON_DEMAND,
            prio,
            &req.requester.key,
            Some(REQUESTER_QUEUE_CAP),
        )
        .await?;
        if r == Enqueued::Waiting {
            sqlx::query(
                "INSERT INTO discovery_state (actor_id, state, source) VALUES ($1, 1, $2)
                 ON CONFLICT (actor_id) DO UPDATE SET state = 1, source = EXCLUDED.source
                   WHERE discovery_state.state <> 2",
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
/// under the scheduler's order (§5.3): within its requester, `high` before
/// `normal` 4:1, each kind oldest first; across requesters, deficit
/// round-robin, estimated with equal per-job cost (each other requester
/// is served about as many entries as this one's rank). Entries not yet
/// due (`not_before` in the future) are not ahead.
async fn queue_position(conn: &mut PgConnection, actor_id: i64) -> Result<Option<i64>> {
    let me: Option<(i16, String, i16, DateTime<Utc>, i64)> = sqlx::query_as(
        "SELECT tier, requester, priority, enqueued_at, id FROM backfill_queue
         WHERE actor_id = $1 AND kind = 1",
    )
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
         WHERE tier = $1 AND requester = $2 AND id <> $5
           AND (not_before IS NULL OR not_before <= now())",
    )
    .bind(tier)
    .bind(&requester)
    .bind(prio)
    .bind(at)
    .bind(qid)
    .fetch_one(&mut *conn)
    .await?;
    let rank = rank_in_requester(prio == priority::HIGH, same_ahead, other_kind);
    let others: Vec<i64> = sqlx::query_scalar(
        "SELECT count(*) FROM backfill_queue
         WHERE tier = $1 AND requester <> $2 AND (not_before IS NULL OR not_before <= now())
         GROUP BY requester",
    )
    .bind(tier)
    .bind(&requester)
    .fetch_all(&mut *conn)
    .await?;
    Ok(Some(
        rank + others.iter().map(|n| (*n).min(rank)).sum::<i64>(),
    ))
}

/// Entries of its own requester served before an entry with `same_ahead`
/// entries of its priority ahead and `other_kind` of the other priority
/// waiting (four `high` per `normal`, §5.3).
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
