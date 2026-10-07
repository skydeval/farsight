//! Non-commit firehose events (see `docs/design/firehose.md`):
//! `identity`, `account` and `#sync`, applied inside the batch
//! transaction so the persisted cursor never runs ahead of their
//! effects.
//!
//! | Event | Effect |
//! |---|---|
//! | `identity` | known DID ⇒ PDS cache cleared (`pds_resolved_at = NULL`) |
//! | `account` | known DID ⇒ status set from the event; `desynchronized` ⇒ `resync` debt; **any** DID becoming active: known with `inactive_at_listing` or a hidden status ⇒ `resync` debt + **OA** on its `unavailable` lists whose list lock the transaction holds; unknown ⇒ no row, no job (counted in the report); `deleted` ⇒ purge after commit |
//! | `#sync` | any DID ⇒ `resync` debt + tier-1 `system:resync` re-list |

use chrono::{DateTime, Utc};
use farsight_core::Did;

use crate::codes::{ActorStatus, DebtReason, Priority, RequesterKey, Tier, TrackState};
use crate::error::Result;
use crate::ids::{ActorId, ListId};
use crate::queue::{self, JobKind};
use crate::tracking::FireArgs;
use crate::transition::Event;
use crate::txn::Txn;

/// A non-commit event for one repo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoEvent {
    /// `identity`: handle or DID document changed.
    Identity {
        /// The repo whose handle or DID document changed.
        did: Did,
        /// Witness time of the event.
        witness: DateTime<Utc>,
    },
    /// `account`: status change.
    Account {
        /// The account whose status changed.
        did: Did,
        /// Witness time of the event. It is stored as the account's
        /// `status_at`, and an event older than the stored one is skipped.
        witness: DateTime<Utc>,
        /// The event's `active` flag.
        active: bool,
        /// Upstream status when inactive.
        status: Option<String>,
    },
    /// `#sync` (v2 only): the repo's commit chain broke; re-list it.
    Sync {
        /// The repo to list again.
        did: Did,
        /// Witness time of the event; the `resync` debt it leaves is
        /// cleared only by a run whose coverage point is at or after it.
        witness: DateTime<Utc>,
    },
}

impl RepoEvent {
    /// The repo the event is about, whichever kind it is.
    pub fn did(&self) -> &Did {
        match self {
            RepoEvent::Identity { did, .. }
            | RepoEvent::Account { did, .. }
            | RepoEvent::Sync { did, .. } => did,
        }
    }

    /// Whether this event can fire **OA** (an `account` event with
    /// `active = true`); the batch then locks the DID's unavailable lists.
    pub fn may_reactivate(&self) -> bool {
        matches!(self, RepoEvent::Account { active: true, .. })
    }
}

/// The stored status code for an upstream account event.
pub fn status_code(active: bool, status: Option<&str>) -> ActorStatus {
    if active {
        ActorStatus::Active
    } else {
        match status {
            None => ActorStatus::Unknown,
            Some(s) => ActorStatus::from_upstream(Some(s)),
        }
    }
}

/// Owner keys (DID, list rkey) of a DID's `unavailable` lists, for lock
/// discovery. Read-only.
pub async fn unavailable_list_keys(t: &mut Txn<'_>, did: &Did) -> Result<Vec<(ListId, String)>> {
    Ok(sqlx::query_as(
        "SELECT l.id, l.rkey FROM lists l JOIN actors a ON a.id = l.owner_id
         WHERE a.did = $1 AND l.track_state = $2",
    )
    .bind(did.as_str())
    .bind(TrackState::Unavailable.code())
    .fetch_all(&mut *t.conn)
    .await?)
}

impl Txn<'_> {
    /// Applies one non-commit event. The caller holds author(did) and,
    /// for reactivations, list(L) exclusive on the DID's unavailable
    /// lists, and the intern lock if the DID has no `actors` row.
    ///
    /// **OA** fires only on lists whose exclusive lock this transaction
    /// holds. A list can turn `unavailable` under its list lock alone (a
    /// list job's timeout), so the set read here may hold a list the
    /// caller did not lock; that one turned `unavailable` after the
    /// caller fixed its lock set, is ordered after this event, and keeps
    /// its own retry.
    pub async fn apply_repo_event(&mut self, ev: &RepoEvent, system_queue_cap: i64) -> Result<()> {
        match ev {
            RepoEvent::Identity { did, .. } => {
                // The account's handle may have changed: the handle pass
                // checks it ahead of its walk. Only accounts Farsight
                // holds are noted.
                sqlx::query(
                    "WITH a AS (UPDATE actors SET pds_resolved_at = NULL WHERE did = $1
                                RETURNING did)
                     INSERT INTO handle_due (did) SELECT did FROM a
                     ON CONFLICT (did) DO UPDATE SET asked_at = now()",
                )
                .bind(did.as_str())
                .execute(&mut *self.conn)
                .await?;
                self.report.repo_events += 1;
            }
            RepoEvent::Sync { did, witness } => {
                // Any DID: the author's own row is never refused.
                let author = self.author(did).await?;
                self.add_debt(author.id, DebtReason::Resync, None, Some(*witness))
                    .await?;
                queue::enqueue(
                    &mut *self.conn,
                    author.id,
                    JobKind::Repo,
                    Tier::OnDemand,
                    Priority::Normal,
                    RequesterKey::Resync,
                    Some(system_queue_cap),
                )
                .await?;
                self.report.repo_events += 1;
                self.report.resyncs += 1;
            }
            RepoEvent::Account {
                did,
                witness,
                active,
                status,
            } => {
                let new = status_code(*active, status.as_deref());
                let known: Option<(ActorId, ActorStatus, bool, Option<DateTime<Utc>>)> =
                    sqlx::query_as(
                        "SELECT a.id, a.status, COALESCE(s.inactive_at_listing, false), a.status_at
                     FROM actors a LEFT JOIN backfill_state s ON s.actor_id = a.id
                     WHERE a.did = $1",
                    )
                    .bind(did.as_str())
                    .fetch_optional(&mut *self.conn)
                    .await?;
                self.report.repo_events += 1;
                let Some((id, old, inactive_at_listing, status_at)) = known else {
                    // An unknown DID becoming active gets no row and no
                    // job, only a metric. Its first authored indexed record
                    // interns it and enqueues the tier-2 job (see `apply`).
                    if *active {
                        self.report.unknown_activations += 1;
                    }
                    return Ok(());
                };
                // Account events carry no rev: order them by witness time so
                // a replayed (older) status never overwrites a newer one and
                // replays are idempotent.
                if status_at.is_some_and(|at| *witness < at) {
                    self.report.stale_repo_events += 1;
                    return Ok(());
                }
                sqlx::query("UPDATE actors SET status = $2, status_at = $3 WHERE id = $1")
                    .bind(id)
                    .bind(new)
                    .bind(*witness)
                    .execute(&mut *self.conn)
                    .await?;
                if new != old {
                    self.notify = true;
                }
                if new == ActorStatus::Desynchronized && old != ActorStatus::Desynchronized {
                    self.add_debt(id, DebtReason::Resync, None, Some(*witness))
                        .await?;
                    self.report.resyncs += 1;
                }
                if *active && (old.is_hidden() || inactive_at_listing) {
                    self.add_debt(id, DebtReason::Resync, None, Some(*witness))
                        .await?;
                    self.report.resyncs += 1;
                    let lists: Vec<(ListId, String)> = sqlx::query_as(
                        "SELECT id, rkey FROM lists WHERE owner_id = $1 AND track_state = $2
                         ORDER BY id",
                    )
                    .bind(id)
                    .bind(TrackState::Unavailable.code())
                    .fetch_all(&mut *self.conn)
                    .await?;
                    for (list_id, rkey) in lists {
                        let key = crate::keys::list_lock_key(did.as_str(), &rkey);
                        if !self.holds_list_exclusive(key) {
                            continue;
                        }
                        self.fire(list_id, Event::OwnerActive, FireArgs::default())
                            .await?;
                    }
                }
                if new == ActorStatus::Deleted && old != ActorStatus::Deleted {
                    self.report.deleted_accounts.push(did.clone());
                }
            }
        }
        Ok(())
    }
}

/// A poisoned event: an event that failed 3 times on its own. Logs it
/// to `op_errors`, raises a `resync` debt for its DID (counted in
/// `pendingResyncs`) and enqueues a tier-1 `system:resync` re-list. The
/// debt turns into `unreachable` after 7 days without a clean run
/// ([`crate::debts::expire_resyncs`]).
pub async fn record_poisoned(
    pool: &sqlx::PgPool,
    limits: &crate::keys::Limits,
    counters: &crate::counters::CounterSink,
    did: &Did,
    message: &str,
    witness: DateTime<Utc>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let deltas = {
        let mut t = Txn::start(&mut tx, limits, crate::txn::Gates::default()).await?;
        t.lock_authors(
            &[crate::keys::author_lock_key(did.as_str())]
                .into_iter()
                .collect(),
        )
        .await?;
        t.lock_new_dids(&[did]).await?;
        let author = t.author(did).await?;
        sqlx::query("INSERT INTO op_errors (component, did, message) VALUES ('ingest', $1, $2)")
            .bind(did.as_str())
            .bind(message)
            .execute(&mut *t.conn)
            .await?;
        t.add_debt(author.id, DebtReason::Resync, None, Some(witness))
            .await?;
        queue::enqueue(
            &mut *t.conn,
            author.id,
            JobKind::Repo,
            Tier::OnDemand,
            Priority::Normal,
            RequesterKey::Resync,
            Some(limits.system_queue_cap),
        )
        .await?;
        t.send_notify().await?;
        t.finish().1
    };
    tx.commit().await?;
    counters.add(deltas);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes() {
        assert_eq!(status_code(true, None), ActorStatus::Active);
        assert_eq!(status_code(true, Some("deactivated")), ActorStatus::Active);
        assert_eq!(
            status_code(false, Some("takendown")),
            ActorStatus::Takendown
        );
        assert_eq!(
            status_code(false, Some("throttled")),
            ActorStatus::Throttled
        );
        assert_eq!(status_code(false, Some("weird")), ActorStatus::Unknown);
        assert_eq!(status_code(false, None), ActorStatus::Unknown);
    }
}
