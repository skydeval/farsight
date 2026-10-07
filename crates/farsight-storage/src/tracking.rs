//! Applying list transitions (see `docs/design/list-indexing.md`) to
//! `lists` rows, and `list_sched_keys` maintenance (see
//! `docs/design/backfill.md`).
//!
//! Every function here must run under list(L) **exclusive**; the
//! transition function itself is pure (`crate::transition`).

use crate::codes::sql::TRACK_ONLY_UNAVAILABLE;
use crate::ids::{ActorId, ListId};
use chrono::{DateTime, Utc};

use crate::codes::{DeferCause, RecordState, TrackState};
use crate::error::{Result, StorageError};
use crate::transition::{Ctx, Effect, Event, ListFacts, Outcome, transition};
use crate::txn::{TransitionRecord, Txn};

/// Extra inputs some events carry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FireArgs {
    /// OK: the run's coverage point (`fetched_witness`).
    pub run_point: Option<DateTime<Utc>>,
    /// OK on a refresh: whether any item was refused (keeps `capped`).
    pub items_refused: bool,
}

/// The tracking columns of one `lists` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTracking {
    /// `lists.id`.
    pub id: ListId,
    /// `owner_id`: the account whose repository holds the list.
    pub owner_id: ActorId,
    /// `track_state`: where the list is in the tracking state machine.
    pub state: TrackState,
    /// `record_state`: whether the list's own record has been seen, and
    /// whether it was deleted.
    pub record_state: RecordState,
    /// `listblock_count`: the counted listblocks that name the list. Exact.
    pub listblock_count: i32,
    /// `purge_then`: the state the list enters when its purge is done.
    /// Meaningful only while `purging`.
    pub purge_then: Option<TrackState>,
}

fn decode_state(code: i16) -> Result<TrackState> {
    TrackState::from_code(code)
        .ok_or_else(|| StorageError::Invariant(format!("unknown track_state {code}")))
}

impl Txn<'_> {
    /// Reads a list's tracking columns, locking the row.
    pub async fn list_tracking(&mut self, list_id: ListId) -> Result<ListTracking> {
        let (owner_id, state, record_state, count, purge_then): (
            ActorId,
            i16,
            i16,
            i32,
            Option<i16>,
        ) = sqlx::query_as(
            "SELECT owner_id, track_state, record_state, listblock_count, purge_then
                 FROM lists WHERE id = $1 FOR UPDATE",
        )
        .bind(list_id)
        .fetch_one(&mut *self.conn)
        .await?;
        Ok(ListTracking {
            id: list_id,
            owner_id,
            state: decode_state(state)?,
            record_state: RecordState::from_code(record_state).unwrap_or(RecordState::Unknown),
            listblock_count: count,
            purge_then: match purge_then {
                Some(c) => Some(decode_state(c)?),
                None => None,
            },
        })
    }

    async fn owner_readmit_available(&mut self, owner_id: ActorId) -> Result<bool> {
        let (day, count): (Option<chrono::NaiveDate>, i32) =
            sqlx::query_as("SELECT readmit_day, readmit_count FROM actors WHERE id = $1")
                .bind(owner_id)
                .fetch_one(&mut *self.conn)
                .await?;
        let limit = i64::from(self.limits.cfg.owner_readmissions_per_day);
        // A day other than today (or none yet) means nothing used today.
        let used = if day == Some(self.today) {
            i64::from(count)
        } else {
            0
        };
        Ok(used < limit)
    }

    /// Fires `event` on list `list_id` and applies the outcome. The caller
    /// holds list(L) exclusive.
    pub async fn fire(&mut self, list_id: ListId, event: Event, args: FireArgs) -> Result<Outcome> {
        let l = self.list_tracking(list_id).await?;
        let facts = ListFacts {
            state: l.state,
            record_state: l.record_state,
            listblock_count: l.listblock_count,
            purge_then: l.purge_then,
        };
        let ctx = Ctx {
            owner_readmit_available: self.owner_readmit_available(l.owner_id).await?,
        };
        let o = transition(&facts, event, &ctx);
        if !o.changed {
            return Ok(o);
        }
        let purge_then = if o.state == TrackState::Purging {
            o.purge_then
        } else {
            None
        };
        sqlx::query("UPDATE lists SET track_state = $2, purge_then = $3 WHERE id = $1")
            .bind(list_id)
            .bind(o.state.code())
            .bind(purge_then.map(TrackState::code))
            .execute(&mut *self.conn)
            .await?;
        let mut admitted = false;
        for effect in &o.effects {
            match *effect {
                Effect::Admit => {
                    admitted = true;
                    self.admit(list_id, l.owner_id).await?;
                }
                Effect::ChargeOwnerReadmission => {
                    sqlx::query(
                        "UPDATE actors SET
                           readmit_count = CASE WHEN readmit_day = $2 THEN readmit_count + 1
                                                ELSE 1 END,
                           readmit_day = $2
                         WHERE id = $1",
                    )
                    .bind(l.owner_id)
                    .bind(self.today)
                    .execute(&mut *self.conn)
                    .await?;
                }
                Effect::Defer(cause) => {
                    let retry = if cause == DeferCause::OwnerReadmissions {
                        self.today
                            .succ_opt()
                            .and_then(|d| d.and_hms_opt(0, 0, 0))
                            .map(|d| d.and_utc())
                    } else {
                        None
                    };
                    sqlx::query(
                        "UPDATE lists SET deferred_by = $2, next_retry_at = $3 WHERE id = $1",
                    )
                    .bind(list_id)
                    .bind(cause.code())
                    .bind(retry)
                    .execute(&mut *self.conn)
                    .await?;
                }
                Effect::BeginPurge => {
                    sqlx::query(
                        "UPDATE lists SET fetch_run_id = NULL, fetch_run_epoch = NULL WHERE id = $1",
                    )
                    .bind(list_id)
                    .execute(&mut *self.conn)
                    .await?;
                }
                Effect::PurgeFinished => {
                    sqlx::query("UPDATE lists SET capped = false WHERE id = $1")
                        .bind(list_id)
                        .execute(&mut *self.conn)
                        .await?;
                }
                Effect::Promote => {
                    sqlx::query(
                        "UPDATE lists SET fetched_at = now(), fetched_witness = $2,
                           fetch_attempts = 0, fetch_run_id = NULL, fetch_run_epoch = NULL,
                           next_retry_at = NULL
                         WHERE id = $1",
                    )
                    .bind(list_id)
                    .bind(args.run_point)
                    .execute(&mut *self.conn)
                    .await?;
                }
                Effect::RefreshDone => {
                    sqlx::query(
                        "UPDATE lists SET refresh_requested = false,
                           fetched_witness = COALESCE($2, fetched_witness),
                           capped = capped AND $3,
                           fetch_run_id = NULL, fetch_run_epoch = NULL
                         WHERE id = $1",
                    )
                    .bind(list_id)
                    .bind(args.run_point)
                    .bind(args.items_refused)
                    .execute(&mut *self.conn)
                    .await?;
                }
                Effect::StartGrace => {
                    sqlx::query(
                        "UPDATE lists SET retain_until = now() + $2 * interval '1 second'
                         WHERE id = $1",
                    )
                    .bind(list_id)
                    .bind(self.limits.cfg.list_grace.get().as_secs_f64())
                    .execute(&mut *self.conn)
                    .await?;
                }
                Effect::ClearGrace => {
                    sqlx::query("UPDATE lists SET retain_until = NULL WHERE id = $1")
                        .bind(list_id)
                        .execute(&mut *self.conn)
                        .await?;
                }
                Effect::StayRetry => {}
            }
        }
        if !admitted {
            if !o.state.is_waiting() {
                self.clear_waiting(list_id).await?;
            } else if !l.state.is_waiting() {
                self.rebuild_sched_keys(list_id).await?;
            }
        }
        if o.state != TrackState::Deferred
            && o.state != TrackState::Missing
            && o.state != TrackState::Purging
            && !admitted
        {
            // Leaving the retry-bearing states clears their bookkeeping.
            sqlx::query(&format!(
                "UPDATE lists SET deferred_by = NULL, next_retry_at = NULL
                 WHERE id = $1 AND (deferred_by IS NOT NULL OR next_retry_at IS NOT NULL)
                   AND track_state NOT IN {TRACK_ONLY_UNAVAILABLE}"
            ))
            .bind(list_id)
            .execute(&mut *self.conn)
            .await?;
        }
        self.notify = true;
        self.report.transitions.push(TransitionRecord {
            list_id,
            event,
            from: l.state,
            to: o.state,
            effects: o.effects.clone(),
        });
        Ok(o)
    }

    async fn admit(&mut self, list_id: ListId, owner_id: ActorId) -> Result<()> {
        let epoch: i32 = sqlx::query_scalar(
            "UPDATE lists SET admit_epoch = admit_epoch + 1, admitted_at = now(),
               fetch_run_id = NULL, fetch_run_epoch = NULL, fetched_at = NULL,
               retain_until = NULL, deferred_by = NULL, next_retry_at = NULL,
               phase1_attempts = 0, fetch_attempts = 0
             WHERE id = $1 RETURNING admit_epoch",
        )
        .bind(list_id)
        .fetch_one(&mut *self.conn)
        .await?;
        // Phase 1 always runs for a new epoch.
        sqlx::query(
            "INSERT INTO list_jobs (list_id, owner_id, admit_epoch) VALUES ($1, $2, $3)
             ON CONFLICT (list_id) DO UPDATE SET owner_id = EXCLUDED.owner_id,
               admit_epoch = EXCLUDED.admit_epoch, enqueued_at = now(), not_before = NULL",
        )
        .bind(list_id)
        .bind(owner_id)
        .bind(epoch)
        .execute(&mut *self.conn)
        .await?;
        self.rebuild_sched_keys(list_id).await
    }

    /// Drops a list's phase-1 queue row and scheduling lanes (it stopped
    /// waiting).
    pub async fn clear_waiting(&mut self, list_id: ListId) -> Result<()> {
        sqlx::query("DELETE FROM list_jobs WHERE list_id = $1")
            .bind(list_id)
            .execute(&mut *self.conn)
            .await?;
        sqlx::query("DELETE FROM list_sched_keys WHERE list_id = $1")
            .bind(list_id)
            .execute(&mut *self.conn)
            .await?;
        Ok(())
    }

    /// Recomputes a waiting list's lanes from its current counted
    /// listblocks, by each row's stored `sched_key`.
    pub async fn rebuild_sched_keys(&mut self, list_id: ListId) -> Result<()> {
        sqlx::query("DELETE FROM list_sched_keys WHERE list_id = $1")
            .bind(list_id)
            .execute(&mut *self.conn)
            .await?;
        sqlx::query(
            "INSERT INTO list_sched_keys (list_id, key, n)
             SELECT $1, sched_key, count(*)::INT FROM list_blocks
             WHERE list_id = $1 AND counted AND sched_key IS NOT NULL
             GROUP BY sched_key",
        )
        .bind(list_id)
        .execute(&mut *self.conn)
        .await?;
        Ok(())
    }

    /// Adjusts one lane by `delta` for a counted listblock change on a
    /// waiting list; a lane is removed at `n = 0`.
    pub async fn adjust_sched_key(&mut self, list_id: ListId, key: &str, delta: i32) -> Result<()> {
        if delta > 0 {
            sqlx::query(
                "INSERT INTO list_sched_keys (list_id, key, n) VALUES ($1, $2, $3)
                 ON CONFLICT (list_id, key) DO UPDATE SET n = list_sched_keys.n + EXCLUDED.n",
            )
            .bind(list_id)
            .bind(key)
            .bind(delta)
            .execute(&mut *self.conn)
            .await?;
        } else {
            sqlx::query("UPDATE list_sched_keys SET n = n + $3 WHERE list_id = $1 AND key = $2")
                .bind(list_id)
                .bind(key)
                .bind(delta)
                .execute(&mut *self.conn)
                .await?;
            sqlx::query("DELETE FROM list_sched_keys WHERE list_id = $1 AND key = $2 AND n <= 0")
                .bind(list_id)
                .bind(key)
                .execute(&mut *self.conn)
                .await?;
        }
        Ok(())
    }

    /// Applies a counted-listblock count change of `delta` on `list_id`
    /// (rows sharing one `sched_key`) under list(L) exclusive: updates
    /// `listblock_count`, the lane of `sched_key` while the list is
    /// waiting, and fires **+** / **−** on a 0 ↔ ≥ 1 crossing.
    pub async fn change_listblock_count(
        &mut self,
        list_id: ListId,
        delta: i32,
        sched_key: Option<&str>,
    ) -> Result<()> {
        let (count, state): (i32, i16) = sqlx::query_as(
            "UPDATE lists SET listblock_count = listblock_count + $2 WHERE id = $1
             RETURNING listblock_count, track_state",
        )
        .bind(list_id)
        .bind(delta)
        .fetch_one(&mut *self.conn)
        .await?;
        if count < 0 {
            return Err(StorageError::Invariant(format!(
                "listblock_count of list {list_id} went negative"
            )));
        }
        let state = decode_state(state)?;
        if delta > 0 && count == delta {
            // 0 → ≥ 1: `+`. An admission rebuilds lanes from all counted
            // rows (including this one), so no separate adjustment.
            self.fire(list_id, Event::Plus, FireArgs::default()).await?;
            let after = self.list_tracking(list_id).await?;
            if after.state.is_waiting() && !state.is_waiting() {
                return Ok(());
            }
            if after.state.is_waiting()
                && state.is_waiting()
                && let Some(k) = sched_key
            {
                self.adjust_sched_key(list_id, k, delta).await?;
            }
            return Ok(());
        }
        if state.is_waiting()
            && let Some(k) = sched_key
        {
            self.adjust_sched_key(list_id, k, delta).await?;
        }
        if delta < 0 && count == 0 {
            self.fire(list_id, Event::Minus, FireArgs::default())
                .await?;
        }
        Ok(())
    }
}
