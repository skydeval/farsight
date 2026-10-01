//! Janitor tasks (design §4.4 purge/GE, §5.2 cursors, §7.1 rate tables,
//! §7.3 tombstones, §7.4 account purge, §11.2 placeholder cleanup).
//!
//! Each function is one pass; the server schedules them (hourly
//! tombstones, nightly the rest, purges continuously). Time is injectable
//! (`now`) so the harness can test TTLs without waiting.

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use farsight_core::Did;
use sqlx::PgPool;

use crate::apply::{block_delete_row, item_delete_row, listblock_delete_row};
use crate::codes::TrackState;
use crate::counters::{CounterSink, stat};
use crate::error::{Result, StorageError};
use crate::keys::{self, CapKind, Limits};
use crate::tracking::FireArgs;
use crate::transition::Event;
use crate::txn::{ApplyReport, Gates, Txn};

/// Items deleted per purge transaction (§4.4 "10k-row batches").
pub const PURGE_BATCH: i64 = 10_000;

/// Deletes tombstones older than the TTL (§7.3; hourly). The TTL is
/// measured from `deleted_at`, the time the delete was processed.
pub async fn purge_tombstones(
    pool: &PgPool,
    now: DateTime<Utc>,
    ttl: std::time::Duration,
) -> Result<u64> {
    let cutoff =
        now - ChronoDuration::from_std(ttl).map_err(|e| StorageError::Invariant(e.to_string()))?;
    Ok(sqlx::query("DELETE FROM tombstones WHERE deleted_at < $1")
        .bind(cutoff)
        .execute(pool)
        .await?
        .rows_affected())
}

/// Drops `admission_rate` and `intern_rate` rows older than 2 days
/// (§7.1; nightly).
pub async fn drop_old_rates(pool: &PgPool, today: NaiveDate) -> Result<u64> {
    let cutoff = today - ChronoDuration::days(2);
    let a = sqlx::query("DELETE FROM admission_rate WHERE utc_day < $1")
        .bind(cutoff)
        .execute(pool)
        .await?
        .rows_affected();
    let i = sqlx::query("DELETE FROM intern_rate WHERE utc_day < $1")
        .bind(cutoff)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(a + i)
}

/// Deletes cursor rows of runs that are no longer current (§5.2; nightly).
pub async fn drop_orphaned_cursors(pool: &PgPool) -> Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM backfill_cursors c
         WHERE (c.job_kind = 1 AND NOT EXISTS (
                  SELECT 1 FROM backfill_state s
                  WHERE s.actor_id = c.actor_id AND s.current_run_id = c.run_id))
            OR (c.job_kind = 2 AND NOT EXISTS (
                  SELECT 1 FROM list_fetch_runs r
                  WHERE r.id = c.run_id AND r.finished_at IS NULL))",
    )
    .execute(pool)
    .await?
    .rows_affected())
}

const PLACEHOLDER_UNREFERENCED: &str = "
    l.record_state = 0 AND l.track_state = 0
    AND NOT EXISTS (SELECT 1 FROM list_blocks x WHERE x.list_id = l.id)
    AND NOT EXISTS (SELECT 1 FROM list_items x WHERE x.list_id = l.id)
    AND NOT EXISTS (SELECT 1 FROM subject_lists x WHERE x.list_id = l.id)
    AND NOT EXISTS (SELECT 1 FROM list_jobs x WHERE x.list_id = l.id)
    AND NOT EXISTS (SELECT 1 FROM list_sched_keys x WHERE x.list_id = l.id)";

/// Placeholder-list cleanup (§11.2; nightly): deletes `lists` rows with
/// `record_state = unknown`, `track_state = untracked` and no reference
/// from `list_blocks`, `list_items`, `subject_lists`, `list_jobs` or
/// `list_sched_keys`. Each deletion takes list(L) exclusive and re-checks
/// the conditions inside that transaction, so an apply about to reference
/// L either commits first (the re-check sees it) or runs after and
/// re-interns L. Returns rows deleted.
pub async fn cleanup_placeholder_lists(pool: &PgPool, limit: i64) -> Result<u64> {
    let candidates: Vec<(i64, String, String)> = sqlx::query_as(&format!(
        "SELECT l.id, a.did, l.rkey FROM lists l JOIN actors a ON a.id = l.owner_id
         WHERE {PLACEHOLDER_UNREFERENCED} ORDER BY l.id LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let mut deleted = 0;
    for (id, owner, rkey) in candidates {
        let mut tx = pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(keys::list_lock_key(&owner, &rkey))
            .execute(&mut *tx)
            .await?;
        deleted += sqlx::query(&format!(
            "DELETE FROM lists l WHERE l.id = $1 AND {PLACEHOLDER_UNREFERENCED}"
        ))
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
    }
    Ok(deleted)
}

async fn list_owner_key(pool: &PgPool, list_id: i64) -> Result<Option<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT a.did, l.rkey FROM lists l JOIN actors a ON a.id = l.owner_id WHERE l.id = $1",
    )
    .bind(list_id)
    .fetch_optional(pool)
    .await?)
}

/// Result of one purge pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PurgeReport {
    /// Items deleted.
    pub items_deleted: u64,
    /// Lists whose purge finished (PD fired).
    pub finished: Vec<i64>,
    /// Transitions fired.
    pub report: ApplyReport,
}

/// Runs one batch of every `purging` list (§4.4 purge→X): deletes up to
/// [`PURGE_BATCH`] items under author(owner) + list(L) exclusive,
/// re-checking `purging`; fires **PD** when none remain.
pub async fn process_purges(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    max_lists: i64,
) -> Result<PurgeReport> {
    let lists: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM lists WHERE track_state = $1 ORDER BY id LIMIT $2")
            .bind(TrackState::Purging.code())
            .bind(max_lists)
            .fetch_all(pool)
            .await?;
    let mut out = PurgeReport::default();
    for list_id in lists {
        let Some((owner, rkey)) = list_owner_key(pool, list_id).await? else {
            continue;
        };
        let owner_did = Did::parse(&owner).map_err(|e| StorageError::Invariant(e.to_string()))?;
        let mut tx = pool.begin().await?;
        let (report, deltas, deleted, finished) = {
            let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
            t.lock_authors(&[keys::author_lock_key(&owner)].into_iter().collect())
                .await?;
            t.lock_lists(
                &[(keys::list_lock_key(&owner, &rkey), true)]
                    .into_iter()
                    .collect(),
            )
            .await?;
            let state = t.list_tracking(list_id).await?.state;
            let mut deleted = 0u64;
            let mut finished = false;
            if state == TrackState::Purging {
                let author = t.author(&owner_did).await?;
                let rkeys: Vec<String> = sqlx::query_scalar(
                    "SELECT rkey FROM list_items WHERE list_id = $1 ORDER BY rkey LIMIT $2",
                )
                .bind(list_id)
                .bind(PURGE_BATCH)
                .fetch_all(&mut *t.conn)
                .await?;
                for rk in &rkeys {
                    if item_delete_row(&mut t, &author, rk).await? {
                        deleted += 1;
                    }
                }
                if (rkeys.len() as i64) < PURGE_BATCH {
                    t.fire(list_id, Event::PurgeDone, FireArgs::default())
                        .await?;
                    finished = true;
                }
            }
            if t.notify {
                t.send_notify().await?;
            }
            let (report, deltas) = t.finish();
            (report, deltas, deleted, finished)
        };
        tx.commit().await?;
        counters.add(deltas);
        out.items_deleted += deleted;
        if finished {
            out.finished.push(list_id);
        }
        out.report.transitions.extend(report.transitions);
    }
    Ok(out)
}

/// Fires **GE** on `retained` lists whose grace ended (§4.4).
pub async fn expire_grace(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    now: DateTime<Utc>,
) -> Result<ApplyReport> {
    let lists: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM lists WHERE track_state = $1 AND retain_until <= $2 ORDER BY id",
    )
    .bind(TrackState::Retained.code())
    .bind(now)
    .fetch_all(pool)
    .await?;
    let mut out = ApplyReport::default();
    for list_id in lists {
        let r = fire_event_guarded(
            pool,
            limits,
            counters,
            list_id,
            Event::GraceExpired,
            FireArgs::default(),
            Some(now),
        )
        .await?;
        out.transitions.extend(r.transitions);
    }
    Ok(out)
}

/// Fires one event on one list in its own transaction under list(L)
/// exclusive (used by the janitor, the backfill list jobs and tests).
pub async fn fire_event(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    list_id: i64,
    event: Event,
    args: FireArgs,
) -> Result<ApplyReport> {
    fire_event_guarded(pool, limits, counters, list_id, event, args, None).await
}

/// [`fire_event`], but with `grace_now = Some(now)` the event fires only if
/// the list is still `retained` with `retain_until <= now` (re-checked under
/// the lock, for GE).
async fn fire_event_guarded(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    list_id: i64,
    event: Event,
    args: FireArgs,
    grace_now: Option<DateTime<Utc>>,
) -> Result<ApplyReport> {
    let Some((owner, rkey)) = list_owner_key(pool, list_id).await? else {
        return Ok(ApplyReport::default());
    };
    let mut tx = pool.begin().await?;
    let (report, deltas) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_lists(
            &[(keys::list_lock_key(&owner, &rkey), true)]
                .into_iter()
                .collect(),
        )
        .await?;
        let proceed = match grace_now {
            Some(now) => {
                sqlx::query_scalar::<_, bool>(
                    "SELECT track_state = 3 AND retain_until IS NOT NULL AND retain_until <= $2
                 FROM lists WHERE id = $1",
                )
                .bind(list_id)
                .bind(now)
                .fetch_one(&mut *t.conn)
                .await?
            }
            None => true,
        };
        if proceed {
            t.fire(list_id, event, args).await?;
        }
        if t.notify {
            t.send_notify().await?;
        }
        t.finish()
    };
    tx.commit().await?;
    counters.add(deltas);
    Ok(report)
}

/// Account purge of `did` (§7.4), one batch: deletes up to `batch` of the
/// DID's authored `blocks`, `list_blocks` (counter path) and `list_items`
/// under author(D) plus the list locks the batch touches, and fires **RD**
/// on the DID's lists (their items are then purged by
/// [`process_purges`]). Returns `true` when nothing authored remains.
/// Rows where the DID is the *subject* stay; the `lists` rows stay with
/// `record_state = deleted`.
pub async fn purge_account_batch(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    did: &Did,
    batch: i64,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let (done, deltas) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_authors(&[keys::author_lock_key(did.as_str())].into_iter().collect())
            .await?;
        let Some(author_id) = t.actor_id(did.as_str()).await? else {
            return Ok(true);
        };
        let author = t.author(did).await?;
        let blocks: Vec<String> = sqlx::query_scalar(
            "SELECT rkey FROM blocks WHERE author_id = $1 ORDER BY rkey LIMIT $2",
        )
        .bind(author_id)
        .bind(batch)
        .fetch_all(&mut *t.conn)
        .await?;
        let lbs: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT r.rkey, a.did, l.rkey FROM list_blocks r
             JOIN lists l ON l.id = r.list_id JOIN actors a ON a.id = l.owner_id
             WHERE r.author_id = $1 ORDER BY r.rkey LIMIT $2",
        )
        .bind(author_id)
        .bind(batch)
        .fetch_all(&mut *t.conn)
        .await?;
        let items: Vec<(String, String)> = sqlx::query_as(
            "SELECT r.rkey, l.rkey FROM list_items r JOIN lists l ON l.id = r.list_id
             WHERE r.owner_id = $1 ORDER BY r.rkey LIMIT $2",
        )
        .bind(author_id)
        .bind(batch)
        .fetch_all(&mut *t.conn)
        .await?;
        let own_lists: Vec<(i64, String, i16)> = sqlx::query_as(
            "SELECT id, rkey, record_state FROM lists WHERE owner_id = $1 AND record_state <> 2",
        )
        .bind(author_id)
        .fetch_all(&mut *t.conn)
        .await?;
        let mut locks = std::collections::BTreeMap::new();
        for (_, owner, lrkey) in &lbs {
            locks.insert(keys::list_lock_key(owner, lrkey), true);
        }
        for (_, lrkey) in &items {
            locks
                .entry(keys::list_lock_key(did.as_str(), lrkey))
                .or_insert(false);
        }
        for (_, lrkey, _) in &own_lists {
            locks.insert(keys::list_lock_key(did.as_str(), lrkey), true);
        }
        t.lock_lists(&locks).await?;
        for rk in &blocks {
            block_delete_row(&mut t, &author, rk).await?;
        }
        for (rk, _, _) in &lbs {
            listblock_delete_row(&mut t, &author, rk).await?;
        }
        for (rk, _) in &items {
            item_delete_row(&mut t, &author, rk).await?;
        }
        for (list_id, _, record_state) in &own_lists {
            sqlx::query(
                "UPDATE lists SET record_state = 2, purpose = NULL, name = NULL, created_at = NULL
                 WHERE id = $1",
            )
            .bind(*list_id)
            .execute(&mut *t.conn)
            .await?;
            if *record_state == 1 {
                sqlx::query("UPDATE actors SET authored_lists = authored_lists - 1 WHERE id = $1")
                    .bind(author_id)
                    .execute(&mut *t.conn)
                    .await?;
                t.deltas.stat(stat::LISTS, -1);
                t.deltas.host(&author.buckets, CapKind::Lists, -1);
            }
            t.fire(*list_id, Event::RecordDeleted, FireArgs::default())
                .await?;
        }
        // Cancel any running fetch run for the owner (§4.4 notes).
        sqlx::query(
            "UPDATE list_fetch_runs SET finished_at = now(), outcome = 4
             WHERE owner_id = $1 AND finished_at IS NULL",
        )
        .bind(author_id)
        .execute(&mut *t.conn)
        .await?;
        let done = (blocks.len() as i64) < batch
            && (lbs.len() as i64) < batch
            && (items.len() as i64) < batch;
        t.send_notify().await?;
        let (_, deltas) = t.finish();
        (done, deltas)
    };
    tx.commit().await?;
    counters.add(deltas);
    Ok(done)
}

/// Run end of a list fetch run (§5.5): under list(L) exclusive, fires
/// **OK** on `list_id` only if it is still claimed by `run_id` for its
/// current epoch (`fetch_run_id = run_id AND fetch_run_epoch =
/// admit_epoch`), so a list re-admitted after run start is never promoted.
/// Returns whether OK fired.
pub async fn promote_claimed(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    list_id: i64,
    run_id: i64,
    args: FireArgs,
) -> Result<bool> {
    let Some((owner, rkey)) = list_owner_key(pool, list_id).await? else {
        return Ok(false);
    };
    let mut tx = pool.begin().await?;
    let (fired, deltas) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_lists(
            &[(keys::list_lock_key(&owner, &rkey), true)]
                .into_iter()
                .collect(),
        )
        .await?;
        let claimed: bool = sqlx::query_scalar(
            "SELECT fetch_run_id = $2 AND fetch_run_epoch = admit_epoch FROM lists WHERE id = $1",
        )
        .bind(list_id)
        .bind(run_id)
        .fetch_optional(&mut *t.conn)
        .await?
        .flatten()
        .unwrap_or(false);
        if claimed {
            t.fire(list_id, Event::Ok, args).await?;
        }
        if t.notify {
            t.send_notify().await?;
        }
        let (_, d) = t.finish();
        (claimed, d)
    };
    tx.commit().await?;
    counters.add(deltas);
    Ok(fired)
}

/// Divergence purge of `did` (§5.2 "Divergence check"), one batch: the
/// repo went backwards, so everything authored under its discarded history
/// goes — blocks, listblocks (counter path), listitems and tombstones — and
/// the DID's `lists` rows lose their stored rev (the record fields stay;
/// the fresh listing re-applies them, which a stored rev from the discarded
/// history would refuse as newer). Unlike an account purge no **RD** fires:
/// the caller fires **DV** on the DID's tracked lists *before* this.
/// Returns `true` when nothing authored remains.
pub async fn purge_for_divergence_batch(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    did: &Did,
    batch: i64,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let (done, deltas) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_authors(&[keys::author_lock_key(did.as_str())].into_iter().collect())
            .await?;
        let Some(author_id) = t.actor_id(did.as_str()).await? else {
            return Ok(true);
        };
        let author = t.author(did).await?;
        let blocks: Vec<String> = sqlx::query_scalar(
            "SELECT rkey FROM blocks WHERE author_id = $1 ORDER BY rkey LIMIT $2",
        )
        .bind(author_id)
        .bind(batch)
        .fetch_all(&mut *t.conn)
        .await?;
        let lbs: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT r.rkey, a.did, l.rkey FROM list_blocks r
             JOIN lists l ON l.id = r.list_id JOIN actors a ON a.id = l.owner_id
             WHERE r.author_id = $1 ORDER BY r.rkey LIMIT $2",
        )
        .bind(author_id)
        .bind(batch)
        .fetch_all(&mut *t.conn)
        .await?;
        let items: Vec<(String, String)> = sqlx::query_as(
            "SELECT r.rkey, l.rkey FROM list_items r JOIN lists l ON l.id = r.list_id
             WHERE r.owner_id = $1 ORDER BY r.rkey LIMIT $2",
        )
        .bind(author_id)
        .bind(batch)
        .fetch_all(&mut *t.conn)
        .await?;
        let mut locks = std::collections::BTreeMap::new();
        for (_, owner, lrkey) in &lbs {
            locks.insert(keys::list_lock_key(owner, lrkey), true);
        }
        for (_, lrkey) in &items {
            locks
                .entry(keys::list_lock_key(did.as_str(), lrkey))
                .or_insert(false);
        }
        t.lock_lists(&locks).await?;
        for rk in &blocks {
            block_delete_row(&mut t, &author, rk).await?;
        }
        for (rk, _, _) in &lbs {
            listblock_delete_row(&mut t, &author, rk).await?;
        }
        for (rk, _) in &items {
            item_delete_row(&mut t, &author, rk).await?;
        }
        let done = (blocks.len() as i64) < batch
            && (lbs.len() as i64) < batch
            && (items.len() as i64) < batch;
        if done {
            sqlx::query("DELETE FROM tombstones WHERE author_id = $1")
                .bind(author_id)
                .execute(&mut *t.conn)
                .await?;
            sqlx::query("UPDATE lists SET rev = NULL WHERE owner_id = $1")
                .bind(author_id)
                .execute(&mut *t.conn)
                .await?;
        }
        t.send_notify().await?;
        let (_, deltas) = t.finish();
        (done, deltas)
    };
    tx.commit().await?;
    counters.add(deltas);
    Ok(done)
}

/// The daily retry task (§4.4): fires **GO** on `deferred` lists whose
/// `next_retry_at` has passed (lists deferred by the owner re-admission
/// budget get the next UTC midnight). Lists deferred by the budget, the
/// ceiling or a bucket cap are re-opened by the budget monitor / counter
/// flush instead, which call [`fire_event`] with GO themselves.
pub async fn retry_deferred(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    now: DateTime<Utc>,
) -> Result<ApplyReport> {
    let lists: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM lists WHERE track_state = $1 AND next_retry_at <= $2 AND listblock_count > 0 ORDER BY id",
    )
    .bind(TrackState::Deferred.code())
    .bind(now)
    .fetch_all(pool)
    .await?;
    let mut out = ApplyReport::default();
    for list_id in lists {
        let r = fire_event(
            pool,
            limits,
            counters,
            list_id,
            Event::GateOpen,
            FireArgs::default(),
        )
        .await?;
        out.transitions.extend(r.transitions);
    }
    Ok(out)
}

/// Purges every authored row of an account that became `deleted`
/// (§7.4), batch by batch, firing RD on its lists.
pub async fn purge_account(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    did: &Did,
) -> Result<()> {
    while !purge_account_batch(pool, limits, counters, did, PURGE_BATCH).await? {}
    Ok(())
}

/// Accounts with status `deleted` that still author rows (a purge was
/// interrupted, e.g. by a crash after the status commit). Ingest runs
/// these at start-up.
pub async fn accounts_pending_purge(pool: &PgPool, limit: i64) -> Result<Vec<Did>> {
    let dids: Vec<String> = sqlx::query_scalar(
        "SELECT did FROM actors a WHERE a.status = $1
           AND (a.authored_blocks > 0 OR a.authored_listblocks > 0 OR a.owned_items > 0
                OR EXISTS (SELECT 1 FROM lists l WHERE l.owner_id = a.id AND l.record_state <> 2))
         ORDER BY id LIMIT $2",
    )
    .bind(crate::codes::actor_status::DELETED)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    dids.iter()
        .map(|d| Did::parse(d).map_err(|e| StorageError::Invariant(e.to_string())))
        .collect()
}
