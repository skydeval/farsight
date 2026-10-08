//! Janitor tasks: list purges and the end of grace, cursor and rate-table
//! cleanup, tombstone expiry, account purges and placeholder cleanup (see
//! `docs/design/storage.md` and `docs/design/list-indexing.md`).
//!
//! Each function is one pass; the server schedules them (hourly
//! tombstones, nightly the rest, purges continuously). Time is injectable
//! (`now`) so the harness can test TTLs without waiting.

use crate::codes::sql::{
    FETCH_CANCELLED, JOB_LIST_FETCH, JOB_REPO, RECORD_DELETED, RECORD_UNKNOWN, TRACK_RETAINED,
    TRACK_UNTRACKED,
};
use crate::ids::{ActorId, ListId, RunId, Stamp};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use farsight_core::Did;
use sqlx::{PgConnection, PgPool};
use std::collections::BTreeMap;

use crate::apply::{block_delete_rows, item_delete_rows, listblock_delete_rows};
use crate::codes::TrackState;
use crate::counters::{CounterSink, stat};
use crate::error::{Result, StorageError};
use crate::history::{self, Cause, Removal};
use crate::keys::{self, CapKind, Limits};
use crate::tracking::FireArgs;
use crate::transition::Event;
use crate::txn::{ApplyReport, Gates, MAX_LOCKS, Txn, retry_deadlocks};

/// Items deleted per purge transaction.
pub const PURGE_BATCH: i64 = 10_000;

/// Lists of its own that one account-purge transaction marks deleted.
pub const PURGE_OWN_LISTS: usize = MAX_LOCKS / 4;

/// Finished `list_fetch_runs` rows are kept this long.
pub const FETCH_RUN_RETENTION: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 3600);
/// `op_errors` rows are kept this long.
pub const OP_ERROR_RETENTION: std::time::Duration = std::time::Duration::from_secs(30 * 24 * 3600);
/// The most `op_errors` rows kept, whatever their age.
pub const OP_ERROR_ROWS: i64 = 100_000;
/// How long a purge that failed waits before it is tried again.
pub const PURGE_RETRY: std::time::Duration = std::time::Duration::from_secs(3600);

/// The head of `rows` whose list locks fit: a row goes in while its key
/// is already in `locks` or fewer than `cap` keys are. The first row that
/// does not fit ends the head. Returns the head and whether rows were
/// left out.
pub(crate) fn head_within<T>(
    rows: Vec<T>,
    key: impl Fn(&T) -> (i64, bool),
    locks: &mut BTreeMap<i64, bool>,
    cap: usize,
) -> (Vec<T>, bool) {
    let total = rows.len();
    let mut head = Vec::with_capacity(total);
    for row in rows {
        let (k, exclusive) = key(&row);
        if !locks.contains_key(&k) && locks.len() >= cap {
            break;
        }
        *locks.entry(k).or_insert(false) |= exclusive;
        head.push(row);
    }
    let cut = head.len() < total;
    (head, cut)
}

/// Deletes finished `list_fetch_runs` rows older than
/// [`FETCH_RUN_RETENTION`] (nightly). A run still claimed by a list, or
/// not finished, stays.
pub async fn prune_fetch_runs(pool: &PgPool, now: DateTime<Utc>) -> Result<u64> {
    let cutoff = now
        - ChronoDuration::from_std(FETCH_RUN_RETENTION)
            .map_err(|e| StorageError::Invariant(e.to_string()))?;
    Ok(sqlx::query(
        "DELETE FROM list_fetch_runs r WHERE r.finished_at < $1
           AND NOT EXISTS (SELECT 1 FROM lists l WHERE l.fetch_run_id = r.id)",
    )
    .bind(cutoff)
    .execute(pool)
    .await?
    .rows_affected())
}

/// Deletes `op_errors` rows older than [`OP_ERROR_RETENTION`], and the
/// oldest beyond [`OP_ERROR_ROWS`] (nightly).
pub async fn prune_op_errors(pool: &PgPool, now: DateTime<Utc>) -> Result<u64> {
    let cutoff = now
        - ChronoDuration::from_std(OP_ERROR_RETENTION)
            .map_err(|e| StorageError::Invariant(e.to_string()))?;
    let old = sqlx::query("DELETE FROM op_errors WHERE at < $1")
        .bind(cutoff)
        .execute(pool)
        .await?
        .rows_affected();
    let over = sqlx::query(
        "DELETE FROM op_errors WHERE id <= (
           SELECT id FROM op_errors ORDER BY id DESC OFFSET $1 LIMIT 1)",
    )
    .bind(OP_ERROR_ROWS)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(old + over)
}

/// Deletes `subject_lists` rows whose list's record is deleted: such a
/// list names nobody (nightly).
pub async fn prune_subject_lists(pool: &PgPool) -> Result<u64> {
    Ok(sqlx::query(&format!(
        "DELETE FROM subject_lists s USING lists l
         WHERE l.id = s.list_id AND l.record_state = {RECORD_DELETED}"
    ))
    .execute(pool)
    .await?
    .rows_affected())
}

/// Deletes tombstones older than the TTL (hourly). The TTL is measured
/// from `deleted_at`, the time the delete was processed.
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

/// Drops `admission_rate`, `intern_rate` and `history_rate` rows older than 2 days
/// (nightly).
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
    let h = sqlx::query("DELETE FROM history_rate WHERE utc_day < $1")
        .bind(cutoff)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(a + i + h)
}

/// Deletes cursor rows of runs that are no longer current (nightly).
pub async fn drop_orphaned_cursors(pool: &PgPool) -> Result<u64> {
    Ok(sqlx::query(&format!(
        "DELETE FROM backfill_cursors c
         WHERE (c.job_kind = {JOB_REPO} AND NOT EXISTS (
                  SELECT 1 FROM backfill_state s
                  WHERE s.actor_id = c.actor_id AND s.current_run_id = c.run_id))
            OR (c.job_kind = {JOB_LIST_FETCH} AND NOT EXISTS (
                  SELECT 1 FROM list_fetch_runs r
                  WHERE r.id = c.run_id AND r.finished_at IS NULL))"
    ))
    .execute(pool)
    .await?
    .rows_affected())
}

/// SQL condition: list `l` is a placeholder nothing refers to.
fn placeholder_unreferenced() -> String {
    format!(
        "
    l.record_state = {RECORD_UNKNOWN} AND l.track_state = {TRACK_UNTRACKED}
    AND NOT EXISTS (SELECT 1 FROM list_blocks x WHERE x.list_id = l.id)
    AND NOT EXISTS (SELECT 1 FROM list_items x WHERE x.list_id = l.id)
    AND NOT EXISTS (SELECT 1 FROM subject_lists x WHERE x.list_id = l.id)
    AND NOT EXISTS (SELECT 1 FROM list_jobs x WHERE x.list_id = l.id)
    AND NOT EXISTS (SELECT 1 FROM list_sched_keys x WHERE x.list_id = l.id)"
    )
}

/// Placeholder-list cleanup (nightly): deletes `lists` rows with
/// `record_state = unknown`, `track_state = untracked` and no reference
/// from `list_blocks`, `list_items`, `subject_lists`, `list_jobs` or
/// `list_sched_keys`. Each deletion takes list(L) exclusive and re-checks
/// the conditions inside that transaction, so an apply about to reference
/// L either commits first (the re-check sees it) or runs after and
/// re-interns L. Returns rows deleted.
pub async fn cleanup_placeholder_lists(pool: &PgPool, limit: i64) -> Result<u64> {
    let candidates: Vec<(ListId, String, String)> = sqlx::query_as(&format!(
        "SELECT l.id, a.did, l.rkey FROM lists l JOIN actors a ON a.id = l.owner_id
         WHERE {unreferenced} ORDER BY l.id LIMIT $1",
        unreferenced = placeholder_unreferenced()
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
            "DELETE FROM lists l WHERE l.id = $1 AND {unreferenced}",
            unreferenced = placeholder_unreferenced()
        ))
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
    }
    Ok(deleted)
}

async fn list_owner_key(pool: &PgPool, list_id: ListId) -> Result<Option<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT a.did, l.rkey FROM lists l JOIN actors a ON a.id = l.owner_id WHERE l.id = $1",
    )
    .bind(list_id)
    .fetch_optional(pool)
    .await?)
}

/// What one call of [`process_purges`] did, over all the lists it visited.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PurgeReport {
    /// `list_items` rows deleted.
    pub items_deleted: u64,
    /// Lists whose purge finished (PD fired).
    pub finished: Vec<ListId>,
    /// The transitions the pass fired, in `transitions`; the report's other
    /// fields are left at zero.
    pub report: ApplyReport,
}

/// Runs one batch of every `purging` list (purge→X): deletes up to
/// [`PURGE_BATCH`] items under author(owner) + list(L) exclusive,
/// re-checking `purging`; fires **PD** when none remain. A batch is read
/// through the list's own index and deleted with one statement.
pub async fn process_purges(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    max_lists: i64,
) -> Result<PurgeReport> {
    let lists: Vec<ListId> =
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
        // The batch holds the owner's author lock and the list's lock
        // while the firehose writes under the same ones: a deadlock
        // aborts the batch, not the pass.
        let (report, deleted, finished) = retry_deadlocks(|| {
            purge_list_batch_once(pool, limits, counters, list_id, &owner, &rkey, &owner_did)
        })
        .await?;
        out.items_deleted += deleted;
        if finished {
            out.finished.push(list_id);
        }
        out.report.transitions.extend(report.transitions);
    }
    Ok(out)
}

/// One batch of one `purging` list, in one transaction: what was fired,
/// how many items were deleted, and whether the purge finished.
async fn purge_list_batch_once(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    list_id: ListId,
    owner: &str,
    rkey: &str,
    owner_did: &Did,
) -> Result<(ApplyReport, u64, bool)> {
    let mut tx = pool.begin().await?;
    let (report, deltas, deleted, finished) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_authors(&[keys::author_lock_key(owner)].into_iter().collect())
            .await?;
        t.lock_lists(
            &[(keys::list_lock_key(owner, rkey), true)]
                .into_iter()
                .collect(),
        )
        .await?;
        let state = t.list_tracking(list_id).await?.state;
        let mut deleted = 0u64;
        let mut finished = false;
        if state == TrackState::Purging {
            let author = t.author(owner_did).await?;
            // In the order of `list_items_by_list`, so the batch is the
            // head of an index scan. Which items go first does not
            // matter: the list is drained to the end.
            let rkeys: Vec<String> = sqlx::query_scalar(
                "SELECT rkey FROM list_items WHERE list_id = $1
                 ORDER BY subject_id, rkey LIMIT $2",
            )
            .bind(list_id)
            .bind(PURGE_BATCH)
            .fetch_all(&mut *t.conn)
            .await?;
            // Recorded only while the list's record is deleted and its
            // owner is not; `item_delete_rows` checks both. Every other
            // drain is a change of tracking.
            let drained = Removal::listing(Cause::ListDeleted);
            deleted = item_delete_rows(&mut t, &author, &rkeys, None, Some(&drained)).await?;
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
    Ok((report, deleted, finished))
}

/// Fires **GE** on `retained` lists whose grace ended.
pub async fn expire_grace(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    now: DateTime<Utc>,
) -> Result<ApplyReport> {
    let lists: Vec<ListId> = sqlx::query_scalar(
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
    list_id: ListId,
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
    list_id: ListId,
    event: Event,
    args: FireArgs,
    grace_now: Option<DateTime<Utc>>,
) -> Result<ApplyReport> {
    let Some((owner, rkey)) = list_owner_key(pool, list_id).await? else {
        return Ok(ApplyReport::default());
    };
    let key = keys::list_lock_key(&owner, &rkey);
    retry_deadlocks(|| {
        fire_event_once(pool, limits, counters, list_id, key, event, args, grace_now)
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn fire_event_once(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    list_id: ListId,
    key: i64,
    event: Event,
    args: FireArgs,
    grace_now: Option<DateTime<Utc>>,
) -> Result<ApplyReport> {
    let mut tx = pool.begin().await?;
    let (report, deltas) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_lists(&[(key, true)].into_iter().collect()).await?;
        // The list may have been deleted (a placeholder nothing refers
        // to) since it was picked.
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM lists WHERE id = $1)")
            .bind(list_id)
            .fetch_one(&mut *t.conn)
            .await?;
        if !exists {
            return Ok(ApplyReport::default());
        }
        let proceed = match grace_now {
            Some(now) => {
                sqlx::query_scalar::<_, bool>(
                    &format!("SELECT track_state = {TRACK_RETAINED} AND retain_until IS NOT NULL AND retain_until <= $2
                 FROM lists WHERE id = $1"),
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

/// Account purge of `did`, one batch: deletes up to `batch` of the DID's
/// authored `blocks`, `list_blocks` (counter path) and `list_items` under
/// author(D) plus the list locks the batch touches, and fires **RD** on
/// up to [`PURGE_OWN_LISTS`] of the DID's lists (their items are then
/// purged by [`process_purges`]). A batch ends where its rows would need
/// more than [`MAX_LOCKS`] list locks. Returns `true` when nothing
/// authored remains, and at once for an account whose status is not
/// `deleted`: a purge asked for an account that has since been
/// reactivated removes nothing. Rows where the DID is the *subject*
/// stay; the `lists` rows stay with `record_state = deleted`.
pub async fn purge_account_batch(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    did: &Did,
    batch: i64,
) -> Result<bool> {
    retry_deadlocks(|| purge_account_batch_once(pool, limits, counters, did, batch)).await
}

/// `(listblock rkey, list owner DID, list rkey)`.
type ListBlockTarget = (String, String, String);

/// Up to `batch` of the author's listblocks (with their lists) and
/// listitems (with their lists' rkeys), each ascending by rkey; with
/// `below`, only rows whose rev is lower.
async fn authored_list_rows(
    conn: &mut PgConnection,
    author_id: ActorId,
    batch: i64,
    below: Option<Stamp>,
) -> Result<(Vec<ListBlockTarget>, Vec<(String, String)>)> {
    let lbs: Vec<ListBlockTarget> = sqlx::query_as(
        "SELECT r.rkey, a.did, l.rkey FROM list_blocks r
         JOIN lists l ON l.id = r.list_id JOIN actors a ON a.id = l.owner_id
         WHERE r.author_id = $1 AND ($3::BIGINT IS NULL OR r.rev < $3)
         ORDER BY r.rkey LIMIT $2",
    )
    .bind(author_id)
    .bind(batch)
    .bind(below)
    .fetch_all(&mut *conn)
    .await?;
    let items: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.rkey, l.rkey FROM list_items r JOIN lists l ON l.id = r.list_id
         WHERE r.owner_id = $1 AND ($3::BIGINT IS NULL OR r.rev < $3)
         ORDER BY r.rkey LIMIT $2",
    )
    .bind(author_id)
    .bind(batch)
    .bind(below)
    .fetch_all(&mut *conn)
    .await?;
    Ok((lbs, items))
}

async fn purge_account_batch_once(
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
        let Some(author_id) = t.actor_id(did).await? else {
            return Ok(true);
        };
        let author = t.author(did).await?;
        // Read under the author lock, which every change of the status
        // holds: an account that is not `deleted` (any more) keeps its
        // rows. A reactivation re-lists the repository instead.
        if author.status != crate::codes::ActorStatus::Deleted {
            return Ok(true);
        }
        let blocks: Vec<String> = sqlx::query_scalar(
            "SELECT rkey FROM blocks WHERE author_id = $1 ORDER BY rkey LIMIT $2",
        )
        .bind(author_id)
        .bind(batch)
        .fetch_all(&mut *t.conn)
        .await?;
        let (lbs, items) = authored_list_rows(&mut *t.conn, author_id, batch, None).await?;
        let own_lists: Vec<(ListId, String, i16)> = sqlx::query_as(
            &format!("SELECT id, rkey, record_state FROM lists WHERE owner_id = $1 AND record_state <> {RECORD_DELETED}
                      ORDER BY id LIMIT $2"),
        )
        .bind(author_id)
        .bind(PURGE_OWN_LISTS as i64)
        .fetch_all(&mut *t.conn)
        .await?;
        let mut locks = BTreeMap::new();
        for (_, lrkey, _) in &own_lists {
            locks.insert(keys::list_lock_key(did.as_str(), lrkey), true);
        }
        let (lbs_read, items_read) = (lbs.len() as i64, items.len() as i64);
        let (lbs, lbs_cut) = head_within(
            lbs,
            |(_, owner, lrkey)| (keys::list_lock_key(owner, lrkey), true),
            &mut locks,
            MAX_LOCKS,
        );
        let (items, items_cut) = head_within(
            items,
            |(_, lrkey)| (keys::list_lock_key(did.as_str(), lrkey), false),
            &mut locks,
            MAX_LOCKS,
        );
        t.lock_lists(&locks).await?;
        let lb_keys: Vec<String> = lbs.iter().map(|(rk, _, _)| rk.clone()).collect();
        let item_keys: Vec<String> = items.iter().map(|(rk, _)| rk.clone()).collect();
        block_delete_rows(&mut t, &author, &blocks, None, None).await?;
        listblock_delete_rows(&mut t, &author, &lb_keys, None, None).await?;
        item_delete_rows(&mut t, &author, &item_keys, None, None).await?;
        for (list_id, _, record_state) in &own_lists {
            sqlx::query(
                &format!("UPDATE lists SET record_state = {RECORD_DELETED}, purpose = NULL, name = NULL, created_at = NULL,
                   description = NULL, avatar_cid = NULL
                 WHERE id = $1"),
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
        // Cancel any running fetch run for the owner.
        sqlx::query(&format!(
            "UPDATE list_fetch_runs SET finished_at = now(), outcome = {FETCH_CANCELLED}
             WHERE owner_id = $1 AND finished_at IS NULL"
        ))
        .bind(author_id)
        .execute(&mut *t.conn)
        .await?;
        let mut done = (blocks.len() as i64) < batch
            && lbs_read < batch
            && items_read < batch
            && !lbs_cut
            && !items_cut
            && own_lists.len() < PURGE_OWN_LISTS;
        if done {
            // The purge writes no history, and deletes the history the DID
            // authored, in batches, after the live rows.
            let n = history::delete_authored(&mut *t.conn, author_id, batch).await?;
            done = (n as i64) < batch;
        }
        t.send_notify().await?;
        let (_, deltas) = t.finish();
        (done, deltas)
    };
    tx.commit().await?;
    counters.add(deltas);
    Ok(done)
}

/// Run end of a list fetch run: under list(L) exclusive, fires **OK** on
/// `list_id` only if it is still claimed by `run_id` for its current epoch
/// (`fetch_run_id = run_id AND fetch_run_epoch = admit_epoch`), so a list
/// re-admitted after run start is never promoted. Returns whether OK
/// fired.
pub async fn promote_claimed(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    list_id: ListId,
    run_id: RunId,
    args: FireArgs,
) -> Result<bool> {
    let Some((owner, rkey)) = list_owner_key(pool, list_id).await? else {
        return Ok(false);
    };
    let key = keys::list_lock_key(&owner, &rkey);
    retry_deadlocks(|| promote_claimed_once(pool, limits, counters, list_id, key, run_id, args))
        .await
}

async fn promote_claimed_once(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    list_id: ListId,
    key: i64,
    run_id: RunId,
    args: FireArgs,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let (fired, deltas) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_lists(&[(key, true)].into_iter().collect()).await?;
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

/// Divergence purge of `did` (the divergence check), one batch: the repo
/// went backwards, so everything authored under its discarded history goes
/// — blocks, listblocks (counter path), listitems and tombstones — and the
/// DID's `lists` rows lose their stored rev (the record fields stay; the
/// fresh listing re-applies them, which a stored rev from the discarded
/// history would refuse as newer). Only rows, tombstones and list revs
/// below `below` go: the caller passes the stamp of the moment it found
/// the divergence, so what the firehose writes while the purge runs,
/// whose revs are later, is kept. Unlike an account purge no **RD**
/// fires: the caller fires **DV** on the DID's tracked lists *before*
/// this. A batch ends where its rows would need more than [`MAX_LOCKS`]
/// list locks. Returns `true` when nothing below `below` remains.
pub async fn purge_for_divergence_batch(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    did: &Did,
    batch: i64,
    below: Stamp,
) -> Result<bool> {
    retry_deadlocks(|| purge_for_divergence_once(pool, limits, counters, did, batch, below)).await
}

async fn purge_for_divergence_once(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    did: &Did,
    batch: i64,
    below: Stamp,
) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let (done, deltas) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_authors(&[keys::author_lock_key(did.as_str())].into_iter().collect())
            .await?;
        let Some(author_id) = t.actor_id(did).await? else {
            return Ok(true);
        };
        let author = t.author(did).await?;
        let blocks: Vec<String> = sqlx::query_scalar(
            "SELECT rkey FROM blocks WHERE author_id = $1 AND rev < $3 ORDER BY rkey LIMIT $2",
        )
        .bind(author_id)
        .bind(batch)
        .bind(below)
        .fetch_all(&mut *t.conn)
        .await?;
        let (lbs, items) = authored_list_rows(&mut *t.conn, author_id, batch, Some(below)).await?;
        let mut locks = BTreeMap::new();
        let (lbs_read, items_read) = (lbs.len() as i64, items.len() as i64);
        let (lbs, lbs_cut) = head_within(
            lbs,
            |(_, owner, lrkey)| (keys::list_lock_key(owner, lrkey), true),
            &mut locks,
            MAX_LOCKS,
        );
        let (items, items_cut) = head_within(
            items,
            |(_, lrkey)| (keys::list_lock_key(did.as_str(), lrkey), false),
            &mut locks,
            MAX_LOCKS,
        );
        t.lock_lists(&locks).await?;
        let lb_keys: Vec<String> = lbs.iter().map(|(rk, _, _)| rk.clone()).collect();
        let item_keys: Vec<String> = items.iter().map(|(rk, _)| rk.clone()).collect();
        block_delete_rows(&mut t, &author, &blocks, Some(below), None).await?;
        listblock_delete_rows(&mut t, &author, &lb_keys, Some(below), None).await?;
        item_delete_rows(&mut t, &author, &item_keys, Some(below), None).await?;
        let done = (blocks.len() as i64) < batch
            && lbs_read < batch
            && items_read < batch
            && !lbs_cut
            && !items_cut;
        if done {
            sqlx::query("DELETE FROM tombstones WHERE author_id = $1 AND rev < $2")
                .bind(author_id)
                .bind(below)
                .execute(&mut *t.conn)
                .await?;
            sqlx::query("UPDATE lists SET rev = NULL WHERE owner_id = $1 AND rev < $2")
                .bind(author_id)
                .bind(below)
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

/// The daily retry task: fires **GO** on `deferred` lists whose
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
    let lists: Vec<ListId> = sqlx::query_scalar(
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

/// Purges every authored row of an account that became `deleted`, batch
/// by batch, firing RD on its lists.
pub async fn purge_account(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    did: &Did,
) -> Result<()> {
    while !purge_account_batch(pool, limits, counters, did, PURGE_BATCH).await? {}
    Ok(())
}

/// Asks for the purge of account `actor_id` (`account_purges`). Called in
/// the transaction that records the account as `deleted`.
pub async fn request_account_purge(conn: &mut PgConnection, actor_id: ActorId) -> Result<()> {
    sqlx::query(
        "INSERT INTO account_purges (actor_id) VALUES ($1)
         ON CONFLICT (actor_id) DO UPDATE SET not_before = NULL",
    )
    .bind(actor_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// What one call of [`run_account_purges`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountPurges {
    /// Accounts of which nothing is left.
    pub purged: u32,
    /// Accounts whose purge goes on in the next call.
    pub unfinished: u32,
    /// Accounts whose purge failed, with the failure; each is tried again
    /// after [`PURGE_RETRY`].
    pub failed: Vec<(Did, String)>,
}

/// Runs the purges asked for in `account_purges`, oldest request first:
/// up to `accounts` accounts, each for up to `batches` batches. An
/// account of which nothing is left loses its row; one with more to
/// remove keeps it for the next call.
pub async fn run_account_purges(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    accounts: i64,
    batches: u32,
) -> Result<AccountPurges> {
    let due: Vec<(ActorId, String)> = sqlx::query_as(
        "SELECT p.actor_id, a.did FROM account_purges p JOIN actors a ON a.id = p.actor_id
         WHERE p.not_before IS NULL OR p.not_before <= now()
         ORDER BY p.requested_at, p.actor_id LIMIT $1",
    )
    .bind(accounts)
    .fetch_all(pool)
    .await?;
    let mut out = AccountPurges::default();
    for (actor_id, did) in due {
        let did = Did::parse(&did).map_err(|e| StorageError::Invariant(e.to_string()))?;
        let mut done = false;
        let mut failure = None;
        for _ in 0..batches {
            match purge_account_batch(pool, limits, counters, &did, PURGE_BATCH).await {
                Ok(true) => {
                    done = true;
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    failure = Some(e.to_string());
                    break;
                }
            }
        }
        if let Some(e) = failure {
            sqlx::query(
                "UPDATE account_purges SET not_before = now() + make_interval(secs => $2)
                 WHERE actor_id = $1",
            )
            .bind(actor_id)
            .bind(PURGE_RETRY.as_secs_f64())
            .execute(pool)
            .await?;
            out.failed.push((did, e));
        } else if done {
            sqlx::query("DELETE FROM account_purges WHERE actor_id = $1")
                .bind(actor_id)
                .execute(pool)
                .await?;
            out.purged += 1;
        } else {
            out.unfinished += 1;
        }
    }
    Ok(out)
}

/// Asks for the purge of every account [`accounts_pending_purge`] finds
/// (up to `limit`): a purge whose request was lost, or an account that
/// got a row again after its purge. Returns how many it found.
pub async fn request_pending_purges(pool: &PgPool, limit: i64) -> Result<usize> {
    let pending = accounts_pending_purge(pool, limit).await?;
    let dids: Vec<String> = pending.iter().map(|d| d.as_str().to_owned()).collect();
    sqlx::query(
        "INSERT INTO account_purges (actor_id)
         SELECT id FROM actors WHERE did = ANY($1)
         ON CONFLICT (actor_id) DO NOTHING",
    )
    .bind(&dids)
    .execute(pool)
    .await?;
    Ok(pending.len())
}

/// Accounts with status `deleted` that still author rows, live or in
/// history (a purge whose request was lost, or a replayed event wrote a
/// history row later).
pub async fn accounts_pending_purge(pool: &PgPool, limit: i64) -> Result<Vec<Did>> {
    let dids: Vec<String> = sqlx::query_scalar(
        // The deleted accounts are read first, in one pass over `actors`.
        // Written as one SELECT with `ORDER BY id LIMIT`, the planner
        // walks the whole table through its primary key looking for
        // matches that are rarely there: minutes for ten million
        // accounts, before the server serves.
        &format!("WITH d AS MATERIALIZED (
           SELECT id, did, authored_blocks, authored_listblocks, owned_items
           FROM actors WHERE status = $1)
         SELECT did FROM d a
         WHERE (a.authored_blocks > 0 OR a.authored_listblocks > 0 OR a.owned_items > 0
                OR EXISTS (SELECT 1 FROM lists l WHERE l.owner_id = a.id AND l.record_state <> {RECORD_DELETED})
                OR EXISTS (SELECT 1 FROM blocks_history h WHERE h.author_id = a.id)
                OR EXISTS (SELECT 1 FROM list_blocks_history h WHERE h.author_id = a.id)
                OR EXISTS (SELECT 1 FROM list_items_history h WHERE h.owner_id = a.id))
         ORDER BY id LIMIT $2"),
    )
    .bind(crate::codes::ActorStatus::Deleted)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    dids.iter()
        .map(|d| Did::parse(d).map_err(|e| StorageError::Invariant(e.to_string())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_purge_batch_ends_where_its_locks_would_exceed_the_bound() {
        // Rows on lists 0, 0, 1, 2, 2, 3: with room for three keys the
        // head is the first five rows.
        let rows: Vec<i64> = vec![0, 0, 1, 2, 2, 3, 0];
        let mut locks = BTreeMap::new();
        let (head, cut) = head_within(rows.clone(), |k| (*k, true), &mut locks, 3);
        assert_eq!((head, cut), (vec![0, 0, 1, 2, 2], true));
        assert_eq!(locks.len(), 3);
        // Keys already held cost nothing; a shared lock does not weaken
        // an exclusive one, and an exclusive one upgrades a shared one.
        let mut locks: BTreeMap<i64, bool> = [(0, true), (1, false)].into_iter().collect();
        let (head, cut) = head_within(vec![0, 1, 0], |k| (*k, false), &mut locks, 2);
        assert_eq!((head.len(), cut), (3, false));
        assert_eq!(locks, [(0, true), (1, false)].into_iter().collect());
        let (_, cut) = head_within(vec![1], |k| (*k, true), &mut locks, 2);
        assert!(!cut && locks[&1]);
        // No room and a new key: nothing fits.
        let (head, cut) = head_within(vec![9, 0], |k| (*k, true), &mut locks, 2);
        assert!(head.is_empty() && cut);
        let (head, cut) = head_within(Vec::<i64>::new(), |k| (*k, true), &mut locks, 0);
        assert!(head.is_empty() && !cut);
    }
}
