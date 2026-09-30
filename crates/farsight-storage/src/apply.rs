//! `farsight-storage::apply` — the one write path for firehose batches,
//! listing pages and discovery writes (design §2, §4, §7).
//!
//! One Postgres transaction per batch, at `READ COMMITTED`:
//! 1. author locks for every author in the batch, ascending by key (§4.3);
//! 2. stored rows of every listblock / listitem key and every reconcile
//!    candidate are read (stable under the author locks), so the list keys
//!    of deletes, subject-changing updates and reconciles are known;
//! 3. list locks, ascending by key: exclusive for listblock and list
//!    writes, shared for listitem writes;
//! 4. writes in batch order, then reconciles, each with the LWW rule of
//!    §7.2, counters and transitions of §4.2/§4.4 and caps of §11;
//! 5. firehose progress (cursor, `applied_through`, `firehose_clock`) in
//!    the same transaction (§6.2, §3.7.1), and `NOTIFY farsight_coverage`.
//!
//! Deadlock aborts (`40P01`) are retried; they never count toward
//! poisoned-event handling (the error type says so).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use farsight_core::{
    BlockRecord, Collection, Did, ListBlockRecord, ListItemRecord, ListRecord, Record, RecordKey,
};
use sqlx::PgPool;

use crate::codes::{CapType, DebtReason, RecordState, TrackState};
use crate::counters::{CounterSink, stat};
use crate::error::{Result, StorageError};
use crate::firehose::FirehoseProgress;
use crate::keys::{self, CapKind, Limits};
use crate::tracking::FireArgs;
use crate::transition::Event;
use crate::txn::{ApplyReport, AuthorInfo, Cause, Gates, Refusal, Txn, lww_upsert_wins};

/// Maximum attempts of one batch transaction when deadlocks abort it.
pub const MAX_DEADLOCK_ATTEMPTS: u32 = 8;

/// A listing stamp may be applied only this long after it was read (§7.3).
pub const STAMP_VALIDITY: Duration = Duration::from_secs(72 * 3600);

/// Where a batch comes from; decides stamps, witnesses and charging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// Firehose events: `W` = commit rev, `witnessed_at` = event witness.
    Firehose,
    /// A listing page: `W = R`, `witnessed_at` NULL (§3.7.4).
    Listing {
        /// When `R` was read (database clock). Batches whose stamp is older
        /// than 72 h are rejected with [`StorageError::StaleStamp`].
        stamp_read_at: DateTime<Utc>,
        /// Budget gate on the job: inserts skipped with a `refused` debt,
        /// deletes and reconcile applied (§5.3).
        deletes_only: bool,
    },
    /// Discovery writes (`W = 0`), charged to the requester (§5.6, §11.2).
    Discovery {
        /// Requester cause key: `token:<id>` or `admin`.
        requester: String,
    },
}

/// What a write does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAction {
    /// Create or update with this version.
    Upsert(Record),
    /// Delete (firehose only): LWW delete plus tombstone.
    Delete,
}

/// One write to one record key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    /// Record author (repo DID).
    pub author: Did,
    /// Collection.
    pub collection: Collection,
    /// Record key.
    pub rkey: RecordKey,
    /// Stamp `W` (§7.2): commit rev, listing stamp `R`, or 0 for discovery.
    pub stamp: i64,
    /// Firehose witness time of the event, if firehose.
    pub witness: Option<DateTime<Utc>>,
    /// Upsert or delete.
    pub action: WriteAction,
}

/// A range or whole-collection reconcile (§5.2 steps 5 and 6): delete the
/// author's rows in `collection` with `rev < stamp` whose rkey lies in
/// `(after, through]` (open ends = unbounded) and is not in `keep`. Never
/// writes a tombstone (§7.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconcile {
    /// The repo.
    pub author: Did,
    /// Collection.
    pub collection: Collection,
    /// The listing stamp `R`.
    pub stamp: i64,
    /// Exclusive lower bound (`prev_last`); `None` = from the start.
    pub after: Option<RecordKey>,
    /// Inclusive upper bound; `None` = to +∞ (last page / whole range).
    pub through: Option<RecordKey>,
    /// Keys present on the page.
    pub keep: Vec<RecordKey>,
}

/// A batch: one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// Where it comes from.
    pub origin: Origin,
    /// Writes, applied in order.
    pub writes: Vec<Write>,
    /// Reconciles, applied after the writes.
    pub reconciles: Vec<Reconcile>,
    /// Firehose progress to persist atomically with the writes.
    pub firehose: Option<FirehoseProgress>,
}

impl Batch {
    /// An empty batch.
    pub fn new(origin: Origin) -> Batch {
        Batch {
            origin,
            writes: Vec::new(),
            reconciles: Vec::new(),
            firehose: None,
        }
    }
}

/// Process-level inputs to `apply`.
#[derive(Debug, Clone, Copy)]
pub struct ApplyCtx<'a> {
    /// Limits in force.
    pub limits: &'a Limits,
    /// Budget / ceiling gates in force.
    pub gates: Gates,
    /// Where committed counter deltas go.
    pub counters: &'a CounterSink,
}

/// Applies a batch in one transaction, retrying deadlock aborts.
pub async fn apply(pool: &PgPool, ctx: &ApplyCtx<'_>, batch: &Batch) -> Result<ApplyReport> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match apply_once(pool, ctx, batch).await {
            Ok((mut report, deltas)) => {
                ctx.counters.add(deltas);
                report.deadlock_retries = attempt - 1;
                return Ok(report);
            }
            Err(e) if e.is_deadlock() => {
                if attempt >= MAX_DEADLOCK_ATTEMPTS {
                    return Err(StorageError::DeadlockRetriesExhausted(attempt));
                }
                tokio::time::sleep(deadlock_backoff(attempt)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Backoff before retry `attempt` (1-based): 5 ms × attempt plus up to
/// 15 ms of jitter, so two colliding writers do not retry in lockstep.
pub fn deadlock_backoff(attempt: u32) -> Duration {
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos() % 15))
        .unwrap_or(0);
    Duration::from_millis(5 * u64::from(attempt) + jitter)
}

/// Lock set of a batch: list keys and whether exclusive.
#[derive(Default)]
struct ListLocks(BTreeMap<i64, bool>);

impl ListLocks {
    fn add(&mut self, owner: &str, rkey: &str, exclusive: bool) {
        let e = self
            .0
            .entry(keys::list_lock_key(owner, rkey))
            .or_insert(false);
        *e |= exclusive;
    }
}

async fn apply_once(
    pool: &PgPool,
    ctx: &ApplyCtx<'_>,
    batch: &Batch,
) -> Result<(ApplyReport, crate::counters::Deltas)> {
    let mut tx = pool.begin().await?;
    let parts = {
        let mut t = Txn::start(&mut *tx, ctx.limits, ctx.gates).await?;
        if let Origin::Listing { stamp_read_at, .. } = &batch.origin {
            check_stamp_fresh(&mut t, *stamp_read_at).await?;
        }

        // 1. Author locks.
        let mut authors: BTreeSet<&Did> = BTreeSet::new();
        for w in &batch.writes {
            authors.insert(&w.author);
        }
        for r in &batch.reconciles {
            authors.insert(&r.author);
        }
        let author_keys: BTreeSet<i64> = authors
            .iter()
            .map(|d| keys::author_lock_key(d.as_str()))
            .collect();
        t.lock_authors(&author_keys).await?;

        // 2. Stored rows → list keys.
        let mut locks = ListLocks::default();
        for w in &batch.writes {
            match (&w.action, w.collection) {
                (WriteAction::Upsert(Record::ListBlock(r)), _) => {
                    locks.add(r.subject.authority.as_str(), r.subject.rkey.as_str(), true);
                }
                (WriteAction::Upsert(Record::ListItem(r)), _) => {
                    locks.add(r.list.authority.as_str(), r.list.rkey.as_str(), false);
                }
                (_, Collection::List) => {
                    locks.add(w.author.as_str(), w.rkey.as_str(), true);
                }
                _ => {}
            }
        }
        let mut stored_keys: BTreeMap<(&Did, Collection), Vec<String>> = BTreeMap::new();
        for w in &batch.writes {
            if matches!(w.collection, Collection::ListBlock | Collection::ListItem) {
                stored_keys
                    .entry((&w.author, w.collection))
                    .or_default()
                    .push(w.rkey.as_str().to_owned());
            }
        }
        for ((author, collection), rkeys) in &stored_keys {
            let a = t.author(author).await?;
            for (owner, lrkey) in stored_list_targets(&mut t, &a, *collection, rkeys).await? {
                locks.add(&owner, &lrkey, *collection == Collection::ListBlock);
            }
        }
        let mut candidates: Vec<Vec<String>> = Vec::with_capacity(batch.reconciles.len());
        for r in &batch.reconciles {
            let a = t.author(&r.author).await?;
            let c = reconcile_candidates(&mut t, &a, r).await?;
            if matches!(r.collection, Collection::ListBlock | Collection::ListItem) {
                for (owner, lrkey) in stored_list_targets(&mut t, &a, r.collection, &c).await? {
                    locks.add(&owner, &lrkey, r.collection == Collection::ListBlock);
                }
            }
            if r.collection == Collection::List {
                for rkey in &c {
                    locks.add(a.did.as_str(), rkey, true);
                }
            }
            candidates.push(c);
        }

        // 3. List locks.
        t.lock_lists(&locks.0).await?;

        // 4. Writes, then reconciles.
        for w in &batch.writes {
            apply_write(&mut t, &batch.origin, w).await?;
        }
        for (r, c) in batch.reconciles.iter().zip(candidates) {
            apply_reconcile(&mut t, r, &c).await?;
        }

        // 5. Firehose progress and notify.
        if let Some(p) = &batch.firehose {
            t.record_firehose_progress(p).await?;
            t.notify = true;
        }
        if t.notify {
            t.send_notify().await?;
        }
        t.finish()
    };
    tx.commit().await?;
    Ok(parts)
}

async fn check_stamp_fresh(t: &mut Txn<'_>, stamp_read_at: DateTime<Utc>) -> Result<()> {
    let stale: bool =
        sqlx::query_scalar("SELECT clock_timestamp() - $1 >= $2 * interval '1 second'")
            .bind(stamp_read_at)
            .bind(STAMP_VALIDITY.as_secs_f64())
            .fetch_one(&mut *t.conn)
            .await?;
    if stale {
        return Err(StorageError::StaleStamp(stamp_read_at));
    }
    Ok(())
}

/// (owner DID, list rkey) of the lists that the author's stored rows at
/// `rkeys` point to.
async fn stored_list_targets(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    collection: Collection,
    rkeys: &[String],
) -> Result<Vec<(String, String)>> {
    if rkeys.is_empty() {
        return Ok(Vec::new());
    }
    let sql = match collection {
        Collection::ListBlock => {
            "SELECT a.did, l.rkey FROM list_blocks r
             JOIN lists l ON l.id = r.list_id JOIN actors a ON a.id = l.owner_id
             WHERE r.author_id = $1 AND r.rkey = ANY($2)"
        }
        Collection::ListItem => {
            "SELECT a.did, l.rkey FROM list_items r
             JOIN lists l ON l.id = r.list_id JOIN actors a ON a.id = l.owner_id
             WHERE r.owner_id = $1 AND r.rkey = ANY($2)"
        }
        _ => return Ok(Vec::new()),
    };
    Ok(sqlx::query_as(sql)
        .bind(author.id)
        .bind(rkeys)
        .fetch_all(&mut *t.conn)
        .await?)
}

fn cause_for(author: &AuthorInfo, origin: &Origin) -> Cause {
    match origin {
        Origin::Discovery { requester } => Cause {
            key: requester.clone(),
            buckets: Vec::new(),
            large: true,
            mask: 0,
        },
        _ => author.cause(),
    }
}

async fn apply_write(t: &mut Txn<'_>, origin: &Origin, w: &Write) -> Result<()> {
    let author = t.author(&w.author).await?;
    if let WriteAction::Upsert(record) = &w.action {
        if record.collection() != w.collection {
            return Err(StorageError::Invariant(format!(
                "write for {} carries a {} record",
                w.collection,
                record.collection()
            )));
        }
    }
    match &w.action {
        WriteAction::Delete => match w.collection {
            Collection::Block => block_delete(t, &author, w).await,
            Collection::ListBlock => listblock_delete(t, &author, w).await,
            Collection::List => list_delete(t, &author, w).await,
            Collection::ListItem => listitem_delete(t, &author, w).await,
        },
        WriteAction::Upsert(record) => {
            if let Origin::Listing {
                deletes_only: true, ..
            } = origin
            {
                return deletes_only_skip(t, &author, w).await;
            }
            let cause = cause_for(&author, origin);
            match record {
                Record::Block(r) => block_upsert(t, &author, &cause, w, r).await,
                Record::ListBlock(r) => listblock_upsert(t, &author, &cause, w, r).await,
                Record::List(r) => list_upsert(t, &author, &cause, w, r).await,
                Record::ListItem(r) => listitem_upsert(t, &author, &cause, w, r).await,
            }
        }
    }
}

/// Deletes-only mode: a would-be insert or update is skipped with a
/// `refused` debt; a stale version (LWW loser) is simply stale.
async fn deletes_only_skip(t: &mut Txn<'_>, author: &AuthorInfo, w: &Write) -> Result<()> {
    let rkey = w.rkey.as_str();
    let row_rev = stored_rev(t, w.collection, author.id, rkey).await?;
    let tomb = t.tombstone_rev(w.collection, author.id, rkey).await?;
    if !lww_upsert_wins(w.stamp, row_rev, tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    t.refuse(
        author,
        w.collection,
        rkey,
        Refusal::Refused(CapType::DeletesOnly),
        w.witness,
    )
    .await
}

async fn stored_rev(
    t: &mut Txn<'_>,
    c: Collection,
    author_id: i64,
    rkey: &str,
) -> Result<Option<i64>> {
    let sql = match c {
        Collection::Block => "SELECT rev FROM blocks WHERE author_id = $1 AND rkey = $2",
        Collection::ListBlock => "SELECT rev FROM list_blocks WHERE author_id = $1 AND rkey = $2",
        Collection::ListItem => "SELECT rev FROM list_items WHERE owner_id = $1 AND rkey = $2",
        Collection::List => {
            "SELECT rev FROM lists WHERE owner_id = $1 AND rkey = $2 AND record_state <> 0"
        }
    };
    let r: Option<Option<i64>> = sqlx::query_scalar(sql)
        .bind(author_id)
        .bind(rkey)
        .fetch_optional(&mut *t.conn)
        .await?;
    Ok(r.flatten())
}

// ---------------------------------------------------------------- blocks

async fn block_upsert(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &BlockRecord,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    let row: Option<(i64, i64)> =
        sqlx::query_as("SELECT subject_id, rev FROM blocks WHERE author_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    let tomb = t.tombstone_rev(Collection::Block, author.id, rkey).await?;
    if !lww_upsert_wins(w.stamp, row.map(|x| x.1), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    if let Some((old_subject, _)) = row {
        let same = t.actor_id(r.subject.as_str()).await? == Some(old_subject);
        if let Some(refusal) = t.gate(cause, CapKind::Blocks) {
            if !same {
                block_delete_row(t, author, rkey).await?;
                t.put_refusal_tombstone(Collection::Block, author.id, rkey, w.stamp)
                    .await?;
            }
            return t
                .refuse(author, Collection::Block, rkey, refusal, w.witness)
                .await;
        }
        let subject_id = if same {
            old_subject
        } else {
            match t.intern_actor(&r.subject, cause).await? {
                Ok(id) => id,
                Err(refusal) => {
                    block_delete_row(t, author, rkey).await?;
                    t.put_refusal_tombstone(Collection::Block, author.id, rkey, w.stamp)
                        .await?;
                    return t
                        .refuse(author, Collection::Block, rkey, refusal, w.witness)
                        .await;
                }
            }
        };
        // Subject change = delete + insert: authored_blocks is unchanged.
        sqlx::query(
            "UPDATE blocks SET subject_id = $3, created_at = $4, rev = $5
             WHERE author_id = $1 AND rkey = $2",
        )
        .bind(author.id)
        .bind(rkey)
        .bind(subject_id)
        .bind(r.created_at)
        .bind(w.stamp)
        .execute(&mut *t.conn)
        .await?;
        t.report.applied += 1;
        return Ok(());
    }
    match block_insert(t, author, cause, w, r).await? {
        Ok(()) => {
            t.report.applied += 1;
            Ok(())
        }
        Err(refusal) => {
            t.refuse(author, Collection::Block, rkey, refusal, w.witness)
                .await
        }
    }
}

async fn block_insert(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &BlockRecord,
) -> Result<Result<(), Refusal>> {
    if let Some(refusal) = t.gate(cause, CapKind::Blocks) {
        return Ok(Err(refusal));
    }
    let subject_id = match t.intern_actor(&r.subject, cause).await? {
        Ok(id) => id,
        Err(refusal) => return Ok(Err(refusal)),
    };
    let ok: Option<i32> = sqlx::query_scalar(
        "UPDATE actors SET authored_blocks = authored_blocks + 1
         WHERE id = $1 AND authored_blocks < $2 RETURNING authored_blocks",
    )
    .bind(author.id)
    .bind(clamp(t.limits.cfg.blocks_per_author))
    .fetch_optional(&mut *t.conn)
    .await?;
    if ok.is_none() {
        return Ok(Err(Refusal::Capped(CapType::BlocksPerAuthor)));
    }
    sqlx::query(
        "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(author.id)
    .bind(w.rkey.as_str())
    .bind(subject_id)
    .bind(r.created_at)
    .bind(w.stamp)
    .execute(&mut *t.conn)
    .await?;
    t.deltas.stat(stat::BLOCKS, 1);
    t.deltas.host(&author.buckets, CapKind::Blocks, 1);
    Ok(Ok(()))
}

/// Deletes one block row (with its counters). Returns whether a row went.
pub(crate) async fn block_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
) -> Result<bool> {
    let gone = sqlx::query("DELETE FROM blocks WHERE author_id = $1 AND rkey = $2")
        .bind(author.id)
        .bind(rkey)
        .execute(&mut *t.conn)
        .await?
        .rows_affected();
    if gone > 0 {
        sqlx::query("UPDATE actors SET authored_blocks = authored_blocks - 1 WHERE id = $1")
            .bind(author.id)
            .execute(&mut *t.conn)
            .await?;
        t.deltas.stat(stat::BLOCKS, -1);
        t.deltas.host(&author.buckets, CapKind::Blocks, -1);
    }
    Ok(gone > 0)
}

async fn block_delete(t: &mut Txn<'_>, author: &AuthorInfo, w: &Write) -> Result<()> {
    let rkey = w.rkey.as_str();
    let rev: Option<i64> =
        sqlx::query_scalar("SELECT rev FROM blocks WHERE author_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    if rev.is_some_and(|r| r < w.stamp) {
        block_delete_row(t, author, rkey).await?;
    }
    t.put_tombstone(Collection::Block, author.id, rkey, w.stamp)
        .await?;
    t.report.applied += 1;
    Ok(())
}

// ------------------------------------------------------------ listblocks

/// The stored listblock row.
struct ListBlockRow {
    list_id: i64,
    rev: i64,
}

async fn listblock_row(
    t: &mut Txn<'_>,
    author_id: i64,
    rkey: &str,
) -> Result<Option<ListBlockRow>> {
    let r: Option<(i64, i64)> =
        sqlx::query_as("SELECT list_id, rev FROM list_blocks WHERE author_id = $1 AND rkey = $2")
            .bind(author_id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    Ok(r.map(|(list_id, rev)| ListBlockRow { list_id, rev }))
}

async fn find_list(t: &mut Txn<'_>, owner: &str, rkey: &str) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT l.id FROM lists l JOIN actors a ON a.id = l.owner_id
         WHERE a.did = $1 AND l.rkey = $2",
    )
    .bind(owner)
    .bind(rkey)
    .fetch_optional(&mut *t.conn)
    .await?)
}

async fn listblock_upsert(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &ListBlockRecord,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    let row = listblock_row(t, author.id, rkey).await?;
    let tomb = t
        .tombstone_rev(Collection::ListBlock, author.id, rkey)
        .await?;
    if !lww_upsert_wins(w.stamp, row.as_ref().map(|x| x.rev), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    let target = find_list(t, r.subject.authority.as_str(), r.subject.rkey.as_str()).await?;
    if let Some(old) = row {
        if Some(old.list_id) == target {
            // Same subject: counted, witnessed_at and sched_key are sticky.
            if let Some(refusal) = t.gate(cause, CapKind::Listblocks) {
                return t
                    .refuse(author, Collection::ListBlock, rkey, refusal, w.witness)
                    .await;
            }
            sqlx::query(
                "UPDATE list_blocks SET created_at = $3, rev = $4
                 WHERE author_id = $1 AND rkey = $2",
            )
            .bind(author.id)
            .bind(rkey)
            .bind(r.created_at)
            .bind(w.stamp)
            .execute(&mut *t.conn)
            .await?;
            t.report.applied += 1;
            return Ok(());
        }
        // Subject change: delete of the old row plus a new insert (§4.2).
        listblock_delete_row(t, author, rkey).await?;
        return match listblock_insert(t, author, cause, w, r).await? {
            Ok(()) => {
                t.report.applied += 1;
                Ok(())
            }
            Err(refusal) => {
                t.put_refusal_tombstone(Collection::ListBlock, author.id, rkey, w.stamp)
                    .await?;
                t.refuse(author, Collection::ListBlock, rkey, refusal, w.witness)
                    .await
            }
        };
    }
    match listblock_insert(t, author, cause, w, r).await? {
        Ok(()) => {
            t.report.applied += 1;
            Ok(())
        }
        Err(refusal) => {
            t.refuse(author, Collection::ListBlock, rkey, refusal, w.witness)
                .await
        }
    }
}

/// Whether counting a new listblock on this list would admit it (§4.2):
/// count 0 and not tracked, excluding the cells where `+` does not admit
/// (a deleted record goes to `dead`; a purge to `dead` stays).
fn would_admit(
    state: TrackState,
    record_state: RecordState,
    count: i32,
    purge_then: Option<TrackState>,
) -> bool {
    count == 0
        && match state {
            TrackState::Untracked => record_state != RecordState::Deleted,
            TrackState::Purging => purge_then != Some(TrackState::Dead),
            _ => false,
        }
}

/// Decides `counted` for a new counted-candidate listblock of `author` on
/// `list_id`: the trigger cap, and the admission rate if it would admit.
/// On success the author's `fetch_triggers` (and the rate) are charged.
pub(crate) async fn decide_counted(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    list_id: i64,
) -> Result<Result<(), CapType>> {
    let l = t.list_tracking(list_id).await?;
    let ok: Option<i32> = sqlx::query_scalar(
        "UPDATE actors SET fetch_triggers = fetch_triggers + 1
         WHERE id = $1 AND fetch_triggers < $2 RETURNING fetch_triggers",
    )
    .bind(author.id)
    .bind(clamp(t.limits.cfg.listblock_fetch_triggers_per_author))
    .fetch_optional(&mut *t.conn)
    .await?;
    if ok.is_none() {
        return Ok(Err(CapType::TriggerCap));
    }
    if would_admit(l.state, l.record_state, l.listblock_count, l.purge_then)
        && !t.charge_admission(&author.key).await?
    {
        sqlx::query("UPDATE actors SET fetch_triggers = fetch_triggers - 1 WHERE id = $1")
            .bind(author.id)
            .execute(&mut *t.conn)
            .await?;
        return Ok(Err(CapType::AdmissionRate));
    }
    Ok(Ok(()))
}

async fn listblock_insert(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &ListBlockRecord,
) -> Result<Result<(), Refusal>> {
    if let Some(refusal) = t.gate(cause, CapKind::Listblocks) {
        return Ok(Err(refusal));
    }
    let list_id = match t
        .intern_list(&r.subject.authority, r.subject.rkey.as_str(), cause)
        .await?
    {
        Ok(id) => id,
        Err(refusal) => return Ok(Err(refusal)),
    };
    let ok: Option<i32> = sqlx::query_scalar(
        "UPDATE actors SET authored_listblocks = authored_listblocks + 1
         WHERE id = $1 AND authored_listblocks < $2 RETURNING authored_listblocks",
    )
    .bind(author.id)
    .bind(clamp(t.limits.cfg.listblocks_per_author))
    .fetch_optional(&mut *t.conn)
    .await?;
    if ok.is_none() {
        return Ok(Err(Refusal::Capped(CapType::ListblocksPerAuthor)));
    }
    let counted = decide_counted(t, author, list_id).await?;
    sqlx::query(
        "INSERT INTO list_blocks (author_id, rkey, list_id, counted, witnessed_at, sched_key,
                                  created_at, rev)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(author.id)
    .bind(w.rkey.as_str())
    .bind(list_id)
    .bind(counted.is_ok())
    .bind(w.witness)
    .bind(author.key.as_str())
    .bind(r.created_at)
    .bind(w.stamp)
    .execute(&mut *t.conn)
    .await?;
    t.deltas.stat(stat::LIST_BLOCKS, 1);
    t.deltas.host(&author.buckets, CapKind::Listblocks, 1);
    match counted {
        Ok(()) => {
            t.change_listblock_count(list_id, 1, Some(author.key.as_str()))
                .await?;
        }
        Err(cap) => {
            // Stored uncounted: not a refusal, but a `capped` debt (§4.2).
            t.add_debt(author.id, DebtReason::Capped, Some(cap), w.witness)
                .await?;
            t.report.uncounted += 1;
        }
    }
    Ok(Ok(()))
}

/// The counter path (§4.2): deletes one listblock row and, if it was
/// counted, decrements `listblock_count`, `fetch_triggers` and the lane
/// of its stored `sched_key`, firing **−** on 1 → 0. Every path deleting
/// `list_blocks` rows uses this function. The caller holds author(A) and
/// list(L) exclusive.
pub(crate) async fn listblock_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
) -> Result<bool> {
    let gone: Option<(i64, bool, Option<String>)> = sqlx::query_as(
        "DELETE FROM list_blocks WHERE author_id = $1 AND rkey = $2
         RETURNING list_id, counted, sched_key",
    )
    .bind(author.id)
    .bind(rkey)
    .fetch_optional(&mut *t.conn)
    .await?;
    let Some((list_id, counted, sched_key)) = gone else {
        return Ok(false);
    };
    sqlx::query("UPDATE actors SET authored_listblocks = authored_listblocks - 1 WHERE id = $1")
        .bind(author.id)
        .execute(&mut *t.conn)
        .await?;
    t.deltas.stat(stat::LIST_BLOCKS, -1);
    t.deltas.host(&author.buckets, CapKind::Listblocks, -1);
    if counted {
        sqlx::query("UPDATE actors SET fetch_triggers = fetch_triggers - 1 WHERE id = $1")
            .bind(author.id)
            .execute(&mut *t.conn)
            .await?;
        t.change_listblock_count(list_id, -1, sched_key.as_deref())
            .await?;
    }
    Ok(true)
}

async fn listblock_delete(t: &mut Txn<'_>, author: &AuthorInfo, w: &Write) -> Result<()> {
    let rkey = w.rkey.as_str();
    if let Some(row) = listblock_row(t, author.id, rkey).await? {
        if row.rev < w.stamp {
            listblock_delete_row(t, author, rkey).await?;
        }
    }
    t.put_tombstone(Collection::ListBlock, author.id, rkey, w.stamp)
        .await?;
    t.report.applied += 1;
    Ok(())
}

// -------------------------------------------------------------- listitems

struct ItemRow {
    list_id: i64,
    subject_id: i64,
    rev: i64,
}

async fn item_row(t: &mut Txn<'_>, owner_id: i64, rkey: &str) -> Result<Option<ItemRow>> {
    let r: Option<(i64, i64, i64)> = sqlx::query_as(
        "SELECT list_id, subject_id, rev FROM list_items WHERE owner_id = $1 AND rkey = $2",
    )
    .bind(owner_id)
    .bind(rkey)
    .fetch_optional(&mut *t.conn)
    .await?;
    Ok(r.map(|(list_id, subject_id, rev)| ItemRow {
        list_id,
        subject_id,
        rev,
    }))
}

/// Marks a list `capped` because one of its items was refused (§4.7,
/// §11.2); `refresh` also requests a refresh run (intern-rate refusals).
async fn mark_list_capped(t: &mut Txn<'_>, list_id: i64, refresh: bool) -> Result<()> {
    sqlx::query(
        "UPDATE lists SET capped = true, refresh_requested = refresh_requested OR $2
         WHERE id = $1",
    )
    .bind(list_id)
    .bind(refresh)
    .execute(&mut *t.conn)
    .await?;
    t.notify = true;
    Ok(())
}

async fn listitem_upsert(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &ListItemRecord,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    let row = item_row(t, author.id, rkey).await?;
    let tomb = t
        .tombstone_rev(Collection::ListItem, author.id, rkey)
        .await?;
    if !lww_upsert_wins(w.stamp, row.as_ref().map(|x| x.rev), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    // Authority rule held at parse time: r.list.authority == author.
    let target: Option<(i64, i16)> =
        sqlx::query_as("SELECT id, track_state FROM lists WHERE owner_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(r.list.rkey.as_str())
            .fetch_optional(&mut *t.conn)
            .await?;
    let tracked = target
        .and_then(|(_, s)| TrackState::from_code(s))
        .is_some_and(TrackState::is_tracked);
    let target_id = target.map(|(id, _)| id);

    if let Some(old) = &row {
        let same_list = Some(old.list_id) == target_id;
        let same_subject = t.actor_id(r.subject.as_str()).await? == Some(old.subject_id);
        if same_list && same_subject && tracked {
            if let Some(refusal) = t.gate(cause, CapKind::Items) {
                return t
                    .refuse(author, Collection::ListItem, rkey, refusal, w.witness)
                    .await;
            }
            sqlx::query(
                "UPDATE list_items SET created_at = $3, rev = $4 WHERE owner_id = $1 AND rkey = $2",
            )
            .bind(author.id)
            .bind(rkey)
            .bind(r.created_at)
            .bind(w.stamp)
            .execute(&mut *t.conn)
            .await?;
            t.report.applied += 1;
            return Ok(());
        }
        // Changed (list or subject) or no longer tracked: delete the old
        // version, then try the new one as an insert.
        item_delete_row(t, author, rkey).await?;
        let result = if tracked {
            item_insert(t, author, cause, w, r, target_id.unwrap_or_default()).await?
        } else {
            Err(ItemRefusal::Untracked)
        };
        return match result {
            Ok(()) => {
                t.report.applied += 1;
                Ok(())
            }
            Err(refusal) => {
                // §4.4 notes: refused new version with a stored row ⇒
                // delete + refusal tombstone at E − 1.
                t.put_refusal_tombstone(Collection::ListItem, author.id, rkey, w.stamp)
                    .await?;
                item_refused(t, author, rkey, w, refusal).await
            }
        };
    }
    if !tracked {
        t.report.untracked_items += 1;
        return Ok(());
    }
    match item_insert(t, author, cause, w, r, target_id.unwrap_or_default()).await? {
        Ok(()) => {
            t.report.applied += 1;
            Ok(())
        }
        Err(refusal) => item_refused(t, author, rkey, w, refusal).await,
    }
}

/// Why an item was not stored.
enum ItemRefusal {
    /// The list is not tracked: refused, no debt (costs nothing, §11.1).
    Untracked,
    /// A per-list or per-owner item cap: the list is marked `capped`.
    ListCap,
    /// A gate or rate: debt, and the list is marked `capped`.
    Debt(Refusal),
}

async fn item_refused(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
    w: &Write,
    refusal: ItemRefusal,
) -> Result<()> {
    match refusal {
        ItemRefusal::Untracked => {
            t.report.untracked_items += 1;
            Ok(())
        }
        ItemRefusal::ListCap => {
            t.report.refused += 1;
            Ok(())
        }
        ItemRefusal::Debt(r) => {
            t.refuse(author, Collection::ListItem, rkey, r, w.witness)
                .await
        }
    }
}

async fn item_insert(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &ListItemRecord,
    list_id: i64,
) -> Result<Result<(), ItemRefusal>> {
    if let Some(refusal) = t.gate(cause, CapKind::Items) {
        mark_list_capped(t, list_id, false).await?;
        return Ok(Err(ItemRefusal::Debt(refusal)));
    }
    let subject_id = match t.intern_actor(&r.subject, cause).await? {
        Ok(id) => id,
        Err(refusal) => {
            // Intern-rate refusals request a refresh for the next day.
            let refresh = refusal == Refusal::Capped(CapType::InternRate);
            mark_list_capped(t, list_id, refresh).await?;
            return Ok(Err(ItemRefusal::Debt(refusal)));
        }
    };
    let per_list: Option<i32> = sqlx::query_scalar(
        "UPDATE lists SET item_count = item_count + 1
         WHERE id = $1 AND item_count < $2 RETURNING item_count",
    )
    .bind(list_id)
    .bind(clamp(t.limits.cfg.list_items_per_list))
    .fetch_optional(&mut *t.conn)
    .await?;
    if per_list.is_none() {
        mark_list_capped(t, list_id, false).await?;
        return Ok(Err(ItemRefusal::ListCap));
    }
    let per_owner: Option<i32> = sqlx::query_scalar(
        "UPDATE actors SET owned_items = owned_items + 1
         WHERE id = $1 AND owned_items < $2 RETURNING owned_items",
    )
    .bind(author.id)
    .bind(clamp(t.limits.cfg.list_items_per_owner))
    .fetch_optional(&mut *t.conn)
    .await?;
    if per_owner.is_none() {
        sqlx::query("UPDATE lists SET item_count = item_count - 1 WHERE id = $1")
            .bind(list_id)
            .execute(&mut *t.conn)
            .await?;
        mark_list_capped(t, list_id, false).await?;
        return Ok(Err(ItemRefusal::ListCap));
    }
    sqlx::query(
        "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(author.id)
    .bind(w.rkey.as_str())
    .bind(list_id)
    .bind(subject_id)
    .bind(r.created_at)
    .bind(w.stamp)
    .execute(&mut *t.conn)
    .await?;
    t.deltas.stat(stat::LIST_ITEMS, 1);
    t.deltas.host(&author.buckets, CapKind::Items, 1);
    Ok(Ok(()))
}

/// Deletes one listitem row and its counters. The caller holds author(O)
/// and list(L) (shared suffices: all writers of L's items hold author(O)).
pub(crate) async fn item_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
) -> Result<bool> {
    let list_id: Option<i64> = sqlx::query_scalar(
        "DELETE FROM list_items WHERE owner_id = $1 AND rkey = $2 RETURNING list_id",
    )
    .bind(author.id)
    .bind(rkey)
    .fetch_optional(&mut *t.conn)
    .await?;
    let Some(list_id) = list_id else {
        return Ok(false);
    };
    sqlx::query("UPDATE lists SET item_count = item_count - 1 WHERE id = $1")
        .bind(list_id)
        .execute(&mut *t.conn)
        .await?;
    sqlx::query("UPDATE actors SET owned_items = owned_items - 1 WHERE id = $1")
        .bind(author.id)
        .execute(&mut *t.conn)
        .await?;
    t.deltas.stat(stat::LIST_ITEMS, -1);
    t.deltas.host(&author.buckets, CapKind::Items, -1);
    Ok(true)
}

async fn listitem_delete(t: &mut Txn<'_>, author: &AuthorInfo, w: &Write) -> Result<()> {
    let rkey = w.rkey.as_str();
    if let Some(row) = item_row(t, author.id, rkey).await? {
        if row.rev < w.stamp {
            item_delete_row(t, author, rkey).await?;
        }
    }
    t.put_tombstone(Collection::ListItem, author.id, rkey, w.stamp)
        .await?;
    t.report.applied += 1;
    Ok(())
}

// ------------------------------------------------------------------ lists

async fn list_upsert(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &ListRecord,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    let row: Option<(i64, Option<i64>, i16)> =
        sqlx::query_as("SELECT id, rev, record_state FROM lists WHERE owner_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    let tomb = t.tombstone_rev(Collection::List, author.id, rkey).await?;
    if !lww_upsert_wins(w.stamp, row.and_then(|x| x.1), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    let was_present = row.is_some_and(|x| x.2 == RecordState::Present.code());
    if let Some(refusal) = t.gate(cause, CapKind::Lists) {
        return t
            .refuse(author, Collection::List, rkey, refusal, w.witness)
            .await;
    }
    if !was_present {
        let ok: Option<i32> = sqlx::query_scalar(
            "UPDATE actors SET authored_lists = authored_lists + 1
             WHERE id = $1 AND authored_lists < $2 RETURNING authored_lists",
        )
        .bind(author.id)
        .bind(clamp(t.limits.cfg.lists_per_author))
        .fetch_optional(&mut *t.conn)
        .await?;
        if ok.is_none() {
            return t
                .refuse(
                    author,
                    Collection::List,
                    rkey,
                    Refusal::Capped(CapType::ListsPerAuthor),
                    w.witness,
                )
                .await;
        }
        t.deltas.stat(stat::LISTS, 1);
        t.deltas.host(&author.buckets, CapKind::Lists, 1);
    }
    let list_id: i64 = sqlx::query_scalar(
        "INSERT INTO lists (owner_id, rkey, record_state, purpose, name, created_at, rev)
         VALUES ($1, $2, 1, $3, $4, $5, $6)
         ON CONFLICT (owner_id, rkey) DO UPDATE SET record_state = 1,
           purpose = EXCLUDED.purpose, name = EXCLUDED.name,
           created_at = EXCLUDED.created_at, rev = EXCLUDED.rev
         RETURNING id",
    )
    .bind(author.id)
    .bind(rkey)
    .bind(r.purpose.code())
    .bind(r.name.as_deref())
    .bind(r.created_at)
    .bind(w.stamp)
    .fetch_one(&mut *t.conn)
    .await?;
    t.report.applied += 1;
    // "Any list record apply for L fires RP" (§5.5).
    t.fire(list_id, Event::RecordPresent, FireArgs::default())
        .await?;
    Ok(())
}

/// Marks a list record deleted at stamp `w` (firehose delete or
/// reconcile) and fires **RD**. The row is kept: other authors'
/// listblocks point at it (§7.4).
pub(crate) async fn list_mark_deleted(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    list_id: i64,
    was_present: bool,
    w: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE lists SET record_state = 2, purpose = NULL, name = NULL, created_at = NULL,
           rev = GREATEST(COALESCE(rev, $2), $2)
         WHERE id = $1",
    )
    .bind(list_id)
    .bind(w)
    .execute(&mut *t.conn)
    .await?;
    if was_present {
        sqlx::query("UPDATE actors SET authored_lists = authored_lists - 1 WHERE id = $1")
            .bind(author.id)
            .execute(&mut *t.conn)
            .await?;
        t.deltas.stat(stat::LISTS, -1);
        t.deltas.host(&author.buckets, CapKind::Lists, -1);
    }
    t.fire(list_id, Event::RecordDeleted, FireArgs::default())
        .await?;
    Ok(())
}

async fn list_delete(t: &mut Txn<'_>, author: &AuthorInfo, w: &Write) -> Result<()> {
    let rkey = w.rkey.as_str();
    let row: Option<(i64, Option<i64>, i16)> =
        sqlx::query_as("SELECT id, rev, record_state FROM lists WHERE owner_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    if let Some((id, rev, state)) = row {
        if rev.is_none_or(|r| r < w.stamp) {
            let was_present = state == RecordState::Present.code();
            list_mark_deleted(t, author, id, was_present, w.stamp).await?;
        }
    }
    t.put_tombstone(Collection::List, author.id, rkey, w.stamp)
        .await?;
    t.report.applied += 1;
    Ok(())
}

// -------------------------------------------------------------- reconcile

async fn reconcile_candidates(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    r: &Reconcile,
) -> Result<Vec<String>> {
    let sql = match r.collection {
        Collection::Block => {
            "SELECT rkey FROM blocks WHERE author_id = $1 AND rev < $2
               AND ($3::text IS NULL OR rkey > $3) AND ($4::text IS NULL OR rkey <= $4)
               AND NOT (rkey = ANY($5)) ORDER BY rkey"
        }
        Collection::ListBlock => {
            "SELECT rkey FROM list_blocks WHERE author_id = $1 AND rev < $2
               AND ($3::text IS NULL OR rkey > $3) AND ($4::text IS NULL OR rkey <= $4)
               AND NOT (rkey = ANY($5)) ORDER BY rkey"
        }
        Collection::ListItem => {
            "SELECT rkey FROM list_items WHERE owner_id = $1 AND rev < $2
               AND ($3::text IS NULL OR rkey > $3) AND ($4::text IS NULL OR rkey <= $4)
               AND NOT (rkey = ANY($5)) ORDER BY rkey"
        }
        Collection::List => {
            "SELECT rkey FROM lists WHERE owner_id = $1 AND record_state = 1
               AND (rev IS NULL OR rev < $2)
               AND ($3::text IS NULL OR rkey > $3) AND ($4::text IS NULL OR rkey <= $4)
               AND NOT (rkey = ANY($5)) ORDER BY rkey"
        }
    };
    let keep: Vec<String> = r.keep.iter().map(|k| k.as_str().to_owned()).collect();
    Ok(sqlx::query_scalar(sql)
        .bind(author.id)
        .bind(r.stamp)
        .bind(r.after.as_ref().map(|k| k.as_str().to_owned()))
        .bind(r.through.as_ref().map(|k| k.as_str().to_owned()))
        .bind(keep)
        .fetch_all(&mut *t.conn)
        .await?)
}

async fn apply_reconcile(t: &mut Txn<'_>, r: &Reconcile, candidates: &[String]) -> Result<()> {
    let author = t.author(&r.author).await?;
    for rkey in candidates {
        // Re-check `rev < R` (a write earlier in this batch may have
        // re-stamped the row).
        let rev = stored_rev(t, r.collection, author.id, rkey).await?;
        let gone = match (r.collection, rev) {
            (_, Some(rev)) if rev >= r.stamp => false,
            (Collection::Block, Some(_)) => block_delete_row(t, &author, rkey).await?,
            (Collection::ListBlock, Some(_)) => listblock_delete_row(t, &author, rkey).await?,
            (Collection::ListItem, Some(_)) => item_delete_row(t, &author, rkey).await?,
            (Collection::List, _) => {
                let row: Option<(i64, i16)> = sqlx::query_as(
                    "SELECT id, record_state FROM lists WHERE owner_id = $1 AND rkey = $2",
                )
                .bind(author.id)
                .bind(rkey.as_str())
                .fetch_optional(&mut *t.conn)
                .await?;
                match row {
                    Some((id, state)) if state == RecordState::Present.code() => {
                        list_mark_deleted(t, &author, id, true, r.stamp).await?;
                        true
                    }
                    _ => false,
                }
            }
            (_, None) => false,
        };
        if gone {
            t.report.reconciled += 1;
        }
    }
    Ok(())
}

fn clamp(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn would_admit_cells() {
        use RecordState as R;
        use TrackState as S;
        assert!(would_admit(S::Untracked, R::Unknown, 0, None));
        assert!(would_admit(S::Untracked, R::Present, 0, None));
        assert!(!would_admit(S::Untracked, R::Deleted, 0, None));
        assert!(would_admit(S::Purging, R::Present, 0, Some(S::Untracked)));
        assert!(would_admit(S::Purging, R::Present, 0, Some(S::Missing)));
        assert!(!would_admit(S::Purging, R::Present, 0, Some(S::Dead)));
        // Tracked lists never consume rate (retained + is not an admission).
        assert!(!would_admit(S::Retained, R::Present, 0, None));
        assert!(!would_admit(S::Ready, R::Present, 3, None));
        assert!(!would_admit(S::Untracked, R::Present, 2, None));
    }

    #[test]
    fn backoff_grows() {
        assert!(deadlock_backoff(1) >= Duration::from_millis(5));
        assert!(deadlock_backoff(4) >= Duration::from_millis(20));
        assert!(deadlock_backoff(4) < Duration::from_millis(40));
    }

    #[test]
    fn list_locks_upgrade_to_exclusive() {
        let mut l = ListLocks::default();
        l.add("did:plc:a", "1", false);
        l.add("did:plc:a", "1", true);
        l.add("did:plc:a", "1", false);
        assert_eq!(l.0.len(), 1);
        assert!(l.0.values().all(|e| *e));
    }
}
