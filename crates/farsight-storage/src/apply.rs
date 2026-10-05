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
use crate::history::{Cause as Removed, Gone, Removal};
use crate::keys::{self, CapKind, Limits};
use crate::repo_events::{RepoEvent, unavailable_list_keys};
use crate::tracking::FireArgs;
use crate::transition::Event;
use crate::txn::{
    ApplyReport, AuthorInfo, Cause, Gates, Refusal, Txn, WriteOutcome, lww_upsert_wins,
};

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
    /// Non-commit firehose events, applied after the writes (§6.4).
    pub events: Vec<RepoEvent>,
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
            events: Vec::new(),
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

/// Backoff before retry `attempt` (1-based): exponential from 10 ms,
/// capped at 1 s, plus up to the same again as jitter, so the aborted
/// writer re-enters after the surviving one has usually committed and two
/// colliding writers do not retry in lockstep.
pub fn deadlock_backoff(attempt: u32) -> Duration {
    let base = 10u64 << attempt.saturating_sub(1).min(7);
    let base = base.min(1_000);
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()) % (base + 1))
        .unwrap_or(0);
    Duration::from_millis(base + jitter)
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
        let mut t = Txn::start(&mut tx, ctx.limits, ctx.gates).await?;
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
        for e in &batch.events {
            authors.insert(e.did());
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
        // Discovery reads only: an author without an `actors` row has no
        // stored rows, and its row is created later, under its intern lock.
        for ((author, collection), rkeys) in &stored_keys {
            let Some(author_id) = t.actor_id(author.as_str()).await? else {
                continue;
            };
            for (owner, lrkey) in stored_list_targets(&mut t, author_id, *collection, rkeys).await?
            {
                locks.add(&owner, &lrkey, *collection == Collection::ListBlock);
            }
        }
        let mut candidates: Vec<Vec<String>> = Vec::with_capacity(batch.reconciles.len());
        for r in &batch.reconciles {
            let Some(author_id) = t.actor_id(r.author.as_str()).await? else {
                candidates.push(Vec::new());
                continue;
            };
            let c = reconcile_candidates(&mut t, author_id, r).await?;
            if matches!(r.collection, Collection::ListBlock | Collection::ListItem) {
                for (owner, lrkey) in
                    stored_list_targets(&mut t, author_id, r.collection, &c).await?
                {
                    locks.add(&owner, &lrkey, r.collection == Collection::ListBlock);
                }
            }
            if r.collection == Collection::List {
                for rkey in &c {
                    locks.add(r.author.as_str(), rkey, true);
                }
            }
            candidates.push(c);
        }

        // A reactivation fires OA on the DID's unavailable lists.
        for e in batch.events.iter().filter(|e| e.may_reactivate()) {
            for (_, lrkey) in unavailable_list_keys(&mut t, e.did()).await? {
                locks.add(e.did().as_str(), &lrkey, true);
            }
        }

        // 3. List locks.
        t.lock_lists(&locks.0).await?;

        // 3b. Intern locks for every DID this batch may create an `actors`
        // row for, ascending. Without them two batches interning the same
        // new DIDs in opposite orders wait on each other's uncommitted
        // inserts into the unique index and deadlock repeatedly (observed:
        // retries exhausted). Taken last, so the global order is authors,
        // lists, interns.
        let mut dids: BTreeSet<&str> = authors.iter().map(|d| d.as_str()).collect();
        for w in &batch.writes {
            match &w.action {
                WriteAction::Upsert(Record::Block(r)) => {
                    dids.insert(r.subject.as_str());
                }
                WriteAction::Upsert(Record::ListBlock(r)) => {
                    dids.insert(r.subject.authority.as_str());
                }
                WriteAction::Upsert(Record::ListItem(r)) => {
                    dids.insert(r.subject.as_str());
                }
                _ => {}
            }
        }
        t.lock_new_dids(&dids.into_iter().collect::<Vec<_>>())
            .await?;

        // 4. Writes, then reconciles.
        for w in &batch.writes {
            let before = (t.report.stale, t.report.refused, t.report.untracked_items);
            apply_write(&mut t, &batch.origin, w).await?;
            let after = (t.report.stale, t.report.refused, t.report.untracked_items);
            let outcome = if after.0 > before.0 {
                WriteOutcome::Stale
            } else if after.1 > before.1 {
                WriteOutcome::Refused
            } else if after.2 > before.2 {
                WriteOutcome::Dropped
            } else {
                WriteOutcome::Applied
            };
            t.report.write_outcomes.push(outcome);
        }
        for (r, c) in batch.reconciles.iter().zip(candidates) {
            apply_reconcile(&mut t, r, &c).await?;
        }
        // §6.4 (r17, T4): a DID's first authored indexed record seen on the
        // firehose interns it (above) and takes the active-DID branch here:
        // a tier-2 repo job, so its pre-existing records get listed.
        if batch.origin == Origin::Firehose && !batch.writes.is_empty() {
            let new: Vec<i64> = std::mem::take(&mut t.report.new_authors);
            for id in &new {
                crate::queue::enqueue(
                    &mut *t.conn,
                    *id,
                    crate::queue::JobKind::Repo,
                    2,
                    crate::repo_events::priority::NORMAL,
                    crate::queue::SYSTEM_FIREHOSE,
                    None,
                )
                .await?;
            }
            t.report.new_authors = new;
        }
        let cap = ctx.limits.system_queue_cap;
        for e in &batch.events {
            t.apply_repo_event(e, cap).await?;
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
    author_id: i64,
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
        .bind(author_id)
        .bind(rkeys)
        .fetch_all(&mut *t.conn)
        .await?)
}

/// The removal a write causes (§7.7): a firehose event names its commit
/// rev and its witness; a listing or discovery write knows neither.
fn removal_for(origin: &Origin, w: &Write, cause: Removed) -> Removal {
    match origin {
        Origin::Firehose => Removal {
            cause,
            rev: Some(w.stamp),
            witness: w.witness,
        },
        _ => Removal::listing(cause),
    }
}

/// The witness a write stamps on the row it stores (§7.7).
fn seen_for(t: &Txn<'_>, origin: &Origin, w: &Write) -> DateTime<Utc> {
    match origin {
        Origin::Firehose => t.seen_at(w.witness),
        _ => t.seen_at(None),
    }
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
            Collection::Block => block_delete(t, origin, &author, w).await,
            Collection::ListBlock => listblock_delete(t, origin, &author, w).await,
            Collection::List => list_delete(t, &author, w).await,
            Collection::ListItem => listitem_delete(t, origin, &author, w).await,
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
                Record::Block(r) => block_upsert(t, origin, &author, &cause, w, r).await,
                Record::ListBlock(r) => listblock_upsert(t, origin, &author, &cause, w, r).await,
                Record::List(r) => list_upsert(t, &author, &cause, w, r).await,
                Record::ListItem(r) => listitem_upsert(t, origin, &author, &cause, w, r).await,
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
    origin: &Origin,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &BlockRecord,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    let row: Option<BlockRow> = sqlx::query_as(
        "SELECT subject_id, rev, created_at, first_seen, last_seen FROM blocks
         WHERE author_id = $1 AND rkey = $2",
    )
    .bind(author.id)
    .bind(rkey)
    .fetch_optional(&mut *t.conn)
    .await?;
    let tomb = t.tombstone_rev(Collection::Block, author.id, rkey).await?;
    if !lww_upsert_wins(w.stamp, row.map(|x| x.1), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    let seen = seen_for(t, origin, w);
    if let Some((old_subject, _, old_created, old_first, old_last)) = row {
        let same = t.actor_id(r.subject.as_str()).await? == Some(old_subject);
        let refused = removal_for(origin, w, Removed::RefusedUpdate);
        if let Some(refusal) = t.gate(cause, CapKind::Blocks) {
            if !same {
                block_delete_row(t, author, rkey, Some(&refused)).await?;
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
                    block_delete_row(t, author, rkey, Some(&refused)).await?;
                    t.put_refusal_tombstone(Collection::Block, author.id, rkey, w.stamp)
                        .await?;
                    return t
                        .refuse(author, Collection::Block, rkey, refusal, w.witness)
                        .await;
                }
            }
        };
        if !same {
            // Subject change = removal of the old target + a fresh insert,
            // done in place: authored_blocks is unchanged, the old target
            // goes to history and the witness bounds start again (§7.2).
            let gone = Gone {
                rkey,
                created_at: old_created,
                first_seen: old_first,
                last_seen: old_last,
            };
            let removal = removal_for(origin, w, Removed::SubjectChange);
            t.record_block_removal(author, &gone, old_subject, &removal)
                .await?;
        }
        sqlx::query(
            "UPDATE blocks SET subject_id = $3, created_at = $4, rev = $5,
               first_seen = CASE WHEN $6 THEN first_seen ELSE $7 END,
               last_seen = CASE WHEN $6 THEN GREATEST(last_seen, $7) ELSE $7 END
             WHERE author_id = $1 AND rkey = $2",
        )
        .bind(author.id)
        .bind(rkey)
        .bind(subject_id)
        .bind(r.created_at)
        .bind(w.stamp)
        .bind(same)
        .bind(seen)
        .execute(&mut *t.conn)
        .await?;
        t.report.applied += 1;
        return Ok(());
    }
    match block_insert(t, author, cause, w, r, seen).await? {
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

/// `subject_id, rev, created_at, first_seen, last_seen` of a stored block.
type BlockRow = (
    i64,
    i64,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

async fn block_insert(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    cause: &Cause,
    w: &Write,
    r: &BlockRecord,
    seen: DateTime<Utc>,
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
        "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
         VALUES ($1, $2, $3, $4, $5, $6, $6)",
    )
    .bind(author.id)
    .bind(w.rkey.as_str())
    .bind(subject_id)
    .bind(r.created_at)
    .bind(w.stamp)
    .bind(seen)
    .execute(&mut *t.conn)
    .await?;
    t.deltas.stat(stat::BLOCKS, 1);
    t.deltas.host(&author.buckets, CapKind::Blocks, 1);
    Ok(Ok(()))
}

/// `subject_id, created_at, first_seen, last_seen` of a deleted block.
type BlockGone = (
    i64,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

/// `list_id, counted, sched_key, created_at, first_seen, last_seen` of a
/// deleted listblock.
type ListBlockGone = (
    i64,
    bool,
    Option<String>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

/// `list_id, subject_id, created_at, first_seen, last_seen` of a deleted
/// listitem.
type ItemGone = (
    i64,
    i64,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

/// Deletes one block row (with its counters). Returns whether a row went.
/// Every path deleting `blocks` rows uses this function; it is also where
/// `blocks_history` is written (§7.7): the caller names the removal, and
/// the account and divergence purges name none.
pub(crate) async fn block_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
    removal: Option<&Removal>,
) -> Result<bool> {
    let gone: Option<BlockGone> = sqlx::query_as(
        "DELETE FROM blocks WHERE author_id = $1 AND rkey = $2
             RETURNING subject_id, created_at, first_seen, last_seen",
    )
    .bind(author.id)
    .bind(rkey)
    .fetch_optional(&mut *t.conn)
    .await?;
    let Some((subject_id, created_at, first_seen, last_seen)) = gone else {
        return Ok(false);
    };
    sqlx::query("UPDATE actors SET authored_blocks = authored_blocks - 1 WHERE id = $1")
        .bind(author.id)
        .execute(&mut *t.conn)
        .await?;
    t.deltas.stat(stat::BLOCKS, -1);
    t.deltas.host(&author.buckets, CapKind::Blocks, -1);
    if let Some(r) = removal {
        let gone = Gone {
            rkey,
            created_at,
            first_seen,
            last_seen,
        };
        t.record_block_removal(author, &gone, subject_id, r).await?;
    }
    Ok(true)
}

async fn block_delete(
    t: &mut Txn<'_>,
    origin: &Origin,
    author: &AuthorInfo,
    w: &Write,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    let rev: Option<i64> =
        sqlx::query_scalar("SELECT rev FROM blocks WHERE author_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    if rev.is_some_and(|r| r < w.stamp) {
        let removal = removal_for(origin, w, Removed::Delete);
        block_delete_row(t, author, rkey, Some(&removal)).await?;
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
    origin: &Origin,
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
    let seen = seen_for(t, origin, w);
    if let Some(old) = row {
        if Some(old.list_id) == target {
            // Same subject: counted, witnessed_at and sched_key are sticky.
            if let Some(refusal) = t.gate(cause, CapKind::Listblocks) {
                return t
                    .refuse(author, Collection::ListBlock, rkey, refusal, w.witness)
                    .await;
            }
            sqlx::query(
                "UPDATE list_blocks SET created_at = $3, rev = $4,
                   last_seen = GREATEST(last_seen, $5)
                 WHERE author_id = $1 AND rkey = $2",
            )
            .bind(author.id)
            .bind(rkey)
            .bind(r.created_at)
            .bind(w.stamp)
            .bind(seen)
            .execute(&mut *t.conn)
            .await?;
            t.report.applied += 1;
            return Ok(());
        }
        // Subject change: delete of the old row plus a new insert (§4.2).
        // The old target goes to history (§7.2); if the new version is
        // then refused, the removal is a refused update.
        let removal = removal_for(origin, w, Removed::SubjectChange);
        t.clear_last_history();
        listblock_delete_row(t, author, rkey, Some(&removal)).await?;
        return match listblock_insert(t, author, cause, w, r, seen).await? {
            Ok(()) => {
                t.clear_last_history();
                t.report.applied += 1;
                Ok(())
            }
            Err(refusal) => {
                t.relabel_last_history(Removed::RefusedUpdate).await?;
                t.put_refusal_tombstone(Collection::ListBlock, author.id, rkey, w.stamp)
                    .await?;
                t.refuse(author, Collection::ListBlock, rkey, refusal, w.witness)
                    .await
            }
        };
    }
    match listblock_insert(t, author, cause, w, r, seen).await? {
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
    seen: DateTime<Utc>,
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
                                  created_at, rev, first_seen, last_seen)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)",
    )
    .bind(author.id)
    .bind(w.rkey.as_str())
    .bind(list_id)
    .bind(counted.is_ok())
    .bind(w.witness)
    .bind(author.key.as_str())
    .bind(r.created_at)
    .bind(w.stamp)
    .bind(seen)
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
/// list(L) exclusive. It is also where `list_blocks_history` is written
/// (§4.2, §7.7): the caller names the removal, and the two purges name
/// none. Counted and uncounted rows are recorded alike.
pub(crate) async fn listblock_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
    removal: Option<&Removal>,
) -> Result<bool> {
    let gone: Option<ListBlockGone> = sqlx::query_as(
        "DELETE FROM list_blocks WHERE author_id = $1 AND rkey = $2
         RETURNING list_id, counted, sched_key, created_at, first_seen, last_seen",
    )
    .bind(author.id)
    .bind(rkey)
    .fetch_optional(&mut *t.conn)
    .await?;
    let Some((list_id, counted, sched_key, created_at, first_seen, last_seen)) = gone else {
        return Ok(false);
    };
    if let Some(r) = removal {
        let gone = Gone {
            rkey,
            created_at,
            first_seen,
            last_seen,
        };
        t.record_listblock_removal(author, &gone, list_id, r)
            .await?;
    }
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

async fn listblock_delete(
    t: &mut Txn<'_>,
    origin: &Origin,
    author: &AuthorInfo,
    w: &Write,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    if let Some(row) = listblock_row(t, author.id, rkey).await? {
        if row.rev < w.stamp {
            let removal = removal_for(origin, w, Removed::Delete);
            listblock_delete_row(t, author, rkey, Some(&removal)).await?;
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
    origin: &Origin,
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
    let seen = seen_for(t, origin, w);

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
                "UPDATE list_items SET created_at = $3, rev = $4,
                   last_seen = GREATEST(last_seen, $5)
                 WHERE owner_id = $1 AND rkey = $2",
            )
            .bind(author.id)
            .bind(rkey)
            .bind(r.created_at)
            .bind(w.stamp)
            .bind(seen)
            .execute(&mut *t.conn)
            .await?;
            t.report.applied += 1;
            return Ok(());
        }
        // Changed (list or subject) or no longer tracked: delete the old
        // version, then try the new one as an insert. The removal goes to
        // history under §7.8's condition: a subject change if the new
        // version is stored, a refused update if it is not (which is also
        // the case of an unchanged target whose new version is refused).
        let removal = removal_for(origin, w, Removed::SubjectChange);
        t.clear_last_history();
        item_delete_row(t, author, rkey, Some(&removal)).await?;
        let result = if tracked {
            item_insert(t, author, cause, w, r, target_id.unwrap_or_default(), seen).await?
        } else {
            Err(ItemRefusal::Untracked)
        };
        return match result {
            Ok(()) => {
                t.clear_last_history();
                t.report.applied += 1;
                Ok(())
            }
            Err(refusal) => {
                t.relabel_last_history(Removed::RefusedUpdate).await?;
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
    match item_insert(t, author, cause, w, r, target_id.unwrap_or_default(), seen).await? {
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
    seen: DateTime<Utc>,
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
        "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev,
                                 first_seen, last_seen)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $7)",
    )
    .bind(author.id)
    .bind(w.rkey.as_str())
    .bind(list_id)
    .bind(subject_id)
    .bind(r.created_at)
    .bind(w.stamp)
    .bind(seen)
    .execute(&mut *t.conn)
    .await?;
    t.deltas.stat(stat::LIST_ITEMS, 1);
    t.deltas.host(&author.buckets, CapKind::Items, 1);
    Ok(Ok(()))
}

/// Deletes one listitem row and its counters. The caller holds author(O)
/// and list(L) (shared suffices: all writers of L's items hold author(O)).
/// Every path deleting `list_items` rows uses this function; it is also
/// where `list_items_history` is written (§4.7, §7.8), when the caller
/// names the removal **and** the owner is not `deleted` **and** the list
/// is tracked or its record is deleted — so a change of tracking is never
/// recorded as a change of membership.
pub(crate) async fn item_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
    removal: Option<&Removal>,
) -> Result<bool> {
    let gone: Option<ItemGone> = sqlx::query_as(
        "DELETE FROM list_items WHERE owner_id = $1 AND rkey = $2
             RETURNING list_id, subject_id, created_at, first_seen, last_seen",
    )
    .bind(author.id)
    .bind(rkey)
    .fetch_optional(&mut *t.conn)
    .await?;
    let Some((list_id, subject_id, created_at, first_seen, last_seen)) = gone else {
        return Ok(false);
    };
    let list: Option<(i16, i16, String)> = sqlx::query_as(
        "UPDATE lists SET item_count = item_count - 1 WHERE id = $1
         RETURNING track_state, record_state, rkey",
    )
    .bind(list_id)
    .fetch_optional(&mut *t.conn)
    .await?;
    if let (Some(r), Some((track_state, record_state, list_rkey))) = (removal, &list) {
        let tracked = TrackState::from_code(*track_state).is_some_and(TrackState::is_tracked);
        let record_deleted = *record_state == RecordState::Deleted.code();
        if author.status != crate::codes::actor_status::DELETED && (tracked || record_deleted) {
            let gone = Gone {
                rkey,
                created_at,
                first_seen,
                last_seen,
            };
            t.record_item_removal(author, &gone, list_rkey, subject_id, r)
                .await?;
        }
    }
    sqlx::query("UPDATE actors SET owned_items = owned_items - 1 WHERE id = $1")
        .bind(author.id)
        .execute(&mut *t.conn)
        .await?;
    t.deltas.stat(stat::LIST_ITEMS, -1);
    t.deltas.host(&author.buckets, CapKind::Items, -1);
    Ok(true)
}

async fn listitem_delete(
    t: &mut Txn<'_>,
    origin: &Origin,
    author: &AuthorInfo,
    w: &Write,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    if let Some(row) = item_row(t, author.id, rkey).await? {
        if row.rev < w.stamp {
            let removal = removal_for(origin, w, Removed::Delete);
            item_delete_row(t, author, rkey, Some(&removal)).await?;
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
        "INSERT INTO lists (owner_id, rkey, record_state, purpose, name, created_at, rev,
                            description, avatar_cid, about_read)
         VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, true)
         ON CONFLICT (owner_id, rkey) DO UPDATE SET record_state = 1,
           purpose = EXCLUDED.purpose, name = EXCLUDED.name,
           created_at = EXCLUDED.created_at, rev = EXCLUDED.rev,
           description = EXCLUDED.description, avatar_cid = EXCLUDED.avatar_cid,
           about_read = true
         RETURNING id",
    )
    .bind(author.id)
    .bind(rkey)
    .bind(r.purpose.code())
    .bind(r.name.as_deref())
    .bind(r.created_at)
    .bind(w.stamp)
    .bind(r.description.as_deref())
    .bind(r.avatar.as_deref())
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
           description = NULL, avatar_cid = NULL,
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
    author_id: i64,
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
        .bind(author_id)
        .bind(r.stamp)
        .bind(r.after.as_ref().map(|k| k.as_str().to_owned()))
        .bind(r.through.as_ref().map(|k| k.as_str().to_owned()))
        .bind(keep)
        .fetch_all(&mut *t.conn)
        .await?)
}

async fn apply_reconcile(t: &mut Txn<'_>, r: &Reconcile, candidates: &[String]) -> Result<()> {
    let author = t.author(&r.author).await?;
    // A listing knows only its stamp, which is neither the removing
    // commit's rev nor a bound on it: no rev, the listing clock (§7.7).
    let found = Removal::listing(Removed::Reconcile);
    for rkey in candidates {
        // Re-check `rev < R` (a write earlier in this batch may have
        // re-stamped the row).
        let rev = stored_rev(t, r.collection, author.id, rkey).await?;
        let gone = match (r.collection, rev) {
            (_, Some(rev)) if rev >= r.stamp => false,
            (Collection::Block, Some(_)) => {
                block_delete_row(t, &author, rkey, Some(&found)).await?
            }
            (Collection::ListBlock, Some(_)) => {
                listblock_delete_row(t, &author, rkey, Some(&found)).await?
            }
            (Collection::ListItem, Some(_)) => {
                item_delete_row(t, &author, rkey, Some(&found)).await?
            }
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
        let b1 = deadlock_backoff(1);
        assert!(b1 >= Duration::from_millis(10) && b1 <= Duration::from_millis(20));
        let b4 = deadlock_backoff(4);
        assert!(b4 >= Duration::from_millis(80) && b4 <= Duration::from_millis(160));
        assert!(deadlock_backoff(30) <= Duration::from_millis(2_000));
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
