//! `farsight-storage::apply` — the one write path for firehose batches,
//! listing pages and discovery writes (see `docs/design/README.md`,
//! `docs/design/list-indexing.md` and `docs/design/storage.md`).
//!
//! One Postgres transaction per batch, at `READ COMMITTED`:
//! 1. author locks for every author in the batch, ascending by key;
//! 2. stored rows of every listblock / listitem key and every reconcile
//!    candidate are read (stable under the author locks), so the list keys
//!    of deletes, subject-changing updates and reconciles are known;
//! 3. list locks, ascending by key: exclusive for listblock and list
//!    writes, shared for listitem writes;
//! 4. writes in batch order, then reconciles, each with the LWW rule,
//!    the listblock counters, the list transitions and the caps;
//! 5. firehose progress (cursor, `applied_through`, `firehose_clock`) in
//!    the same transaction (see `docs/design/firehose.md` and
//!    `docs/design/coverage.md`), and `NOTIFY farsight_coverage`.
//!
//! Deadlock aborts (`40P01`) are retried, and so is a batch whose lock set
//! changed while it was being taken (a reactivated account's list turned
//! `unavailable` between the read in step 2 and the locks of step 3).
//! Neither counts toward poisoned-event handling (the error type says so).
//!
//! Rows are deleted by one function per table, a set at a time: one
//! statement removes the rows and adjusts the counters they were counted
//! in, and their history is written by one more.

use crate::codes::sql::{RECORD_DELETED, RECORD_PRESENT, RECORD_UNKNOWN};
use crate::ids::{ActorId, ListId, Stamp};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use farsight_core::{
    BlockRecord, Collection, Did, ListBlockRecord, ListItemRecord, ListRecord, Record, RecordKey,
};
use sqlx::PgPool;

use crate::codes::{CapType, DebtReason, RecordState, RequesterKey, TrackState};
use crate::counters::{CounterSink, stat};
use crate::error::{Result, StorageError};
use crate::firehose::FirehoseProgress;
use crate::history::{Cause as Removed, Gone, GoneRow, Removal, Table};
use crate::keys::{self, CapKind, Limits, clamp};
use crate::repo_events::{RepoEvent, unavailable_list_keys};
use crate::tracking::FireArgs;
use crate::transition::Event;
use crate::txn::{
    ApplyReport, AuthorInfo, Cause, Gates, Refusal, Txn, WriteOutcome, lww_upsert_wins,
};

/// Maximum attempts of one batch transaction when deadlocks abort it.
pub const MAX_DEADLOCK_ATTEMPTS: u32 = 8;

/// A listing stamp may be applied only this long after it was read.
pub const STAMP_VALIDITY: Duration = Duration::from_secs(72 * 3600);

/// Where a batch comes from; decides stamps, witnesses and charging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// Firehose events: `W` = commit rev, `witnessed_at` = event witness.
    Firehose,
    /// A listing page: `W = R`, `witnessed_at` NULL.
    Listing {
        /// When `R` was read (database clock). Batches whose stamp is older
        /// than 72 h are rejected with [`StorageError::StaleStamp`].
        stamp_read_at: DateTime<Utc>,
        /// Budget gate on the job: inserts skipped with a `refused` debt,
        /// deletes and reconcile applied.
        deletes_only: bool,
    },
    /// Discovery writes (`W = 0`), charged to the requester.
    Discovery {
        /// Who the interning is charged to.
        requester: RequesterKey,
    },
}

/// What a write does to its record key.
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
    /// Which of the four indexed collections the record is in.
    pub collection: Collection,
    /// Record key; with `author` and `collection` it names the row the
    /// write is for, and the tombstone a delete leaves.
    pub rkey: RecordKey,
    /// Stamp `W`: commit rev, listing stamp `R`, or 0 for discovery.
    pub stamp: Stamp,
    /// Firehose witness time of the event, if firehose.
    pub witness: Option<DateTime<Utc>>,
    /// Upsert or delete.
    pub action: WriteAction,
}

/// A range or whole-collection reconcile: delete the author's rows in
/// `collection` with `rev < stamp` whose rkey lies in `(after, through]`
/// (open ends = unbounded) and is not in `keep`. Never writes a
/// tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconcile {
    /// The repo that was listed; only its rows are candidates.
    pub author: Did,
    /// The collection that was listed.
    pub collection: Collection,
    /// The listing stamp `R`.
    pub stamp: Stamp,
    /// Exclusive lower bound (`prev_last`); `None` = from the start.
    pub after: Option<RecordKey>,
    /// Inclusive upper bound; `None` = to +∞ (last page / whole range).
    pub through: Option<RecordKey>,
    /// Record keys the page listed. A row at one of them is kept whatever
    /// its rev.
    pub keep: Vec<RecordKey>,
}

/// A batch: one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// Where the batch comes from; one origin for everything in it.
    pub origin: Origin,
    /// Writes, applied in order.
    pub writes: Vec<Write>,
    /// Reconciles, applied after the writes.
    pub reconciles: Vec<Reconcile>,
    /// Non-commit firehose events, applied after the writes.
    pub events: Vec<RepoEvent>,
    /// Firehose progress to persist atomically with the writes.
    pub firehose: Option<FirehoseProgress>,
}

impl Batch {
    /// A batch of `origin` with nothing in it and no firehose progress.
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
    /// The caps and rates the batch is checked against.
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
            Err(e) if e.is_retryable_abort() => {
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
            let Some(author_id) = t.actor_id(author).await? else {
                continue;
            };
            for (owner, lrkey) in stored_list_targets(&mut t, author_id, *collection, rkeys).await?
            {
                locks.add(&owner, &lrkey, *collection == Collection::ListBlock);
            }
        }
        let mut candidates: Vec<Vec<String>> = Vec::with_capacity(batch.reconciles.len());
        for r in &batch.reconciles {
            let Some(author_id) = t.actor_id(&r.author).await? else {
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

        // The author lock does not keep a list from turning `unavailable`:
        // a list job's timeout runs under list(L) alone. One that did so
        // after the read above is not locked, and locking it now would
        // break the order; the transaction is given up and run again, and
        // its next read sees the list. Lists that turn later still are
        // ordered after this batch (`apply_repo_event` fires OA only under
        // a lock that is held).
        for e in batch.events.iter().filter(|e| e.may_reactivate()) {
            for (_, lrkey) in unavailable_list_keys(&mut t, e.did()).await? {
                if !t.holds_list_exclusive(keys::list_lock_key(e.did().as_str(), &lrkey)) {
                    return Err(StorageError::LockSetChanged);
                }
            }
        }

        // 3b. Intern locks for every DID this batch may create an `actors`
        // row for, ascending. Without them two batches interning the same
        // new DIDs in opposite orders wait on each other's uncommitted
        // inserts into the unique index and deadlock repeatedly (observed:
        // retries exhausted). Taken last, so the global order is authors,
        // lists, interns.
        let mut dids: BTreeSet<&Did> = authors.clone();
        for w in &batch.writes {
            match &w.action {
                WriteAction::Upsert(Record::Block(r)) => {
                    dids.insert(&r.subject);
                }
                WriteAction::Upsert(Record::ListBlock(r)) => {
                    dids.insert(&r.subject.authority);
                }
                WriteAction::Upsert(Record::ListItem(r)) => {
                    dids.insert(&r.subject);
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
        // A DID's first authored indexed record seen on the firehose
        // interns it (above) and takes the active-DID branch here: a tier-2
        // repo job, so its pre-existing records get listed.
        if batch.origin == Origin::Firehose && !batch.writes.is_empty() {
            let new: Vec<ActorId> = std::mem::take(&mut t.report.new_authors);
            for id in &new {
                crate::queue::enqueue(
                    &mut *t.conn,
                    *id,
                    crate::queue::JobKind::Repo,
                    crate::codes::Tier::Active,
                    crate::codes::Priority::Normal,
                    crate::codes::RequesterKey::Firehose,
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
    author_id: ActorId,
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

/// The removal a write causes: a firehose event names its commit rev
/// and its witness; a listing or discovery write knows neither.
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

/// The witness a write stamps on the row it stores.
fn seen_for(t: &Txn<'_>, origin: &Origin, w: &Write) -> DateTime<Utc> {
    match origin {
        Origin::Firehose => t.seen_at(w.witness),
        _ => t.seen_at(None),
    }
}

fn cause_for(author: &AuthorInfo, origin: &Origin) -> Cause {
    match origin {
        Origin::Discovery { requester } => Cause {
            key: requester.to_string(),
            buckets: Vec::new(),
            large: true,
            mask: 0,
        },
        _ => author.cause(),
    }
}

async fn apply_write(t: &mut Txn<'_>, origin: &Origin, w: &Write) -> Result<()> {
    let author = t.author(&w.author).await?;
    if let WriteAction::Upsert(record) = &w.action
        && record.collection() != w.collection
    {
        return Err(StorageError::Invariant(format!(
            "write for {} carries a {} record",
            w.collection,
            record.collection()
        )));
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
    let tomb = t.tombstone_rev(w.collection, author.id, &w.rkey).await?;
    if !lww_upsert_wins(w.stamp, row_rev, tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    t.refuse(
        author,
        w.collection,
        &w.rkey,
        Refusal::Refused(CapType::DeletesOnly),
        w.witness,
    )
    .await
}

async fn stored_rev(
    t: &mut Txn<'_>,
    c: Collection,
    author_id: ActorId,
    rkey: &str,
) -> Result<Option<Stamp>> {
    let sql = match c {
        Collection::Block => "SELECT rev FROM blocks WHERE author_id = $1 AND rkey = $2",
        Collection::ListBlock => "SELECT rev FROM list_blocks WHERE author_id = $1 AND rkey = $2",
        Collection::ListItem => "SELECT rev FROM list_items WHERE owner_id = $1 AND rkey = $2",
        Collection::List => &format!(
            "SELECT rev FROM lists WHERE owner_id = $1 AND rkey = $2 AND record_state <> {RECORD_UNKNOWN}"
        ),
    };
    let r: Option<Option<Stamp>> = sqlx::query_scalar(sql)
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
    let tomb = t
        .tombstone_rev(Collection::Block, author.id, &w.rkey)
        .await?;
    if !lww_upsert_wins(w.stamp, row.map(|x| x.1), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    let seen = seen_for(t, origin, w);
    if let Some((old_subject, _, old_created, old_first, old_last)) = row {
        let same = t.actor_id(&r.subject).await? == Some(old_subject);
        let refused = removal_for(origin, w, Removed::RefusedUpdate);
        if let Some(refusal) = t.gate(cause, CapKind::Blocks) {
            if !same {
                block_delete_row(t, author, rkey, Some(&refused)).await?;
                t.put_refusal_tombstone(Collection::Block, author.id, &w.rkey, w.stamp)
                    .await?;
            }
            return t
                .refuse(author, Collection::Block, &w.rkey, refusal, w.witness)
                .await;
        }
        let subject_id = if same {
            old_subject
        } else {
            match t.intern_actor(&r.subject, cause).await? {
                Ok(id) => id,
                Err(refusal) => {
                    block_delete_row(t, author, rkey, Some(&refused)).await?;
                    t.put_refusal_tombstone(Collection::Block, author.id, &w.rkey, w.stamp)
                        .await?;
                    return t
                        .refuse(author, Collection::Block, &w.rkey, refusal, w.witness)
                        .await;
                }
            }
        };
        if !same {
            // Subject change = removal of the old target + a fresh insert,
            // done in place: authored_blocks is unchanged, the old target
            // goes to history and the witness bounds start again.
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
            t.refuse(author, Collection::Block, &w.rkey, refusal, w.witness)
                .await
        }
    }
}

/// `subject_id, rev, created_at, first_seen, last_seen` of a stored block.
type BlockRow = (
    ActorId,
    Stamp,
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
    // For the "last 24 hours" top lists: history read by the backfill
    // is not recent.
    if crate::top::is_recent(r.created_at, seen) {
        crate::top::log(&mut *t.conn, seen, author.id, w.rkey.as_str(), subject_id).await?;
    }
    t.deltas.stat(stat::BLOCKS, 1);
    t.deltas.host(&author.buckets, CapKind::Blocks, 1);
    Ok(Ok(()))
}

/// `rkey, subject_id, created_at, first_seen, last_seen` of a deleted
/// block.
type BlockGone = (
    String,
    ActorId,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

/// `rkey, list_id, counted, sched_key, created_at, first_seen, last_seen`
/// of a deleted listblock, and its list's `owner_id, rkey`.
type ListBlockGone = (
    String,
    ListId,
    bool,
    Option<String>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<ActorId>,
    Option<String>,
);

/// `rkey, subject_id, created_at, first_seen, last_seen` of a deleted
/// listitem, and its list's `track_state, record_state, rkey`.
type ItemGone = (
    String,
    ActorId,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<i16>,
    Option<RecordState>,
    Option<String>,
);

fn count(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Deletes the author's block rows at `rkeys` (with their counters) in
/// one statement; with `below`, only rows whose rev is lower. Returns how
/// many rows went. Every path deleting `blocks` rows uses this function;
/// it is also where `blocks_history` is written: the caller names the
/// removal, and the account and divergence purges name none.
pub(crate) async fn block_delete_rows(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkeys: &[String],
    below: Option<Stamp>,
    removal: Option<&Removal>,
) -> Result<u64> {
    if rkeys.is_empty() {
        return Ok(0);
    }
    let gone: Vec<BlockGone> = sqlx::query_as(
        "WITH gone AS (
           DELETE FROM blocks
           WHERE author_id = $1 AND rkey = ANY($2) AND ($3::BIGINT IS NULL OR rev < $3)
           RETURNING rkey, subject_id, created_at, first_seen, last_seen),
         author AS (
           UPDATE actors
           SET authored_blocks = authored_blocks - (SELECT count(*) FROM gone)::INT
           WHERE id = $1 AND EXISTS (SELECT 1 FROM gone))
         SELECT rkey, subject_id, created_at, first_seen, last_seen FROM gone ORDER BY rkey",
    )
    .bind(author.id)
    .bind(rkeys)
    .bind(below)
    .fetch_all(&mut *t.conn)
    .await?;
    if gone.is_empty() {
        return Ok(0);
    }
    let n = count(gone.len());
    t.deltas.stat(stat::BLOCKS, -n);
    t.deltas.host(&author.buckets, CapKind::Blocks, -n);
    if let Some(r) = removal {
        let rows: Vec<GoneRow> = gone
            .into_iter()
            .map(
                |(rkey, subject_id, created_at, first_seen, last_seen)| GoneRow {
                    rkey,
                    actor_id: subject_id,
                    list_rkey: None,
                    created_at,
                    first_seen,
                    last_seen,
                },
            )
            .collect();
        t.record_removals(author, Table::Blocks, &rows, r).await?;
    }
    Ok(n as u64)
}

/// [`block_delete_rows`] for one row. Returns whether a row went.
pub(crate) async fn block_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
    removal: Option<&Removal>,
) -> Result<bool> {
    Ok(block_delete_rows(t, author, &[rkey.to_owned()], None, removal).await? > 0)
}

async fn block_delete(
    t: &mut Txn<'_>,
    origin: &Origin,
    author: &AuthorInfo,
    w: &Write,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    let rev: Option<Stamp> =
        sqlx::query_scalar("SELECT rev FROM blocks WHERE author_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    if rev.is_some_and(|r| r < w.stamp) {
        let removal = removal_for(origin, w, Removed::Delete);
        block_delete_row(t, author, rkey, Some(&removal)).await?;
    }
    t.put_tombstone(Collection::Block, author.id, &w.rkey, w.stamp)
        .await?;
    t.report.applied += 1;
    Ok(())
}

// ------------------------------------------------------------ listblocks

/// The stored listblock row.
struct ListBlockRow {
    list_id: ListId,
    rev: Stamp,
}

async fn listblock_row(
    t: &mut Txn<'_>,
    author_id: ActorId,
    rkey: &str,
) -> Result<Option<ListBlockRow>> {
    let r: Option<(ListId, Stamp)> =
        sqlx::query_as("SELECT list_id, rev FROM list_blocks WHERE author_id = $1 AND rkey = $2")
            .bind(author_id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    Ok(r.map(|(list_id, rev)| ListBlockRow { list_id, rev }))
}

async fn find_list(t: &mut Txn<'_>, owner: &Did, rkey: &RecordKey) -> Result<Option<ListId>> {
    Ok(sqlx::query_scalar(
        "SELECT l.id FROM lists l JOIN actors a ON a.id = l.owner_id
         WHERE a.did = $1 AND l.rkey = $2",
    )
    .bind(owner.as_str())
    .bind(rkey.as_str())
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
        .tombstone_rev(Collection::ListBlock, author.id, &w.rkey)
        .await?;
    if !lww_upsert_wins(w.stamp, row.as_ref().map(|x| x.rev), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    let target = find_list(t, &r.subject.authority, &r.subject.rkey).await?;
    let seen = seen_for(t, origin, w);
    if let Some(old) = row {
        if Some(old.list_id) == target {
            // Same subject: counted, witnessed_at and sched_key are sticky.
            if let Some(refusal) = t.gate(cause, CapKind::Listblocks) {
                return t
                    .refuse(author, Collection::ListBlock, &w.rkey, refusal, w.witness)
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
        // Subject change: delete of the old row plus a new insert. The
        // old target goes to history; if the new version is then
        // refused, the removal is a refused update.
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
                t.put_refusal_tombstone(Collection::ListBlock, author.id, &w.rkey, w.stamp)
                    .await?;
                t.refuse(author, Collection::ListBlock, &w.rkey, refusal, w.witness)
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
            t.refuse(author, Collection::ListBlock, &w.rkey, refusal, w.witness)
                .await
        }
    }
}

/// Whether counting a new listblock on this list would admit it: count 0
/// and not tracked, excluding the cells where `+` does not admit (a
/// deleted record goes to `dead`; a purge to `dead` stays).
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
    list_id: ListId,
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
        .intern_list(&r.subject.authority, &r.subject.rkey, cause)
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
            // Stored uncounted: not a refusal, but a `capped` debt.
            t.add_debt(author.id, DebtReason::Capped, Some(cap), w.witness)
                .await?;
            t.report.uncounted += 1;
        }
    }
    Ok(Ok(()))
}

/// The counter path: deletes the author's listblock rows at `rkeys` in
/// one statement (with `below`, only rows whose rev is lower) and, for
/// those that were counted, decrements `listblock_count`,
/// `fetch_triggers` and the lane of each row's stored `sched_key`, firing
/// **−** on a list that reaches 0. Every path deleting `list_blocks` rows
/// uses this function. The caller holds author(A) and list(L) exclusive
/// for every list the rows target. It is also where `list_blocks_history`
/// is written: the caller names the removal, and the two purges name
/// none. Counted and uncounted rows are recorded alike. Returns how many
/// rows went.
pub(crate) async fn listblock_delete_rows(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkeys: &[String],
    below: Option<Stamp>,
    removal: Option<&Removal>,
) -> Result<u64> {
    if rkeys.is_empty() {
        return Ok(0);
    }
    let gone: Vec<ListBlockGone> = sqlx::query_as(
        "WITH gone AS (
           DELETE FROM list_blocks
           WHERE author_id = $1 AND rkey = ANY($2) AND ($3::BIGINT IS NULL OR rev < $3)
           RETURNING rkey, list_id, counted, sched_key, created_at, first_seen, last_seen),
         author AS (
           UPDATE actors
           SET authored_listblocks = authored_listblocks - (SELECT count(*) FROM gone)::INT,
               fetch_triggers = fetch_triggers - (SELECT count(*) FROM gone WHERE counted)::INT
           WHERE id = $1 AND EXISTS (SELECT 1 FROM gone))
         SELECT g.rkey, g.list_id, g.counted, g.sched_key, g.created_at, g.first_seen,
                g.last_seen, l.owner_id, l.rkey
         FROM gone g LEFT JOIN lists l ON l.id = g.list_id ORDER BY g.rkey",
    )
    .bind(author.id)
    .bind(rkeys)
    .bind(below)
    .fetch_all(&mut *t.conn)
    .await?;
    if gone.is_empty() {
        return Ok(0);
    }
    let n = count(gone.len());
    t.deltas.stat(stat::LIST_BLOCKS, -n);
    t.deltas.host(&author.buckets, CapKind::Listblocks, -n);
    // Counted rows per list and lane, in the order the rows went.
    let mut lanes: Vec<((ListId, Option<String>), i32)> = Vec::new();
    let mut rows: Vec<GoneRow> = Vec::new();
    for (rkey, list_id, counted, sched_key, created_at, first_seen, last_seen, owner, list_rkey) in
        gone
    {
        if counted {
            let lane = (list_id, sched_key);
            match lanes.iter_mut().find(|(l, _)| *l == lane) {
                Some((_, n)) => *n += 1,
                None => lanes.push((lane, 1)),
            }
        }
        if let (Some(owner), Some(list_rkey)) = (owner, list_rkey) {
            rows.push(GoneRow {
                rkey,
                actor_id: owner,
                list_rkey: Some(list_rkey),
                created_at,
                first_seen,
                last_seen,
            });
        }
    }
    if let Some(r) = removal {
        t.record_removals(author, Table::ListBlocks, &rows, r)
            .await?;
    }
    for ((list_id, sched_key), n) in lanes {
        t.change_listblock_count(list_id, -n, sched_key.as_deref())
            .await?;
    }
    Ok(n as u64)
}

/// [`listblock_delete_rows`] for one row. Returns whether a row went.
pub(crate) async fn listblock_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
    removal: Option<&Removal>,
) -> Result<bool> {
    Ok(listblock_delete_rows(t, author, &[rkey.to_owned()], None, removal).await? > 0)
}

async fn listblock_delete(
    t: &mut Txn<'_>,
    origin: &Origin,
    author: &AuthorInfo,
    w: &Write,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    if let Some(row) = listblock_row(t, author.id, rkey).await?
        && row.rev < w.stamp
    {
        let removal = removal_for(origin, w, Removed::Delete);
        listblock_delete_row(t, author, rkey, Some(&removal)).await?;
    }
    t.put_tombstone(Collection::ListBlock, author.id, &w.rkey, w.stamp)
        .await?;
    t.report.applied += 1;
    Ok(())
}

// -------------------------------------------------------------- listitems

struct ItemRow {
    list_id: ListId,
    subject_id: ActorId,
    rev: Stamp,
}

async fn item_row(t: &mut Txn<'_>, owner_id: ActorId, rkey: &str) -> Result<Option<ItemRow>> {
    let r: Option<(ListId, ActorId, Stamp)> = sqlx::query_as(
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

/// Marks a list `capped` because one of its items was refused;
/// `refresh` also requests a refresh run (intern-rate refusals).
async fn mark_list_capped(t: &mut Txn<'_>, list_id: ListId, refresh: bool) -> Result<()> {
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
        .tombstone_rev(Collection::ListItem, author.id, &w.rkey)
        .await?;
    if !lww_upsert_wins(w.stamp, row.as_ref().map(|x| x.rev), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    // Authority rule held at parse time: r.list.authority == author.
    let target: Option<(ListId, i16)> =
        sqlx::query_as("SELECT id, track_state FROM lists WHERE owner_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(r.list.rkey.as_str())
            .fetch_optional(&mut *t.conn)
            .await?;
    let tracked = target
        .and_then(|(_, s)| TrackState::from_code(s))
        .is_some_and(TrackState::is_tracked);
    // `tracked` comes from the same row: where an insert is tried the id is
    // there, and the fallback below (an id no list has) is never stored.
    let target_id = target.map(|(id, _)| id);
    let seen = seen_for(t, origin, w);

    if let Some(old) = &row {
        let same_list = Some(old.list_id) == target_id;
        let same_subject = t.actor_id(&r.subject).await? == Some(old.subject_id);
        if same_list && same_subject && tracked {
            if let Some(refusal) = t.gate(cause, CapKind::Items) {
                return t
                    .refuse(author, Collection::ListItem, &w.rkey, refusal, w.witness)
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
        // history under the membership condition: a subject change if the new
        // version is stored, a refused update if it is not (which is also the
        // case of an unchanged target whose new version is refused).
        let removal = removal_for(origin, w, Removed::SubjectChange);
        t.clear_last_history();
        item_delete_row(t, author, rkey, Some(&removal)).await?;
        let result = if tracked {
            item_insert(
                t,
                author,
                cause,
                w,
                r,
                target_id.unwrap_or(ListId::new(0)),
                seen,
            )
            .await?
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
                // Refused new version with a stored row ⇒ delete +
                // refusal tombstone at E − 1.
                t.put_refusal_tombstone(Collection::ListItem, author.id, &w.rkey, w.stamp)
                    .await?;
                item_refused(t, author, w, refusal).await
            }
        };
    }
    if !tracked {
        t.report.untracked_items += 1;
        return Ok(());
    }
    match item_insert(
        t,
        author,
        cause,
        w,
        r,
        target_id.unwrap_or(ListId::new(0)),
        seen,
    )
    .await?
    {
        Ok(()) => {
            t.report.applied += 1;
            Ok(())
        }
        Err(refusal) => item_refused(t, author, w, refusal).await,
    }
}

/// Why an item was not stored.
enum ItemRefusal {
    /// The list is not tracked: refused, no debt (costs nothing).
    Untracked,
    /// A per-list or per-owner item cap: the list is marked `capped`.
    ListCap,
    /// A gate or rate: debt, and the list is marked `capped`.
    Debt(Refusal),
}

async fn item_refused(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
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
            t.refuse(author, Collection::ListItem, &w.rkey, r, w.witness)
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
    list_id: ListId,
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

/// Deletes the owner's listitem rows at `rkeys` and their counters in one
/// statement; with `below`, only rows whose rev is lower. The caller
/// holds author(O) and list(L) for every list the rows are in (shared
/// suffices: all writers of L's items hold author(O)). Every path
/// deleting `list_items` rows uses this function; it is also where
/// `list_items_history` is written, when the caller names the removal
/// **and** the owner is not `deleted` **and** the row's list is tracked
/// or its record is deleted — so a change of tracking is never recorded
/// as a change of membership. Returns how many rows went.
pub(crate) async fn item_delete_rows(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkeys: &[String],
    below: Option<Stamp>,
    removal: Option<&Removal>,
) -> Result<u64> {
    if rkeys.is_empty() {
        return Ok(0);
    }
    let gone: Vec<ItemGone> = sqlx::query_as(
        "WITH gone AS (
           DELETE FROM list_items
           WHERE owner_id = $1 AND rkey = ANY($2) AND ($3::BIGINT IS NULL OR rev < $3)
           RETURNING rkey, list_id, subject_id, created_at, first_seen, last_seen),
         per_list AS (
           UPDATE lists l SET item_count = l.item_count - g.n
           FROM (SELECT list_id, count(*)::INT AS n FROM gone GROUP BY list_id) g
           WHERE l.id = g.list_id
           RETURNING l.id, l.track_state, l.record_state, l.rkey),
         owner AS (
           UPDATE actors SET owned_items = owned_items - (SELECT count(*) FROM gone)::INT
           WHERE id = $1 AND EXISTS (SELECT 1 FROM gone))
         SELECT g.rkey, g.subject_id, g.created_at, g.first_seen, g.last_seen,
                p.track_state, p.record_state, p.rkey
         FROM gone g LEFT JOIN per_list p ON p.id = g.list_id ORDER BY g.rkey",
    )
    .bind(author.id)
    .bind(rkeys)
    .bind(below)
    .fetch_all(&mut *t.conn)
    .await?;
    if gone.is_empty() {
        return Ok(0);
    }
    let n = count(gone.len());
    t.deltas.stat(stat::LIST_ITEMS, -n);
    t.deltas.host(&author.buckets, CapKind::Items, -n);
    if let Some(r) = removal
        && author.status != crate::codes::ActorStatus::Deleted
    {
        let rows: Vec<GoneRow> = gone
            .into_iter()
            .filter_map(
                |(rkey, subject_id, created_at, first_seen, last_seen, ts, rs, lr)| {
                    let tracked = ts
                        .and_then(TrackState::from_code)
                        .is_some_and(TrackState::is_tracked);
                    let record_deleted = rs == Some(RecordState::Deleted);
                    let list_rkey = lr?;
                    (tracked || record_deleted).then_some(GoneRow {
                        rkey,
                        actor_id: subject_id,
                        list_rkey: Some(list_rkey),
                        created_at,
                        first_seen,
                        last_seen,
                    })
                },
            )
            .collect();
        t.record_removals(author, Table::ListItems, &rows, r)
            .await?;
    }
    Ok(n as u64)
}

/// [`item_delete_rows`] for one row. Returns whether a row went.
pub(crate) async fn item_delete_row(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    rkey: &str,
    removal: Option<&Removal>,
) -> Result<bool> {
    Ok(item_delete_rows(t, author, &[rkey.to_owned()], None, removal).await? > 0)
}

async fn listitem_delete(
    t: &mut Txn<'_>,
    origin: &Origin,
    author: &AuthorInfo,
    w: &Write,
) -> Result<()> {
    let rkey = w.rkey.as_str();
    if let Some(row) = item_row(t, author.id, rkey).await?
        && row.rev < w.stamp
    {
        let removal = removal_for(origin, w, Removed::Delete);
        item_delete_row(t, author, rkey, Some(&removal)).await?;
    }
    t.put_tombstone(Collection::ListItem, author.id, &w.rkey, w.stamp)
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
    let row: Option<(ListId, Option<Stamp>, RecordState)> =
        sqlx::query_as("SELECT id, rev, record_state FROM lists WHERE owner_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    let tomb = t
        .tombstone_rev(Collection::List, author.id, &w.rkey)
        .await?;
    if !lww_upsert_wins(w.stamp, row.and_then(|x| x.1), tomb) {
        t.report.stale += 1;
        return Ok(());
    }
    let was_present = row.is_some_and(|x| x.2 == RecordState::Present);
    if let Some(refusal) = t.gate(cause, CapKind::Lists) {
        return t
            .refuse(author, Collection::List, &w.rkey, refusal, w.witness)
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
                    &w.rkey,
                    Refusal::Capped(CapType::ListsPerAuthor),
                    w.witness,
                )
                .await;
        }
        t.deltas.stat(stat::LISTS, 1);
        t.deltas.host(&author.buckets, CapKind::Lists, 1);
    }
    let list_id: ListId = sqlx::query_scalar(&format!(
        "INSERT INTO lists (owner_id, rkey, record_state, purpose, name, created_at, rev,
                            description, avatar_cid)
         VALUES ($1, $2, {RECORD_PRESENT}, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (owner_id, rkey) DO UPDATE SET record_state = {RECORD_PRESENT},
           purpose = EXCLUDED.purpose, name = EXCLUDED.name,
           created_at = EXCLUDED.created_at, rev = EXCLUDED.rev,
           description = EXCLUDED.description, avatar_cid = EXCLUDED.avatar_cid
         RETURNING id"
    ))
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
    // Any list record apply for L fires RP.
    t.fire(list_id, Event::RecordPresent, FireArgs::default())
        .await?;
    Ok(())
}

/// Marks a list record deleted at stamp `w` (firehose delete or
/// reconcile) and fires **RD**. The row is kept: other authors'
/// listblocks point at it.
pub(crate) async fn list_mark_deleted(
    t: &mut Txn<'_>,
    author: &AuthorInfo,
    list_id: ListId,
    was_present: bool,
    w: Stamp,
) -> Result<()> {
    sqlx::query(
        &format!("UPDATE lists SET record_state = {RECORD_DELETED}, purpose = NULL, name = NULL, created_at = NULL,
           description = NULL, avatar_cid = NULL,
           rev = GREATEST(COALESCE(rev, $2), $2)
         WHERE id = $1"),
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
    let row: Option<(ListId, Option<Stamp>, RecordState)> =
        sqlx::query_as("SELECT id, rev, record_state FROM lists WHERE owner_id = $1 AND rkey = $2")
            .bind(author.id)
            .bind(rkey)
            .fetch_optional(&mut *t.conn)
            .await?;
    if let Some((id, rev, state)) = row
        && rev.is_none_or(|r| r < w.stamp)
    {
        let was_present = state == RecordState::Present;
        list_mark_deleted(t, author, id, was_present, w.stamp).await?;
    }
    t.put_tombstone(Collection::List, author.id, &w.rkey, w.stamp)
        .await?;
    t.report.applied += 1;
    Ok(())
}

// -------------------------------------------------------------- reconcile

async fn reconcile_candidates(
    t: &mut Txn<'_>,
    author_id: ActorId,
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
        Collection::List => &format!(
            "SELECT rkey FROM lists WHERE owner_id = $1 AND record_state = {RECORD_PRESENT}
               AND (rev IS NULL OR rev < $2)
               AND ($3::text IS NULL OR rkey > $3) AND ($4::text IS NULL OR rkey <= $4)
               AND NOT (rkey = ANY($5)) ORDER BY rkey"
        ),
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
    // commit's rev nor a bound on it: no rev, the listing clock.
    let found = Removal::listing(Removed::Reconcile);
    // `rev < R` is checked again as the rows go: a write earlier in this
    // batch may have re-stamped one.
    let below = Some(r.stamp);
    let gone = match r.collection {
        Collection::Block => block_delete_rows(t, &author, candidates, below, Some(&found)).await?,
        Collection::ListBlock => {
            listblock_delete_rows(t, &author, candidates, below, Some(&found)).await?
        }
        Collection::ListItem => {
            item_delete_rows(t, &author, candidates, below, Some(&found)).await?
        }
        Collection::List => {
            let mut gone = 0;
            for rkey in candidates {
                let row: Option<(ListId, RecordState, Option<Stamp>)> = sqlx::query_as(
                    "SELECT id, record_state, rev FROM lists WHERE owner_id = $1 AND rkey = $2",
                )
                .bind(author.id)
                .bind(rkey.as_str())
                .fetch_optional(&mut *t.conn)
                .await?;
                match row {
                    Some((id, state, rev))
                        if state == RecordState::Present && rev.is_none_or(|rev| rev < r.stamp) =>
                    {
                        list_mark_deleted(t, &author, id, true, r.stamp).await?;
                        gone += 1;
                    }
                    _ => {}
                }
            }
            gone
        }
    };
    t.report.reconciled += gone;
    Ok(())
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
