//! Block and list-membership history (design §7.7, §7.8): the record of
//! rows Farsight stored and later removed.
//!
//! History is display data. It takes no part in LWW, list tracking or
//! coverage; nothing here is read by the apply path. Rows are written by
//! the three row-deleting functions of [`crate::apply`] when their caller
//! names a cause, inside the removing transaction and under the same
//! author lock. The account purge and the divergence purge name none.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::PgPool;

use crate::error::{Result, StorageError};
use crate::txn::{AuthorInfo, Txn};

/// `farsight_block_history_written_total{table,cause}`.
pub const WRITTEN: &str = "farsight_block_history_written_total";
/// `farsight_block_history_skipped_total{table,reason}`.
pub const SKIPPED: &str = "farsight_block_history_skipped_total";
/// `farsight_block_history_pruned_total{table}`.
pub const PRUNED: &str = "farsight_block_history_pruned_total";

/// Rows examined per retention batch (§7.7).
pub const PRUNE_BATCH: i64 = 10_000;

/// Why a live row was removed (the `cause` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cause {
    /// Firehose delete.
    Delete,
    /// A winning upsert named a different target.
    SubjectChange,
    /// The same, with the new version refused and the row deleted.
    RefusedUpdate,
    /// Range or whole reconcile.
    Reconcile,
    /// Purge drain while the list's record is deleted (listitems only).
    ListDeleted,
}

impl Cause {
    /// Every cause.
    pub const ALL: [Cause; 5] = [
        Cause::Delete,
        Cause::SubjectChange,
        Cause::RefusedUpdate,
        Cause::Reconcile,
        Cause::ListDeleted,
    ];

    /// Storage code.
    pub fn code(self) -> i16 {
        match self {
            Cause::Delete => 1,
            Cause::SubjectChange => 2,
            Cause::RefusedUpdate => 3,
            Cause::Reconcile => 4,
            Cause::ListDeleted => 5,
        }
    }

    /// From a storage code.
    pub fn from_code(c: i16) -> Option<Cause> {
        Cause::ALL.into_iter().find(|x| x.code() == c)
    }

    /// Metric label.
    pub fn label(self) -> &'static str {
        match self {
            Cause::Delete => "delete",
            Cause::SubjectChange => "subject_change",
            Cause::RefusedUpdate => "refused_update",
            Cause::Reconcile => "reconcile",
            Cause::ListDeleted => "list_deleted",
        }
    }
}

/// A history table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    /// `blocks_history`.
    Blocks,
    /// `list_blocks_history`.
    ListBlocks,
    /// `list_items_history`.
    ListItems,
}

impl Table {
    /// Every table.
    pub const ALL: [Table; 3] = [Table::Blocks, Table::ListBlocks, Table::ListItems];

    /// Metric label (the live table's name).
    pub fn label(self) -> &'static str {
        match self {
            Table::Blocks => "blocks",
            Table::ListBlocks => "list_blocks",
            Table::ListItems => "list_items",
        }
    }

    /// The history table's name.
    pub fn name(self) -> &'static str {
        match self {
            Table::Blocks => "blocks_history",
            Table::ListBlocks => "list_blocks_history",
            Table::ListItems => "list_items_history",
        }
    }
}

/// What the caller of a row-deleting function says about the removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Removal {
    /// Why the row goes.
    pub cause: Cause,
    /// Commit rev of the removing firehose event; `None` for a listing.
    pub rev: Option<i64>,
    /// Witness of the removing firehose event; `None` for a listing, which
    /// uses the transaction's clock (§7.7).
    pub witness: Option<DateTime<Utc>>,
}

impl Removal {
    /// A removal found by a listing or a drain: no rev, the listing clock.
    pub fn listing(cause: Cause) -> Removal {
        Removal {
            cause,
            rev: None,
            witness: None,
        }
    }
}

/// The columns a removed live row hands to its history row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gone<'a> {
    /// Record key.
    pub rkey: &'a str,
    /// Author-claimed `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
    /// Witness bound.
    pub first_seen: Option<DateTime<Utc>>,
    /// Witness bound.
    pub last_seen: Option<DateTime<Utc>>,
}

/// The history row a transaction wrote last, so that a subject change
/// whose new version is then refused can be re-labelled (§7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    table: Table,
    id: i64,
    cause: Cause,
}

/// History counts of one transaction, published after it commits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    /// Rows written per (table, cause).
    pub written: Vec<(Table, Cause, u64)>,
    /// Removals not recorded because the daily rate was spent, per table.
    pub skipped: Vec<(Table, u64)>,
}

impl Counts {
    fn bump_written(&mut self, table: Table, cause: Cause, by: i64) {
        match self
            .written
            .iter_mut()
            .find(|(t, c, _)| *t == table && *c == cause)
        {
            Some((_, _, n)) => *n = n.saturating_add_signed(by),
            None if by > 0 => self.written.push((table, cause, by as u64)),
            None => {}
        }
    }

    fn bump_skipped(&mut self, table: Table) {
        match self.skipped.iter_mut().find(|(t, _)| *t == table) {
            Some((_, n)) => *n += 1,
            None => self.skipped.push((table, 1)),
        }
    }

    /// Nothing to publish.
    pub fn is_empty(&self) -> bool {
        self.written.iter().all(|(_, _, n)| *n == 0) && self.skipped.is_empty()
    }

    /// Adds the counts to the process's metrics. Called once the
    /// transaction has committed.
    pub fn publish(&self) {
        for (t, c, n) in &self.written {
            if *n > 0 {
                metrics::counter!(WRITTEN, "table" => t.label(), "cause" => c.label())
                    .increment(*n);
            }
        }
        for (t, n) in &self.skipped {
            metrics::counter!(SKIPPED, "table" => t.label(), "reason" => "rate").increment(*n);
        }
    }
}

/// Registers the history series at zero so they are visible before first
/// use.
pub fn register_metrics() {
    for t in Table::ALL {
        metrics::counter!(SKIPPED, "table" => t.label(), "reason" => "rate").increment(0);
        metrics::counter!(PRUNED, "table" => t.label()).increment(0);
        for c in Cause::ALL {
            if c == Cause::ListDeleted && t != Table::ListItems {
                continue;
            }
            metrics::counter!(WRITTEN, "table" => t.label(), "cause" => c.label()).increment(0);
        }
    }
}

impl Txn<'_> {
    /// The witness time `w` of a write (§7.7): the event's witness for a
    /// firehose event; otherwise `clock()` of the transaction's start, or
    /// the database's `now()` while the clock is still undefined.
    pub fn seen_at(&self, witness: Option<DateTime<Utc>>) -> DateTime<Utc> {
        witness.or(self.clock_witness).unwrap_or(self.now)
    }

    /// Charges one history row to `key` for today, if under its daily
    /// limit. Exact: runs inside the removing transaction.
    async fn charge_history(&mut self, key: &str) -> Result<bool> {
        let limit = self.limits.history_limit(key);
        if limit <= 0 {
            return Ok(false);
        }
        let r: Option<i64> = sqlx::query_scalar(
            "INSERT INTO history_rate (key, utc_day, n) VALUES ($1, $2, 1)
             ON CONFLICT (key, utc_day) DO UPDATE SET n = history_rate.n + 1
               WHERE history_rate.n < $3
             RETURNING n",
        )
        .bind(key)
        .bind(self.today)
        .bind(limit)
        .fetch_optional(&mut *self.conn)
        .await?;
        Ok(r.is_some())
    }

    /// Whether a history row may be written for `author`'s removed row:
    /// history is enabled and the author's admission key has rate left.
    /// Counts the skip otherwise.
    async fn history_admit(&mut self, author: &AuthorInfo, table: Table) -> Result<bool> {
        if !self.limits.history_enabled {
            return Ok(false);
        }
        if !self.charge_history(&author.key).await? {
            self.deltas.history.bump_skipped(table);
            return Ok(false);
        }
        Ok(true)
    }

    fn history_written(&mut self, table: Table, id: i64, cause: Cause) {
        self.deltas.history.bump_written(table, cause, 1);
        self.last_history = Some(Written { table, id, cause });
    }

    /// Records a removed `blocks` row.
    pub(crate) async fn record_block_removal(
        &mut self,
        author: &AuthorInfo,
        gone: &Gone<'_>,
        subject_id: i64,
        r: &Removal,
    ) -> Result<()> {
        if !self.history_admit(author, Table::Blocks).await? {
            return Ok(());
        }
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO blocks_history (author_id, rkey, subject_id, created_at, first_seen,
                                         last_seen, removed_at, removed_rev, cause)
             VALUES ($1, $2, $3, $4, $5, $6, GREATEST($7, $6), $8, $9) RETURNING id",
        )
        .bind(author.id)
        .bind(gone.rkey)
        .bind(subject_id)
        .bind(gone.created_at)
        .bind(gone.first_seen)
        .bind(gone.last_seen)
        .bind(self.seen_at(r.witness))
        .bind(r.rev)
        .bind(r.cause.code())
        .fetch_one(&mut *self.conn)
        .await?;
        self.history_written(Table::Blocks, id, r.cause);
        Ok(())
    }

    /// Records a removed `list_blocks` row. The target is stored as the
    /// list's owner and rkey: `lists` rows can be deleted (§11.2).
    pub(crate) async fn record_listblock_removal(
        &mut self,
        author: &AuthorInfo,
        gone: &Gone<'_>,
        list_id: i64,
        r: &Removal,
    ) -> Result<()> {
        if !self.history_admit(author, Table::ListBlocks).await? {
            return Ok(());
        }
        let id: Option<i64> = sqlx::query_scalar(
            "INSERT INTO list_blocks_history (author_id, rkey, list_owner_id, list_rkey,
                                              created_at, first_seen, last_seen, removed_at,
                                              removed_rev, cause)
             SELECT $1, $2, l.owner_id, l.rkey, $4, $5, $6, GREATEST($7, $6), $8, $9
             FROM lists l WHERE l.id = $3 RETURNING id",
        )
        .bind(author.id)
        .bind(gone.rkey)
        .bind(list_id)
        .bind(gone.created_at)
        .bind(gone.first_seen)
        .bind(gone.last_seen)
        .bind(self.seen_at(r.witness))
        .bind(r.rev)
        .bind(r.cause.code())
        .fetch_optional(&mut *self.conn)
        .await?;
        if let Some(id) = id {
            self.history_written(Table::ListBlocks, id, r.cause);
        }
        Ok(())
    }

    /// Records a removed `list_items` row. The caller has checked §7.8's
    /// condition (the owner is not `deleted`; the list is tracked or its
    /// record is deleted).
    pub(crate) async fn record_item_removal(
        &mut self,
        owner: &AuthorInfo,
        gone: &Gone<'_>,
        list_rkey: &str,
        subject_id: i64,
        r: &Removal,
    ) -> Result<()> {
        if !self.history_admit(owner, Table::ListItems).await? {
            return Ok(());
        }
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO list_items_history (owner_id, rkey, list_rkey, subject_id, created_at,
                                             first_seen, last_seen, removed_at, removed_rev, cause)
             VALUES ($1, $2, $3, $4, $5, $6, $7, GREATEST($8, $7), $9, $10) RETURNING id",
        )
        .bind(owner.id)
        .bind(gone.rkey)
        .bind(list_rkey)
        .bind(subject_id)
        .bind(gone.created_at)
        .bind(gone.first_seen)
        .bind(gone.last_seen)
        .bind(self.seen_at(r.witness))
        .bind(r.rev)
        .bind(r.cause.code())
        .fetch_one(&mut *self.conn)
        .await?;
        self.history_written(Table::ListItems, id, r.cause);
        Ok(())
    }

    /// Forgets the last written history row, so that a later
    /// [`Txn::relabel_last_history`] cannot touch a row from an earlier
    /// write of the batch.
    pub(crate) fn clear_last_history(&mut self) {
        self.last_history = None;
    }

    /// Re-labels the history row this transaction wrote last (§7.2: a
    /// subject change whose new version turned out to be refused is a
    /// `refused_update`). A no-op when the removal was not recorded.
    pub(crate) async fn relabel_last_history(&mut self, cause: Cause) -> Result<()> {
        let Some(w) = self.last_history.take() else {
            return Ok(());
        };
        if w.cause == cause {
            return Ok(());
        }
        sqlx::query(&format!(
            "UPDATE {} SET cause = $2 WHERE id = $1",
            w.table.name()
        ))
        .bind(w.id)
        .bind(cause.code())
        .execute(&mut *self.conn)
        .await?;
        self.deltas.history.bump_written(w.table, w.cause, -1);
        self.deltas.history.bump_written(w.table, cause, 1);
        Ok(())
    }
}

/// Opens or closes the recording window at server start (§7.7): opens a
/// row if history is enabled and none is open, closes the open row if it
/// is disabled. Returns whether a window is open afterwards.
pub async fn sync_window(pool: &PgPool, enabled: bool) -> Result<bool> {
    let mut tx = pool.begin().await?;
    // One writer at a time; the table has a handful of rows.
    sqlx::query("LOCK TABLE history_windows IN EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    let open: i64 = sqlx::query_scalar("SELECT count(*) FROM history_windows WHERE to_at IS NULL")
        .fetch_one(&mut *tx)
        .await?;
    if enabled && open == 0 {
        sqlx::query("INSERT INTO history_windows DEFAULT VALUES")
            .execute(&mut *tx)
            .await?;
    } else if !enabled && open > 0 {
        sqlx::query("UPDATE history_windows SET to_at = now() WHERE to_at IS NULL")
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(enabled)
}

/// What one retention pass deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneReport {
    /// Rows deleted per table, in [`Table::ALL`] order.
    pub rows: [u64; 3],
    /// Closed `history_windows` rows deleted.
    pub windows: u64,
}

/// The daily retention pass (§7.7): walks each history table in `id` order
/// from the lowest, in batches of [`PRUNE_BATCH`], deleting rows with
/// `removed_at < now − retention`, and stops at the first batch that holds
/// no expired row. Also deletes recording windows closed before the
/// horizon. With a zero retention nothing is deleted. `now` is injectable
/// for tests.
pub async fn prune(
    pool: &PgPool,
    now: DateTime<Utc>,
    retention: std::time::Duration,
) -> Result<PruneReport> {
    let mut out = PruneReport::default();
    if retention.is_zero() {
        return Ok(out);
    }
    let cutoff = now
        - ChronoDuration::from_std(retention)
            .map_err(|e| StorageError::Invariant(e.to_string()))?;
    for (i, t) in Table::ALL.into_iter().enumerate() {
        let name = t.name();
        let mut after: i64 = 0;
        loop {
            let (last, seen, deleted): (Option<i64>, i64, i64) = sqlx::query_as(&format!(
                "WITH batch AS (
                   SELECT id, removed_at FROM {name} WHERE id > $1 ORDER BY id LIMIT $2),
                 del AS (
                   DELETE FROM {name} WHERE id IN (SELECT id FROM batch WHERE removed_at < $3)
                   RETURNING 1)
                 SELECT (SELECT max(id) FROM batch), (SELECT count(*) FROM batch),
                        (SELECT count(*) FROM del)"
            ))
            .bind(after)
            .bind(PRUNE_BATCH)
            .bind(cutoff)
            .fetch_one(pool)
            .await?;
            out.rows[i] += deleted as u64;
            match last {
                Some(l) if deleted > 0 && seen == PRUNE_BATCH => after = l,
                _ => break,
            }
        }
        if out.rows[i] > 0 {
            metrics::counter!(PRUNED, "table" => t.label()).increment(out.rows[i]);
        }
    }
    out.windows = sqlx::query("DELETE FROM history_windows WHERE to_at IS NOT NULL AND to_at < $1")
        .bind(cutoff)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(out)
}

/// Deletes up to `batch` history rows authored by `author_id` from each
/// table (account purge, §7.4). Returns the rows deleted.
pub(crate) async fn delete_authored(
    conn: &mut sqlx::PgConnection,
    author_id: i64,
    batch: i64,
) -> Result<u64> {
    let mut n = 0;
    for (table, col) in [
        ("blocks_history", "author_id"),
        ("list_blocks_history", "author_id"),
        ("list_items_history", "owner_id"),
    ] {
        n += sqlx::query(&format!(
            "DELETE FROM {table} WHERE id IN (
               SELECT id FROM {table} WHERE {col} = $1 LIMIT $2)"
        ))
        .bind(author_id)
        .bind(batch)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cause_codes_round_trip() {
        for c in Cause::ALL {
            assert_eq!(Cause::from_code(c.code()), Some(c));
        }
        assert_eq!(Cause::from_code(0), None);
        assert_eq!(Cause::ListDeleted.code(), 5);
    }

    #[test]
    fn counts_relabel() {
        let mut c = Counts::default();
        assert!(c.is_empty());
        c.bump_written(Table::ListBlocks, Cause::SubjectChange, 1);
        c.bump_written(Table::ListBlocks, Cause::SubjectChange, -1);
        c.bump_written(Table::ListBlocks, Cause::RefusedUpdate, 1);
        assert_eq!(
            c.written
                .iter()
                .filter(|(_, _, n)| *n > 0)
                .collect::<Vec<_>>(),
            [&(Table::ListBlocks, Cause::RefusedUpdate, 1)]
        );
        c.bump_skipped(Table::Blocks);
        c.bump_skipped(Table::Blocks);
        assert_eq!(c.skipped, [(Table::Blocks, 2)]);
        assert!(!c.is_empty());
    }
}
