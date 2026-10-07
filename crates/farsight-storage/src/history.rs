//! Block and list-membership history (see `docs/design/history.md`):
//! the record of rows Farsight stored and later removed.
//!
//! History is display data. It takes no part in LWW, list tracking or
//! coverage; nothing here is read by the apply path. Rows are written by
//! the three row-deleting functions of [`crate::apply`] when their caller
//! names a cause, inside the removing transaction and under the same
//! author lock. The account purge and the divergence purge name none.
//!
//! Each of those functions removes a set of rows with one statement, and
//! the history of the set is written the same way: one charge to the
//! daily rate for as many rows as it allows, one insert for those rows.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::PgPool;

use crate::error::{Result, StorageError};
use crate::ids::{ActorId, HistoryId, Stamp};
use crate::txn::{AuthorInfo, Txn};

/// `farsight_block_history_written_total{table,cause}`.
pub const WRITTEN: &str = "farsight_block_history_written_total";
/// `farsight_block_history_skipped_total{table,reason}`.
pub const SKIPPED: &str = "farsight_block_history_skipped_total";
/// `farsight_block_history_pruned_total{table}`.
pub const PRUNED: &str = "farsight_block_history_pruned_total";

/// Rows examined per retention batch.
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
    /// A listing no longer had the record: a range or whole-collection
    /// reconcile removed the row.
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

    /// The stored `cause` code: 1 to 5, in the order of [`Cause::ALL`].
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

    fn sql_value(&self) -> i16 {
        self.code()
    }

    fn from_sql_value(code: &i16) -> Option<Cause> {
        Cause::from_code(*code)
    }

    /// The `cause` label of `farsight_block_history_written_total`.
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

crate::codes::sqlx_code!(Cause, i16);

/// One of the three history tables; each takes the rows removed from one
/// live table.
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
    pub rev: Option<Stamp>,
    /// Witness of the removing firehose event; `None` for a listing, which
    /// uses the transaction's clock.
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
    /// Record key of the removed row.
    pub rkey: &'a str,
    /// Author-claimed `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
    /// The live row's `first_seen`: the witness time at which Farsight
    /// first stored it.
    pub first_seen: Option<DateTime<Utc>>,
    /// The live row's `last_seen`: the witness time of the last write
    /// applied to it.
    pub last_seen: Option<DateTime<Utc>>,
}

/// A removed live row on its way to its history table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GoneRow {
    /// Record key of the removed row.
    pub rkey: String,
    /// `blocks`, `list_items`: the subject. `list_blocks`: the list's
    /// owner.
    pub actor_id: ActorId,
    /// `list_blocks`, `list_items`: the list's rkey.
    pub list_rkey: Option<String>,
    /// Author-claimed `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
    /// The live row's `first_seen`: the witness time at which Farsight
    /// first stored it.
    pub first_seen: Option<DateTime<Utc>>,
    /// The live row's `last_seen`: the witness time of the last write
    /// applied to it.
    pub last_seen: Option<DateTime<Utc>>,
}

/// The history row a transaction wrote last, so that a subject change
/// whose new version is then refused can be re-labelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    table: Table,
    id: HistoryId,
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

    fn bump_skipped(&mut self, table: Table, by: u64) {
        if by == 0 {
            return;
        }
        match self.skipped.iter_mut().find(|(t, _)| *t == table) {
            Some((_, n)) => *n += by,
            None => self.skipped.push((table, by)),
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
    /// The witness time `w` of a write: the event's witness for a
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

    /// Charges up to `want` history rows to `key` for today and returns
    /// how many its daily limit left room for. Exact: runs inside the
    /// removing transaction, and the row is locked while it is read.
    async fn charge_history_many(&mut self, key: &str, want: i64) -> Result<i64> {
        let limit = self.limits.history_limit(key);
        if limit <= 0 || want <= 0 {
            return Ok(0);
        }
        if want == 1 {
            return Ok(i64::from(self.charge_history(key).await?));
        }
        sqlx::query(
            "INSERT INTO history_rate (key, utc_day, n) VALUES ($1, $2, 0)
             ON CONFLICT (key, utc_day) DO NOTHING",
        )
        .bind(key)
        .bind(self.today)
        .execute(&mut *self.conn)
        .await?;
        let granted: Option<i64> = sqlx::query_scalar(
            "WITH cur AS (
               SELECT n FROM history_rate WHERE key = $1 AND utc_day = $2 FOR UPDATE)
             UPDATE history_rate h SET n = LEAST(cur.n + $3, GREATEST(cur.n, $4))
             FROM cur WHERE h.key = $1 AND h.utc_day = $2
             RETURNING h.n - cur.n",
        )
        .bind(key)
        .bind(self.today)
        .bind(want)
        .bind(limit)
        .fetch_optional(&mut *self.conn)
        .await?;
        Ok(granted.unwrap_or(0).clamp(0, want))
    }

    /// How many of `want` removed rows of `author` may get a history row:
    /// none with history off, otherwise as many as the author's admission
    /// key has rate left for. Counts the rest as skipped.
    async fn history_admit(
        &mut self,
        author: &AuthorInfo,
        table: Table,
        want: usize,
    ) -> Result<usize> {
        if !self.limits.history_enabled || want == 0 {
            return Ok(0);
        }
        let asked = i64::try_from(want).unwrap_or(i64::MAX);
        let granted = self.charge_history_many(&author.key, asked).await?;
        let granted = usize::try_from(granted).unwrap_or(0).min(want);
        self.deltas
            .history
            .bump_skipped(table, (want - granted) as u64);
        Ok(granted)
    }

    /// Records removed rows of `table`, in the order given, as far as the
    /// author's daily rate allows: the first rows are recorded, the rest
    /// counted as skipped. For `list_items` the caller has checked the
    /// membership condition (the owner is not `deleted`; the list is
    /// tracked or its record is deleted). `list_blocks` and `list_items`
    /// rows name the list by owner and rkey, because `lists` rows can be
    /// deleted.
    pub(crate) async fn record_removals(
        &mut self,
        author: &AuthorInfo,
        table: Table,
        rows: &[GoneRow],
        r: &Removal,
    ) -> Result<()> {
        let granted = self.history_admit(author, table, rows.len()).await?;
        let rows = &rows[..granted];
        if rows.is_empty() {
            return Ok(());
        }
        let columns = match table {
            Table::Blocks => {
                "blocks_history (author_id, rkey, subject_id, created_at, first_seen, last_seen,
                                 removed_at, removed_rev, cause)
                 SELECT $1, u.rkey, u.actor_id,"
            }
            Table::ListBlocks => {
                "list_blocks_history (author_id, rkey, list_owner_id, list_rkey, created_at,
                                      first_seen, last_seen, removed_at, removed_rev, cause)
                 SELECT $1, u.rkey, u.actor_id, u.list_rkey,"
            }
            Table::ListItems => {
                "list_items_history (owner_id, rkey, list_rkey, subject_id, created_at,
                                     first_seen, last_seen, removed_at, removed_rev, cause)
                 SELECT $1, u.rkey, u.list_rkey, u.actor_id,"
            }
        };
        let ids: Vec<HistoryId> = sqlx::query_scalar(&format!(
            "INSERT INTO {columns} u.created_at, u.first_seen, u.last_seen,
                    GREATEST($8, u.last_seen), $9, $10
             FROM UNNEST($2::text[], $3::bigint[], $4::text[], $5::timestamptz[],
                         $6::timestamptz[], $7::timestamptz[]) WITH ORDINALITY
                  AS u(rkey, actor_id, list_rkey, created_at, first_seen, last_seen, ord)
             ORDER BY u.ord
             RETURNING id"
        ))
        .bind(author.id)
        .bind(rows.iter().map(|g| g.rkey.clone()).collect::<Vec<_>>())
        .bind(rows.iter().map(|g| g.actor_id).collect::<Vec<_>>())
        .bind(rows.iter().map(|g| g.list_rkey.clone()).collect::<Vec<_>>())
        .bind(rows.iter().map(|g| g.created_at).collect::<Vec<_>>())
        .bind(rows.iter().map(|g| g.first_seen).collect::<Vec<_>>())
        .bind(rows.iter().map(|g| g.last_seen).collect::<Vec<_>>())
        .bind(self.seen_at(r.witness))
        .bind(r.rev)
        .bind(r.cause.code())
        .fetch_all(&mut *self.conn)
        .await?;
        self.deltas.history.bump_written(
            table,
            r.cause,
            i64::try_from(ids.len()).unwrap_or(i64::MAX),
        );
        if let Some(id) = ids.iter().max() {
            self.last_history = Some(Written {
                table,
                id: *id,
                cause: r.cause,
            });
        }
        Ok(())
    }

    /// Records one removed `blocks` row (a subject change keeps the live
    /// row and records its old target).
    pub(crate) async fn record_block_removal(
        &mut self,
        author: &AuthorInfo,
        gone: &Gone<'_>,
        subject_id: ActorId,
        r: &Removal,
    ) -> Result<()> {
        let row = GoneRow {
            rkey: gone.rkey.to_owned(),
            actor_id: subject_id,
            list_rkey: None,
            created_at: gone.created_at,
            first_seen: gone.first_seen,
            last_seen: gone.last_seen,
        };
        self.record_removals(author, Table::Blocks, &[row], r).await
    }

    /// Forgets the last written history row, so that a later
    /// [`Txn::relabel_last_history`] cannot touch a row from an earlier
    /// write of the batch.
    pub(crate) fn clear_last_history(&mut self) {
        self.last_history = None;
    }

    /// Re-labels the history row this transaction wrote last (a subject
    /// change whose new version turned out to be refused is a
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

/// Opens or closes the recording window at server start: opens a row if
/// history is enabled and none is open, closes the open row if it is
/// disabled. Returns whether a window is open afterwards.
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

/// The daily retention pass: walks each history table in `id` order from
/// the lowest, in batches of [`PRUNE_BATCH`], deleting rows with
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
        let mut after = HistoryId::new(0);
        loop {
            let (last, seen, deleted): (Option<HistoryId>, i64, i64) = sqlx::query_as(&format!(
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
/// table (account purge). Returns the rows deleted.
pub(crate) async fn delete_authored(
    conn: &mut sqlx::PgConnection,
    author_id: ActorId,
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
        c.bump_skipped(Table::Blocks, 1);
        c.bump_skipped(Table::Blocks, 0);
        c.bump_skipped(Table::Blocks, 3);
        assert_eq!(c.skipped, [(Table::Blocks, 4)]);
        assert!(!c.is_empty());
    }
}
