//! Firehose progress, the witness clock and gaps (see
//! `docs/design/firehose.md` and `docs/design/coverage.md`).
//!
//! - Each committed ingest batch persists its cursor and the running
//!   maximum `applied_through`, and appends one `firehose_clock` row
//!   `(clock_timestamp(), applied_through)`, in the batch's own
//!   transaction ([`crate::apply`]).
//! - [`clock`] maps a server instant to the witness clock, rounding down;
//!   it is undefined (`None`) before the first batch.
//! - Gaps are stored on the witness clock. The v1 interval is one open
//!   `sync_unavailable` gap per v1 session.
//! - A seam window (`firehose_seams`) is stored when a resumed session
//!   delivers its first event and deleted when its re-read has been
//!   applied. One whose re-read keeps failing becomes a gap.

use chrono::{DateTime, Utc};
use sqlx::{PgExecutor, PgPool};

use crate::codes::{GapCause, Protocol, SeamTrigger};
use crate::error::Result;
use crate::ids::{CycleId, GapId, SeamId};
use crate::txn::Txn;

/// What an ingest batch persists alongside its writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirehoseProgress {
    /// The Jetstream instance the batch was read from, as configured in
    /// `firehose.urls`.
    pub source_url: String,
    /// Protocol of the session the batch was read on.
    pub protocol: Protocol,
    /// v2 cursor (`seq`, instance-local).
    pub cursor_seq: Option<i64>,
    /// v1 / failover cursor (`time_us`).
    pub cursor_us: Option<i64>,
    /// Witness time of the last event in the batch. Stored as a running
    /// maximum: a failover rewind never lowers it.
    pub applied_through: DateTime<Utc>,
}

impl Txn<'_> {
    /// Persists firehose progress and appends the clock row, in the
    /// caller's (batch) transaction.
    pub async fn record_firehose_progress(&mut self, p: &FirehoseProgress) -> Result<()> {
        let applied: DateTime<Utc> = sqlx::query_scalar(
            "INSERT INTO firehose_state (id, source_url, protocol, cursor_seq, cursor_us,
                                         applied_through, first_applied_at, connected)
             VALUES (1, $1, $2, $3, $4, $5, clock_timestamp(), true)
             ON CONFLICT (id) DO UPDATE SET
               source_url = EXCLUDED.source_url,
               protocol = EXCLUDED.protocol,
               -- Monotonic per instance (a v1 resume replays from cursor − 120 s);
               -- a different instance (failover) restarts the cursor space.
               cursor_seq = CASE WHEN firehose_state.source_url IS NOT DISTINCT FROM EXCLUDED.source_url
                                 THEN GREATEST(firehose_state.cursor_seq, EXCLUDED.cursor_seq)
                                 ELSE EXCLUDED.cursor_seq END,
               cursor_us = CASE WHEN firehose_state.source_url IS NOT DISTINCT FROM EXCLUDED.source_url
                                THEN GREATEST(firehose_state.cursor_us, EXCLUDED.cursor_us)
                                ELSE EXCLUDED.cursor_us END,
               applied_through = GREATEST(firehose_state.applied_through, EXCLUDED.applied_through),
               first_applied_at = COALESCE(firehose_state.first_applied_at, clock_timestamp()),
               connected = true
             RETURNING applied_through",
        )
        .bind(p.source_url.as_str())
        .bind(p.protocol.code())
        .bind(p.cursor_seq)
        .bind(p.cursor_us)
        .bind(p.applied_through)
        .fetch_one(&mut *self.conn)
        .await?;
        sqlx::query(
            "INSERT INTO firehose_clock (server_at, witness_at) VALUES (clock_timestamp(), $1)
             ON CONFLICT (server_at) DO UPDATE
               SET witness_at = GREATEST(firehose_clock.witness_at, EXCLUDED.witness_at)",
        )
        .bind(applied)
        .execute(&mut *self.conn)
        .await?;
        // The instance's own cursor: monotonic per source_url, kept
        // across failovers so a failback resumes from it.
        sqlx::query(
            "INSERT INTO firehose_cursors (source_url, protocol, cursor_seq, cursor_us,
                                          last_applied_through)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (source_url) DO UPDATE SET
               protocol = EXCLUDED.protocol,
               cursor_seq = GREATEST(firehose_cursors.cursor_seq, EXCLUDED.cursor_seq),
               cursor_us = GREATEST(firehose_cursors.cursor_us, EXCLUDED.cursor_us),
               last_applied_through = GREATEST(firehose_cursors.last_applied_through,
                                               EXCLUDED.last_applied_through)",
        )
        .bind(p.source_url.as_str())
        .bind(p.protocol.code())
        .bind(p.cursor_seq)
        .bind(p.cursor_us)
        .bind(p.applied_through)
        .execute(&mut *self.conn)
        .await?;
        Ok(())
    }
}

/// One instance's persisted cursor (`firehose_cursors`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceCursor {
    /// The instance URL; the row's key.
    pub source_url: String,
    /// Protocol of its last committed batch.
    pub protocol: Option<Protocol>,
    /// v2 cursor: the highest `seq` applied from this instance. A `seq`
    /// means something on its own instance only.
    pub cursor_seq: Option<i64>,
    /// v1 / timestamp cursor.
    pub cursor_us: Option<i64>,
    /// When a session to it last started.
    pub last_connected_at: Option<DateTime<Utc>>,
    /// Witness time of the last event applied from it.
    pub last_applied_through: Option<DateTime<Utc>>,
}

type CursorRow = (
    String,
    Option<i16>,
    Option<i64>,
    Option<i64>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

/// The persisted cursor of instance `source_url`, if it was ever used.
pub async fn instance_cursor<'e>(
    ex: impl PgExecutor<'e>,
    source_url: &str,
) -> Result<Option<InstanceCursor>> {
    let row: Option<CursorRow> = sqlx::query_as(
        "SELECT source_url, protocol, cursor_seq, cursor_us, last_connected_at,
                last_applied_through
         FROM firehose_cursors WHERE source_url = $1",
    )
    .bind(source_url)
    .fetch_optional(ex)
    .await?;
    Ok(row.map(|r| InstanceCursor {
        source_url: r.0,
        protocol: r.1.and_then(Protocol::from_code),
        cursor_seq: r.2,
        cursor_us: r.3,
        last_connected_at: r.4,
        last_applied_through: r.5,
    }))
}

/// Records that a session to `source_url` started (`last_connected_at`).
pub async fn mark_connected(pool: &PgPool, source_url: &str, protocol: Protocol) -> Result<()> {
    sqlx::query(
        "INSERT INTO firehose_cursors (source_url, protocol, last_connected_at)
         VALUES ($1, $2, now())
         ON CONFLICT (source_url) DO UPDATE SET last_connected_at = now()",
    )
    .bind(source_url)
    .bind(protocol.code())
    .execute(pool)
    .await?;
    Ok(())
}

/// Forgets the `seq` stored for instance `source_url`: the instance's
/// sequence started again, so the stored number names nothing in it. The
/// cursor is kept as a running maximum, which a lower `seq` could never
/// replace; the next batch from the instance stores its own. The
/// timestamp form of the cursor still holds and is kept.
pub async fn forget_instance_seq(pool: &PgPool, source_url: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE firehose_cursors SET cursor_seq = NULL WHERE source_url = $1")
        .bind(source_url)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE firehose_state SET cursor_seq = NULL WHERE id = 1 AND source_url = $1")
        .bind(source_url)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// The single `firehose_state` row. Every `Option` is `None` before the
/// first batch has committed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FirehoseState {
    /// Instance the last committed batch was read from.
    pub source_url: Option<String>,
    /// Protocol of the last committed batch.
    pub protocol: Option<Protocol>,
    /// v2 cursor: the highest `seq` applied from `source_url`. A `seq`
    /// means something on its own instance only.
    pub cursor_seq: Option<i64>,
    /// v1 and failover cursor: the highest witness time applied from
    /// `source_url`, in microseconds since the epoch.
    pub cursor_us: Option<i64>,
    /// Running-maximum witness time.
    pub applied_through: Option<DateTime<Utc>>,
    /// Server time of the first committed batch.
    pub first_applied_at: Option<DateTime<Utc>>,
    /// Whether a session is open, as last recorded by ingest. Coverage
    /// treats `false` as a synthetic gap.
    pub connected: bool,
}

type StateRow = (
    Option<String>,
    Option<i16>,
    Option<i64>,
    Option<i64>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    bool,
);

/// Reads `firehose_state` (default if no batch was ever committed).
pub async fn read_state<'e>(ex: impl PgExecutor<'e>) -> Result<FirehoseState> {
    let row: Option<StateRow> = sqlx::query_as(
        "SELECT source_url, protocol, cursor_seq, cursor_us, applied_through,
                first_applied_at, connected
         FROM firehose_state WHERE id = 1",
    )
    .fetch_optional(ex)
    .await?;
    Ok(match row {
        None => FirehoseState::default(),
        Some((source_url, protocol, cursor_seq, cursor_us, applied_through, first, connected)) => {
            FirehoseState {
                source_url,
                protocol: protocol.and_then(Protocol::from_code),
                cursor_seq,
                cursor_us,
                applied_through,
                first_applied_at: first,
                connected,
            }
        }
    })
}

/// Sets the connected flag (the ingest task calls this on connect and
/// disconnect; disconnection is a synthetic gap for coverage).
pub async fn set_connected(pool: &PgPool, connected: bool) -> Result<()> {
    sqlx::query(
        "INSERT INTO firehose_state (id, connected) VALUES (1, $1)
         ON CONFLICT (id) DO UPDATE SET connected = EXCLUDED.connected",
    )
    .bind(connected)
    .execute(pool)
    .await?;
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(pool)
        .await?;
    Ok(())
}

/// `clock(t)`: `applied_through` of the latest clock row with
/// `server_at ≤ t` (round down); `None` before the first batch.
pub async fn clock<'e>(ex: impl PgExecutor<'e>, t: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
    Ok(sqlx::query_scalar(
        "SELECT witness_at FROM firehose_clock WHERE server_at <= $1
         ORDER BY server_at DESC LIMIT 1",
    )
    .bind(t)
    .fetch_optional(ex)
    .await?)
}

/// `clock(now)` using the database clock, for coverage points taken "at the
/// start of the work" (`t` is read from the database).
pub async fn clock_now<'e>(ex: impl PgExecutor<'e>) -> Result<Option<DateTime<Utc>>> {
    Ok(sqlx::query_scalar(
        "SELECT witness_at FROM firehose_clock WHERE server_at <= clock_timestamp()
         ORDER BY server_at DESC LIMIT 1",
    )
    .fetch_optional(ex)
    .await?)
}

/// Downsamples `firehose_clock` rows older than 24 h to one per minute
/// (the latest of each minute — any subset still rounds down, since the
/// witness column is non-decreasing in server time) and deletes rows older
/// than 30 days. `now` is injectable for tests. Returns rows deleted.
pub async fn maintain_clock(pool: &PgPool, now: DateTime<Utc>) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let thinned = sqlx::query(
        "DELETE FROM firehose_clock c
         WHERE c.server_at < $1 - interval '24 hours'
           AND c.server_at >= $1 - interval '30 days'
           AND EXISTS (SELECT 1 FROM firehose_clock d
                       WHERE date_trunc('minute', d.server_at) = date_trunc('minute', c.server_at)
                         AND d.server_at > c.server_at)",
    )
    .bind(now)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    let expired =
        sqlx::query("DELETE FROM firehose_clock WHERE server_at < $1 - interval '30 days'")
            .bind(now)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    tx.commit().await?;
    Ok(thinned + expired)
}

/// A `firehose_gaps` row: an interval of the witness clock in which events
/// may have been lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gap {
    /// `firehose_gaps.id`.
    pub id: GapId,
    /// Start (witness clock).
    pub from_at: DateTime<Utc>,
    /// End; `None` while open (v1 interval).
    pub to_at: Option<DateTime<Utc>>,
    /// How the gap was detected.
    pub cause: GapCause,
    /// Healed by a repair cycle at this witness time.
    pub healed_witness: Option<DateTime<Utc>>,
}

async fn notify(pool: &PgPool) -> Result<()> {
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(pool)
        .await?;
    Ok(())
}

/// Records a closed gap `[from, to]` (`CursorTooOld`, heuristic v1 gap,
/// failover without a safe rewind).
pub async fn record_gap(
    pool: &PgPool,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    cause: GapCause,
) -> Result<GapId> {
    let id: GapId = sqlx::query_scalar(
        "INSERT INTO firehose_gaps (from_at, to_at, cause) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(from)
    .bind(to)
    .bind(cause.code())
    .fetch_one(pool)
    .await?;
    notify(pool).await?;
    Ok(id)
}

/// Opens the v1-interval gap when a v1 session starts. Idempotent:
/// returns the already-open `sync_unavailable` gap if there is one.
pub async fn open_sync_unavailable(pool: &PgPool, from: DateTime<Utc>) -> Result<GapId> {
    let mut tx = pool.begin().await?;
    // Serialize concurrent openers on a fixed advisory key.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('farsight:sync_unavailable_gap'))")
        .execute(&mut *tx)
        .await?;
    let open: Option<GapId> = sqlx::query_scalar(
        "SELECT id FROM firehose_gaps WHERE cause = $1 AND to_at IS NULL ORDER BY id LIMIT 1",
    )
    .bind(GapCause::SyncUnavailable.code())
    .fetch_optional(&mut *tx)
    .await?;
    let id = match open {
        Some(id) => id,
        None => {
            sqlx::query_scalar(
                "INSERT INTO firehose_gaps (from_at, cause) VALUES ($1, $2) RETURNING id",
            )
            .bind(from)
            .bind(GapCause::SyncUnavailable.code())
            .fetch_one(&mut *tx)
            .await?
        }
    };
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(id)
}

/// Closes the open v1-interval gap when a v2 session takes over.
/// Returns the closed gap's id, if one was open.
pub async fn close_sync_unavailable(pool: &PgPool, to: DateTime<Utc>) -> Result<Option<GapId>> {
    let id: Option<GapId> = sqlx::query_scalar(
        "UPDATE firehose_gaps SET to_at = $1 WHERE cause = $2 AND to_at IS NULL RETURNING id",
    )
    .bind(to)
    .bind(GapCause::SyncUnavailable.code())
    .fetch_optional(pool)
    .await?;
    notify(pool).await?;
    Ok(id)
}

/// Marks gaps healed by a completed repair cycle. Only closed gaps can
/// be healed.
pub async fn heal_gaps(
    pool: &PgPool,
    ids: &[GapId],
    healed_witness: DateTime<Utc>,
    repair_cycle_id: CycleId,
) -> Result<u64> {
    let n = sqlx::query(
        "UPDATE firehose_gaps SET healed_at = now(), healed_witness = $2, repair_cycle_id = $3
         WHERE id = ANY($1) AND to_at IS NOT NULL AND healed_at IS NULL",
    )
    .bind(ids)
    .bind(healed_witness)
    .bind(repair_cycle_id)
    .execute(pool)
    .await?
    .rows_affected();
    notify(pool).await?;
    Ok(n)
}

type GapRow = (
    GapId,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
    i16,
    Option<DateTime<Utc>>,
);

fn gap_from_row(r: GapRow) -> Gap {
    Gap {
        id: r.0,
        from_at: r.1,
        to_at: r.2,
        cause: GapCause::from_code(r.3).unwrap_or(GapCause::Heuristic),
        healed_witness: r.4,
    }
}

/// Unhealed gaps (open or closed), oldest first.
pub async fn unhealed_gaps<'e>(ex: impl PgExecutor<'e>) -> Result<Vec<Gap>> {
    let rows: Vec<GapRow> = sqlx::query_as(
        "SELECT id, from_at, to_at, cause, healed_witness FROM firehose_gaps
         WHERE healed_at IS NULL ORDER BY from_at, id",
    )
    .fetch_all(ex)
    .await?;
    Ok(rows.into_iter().map(gap_from_row).collect())
}

/// Every gap (for coverage: `completeSince` looks at healed gaps too).
pub async fn all_gaps<'e>(ex: impl PgExecutor<'e>) -> Result<Vec<Gap>> {
    let rows: Vec<GapRow> = sqlx::query_as(
        "SELECT id, from_at, to_at, cause, healed_witness FROM firehose_gaps ORDER BY from_at, id",
    )
    .fetch_all(ex)
    .await?;
    Ok(rows.into_iter().map(gap_from_row).collect())
}

/// A `firehose_seams` row whose window is closed: a stretch of one
/// instance's stream to read again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seam {
    /// `firehose_seams.id`.
    pub id: SeamId,
    /// The instance the window is read from.
    pub source_url: String,
    /// The protocol the resumed session spoke.
    pub protocol: Protocol,
    /// What kind of resume the window follows.
    pub trigger: SeamTrigger,
    /// Start of the window (witness clock).
    pub from_at: DateTime<Utc>,
    /// End of the window (witness clock).
    pub to_at: DateTime<Utc>,
    /// Re-reads of it that failed.
    pub attempts: i32,
}

/// Stores the seam window of a session resumed on `source_url`, open
/// (without an end) until [`close_seams`] is told how far the session
/// got. Written before the session's first event is applied, so a stop at
/// any later point leaves the window on record.
pub async fn open_seam(
    pool: &PgPool,
    source_url: &str,
    protocol: Protocol,
    trigger: SeamTrigger,
    from: DateTime<Utc>,
) -> Result<SeamId> {
    Ok(sqlx::query_scalar(
        "INSERT INTO firehose_seams (source_url, protocol, trigger, from_at)
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(source_url)
    .bind(protocol.code())
    .bind(trigger.code())
    .bind(from)
    .fetch_one(pool)
    .await?)
}

/// Closes every open seam window at `through` (witness clock), the point
/// its session reached: the window ends `after` later and its re-read is
/// due `delay` from now. A window whose session never reached its start
/// had no hand-over from replay to the live tail and is deleted. Returns
/// the number of windows closed.
pub async fn close_seams(
    pool: &PgPool,
    through: DateTime<Utc>,
    after: std::time::Duration,
    delay: std::time::Duration,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM firehose_seams WHERE to_at IS NULL AND from_at > $1")
        .bind(through)
        .execute(&mut *tx)
        .await?;
    let n = sqlx::query(
        "UPDATE firehose_seams
         SET to_at = $1 + $2 * interval '1 second',
             due_at = clock_timestamp() + $3 * interval '1 second'
         WHERE to_at IS NULL",
    )
    .bind(through)
    .bind(after.as_secs_f64())
    .bind(delay.as_secs_f64())
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(n)
}

type SeamRow = (SeamId, String, i16, i16, DateTime<Utc>, DateTime<Utc>, i32);

/// The closed seam windows whose re-read is due, oldest first.
pub async fn due_seams<'e>(ex: impl PgExecutor<'e>) -> Result<Vec<Seam>> {
    let rows: Vec<SeamRow> = sqlx::query_as(
        "SELECT id, source_url, protocol, trigger, from_at, to_at, attempts
         FROM firehose_seams
         WHERE to_at IS NOT NULL AND due_at <= clock_timestamp()
         ORDER BY from_at, id",
    )
    .fetch_all(ex)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| Seam {
            id: r.0,
            source_url: r.1,
            protocol: Protocol::from_code(r.2).unwrap_or(Protocol::V2),
            trigger: SeamTrigger::from_code(r.3).unwrap_or(SeamTrigger::Resume),
            from_at: r.4,
            to_at: r.5,
            attempts: r.6,
        })
        .collect())
}

/// The number of seam windows on record, open or closed.
pub async fn pending_seams<'e>(ex: impl PgExecutor<'e>) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM firehose_seams")
        .fetch_one(ex)
        .await?)
}

/// Deletes seam windows whose re-read has been applied.
pub async fn finish_seams(pool: &PgPool, ids: &[SeamId]) -> Result<u64> {
    Ok(sqlx::query("DELETE FROM firehose_seams WHERE id = ANY($1)")
        .bind(ids)
        .execute(pool)
        .await?
        .rows_affected())
}

/// Puts the re-read of seam windows off by `retry_in`. `failed` counts
/// the attempt against them; a re-read that ended without an answer
/// either way is put off without being counted.
pub async fn defer_seams(
    pool: &PgPool,
    ids: &[SeamId],
    retry_in: std::time::Duration,
    failed: bool,
) -> Result<()> {
    sqlx::query(
        "UPDATE firehose_seams
         SET due_at = clock_timestamp() + $2 * interval '1 second',
             attempts = attempts + $3
         WHERE id = ANY($1)",
    )
    .bind(ids)
    .bind(retry_in.as_secs_f64())
    .bind(i32::from(failed))
    .execute(pool)
    .await?;
    Ok(())
}

/// Gives up on seam windows: each becomes a closed gap of cause
/// [`GapCause::SeamUnrepaired`] over its window and its row is deleted,
/// in one transaction. Returns the gaps recorded.
pub async fn abandon_seams(pool: &PgPool, ids: &[SeamId]) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let n = sqlx::query(
        "WITH gone AS (DELETE FROM firehose_seams WHERE id = ANY($1) AND to_at IS NOT NULL
                       RETURNING from_at, to_at)
         INSERT INTO firehose_gaps (from_at, to_at, cause)
         SELECT from_at, to_at, $2 FROM gone",
    )
    .bind(ids)
    .bind(GapCause::SeamUnrepaired.code())
    .execute(&mut *tx)
    .await?
    .rows_affected();
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(n)
}
