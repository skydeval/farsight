//! Firehose progress, the witness clock and gaps (design §3.7.1, §6.2,
//! §6.3, §6.5).
//!
//! - Each committed ingest batch persists its cursor and the running
//!   maximum `applied_through`, and appends one `firehose_clock` row
//!   `(clock_timestamp(), applied_through)`, in the batch's own
//!   transaction ([`crate::apply`]).
//! - [`clock`] maps a server instant to the witness clock, rounding down;
//!   it is undefined (`None`) before the first batch.
//! - Gaps are stored on the witness clock. The v1 interval is one open
//!   `sync_unavailable` gap per v1 session.

use chrono::{DateTime, Utc};
use sqlx::{PgExecutor, PgPool};

use crate::codes::{GapCause, Protocol};
use crate::error::Result;
use crate::txn::Txn;

/// What an ingest batch persists alongside its writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FirehoseProgress {
    /// The Jetstream instance URL.
    pub source_url: String,
    /// Protocol of the session.
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
        // The instance's own cursor (§6.2, r17.2): monotonic per source_url,
        // kept across failovers so a failback resumes from it.
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

/// One instance's persisted cursor (`firehose_cursors`, §6.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceCursor {
    /// The instance URL.
    pub source_url: String,
    /// Protocol of its last committed batch.
    pub protocol: Option<Protocol>,
    /// v2 cursor.
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

/// The persisted firehose state.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FirehoseState {
    /// Source URL.
    pub source_url: Option<String>,
    /// Protocol.
    pub protocol: Option<Protocol>,
    /// v2 cursor.
    pub cursor_seq: Option<i64>,
    /// v1 cursor.
    pub cursor_us: Option<i64>,
    /// Running-maximum witness time.
    pub applied_through: Option<DateTime<Utc>>,
    /// Server time of the first committed batch.
    pub first_applied_at: Option<DateTime<Utc>>,
    /// Connected flag.
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
/// disconnect; disconnection is a synthetic gap for coverage, §6.3).
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

/// `clock(t)` (§3.7.1): `applied_through` of the latest clock row with
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
/// start of the work" (§3.7.1: `t` is read from the database).
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

/// A stored gap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gap {
    /// `firehose_gaps.id`.
    pub id: i64,
    /// Start (witness clock).
    pub from_at: DateTime<Utc>,
    /// End; `None` while open (v1 interval).
    pub to_at: Option<DateTime<Utc>>,
    /// Cause.
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
/// failover without a safe rewind; §6.3).
pub async fn record_gap(
    pool: &PgPool,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    cause: GapCause,
) -> Result<i64> {
    let id: i64 = sqlx::query_scalar(
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

/// Opens the v1-interval gap when a v1 session starts (§6.5). Idempotent:
/// returns the already-open `sync_unavailable` gap if there is one.
pub async fn open_sync_unavailable(pool: &PgPool, from: DateTime<Utc>) -> Result<i64> {
    let mut tx = pool.begin().await?;
    // Serialize concurrent openers on a fixed advisory key.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('farsight:sync_unavailable_gap'))")
        .execute(&mut *tx)
        .await?;
    let open: Option<i64> = sqlx::query_scalar(
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

/// Closes the open v1-interval gap when a v2 session takes over (§6.5).
/// Returns the closed gap's id, if one was open.
pub async fn close_sync_unavailable(pool: &PgPool, to: DateTime<Utc>) -> Result<Option<i64>> {
    let id: Option<i64> = sqlx::query_scalar(
        "UPDATE firehose_gaps SET to_at = $1 WHERE cause = $2 AND to_at IS NULL RETURNING id",
    )
    .bind(to)
    .bind(GapCause::SyncUnavailable.code())
    .fetch_optional(pool)
    .await?;
    notify(pool).await?;
    Ok(id)
}

/// Marks gaps healed by a completed repair cycle (§7.5 step 4). Only
/// closed gaps can be healed.
pub async fn heal_gaps(
    pool: &PgPool,
    ids: &[i64],
    healed_witness: DateTime<Utc>,
    repair_cycle_id: i64,
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
    i64,
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
