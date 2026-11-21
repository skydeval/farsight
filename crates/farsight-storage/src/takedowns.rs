//! Lists the configured AppView no longer serves although their record
//! is present: taken down by the AppView's moderation (see
//! `docs/design/list-indexing.md`, `backfill.list_takedowns`).
//!
//! The columns `lists.appview_*` are written by the backfill process's
//! checker only, one list at a time, and read by the API and the pages:
//! `appview_absent_since` set is the mark. Nothing else about a list
//! changes with it; it still blocks in every answer, which says that it
//! is taken down.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

use crate::codes::sql::RECORD_PRESENT;
use crate::error::Result;
use crate::ids::ListId;

/// One list the checker is due to ask about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Due {
    /// `lists.id`.
    pub id: ListId,
    /// The owner's DID.
    pub owner_did: String,
    /// The list's record key.
    pub rkey: String,
    /// The record's `createdAt`, as its owner states it.
    pub created_at: Option<DateTime<Utc>>,
    /// What the checker found so far.
    pub was: Seen,
}

/// What the checker has found of a list: the `lists.appview_*` columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Seen {
    /// Answers in a row that said the list is not served.
    pub misses: i16,
    /// The last conclusive answer; while `misses` is 1, the first miss.
    pub checked_at: Option<DateTime<Utc>>,
    /// Set: the list is marked taken down, since then.
    pub absent_since: Option<DateTime<Utc>>,
    /// When the list is asked about next; `None`: never asked yet.
    pub due_at: Option<DateTime<Utc>>,
}

/// Up to `limit` lists due at `now`: those with a present record and at
/// least one counted listblock. Lists asked about before and due again
/// come first, longest due first, then lists never asked about, in id
/// order. Rechecks go first because they are few and they are what
/// confirms or clears a mark; taken the other way round, a first pass
/// over every list would hold a confirmation back for hours. Each half
/// is a range of `lists_appview_due`.
pub async fn due(pool: &PgPool, now: DateTime<Utc>, limit: i64) -> Result<Vec<Due>> {
    type Row = (
        ListId,
        String,
        String,
        Option<DateTime<Utc>>,
        i16,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
    );
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT l.id, o.did, l.rkey, l.created_at, l.appview_misses, l.appview_checked_at,
                l.appview_absent_since, l.appview_due_at
         FROM ((SELECT id, 0 AS k FROM lists
                WHERE listblock_count > 0 AND record_state = {RECORD_PRESENT}
                  AND appview_due_at <= $1
                ORDER BY appview_due_at, id LIMIT $2)
               UNION ALL
               (SELECT id, 1 AS k FROM lists
                WHERE listblock_count > 0 AND record_state = {RECORD_PRESENT}
                  AND appview_due_at IS NULL
                ORDER BY id LIMIT $2)) d
         JOIN lists l ON l.id = d.id
         JOIN actors o ON o.id = l.owner_id
         ORDER BY d.k, l.appview_due_at, l.id
         LIMIT $2"
    ))
    .bind(now)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| Due {
            id: r.0,
            owner_did: r.1,
            rkey: r.2,
            created_at: r.3,
            was: Seen {
                misses: r.4,
                checked_at: r.5,
                absent_since: r.6,
                due_at: r.7,
            },
        })
        .collect())
}

/// Stores what the checker found of list `id`.
pub async fn record(pool: &PgPool, id: ListId, s: &Seen) -> Result<()> {
    sqlx::query(
        "UPDATE lists SET appview_misses = $2, appview_checked_at = $3,
                          appview_absent_since = $4, appview_due_at = $5
         WHERE id = $1",
    )
    .bind(id)
    .bind(s.misses)
    .bind(s.checked_at)
    .bind(s.absent_since)
    .bind(s.due_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Clears the mark of every list the checker no longer looks at (its
/// record deleted, or no counted listblock left): nothing would check
/// it again. Returns how many were cleared.
pub async fn clear_left(pool: &PgPool) -> Result<u64> {
    Ok(sqlx::query(&format!(
        "UPDATE lists SET appview_absent_since = NULL, appview_misses = 0
         WHERE appview_absent_since IS NOT NULL
           AND (listblock_count = 0 OR record_state <> {RECORD_PRESENT})"
    ))
    .execute(pool)
    .await?
    .rows_affected())
}

/// Clears every mark (the checker is off). Returns how many were
/// cleared.
pub async fn clear_all(pool: &PgPool) -> Result<u64> {
    Ok(sqlx::query(
        "UPDATE lists SET appview_absent_since = NULL, appview_misses = 0
         WHERE appview_absent_since IS NOT NULL",
    )
    .execute(pool)
    .await?
    .rows_affected())
}

/// How many lists are marked taken down.
pub async fn count(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM lists WHERE appview_absent_since IS NOT NULL")
            .fetch_one(conn)
            .await?,
    )
}

/// The checker's last pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pass {
    /// When it ended; `None` before the first.
    pub at: Option<DateTime<Utc>>,
    /// Lists asked about in it.
    pub checks: i32,
    /// Of those, the answers that gave nothing to go by.
    pub errors: i32,
    /// The last such answer, in words, and when it came (kept across
    /// passes).
    pub last_error: Option<(String, DateTime<Utc>)>,
}

/// Stores the pass that just ended. `error` is the last inconclusive
/// answer of the pass, if it had one.
pub async fn record_pass(
    pool: &PgPool,
    checks: i32,
    errors: i32,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO list_takedown_checks (id, last_pass_at, last_checks, last_errors, last_error, last_error_at)
         VALUES (1, now(), $1, $2, $3, CASE WHEN $3::text IS NULL THEN NULL ELSE now() END)
         ON CONFLICT (id) DO UPDATE SET
           last_pass_at = EXCLUDED.last_pass_at,
           last_checks = EXCLUDED.last_checks,
           last_errors = EXCLUDED.last_errors,
           last_error = COALESCE(EXCLUDED.last_error, list_takedown_checks.last_error),
           last_error_at = COALESCE(EXCLUDED.last_error_at, list_takedown_checks.last_error_at)",
    )
    .bind(checks)
    .bind(errors)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

/// The checker's last pass, as [`record_pass`] stored it.
pub async fn last_pass(conn: &mut PgConnection) -> Result<Pass> {
    type Row = (
        Option<DateTime<Utc>>,
        i32,
        i32,
        Option<String>,
        Option<DateTime<Utc>>,
    );
    let row: Option<Row> = sqlx::query_as(
        "SELECT last_pass_at, last_checks, last_errors, last_error, last_error_at
         FROM list_takedown_checks WHERE id = 1",
    )
    .fetch_optional(conn)
    .await?;
    Ok(row
        .map(|r| Pass {
            at: r.0,
            checks: r.1,
            errors: r.2,
            last_error: r.3.zip(r.4),
        })
        .unwrap_or_default())
}
