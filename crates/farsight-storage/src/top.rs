//! The home page's top lists (design §8.6): the accounts that block the
//! most and that are blocked the most, for the last 24 hours and for all
//! time.
//!
//! None of these can be counted while a page is served: ranking every
//! subject of `blocks` reads the whole table. A background task computes
//! each list and stores it in `top_lists`; the page reads that row.
//!
//! "The last 24 hours" is counted from `block_recent`, a log the apply
//! path adds to when it stores a block whose own date is recent. A row
//! counts only while its block is still stored, so a block made and
//! removed again does not.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::error::Result;

/// How long a logged block counts as recent.
pub const RECENT_SECS: i64 = 24 * 3600;
/// How far ahead of its arrival a block's own date may be and still be
/// logged: a clock that runs a little fast is not a date in the future.
pub const SKEW_SECS: i64 = 300;

/// One of the four lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Most blocks made, all time.
    BlockersAll,
    /// Most blocked, all time.
    BlockedAll,
    /// Most blocks made in the last 24 hours.
    BlockersDay,
    /// Most blocked in the last 24 hours.
    BlockedDay,
}

impl Kind {
    /// The four.
    pub const ALL: [Kind; 4] = [
        Kind::BlockersAll,
        Kind::BlockedAll,
        Kind::BlockersDay,
        Kind::BlockedDay,
    ];

    /// `top_lists.kind`.
    pub fn key(self) -> &'static str {
        match self {
            Kind::BlockersAll => "blockers_all",
            Kind::BlockedAll => "blocked_all",
            Kind::BlockersDay => "blockers_day",
            Kind::BlockedDay => "blocked_day",
        }
    }

    /// Whether the list ranks the accounts that made the blocks.
    pub fn blockers(self) -> bool {
        matches!(self, Kind::BlockersAll | Kind::BlockersDay)
    }
}

/// A stored list: when it was computed and its `(DID, count)` rows, the
/// largest count first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// When the list was computed.
    pub computed_at: DateTime<Utc>,
    /// `(DID, count)`.
    pub rows: Vec<(String, i64)>,
}

/// Whether a block dated `created` that arrived at `seen` is logged.
pub fn is_recent(created: Option<DateTime<Utc>>, seen: DateTime<Utc>) -> bool {
    created.is_some_and(|c| {
        let age = (seen - c).num_seconds();
        (-SKEW_SECS..RECENT_SECS).contains(&age)
    })
}

/// Logs a block just stored. The caller has checked [`is_recent`].
pub async fn log(
    conn: &mut PgConnection,
    at: DateTime<Utc>,
    author_id: i64,
    rkey: &str,
    subject_id: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO block_recent (at, author_id, rkey, subject_id) VALUES ($1, $2, $3, $4)",
    )
    .bind(at)
    .bind(author_id)
    .bind(rkey)
    .bind(subject_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// Drops log rows older than the lists look back, with an hour to spare.
pub async fn trim(conn: &mut PgConnection) -> Result<u64> {
    Ok(
        sqlx::query("DELETE FROM block_recent WHERE at < now() - $1 * interval '1 second'")
            .bind(RECENT_SECS + 3600)
            .execute(conn)
            .await?
            .rows_affected(),
    )
}

/// Computes a list: the `limit` accounts with the largest counts, as
/// `(DID, count)`. Deactivated and deleted accounts are left out here;
/// what else a page hides is the page's rule. The all-time "most
/// blocked" list reads every row of `blocks` (about a minute and a half
/// for 150 million rows, with temporary files of a few GB); the others
/// take seconds. The caller sets the statement timeout.
pub async fn compute(
    conn: &mut PgConnection,
    kind: Kind,
    limit: i64,
) -> Result<Vec<(String, i64)>> {
    let sql = match kind {
        Kind::BlockersAll => "SELECT did, authored_blocks::bigint FROM actors
             WHERE authored_blocks > 0 AND status NOT IN (1, 4)
             ORDER BY authored_blocks DESC, id LIMIT $1"
            .to_owned(),
        Kind::BlockedAll => "SELECT a.did, t.n FROM (
               SELECT subject_id, count(*) AS n FROM blocks
               GROUP BY subject_id ORDER BY n DESC, subject_id LIMIT $1 * 2) t
             JOIN actors a ON a.id = t.subject_id
             WHERE a.status NOT IN (1, 4)
             ORDER BY t.n DESC, a.id LIMIT $1"
            .to_owned(),
        Kind::BlockersDay | Kind::BlockedDay => {
            let by = if kind.blockers() {
                "author_id"
            } else {
                "subject_id"
            };
            format!(
                "SELECT a.did, t.n FROM (
                   SELECT r.{by} AS id, count(*) AS n FROM block_recent r
                   JOIN blocks b ON b.author_id = r.author_id AND b.rkey = r.rkey
                                AND b.subject_id = r.subject_id
                   WHERE r.at > now() - {RECENT_SECS} * interval '1 second'
                   GROUP BY r.{by} ORDER BY n DESC, id LIMIT $1 * 2) t
                 JOIN actors a ON a.id = t.id
                 WHERE a.status NOT IN (1, 4)
                 ORDER BY t.n DESC, a.id LIMIT $1"
            )
        }
    };
    Ok(sqlx::query_as(&sql).bind(limit).fetch_all(conn).await?)
}

/// Stores a computed list in place of the previous one.
pub async fn save(conn: &mut PgConnection, kind: Kind, rows: &[(String, i64)]) -> Result<()> {
    let text = serde_json::to_string(rows).unwrap_or_else(|_| "[]".into());
    sqlx::query(
        "INSERT INTO top_lists (kind, computed_at, rows) VALUES ($1, now(), $2)
         ON CONFLICT (kind) DO UPDATE SET computed_at = EXCLUDED.computed_at, rows = EXCLUDED.rows",
    )
    .bind(kind.key())
    .bind(text)
    .execute(conn)
    .await?;
    Ok(())
}

/// When a list was last computed.
pub async fn computed_at(conn: &mut PgConnection, kind: Kind) -> Result<Option<DateTime<Utc>>> {
    Ok(
        sqlx::query_scalar("SELECT computed_at FROM top_lists WHERE kind = $1")
            .bind(kind.key())
            .fetch_optional(conn)
            .await?,
    )
}

/// A stored list, if it has been computed. A row that does not parse
/// reads as an empty list.
pub async fn load(conn: &mut PgConnection, kind: Kind) -> Result<Option<Stored>> {
    let row: Option<(DateTime<Utc>, String)> =
        sqlx::query_as("SELECT computed_at, rows FROM top_lists WHERE kind = $1")
            .bind(kind.key())
            .fetch_optional(conn)
            .await?;
    Ok(row.map(|(computed_at, text)| Stored {
        computed_at,
        rows: serde_json::from_str(&text).unwrap_or_default(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn only_a_block_dated_within_the_day_before_it_arrived_is_recent() {
        let seen = Utc.with_ymd_and_hms(2026, 10, 5, 12, 0, 0).unwrap();
        let ago = |s: i64| Some(seen - chrono::Duration::seconds(s));
        assert!(is_recent(ago(0), seen));
        assert!(is_recent(ago(RECENT_SECS - 1), seen));
        // History read by the backfill.
        assert!(!is_recent(ago(RECENT_SECS), seen));
        assert!(!is_recent(ago(400 * 86_400), seen));
        // A fast clock, but not a date in the future.
        assert!(is_recent(ago(-SKEW_SECS), seen));
        assert!(!is_recent(ago(-SKEW_SECS - 1), seen));
        assert!(!is_recent(None, seen));
    }
}
