//! The home page's top lists (see `docs/design/web-ui.md`): the
//! accounts that block the most and that are blocked the most, for the
//! last day and for all time.
//!
//! None of these can be counted while a page is served: ranking every
//! subject of `blocks` reads the whole table. A day runs from 05:00 EST
//! (10:00 UTC) to the same hour of the next; when one ends, a background
//! task counts the four lists and stores them in `top_lists`, and the
//! page reads those rows until the next day ends.
//!
//! The day's lists are counted from `block_recent`, a log the apply
//! path adds to when it stores a block whose own date is recent. A row
//! counts only while its block is still stored, so a block made and
//! removed again does not.

use crate::codes::sql::GONE;
use crate::ids::ActorId;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::error::Result;

/// How long a logged block counts as recent.
pub const RECENT_SECS: i64 = 24 * 3600;
/// How far ahead of its arrival a block's own date may be and still be
/// logged: a clock that runs a little fast is not a date in the future.
pub const SKEW_SECS: i64 = 300;

/// The hour of the day, UTC, at which a day of the lists ends and the
/// next begins: 05:00 EST.
pub const DAY_STARTS_UTC_HOUR: u32 = 10;

/// The end of the last whole day at `now`: the most recent 05:00 EST.
/// The lists shown until the next one were counted for it.
pub fn day_end(now: DateTime<Utc>) -> DateTime<Utc> {
    let today = now
        .date_naive()
        .and_hms_opt(DAY_STARTS_UTC_HOUR, 0, 0)
        .unwrap_or_default()
        .and_utc();
    if today <= now {
        today
    } else {
        today - chrono::Duration::days(1)
    }
}

/// One of the four top lists; each is one `top_lists` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Most blocks made, all time.
    BlockersAll,
    /// Most blocked, all time.
    BlockedAll,
    /// Most blocks made in the day that ended at the last 10:00 UTC.
    BlockersDay,
    /// Most blocked in the day that ended at the last 10:00 UTC.
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

/// A stored list: its `(DID, count)` rows, the largest count first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
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
    author_id: ActorId,
    rkey: &str,
    subject_id: ActorId,
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

/// Drops log rows no list can need any more: a day is counted from rows
/// up to two days old when a switch is turned on late in the next day.
pub async fn trim(conn: &mut PgConnection) -> Result<u64> {
    Ok(
        sqlx::query("DELETE FROM block_recent WHERE at < now() - $1 * interval '1 second'")
            .bind(2 * RECENT_SECS + 3600)
            .execute(conn)
            .await?
            .rows_affected(),
    )
}

/// Counts a list: the `limit` accounts with the largest counts, as
/// `(DID, count)`. The day's lists count the 24 hours before `end`; the
/// all-time lists count what is stored now. Deactivated and deleted accounts are left out here;
/// what else a page hides is the page's rule. The all-time "most
/// blocked" list reads every row of `blocks` (about a minute and a half
/// for 150 million rows, with temporary files of a few GB); the others
/// take seconds. The caller sets the statement timeout.
pub async fn compute(
    conn: &mut PgConnection,
    kind: Kind,
    end: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<(String, i64)>> {
    let sql = match kind {
        Kind::BlockersAll => format!(
            "SELECT did, authored_blocks::bigint FROM actors
             WHERE authored_blocks > 0 AND status NOT IN {GONE}
             ORDER BY authored_blocks DESC, id LIMIT $1"
        ),
        Kind::BlockedAll => format!(
            "SELECT a.did, t.n FROM (
               SELECT subject_id, count(*) AS n FROM blocks
               GROUP BY subject_id ORDER BY n DESC, subject_id LIMIT $1 * 2) t
             JOIN actors a ON a.id = t.subject_id
             WHERE a.status NOT IN {GONE}
             ORDER BY t.n DESC, a.id LIMIT $1"
        ),
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
                   WHERE r.at >= $2 - {RECENT_SECS} * interval '1 second' AND r.at < $2
                   GROUP BY r.{by} ORDER BY n DESC, id LIMIT $1 * 2) t
                 JOIN actors a ON a.id = t.id
                 WHERE a.status NOT IN {GONE}
                 ORDER BY t.n DESC, a.id LIMIT $1"
            )
        }
    };
    let q = sqlx::query_as(&sql).bind(limit);
    // Only the day's lists name the day.
    let q = if matches!(kind, Kind::BlockersDay | Kind::BlockedDay) {
        q.bind(end)
    } else {
        q
    };
    Ok(q.fetch_all(conn).await?)
}

/// Stores the list counted for the day that ended at `end` in place of
/// the previous one.
pub async fn save(
    conn: &mut PgConnection,
    kind: Kind,
    end: DateTime<Utc>,
    rows: &[(String, i64)],
) -> Result<()> {
    let text = serde_json::to_string(rows).unwrap_or_else(|_| "[]".into());
    sqlx::query(
        "INSERT INTO top_lists (kind, computed_at, rows) VALUES ($1, $2, $3)
         ON CONFLICT (kind) DO UPDATE SET computed_at = EXCLUDED.computed_at, rows = EXCLUDED.rows",
    )
    .bind(kind.key())
    .bind(end)
    .bind(text)
    .execute(conn)
    .await?;
    Ok(())
}

/// The end of the day the stored list was counted for
/// (`top_lists.computed_at`).
pub async fn stored_day(conn: &mut PgConnection, kind: Kind) -> Result<Option<DateTime<Utc>>> {
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
    let row: Option<String> = sqlx::query_scalar("SELECT rows FROM top_lists WHERE kind = $1")
        .bind(kind.key())
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|text| Stored {
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

    #[test]
    fn a_day_ends_at_five_in_the_morning_est() {
        let at = |d, h, m| Utc.with_ymd_and_hms(2026, 10, d, h, m, 0).unwrap();
        assert_eq!(day_end(at(5, 10, 0)), at(5, 10, 0));
        assert_eq!(day_end(at(5, 23, 59)), at(5, 10, 0));
        assert_eq!(day_end(at(6, 9, 59)), at(5, 10, 0));
        assert_eq!(day_end(at(6, 10, 1)), at(6, 10, 0));
    }
}
