//! Read-only queries behind the public UI and the admin history pages
//! (design §7.7, §7.8, §8.6): removed blocks, listblocks and list
//! memberships, bounded counts and recording windows. The rows of the live
//! sections are read in [`crate::ui_rows`].
//!
//! Two filters are part of every query here and run at query time, so a
//! status change or a settings change shows on the next page view:
//!
//! - **hidden status** (§3.1, §7.4): rows whose relevant account — the
//!   author, the list owner or the subject, whichever the page lists — is
//!   `deactivated`, `takendown`, `suspended` or `deleted` are left out.
//!   For `deleted` accounts this filter, not the purge, is the guarantee.
//! - **operator exclusion**: rows whose relevant account is in `excluded`
//!   (`actors.id` values of `public_ui.excluded_dids`) are left out the
//!   same way.
//!
//! History pages order by (`removed_at`, `id`) descending and the cursor
//! carries both: one reconcile gives many rows the same `removed_at`, and
//! `id` tells them apart. Every history query is an index range on one of
//! the six history indexes, which all end in `id`.
//!
//! All functions take a connection so the caller can run them inside one
//! read transaction with `statement_timeout` set (§3.6).

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::codes::TrackState;
use crate::error::Result;

/// SQL fragment: status codes hidden by default (§3.1, §7.4).
const HIDDEN: &str = "(1, 2, 3, 4)";

/// Position in a history section: the last row returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryCursor {
    /// Its `removed_at`.
    pub removed_at: DateTime<Utc>,
    /// Its `id`.
    pub id: i64,
}

/// What every history query takes besides its key.
#[derive(Debug, Clone, Copy)]
pub struct HistoryArgs<'a> {
    /// `actors.id` values the operator excludes.
    pub excluded: &'a [i64],
    /// Retention horizon: rows removed before it are not returned, whether
    /// or not the janitor has deleted them yet. `None` = keep forever.
    pub horizon: Option<DateTime<Utc>>,
    /// Keyset position; `None` = first page.
    pub after: Option<HistoryCursor>,
    /// Page size.
    pub limit: i64,
}

/// The list a removed listblock or listitem pointed at, as far as a
/// `lists` row for it still exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedList {
    /// Owner DID.
    pub owner_did: String,
    /// List record key.
    pub rkey: String,
    /// Name, when the row has one.
    pub name: Option<String>,
    /// `purpose` code, when the record is present.
    pub purpose: Option<i16>,
    /// The list's state as `getListMembers` reports it; `None` when no
    /// `lists` row exists.
    pub state: Option<TrackState>,
}

/// One removed record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removed {
    /// History row id (second cursor key).
    pub id: i64,
    /// When Farsight applied the removal (first cursor key).
    pub removed_at: DateTime<Utc>,
    /// The other account of the row: the blocker on a by-subject or
    /// by-list page, the blocked account or the member on a by-author or
    /// by-list membership page. Empty for rows identified by `list` alone.
    pub party: String,
    /// The list, for rows that point at one from an account's page.
    pub list: Option<RemovedList>,
    /// Record key of the removed record.
    pub rkey: String,
    /// Author-claimed `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
    /// When Farsight first stored the record; `None`: before it kept dates.
    pub first_seen: Option<DateTime<Utc>>,
    /// When Farsight last saw the record.
    pub last_seen: Option<DateTime<Utc>>,
    /// `cause` code (`history::Cause`).
    pub cause: i16,
    /// The same (author, target) or (list, subject) has a live row now.
    pub live: bool,
}

impl Removed {
    /// The cursor that continues after this row.
    pub fn cursor(&self) -> HistoryCursor {
        HistoryCursor {
            removed_at: self.removed_at,
            id: self.id,
        }
    }
}

/// The cursor after a full page; `None` when the query ran out.
pub fn next_cursor(rows: &[Removed], limit: i64) -> Option<HistoryCursor> {
    if rows.len() as i64 == limit {
        rows.last().map(Removed::cursor)
    } else {
        None
    }
}

/// The `WHERE` tail shared by the history queries: retention horizon and
/// keyset. `$3` is the horizon, `$4`/`$5` the cursor, `$6` the limit. The
/// fragments are chosen by which arguments are present so that the keyset
/// is always an index boundary, never a filter.
fn tail(a: &HistoryArgs<'_>) -> String {
    let horizon = if a.horizon.is_some() {
        "AND h.removed_at >= $3"
    } else {
        "AND $3::timestamptz IS NULL"
    };
    let keyset = if a.after.is_some() {
        "AND (h.removed_at, h.id) < ($4, $5)"
    } else {
        "AND $4::timestamptz IS NULL AND $5::bigint IS NULL"
    };
    format!("{horizon} {keyset} ORDER BY h.removed_at DESC, h.id DESC LIMIT $6")
}

type PartyRow = (
    i64,
    DateTime<Utc>,
    String,
    String,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    i16,
    bool,
);

fn party_rows(rows: Vec<PartyRow>) -> Vec<Removed> {
    rows.into_iter()
        .map(|r| Removed {
            id: r.0,
            removed_at: r.1,
            party: r.2,
            list: None,
            rkey: r.3,
            created_at: r.4,
            first_seen: r.5,
            last_seen: r.6,
            cause: r.7,
            live: r.8,
        })
        .collect()
}

/// Removed blocks naming `subject_id`, newest removal first
/// (`blocks_history_by_subject`). `party` is the blocker; rows whose
/// blocker is hidden or excluded are left out. `live`: the blocker has a
/// block on the subject now.
pub async fn blocks_history_by_subject(
    conn: &mut PgConnection,
    subject_id: i64,
    a: HistoryArgs<'_>,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, p.did, h.rkey, h.created_at, h.first_seen, h.last_seen,
                h.cause,
                EXISTS (SELECT 1 FROM blocks b
                        WHERE b.subject_id = h.subject_id AND b.author_id = h.author_id)
         FROM blocks_history h JOIN actors p ON p.id = h.author_id
         WHERE h.subject_id = $1 AND p.status NOT IN {HIDDEN} AND NOT (p.id = ANY($2)) {}",
        tail(&a)
    );
    fetch_party(conn, &sql, subject_id, a).await
}

/// Removed blocks authored by `author_id`, newest removal first
/// (`blocks_history_by_author`). `party` is the blocked account; rows
/// whose blocked account is hidden or excluded are left out. `live`: the
/// author blocks that account now.
pub async fn blocks_history_by_author(
    conn: &mut PgConnection,
    author_id: i64,
    a: HistoryArgs<'_>,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, p.did, h.rkey, h.created_at, h.first_seen, h.last_seen,
                h.cause,
                EXISTS (SELECT 1 FROM blocks b
                        WHERE b.subject_id = h.subject_id AND b.author_id = h.author_id)
         FROM blocks_history h JOIN actors p ON p.id = h.subject_id
         WHERE h.author_id = $1 AND p.status NOT IN {HIDDEN} AND NOT (p.id = ANY($2)) {}",
        tail(&a)
    );
    fetch_party(conn, &sql, author_id, a).await
}

async fn fetch_party(
    conn: &mut PgConnection,
    sql: &str,
    key: i64,
    a: HistoryArgs<'_>,
) -> Result<Vec<Removed>> {
    let rows: Vec<PartyRow> = sqlx::query_as(sql)
        .bind(key)
        .bind(a.excluded)
        .bind(a.horizon)
        .bind(a.after.map(|c| c.removed_at))
        .bind(a.after.map(|c| c.id))
        .bind(a.limit)
        .fetch_all(conn)
        .await?;
    Ok(party_rows(rows))
}

/// Removed listblocks on the list (`owner_id`, `list_rkey`), newest
/// removal first (`list_blocks_history_by_list`). `party` is the
/// listblocker; rows whose listblocker is hidden or excluded are left out
/// (the caller withholds the whole page when the owner is). `live`: the
/// listblocker has a listblock on the list now. Needs no `lists` row.
pub async fn list_blocks_history_by_list(
    conn: &mut PgConnection,
    owner_id: i64,
    list_rkey: &str,
    a: HistoryArgs<'_>,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, p.did, h.rkey, h.created_at, h.first_seen, h.last_seen,
                h.cause,
                EXISTS (SELECT 1 FROM lists l JOIN list_blocks b ON b.list_id = l.id
                        WHERE l.owner_id = h.list_owner_id AND l.rkey = h.list_rkey
                          AND b.author_id = h.author_id)
         FROM list_blocks_history h JOIN actors p ON p.id = h.author_id
         WHERE h.list_owner_id = $1 AND h.list_rkey = $7
           AND p.status NOT IN {HIDDEN} AND NOT (p.id = ANY($2)) {}",
        tail(&a)
    );
    let rows: Vec<PartyRow> = sqlx::query_as(&sql)
        .bind(owner_id)
        .bind(a.excluded)
        .bind(a.horizon)
        .bind(a.after.map(|c| c.removed_at))
        .bind(a.after.map(|c| c.id))
        .bind(a.limit)
        .bind(list_rkey)
        .fetch_all(conn)
        .await?;
    Ok(party_rows(rows))
}

type ListRow = (
    i64,
    DateTime<Utc>,
    String,
    String,
    String,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    i16,
    bool,
    Option<String>,
    Option<i16>,
    Option<i16>,
    Option<i16>,
    Option<i16>,
    Option<i32>,
);

/// `l.name, l.purpose, l.record_state, l.track_state, l.purge_then,
/// l.listblock_count` of an outer-joined `lists` row.
const LIST_COLS: &str =
    "l.name, l.purpose, l.record_state, l.track_state, l.purge_then, l.listblock_count";

fn list_rows(rows: Vec<ListRow>) -> Vec<Removed> {
    rows.into_iter()
        .map(|r| {
            // `purging` reads as `pending` when it will be re-admitted,
            // else `untracked`, as `getListMembers` reports it (§3.2).
            let state = r.13.and_then(TrackState::from_code).map(|s| match s {
                TrackState::Purging => {
                    if r.14 == Some(TrackState::Untracked.code()) && r.15.unwrap_or(0) > 0 {
                        TrackState::Pending
                    } else {
                        TrackState::Untracked
                    }
                }
                s => s,
            });
            Removed {
                id: r.0,
                removed_at: r.1,
                party: String::new(),
                list: Some(RemovedList {
                    owner_did: r.2,
                    rkey: r.3,
                    name: r.10,
                    // A deleted list has no purpose (§7.4).
                    purpose: if r.12 == Some(1) { r.11 } else { None },
                    state,
                }),
                rkey: r.4,
                created_at: r.5,
                first_seen: r.6,
                last_seen: r.7,
                cause: r.8,
                live: r.9,
            }
        })
        .collect()
}

/// Removed listblocks authored by `author_id`, newest removal first
/// (`list_blocks_history_by_author`). Each row names the list; rows whose
/// list owner is hidden or excluded are left out. `lists` is outer-joined:
/// the row can be missing (§11.2).
pub async fn list_blocks_history_by_author(
    conn: &mut PgConnection,
    author_id: i64,
    a: HistoryArgs<'_>,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, o.did, h.list_rkey, h.rkey, h.created_at, h.first_seen,
                h.last_seen, h.cause,
                EXISTS (SELECT 1 FROM list_blocks b
                        WHERE b.author_id = h.author_id AND b.list_id = l.id),
                {LIST_COLS}
         FROM list_blocks_history h
         JOIN actors o ON o.id = h.list_owner_id
         LEFT JOIN lists l ON l.owner_id = h.list_owner_id AND l.rkey = h.list_rkey
         WHERE h.author_id = $1 AND o.status NOT IN {HIDDEN} AND NOT (o.id = ANY($2)) {}",
        tail(&a)
    );
    fetch_list(conn, &sql, author_id, a).await
}

/// Removed list memberships naming `subject_id`, newest removal first
/// (`list_items_history_by_subject`). Each row names the list; rows whose
/// list owner is hidden or excluded are left out. `lists` is outer-joined
/// (§7.8). `live`: the list has a live listitem naming the subject now.
pub async fn list_items_history_by_subject(
    conn: &mut PgConnection,
    subject_id: i64,
    a: HistoryArgs<'_>,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, o.did, h.list_rkey, h.rkey, h.created_at, h.first_seen,
                h.last_seen, h.cause,
                EXISTS (SELECT 1 FROM list_items li
                        WHERE li.subject_id = h.subject_id AND li.list_id = l.id),
                {LIST_COLS}
         FROM list_items_history h
         JOIN actors o ON o.id = h.owner_id
         LEFT JOIN lists l ON l.owner_id = h.owner_id AND l.rkey = h.list_rkey
         WHERE h.subject_id = $1 AND o.status NOT IN {HIDDEN} AND NOT (o.id = ANY($2)) {}",
        tail(&a)
    );
    fetch_list(conn, &sql, subject_id, a).await
}

async fn fetch_list(
    conn: &mut PgConnection,
    sql: &str,
    key: i64,
    a: HistoryArgs<'_>,
) -> Result<Vec<Removed>> {
    let rows: Vec<ListRow> = sqlx::query_as(sql)
        .bind(key)
        .bind(a.excluded)
        .bind(a.horizon)
        .bind(a.after.map(|c| c.removed_at))
        .bind(a.after.map(|c| c.id))
        .bind(a.limit)
        .fetch_all(conn)
        .await?;
    Ok(list_rows(rows))
}

/// Removed members of the list (`owner_id`, `list_rkey`), newest removal
/// first (`list_items_history_by_list`). `party` is the removed member;
/// rows whose member is hidden or excluded are left out. `live`: the
/// member has a live listitem on the list now. Needs no `lists` row.
pub async fn list_items_history_by_list(
    conn: &mut PgConnection,
    owner_id: i64,
    list_rkey: &str,
    a: HistoryArgs<'_>,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, p.did, h.rkey, h.created_at, h.first_seen, h.last_seen,
                h.cause,
                EXISTS (SELECT 1 FROM lists l JOIN list_items li ON li.list_id = l.id
                        WHERE l.owner_id = h.owner_id AND l.rkey = h.list_rkey
                          AND li.subject_id = h.subject_id)
         FROM list_items_history h JOIN actors p ON p.id = h.subject_id
         WHERE h.owner_id = $1 AND h.list_rkey = $7
           AND p.status NOT IN {HIDDEN} AND NOT (p.id = ANY($2)) {}",
        tail(&a)
    );
    let rows: Vec<PartyRow> = sqlx::query_as(&sql)
        .bind(owner_id)
        .bind(a.excluded)
        .bind(a.horizon)
        .bind(a.after.map(|c| c.removed_at))
        .bind(a.after.map(|c| c.id))
        .bind(a.limit)
        .bind(list_rkey)
        .fetch_all(conn)
        .await?;
    Ok(party_rows(rows))
}

/// Which section a bounded count is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Counted {
    /// Blocks naming an account (`blocks_by_subject`).
    IncomingBlocks,
    /// Blocks authored by an account (`blocks` primary key).
    OutgoingBlocks,
    /// Listblocks on a list (`list_blocks_by_list`).
    ListBlockers,
    /// Listitems of a list (`list_items_by_list`).
    ListMembers,
}

/// Counts the records of a section with the section's filters, scanning
/// its own index and stopping after `cap + 1` rows: a result above `cap`
/// means "more than `cap`". Counts records, not accounts.
///
/// Counts with the public tables' rule: suspended accounts count;
/// accounts a host has taken down count only with `takendown`; deactivated
/// and deleted accounts never do.
pub async fn bounded_count(
    conn: &mut PgConnection,
    what: Counted,
    key: i64,
    excluded: &[i64],
    taken_down: bool,
    find: Option<&crate::ui_rows::Find>,
    cap: i64,
) -> Result<i64> {
    let (from, on, col) = match what {
        Counted::IncomingBlocks => ("blocks", "r.author_id", "r.subject_id"),
        Counted::OutgoingBlocks => ("blocks", "r.subject_id", "r.author_id"),
        Counted::ListBlockers => ("list_blocks", "r.author_id", "r.list_id"),
        Counted::ListMembers => ("list_items", "r.subject_id", "r.list_id"),
    };
    Ok(sqlx::query_scalar(&format!(
        "SELECT count(*) FROM (
           SELECT 1 FROM {from} r JOIN actors a ON a.id = {on}
           WHERE {col} = $1 AND a.status NOT IN {} AND NOT (a.id = ANY($2))
             AND ($4::bigint[] IS NULL OR a.id = ANY($4) OR a.did LIKE $5::text
                  OR EXISTS (SELECT 1 FROM handle_cache h
                             WHERE h.did = a.did AND h.handle LIKE $5::text))
           LIMIT $3) x",
        crate::ui_rows::hidden_statuses(true, taken_down)
    ))
    .bind(key)
    .bind(excluded)
    .bind(cap + 1)
    .bind(find.map(|f| &f.ids[..]))
    .bind(find.and_then(|f| f.pattern.as_deref()))
    .fetch_one(conn)
    .await?)
}

/// The recording windows (§7.7), oldest first: (from, to); `to` is `None`
/// for the open one.
pub async fn history_windows(
    conn: &mut PgConnection,
) -> Result<Vec<(DateTime<Utc>, Option<DateTime<Utc>>)>> {
    Ok(
        sqlx::query_as("SELECT from_at, to_at FROM history_windows ORDER BY from_at, id")
            .fetch_all(conn)
            .await?,
    )
}

/// `actors.id` of those of `dids` that are interned (the operator's
/// exclusion list as ids).
pub async fn actor_ids(conn: &mut PgConnection, dids: &[String]) -> Result<Vec<i64>> {
    if dids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(
        sqlx::query_scalar("SELECT id FROM actors WHERE did = ANY($1) ORDER BY id")
            .bind(dids)
            .fetch_all(conn)
            .await?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(horizon: bool, after: bool) -> HistoryArgs<'static> {
        let t = DateTime::<Utc>::UNIX_EPOCH;
        HistoryArgs {
            excluded: &[],
            horizon: horizon.then_some(t),
            after: after.then_some(HistoryCursor {
                removed_at: t,
                id: 1,
            }),
            limit: 50,
        }
    }

    #[test]
    fn keyset_is_a_boundary_only_with_a_cursor() {
        let first = tail(&args(false, false));
        assert!(first.contains("$4::timestamptz IS NULL") && first.contains("$3::timestamptz"));
        let next = tail(&args(true, true));
        assert!(next.contains("(h.removed_at, h.id) < ($4, $5)"));
        assert!(next.contains("h.removed_at >= $3"));
        assert!(next.ends_with("ORDER BY h.removed_at DESC, h.id DESC LIMIT $6"));
    }

    #[test]
    fn cursor_only_after_a_full_page() {
        let row = |id| Removed {
            id,
            removed_at: DateTime::<Utc>::UNIX_EPOCH,
            party: String::new(),
            list: None,
            rkey: String::new(),
            created_at: None,
            first_seen: None,
            last_seen: None,
            cause: 1,
            live: false,
        };
        assert_eq!(next_cursor(&[row(3), row(2)], 3), None);
        assert_eq!(next_cursor(&[row(3), row(2)], 2).map(|c| c.id), Some(2));
    }
}
