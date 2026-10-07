//! Read-only queries behind the public UI and the admin history pages
//! (see `docs/design/history.md` and `docs/design/web-ui.md`): removed
//! blocks, listblocks and list memberships, bounded counts and recording
//! windows. The rows of the live sections are read in [`crate::ui_rows`].
//!
//! Two filters run at query time, so a status change or a settings change
//! shows on the next page view:
//!
//! - **hidden status** (see `docs/design/api.md` and
//!   `docs/design/storage.md`), in every query here: rows whose relevant
//!   account — the author, the list owner or the subject, whichever the
//!   page lists — is `deactivated`, `takendown`, `suspended` or `deleted`
//!   are left out. For `deleted` accounts this filter, not the purge, is
//!   the guarantee.
//! - **operator exclusion**, in the counts of the live sections: rows
//!   whose relevant account is in `excluded` (`actors.id` values of
//!   `public_ui.excluded_dids`) are left out the same way. It governs the
//!   public pages; the history pages are the admin's.
//!
//! History pages order by (`removed_at`, `id`) descending and the cursor
//! carries both: one reconcile gives many rows the same `removed_at`, and
//! `id` tells them apart. Every history query is an index range on a
//! history index, which all end in `id`.
//!
//! All functions take a connection so the caller can run them inside one
//! read transaction with `statement_timeout` set.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::codes::sql::HIDDEN;
use crate::codes::{RecordState, TrackState};
use crate::error::Result;
use crate::history::Cause;
use crate::ids::{ActorId, HistoryId};
use crate::ui_rows::SectionKey;
use farsight_core::{ListPurpose, RecordKey};

/// Position in a history section: the last row returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryCursor {
    /// Its `removed_at`: the first cursor key, newest first.
    pub removed_at: DateTime<Utc>,
    /// Its `id`: the second cursor key, which orders rows removed at the
    /// same instant.
    pub id: HistoryId,
}

/// What every history query takes besides its key.
#[derive(Debug, Clone, Copy)]
pub struct HistoryArgs {
    /// Retention horizon: rows removed before it are not returned, whether
    /// or not the janitor has deleted them yet. `None` = keep forever.
    pub horizon: Option<DateTime<Utc>>,
    /// Keyset position; `None` = first page.
    pub after: Option<HistoryCursor>,
    /// Most rows the query returns.
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
    /// The list's purpose, when the record is present.
    pub purpose: Option<ListPurpose>,
    /// The list's state as `getListMembers` reports it; `None` when no
    /// `lists` row exists.
    pub state: Option<TrackState>,
}

/// One removed record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removed {
    /// History row id (second cursor key).
    pub id: HistoryId,
    /// When Farsight applied the removal (first cursor key).
    pub removed_at: DateTime<Utc>,
    /// The other account of the row: the blocker, or the member on a
    /// list's page. Empty for rows identified by `list` alone.
    pub party: String,
    /// The list, for rows that point at one from an account's page.
    pub list: Option<RemovedList>,
    /// Record key of the removed record.
    pub rkey: String,
    /// Author-claimed `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
    /// When Farsight first stored the record.
    pub first_seen: Option<DateTime<Utc>>,
    /// When Farsight last saw the record.
    pub last_seen: Option<DateTime<Utc>>,
    /// Why the record was removed.
    pub cause: Cause,
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
/// keyset. `$2` is the horizon, `$3`/`$4` the cursor, `$5` the limit. The
/// fragments are chosen by which arguments are present so that the keyset
/// is always an index boundary, never a filter.
fn tail(a: &HistoryArgs) -> String {
    let horizon = if a.horizon.is_some() {
        "AND h.removed_at >= $2"
    } else {
        "AND $2::timestamptz IS NULL"
    };
    let keyset = if a.after.is_some() {
        "AND (h.removed_at, h.id) < ($3, $4)"
    } else {
        "AND $3::timestamptz IS NULL AND $4::bigint IS NULL"
    };
    format!("{horizon} {keyset} ORDER BY h.removed_at DESC, h.id DESC LIMIT $5")
}

type PartyRow = (
    HistoryId,
    DateTime<Utc>,
    String,
    String,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Cause,
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
/// blocker is hidden are left out. `live`: the blocker has a
/// block on the subject now.
pub async fn blocks_history_by_subject(
    conn: &mut PgConnection,
    subject_id: ActorId,
    a: HistoryArgs,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, p.did, h.rkey, h.created_at, h.first_seen, h.last_seen,
                h.cause,
                EXISTS (SELECT 1 FROM blocks b
                        WHERE b.subject_id = h.subject_id AND b.author_id = h.author_id)
         FROM blocks_history h JOIN actors p ON p.id = h.author_id
         WHERE h.subject_id = $1 AND p.status NOT IN {HIDDEN} {}",
        tail(&a)
    );
    fetch_party(conn, &sql, subject_id, a).await
}

async fn fetch_party(
    conn: &mut PgConnection,
    sql: &str,
    key: ActorId,
    a: HistoryArgs,
) -> Result<Vec<Removed>> {
    let rows: Vec<PartyRow> = sqlx::query_as(sql)
        .bind(key)
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
/// listblocker; rows whose listblocker is hidden are left out
/// (the caller withholds the whole page when the owner is). `live`: the
/// listblocker has a listblock on the list now. Needs no `lists` row.
pub async fn list_blocks_history_by_list(
    conn: &mut PgConnection,
    owner_id: ActorId,
    list_rkey: &RecordKey,
    a: HistoryArgs,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, p.did, h.rkey, h.created_at, h.first_seen, h.last_seen,
                h.cause,
                EXISTS (SELECT 1 FROM lists l JOIN list_blocks b ON b.list_id = l.id
                        WHERE l.owner_id = h.list_owner_id AND l.rkey = h.list_rkey
                          AND b.author_id = h.author_id)
         FROM list_blocks_history h JOIN actors p ON p.id = h.author_id
         WHERE h.list_owner_id = $1 AND h.list_rkey = $6
           AND p.status NOT IN {HIDDEN} {}",
        tail(&a)
    );
    let rows: Vec<PartyRow> = sqlx::query_as(&sql)
        .bind(owner_id)
        .bind(a.horizon)
        .bind(a.after.map(|c| c.removed_at))
        .bind(a.after.map(|c| c.id))
        .bind(a.limit)
        .bind(list_rkey.as_str())
        .fetch_all(conn)
        .await?;
    Ok(party_rows(rows))
}

type ListRow = (
    HistoryId,
    DateTime<Utc>,
    String,
    String,
    String,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Cause,
    bool,
    Option<String>,
    Option<i16>,
    Option<RecordState>,
    Option<i16>,
    Option<TrackState>,
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
            // else `untracked`, as `getListMembers` reports it.
            let state = r.13.and_then(TrackState::from_code).map(|s| match s {
                TrackState::Purging => {
                    if r.14 == Some(TrackState::Untracked) && r.15.unwrap_or(0) > 0 {
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
                    // A deleted list has no purpose.
                    purpose: if r.12 == Some(RecordState::Present) {
                        r.11.map(ListPurpose::from_code)
                    } else {
                        None
                    },
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

/// Removed list memberships naming `subject_id`, newest removal first
/// (`list_items_history_by_subject`). Each row names the list; rows whose
/// list owner is hidden are left out. `lists` is outer-joined.
/// `live`: the list has a live listitem naming the subject now.
pub async fn list_items_history_by_subject(
    conn: &mut PgConnection,
    subject_id: ActorId,
    a: HistoryArgs,
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
         WHERE h.subject_id = $1 AND o.status NOT IN {HIDDEN} {}",
        tail(&a)
    );
    fetch_list(conn, &sql, subject_id, a).await
}

async fn fetch_list(
    conn: &mut PgConnection,
    sql: &str,
    key: ActorId,
    a: HistoryArgs,
) -> Result<Vec<Removed>> {
    let rows: Vec<ListRow> = sqlx::query_as(sql)
        .bind(key)
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
/// rows whose member is hidden are left out. `live`: the
/// member has a live listitem on the list now. Needs no `lists` row.
pub async fn list_items_history_by_list(
    conn: &mut PgConnection,
    owner_id: ActorId,
    list_rkey: &RecordKey,
    a: HistoryArgs,
) -> Result<Vec<Removed>> {
    let sql = format!(
        "SELECT h.id, h.removed_at, p.did, h.rkey, h.created_at, h.first_seen, h.last_seen,
                h.cause,
                EXISTS (SELECT 1 FROM lists l JOIN list_items li ON li.list_id = l.id
                        WHERE l.owner_id = h.owner_id AND l.rkey = h.list_rkey
                          AND li.subject_id = h.subject_id)
         FROM list_items_history h JOIN actors p ON p.id = h.subject_id
         WHERE h.owner_id = $1 AND h.list_rkey = $6
           AND p.status NOT IN {HIDDEN} {}",
        tail(&a)
    );
    let rows: Vec<PartyRow> = sqlx::query_as(&sql)
        .bind(owner_id)
        .bind(a.horizon)
        .bind(a.after.map(|c| c.removed_at))
        .bind(a.after.map(|c| c.id))
        .bind(a.limit)
        .bind(list_rkey.as_str())
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
    key: SectionKey,
    excluded: &[ActorId],
    taken_down: bool,
    find: Option<&crate::ui_rows::Find>,
    cap: i64,
) -> Result<i64> {
    let hidden = crate::ui_rows::hidden_statuses(true, taken_down);
    bounded_count_hiding(conn, what, key, excluded, hidden, find, cap).await
}

/// [`bounded_count`] with the statuses left out given as a SQL list, as
/// [`crate::ui_rows::hidden_statuses`] writes it. The admin tables leave
/// out the API's hidden set, which is not the public tables' rule.
pub async fn bounded_count_hiding(
    conn: &mut PgConnection,
    what: Counted,
    key: SectionKey,
    excluded: &[ActorId],
    hidden: &'static str,
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
           WHERE {col} = $1 AND a.status NOT IN {hidden} AND NOT (a.id = ANY($2))
             AND ($4::bigint[] IS NULL OR a.id = ANY($4) OR a.did LIKE $5::text
                  OR EXISTS (SELECT 1 FROM handle_cache h
                             WHERE h.did = a.did AND h.handle LIKE $5::text))
           LIMIT $3) x"
    ))
    .bind(key.get())
    .bind(excluded)
    .bind(cap + 1)
    .bind(find.map(|f| &f.ids[..]))
    .bind(find.and_then(|f| f.pattern.as_deref()))
    .fetch_one(conn)
    .await?)
}

/// The recording windows, oldest first: (from, to); `to` is `None` for
/// the open one.
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
pub async fn actor_ids(conn: &mut PgConnection, dids: &[String]) -> Result<Vec<ActorId>> {
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

    fn args(horizon: bool, after: bool) -> HistoryArgs {
        let t = DateTime::<Utc>::UNIX_EPOCH;
        HistoryArgs {
            horizon: horizon.then_some(t),
            after: after.then_some(HistoryCursor {
                removed_at: t,
                id: HistoryId::new(1),
            }),
            limit: 50,
        }
    }

    #[test]
    fn keyset_is_a_boundary_only_with_a_cursor() {
        let first = tail(&args(false, false));
        assert!(first.contains("$3::timestamptz IS NULL") && first.contains("$2::timestamptz"));
        let next = tail(&args(true, true));
        assert!(next.contains("(h.removed_at, h.id) < ($3, $4)"));
        assert!(next.contains("h.removed_at >= $2"));
        assert!(next.ends_with("ORDER BY h.removed_at DESC, h.id DESC LIMIT $5"));
    }

    #[test]
    fn cursor_only_after_a_full_page() {
        let row = |id| Removed {
            id: HistoryId::new(id),
            removed_at: DateTime::<Utc>::UNIX_EPOCH,
            party: String::new(),
            list: None,
            rkey: String::new(),
            created_at: None,
            first_seen: None,
            last_seen: None,
            cause: Cause::Delete,
            live: false,
        };
        assert_eq!(next_cursor(&[row(3), row(2)], 3), None);
        assert_eq!(
            next_cursor(&[row(3), row(2)], 2).map(|c| c.id.get()),
            Some(2)
        );
    }
}
