//! Rows of the four UI sections that show a creation time (see
//! `docs/design/web-ui.md` and `docs/design/storage.md`): incoming
//! blocks, outgoing blocks, the listblocks on a list and a list's
//! members, on the public and the admin pages alike.
//!
//! Each section reads its rows here, in one of two orders:
//!
//! - [`Order::Shown`]: newest first by the row's **shown time**,
//!   [`SHOWN_TIME`] — the stated `createdAt`, but never later than the
//!   moment Farsight first stored the record. `created_at` is the author's
//!   claim; `first_seen` is on the witness clock. A record dated in the
//!   future therefore sorts where it arrived, not at the top. The order
//!   needs the section's index ([`Section::index`]).
//! - [`Order::Stored`]: the order the section had before those indexes
//!   existed, on the indexes the API uses. A section keeps it until its
//!   index is valid ([`SortIndexes`]).
//!
//! Columns and filters are the same in both orders. The API's queries
//! ([`crate::queries`]) are not touched: their order is part of the stable
//! contract.
//!
//! A fifth section, the lists naming an account ([`lists_naming`]), sorts
//! by the same shown time — of the listitem naming the account — without
//! an index of its own: it reads the account's listitems on
//! `list_items_by_subject` and sorts them, so it has one order and no flag.
//!
//! The indexes are not created by a migration: a build inside the migration
//! transaction would outlast a health check on a large table and be rolled
//! back by the restart. The server builds them after it starts serving;
//! the pieces it needs are here ([`index_state`], [`estimate_bytes`],
//! [`create_index`], [`drop_index`]).

use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use crate::error::Result;

/// The shown time of a row, exactly as the indexes spell it. `LEAST`
/// ignores a NULL argument; a row with neither time sorts last.
pub const SHOWN_TIME: &str = "COALESCE(LEAST(created_at, first_seen), '-infinity'::timestamptz)";

/// The same on the row alias the queries use.
const SHOWN_B: &str = "COALESCE(LEAST(b.created_at, b.first_seen), '-infinity'::timestamptz)";

/// SQL fragment: status codes hidden by default.
const HIDDEN: &str = "(1, 2, 3, 4)";

/// The statuses a table leaves out, as an SQL list: deactivated (1) and
/// deleted (4) always; taken down (2) unless `takendown`; suspended (3)
/// unless `suspended`. With neither, the API's hidden set.
pub fn hidden_statuses(suspended: bool, taken_down: bool) -> &'static str {
    match (suspended, taken_down) {
        (false, false) => HIDDEN,
        (true, false) => "(1, 2, 4)",
        (false, true) => "(1, 3, 4)",
        (true, true) => "(1, 4)",
    }
}

/// Bytes one index entry is estimated at before a build.
pub const ENTRY_BYTES: u64 = 62;

/// Advisory lock key of the index build: one server process builds at a
/// time.
pub const BUILD_LOCK: i64 = 0x4653_5549_5352_5431;

/// A section whose rows sort by their shown time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    /// Blocks naming an account, by blocker.
    IncomingBlocks,
    /// Blocks an account has made.
    OutgoingBlocks,
    /// Listblocks on a list, by listblocker.
    ListBlockers,
    /// Items of a list.
    ListMembers,
}

impl Section {
    /// Every section, in the order the indexes are built: the small
    /// tables first.
    pub const ALL: [Section; 4] = [
        Section::ListBlockers,
        Section::ListMembers,
        Section::IncomingBlocks,
        Section::OutgoingBlocks,
    ];

    fn slot(self) -> usize {
        match self {
            Section::ListBlockers => 0,
            Section::ListMembers => 1,
            Section::IncomingBlocks => 2,
            Section::OutgoingBlocks => 3,
        }
    }

    /// The section's table.
    pub fn table(self) -> &'static str {
        match self {
            Section::IncomingBlocks | Section::OutgoingBlocks => "blocks",
            Section::ListBlockers => "list_blocks",
            Section::ListMembers => "list_items",
        }
    }

    /// The index its shown-time order needs.
    pub fn index(self) -> &'static str {
        match self {
            Section::IncomingBlocks => "blocks_by_subject_created",
            Section::OutgoingBlocks => "blocks_by_author_created",
            Section::ListBlockers => "list_blocks_by_list_created",
            Section::ListMembers => "list_items_by_list_created",
        }
    }

    /// The column the section is keyed by.
    fn key(self) -> &'static str {
        match self {
            Section::IncomingBlocks => "subject_id",
            Section::OutgoingBlocks => "author_id",
            Section::ListBlockers | Section::ListMembers => "list_id",
        }
    }

    /// The column naming the account a row lists.
    fn party(self) -> &'static str {
        match self {
            Section::IncomingBlocks | Section::ListBlockers => "author_id",
            Section::OutgoingBlocks | Section::ListMembers => "subject_id",
        }
    }

    /// Whether the shown-time order breaks ties by the listed account
    /// before the record key: the sections that list several authors.
    /// The other two list one author's or one list's records, whose
    /// record keys are unique within the section.
    pub fn ties_by_party(self) -> bool {
        matches!(self, Section::IncomingBlocks | Section::ListBlockers)
    }

    /// The columns of the index after the key.
    fn index_tail(self) -> &'static str {
        if self.ties_by_party() {
            "author_id DESC, rkey DESC"
        } else {
            "rkey DESC"
        }
    }

    /// `CREATE INDEX CONCURRENTLY …` for the section's index.
    pub fn create_index_sql(self) -> String {
        format!(
            "CREATE INDEX CONCURRENTLY {} ON {} ({}, ({SHOWN_TIME}) DESC, {})",
            self.index(),
            self.table(),
            self.key(),
            self.index_tail()
        )
    }
}

/// One flag per section: its index is valid and its rows sort by shown
/// time. Set at start and after each build; a section has one order at any
/// moment.
#[derive(Debug, Default)]
pub struct SortIndexes {
    ready: [AtomicBool; 4],
}

impl SortIndexes {
    /// Whether `section` sorts by shown time.
    pub fn ready(&self, section: Section) -> bool {
        self.ready[section.slot()].load(Ordering::Relaxed)
    }

    /// Records the state of `section`'s index.
    pub fn set(&self, section: Section, ready: bool) {
        self.ready[section.slot()].store(ready, Ordering::Relaxed);
    }

    /// How many of the four are ready.
    pub fn count(&self) -> usize {
        Section::ALL.iter().filter(|s| self.ready(**s)).count()
    }

    /// The order `section` uses now.
    pub fn order(&self, section: Section) -> Order {
        if self.ready(section) {
            Order::Shown
        } else {
            Order::Stored
        }
    }
}

/// What a section's rows are ordered by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// The order before the sort indexes: the listed account's
    /// `actors.id` then the record key, ascending (outgoing blocks: the
    /// record key alone).
    Stored,
    /// Shown time, newest first; ties by the rest of the primary key,
    /// descending.
    Shown,
}

/// One row of a section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// `actors.id` of the account the row lists.
    pub party_id: i64,
    /// Its DID.
    pub did: String,
    /// Record key of the block, listblock or listitem.
    pub rkey: String,
    /// Author-claimed `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
    /// When Farsight first stored the record; `None`: before it kept the
    /// date.
    pub first_seen: Option<DateTime<Utc>>,
}

impl Row {
    /// The row's shown time; `None` when it has neither time and sorts
    /// last.
    pub fn shown_time(&self) -> Option<DateTime<Utc>> {
        match (self.created_at, self.first_seen) {
            (Some(c), Some(f)) => Some(c.min(f)),
            (c, f) => c.or(f),
        }
    }

    /// The position after this row.
    pub fn position(&self) -> Position {
        Position {
            time: self.shown_time(),
            party_id: self.party_id,
            rkey: self.rkey.clone(),
        }
    }
}

/// A keyset position: the last row of the page before. Which fields count
/// depends on the section and the order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    /// Its shown time ([`Order::Shown`] only); `None` = "last".
    pub time: Option<DateTime<Utc>>,
    /// The listed account's `actors.id` (unused where the section orders
    /// by record key alone).
    pub party_id: i64,
    /// Its record key.
    pub rkey: String,
}

/// What a visitor typed into a table's filter box, as the queries use
/// it: the rows whose account is one of `ids`, or whose DID or stored
/// handle matches `pattern`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Find {
    /// `actors.id` of the accounts named outright (a DID, or a handle
    /// that was resolved).
    pub ids: Vec<i64>,
    /// A `LIKE` pattern, lower-case, its wildcards already escaped: it is
    /// matched against DIDs and against the handles in `handle_cache`, so
    /// an account whose handle was never verified is found by its DID
    /// only.
    pub pattern: Option<String>,
}

impl Find {
    /// The `LIKE` pattern for "contains `text`": lower-cased, with `%`,
    /// `_` and `\` taken literally.
    pub fn containing(text: &str) -> String {
        let mut p = String::with_capacity(text.len() + 2);
        p.push('%');
        for c in text.to_lowercase().chars() {
            if matches!(c, '%' | '_' | '\\') {
                p.push('\\');
            }
            p.push(c);
        }
        p.push('%');
        p
    }
}

/// Which rows a page leaves out, in the query.
#[derive(Debug, Clone, Copy, Default)]
pub struct Filter<'a> {
    /// Rows whose listed account has a hidden status.
    pub hide_inactive: bool,
    /// With `hide_inactive`: keep suspended accounts (a suspension is
    /// temporary; the public tables show them).
    pub show_suspended: bool,
    /// With `hide_inactive`: keep the accounts a host has taken down
    /// ("taken down"). Deactivated and deleted accounts always stay out.
    pub show_taken_down: bool,
    /// Rows whose listed account is one of these `actors.id` values
    /// (`public_ui.excluded_dids`).
    pub excluded: &'a [i64],
    /// Only the rows the filter box asks for.
    pub find: Option<&'a Find>,
}

/// Whether `section` in `order` compares the listed account in its keyset.
fn keyed_by_party(section: Section, order: Order) -> bool {
    match order {
        Order::Stored => section != Section::OutgoingBlocks,
        Order::Shown => section.ties_by_party(),
    }
}

fn build<'a>(
    prefix: &str,
    section: Section,
    key: i64,
    order: Order,
    filter: Filter<'a>,
    after: Option<&'a Position>,
    limit: i64,
) -> QueryBuilder<'a, Postgres> {
    let party = section.party();
    let mut q = QueryBuilder::new(format!(
        "{prefix}SELECT b.{party}, a.did, b.rkey, b.created_at, b.first_seen \
         FROM {} b JOIN actors a ON a.id = b.{party} WHERE b.{} = ",
        section.table(),
        section.key()
    ));
    q.push_bind(key);
    if filter.hide_inactive {
        q.push(format!(
            " AND a.status NOT IN {}",
            hidden_statuses(filter.show_suspended, filter.show_taken_down)
        ));
    }
    if !filter.excluded.is_empty() {
        q.push(" AND NOT (a.id = ANY(");
        q.push_bind(filter.excluded);
        q.push("))");
    }
    if let Some(f) = filter.find {
        q.push(" AND (a.id = ANY(");
        q.push_bind(&f.ids[..]);
        q.push(")");
        if let Some(p) = &f.pattern {
            q.push(" OR a.did LIKE ");
            q.push_bind(p.as_str());
            q.push(
                " OR EXISTS (SELECT 1 FROM handle_cache h WHERE h.did = a.did AND h.handle LIKE ",
            );
            q.push_bind(p.as_str());
            q.push(")");
        }
        q.push(")");
    }
    let by_party = keyed_by_party(section, order);
    // The keyset is a row comparison on the index's own columns, so it is
    // an index boundary, never a filter.
    if let Some(p) = after {
        match order {
            Order::Stored => {
                q.push(if by_party {
                    format!(" AND (b.{party}, b.rkey) > (")
                } else {
                    " AND (b.rkey) > (".to_owned()
                });
            }
            Order::Shown => {
                q.push(if by_party {
                    format!(" AND ({SHOWN_B}, b.{party}, b.rkey) < (COALESCE(")
                } else {
                    format!(" AND ({SHOWN_B}, b.rkey) < (COALESCE(")
                });
                q.push_bind(p.time);
                q.push("::timestamptz, '-infinity'::timestamptz), ");
            }
        }
        if by_party {
            q.push_bind(p.party_id);
            q.push(", ");
        }
        q.push_bind(p.rkey.as_str());
        q.push(")");
    }
    q.push(match (order, by_party) {
        (Order::Stored, true) => format!(" ORDER BY b.{party}, b.rkey"),
        (Order::Stored, false) => " ORDER BY b.rkey".to_owned(),
        (Order::Shown, true) => format!(" ORDER BY {SHOWN_B} DESC, b.{party} DESC, b.rkey DESC"),
        (Order::Shown, false) => format!(" ORDER BY {SHOWN_B} DESC, b.rkey DESC"),
    });
    q.push(" LIMIT ");
    q.push_bind(limit);
    q
}

type Raw = (
    i64,
    String,
    String,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

/// One page of `section` for `key` (the subject, the author or the list),
/// in `order`, after `after`.
pub async fn rows(
    conn: &mut PgConnection,
    section: Section,
    key: i64,
    order: Order,
    filter: Filter<'_>,
    after: Option<&Position>,
    limit: i64,
) -> Result<Vec<Row>> {
    let mut q = build("", section, key, order, filter, after, limit);
    let rows: Vec<Raw> = q.build_query_as().fetch_all(conn).await?;
    Ok(rows
        .into_iter()
        .map(|(party_id, did, rkey, created_at, first_seen)| Row {
            party_id,
            did,
            rkey,
            created_at,
            first_seen,
        })
        .collect())
}

/// `limit` rows of `section` for `key` in `order`, skipping the first
/// `offset`: a numbered page of the public tables. The cost grows with
/// the offset; the order is total, so pages do not overlap.
pub async fn rows_at(
    conn: &mut PgConnection,
    section: Section,
    key: i64,
    order: Order,
    filter: Filter<'_>,
    offset: i64,
    limit: i64,
) -> Result<Vec<Row>> {
    let mut q = build("", section, key, order, filter, None, limit);
    q.push(" OFFSET ");
    q.push_bind(offset);
    let rows: Vec<Raw> = q.build_query_as().fetch_all(conn).await?;
    Ok(rows
        .into_iter()
        .map(|(party_id, did, rkey, created_at, first_seen)| Row {
            party_id,
            did,
            rkey,
            created_at,
            first_seen,
        })
        .collect())
}

/// The shown time of the listitem a list names the account with.
const SHOWN_X: &str = "COALESCE(LEAST(x.created_at, x.first_seen), '-infinity'::timestamptz)";

/// One list naming an account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamingRow {
    /// `lists.id`.
    pub list_id: i64,
    /// The list owner's DID.
    pub owner_did: String,
    /// The list's record key.
    pub rkey: String,
    /// Its name, if the record has one.
    pub name: Option<String>,
    /// Counted listblocks on it.
    pub listblock_count: i32,
    /// Its `purpose` code, if the record states one Farsight knows.
    pub purpose: Option<i16>,
    /// Owner-claimed `createdAt` of the listitem naming the account (the
    /// one with the lowest record key if it is named twice).
    pub added_at: Option<DateTime<Utc>>,
    /// When Farsight first stored that listitem.
    pub first_seen: Option<DateTime<Utc>>,
}

impl NamingRow {
    /// The row's shown time; `None` when it has neither time and sorts
    /// last.
    pub fn shown_time(&self) -> Option<DateTime<Utc>> {
        match (self.added_at, self.first_seen) {
            (Some(c), Some(f)) => Some(c.min(f)),
            (c, f) => c.or(f),
        }
    }
}

/// Which lists name `$1` and may be shown; `$2` is the excluded owners.
/// `ids` and `pattern` are the numbers of the two parameters of the
/// filter box: a `bigint[]` of owners named outright (NULL: no filter)
/// and a pattern for the list's name and its owner's DID or stored
/// handle (NULL: none).
fn naming_from(ids: u8, pattern: u8) -> String {
    format!(
        "JOIN lists l ON l.id = x.list_id
         JOIN actors o ON o.id = l.owner_id
         WHERE l.track_state IN (2, 3) AND l.record_state = 1
           AND o.status NOT IN {} AND NOT (o.id = ANY($2))
           AND (${ids}::bigint[] IS NULL OR o.id = ANY(${ids})
                OR lower(l.name) LIKE ${pattern}::text OR o.did LIKE ${pattern}::text
                OR EXISTS (SELECT 1 FROM handle_cache h
                           WHERE h.did = o.did AND h.handle LIKE ${pattern}::text))",
        hidden_statuses(true, false)
    )
}

fn naming_sql() -> String {
    format!(
        "SELECT l.id, o.did, l.rkey, l.name, l.listblock_count, x.created_at, x.first_seen,
                l.purpose
         FROM (SELECT DISTINCT ON (li.list_id) li.list_id, li.created_at, li.first_seen
               FROM list_items li WHERE li.subject_id = $1
               ORDER BY li.list_id, li.rkey) x
         {}
         ORDER BY {SHOWN_X} DESC, l.id DESC LIMIT $3 OFFSET $4",
        naming_from(5, 6)
    )
}

/// Ready or retained lists with a present record naming `subject_id`, once
/// per list, newest first by the shown time of the listitem, then by list
/// id descending. Lists of hidden owners and of `excluded` owners are left
/// out. `limit` rows, skipping the first `offset`.
///
/// The same lists as `queries::lists_naming`, whose order (by list id) is
/// part of the stable API and is not touched.
pub async fn lists_naming(
    conn: &mut PgConnection,
    subject_id: i64,
    excluded: &[i64],
    find: Option<&Find>,
    offset: i64,
    limit: i64,
) -> Result<Vec<NamingRow>> {
    type Raw = (
        i64,
        String,
        String,
        Option<String>,
        i32,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Option<i16>,
    );
    let rows: Vec<Raw> = sqlx::query_as(&naming_sql())
        .bind(subject_id)
        .bind(excluded)
        .bind(limit)
        .bind(offset)
        .bind(find.map(|f| &f.ids[..]))
        .bind(find.and_then(|f| f.pattern.as_deref()))
        .fetch_all(conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(
            |(list_id, owner_did, rkey, name, listblock_count, added_at, first_seen, purpose)| {
                NamingRow {
                    list_id,
                    owner_did,
                    rkey,
                    name,
                    listblock_count,
                    purpose,
                    added_at,
                    first_seen,
                }
            },
        )
        .collect())
}

/// How many lists [`lists_naming`] would return for `subject_id` in all,
/// counted up to `cap + 1`: a result above `cap` means "more than `cap`".
pub async fn lists_naming_count(
    conn: &mut PgConnection,
    subject_id: i64,
    excluded: &[i64],
    find: Option<&Find>,
    cap: i64,
) -> Result<i64> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT count(*) FROM (
           SELECT 1 FROM (SELECT DISTINCT li.list_id FROM list_items li
                          WHERE li.subject_id = $1) x
           {}
           LIMIT $3) y",
        naming_from(4, 5)
    ))
    .bind(subject_id)
    .bind(excluded)
    .bind(cap + 1)
    .bind(find.map(|f| &f.ids[..]))
    .bind(find.and_then(|f| f.pattern.as_deref()))
    .fetch_one(conn)
    .await?)
}

/// A list an account subscribes to as a block list: one of the account's
/// own listblock records and the list it points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedList {
    /// `lists.id`.
    pub list_id: i64,
    /// The list owner's DID.
    pub owner_did: String,
    /// The list's record key.
    pub rkey: String,
    /// Its name, if Farsight holds its record and the record has one.
    pub name: Option<String>,
    /// Its `purpose` code, likewise.
    pub purpose: Option<i16>,
    /// The record key of the account's listblock.
    pub block_rkey: String,
    /// The listblock's stated `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
    /// When Farsight first stored the listblock.
    pub first_seen: Option<DateTime<Utc>>,
}

/// The `WHERE` of [`lists_blocked`] and its count. `$1` the author, `$2`
/// excluded owners, `${all}` whether lists Farsight does not serve are
/// included, then the filter's ids and pattern.
fn blocked_where(all: u8, ids: u8, pattern: u8) -> String {
    format!(
        "WHERE b.author_id = $1 AND NOT (o.id = ANY($2))
           AND (${all} OR (l.track_state IN (2, 3) AND l.record_state = 1
                          AND o.status NOT IN {}))
           AND (${ids}::bigint[] IS NULL OR o.id = ANY(${ids})
                OR lower(l.name) LIKE ${pattern}::text OR o.did LIKE ${pattern}::text
                OR EXISTS (SELECT 1 FROM handle_cache h
                           WHERE h.did = o.did AND h.handle LIKE ${pattern}::text))",
        hidden_statuses(true, false)
    )
}

/// The lists `author_id` subscribes to as block lists, newest listblock
/// first, `limit` from `offset`. An account's listblocks are few, so the
/// order needs no index of its own. Without `all`, only the lists the
/// public pages would show: indexed, their record present, their owner
/// neither hidden nor excluded. With `all` (the admin lookup), every
/// listblock the account has, whatever Farsight knows of the list.
pub async fn lists_blocked(
    conn: &mut PgConnection,
    author_id: i64,
    excluded: &[i64],
    find: Option<&Find>,
    all: bool,
    offset: i64,
    limit: i64,
) -> Result<Vec<BlockedList>> {
    type Raw = (
        i64,
        String,
        String,
        Option<String>,
        Option<i16>,
        String,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
    );
    let rows: Vec<Raw> = sqlx::query_as(&format!(
        "SELECT l.id, o.did, l.rkey, l.name, l.purpose, b.rkey, b.created_at, b.first_seen
         FROM list_blocks b
         JOIN lists l ON l.id = b.list_id
         JOIN actors o ON o.id = l.owner_id
         {}
         ORDER BY {SHOWN_B} DESC, b.rkey DESC LIMIT $3 OFFSET $4",
        blocked_where(5, 6, 7)
    ))
    .bind(author_id)
    .bind(excluded)
    .bind(limit)
    .bind(offset)
    .bind(all)
    .bind(find.map(|f| &f.ids[..]))
    .bind(find.and_then(|f| f.pattern.as_deref()))
    .fetch_all(conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(list_id, owner_did, rkey, name, purpose, block_rkey, created_at, first_seen)| {
                BlockedList {
                    list_id,
                    owner_did,
                    rkey,
                    name,
                    purpose,
                    block_rkey,
                    created_at,
                    first_seen,
                }
            },
        )
        .collect())
}

/// How many rows [`lists_blocked`] pages over, counted up to `cap + 1`.
pub async fn lists_blocked_count(
    conn: &mut PgConnection,
    author_id: i64,
    excluded: &[i64],
    find: Option<&Find>,
    all: bool,
    cap: i64,
) -> Result<i64> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT count(*) FROM (
           SELECT 1 FROM list_blocks b
           JOIN lists l ON l.id = b.list_id
           JOIN actors o ON o.id = l.owner_id
           {}
           LIMIT $3) y",
        blocked_where(4, 5, 6)
    ))
    .bind(author_id)
    .bind(excluded)
    .bind(cap + 1)
    .bind(all)
    .bind(find.map(|f| &f.ids[..]))
    .bind(find.and_then(|f| f.pattern.as_deref()))
    .fetch_one(conn)
    .await?)
}

/// The listblock counters of the lists [`lists_naming`] would return for
/// `subject_id`, added up: how many listblock records there are on the
/// lists that name the account. An account that blocks two of the lists
/// counts twice.
pub async fn lists_naming_listblocks(
    conn: &mut PgConnection,
    subject_id: i64,
    excluded: &[i64],
) -> Result<i64> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT COALESCE(sum(l.listblock_count), 0)::bigint
         FROM (SELECT DISTINCT li.list_id FROM list_items li
               WHERE li.subject_id = $1) x
         {}",
        naming_from(3, 4)
    ))
    .bind(subject_id)
    .bind(excluded)
    .bind(None::<&[i64]>)
    .bind(None::<&str>)
    .fetch_one(conn)
    .await?)
}

/// The plan of the query [`rows`] runs, one line per plan node
/// (`EXPLAIN (ANALYZE, COSTS OFF)`). The harness checks that each
/// section's shown-time order runs on its index.
#[cfg(feature = "harness")]
pub async fn explain(
    conn: &mut PgConnection,
    section: Section,
    key: i64,
    order: Order,
    filter: Filter<'_>,
    after: Option<&Position>,
    limit: i64,
) -> Result<Vec<String>> {
    let mut q = build(
        "EXPLAIN (ANALYZE, COSTS OFF) ",
        section,
        key,
        order,
        filter,
        after,
        limit,
    );
    Ok(q.build_query_scalar().fetch_all(conn).await?)
}

/// What a section's index is, as `pg_index` has it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// Built and usable.
    Valid,
    /// Present but not valid: a build that was interrupted.
    Invalid,
    /// Not there.
    Absent,
}

/// The state of `section`'s index.
pub async fn index_state(conn: &mut PgConnection, section: Section) -> Result<IndexState> {
    let valid: Option<bool> = sqlx::query_scalar(
        "SELECT i.indisvalid FROM pg_index i
         WHERE i.indexrelid = to_regclass($1) AND i.indrelid = to_regclass($2)",
    )
    .bind(section.index())
    .bind(section.table())
    .fetch_optional(conn)
    .await?;
    Ok(match valid {
        Some(true) => IndexState::Valid,
        Some(false) => IndexState::Invalid,
        None => IndexState::Absent,
    })
}

/// Sets every flag of `sort` from the database.
pub async fn load_states(conn: &mut PgConnection, sort: &SortIndexes) -> Result<()> {
    for s in Section::ALL {
        sort.set(s, index_state(&mut *conn, s).await? == IndexState::Valid);
    }
    Ok(())
}

/// The estimated size of `section`'s index before it is built:
/// `reltuples` × [`ENTRY_BYTES`]. A table never analysed reports a
/// negative `reltuples`, which counts as 0.
pub async fn estimate_bytes(conn: &mut PgConnection, section: Section) -> Result<u64> {
    let tuples: f32 =
        sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE oid = to_regclass($1)")
            .bind(section.table())
            .fetch_one(conn)
            .await?;
    Ok(estimate_from_tuples(tuples))
}

fn estimate_from_tuples(tuples: f32) -> u64 {
    if tuples.is_finite() && tuples > 0.0 {
        (f64::from(tuples) * ENTRY_BYTES as f64) as u64
    } else {
        0
    }
}

/// `pg_database_size` of the connected database.
pub async fn database_bytes(conn: &mut PgConnection) -> Result<u64> {
    let n: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(conn)
        .await?;
    Ok(n.max(0) as u64)
}

/// The size of `section`'s index; 0 when it does not exist.
pub async fn index_bytes(conn: &mut PgConnection, section: Section) -> Result<u64> {
    let n: Option<i64> = sqlx::query_scalar("SELECT pg_relation_size(to_regclass($1))")
        .bind(section.index())
        .fetch_one(conn)
        .await?;
    Ok(n.unwrap_or(0).max(0) as u64)
}

/// Builds `section`'s index without blocking writers. Must run outside a
/// transaction, on a connection with no statement timeout.
pub async fn create_index(conn: &mut PgConnection, section: Section) -> Result<()> {
    sqlx::query(&section.create_index_sql())
        .execute(conn)
        .await?;
    Ok(())
}

/// Drops `section`'s index (an interrupted build leaves an invalid one).
/// Must run outside a transaction.
pub async fn drop_index(conn: &mut PgConnection, section: Section) -> Result<()> {
    sqlx::query(&format!(
        "DROP INDEX CONCURRENTLY IF EXISTS {}",
        section.index()
    ))
    .execute(conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sql(section: Section, order: Order, cursor: bool, filter: Filter<'_>) -> String {
        let p = Position {
            time: None,
            party_id: 1,
            rkey: "k".into(),
        };
        build("", section, 1, order, filter, cursor.then_some(&p), 50)
            .sql()
            .to_owned()
    }

    #[test]
    fn index_statements_are_the_documented_ones() {
        assert_eq!(
            Section::IncomingBlocks.create_index_sql(),
            "CREATE INDEX CONCURRENTLY blocks_by_subject_created ON blocks (subject_id, \
             (COALESCE(LEAST(created_at, first_seen), '-infinity'::timestamptz)) DESC, \
             author_id DESC, rkey DESC)"
        );
        assert_eq!(
            Section::OutgoingBlocks.create_index_sql(),
            "CREATE INDEX CONCURRENTLY blocks_by_author_created ON blocks (author_id, \
             (COALESCE(LEAST(created_at, first_seen), '-infinity'::timestamptz)) DESC, rkey DESC)"
        );
        assert!(
            Section::ListBlockers
                .create_index_sql()
                .contains("list_blocks_by_list_created ON list_blocks (list_id, ")
        );
        assert!(
            Section::ListMembers
                .create_index_sql()
                .ends_with("ON list_items (list_id, (COALESCE(LEAST(created_at, first_seen), '-infinity'::timestamptz)) DESC, rkey DESC)")
        );
        // Small tables first.
        assert_eq!(
            Section::ALL.map(Section::table),
            ["list_blocks", "list_items", "blocks", "blocks"]
        );
    }

    #[test]
    fn shown_order_never_sorts_by_the_stated_time_alone() {
        for s in Section::ALL {
            let first = sql(s, Order::Shown, false, Filter::default());
            assert!(
                first.contains(&format!("ORDER BY {SHOWN_B} DESC")),
                "{first}"
            );
            assert!(!first.contains("ORDER BY b.created_at"), "{first}");
            let next = sql(s, Order::Shown, true, Filter::default());
            // The keyset compares the index's own columns.
            assert!(next.contains(&format!("AND ({SHOWN_B}, b.")), "{next}");
            assert!(next.contains("< (COALESCE($2::timestamptz, '-infinity'::timestamptz), "));
        }
        let q = sql(
            Section::IncomingBlocks,
            Order::Shown,
            true,
            Filter::default(),
        );
        assert!(q.ends_with("b.author_id DESC, b.rkey DESC LIMIT $5"), "{q}");
        let q = sql(
            Section::OutgoingBlocks,
            Order::Shown,
            true,
            Filter::default(),
        );
        assert!(q.contains("b.rkey) < (COALESCE($2"), "{q}");
        assert!(q.ends_with("DESC, b.rkey DESC LIMIT $4"), "{q}");
        let q = sql(Section::ListMembers, Order::Shown, false, Filter::default());
        assert!(q.contains("FROM list_items b JOIN actors a ON a.id = b.subject_id"));
        assert!(q.contains("WHERE b.list_id = $1"));
    }

    #[test]
    fn stored_order_is_the_previous_one() {
        let q = sql(
            Section::IncomingBlocks,
            Order::Stored,
            true,
            Filter::default(),
        );
        assert!(q.contains("AND (b.author_id, b.rkey) > ($2, $3) ORDER BY b.author_id, b.rkey"));
        let q = sql(
            Section::OutgoingBlocks,
            Order::Stored,
            true,
            Filter::default(),
        );
        assert!(
            q.contains("AND (b.rkey) > ($2) ORDER BY b.rkey LIMIT $3"),
            "{q}"
        );
        let q = sql(
            Section::ListMembers,
            Order::Stored,
            false,
            Filter::default(),
        );
        assert!(q.ends_with("ORDER BY b.subject_id, b.rkey LIMIT $2"), "{q}");
    }

    #[test]
    fn filters_run_in_the_query() {
        let ids = [7i64];
        let f = Filter {
            hide_inactive: true,
            show_suspended: false,
            show_taken_down: false,
            find: None,
            excluded: &ids,
        };
        let q = sql(Section::ListBlockers, Order::Shown, false, f);
        assert!(q.contains("a.status NOT IN (1, 2, 3, 4)"));
        assert!(q.contains("NOT (a.id = ANY($2))"));
        let q = sql(
            Section::ListBlockers,
            Order::Shown,
            false,
            Filter::default(),
        );
        assert!(!q.contains("a.status") && !q.contains("ANY("));
    }

    #[test]
    fn shown_time_is_the_earlier_of_the_two() {
        let t = |h| Utc.with_ymd_and_hms(2026, 10, 3, h, 0, 0).unwrap();
        let row = |c, f| Row {
            party_id: 1,
            did: String::new(),
            rkey: String::new(),
            created_at: c,
            first_seen: f,
        };
        // An honest row: stated before it arrived.
        assert_eq!(row(Some(t(1)), Some(t(2))).shown_time(), Some(t(1)));
        // Stated after it arrived (a future date): where it arrived.
        assert_eq!(row(Some(t(9)), Some(t(2))).shown_time(), Some(t(2)));
        assert_eq!(row(None, Some(t(2))).shown_time(), Some(t(2)));
        assert_eq!(row(Some(t(1)), None).shown_time(), Some(t(1)));
        assert_eq!(row(None, None).shown_time(), None);
    }

    #[test]
    fn lists_naming_sorts_by_the_clamped_time() {
        let q = naming_sql();
        assert!(q.contains(&format!(
            "ORDER BY {SHOWN_X} DESC, l.id DESC LIMIT $3 OFFSET $4"
        )));
        assert!(!q.contains("ORDER BY x.created_at"), "{q}");
        // The count applies the filters of the rows.
        assert!(q.contains(&naming_from(5, 6)));
        // The filter box: wildcards typed by a visitor are literal.
        assert_eq!(Find::containing("Al_ice%"), "%al\\_ice\\%%");
        assert_eq!(Find::containing(""), "%%");
        let t = |h| Utc.with_ymd_and_hms(2026, 10, 3, h, 0, 0).unwrap();
        let row = |c, f| NamingRow {
            purpose: None,
            list_id: 1,
            owner_did: String::new(),
            rkey: String::new(),
            name: None,
            listblock_count: 0,
            added_at: c,
            first_seen: f,
        };
        assert_eq!(row(Some(t(9)), Some(t(2))).shown_time(), Some(t(2)));
        assert_eq!(row(Some(t(1)), Some(t(2))).shown_time(), Some(t(1)));
        assert_eq!(row(None, None).shown_time(), None);
    }

    #[test]
    fn flags_switch_one_section_at_a_time() {
        let s = SortIndexes::default();
        assert_eq!(s.count(), 0);
        assert_eq!(s.order(Section::ListMembers), Order::Stored);
        s.set(Section::ListMembers, true);
        assert_eq!(s.order(Section::ListMembers), Order::Shown);
        assert_eq!(s.order(Section::IncomingBlocks), Order::Stored);
        assert_eq!(s.count(), 1);
    }

    #[test]
    fn estimates() {
        assert_eq!(estimate_from_tuples(-1.0), 0);
        assert_eq!(estimate_from_tuples(1_000_000.0), 62_000_000);
    }
}
