//! Read queries behind the stable XRPC endpoints (see
//! `docs/design/api.md`) and the UI lookups (see
//! `docs/design/web-ui.md`). Every query is keyset-paginated: the
//! caller passes the last key it returned, so items inserted behind the
//! cursor are not returned and items deleted ahead of it are skipped.
//!
//! All functions take a connection so the API can run them inside one
//! transaction with `statement_timeout` set.

use crate::codes::sql::{
    CYCLE_REPAIR, HIDDEN, RECORD_PRESENT, SCOPE_BLOCK, SCOPE_LIST_CHAIN, TRACK_PENDING,
    TRACK_PURGING, TRACK_SERVED, TRACK_UNINDEXED, TRACK_UNTRACKED, TRACKED,
};
use crate::ids::{ActorId, CycleId, ListId, OpErrorId};
use farsight_core::{Did, ListPurpose};
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::codes::{ActorStatus, CycleKind, CycleSource, RecordState, Tier, TrackState};
use crate::error::Result;

/// An interned actor: its id, and the status that decides whether its rows
/// are shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorRef {
    /// `actors.id`.
    pub id: ActorId,
    /// `actors.status` code.
    pub status: ActorStatus,
}

impl ActorRef {
    /// Whether the account's rows are left out unless the caller asks for
    /// inactive accounts: it is deactivated, taken down, suspended or
    /// deleted.
    pub fn hidden(self) -> bool {
        self.status.is_hidden()
    }
}

/// The actor row of `did`, if interned.
pub async fn actor(conn: &mut PgConnection, did: &Did) -> Result<Option<ActorRef>> {
    let row: Option<(ActorId, ActorStatus)> =
        sqlx::query_as("SELECT id, status FROM actors WHERE did = $1")
            .bind(did.as_str())
            .fetch_optional(conn)
            .await?;
    Ok(row.map(|(id, status)| ActorRef { id, status }))
}

/// The actor rows of `dids` that are interned, keyed by DID.
pub async fn actors(conn: &mut PgConnection, dids: &[String]) -> Result<HashMap<String, ActorRef>> {
    let rows: Vec<(String, ActorId, ActorStatus)> =
        sqlx::query_as("SELECT did, id, status FROM actors WHERE did = ANY($1)")
            .bind(dids)
            .fetch_all(conn)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(did, id, status)| (did, ActorRef { id, status }))
        .collect())
}

/// One incoming block (`getIncomingBlocks`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingBlock {
    /// Blocker `actors.id` (first sort key).
    pub author_id: ActorId,
    /// Blocker DID.
    pub did: String,
    /// Block record key (second sort key).
    pub rkey: String,
    /// Author-claimed `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
}

/// Blocks naming `subject_id`, ordered by blocker actor id then rkey.
/// Duplicate records from one blocker are all returned.
pub async fn incoming_blocks(
    conn: &mut PgConnection,
    subject_id: ActorId,
    include_inactive: bool,
    after: Option<(ActorId, &str)>,
    limit: i64,
) -> Result<Vec<IncomingBlock>> {
    let keyset = if after.is_some() {
        "AND (b.author_id, b.rkey) > ($3, $4)"
    } else {
        "AND $3::bigint IS NULL AND $4::text IS NULL"
    };
    let sql = format!(
        "SELECT b.author_id, a.did, b.rkey, b.created_at
         FROM blocks b JOIN actors a ON a.id = b.author_id
         WHERE b.subject_id = $1 AND ($2 OR a.status NOT IN {HIDDEN}) {keyset}
         ORDER BY b.author_id, b.rkey LIMIT $5"
    );
    let rows: Vec<(ActorId, String, String, Option<DateTime<Utc>>)> = sqlx::query_as(&sql)
        .bind(subject_id)
        .bind(include_inactive)
        .bind(after.map(|a| a.0))
        .bind(after.map(|a| a.1))
        .bind(limit)
        .fetch_all(conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(author_id, did, rkey, created_at)| IncomingBlock {
            author_id,
            did,
            rkey,
            created_at,
        })
        .collect())
}

/// A list as seen by the list endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRef {
    /// `lists.id` (sort key of the list endpoints).
    pub id: ListId,
    /// Owner DID (URI authority).
    pub owner_did: String,
    /// List record key.
    pub rkey: String,
    /// `lists.purpose`; `None` while the record is unknown or deleted.
    pub purpose: Option<ListPurpose>,
    /// `lists.name`: the record's name, as truncated when it was stored.
    pub name: Option<String>,
}

/// One (list, blocker) pair (`getIncomingListBlocks`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingListBlock {
    /// The list that names the subject and that the blocker subscribes to.
    pub list: ListRef,
    /// Blocker `actors.id`.
    pub author_id: ActorId,
    /// Blocker DID.
    pub blocker: String,
    /// Listblock record key.
    pub rkey: String,
    /// Author-claimed `createdAt` of the listblock.
    pub created_at: Option<DateTime<Utc>>,
}

/// One numbered page of [`incoming_list_blocks`], hidden accounts left
/// out: the `limit` listblocks from `offset`, in the same order. The
/// admin lookup page turns pages by number.
///
/// The page is cut from the listblocks before their authors are read:
/// an account on 400 lists has tens of thousands of listblocks, and
/// reading every author's row to sort them all took minutes. A hidden
/// author's listblock is therefore dropped from its page, not replaced,
/// so a page can hold fewer than `limit` rows.
pub async fn incoming_list_blocks_at(
    conn: &mut PgConnection,
    subject_id: ActorId,
    offset: i64,
    limit: i64,
) -> Result<Vec<IncomingListBlock>> {
    type Row = (
        ListId,
        String,
        String,
        Option<i16>,
        Option<String>,
        ActorId,
        String,
        String,
        Option<DateTime<Utc>>,
    );
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT p.id, p.owner_did, p.lrkey, p.purpose, p.name, p.author_id, ba.did, p.rkey,
                p.created_at
         FROM (
           SELECT l.id, o.did AS owner_did, l.rkey AS lrkey, l.purpose, l.name, b.author_id,
                  b.rkey, b.created_at
           FROM lists l
           JOIN actors o ON o.id = l.owner_id
           JOIN list_blocks b ON b.list_id = l.id
           WHERE l.id IN (SELECT li.list_id FROM list_items li WHERE li.subject_id = $1)
             AND l.track_state IN {TRACK_SERVED} AND l.record_state = {RECORD_PRESENT}
             AND o.status NOT IN {HIDDEN}
           ORDER BY l.id, b.author_id, b.rkey LIMIT $2 OFFSET $3) p
         JOIN actors ba ON ba.id = p.author_id
         WHERE ba.status NOT IN {HIDDEN}
         ORDER BY p.id, p.author_id, p.rkey"
    ))
    .bind(subject_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, owner_did, lrkey, purpose, name, author_id, blocker, rkey, created_at)| {
                IncomingListBlock {
                    list: ListRef {
                        id,
                        owner_did,
                        rkey: lrkey,
                        purpose: purpose.map(ListPurpose::from_code),
                        name,
                    },
                    author_id,
                    blocker,
                    rkey,
                    created_at,
                }
            },
        )
        .collect())
}

/// How many listblocks [`incoming_list_blocks_at`] pages through, up to
/// `cap`. It counts the listblocks, not their authors' rows, so those
/// of a hidden author are counted though their rows are not shown.
pub async fn incoming_list_blocks_count(
    conn: &mut PgConnection,
    subject_id: ActorId,
    cap: i64,
) -> Result<i64> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT count(*) FROM (
           SELECT 1 FROM lists l
           JOIN actors o ON o.id = l.owner_id
           JOIN list_blocks b ON b.list_id = l.id
           WHERE l.id IN (SELECT li.list_id FROM list_items li WHERE li.subject_id = $1)
             AND l.track_state IN {TRACK_SERVED} AND l.record_state = {RECORD_PRESENT}
             AND o.status NOT IN {HIDDEN}
           LIMIT $2) x"
    ))
    .bind(subject_id)
    .bind(cap + 1)
    .fetch_one(conn)
    .await?)
}

/// Every listblock on every **ready or retained** list naming
/// `subject_id` whose record is present: one pair per listblock,
/// ordered by list id, blocker actor id, listblock rkey. The order and
/// the cursor are written on the columns of `list_blocks_by_list`
/// alone, so a later page is read from the cursor on and not from the
/// start. Uncounted
/// listblocks are returned (they are real blocks). A hidden list owner
/// suppresses its lists, a hidden blocker its listblocks, unless
/// `include_inactive`.
pub async fn incoming_list_blocks(
    conn: &mut PgConnection,
    subject_id: ActorId,
    include_inactive: bool,
    purpose: Option<ListPurpose>,
    after: Option<(ListId, ActorId, &str)>,
    limit: i64,
) -> Result<Vec<IncomingListBlock>> {
    let keyset = if after.is_some() {
        "AND (b.list_id, b.author_id, b.rkey) > ($4, $5, $6)"
    } else {
        "AND $4::bigint IS NULL AND $5::bigint IS NULL AND $6::text IS NULL"
    };
    let sql = format!(
        "SELECT l.id, o.did, l.rkey, l.purpose, l.name, b.author_id, ba.did, b.rkey, b.created_at
         FROM lists l
         JOIN actors o ON o.id = l.owner_id
         JOIN list_blocks b ON b.list_id = l.id
         JOIN actors ba ON ba.id = b.author_id
         WHERE l.id IN (SELECT li.list_id FROM list_items li WHERE li.subject_id = $1)
           AND l.track_state IN {TRACK_SERVED} AND l.record_state = {RECORD_PRESENT}
           AND ($2 OR (o.status NOT IN {HIDDEN} AND ba.status NOT IN {HIDDEN}))
           AND ($3::smallint IS NULL OR l.purpose = $3)
           {keyset}
         ORDER BY b.list_id, b.author_id, b.rkey LIMIT $7"
    );
    type Row = (
        ListId,
        String,
        String,
        Option<i16>,
        Option<String>,
        ActorId,
        String,
        String,
        Option<DateTime<Utc>>,
    );
    let rows: Vec<Row> = sqlx::query_as(&sql)
        .bind(subject_id)
        .bind(include_inactive)
        .bind(purpose.map(ListPurpose::code))
        .bind(after.map(|a| a.0))
        .bind(after.map(|a| a.1))
        .bind(after.map(|a| a.2))
        .bind(limit)
        .fetch_all(conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, owner_did, lrkey, purpose, name, author_id, blocker, rkey, created_at)| {
                IncomingListBlock {
                    list: ListRef {
                        id,
                        owner_did,
                        rkey: lrkey,
                        purpose: purpose.map(ListPurpose::from_code),
                        name,
                    },
                    author_id,
                    blocker,
                    rkey,
                    created_at,
                }
            },
        )
        .collect())
}

/// One list naming the subject (`getListsNaming`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListNaming {
    /// The list that has the subject as a member.
    pub list: ListRef,
    /// `lists.listblock_count`: the counted listblocks that name the list.
    pub listblock_count: i32,
    /// `createdAt` of the listitem reported in `item_rkey`.
    pub added_at: Option<DateTime<Utc>>,
    /// The listitem naming the subject (lowest rkey if named twice).
    pub item_rkey: String,
}

/// Ready or retained lists with a present record naming `subject_id`,
/// once per list, ordered by list id. Hidden owners are excluded unless
/// `include_inactive`. The page starts after list `after`: that bound is
/// applied where the subject's items are read, so a later page does not
/// read the items of the pages before it again.
pub async fn lists_naming(
    conn: &mut PgConnection,
    subject_id: ActorId,
    include_inactive: bool,
    purpose: Option<ListPurpose>,
    after: Option<ListId>,
    limit: i64,
) -> Result<Vec<ListNaming>> {
    type Row = (
        ListId,
        String,
        String,
        Option<i16>,
        Option<String>,
        i32,
        Option<DateTime<Utc>>,
        String,
    );
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT l.id, o.did, l.rkey, l.purpose, l.name, l.listblock_count, x.created_at, x.rkey
         FROM (SELECT DISTINCT ON (li.list_id) li.list_id, li.rkey, li.created_at
               FROM list_items li
               WHERE li.subject_id = $1 AND ($4::bigint IS NULL OR li.list_id > $4)
               ORDER BY li.list_id, li.rkey) x
         JOIN lists l ON l.id = x.list_id
         JOIN actors o ON o.id = l.owner_id
         WHERE l.track_state IN {TRACK_SERVED} AND l.record_state = {RECORD_PRESENT}
           AND ($2 OR o.status NOT IN {HIDDEN})
           AND ($3::smallint IS NULL OR l.purpose = $3)
         ORDER BY l.id LIMIT $5"
    ))
    .bind(subject_id)
    .bind(include_inactive)
    .bind(purpose.map(ListPurpose::code))
    .bind(after)
    .bind(limit)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, owner_did, rkey, purpose, name, count, added_at, item_rkey)| ListNaming {
                list: ListRef {
                    id,
                    owner_did,
                    rkey,
                    purpose: purpose.map(ListPurpose::from_code),
                    name,
                },
                listblock_count: count,
                added_at,
                item_rkey,
            },
        )
        .collect())
}

/// Coverage inputs for the lists naming a subject, read live.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NamingCoverage {
    /// `min(fetched_witness)` over ready/retained lists naming the subject.
    pub min_fetched_witness: Option<DateTime<Utc>>,
    /// Some ready/retained list naming the subject has no
    /// `fetched_witness`.
    pub unfetched: bool,
    /// Lists naming the subject that are live `pending`, or `purging` with
    /// `purge_then = untracked` and count > 0 (the live rule).
    pub live_pending: Vec<ListId>,
}

/// Reads [`NamingCoverage`] for `subject_id`.
pub async fn naming_coverage(
    conn: &mut PgConnection,
    subject_id: ActorId,
) -> Result<NamingCoverage> {
    let (min_fw, unfetched, live_pending): (Option<DateTime<Utc>>, Option<bool>, Vec<ListId>) =
        sqlx::query_as(
            &format!("SELECT min(l.fetched_witness) FILTER (WHERE l.track_state IN {TRACK_SERVED}),
                    bool_or(l.fetched_witness IS NULL) FILTER (WHERE l.track_state IN {TRACK_SERVED}),
                    COALESCE(array_agg(l.id ORDER BY l.id) FILTER (
                      WHERE l.track_state = {TRACK_PENDING}
                         OR (l.track_state = {TRACK_PURGING} AND l.purge_then = {TRACK_UNTRACKED} AND l.listblock_count > 0)),
                      '{{}}')
             FROM lists l
             WHERE l.id IN (SELECT li.list_id FROM list_items li WHERE li.subject_id = $1)"),
        )
        .bind(subject_id)
        .fetch_one(conn)
        .await?;
    Ok(NamingCoverage {
        min_fetched_witness: min_fw,
        unfetched: unfetched.unwrap_or(false),
        live_pending,
    })
}

/// States of the lists discovery found naming the subject
/// (`subject_lists(X)`) that lower subject-scope coverage: pending,
/// purging with re-admit (reported as `(Purging, true)`),
/// `unavailable`, `deferred`, `missing`.
pub async fn subject_list_states(
    conn: &mut PgConnection,
    actor_id: ActorId,
) -> Result<Vec<(TrackState, bool)>> {
    let rows: Vec<(i16, bool)> = sqlx::query_as(
        &format!("SELECT DISTINCT l.track_state,
                (l.track_state = {TRACK_PURGING} AND l.purge_then = {TRACK_UNTRACKED} AND l.listblock_count > 0)
         FROM subject_lists s JOIN lists l ON l.id = s.list_id
         WHERE s.actor_id = $1
           AND (l.track_state IN {TRACK_UNINDEXED}
                OR (l.track_state = {TRACK_PURGING} AND l.purge_then = {TRACK_UNTRACKED} AND l.listblock_count > 0))"),
    )
    .bind(actor_id)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(s, r)| TrackState::from_code(s).map(|s| (s, r)))
        .collect())
}

/// A list row for `getListMembers` and the list lookup page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListInfo {
    /// `lists.id`.
    pub id: ListId,
    /// Owner DID.
    pub owner_did: String,
    /// `actors.status` of the owner.
    pub owner_status: ActorStatus,
    /// Record key of the list record in the owner's repository.
    pub rkey: String,
    /// `lists.record_state`: whether the list's record is unknown, present
    /// or deleted.
    pub record_state: RecordState,
    /// `purpose`; `None` while the record is unknown or deleted.
    pub purpose: Option<ListPurpose>,
    /// `lists.name`: the record's name, as truncated when it was stored.
    pub name: Option<String>,
    /// `lists.listblock_count`: the counted listblocks that name the list.
    pub listblock_count: i32,
    /// `lists.track_state` as stored; [`ListInfo::reported_state`] is what
    /// the API reports.
    pub track_state: TrackState,
    /// `lists.capped`: the stored item set is cut by a cap.
    pub capped: bool,
    /// `lists.item_count`: the list's stored items. Exact.
    pub item_count: i32,
    /// Coverage point of the last promoting or refresh run.
    pub fetched_witness: Option<DateTime<Utc>>,
    /// Target after a purge (`TrackState` code).
    pub purge_then: Option<TrackState>,
    /// `lists.admitted_at`: time of the current admission, on the
    /// database's clock; `None` for a list never admitted.
    pub admitted_at: Option<DateTime<Utc>>,
}

impl ListInfo {
    /// The `state` reported by `getListMembers`: `purging` is reported
    /// as `pending` if it will be re-admitted (`purge_then = untracked`
    /// and count > 0), else `untracked`.
    pub fn reported_state(&self) -> TrackState {
        match self.track_state {
            TrackState::Purging => {
                if self.readmits() {
                    TrackState::Pending
                } else {
                    TrackState::Untracked
                }
            }
            s => s,
        }
    }

    /// `purging` with `purge_then = untracked` and count > 0.
    pub fn readmits(&self) -> bool {
        self.track_state == TrackState::Purging
            && self.purge_then == Some(TrackState::Untracked)
            && self.listblock_count > 0
    }
}

/// The list `at://owner_did/app.bsky.graph.list/rkey`, if known.
pub async fn list_info(
    conn: &mut PgConnection,
    owner_did: &Did,
    rkey: &str,
) -> Result<Option<ListInfo>> {
    type Row = (
        ListId,
        String,
        ActorStatus,
        String,
        RecordState,
        Option<i16>,
        Option<String>,
        i32,
        i16,
        bool,
        i32,
        Option<DateTime<Utc>>,
        Option<TrackState>,
        Option<DateTime<Utc>>,
    );
    let row: Option<Row> = sqlx::query_as(
        "SELECT l.id, o.did, o.status, l.rkey, l.record_state, l.purpose, l.name,
                l.listblock_count, l.track_state, l.capped, l.item_count, l.fetched_witness,
                l.purge_then, l.admitted_at
         FROM lists l JOIN actors o ON o.id = l.owner_id
         WHERE o.did = $1 AND l.rkey = $2",
    )
    .bind(owner_did.as_str())
    .bind(rkey)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|r| ListInfo {
        id: r.0,
        owner_did: r.1,
        owner_status: r.2,
        rkey: r.3,
        record_state: r.4,
        purpose: r.5.map(ListPurpose::from_code),
        name: r.6,
        listblock_count: r.7,
        track_state: TrackState::from_code(r.8).unwrap_or(TrackState::Untracked),
        capped: r.9,
        item_count: r.10,
        fetched_witness: r.11,
        purge_then: r.12,
        admitted_at: r.13,
    }))
}

/// What a list's record says about itself, for the list's page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListAbout {
    /// The record's description, truncated.
    pub description: Option<String>,
    /// CID of the record's avatar blob.
    pub avatar_cid: Option<String>,
}

/// [`ListAbout`] of the list `list_id`.
pub async fn list_about(conn: &mut PgConnection, list_id: ListId) -> Result<ListAbout> {
    let row: Option<(Option<String>, Option<String>)> =
        sqlx::query_as("SELECT description, avatar_cid FROM lists WHERE id = $1")
            .bind(list_id)
            .fetch_optional(conn)
            .await?;
    Ok(row
        .map(|r| ListAbout {
            description: r.0,
            avatar_cid: r.1,
        })
        .unwrap_or_default())
}

/// One list member (`getListMembers`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMember {
    /// Member `actors.id` (first sort key).
    pub subject_id: ActorId,
    /// Member DID.
    pub did: String,
    /// Listitem record key (second sort key).
    pub item_rkey: String,
    /// Author-claimed `createdAt` of the listitem.
    pub added_at: Option<DateTime<Utc>>,
}

/// Items of `list_id`, ordered by member actor id then listitem rkey.
pub async fn list_members(
    conn: &mut PgConnection,
    list_id: ListId,
    after: Option<(ActorId, &str)>,
    limit: i64,
) -> Result<Vec<ListMember>> {
    let keyset = if after.is_some() {
        "AND (li.subject_id, li.rkey) > ($2, $3)"
    } else {
        "AND $2::bigint IS NULL AND $3::text IS NULL"
    };
    let rows: Vec<(ActorId, String, String, Option<DateTime<Utc>>)> = sqlx::query_as(&format!(
        "SELECT li.subject_id, a.did, li.rkey, li.created_at
         FROM list_items li JOIN actors a ON a.id = li.subject_id
         WHERE li.list_id = $1 {keyset}
         ORDER BY li.subject_id, li.rkey LIMIT $4"
    ))
    .bind(list_id)
    .bind(after.map(|a| a.0))
    .bind(after.map(|a| a.1))
    .bind(limit)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(subject_id, did, item_rkey, added_at)| ListMember {
            subject_id,
            did,
            item_rkey,
            added_at,
        })
        .collect())
}

/// A list some party of a `checkBlocks` call holds a listblock on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartyList {
    /// `lists.id`.
    pub id: ListId,
    /// Owner DID.
    pub owner_did: String,
    /// `actors.status` of the owner; a hidden owner's list does not block
    /// unless inactive accounts are included.
    pub owner_status: ActorStatus,
    /// Record key of the list record in the owner's repository.
    pub rkey: String,
    /// `lists.track_state`, as stored.
    pub track_state: TrackState,
    /// `lists.record_state`: whether the list's record is unknown, present
    /// or deleted.
    pub record_state: RecordState,
    /// `lists.capped`: the stored item set is cut by a cap.
    pub capped: bool,
    /// `lists.purge_then`: the state the list enters after its purge.
    pub purge_then: Option<TrackState>,
    /// `lists.listblock_count`: the counted listblocks that name the list.
    pub listblock_count: i32,
    /// Coverage point of the last fetch run.
    pub fetched_witness: Option<DateTime<Utc>>,
}

impl PartyList {
    /// `ready`/`retained` with a present record and a shown owner: its
    /// items block.
    pub fn blocks(&self, include_inactive: bool) -> bool {
        matches!(self.track_state, TrackState::Ready | TrackState::Retained)
            && self.record_state == RecordState::Present
            && (include_inactive || !self.owner_status.is_hidden())
    }

    /// `purging` with `purge_then = untracked` and count > 0.
    pub fn readmits(&self) -> bool {
        self.track_state == TrackState::Purging
            && self.purge_then == Some(TrackState::Untracked)
            && self.listblock_count > 0
    }
}

/// Lists weighed per party of a `checkBlocks` call: the lists an account
/// holds a listblock on are read in `lists.id` order up to this many.
/// With up to 101 parties it bounds the rows one call reads and the work
/// of putting its answer together. A party that listblocks more lists is
/// reported in [`CheckRows::truncated`].
pub const CHECK_LISTS_PER_PARTY: usize = 1_000;

/// Everything `checkBlocks(X, others)` reads: direct blocks in both
/// directions, the parties' listblocks joined with list states, and the
/// items on those lists that name the other side of a pair.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckRows {
    /// `(author_id, subject_id)` of blocks between X and the others, in
    /// either direction, with the author's status.
    pub direct: Vec<(ActorId, ActorId, ActorStatus)>,
    /// `author_id` → lists the author holds a listblock on, at most
    /// [`CHECK_LISTS_PER_PARTY`] of them.
    pub listblocks: HashMap<ActorId, Vec<ListId>>,
    /// Lists referenced by `listblocks`.
    pub lists: HashMap<ListId, PartyList>,
    /// `(list_id, subject_id)` items on those lists: X on a list one of
    /// the others listblocks, one of the others on a list X listblocks.
    pub items: HashSet<(ListId, ActorId)>,
    /// Status of each party author (for hiding listblocks).
    pub status: HashMap<ActorId, ActorStatus>,
    /// Parties that listblock more lists than `listblocks` holds for
    /// them: what the lists left out say about their pairs is not known.
    pub truncated: HashSet<ActorId>,
}

/// Reads [`CheckRows`] for viewer `x` and `others` (actor ids).
pub async fn check_rows(
    conn: &mut PgConnection,
    x: ActorId,
    others: &[ActorId],
) -> Result<CheckRows> {
    check_rows_capped(conn, x, others, CHECK_LISTS_PER_PARTY).await
}

/// [`check_rows`] with `per_party` in place of
/// [`CHECK_LISTS_PER_PARTY`].
pub async fn check_rows_capped(
    conn: &mut PgConnection,
    x: ActorId,
    others: &[ActorId],
    per_party: usize,
) -> Result<CheckRows> {
    let mut out = CheckRows::default();
    let parties: Vec<ActorId> = std::iter::once(x).chain(others.iter().copied()).collect();
    let statuses: Vec<(ActorId, ActorStatus)> =
        sqlx::query_as("SELECT id, status FROM actors WHERE id = ANY($1)")
            .bind(&parties)
            .fetch_all(&mut *conn)
            .await?;
    out.status = statuses.into_iter().collect();
    let direct: Vec<(ActorId, ActorId)> = sqlx::query_as(
        "SELECT DISTINCT b.author_id, b.subject_id FROM blocks b
         WHERE (b.subject_id = ANY($2) AND b.author_id = $1)
            OR (b.subject_id = $1 AND b.author_id = ANY($2))",
    )
    .bind(x)
    .bind(others)
    .fetch_all(&mut *conn)
    .await?;
    out.direct = direct
        .into_iter()
        .map(|(a, s)| {
            (
                a,
                s,
                out.status.get(&a).copied().unwrap_or(ActorStatus::Active),
            )
        })
        .collect();
    type LRow = (
        ActorId,
        ListId,
        String,
        ActorStatus,
        String,
        i16,
        RecordState,
        bool,
        Option<TrackState>,
        i32,
        Option<DateTime<Utc>>,
    );
    // Per party, its lists in id order, one more than are weighed: the
    // extra row says that there are more. `list_blocks_by_author_list`
    // hands them over in that order, so a party with many listblocks
    // costs no more than one with `per_party`.
    let cap = i64::try_from(per_party).unwrap_or(i64::MAX - 1);
    let rows: Vec<LRow> = sqlx::query_as(
        "SELECT p.id, l.id, o.did, o.status, l.rkey, l.track_state,
                l.record_state, l.capped, l.purge_then, l.listblock_count, l.fetched_witness
         FROM unnest($1::BIGINT[]) AS p(id)
         CROSS JOIN LATERAL (
           SELECT DISTINCT b.list_id FROM list_blocks b
           WHERE b.author_id = p.id ORDER BY b.list_id LIMIT $2) s
         JOIN lists l ON l.id = s.list_id
         JOIN actors o ON o.id = l.owner_id
         ORDER BY p.id, l.id",
    )
    .bind(&parties)
    .bind(cap.saturating_add(1))
    .fetch_all(&mut *conn)
    .await?;
    for r in rows {
        let of_party = out.listblocks.entry(r.0).or_default();
        if of_party.len() >= per_party {
            out.truncated.insert(r.0);
            continue;
        }
        of_party.push(r.1);
        out.lists.entry(r.1).or_insert(PartyList {
            id: r.1,
            owner_did: r.2,
            owner_status: r.3,
            rkey: r.4,
            track_state: TrackState::from_code(r.5).unwrap_or(TrackState::Untracked),
            record_state: r.6,
            capped: r.7,
            purge_then: r.8,
            listblock_count: r.9,
            fetched_witness: r.10,
        });
    }
    // Only the items a pair can turn on: X on the others' lists, the
    // others on X's lists.
    let x_lists: Vec<ListId> = out.listblocks.get(&x).cloned().unwrap_or_default();
    let mut other_lists: Vec<ListId> = others
        .iter()
        .filter_map(|o| out.listblocks.get(o))
        .flatten()
        .copied()
        .collect();
    other_lists.sort_unstable();
    other_lists.dedup();
    if !other_lists.is_empty() {
        let named: Vec<ListId> = sqlx::query_scalar(
            "SELECT DISTINCT li.list_id FROM list_items li
             WHERE li.subject_id = $1 AND li.list_id = ANY($2)",
        )
        .bind(x)
        .bind(&other_lists)
        .fetch_all(&mut *conn)
        .await?;
        out.items.extend(named.into_iter().map(|l| (l, x)));
    }
    if !x_lists.is_empty() && !others.is_empty() {
        let named: Vec<(ListId, ActorId)> = sqlx::query_as(
            "SELECT DISTINCT li.list_id, li.subject_id FROM list_items li
             WHERE li.subject_id = ANY($1) AND li.list_id = ANY($2)",
        )
        .bind(others)
        .bind(&x_lists)
        .fetch_all(&mut *conn)
        .await?;
        out.items.extend(named);
    }
    Ok(out)
}

/// Per-actor coverage inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActorCoverage {
    /// The discovery coverage point `D`: the point of the last run that
    /// completed untruncated, read from `subject_coverage`. A run that is
    /// in flight, failed or truncated does not move it.
    pub discovered_witness: Option<DateTime<Utc>>,
    /// The last discovery was truncated.
    pub truncated: bool,
    /// `subject_coverage(X, block)` exists (untruncated completion).
    pub subject_block: bool,
    /// `subject_coverage(X, list_chain)` exists.
    pub subject_list_chain: bool,
    /// `backfill_state.clean_witness`.
    pub clean_witness: Option<DateTime<Utc>>,
}

/// Reads [`ActorCoverage`] for one actor.
pub async fn actor_coverage(conn: &mut PgConnection, actor_id: ActorId) -> Result<ActorCoverage> {
    type Row = (
        Option<DateTime<Utc>>,
        Option<bool>,
        bool,
        bool,
        Option<DateTime<Utc>>,
    );
    let r: Row = sqlx::query_as(
        &format!("SELECT (SELECT min(c.discovered_witness) FROM subject_coverage c WHERE c.actor_id = $1), d.truncated,
                EXISTS (SELECT 1 FROM subject_coverage c WHERE c.actor_id = $1 AND c.scope = {SCOPE_BLOCK}),
                EXISTS (SELECT 1 FROM subject_coverage c WHERE c.actor_id = $1 AND c.scope = {SCOPE_LIST_CHAIN}),
                (SELECT s.clean_witness FROM backfill_state s WHERE s.actor_id = $1)
         FROM (SELECT 1) one
         LEFT JOIN discovery_state d ON d.actor_id = $1"),
    )
    .bind(actor_id)
    .fetch_one(conn)
    .await?;
    Ok(ActorCoverage {
        discovered_witness: r.0,
        truncated: r.1.unwrap_or(false),
        subject_block: r.2,
        subject_list_chain: r.3,
        clean_witness: r.4,
    })
}

/// Index-wide counts for `getStats`: the maintained counters plus the
/// tracked-list count from the partial state index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    /// `blocks` rows, from the maintained counter (approximate between
    /// nightly rebuilds).
    pub blocks: i64,
    /// `list_blocks` rows, from the maintained counter.
    pub list_blocks: i64,
    /// Lists with a present record.
    pub lists: i64,
    /// Lists in a tracked state, counted at the time of the read.
    pub tracked_lists: i64,
    /// `list_items` rows, from the maintained counter.
    pub list_items: i64,
    /// `actors` rows, from the maintained counter.
    pub actors: i64,
}

/// Reads [`Counts`].
pub async fn counts(conn: &mut PgConnection) -> Result<Counts> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT name, COALESCE(sum(value), 0)::bigint FROM stats_counters GROUP BY name",
    )
    .fetch_all(&mut *conn)
    .await?;
    let m: HashMap<String, i64> = rows.into_iter().collect();
    let get = |k: &str| m.get(k).copied().unwrap_or(0).max(0);
    let tracked: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM lists WHERE track_state IN {TRACKED}"
    ))
    .fetch_one(&mut *conn)
    .await?;
    Ok(Counts {
        blocks: get(crate::counters::stat::BLOCKS),
        list_blocks: get(crate::counters::stat::LIST_BLOCKS),
        lists: get(crate::counters::stat::LISTS),
        tracked_lists: tracked,
        list_items: get(crate::counters::stat::LIST_ITEMS),
        actors: get(crate::counters::stat::ACTORS),
    })
}

/// Sweep and queue status for `getStats.backfill` and the dashboard.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BackfillOverview {
    /// The cycle started last, full or repair, finished or not; `None`
    /// before the first cycle.
    pub cycle: Option<CycleRow>,
    /// Waiting queue entries per tier (index 0 = tier 1).
    pub queue_by_tier: [i64; 3],
    /// Repo jobs that finished in the last hour.
    pub repos_last_hour: i64,
}

/// One `sweep_cycles` row, as `getStats` reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct CycleRow {
    /// `sweep_cycles.id`.
    pub id: CycleId,
    /// Full sweep or repair.
    pub kind: CycleKind,
    /// What the cycle enumerates.
    pub source: CycleSource,
    /// `sweep_cycles.started_at`.
    pub started_at: DateTime<Utc>,
    /// `sweep_cycles.completed_at`; `None` while the cycle runs.
    pub completed_at: Option<DateTime<Utc>>,
    /// `sweep_cycles.done`: members finished so far.
    pub done: i64,
    /// `sweep_cycles.total_est`: an estimate of how many members the cycle
    /// has, when there is one.
    pub total_est: Option<i64>,
}

/// Reads [`BackfillOverview`].
pub async fn backfill_overview(conn: &mut PgConnection) -> Result<BackfillOverview> {
    type Row = (
        CycleId,
        CycleKind,
        CycleSource,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
        i64,
        Option<i64>,
    );
    let cycle: Option<Row> = sqlx::query_as(
        "SELECT id, kind, source, started_at, completed_at, done, total_est
         FROM sweep_cycles ORDER BY started_at DESC, id DESC LIMIT 1",
    )
    .fetch_optional(&mut *conn)
    .await?;
    let tiers: Vec<(Tier, i64)> = sqlx::query_as(
        "SELECT tier, count(*) FROM backfill_queue WHERE claimed_by IS NULL GROUP BY tier",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut queue_by_tier = [0i64; 3];
    for (t, n) in tiers {
        queue_by_tier[t.index()] = n;
    }
    let repos_last_hour: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM backfill_state WHERE backfilled_at > now() - interval '1 hour'",
    )
    .fetch_one(&mut *conn)
    .await?;
    Ok(BackfillOverview {
        cycle: cycle.map(|r| CycleRow {
            id: r.0,
            kind: r.1,
            source: r.2,
            started_at: r.3,
            completed_at: r.4,
            done: r.5,
            total_est: r.6,
        }),
        queue_by_tier,
        repos_last_hour,
    })
}

/// Counts of lists per tracking state (`farsight_lists{state}`, dashboard).
pub async fn lists_by_state(conn: &mut PgConnection) -> Result<Vec<(TrackState, i64)>> {
    let rows: Vec<(i16, i64)> =
        sqlx::query_as("SELECT track_state, count(*) FROM lists GROUP BY track_state")
            .fetch_all(conn)
            .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(s, n)| TrackState::from_code(s).map(|s| (s, n)))
        .collect())
}

/// Recent `op_errors` rows: (id, at, component, did, host, message).
pub type OpErrorRow = (
    OpErrorId,
    DateTime<Utc>,
    String,
    Option<String>,
    Option<String>,
    String,
);

/// The most recent `op_errors` (operations page, `admin.listErrors`),
/// newest first, before `before_id` when paging.
pub async fn op_errors(
    conn: &mut PgConnection,
    before_id: Option<OpErrorId>,
    limit: i64,
) -> Result<Vec<OpErrorRow>> {
    Ok(sqlx::query_as(
        "SELECT id, at, component, did, host, message FROM op_errors
         WHERE ($1::bigint IS NULL OR id < $1) ORDER BY id DESC LIMIT $2",
    )
    .bind(before_id)
    .bind(limit)
    .fetch_all(conn)
    .await?)
}

/// The repair cycle that has not finished, if there is one: its id, the
/// accounts it has re-read so far and whether it has read the relay's
/// list to the end (until then the number still to do is not known).
pub async fn repair_running(conn: &mut PgConnection) -> Result<Option<(CycleId, i64, bool)>> {
    Ok(sqlx::query_as(&format!(
        "SELECT id, done, enumerated_at IS NOT NULL FROM sweep_cycles
         WHERE kind = {CYCLE_REPAIR} AND completed_at IS NULL ORDER BY id DESC LIMIT 1"
    ))
    .fetch_optional(conn)
    .await?)
}

/// Top host buckets by lifetime interning (dashboard).
pub async fn top_buckets(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<(String, i64, i64, i64, i64, i64, i16)>> {
    Ok(sqlx::query_as(
        "SELECT bucket, stored_interned, stored_blocks, stored_items, stored_listblocks,
                stored_lists, capped_mask
         FROM host_usage ORDER BY stored_interned DESC, bucket LIMIT $1",
    )
    .bind(limit)
    .fetch_all(conn)
    .await?)
}
