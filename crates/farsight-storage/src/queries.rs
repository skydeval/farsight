//! Read queries behind the stable XRPC endpoints (design §3.2) and the UI
//! lookups (§8.6). Every query is keyset-paginated (§3.1): the caller
//! passes the last key it returned, so items inserted behind the cursor
//! are not returned and items deleted ahead of it are skipped.
//!
//! All functions take a connection so the API can run them inside one
//! transaction with `statement_timeout` set (§3.6).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::codes::{TrackState, actor_status};
use crate::error::Result;

/// SQL fragment: status codes hidden by default (§3.1, §7.4).
const HIDDEN: &str = "(1, 2, 3, 4)";

/// An interned actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorRef {
    /// `actors.id`.
    pub id: i64,
    /// `actors.status` (§7.4 codes).
    pub status: i16,
}

impl ActorRef {
    /// Hidden by default (§3.1).
    pub fn hidden(self) -> bool {
        actor_status::is_hidden(self.status)
    }
}

/// The actor row of `did`, if interned.
pub async fn actor(conn: &mut PgConnection, did: &str) -> Result<Option<ActorRef>> {
    let row: Option<(i64, i16)> = sqlx::query_as("SELECT id, status FROM actors WHERE did = $1")
        .bind(did)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|(id, status)| ActorRef { id, status }))
}

/// The actor rows of `dids` that are interned, keyed by DID.
pub async fn actors(conn: &mut PgConnection, dids: &[String]) -> Result<HashMap<String, ActorRef>> {
    let rows: Vec<(String, i64, i16)> =
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
    pub author_id: i64,
    /// Blocker DID.
    pub did: String,
    /// Block record key (second sort key).
    pub rkey: String,
    /// Author-claimed `createdAt`.
    pub created_at: Option<DateTime<Utc>>,
}

/// Blocks naming `subject_id`, ordered by blocker actor id then rkey
/// (§3.2). Duplicate records from one blocker are all returned.
pub async fn incoming_blocks(
    conn: &mut PgConnection,
    subject_id: i64,
    include_inactive: bool,
    after: Option<(i64, &str)>,
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
    let rows: Vec<(i64, String, String, Option<DateTime<Utc>>)> = sqlx::query_as(&sql)
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
    pub id: i64,
    /// Owner DID (URI authority).
    pub owner_did: String,
    /// List record key.
    pub rkey: String,
    /// `lists.purpose` code.
    pub purpose: Option<i16>,
    /// Truncated name.
    pub name: Option<String>,
}

/// One (list, blocker) pair (`getIncomingListBlocks`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingListBlock {
    /// The list.
    pub list: ListRef,
    /// Blocker `actors.id`.
    pub author_id: i64,
    /// Blocker DID.
    pub blocker: String,
    /// Listblock record key.
    pub rkey: String,
    /// Author-claimed `createdAt` of the listblock.
    pub created_at: Option<DateTime<Utc>>,
}

/// Every listblock on every **ready or retained** list naming
/// `subject_id` whose record is present (§3.2): one pair per listblock,
/// ordered by list id, blocker actor id, listblock rkey. Uncounted
/// listblocks are returned (they are real blocks, §4.2). A hidden list
/// owner suppresses its lists, a hidden blocker its listblocks, unless
/// `include_inactive`.
pub async fn incoming_list_blocks(
    conn: &mut PgConnection,
    subject_id: i64,
    include_inactive: bool,
    purpose: Option<i16>,
    after: Option<(i64, i64, &str)>,
    limit: i64,
) -> Result<Vec<IncomingListBlock>> {
    let keyset = if after.is_some() {
        "AND (l.id, b.author_id, b.rkey) > ($4, $5, $6)"
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
           AND l.track_state IN (2, 3) AND l.record_state = 1
           AND ($2 OR (o.status NOT IN {HIDDEN} AND ba.status NOT IN {HIDDEN}))
           AND ($3::smallint IS NULL OR l.purpose = $3)
           {keyset}
         ORDER BY l.id, b.author_id, b.rkey LIMIT $7"
    );
    type Row = (
        i64,
        String,
        String,
        Option<i16>,
        Option<String>,
        i64,
        String,
        String,
        Option<DateTime<Utc>>,
    );
    let rows: Vec<Row> = sqlx::query_as(&sql)
        .bind(subject_id)
        .bind(include_inactive)
        .bind(purpose)
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
                        purpose,
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
    /// The list.
    pub list: ListRef,
    /// Counted listblocks on it.
    pub listblock_count: i32,
    /// `createdAt` of the listitem reported in `item_rkey`.
    pub added_at: Option<DateTime<Utc>>,
    /// The listitem naming the subject (lowest rkey if named twice).
    pub item_rkey: String,
}

/// Ready or retained lists with a present record naming `subject_id`,
/// once per list, ordered by list id (§3.2). Hidden owners are excluded
/// unless `include_inactive`.
pub async fn lists_naming(
    conn: &mut PgConnection,
    subject_id: i64,
    include_inactive: bool,
    purpose: Option<i16>,
    after: Option<i64>,
    limit: i64,
) -> Result<Vec<ListNaming>> {
    type Row = (
        i64,
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
               FROM list_items li WHERE li.subject_id = $1
               ORDER BY li.list_id, li.rkey) x
         JOIN lists l ON l.id = x.list_id
         JOIN actors o ON o.id = l.owner_id
         WHERE l.track_state IN (2, 3) AND l.record_state = 1
           AND ($2 OR o.status NOT IN {HIDDEN})
           AND ($3::smallint IS NULL OR l.purpose = $3)
           AND ($4::bigint IS NULL OR l.id > $4)
         ORDER BY l.id LIMIT $5"
    ))
    .bind(subject_id)
    .bind(include_inactive)
    .bind(purpose)
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
                    purpose,
                    name,
                },
                listblock_count: count,
                added_at,
                item_rkey,
            },
        )
        .collect())
}

/// Coverage inputs for the lists naming a subject (§3.7.5 item 4), read
/// live.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NamingCoverage {
    /// `min(fetched_witness)` over ready/retained lists naming the subject.
    pub min_fetched_witness: Option<DateTime<Utc>>,
    /// Some ready/retained list naming the subject has no
    /// `fetched_witness`.
    pub unfetched: bool,
    /// Lists naming the subject that are live `pending`, or `purging` with
    /// `purge_then = untracked` and count > 0 (§3.7.1 live rule).
    pub live_pending: Vec<i64>,
}

/// Reads [`NamingCoverage`] for `subject_id`.
pub async fn naming_coverage(conn: &mut PgConnection, subject_id: i64) -> Result<NamingCoverage> {
    let (min_fw, unfetched, live_pending): (Option<DateTime<Utc>>, Option<bool>, Vec<i64>) =
        sqlx::query_as(
            "SELECT min(l.fetched_witness) FILTER (WHERE l.track_state IN (2, 3)),
                    bool_or(l.fetched_witness IS NULL) FILTER (WHERE l.track_state IN (2, 3)),
                    COALESCE(array_agg(l.id ORDER BY l.id) FILTER (
                      WHERE l.track_state = 1
                         OR (l.track_state = 5 AND l.purge_then = 0 AND l.listblock_count > 0)),
                      '{}')
             FROM lists l
             WHERE l.id IN (SELECT li.list_id FROM list_items li WHERE li.subject_id = $1)",
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
/// (`subject_lists(X)`) that lower subject-scope coverage (§3.7.5 item 4):
/// pending, purging with re-admit (reported as `(Purging, true)`),
/// `unavailable`, `deferred`, `missing`.
pub async fn subject_list_states(
    conn: &mut PgConnection,
    actor_id: i64,
) -> Result<Vec<(TrackState, bool)>> {
    let rows: Vec<(i16, bool)> = sqlx::query_as(
        "SELECT DISTINCT l.track_state,
                (l.track_state = 5 AND l.purge_then = 0 AND l.listblock_count > 0)
         FROM subject_lists s JOIN lists l ON l.id = s.list_id
         WHERE s.actor_id = $1
           AND (l.track_state IN (1, 4, 6, 8)
                OR (l.track_state = 5 AND l.purge_then = 0 AND l.listblock_count > 0))",
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
    pub id: i64,
    /// Owner DID.
    pub owner_did: String,
    /// Owner status (§7.4 codes).
    pub owner_status: i16,
    /// Record key.
    pub rkey: String,
    /// `record_state` code.
    pub record_state: i16,
    /// `purpose` code.
    pub purpose: Option<i16>,
    /// Truncated name.
    pub name: Option<String>,
    /// Counted listblocks.
    pub listblock_count: i32,
    /// Tracking state.
    pub track_state: TrackState,
    /// `capped`.
    pub capped: bool,
    /// Stored items.
    pub item_count: i32,
    /// Coverage point of the last promoting or refresh run.
    pub fetched_witness: Option<DateTime<Utc>>,
    /// Target after a purge (`TrackState` code).
    pub purge_then: Option<i16>,
    /// Admission time.
    pub admitted_at: Option<DateTime<Utc>>,
}

impl ListInfo {
    /// The `state` reported by `getListMembers` (§3.2): `purging` is
    /// reported as `pending` if it will be re-admitted (`purge_then =
    /// untracked` and count > 0), else `untracked`.
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
            && self.purge_then == Some(TrackState::Untracked.code())
            && self.listblock_count > 0
    }
}

/// The list `at://owner_did/app.bsky.graph.list/rkey`, if known.
pub async fn list_info(
    conn: &mut PgConnection,
    owner_did: &str,
    rkey: &str,
) -> Result<Option<ListInfo>> {
    type Row = (
        i64,
        String,
        i16,
        String,
        i16,
        Option<i16>,
        Option<String>,
        i32,
        i16,
        bool,
        i32,
        Option<DateTime<Utc>>,
        Option<i16>,
        Option<DateTime<Utc>>,
    );
    let row: Option<Row> = sqlx::query_as(
        "SELECT l.id, o.did, o.status, l.rkey, l.record_state, l.purpose, l.name,
                l.listblock_count, l.track_state, l.capped, l.item_count, l.fetched_witness,
                l.purge_then, l.admitted_at
         FROM lists l JOIN actors o ON o.id = l.owner_id
         WHERE o.did = $1 AND l.rkey = $2",
    )
    .bind(owner_did)
    .bind(rkey)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|r| ListInfo {
        id: r.0,
        owner_did: r.1,
        owner_status: r.2,
        rkey: r.3,
        record_state: r.4,
        purpose: r.5,
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
    /// Whether the record has been read for the two fields above (false
    /// on rows older than the columns).
    pub read: bool,
}

/// [`ListAbout`] of the list `list_id`.
pub async fn list_about(conn: &mut PgConnection, list_id: i64) -> Result<ListAbout> {
    let row: Option<(Option<String>, Option<String>, bool)> =
        sqlx::query_as("SELECT description, avatar_cid, about_read FROM lists WHERE id = $1")
            .bind(list_id)
            .fetch_optional(conn)
            .await?;
    Ok(row
        .map(|r| ListAbout {
            description: r.0,
            avatar_cid: r.1,
            read: r.2,
        })
        .unwrap_or_default())
}

/// Present lists whose record has not been read for its description,
/// after `after` in id order: `(id, owner DID, rkey)`.
pub async fn lists_unread(
    conn: &mut PgConnection,
    after: i64,
    limit: i64,
) -> Result<Vec<(i64, String, String)>> {
    Ok(sqlx::query_as(
        "SELECT l.id, o.did, l.rkey FROM lists l JOIN actors o ON o.id = l.owner_id
         WHERE l.id > $1 AND l.record_state = 1 AND NOT l.about_read
         ORDER BY l.id LIMIT $2",
    )
    .bind(after)
    .bind(limit)
    .fetch_all(conn)
    .await?)
}

/// Stores what a list's record says about itself, read for a row older
/// than the columns. Only a present record that has not been read is
/// written: a record applied in the meantime is newer than this read.
pub async fn list_about_fill(
    conn: &mut PgConnection,
    list_id: i64,
    description: Option<&str>,
    avatar_cid: Option<&str>,
) -> Result<bool> {
    let done = sqlx::query(
        "UPDATE lists SET description = $2, avatar_cid = $3, about_read = true
         WHERE id = $1 AND NOT about_read AND record_state = 1",
    )
    .bind(list_id)
    .bind(description)
    .bind(avatar_cid)
    .execute(conn)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// One list member (`getListMembers`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMember {
    /// Member `actors.id` (first sort key).
    pub subject_id: i64,
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
    list_id: i64,
    after: Option<(i64, &str)>,
    limit: i64,
) -> Result<Vec<ListMember>> {
    let keyset = if after.is_some() {
        "AND (li.subject_id, li.rkey) > ($2, $3)"
    } else {
        "AND $2::bigint IS NULL AND $3::text IS NULL"
    };
    let rows: Vec<(i64, String, String, Option<DateTime<Utc>>)> = sqlx::query_as(&format!(
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
    pub id: i64,
    /// Owner DID.
    pub owner_did: String,
    /// Owner status code.
    pub owner_status: i16,
    /// Record key.
    pub rkey: String,
    /// Tracking state.
    pub track_state: TrackState,
    /// `record_state` code.
    pub record_state: i16,
    /// `capped`.
    pub capped: bool,
    /// `purge_then` code.
    pub purge_then: Option<i16>,
    /// Counted listblocks.
    pub listblock_count: i32,
    /// Coverage point of the last fetch run.
    pub fetched_witness: Option<DateTime<Utc>>,
}

impl PartyList {
    /// `ready`/`retained` with a present record and a shown owner: its
    /// items block (§3.2).
    pub fn blocks(&self, include_inactive: bool) -> bool {
        matches!(self.track_state, TrackState::Ready | TrackState::Retained)
            && self.record_state == 1
            && (include_inactive || !actor_status::is_hidden(self.owner_status))
    }

    /// `purging` with `purge_then = untracked` and count > 0.
    pub fn readmits(&self) -> bool {
        self.track_state == TrackState::Purging
            && self.purge_then == Some(TrackState::Untracked.code())
            && self.listblock_count > 0
    }
}

/// Everything `checkBlocks(X, others)` reads (§3.7.5 item 5 cost model):
/// direct blocks in both directions, the parties' listblocks joined with
/// list states, and items naming a party on those lists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckRows {
    /// `(author_id, subject_id)` of blocks between X and the others, in
    /// either direction, with the author's status.
    pub direct: Vec<(i64, i64, i16)>,
    /// `author_id` → lists the author holds a listblock on.
    pub listblocks: HashMap<i64, Vec<i64>>,
    /// Lists referenced by `listblocks`.
    pub lists: HashMap<i64, PartyList>,
    /// `(list_id, subject_id)` items on those lists naming a party.
    pub items: Vec<(i64, i64)>,
    /// Status of each party author (for hiding listblocks).
    pub status: HashMap<i64, i16>,
}

/// Reads [`CheckRows`] for viewer `x` and `others` (actor ids).
pub async fn check_rows(conn: &mut PgConnection, x: i64, others: &[i64]) -> Result<CheckRows> {
    let mut out = CheckRows::default();
    let parties: Vec<i64> = std::iter::once(x).chain(others.iter().copied()).collect();
    let statuses: Vec<(i64, i16)> =
        sqlx::query_as("SELECT id, status FROM actors WHERE id = ANY($1)")
            .bind(&parties)
            .fetch_all(&mut *conn)
            .await?;
    out.status = statuses.into_iter().collect();
    let direct: Vec<(i64, i64)> = sqlx::query_as(
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
        .map(|(a, s)| (a, s, out.status.get(&a).copied().unwrap_or(0)))
        .collect();
    type LRow = (
        i64,
        i64,
        String,
        i16,
        String,
        i16,
        i16,
        bool,
        Option<i16>,
        i32,
        Option<DateTime<Utc>>,
    );
    let rows: Vec<LRow> = sqlx::query_as(
        "SELECT DISTINCT b.author_id, l.id, o.did, o.status, l.rkey, l.track_state,
                l.record_state, l.capped, l.purge_then, l.listblock_count, l.fetched_witness
         FROM list_blocks b
         JOIN lists l ON l.id = b.list_id
         JOIN actors o ON o.id = l.owner_id
         WHERE b.author_id = ANY($1)",
    )
    .bind(&parties)
    .fetch_all(&mut *conn)
    .await?;
    for r in rows {
        out.listblocks.entry(r.0).or_default().push(r.1);
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
    let list_ids: Vec<i64> = out.lists.keys().copied().collect();
    if !list_ids.is_empty() {
        out.items = sqlx::query_as(
            "SELECT DISTINCT li.list_id, li.subject_id FROM list_items li
             WHERE li.subject_id = ANY($1) AND li.list_id = ANY($2)",
        )
        .bind(&parties)
        .bind(&list_ids)
        .fetch_all(&mut *conn)
        .await?;
    }
    Ok(out)
}

/// Per-actor coverage inputs (§3.7.5 items 2 and 5).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActorCoverage {
    /// `discovery_state.state` code.
    pub discovery_state: Option<i16>,
    /// `discovered_witness`: the discovery coverage point `D`.
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
pub async fn actor_coverage(conn: &mut PgConnection, actor_id: i64) -> Result<ActorCoverage> {
    type Row = (
        Option<i16>,
        Option<DateTime<Utc>>,
        Option<bool>,
        bool,
        bool,
        Option<DateTime<Utc>>,
    );
    let r: Row = sqlx::query_as(
        "SELECT d.state, d.discovered_witness, d.truncated,
                EXISTS (SELECT 1 FROM subject_coverage c WHERE c.actor_id = $1 AND c.scope = 1),
                EXISTS (SELECT 1 FROM subject_coverage c WHERE c.actor_id = $1 AND c.scope = 2),
                (SELECT s.clean_witness FROM backfill_state s WHERE s.actor_id = $1)
         FROM (SELECT 1) one
         LEFT JOIN discovery_state d ON d.actor_id = $1",
    )
    .bind(actor_id)
    .fetch_one(conn)
    .await?;
    Ok(ActorCoverage {
        discovery_state: r.0,
        discovered_witness: r.1,
        truncated: r.2.unwrap_or(false),
        subject_block: r.3,
        subject_list_chain: r.4,
        clean_witness: r.5,
    })
}

/// Index-wide counts for `getStats` (§3.2): the maintained counters
/// (§7.1) plus the tracked-list count from the partial state index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    /// Stored blocks.
    pub blocks: i64,
    /// Stored listblocks.
    pub list_blocks: i64,
    /// Lists with a present record.
    pub lists: i64,
    /// Tracked lists.
    pub tracked_lists: i64,
    /// Stored listitems.
    pub list_items: i64,
    /// Interned actors.
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
    let tracked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM lists WHERE track_state IN (1, 2, 3, 4)")
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
    /// The latest sweep cycle, if any: (id, kind code, source, started_at,
    /// completed_at, done, total_est).
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
    pub id: i64,
    /// `kind` code (1 full, 2 repair).
    pub kind: i16,
    /// Enumeration source.
    pub source: String,
    /// Start.
    pub started_at: DateTime<Utc>,
    /// Completion.
    pub completed_at: Option<DateTime<Utc>>,
    /// Members done.
    pub done: i64,
    /// Estimated total.
    pub total_est: Option<i64>,
}

/// Reads [`BackfillOverview`].
pub async fn backfill_overview(conn: &mut PgConnection) -> Result<BackfillOverview> {
    type Row = (
        i64,
        i16,
        String,
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
    let tiers: Vec<(i16, i64)> =
        sqlx::query_as("SELECT tier, count(*) FROM backfill_queue GROUP BY tier")
            .fetch_all(&mut *conn)
            .await?;
    let mut queue_by_tier = [0i64; 3];
    for (t, n) in tiers {
        if (1..=3).contains(&t) {
            queue_by_tier[(t - 1) as usize] = n;
        }
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

/// The oldest pending lists (dashboard): (owner DID, rkey, admitted_at).
pub async fn oldest_pending(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<(String, String, Option<DateTime<Utc>>)>> {
    Ok(sqlx::query_as(
        "SELECT o.did, l.rkey, l.admitted_at FROM lists l JOIN actors o ON o.id = l.owner_id
         WHERE l.track_state = 1 ORDER BY l.admitted_at NULLS LAST, l.id LIMIT $1",
    )
    .bind(limit)
    .fetch_all(conn)
    .await?)
}

/// Recent `op_errors` rows: (id, at, component, did, host, message).
pub type OpErrorRow = (
    i64,
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
    before_id: Option<i64>,
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

/// Top host buckets by lifetime interning (dashboard, §11.2).
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
