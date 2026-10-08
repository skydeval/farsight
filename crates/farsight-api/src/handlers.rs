//! The stable read queries (see `docs/design/api.md`) with their coverage
//! (see `docs/design/coverage.md`).

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use farsight_core::{AtUri, Collection, ListPurpose};
use farsight_storage::codes::{ActorStatus, Protocol, RecordState, TrackState};
use farsight_storage::coverage::{GlobalSnapshot, Level};
use farsight_storage::ids::{ActorId, ListId};
use farsight_storage::queries::{self, ActorCoverage, ActorRef, ListInfo};
use serde_json::{Map, Value, json};
use sqlx::PgConnection;

use crate::error::XrpcError;
use crate::freshness::{Cov, SubjectKind, View, list_state_reason, ts};
use crate::params::{Params, purpose_filter};
use crate::{ApiState, Reply, cursor};

/// The snapshot, or `503 Overloaded` before the first read.
pub fn snapshot(st: &ApiState) -> Result<Arc<GlobalSnapshot>, XrpcError> {
    st.snapshot
        .get()
        .ok_or_else(|| XrpcError::overloaded("coverage snapshot not ready yet"))
}

pub(crate) fn view<'a>(st: &ApiState, snap: &'a GlobalSnapshot) -> View<'a> {
    View {
        snap,
        lag: st
            .config
            .current()
            .config
            .firehose
            .tuning
            .synthetic_gap_lag
            .get(),
    }
}

/// Harness hook: `_sleep=<seconds>` holds the query slot inside the read
/// transaction (semaphore and `statement_timeout` checks of the harness).
#[cfg(feature = "harness")]
async fn harness_sleep(conn: &mut PgConnection, p: &Params) -> Result<(), XrpcError> {
    if let Some(s) = p.get("_sleep").and_then(|s| s.parse::<f64>().ok()) {
        sqlx::query("SELECT pg_sleep($1)")
            .bind(s)
            .execute(conn)
            .await?;
    }
    Ok(())
}

#[cfg(not(feature = "harness"))]
async fn harness_sleep(_conn: &mut PgConnection, _p: &Params) -> Result<(), XrpcError> {
    Ok(())
}

fn opt_ts(t: Option<DateTime<Utc>>) -> Option<String> {
    t.map(ts)
}

fn insert_opt(m: &mut Map<String, Value>, k: &str, v: Option<impl Into<Value>>) {
    if let Some(v) = v {
        m.insert(k.to_owned(), v.into());
    }
}

fn uri(did: &str, collection: Collection, rkey: &str) -> String {
    format!("at://{did}/{}/{rkey}", collection.nsid())
}

/// The API name of a stored purpose; a row without one reads `other`.
fn purpose_name(purpose: Option<ListPurpose>) -> &'static str {
    purpose.unwrap_or(ListPurpose::Other).api_name()
}

/// Stands for an account Farsight has no row for. Ids start at 1, so no
/// row or reference holds it and every read keyed by it finds nothing.
pub(crate) const NO_ACTOR: ActorId = ActorId::new(-1);

/// `query.getIncomingBlocks`.
pub async fn get_incoming_blocks(st: &Arc<ApiState>, p: &Params) -> Result<Reply, XrpcError> {
    let actor = p.did("actor")?;
    let limit = p.limit()?;
    let after = cursor::id_rkey(p.get("cursor"))?;
    let inc = p.bool("includeInactive")?;
    let snap = snapshot(st)?;
    let v = view(st, &snap);
    let mut tx = st.read_tx().await?;
    harness_sleep(&mut tx, p).await?;
    let a = queries::actor(&mut tx, &actor).await?;
    let (rows, ac) = match a {
        Some(a) => (
            queries::incoming_blocks(
                &mut tx,
                a.id,
                inc,
                after.as_ref().map(|(i, r)| (ActorId::new(*i), r.as_str())),
                limit,
            )
            .await?,
            Some(queries::actor_coverage(&mut tx, a.id).await?),
        ),
        None => (Vec::new(), None),
    };
    tx.rollback().await?;
    let (cov, _) = v.network_or_subject(Collection::Block, ac.as_ref(), SubjectKind::Block);
    let blocks: Vec<Value> = rows
        .iter()
        .map(|b| {
            let mut m = Map::new();
            m.insert("did".into(), json!(b.did));
            m.insert("uri".into(), json!(uri(&b.did, Collection::Block, &b.rkey)));
            insert_opt(&mut m, "createdAt", opt_ts(b.created_at));
            Value::Object(m)
        })
        .collect();
    let mut body = Map::new();
    body.insert("actor".into(), json!(actor.as_str()));
    body.insert("blocks".into(), Value::Array(blocks));
    if rows.len() as i64 == limit {
        let last = rows.last().expect("non-empty");
        body.insert(
            "cursor".into(),
            json!(cursor::encode(&[
                json!(last.author_id.get()),
                json!(last.rkey)
            ])),
        );
    }
    body.insert(
        "freshness".into(),
        v.render(&cov, Utc::now(), st.source_lag()),
    );
    Ok(Reply::ok(Value::Object(body)))
}

/// Composite scope for X with the live rule of the snapshot.
async fn composite(
    v: &View<'_>,
    conn: &mut PgConnection,
    x: Option<(ActorRef, &ActorCoverage)>,
) -> Result<Cov, XrpcError> {
    let (mut c, subject) = v.network_or_subject(
        Collection::ListBlock,
        x.map(|(_, ac)| ac),
        SubjectKind::ListChain,
    );
    if let Some((a, ac)) = x {
        let nc = queries::naming_coverage(conn, a.id).await?;
        if nc.unfetched {
            c.partial("list_pending");
        } else if nc.min_fetched_witness.is_some() && !v.covered(nc.min_fetched_witness) {
            c.partial(v.uncovered_reason());
        }
        // Live rule: a list naming X that turned pending after the snapshot
        // is excluded and makes the response partial.
        if nc
            .live_pending
            .iter()
            .any(|id| v.snap.pending.considered_ids.binary_search(id).is_err())
        {
            c.partial("list_pending");
        }
        if subject {
            for (state, readmits) in queries::subject_list_states(conn, a.id).await? {
                if let Some(r) = list_state_reason(state, readmits) {
                    c.partial(r);
                }
            }
            if v.snap.pending.considered > 0
                && let Some(d) = ac.discovered_witness
            {
                c.cap_indexed(d);
                c.note("list_pending");
            }
        }
    }
    if !subject {
        v.apply_pending_table(&mut c);
    }
    Ok(c)
}

/// `query.getIncomingListBlocks`.
pub async fn get_incoming_list_blocks(st: &Arc<ApiState>, p: &Params) -> Result<Reply, XrpcError> {
    let actor = p.did("actor")?;
    let limit = p.limit()?;
    let after = cursor::id_id_rkey(p.get("cursor"))?;
    let inc = p.bool("includeInactive")?;
    let purpose = purpose_filter(p.get("purpose"))?;
    let snap = snapshot(st)?;
    let v = view(st, &snap);
    let mut tx = st.read_tx().await?;
    harness_sleep(&mut tx, p).await?;
    let a = queries::actor(&mut tx, &actor).await?;
    let (rows, cov) = match a {
        Some(a) => {
            let rows = queries::incoming_list_blocks(
                &mut tx,
                a.id,
                inc,
                purpose,
                after
                    .as_ref()
                    .map(|(l, b, r)| (ListId::new(*l), ActorId::new(*b), r.as_str())),
                limit,
            )
            .await?;
            let ac = queries::actor_coverage(&mut tx, a.id).await?;
            let cov = composite(&v, &mut tx, Some((a, &ac))).await?;
            (rows, cov)
        }
        None => (Vec::new(), composite(&v, &mut tx, None).await?),
    };
    tx.rollback().await?;
    let items: Vec<Value> = rows
        .iter()
        .map(|r| {
            let mut m = Map::new();
            m.insert(
                "list".into(),
                json!(uri(&r.list.owner_did, Collection::List, &r.list.rkey)),
            );
            m.insert("listPurpose".into(), json!(purpose_name(r.list.purpose)));
            insert_opt(&mut m, "listName", r.list.name.clone());
            m.insert("blocker".into(), json!(r.blocker));
            m.insert(
                "listblockUri".into(),
                json!(uri(&r.blocker, Collection::ListBlock, &r.rkey)),
            );
            insert_opt(&mut m, "createdAt", opt_ts(r.created_at));
            Value::Object(m)
        })
        .collect();
    let mut body = Map::new();
    body.insert("actor".into(), json!(actor.as_str()));
    body.insert("items".into(), Value::Array(items));
    if rows.len() as i64 == limit {
        let last = rows.last().expect("non-empty");
        body.insert(
            "cursor".into(),
            json!(cursor::encode(&[
                json!(last.list.id.get()),
                json!(last.author_id.get()),
                json!(last.rkey)
            ])),
        );
    }
    body.insert(
        "freshness".into(),
        v.render(&cov, Utc::now(), st.source_lag()),
    );
    Ok(Reply::ok(Value::Object(body)))
}

/// `query.getListsNaming`.
pub async fn get_lists_naming(st: &Arc<ApiState>, p: &Params) -> Result<Reply, XrpcError> {
    let actor = p.did("actor")?;
    let limit = p.limit()?;
    let after = cursor::id(p.get("cursor"))?;
    let inc = p.bool("includeInactive")?;
    let purpose = purpose_filter(p.get("purpose"))?;
    let snap = snapshot(st)?;
    let v = view(st, &snap);
    let mut tx = st.read_tx().await?;
    harness_sleep(&mut tx, p).await?;
    let a = queries::actor(&mut tx, &actor).await?;
    let (rows, cov) = match a {
        Some(a) => {
            let rows =
                queries::lists_naming(&mut tx, a.id, inc, purpose, after.map(ListId::new), limit)
                    .await?;
            let ac = queries::actor_coverage(&mut tx, a.id).await?;
            let cov = composite(&v, &mut tx, Some((a, &ac))).await?;
            (rows, cov)
        }
        None => (Vec::new(), composite(&v, &mut tx, None).await?),
    };
    tx.rollback().await?;
    let lists: Vec<Value> = rows
        .iter()
        .map(|r| {
            let mut m = Map::new();
            m.insert(
                "uri".into(),
                json!(uri(&r.list.owner_did, Collection::List, &r.list.rkey)),
            );
            m.insert("purpose".into(), json!(purpose_name(r.list.purpose)));
            insert_opt(&mut m, "name", r.list.name.clone());
            m.insert("listblockCount".into(), json!(r.listblock_count));
            insert_opt(&mut m, "addedAt", opt_ts(r.added_at));
            m.insert(
                "itemUri".into(),
                json!(uri(&r.list.owner_did, Collection::ListItem, &r.item_rkey)),
            );
            Value::Object(m)
        })
        .collect();
    let mut body = Map::new();
    body.insert("actor".into(), json!(actor.as_str()));
    body.insert("lists".into(), Value::Array(lists));
    if rows.len() as i64 == limit {
        let last = rows.last().expect("non-empty");
        body.insert(
            "cursor".into(),
            json!(cursor::encode(&[json!(last.list.id.get())])),
        );
    }
    body.insert(
        "freshness".into(),
        v.render(&cov, Utc::now(), st.source_lag()),
    );
    Ok(Reply::ok(Value::Object(body)))
}

/// Whether the session is on v2 without an open v1 interval.
fn on_v2(s: &GlobalSnapshot) -> bool {
    s.firehose.protocol == Some(Protocol::V2)
        && !s.gaps.iter().any(|g| {
            g.cause == farsight_storage::codes::GapCause::SyncUnavailable && g.to_at.is_none()
        })
}

/// What `getListMembers` says of a list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListReport {
    /// The `state` reported.
    pub state: TrackState,
    /// Whether members are returned.
    pub serves: bool,
    /// Whether the list's own facts (`purpose`, `name`, `capped`,
    /// `listblockCount`) are reported.
    pub facts: bool,
}

/// Decides what `getListMembers` says of the list `info` (`None` for a
/// list Farsight has no row for).
///
/// A list is authored by its owner, so a hidden owner hides it: the list
/// is reported `unavailable`, the state of a list whose owner is
/// inactive, with no members and none of its facts. Reporting its stored
/// state instead would present a `ready` list with an empty membership,
/// which reads as a list known to be empty. `any_owner` is for a caller
/// that applies its own rule to hidden owners and wants the stored state.
pub fn list_report(info: Option<&ListInfo>, any_owner: bool) -> ListReport {
    let Some(info) = info else {
        return ListReport {
            state: TrackState::Untracked,
            serves: false,
            facts: false,
        };
    };
    let hidden = info.owner_status.is_hidden();
    if hidden && !any_owner {
        return ListReport {
            state: TrackState::Unavailable,
            serves: false,
            facts: false,
        };
    }
    let state = info.reported_state();
    ListReport {
        state,
        serves: matches!(state, TrackState::Ready | TrackState::Retained) && !hidden,
        facts: true,
    }
}

/// The state a list's coverage is worked out from. Coverage describes
/// the members a response returns, and a hidden owner's list returns
/// none under either rule of [`list_report`]: its coverage is that of an
/// `unavailable` list even where its stored state is the one reported.
/// "Complete" over members that are withheld would read as a list known
/// to be empty.
pub fn scope_state(info: Option<&ListInfo>, reported: TrackState) -> TrackState {
    if info.is_some_and(|i| i.owner_status.is_hidden()) {
        TrackState::Unavailable
    } else {
        reported
    }
}

/// List scope: the coverage of one list's membership, from the state it
/// is reported in ([`list_report`]) or, for a hidden owner's list, from
/// [`scope_state`]. `info` is `None` for a list Farsight
/// has no row for. A `ready` or `retained` list is complete since its
/// fetch, unless the firehose has not covered the time since or the
/// session lacks sync events; a list that is not tracked falls back to
/// network scope for `listblock`.
pub fn list_scope(v: &View<'_>, info: Option<&ListInfo>, state: TrackState) -> Cov {
    let a = v.applied_through();
    let base = |level| Cov {
        level,
        complete_since: None,
        reasons: BTreeSet::new(),
        indexed_at: a,
    };
    match (state, info) {
        (TrackState::Ready | TrackState::Retained, Some(info)) => {
            let mut c = base(Level::Complete);
            c.complete_since = info.fetched_witness;
            if !v.covered(info.fetched_witness) {
                c.partial(v.uncovered_reason());
            }
            if !on_v2(v.snap) {
                c.partial("sync_events_unavailable");
            }
            if info.capped {
                c.note("list_capped");
            }
            c
        }
        (TrackState::Pending, _) => {
            let mut c = base(Level::Complete);
            c.partial("list_pending");
            c
        }
        (TrackState::Unavailable, _) => {
            let mut c = base(Level::Complete);
            c.note("list_unavailable");
            c
        }
        (TrackState::Missing | TrackState::Dead, _) => {
            let mut c = base(Level::Complete);
            c.note("list_missing");
            c
        }
        (TrackState::Deferred, _) => {
            let mut c = base(Level::Complete);
            c.note("list_deferred");
            c
        }
        // A served state without a row does not occur; it claims no more
        // than a list that is not tracked.
        (TrackState::Untracked | TrackState::Purging, _)
        | (TrackState::Ready | TrackState::Retained, None) => {
            let mut n = v.network(Collection::ListBlock);
            n.note("list_not_tracked");
            n
        }
    }
}

/// `query.getListMembers`.
pub async fn get_list_members(st: &Arc<ApiState>, p: &Params) -> Result<Reply, XrpcError> {
    list_members(st, p, false).await
}

/// [`get_list_members`] for the pages of the web UI, which decide for
/// themselves what to show of a list whose owner is hidden: such a list
/// keeps its stored state and its facts here, and still returns no
/// members.
pub async fn get_list_members_any_owner(
    st: &Arc<ApiState>,
    p: &Params,
) -> Result<Reply, XrpcError> {
    list_members(st, p, true).await
}

async fn list_members(st: &Arc<ApiState>, p: &Params, any_owner: bool) -> Result<Reply, XrpcError> {
    let raw = p
        .get("list")
        .ok_or_else(|| XrpcError::invalid("missing required parameter `list`"))?;
    let list = AtUri::parse(raw).map_err(|e| XrpcError::invalid(format!("`list`: {e}")))?;
    if list.indexed_collection() != Some(Collection::List) {
        return Err(XrpcError::invalid(
            "`list` must be an app.bsky.graph.list record URI",
        ));
    }
    let limit = p.limit()?;
    let after = cursor::id_rkey(p.get("cursor"))?;
    let snap = snapshot(st)?;
    let v = view(st, &snap);
    let mut tx = st.read_tx().await?;
    harness_sleep(&mut tx, p).await?;
    let info = queries::list_info(&mut tx, &list.authority, list.rkey.as_str()).await?;
    let report = list_report(info.as_ref(), any_owner);
    let state = report.state;
    let members = match (&info, report.serves) {
        (Some(i), true) => {
            queries::list_members(
                &mut tx,
                i.id,
                after.as_ref().map(|(s, r)| (ActorId::new(*s), r.as_str())),
                limit,
            )
            .await?
        }
        _ => Vec::new(),
    };
    tx.rollback().await?;
    let cov = list_scope(&v, info.as_ref(), scope_state(info.as_ref(), state));
    // The list's own facts, unless its owner hides it.
    let facts = info.as_ref().filter(|_| report.facts);
    let mut body = Map::new();
    body.insert("list".into(), json!(raw));
    body.insert("state".into(), json!(state.api_name()));
    body.insert("capped".into(), json!(facts.is_some_and(|i| i.capped)));
    if let Some(i) = facts {
        if i.record_state == RecordState::Present {
            body.insert("purpose".into(), json!(purpose_name(i.purpose)));
        }
        insert_opt(&mut body, "name", i.name.clone());
    }
    body.insert(
        "listblockCount".into(),
        json!(facts.map_or(0, |i| i.listblock_count)),
    );
    let owner = list.authority.as_str().to_owned();
    body.insert(
        "members".into(),
        Value::Array(
            members
                .iter()
                .map(|m| {
                    let mut o = Map::new();
                    o.insert("did".into(), json!(m.did));
                    o.insert(
                        "itemUri".into(),
                        json!(uri(&owner, Collection::ListItem, &m.item_rkey)),
                    );
                    insert_opt(&mut o, "addedAt", opt_ts(m.added_at));
                    Value::Object(o)
                })
                .collect(),
        ),
    );
    if members.len() as i64 == limit {
        let last = members.last().expect("non-empty");
        body.insert(
            "cursor".into(),
            json!(cursor::encode(&[
                json!(last.subject_id.get()),
                json!(last.item_rkey)
            ])),
        );
    }
    body.insert(
        "freshness".into(),
        v.render(&cov, Utc::now(), st.source_lag()),
    );
    Ok(Reply::ok(Value::Object(body)))
}

/// Whether a listblocked list of a party bears on coverage: its record
/// is not deleted and its owner is shown.
fn list_relevant(l: &queries::PartyList, inc: bool) -> bool {
    l.record_state != RecordState::Deleted && (inc || !l.owner_status.is_hidden())
}

/// The X side of `checkBlocks` at response level: network or subject
/// scope for blocks and the list chain, X's last clean listing while the
/// network is not complete, X's debts, and the state of every list X
/// listblocks. `rows` must hold X's listblocks (`check_rows`). The public
/// UI reads the same value for an account's outgoing blocks.
pub(crate) fn actor_side_coverage(
    v: &View<'_>,
    ac: Option<&ActorCoverage>,
    x_id: ActorId,
    x_debt: bool,
    rows: &queries::CheckRows,
    inc: bool,
) -> Cov {
    let (cb, _) = v.network_or_subject(Collection::Block, ac, SubjectKind::Block);
    let (cl, _) = v.network_or_subject(Collection::ListBlock, ac, SubjectKind::ListChain);
    let net_complete = v.network(Collection::Block).level == Level::Complete
        && v.network(Collection::ListBlock).level == Level::Complete;
    let mut c = cb.combine(cl);
    if !net_complete && !v.covered(ac.and_then(|a| a.clean_witness)) {
        c.partial("sweep_incomplete");
    }
    if x_debt {
        c.partial("party_debt");
    }
    for l in rows
        .listblocks
        .get(&x_id)
        .into_iter()
        .flatten()
        .filter_map(|l| rows.lists.get(l))
        .filter(|l| list_relevant(l, inc))
    {
        if l.capped && l.track_state.is_tracked() {
            c.partial("list_capped");
        }
        if let Some(r) = list_state_reason(l.track_state, l.readmits()) {
            c.partial(r);
        }
        if matches!(l.track_state, TrackState::Ready | TrackState::Retained)
            && !v.covered(l.fetched_witness)
        {
            c.partial(v.uncovered_reason());
        }
    }
    c
}

/// Maximum `others` per `checkBlocks` call.
pub const MAX_OTHERS: usize = 100;

/// `query.checkBlocks` (per-result coverage).
pub async fn check_blocks(st: &Arc<ApiState>, p: &Params) -> Result<Reply, XrpcError> {
    let x = p.did("actor")?;
    let raw_others = p.all("others");
    if raw_others.is_empty() {
        return Err(XrpcError::invalid("missing required parameter `others`"));
    }
    if raw_others.len() > MAX_OTHERS {
        return Err(XrpcError::invalid(format!(
            "`others` takes at most {MAX_OTHERS} DIDs"
        )));
    }
    let mut others: Vec<String> = Vec::new();
    for o in raw_others {
        let d = crate::params::parse_did("others", o)?;
        if !others.iter().any(|e| e == d.as_str()) {
            others.push(d.into_string());
        }
    }
    let inc = p.bool("includeInactive")?;
    let snap = snapshot(st)?;
    let v = view(st, &snap);
    let mut tx = st.read_tx().await?;
    harness_sleep(&mut tx, p).await?;
    let xa = queries::actor(&mut tx, &x).await?;
    let oa = queries::actors(&mut tx, &others).await?;
    let x_id = xa.map_or(NO_ACTOR, |a| a.id);
    let other_ids: Vec<ActorId> = others
        .iter()
        .filter_map(|d| oa.get(d).map(|a| a.id))
        .collect();
    let rows = queries::check_rows(&mut tx, x_id, &other_ids).await?;
    let mut party_ids = other_ids.clone();
    if xa.is_some() {
        party_ids.push(x_id);
    }
    let debts = farsight_storage::debts::debts_for(&mut *tx, &party_ids).await?;
    let ac = match xa {
        Some(a) => Some(queries::actor_coverage(&mut tx, a.id).await?),
        None => None,
    };
    tx.rollback().await?;

    let hidden = |id: ActorId| {
        rows.status
            .get(&id)
            .copied()
            .unwrap_or(ActorStatus::Active)
            .is_hidden()
    };
    let direct = |a: ActorId, s: ActorId| {
        rows.direct.iter().any(|(au, su, _)| *au == a && *su == s) && (inc || !hidden(a))
    };
    let names = |l: ListId, s: ActorId| rows.items.iter().any(|(li, si)| *li == l && *si == s);
    let empty = Vec::new();
    let lists_of = |a: ActorId| rows.listblocks.get(&a).unwrap_or(&empty);
    let list_uris = |a: ActorId, s: ActorId| -> Vec<String> {
        if !inc && hidden(a) {
            return Vec::new();
        }
        let mut v: Vec<String> = lists_of(a)
            .iter()
            .filter_map(|l| rows.lists.get(l))
            .filter(|l| l.blocks(inc) && names(l.id, s))
            .map(|l| uri(&l.owner_did, Collection::List, &l.rkey))
            .collect();
        v.sort();
        v
    };

    let mut results = Vec::new();
    for d in &others {
        let Some(o) = oa.get(d) else { continue };
        let (o_x_direct, o_x_lists) = (direct(o.id, x_id), list_uris(o.id, x_id));
        let (x_o_direct, x_o_lists) = (direct(x_id, o.id), list_uris(x_id, o.id));
        if o_x_direct || x_o_direct || !o_x_lists.is_empty() || !x_o_lists.is_empty() {
            results.push(json!({
                "did": d,
                "blocksActor": { "direct": o_x_direct, "lists": o_x_lists },
                "blockedByActor": { "direct": x_o_direct, "lists": x_o_lists },
            }));
        }
    }

    // Response-level coverage: what is common to every pair.
    let x_debt = xa.is_some() && debts.get(&x_id).is_some_and(|d| !d.is_empty());
    let c = actor_side_coverage(&v, ac.as_ref(), x_id, x_debt, &rows, inc);
    let relevant = |l: &queries::PartyList| list_relevant(l, inc);
    // Per pair.
    let mut partial_for = Vec::new();
    for d in &others {
        let Some(o) = oa.get(d) else { continue };
        let mut reasons: BTreeSet<&'static str> = BTreeSet::new();
        if debts.get(&o.id).is_some_and(|d| !d.is_empty()) {
            reasons.insert("party_debt");
        }
        for l in lists_of(o.id)
            .iter()
            .filter_map(|l| rows.lists.get(l))
            .filter(|l| relevant(l))
        {
            if l.capped && l.track_state.is_tracked() {
                reasons.insert("list_capped");
            }
            if let Some(r) = list_state_reason(l.track_state, l.readmits()) {
                reasons.insert(r);
            }
            if matches!(l.track_state, TrackState::Ready | TrackState::Retained)
                && names(l.id, x_id)
                && !v.covered(l.fetched_witness)
            {
                reasons.insert(v.uncovered_reason());
            }
        }
        if !reasons.is_empty() {
            partial_for
                .push(json!({ "did": d, "reasons": reasons.into_iter().collect::<Vec<_>>() }));
        }
    }
    let body = json!({
        "actor": x.as_str(),
        "results": results,
        "partialFor": partial_for,
        "freshness": v.render(&c, Utc::now(), st.source_lag()),
    });
    Ok(Reply::ok(body))
}

/// `backfill.sweep.state` in `getStats`: where the latest cycle stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepState {
    /// The cycle has finished.
    Completed,
    /// The cycle is open and `backfill.sweep.enabled` is off.
    Paused,
    /// The cycle is open and the sweep is on.
    Running,
}

impl SweepState {
    /// The state as `backfill.sweep.state` spells it.
    pub const fn api_name(self) -> &'static str {
        match self {
            SweepState::Completed => "completed",
            SweepState::Paused => "paused",
            SweepState::Running => "running",
        }
    }
}

/// `query.getStats`.
pub async fn get_stats(st: &Arc<ApiState>, p: &Params) -> Result<Reply, XrpcError> {
    let snap = snapshot(st)?;
    let v = view(st, &snap);
    let cfg = st.config.current();
    let mut tx = st.read_tx().await?;
    harness_sleep(&mut tx, p).await?;
    let counts = queries::counts(&mut tx).await?;
    let bf = queries::backfill_overview(&mut tx).await?;
    tx.rollback().await?;
    let now = Utc::now();
    let mut firehose = Map::new();
    firehose.insert("connected".into(), json!(snap.firehose.connected));
    if let Some(p) = snap.firehose.protocol {
        firehose.insert("protocol".into(), json!(p.api_name()));
    }
    if let Some(a) = snap.firehose.applied_through {
        firehose.insert("lagSeconds".into(), json!(crate::freshness::secs(now - a)));
    }
    if let Some(l) = st.source_lag() {
        firehose.insert("sourceLagSeconds".into(), json!(l));
    }
    firehose.insert(
        "openGaps".into(),
        json!(
            snap.gaps
                .iter()
                .filter(|g| g.healed_witness.is_none())
                .count()
        ),
    );
    let mut backfill = Map::new();
    if let Some(c) = &bf.cycle {
        let mut s = Map::new();
        s.insert("cycle".into(), json!(c.id.get()));
        s.insert("source".into(), json!(c.source.as_str()));
        let state = if c.completed_at.is_some() {
            SweepState::Completed
        } else if !cfg.config.backfill.sweep.enabled {
            SweepState::Paused
        } else {
            SweepState::Running
        };
        s.insert("state".into(), json!(state.api_name()));
        if let Some(total) = c.total_est.filter(|t| *t > 0) {
            s.insert(
                "progress".into(),
                json!(((c.done as f64 / total as f64).min(1.0) * 1000.0).round() / 1000.0),
            );
            if c.completed_at.is_none() && bf.repos_last_hour > 0 {
                let remaining = (total - c.done).max(0);
                s.insert(
                    "etaSeconds".into(),
                    json!(remaining * 3600 / bf.repos_last_hour),
                );
            }
        }
        insert_opt(&mut s, "completedAt", opt_ts(c.completed_at));
        backfill.insert("sweep".into(), Value::Object(s));
    }
    backfill.insert(
        "queue".into(),
        json!({ "onDemand": bf.queue_by_tier[0], "active": bf.queue_by_tier[1] }),
    );
    backfill.insert("reposPerHour".into(), json!(bf.repos_last_hour));
    let cov = v.global();
    let detail = json!({
        "exceptions": v.exceptions(),
        "queueByTier": bf.queue_by_tier,
        "storageRefusalActive": snap.storage_refusal_active,
        "pendingListsConsidered": snap.pending.considered,
        "gaps": snap.gaps.iter().filter(|g| g.healed_witness.is_none()).map(|g| json!({
            "from": ts(g.from_at),
            "to": g.to_at.map(ts),
            "cause": g.cause.api_name(),
        })).collect::<Vec<_>>(),
    });
    let body = json!({
        "service": {
            "version": st.version,
            "hostname": cfg.config.server.hostname,
            "contact": cfg.config.server.contact,
        },
        "counts": {
            "blocks": counts.blocks,
            "listBlocks": counts.list_blocks,
            "lists": counts.lists,
            "trackedLists": counts.tracked_lists,
            "listItems": counts.list_items,
            "actors": counts.actors,
        },
        "firehose": Value::Object(firehose),
        "backfill": Value::Object(backfill),
        "freshness": v.render(&cov, now, st.source_lag()),
        "detail": detail,
    });
    Ok(Reply::ok(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(track_state: TrackState, owner_status: ActorStatus) -> ListInfo {
        ListInfo {
            id: ListId::new(1),
            owner_did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(),
            owner_status,
            rkey: "3k2abc".into(),
            record_state: RecordState::Present,
            purpose: Some(ListPurpose::Mod),
            name: Some("a list".into()),
            listblock_count: 7,
            track_state,
            capped: false,
            item_count: 3,
            fetched_witness: None,
            purge_then: None,
            admitted_at: None,
        }
    }

    #[test]
    fn a_hidden_owner_hides_the_list() {
        for status in ActorStatus::ALL {
            for state in TrackState::ALL {
                let i = info(*state, *status);
                let r = list_report(Some(&i), false);
                if status.is_hidden() {
                    // Never `ready` with no members, which would read as
                    // a list known to be empty; and none of its facts.
                    assert_eq!(
                        r,
                        ListReport {
                            state: TrackState::Unavailable,
                            serves: false,
                            facts: false
                        },
                        "{status:?} {state:?}"
                    );
                } else {
                    assert_eq!(r.state, i.reported_state());
                    assert!(r.facts);
                    assert_eq!(
                        r.serves,
                        matches!(state, TrackState::Ready | TrackState::Retained)
                    );
                }
                // A page that applies its own rule gets the stored state,
                // and members only where the API would serve them.
                let own = list_report(Some(&i), true);
                assert_eq!(own.state, i.reported_state());
                assert!(own.facts);
                assert_eq!(own.serves, r.serves);
                // Under either rule the coverage is that of the members
                // returned: of an unavailable list when the owner is
                // hidden, never "complete" over members that are withheld.
                for reported in [r.state, own.state] {
                    let scoped = scope_state(Some(&i), reported);
                    if status.is_hidden() {
                        assert_eq!(scoped, TrackState::Unavailable, "{status:?} {state:?}");
                    } else {
                        assert_eq!(scoped, reported);
                    }
                }
            }
        }
        assert_eq!(
            scope_state(None, TrackState::Untracked),
            TrackState::Untracked
        );
        assert_eq!(
            list_report(None, false),
            ListReport {
                state: TrackState::Untracked,
                serves: false,
                facts: false
            }
        );
    }
}
