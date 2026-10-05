//! The admin history pages (design §7.7, §7.8, §8.6):
//! `/admin/did/{did}/history` and `/admin/list/{did}/{rkey}/history`.
//!
//! They show the records this instance stored and later removed. They
//! need an admin session, whether or not the public UI
//! is on; no public page shows or links to them.
//!
//! Rules kept here:
//!
//! - **The display filters of §7.7 and §7.8 bind every surface**, this one
//!   included: a row whose author or list owner is hidden is not shown.
//!   That, not the purge, is what keeps a deleted account's history from
//!   being shown; an admin session does not lift it.
//!   `public_ui.excluded_dids` is not applied: it governs the public pages.
//! - **Nothing is resolved while rendering.** An account cell shows a
//!   handle when the cache holds a verified one and opens a profile card;
//!   accounts shown as DIDs are handed to the warming worker.
//! - **Record cells are text.** The records were removed; a viewer link
//!   would open "not found".
//! - History is outside the coverage contract (§3.7): the pages print no
//!   coverage and state their own limits.

use std::collections::HashMap;
use std::sync::Arc;

use askama::Template;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use farsight_api::cursor;
use farsight_api::params::Params;
use farsight_core::{Did, RecordKey};
use farsight_storage::codes::{TrackState, actor_status};
use farsight_storage::history::Cause;
use farsight_storage::public::{self as store, HistoryArgs, HistoryCursor, Removed};
use farsight_storage::queries::{self, ActorRef};
use serde_json::json;

use crate::cells::{self, Account, lookup_did_href};
use crate::common::render_private;
use crate::pages::{Nav, WebState, gate, message, nav, permit};
use crate::public::pages::{purpose_words, state_words};
use crate::public::text::{
    BLOCK, LISTBLOCK, LISTITEM, Stamp, admin_did_history_href, admin_list_history_href, clean,
    duration_words, list_uri,
};
use crate::public::warming::Asked;
use crate::public::{MAX_DID_SEGMENT, PAGE_ROWS};

/// The cursor parameters of the two pages. Cursors are opaque and
/// unstable.
const DID_CURSORS: [&str; 2] = ["hb", "hm"];
const LIST_CURSORS: [&str; 2] = ["hl", "hm"];

/// The admin UI's time format.
fn when(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

/// `/admin/lookup/list?q=at://…`.
fn lookup_list_href(owner: &str, rkey: &str) -> String {
    let q: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("q", &list_uri(owner, rkey))
        .finish();
    format!("/admin/lookup/list?{q}")
}

/// The list a removed membership or listblock pointed at.
#[derive(Debug, Clone)]
pub struct ListCell {
    /// at-uri, built from the owner's DID and the list's key.
    pub uri: String,
    /// Its lookup page.
    pub href: String,
    /// Name, when the list row has one.
    pub name: Option<String>,
    /// Purpose in words, when the record is present.
    pub purpose: Option<&'static str>,
    /// The list's state in words, if a row exists.
    pub state: Option<String>,
}

/// One removed record as a history page shows it.
#[derive(Debug, Clone)]
pub struct RemovedRow {
    /// The other account, for rows that name one.
    pub who: Option<Account>,
    /// The list, for rows that point at one.
    pub list: Option<ListCell>,
    /// The removed record's at-uri. Always text: the record is gone.
    pub record: String,
    /// Created, as stated by the author.
    pub created: Option<String>,
    /// First seen; `None`: before this instance kept dates.
    pub first_seen: Option<String>,
    /// Last seen.
    pub last_seen: Option<String>,
    /// Removed.
    pub removed: String,
    /// The same four as instants, for a page whose script writes times
    /// in the viewer's zone.
    pub created_at: Option<Stamp>,
    /// First seen, as an instant.
    pub first_seen_at: Option<Stamp>,
    /// Last seen, as an instant.
    pub last_seen_at: Option<Stamp>,
    /// Removed, as an instant.
    pub removed_at: Stamp,
    /// How it ended.
    pub cause: &'static str,
    /// "blocks this account again" / "on this list now".
    pub mark: Option<&'static str>,
}

/// A history section.
#[derive(Debug, Clone)]
pub struct HistorySection {
    /// Rows shown.
    pub rows: Vec<RemovedRow>,
    /// Next link.
    pub pager: Pager,
    /// "No removals recorded.": only on a first page with nothing after it.
    pub empty: bool,
}

/// The "next" link of a history section: a plain link that htmx upgrades
/// to an in-place swap of the section. The response is always the full
/// page. (The public tables turn by page number; these pages keep their
/// cursors.)
#[derive(Debug, Clone, Template)]
#[template(
    source = r##"{% if let Some(n) = next %}<p class="pager"><a href="{{ n }}#{{ section }}" hx-get="{{ n }}" hx-select="#{{ section }}" hx-target="#{{ section }}" hx-swap="outerHTML" hx-push-url="true" rel="nofollow">Next</a></p>{% endif %}"##,
    ext = "html"
)]
pub struct Pager {
    /// The section's fragment id.
    pub section: &'static str,
    /// Target without the fragment, if there is a next page.
    pub next: Option<String>,
}

/// `base?…` keeping the other sections' positions and setting `key`.
fn next_link(base: &str, q: &Params, keys: &[&str], key: &str, value: &str) -> String {
    let mut s = url::form_urlencoded::Serializer::new(String::new());
    for k in keys {
        if *k != key {
            if let Some(v) = q.get(k) {
                s.append_pair(k, v);
            }
        }
    }
    s.append_pair(key, value);
    format!("{base}?{}", s.finish())
}

/// What kind of record a history section lists (the wording of causes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Direct blocks.
    Block,
    /// Listblocks.
    ListBlock,
    /// List memberships.
    Membership,
}

/// How a removal is worded.
pub fn cause_words(kind: Kind, cause: i16) -> &'static str {
    use Cause::*;
    match (kind, Cause::from_code(cause)) {
        (Kind::Block, Some(Delete)) => "Block deleted.",
        (Kind::Block, Some(SubjectChange)) => "Record changed to block a different account.",
        (Kind::ListBlock, Some(Delete)) => "Listblock deleted.",
        (Kind::ListBlock, Some(SubjectChange)) => "Record changed to block a different list.",
        (Kind::Block | Kind::ListBlock, Some(RefusedUpdate)) => {
            "Record changed; the new version was not stored."
        }
        (Kind::Block | Kind::ListBlock, Some(Reconcile)) => {
            "Found missing when the author's records were re-read. Removed some time between \
             'last seen' and this time."
        }
        (Kind::Membership, Some(Delete)) => "Removed from the list.",
        (Kind::Membership, Some(SubjectChange)) => {
            "List entry changed to name another account or list."
        }
        (Kind::Membership, Some(RefusedUpdate)) => {
            "List entry changed; the new version was not stored."
        }
        (Kind::Membership, Some(Reconcile)) => {
            "Found missing when the owner's records were re-read. Removed some time between \
             'last seen' and this time."
        }
        (Kind::Membership, Some(ListDeleted)) => "The list was deleted.",
        _ => "Removed.",
    }
}

/// How a removed record is marked when the same pair has a live row now:
/// a row is a removed record, not a statement about the present.
fn live_mark(kind: Kind) -> &'static str {
    match kind {
        Kind::Block => "blocks this account again",
        Kind::ListBlock => "blocks this list again",
        Kind::Membership => "on this list now",
    }
}

fn list_state_words(s: TrackState) -> String {
    state_words(s.api_name()).0
}

fn purpose_of_code(c: Option<i16>) -> Option<&'static str> {
    c.map(|c| purpose_words(farsight_core::ListPurpose::from_code(c).api_name()))
}

/// The at-uri of a removed record. A block or a listblock is in its
/// author's repo — the row's account. A listitem is in the repo of the
/// list's owner: the row's list on an account's page, `owner` on a list's
/// page.
fn record_uri(kind: Kind, h: &Removed, owner: Option<&str>) -> String {
    let (authority, collection) = match kind {
        Kind::Block => (h.party.as_str(), BLOCK),
        Kind::ListBlock => (h.party.as_str(), LISTBLOCK),
        Kind::Membership => (
            h.list
                .as_ref()
                .map(|l| l.owner_did.as_str())
                .or(owner)
                .unwrap_or(""),
            LISTITEM,
        ),
    };
    format!("at://{authority}/{collection}/{}", h.rkey)
}

fn removed_row(
    st: &WebState,
    asked: &mut Asked,
    kind: Kind,
    h: &Removed,
    owner: Option<&str>,
) -> RemovedRow {
    RemovedRow {
        // These pages are served to a signed-in admin only.
        who: (!h.party.is_empty()).then(|| cells::account(st, asked, &h.party)),
        record: record_uri(kind, h, owner),
        list: h.list.as_ref().map(|l| ListCell {
            uri: list_uri(&l.owner_did, &l.rkey),
            href: lookup_list_href(&l.owner_did, &l.rkey),
            name: l.name.as_deref().map(clean).filter(|n| !n.is_empty()),
            purpose: purpose_of_code(l.purpose),
            state: l.state.map(list_state_words),
        }),
        created: h.created_at.map(when),
        first_seen: h.first_seen.map(when),
        last_seen: h.last_seen.map(when),
        removed: when(h.removed_at),
        created_at: h.created_at.map(Stamp::of),
        first_seen_at: h.first_seen.map(Stamp::of),
        last_seen_at: h.last_seen.map(Stamp::of),
        removed_at: Stamp::of(h.removed_at),
        cause: cause_words(kind, h.cause),
        mark: h.live.then(|| live_mark(kind)),
    }
}

/// A cursor that cannot be read.
struct BadCursor;

fn history_cursor(q: &Params, key: &str) -> Result<Option<HistoryCursor>, BadCursor> {
    let Some((micros, id)) = cursor::micros_id(q.get(key)).map_err(|_| BadCursor)? else {
        return Ok(None);
    };
    let removed_at = DateTime::<Utc>::from_timestamp_micros(micros).ok_or(BadCursor)?;
    Ok(Some(HistoryCursor { removed_at, id }))
}

fn encode_history_cursor(c: HistoryCursor) -> String {
    cursor::encode(&[json!(c.removed_at.timestamp_micros()), json!(c.id)])
}

/// One recording window, clipped to the retention horizon.
#[derive(Debug, Clone)]
pub struct Window {
    /// Start.
    pub from: String,
    /// End; `None` for the open window.
    pub to: Option<String>,
}

/// What a history page says about itself (`#limits`).
#[derive(Debug, Clone, Template)]
#[template(path = "_history_limits.html")]
pub struct HistoryLimits {
    /// Recording windows, oldest first.
    pub windows: Vec<Window>,
    /// A window is open.
    pub recording: bool,
    /// The retention sentence.
    pub retention: String,
}

/// "Removals older than 365 days are deleted." / kept indefinitely.
pub fn retention_words(cfg: &farsight_core::Config) -> String {
    let d = cfg.storage.block_history_retention.get();
    if d.is_zero() {
        "Removals are kept indefinitely.".to_owned()
    } else {
        format!("Removals older than {} are deleted.", duration_words(d))
    }
}

/// The retention horizon: nothing before it is shown or claimed.
fn horizon(cfg: &farsight_core::Config, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let d = cfg.storage.block_history_retention.get();
    if d.is_zero() {
        None
    } else {
        chrono::Duration::from_std(d).ok().map(|d| now - d)
    }
}

/// Clips the recording windows to the horizon: a window that closed
/// before it is dropped, one that straddles it starts at it.
pub fn clip_windows(
    windows: &[(DateTime<Utc>, Option<DateTime<Utc>>)],
    horizon: Option<DateTime<Utc>>,
) -> Vec<(DateTime<Utc>, Option<DateTime<Utc>>)> {
    windows
        .iter()
        .filter(|(_, to)| match (to, horizon) {
            (Some(t), Some(h)) => *t >= h,
            _ => true,
        })
        .map(|(from, to)| {
            (
                match horizon {
                    Some(h) if *from < h => h,
                    _ => *from,
                },
                *to,
            )
        })
        .collect()
}

struct HistoryData {
    first: Vec<Removed>,
    second: Vec<Removed>,
    windows: Vec<(DateTime<Utc>, Option<DateTime<Utc>>)>,
    /// Status of every DID the rows name (the hidden filter).
    status: HashMap<String, ActorRef>,
}

impl HistoryData {
    /// Whether a row naming `did`, as author, list owner or member, may be
    /// shown.
    fn ok(&self, did: &str) -> bool {
        !self
            .status
            .get(did)
            .is_some_and(|a| actor_status::is_hidden(a.status))
    }
}

fn limits(
    cfg: &farsight_core::Config,
    d: &HistoryData,
    hz: Option<DateTime<Utc>>,
) -> HistoryLimits {
    let windows = clip_windows(&d.windows, hz);
    HistoryLimits {
        recording: windows.iter().any(|(_, to)| to.is_none()),
        windows: windows
            .into_iter()
            .map(|(from, to)| Window {
                from: when(from),
                to: to.map(when),
            })
            .collect(),
        retention: retention_words(cfg),
    }
}

#[allow(clippy::too_many_arguments)]
fn history_section(
    st: &WebState,
    asked: &mut Asked,
    owner: Option<&str>,
    data: &HistoryData,
    kind: Kind,
    rows: &[Removed],
    base: &str,
    q: &Params,
    keys: &[&str],
    key: &'static str,
    section: &'static str,
) -> HistorySection {
    // The cursor comes from the last row read, not the last row shown: a
    // short page still offers "next".
    let next = store::next_cursor(rows, PAGE_ROWS)
        .map(|c| next_link(base, q, keys, key, &encode_history_cursor(c)));
    let out: Vec<RemovedRow> = rows
        .iter()
        .filter(|h| {
            (h.party.is_empty() || data.ok(&h.party))
                && h.list.as_ref().is_none_or(|l| data.ok(&l.owner_did))
        })
        .map(|h| removed_row(st, asked, kind, h, owner))
        .collect();
    HistorySection {
        empty: out.is_empty() && next.is_none() && q.get(key).is_none(),
        rows: out,
        pager: Pager { section, next },
    }
}

fn history_dids(first: &[Removed], second: &[Removed]) -> Vec<String> {
    first
        .iter()
        .chain(second)
        .flat_map(|h| {
            [
                (!h.party.is_empty()).then(|| h.party.clone()),
                h.list.as_ref().map(|l| l.owner_did.clone()),
            ]
        })
        .flatten()
        .collect()
}

/// Which page.
enum Subject<'a> {
    Did(&'a Did),
    List(&'a Did, &'a RecordKey),
}

async fn load(
    st: &WebState,
    subject: &Subject<'_>,
    a: Option<HistoryCursor>,
    b: Option<HistoryCursor>,
    hz: Option<DateTime<Utc>>,
) -> Result<HistoryData, farsight_api::error::XrpcError> {
    let args = |after| HistoryArgs {
        // `excluded_dids` governs the public pages only.
        excluded: &[],
        horizon: hz,
        after,
        limit: PAGE_ROWS,
    };
    let mut tx = st.api.read_tx().await?;
    let (did, rkey) = match subject {
        Subject::Did(d) => (*d, None),
        Subject::List(d, k) => (*d, Some(*k)),
    };
    // Works from the `actors` id (and the list's key), so a deleted list
    // keeps its history without a `lists` row.
    let actor = queries::actor(&mut tx, did.as_str()).await?;
    let (first, second) = match (actor, rkey) {
        (None, _) => (Vec::new(), Vec::new()),
        (Some(x), None) => (
            store::blocks_history_by_subject(&mut tx, x.id, args(a)).await?,
            store::list_items_history_by_subject(&mut tx, x.id, args(b)).await?,
        ),
        // A list's rows are all authored by, or point at a list of, its
        // owner: a hidden owner shows nothing.
        (Some(x), Some(_)) if actor_status::is_hidden(x.status) => (Vec::new(), Vec::new()),
        (Some(x), Some(k)) => (
            store::list_blocks_history_by_list(&mut tx, x.id, k.as_str(), args(a)).await?,
            store::list_items_history_by_list(&mut tx, x.id, k.as_str(), args(b)).await?,
        ),
    };
    let windows = store::history_windows(&mut tx).await?;
    let dids = history_dids(&first, &second);
    let status = if dids.is_empty() {
        HashMap::new()
    } else {
        queries::actors(&mut tx, &dids).await?
    };
    tx.rollback().await?;
    Ok(HistoryData {
        first,
        second,
        windows,
        status,
    })
}

/// The removed records naming an account, for the History tab of the
/// DID lookup page: the two sections of the account's history page, their
/// "next" links leading back into the tab (`lead` is the lookup page's
/// address with the tab chosen, ending in `&`).
pub(crate) struct DidRemoved {
    /// Removed blocks.
    pub blocks: HistorySection,
    /// Removed list memberships.
    pub memberships: HistorySection,
}

/// Reads [`DidRemoved`]. `q` carries the sections' cursors (`hb`, `hm`).
/// `Err`: what to say in the tab instead.
pub(crate) async fn did_removed(
    st: &WebState,
    did: &Did,
    q: &Params,
    lead: &str,
) -> Result<DidRemoved, &'static str> {
    let (Ok(a), Ok(b)) = (
        history_cursor(q, DID_CURSORS[0]),
        history_cursor(q, DID_CURSORS[1]),
    ) else {
        return Err("This link carries a position that can no longer be read. Open the tab again.");
    };
    let cfg = st.api.config.current();
    let cfg = &cfg.config;
    let hz = horizon(cfg, Utc::now());
    let data = match load(st, &Subject::Did(did), a, b, hz).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(error = %e.message, "history tab query failed");
            return Err("The removed records could not be read. The details were logged.");
        }
    };
    let parties: Vec<String> = data
        .first
        .iter()
        .chain(&data.second)
        .filter(|h| !h.party.is_empty())
        .map(|h| h.party.clone())
        .collect();
    crate::public::handles::recall(st, cfg, &parties).await;
    let mut asked = Asked::new(cfg);
    // The sections' links are made for the history page's address; here
    // they continue the lookup page's.
    let into_tab = |mut sec: HistorySection| {
        sec.pager.next = sec
            .pager
            .next
            .and_then(|n| n.split_once('?').map(|(_, rest)| format!("{lead}{rest}")));
        sec
    };
    let base = admin_did_history_href(did.as_str());
    let out = DidRemoved {
        blocks: into_tab(history_section(
            st,
            &mut asked,
            None,
            &data,
            Kind::Block,
            &data.first,
            &base,
            q,
            &DID_CURSORS,
            "hb",
            "removed-blocks",
        )),
        memberships: into_tab(history_section(
            st,
            &mut asked,
            None,
            &data,
            Kind::Membership,
            &data.second,
            &base,
            q,
            &DID_CURSORS,
            "hm",
            "removed-memberships",
        )),
    };
    asked.submit(st);
    Ok(out)
}

#[derive(Template)]
#[template(path = "admin_did_history.html")]
struct DidHistoryPage {
    nav: Nav,
    did: String,
    back: String,
    blocks: HistorySection,
    memberships: HistorySection,
    limits: HistoryLimits,
}

#[derive(Template)]
#[template(path = "admin_list_history.html")]
struct ListHistoryPage {
    nav: Nav,
    uri: String,
    back: String,
    listblocks: HistorySection,
    members: HistorySection,
    limits: HistoryLimits,
}

fn parse_did(p: Result<Path<String>, PathRejection>) -> Option<Did> {
    let Path(s) = p.ok()?;
    if s.len() > MAX_DID_SEGMENT {
        return None;
    }
    Did::parse(&s).ok()
}

fn parse_list(p: Result<Path<(String, String)>, PathRejection>) -> Option<(Did, RecordKey)> {
    let Path((d, r)) = p.ok()?;
    if d.len() > MAX_DID_SEGMENT {
        return None;
    }
    Some((Did::parse(&d).ok()?, RecordKey::parse(&r).ok()?))
}

/// Shared flow of the two pages: the session gate, the cursors, the
/// queries. `render` builds the page from the data.
async fn serve(
    st: &Arc<WebState>,
    headers: &HeaderMap,
    subject: Option<Subject<'_>>,
    raw_query: Option<String>,
    keys: [&'static str; 2],
) -> Response {
    let admin = match gate(st, headers).await {
        Ok(s) => Some(s),
        Err(r) => return r,
    };
    let Some(subject) = subject else {
        return message(
            &admin,
            StatusCode::BAD_REQUEST,
            "That address cannot be read",
            "The address needs a DID (and, for a list, the list's record key).",
        );
    };
    let q = Params::parse(raw_query.as_deref().unwrap_or(""));
    let (Ok(a), Ok(b)) = (history_cursor(&q, keys[0]), history_cursor(&q, keys[1])) else {
        return message(
            &admin,
            StatusCode::BAD_REQUEST,
            "That address cannot be read",
            "This link carries a position that can no longer be read. Open the page again from \
             the lookup page.",
        );
    };
    let _permit = match permit(st).await {
        Ok(p) => p,
        Err(e) => {
            return message(&admin, StatusCode::SERVICE_UNAVAILABLE, "Busy", &e);
        }
    };
    let cfg = st.api.config.current();
    let cfg = &cfg.config;
    let hz = horizon(cfg, Utc::now());
    let data = match load(st, &subject, a, b, hz).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(error = %e.message, "history page query failed");
            return message(
                &admin,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Something went wrong",
                "The history could not be read. The details were logged.",
            );
        }
    };
    let nav = nav(&admin);
    let parties: Vec<String> = data
        .first
        .iter()
        .chain(&data.second)
        .filter(|h| !h.party.is_empty())
        .map(|h| h.party.clone())
        .collect();
    crate::public::handles::recall(st, cfg, &parties).await;
    let mut asked = Asked::new(cfg);
    let page = match subject {
        Subject::Did(did) => {
            let base = admin_did_history_href(did.as_str());
            render_private(&DidHistoryPage {
                nav,
                did: did.to_string(),
                back: lookup_did_href(did.as_str()),
                blocks: history_section(
                    st,
                    &mut asked,
                    None,
                    &data,
                    Kind::Block,
                    &data.first,
                    &base,
                    &q,
                    &DID_CURSORS,
                    "hb",
                    "removed-blocks",
                ),
                memberships: history_section(
                    st,
                    &mut asked,
                    None,
                    &data,
                    Kind::Membership,
                    &data.second,
                    &base,
                    &q,
                    &DID_CURSORS,
                    "hm",
                    "removed-memberships",
                ),
                limits: limits(cfg, &data, hz),
            })
        }
        Subject::List(owner, rkey) => {
            let base = admin_list_history_href(owner.as_str(), rkey.as_str());
            render_private(&ListHistoryPage {
                nav,
                uri: list_uri(owner.as_str(), rkey.as_str()),
                back: lookup_list_href(owner.as_str(), rkey.as_str()),
                listblocks: history_section(
                    st,
                    &mut asked,
                    Some(owner.as_str()),
                    &data,
                    Kind::ListBlock,
                    &data.first,
                    &base,
                    &q,
                    &LIST_CURSORS,
                    "hl",
                    "removed-listblocks",
                ),
                members: history_section(
                    st,
                    &mut asked,
                    Some(owner.as_str()),
                    &data,
                    Kind::Membership,
                    &data.second,
                    &base,
                    &q,
                    &LIST_CURSORS,
                    "hm",
                    "removed-members",
                ),
                limits: limits(cfg, &data, hz),
            })
        }
    };
    asked.submit(st);
    page
}

/// `GET /admin/did/{did}/history`.
pub async fn did_history(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    path: Result<Path<String>, PathRejection>,
    RawQuery(q): RawQuery,
) -> Response {
    let did = parse_did(path);
    serve(
        &st,
        &headers,
        did.as_ref().map(Subject::Did),
        q,
        DID_CURSORS,
    )
    .await
}

/// `GET /admin/list/{did}/{rkey}/history`.
pub async fn list_history(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    path: Result<Path<(String, String)>, PathRejection>,
    RawQuery(q): RawQuery,
) -> Response {
    let list = parse_list(path);
    serve(
        &st,
        &headers,
        list.as_ref().map(|(d, k)| Subject::List(d, k)),
        q,
        LIST_CURSORS,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn causes_are_worded_per_kind() {
        assert_eq!(cause_words(Kind::Block, 1), "Block deleted.");
        assert_eq!(cause_words(Kind::ListBlock, 1), "Listblock deleted.");
        assert_eq!(cause_words(Kind::Membership, 1), "Removed from the list.");
        assert_eq!(cause_words(Kind::Membership, 5), "The list was deleted.");
        assert!(cause_words(Kind::Block, 4).contains("author's records were re-read"));
        assert!(cause_words(Kind::Membership, 4).contains("owner's records were re-read"));
        assert_eq!(
            cause_words(Kind::ListBlock, 2),
            "Record changed to block a different list."
        );
        // `list_deleted` is a membership cause only; anything unknown is
        // still a removal.
        assert_eq!(cause_words(Kind::Block, 5), "Removed.");
        assert_eq!(cause_words(Kind::Block, 99), "Removed.");
    }

    #[test]
    fn windows_are_clipped_to_the_horizon() {
        let t = |d| Utc.with_ymd_and_hms(2026, 1, d, 0, 0, 0).unwrap();
        let w = [(t(1), Some(t(3))), (t(5), Some(t(9))), (t(12), None)];
        assert_eq!(clip_windows(&w, None), w);
        assert_eq!(
            clip_windows(&w, Some(t(7))),
            [(t(7), Some(t(9))), (t(12), None)]
        );
        assert_eq!(clip_windows(&w, Some(t(20))), [(t(20), None)]);
    }

    #[test]
    fn removed_records_are_named_by_their_repo() {
        let h = |party: &str, list: Option<&str>| Removed {
            id: 1,
            removed_at: DateTime::<Utc>::UNIX_EPOCH,
            party: party.to_owned(),
            list: list.map(|o| store::RemovedList {
                owner_did: o.to_owned(),
                rkey: "l".into(),
                name: None,
                purpose: None,
                state: None,
            }),
            rkey: "3k".into(),
            created_at: None,
            first_seen: None,
            last_seen: None,
            cause: 1,
            live: false,
        };
        assert_eq!(
            record_uri(Kind::Block, &h("did:plc:blocker", None), None),
            "at://did:plc:blocker/app.bsky.graph.block/3k"
        );
        assert_eq!(
            record_uri(
                Kind::ListBlock,
                &h("did:plc:blocker", None),
                Some("did:plc:owner")
            ),
            "at://did:plc:blocker/app.bsky.graph.listblock/3k"
        );
        // A listitem is in the list owner's repo, whichever page shows it.
        assert_eq!(
            record_uri(Kind::Membership, &h("", Some("did:plc:owner")), None),
            "at://did:plc:owner/app.bsky.graph.listitem/3k"
        );
        assert_eq!(
            record_uri(
                Kind::Membership,
                &h("did:plc:member", None),
                Some("did:plc:owner")
            ),
            "at://did:plc:owner/app.bsky.graph.listitem/3k"
        );
    }

    #[test]
    fn links_point_into_the_admin_ui() {
        assert_eq!(
            lookup_did_href("did:plc:abc"),
            "/admin/lookup/did?q=did%3Aplc%3Aabc"
        );
        assert!(lookup_list_href("did:plc:abc", "3k").starts_with(
            "/admin/lookup/list?q=at%3A%2F%2Fdid%3Aplc%3Aabc%2Fapp.bsky.graph.list%2F3k"
        ));
    }
}
