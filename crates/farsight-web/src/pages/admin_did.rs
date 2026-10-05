//! The admin DID lookup page (`/admin/lookup/did`): a header for the
//! account with its backfill state, and three tables behind tabs —
//! incoming blocks, incoming listblocks, and the listblocked lists that
//! name the account — each turned by page number, the first and the last
//! with a filter box.
//!
//! The page looks like the public account page and shares none of its
//! surface: its templates, its stylesheet rules (`farsight.css`) and its
//! script (`admin.js`) are the admin's own, so that either can change
//! without the other. What is shared is below the surface: the row
//! queries, the page arithmetic and the handle cache.

use std::collections::HashMap;
use std::sync::Arc;

use askama::Template;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use farsight_api::handlers;
use farsight_api::params::Params;
use farsight_core::Did;
use farsight_storage::backfill_api;
use farsight_storage::public::{self as store, Counted};
use farsight_storage::ui_rows::{self, Filter, Find, Section as Rows};

use super::{Nav, WebState, coverage_words, gate, nav, permit, resolve_handle};
use crate::cells;
use crate::common::render_private;
use crate::public::card;
use crate::public::paging::{self, Item, Total};
use crate::public::text::{BLOCK, LISTBLOCK, Record, Stamp, clean, thousands};
use crate::public::warming::Asked;
use crate::rows;

/// Rows a table shows on one page.
pub const PAGE_ROWS: i64 = 50;
/// Most rows a table's count reads before it says "more than".
pub const COUNT_CAP: i64 = 5_000_000;
/// Longest filter text read.
pub const MAX_FIND: usize = 100;
/// The page's address.
pub const BASE: &str = "/admin/lookup/did";

/// The tables, in tab order: `(id, label, page parameter)`. The first is
/// shown when the address names none.
pub const TABLES: [(&str, &str, &str); 4] = [
    ("blocks", "Incoming blocks", "page"),
    ("listblocks", "Incoming listblocks", "lb"),
    ("lists", "Lists", "lists"),
    ("blockinglists", "Blocking lists", "bl"),
];

/// The History tab: read only when the address asks for it.
pub const HISTORY: &str = "history";

/// What the address asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Ask {
    /// The account, as a DID.
    did: String,
    /// The table in view.
    tab: &'static str,
    /// The filter text, trimmed; empty for none.
    find: String,
    /// The page of each table, in [`TABLES`] order.
    pages: [i64; 4],
}

impl Ask {
    fn read(did: &str, q: &HashMap<String, String>) -> Ask {
        let tab = match q.get("tab").map(String::as_str) {
            Some(HISTORY) => HISTORY,
            Some(t) => TABLES
                .iter()
                .find(|x| x.0 == t)
                .map_or(TABLES[0].0, |x| x.0),
            None => TABLES[0].0,
        };
        let page = |key: &str| {
            q.get(key)
                .and_then(|v| v.parse::<i64>().ok())
                .filter(|n| (1..=paging::MAX_PAGE).contains(n))
                .unwrap_or(1)
        };
        let find: String = q
            .get("find")
            .map(|f| f.trim().chars().take(MAX_FIND).collect())
            .unwrap_or_default();
        Ask {
            did: did.to_owned(),
            tab,
            find,
            pages: [
                page(TABLES[0].2),
                page(TABLES[1].2),
                page(TABLES[2].2),
                page(TABLES[3].2),
            ],
        }
    }

    /// The address of this view with `tab` in front and table `turn` on
    /// page `to`. Page 1 and the first tab are left out of the address.
    fn link(&self, tab: &str, turn: Option<(usize, i64)>) -> String {
        let mut s = url::form_urlencoded::Serializer::new(String::new());
        s.append_pair("q", &self.did);
        if tab != TABLES[0].0 {
            s.append_pair("tab", tab);
        }
        if !self.find.is_empty() {
            s.append_pair("find", &self.find);
        }
        for (i, (_, _, key)) in TABLES.iter().enumerate() {
            let n = match turn {
                Some((t, to)) if t == i => to,
                _ => self.pages[i],
            };
            if n > 1 {
                s.append_pair(key, &n.to_string());
            }
        }
        format!("{BASE}?{}", s.finish())
    }
}

/// One tab.
#[derive(Debug, Clone)]
pub struct Tab {
    /// The table's id.
    pub id: &'static str,
    /// Its label.
    pub label: &'static str,
    /// The address that shows it.
    pub href: String,
    /// It is the one in view.
    pub active: bool,
}

/// A page control, as the template prints it.
#[derive(Debug, Clone)]
pub struct Control {
    /// The page number; `None`: a gap.
    pub page: Option<i64>,
    /// Its address; `None`: the current page or a gap.
    pub href: Option<String>,
    /// Shown however narrow the row: the first page, the current one,
    /// and the last when it is known.
    pub keep: bool,
}

/// The page controls of one table.
#[derive(Debug, Clone, Template)]
#[template(path = "_admin_pagination.html")]
pub struct Pager {
    /// The table's id.
    pub section: &'static str,
    /// Its name, for the controls' label.
    pub label: &'static str,
    /// The controls.
    pub controls: Vec<Control>,
    /// The page before, if any.
    pub prev: Option<String>,
    /// The page after, if any.
    pub next: Option<String>,
    /// The last page is not known.
    pub open: bool,
}

impl Pager {
    fn new(ask: &Ask, table: usize, total: Total, more: bool) -> Pager {
        let (id, label, _) = TABLES[table];
        Pager::build(id, label, ask.pages[table], total, more, |p| {
            ask.link(id, Some((table, p)))
        })
    }

    /// The controls of table `id` on page `current`; `to` gives a page's
    /// address.
    pub(super) fn build(
        id: &'static str,
        label: &'static str,
        current: i64,
        total: Total,
        more: bool,
        to: impl Fn(i64) -> String,
    ) -> Pager {
        let (items, open) = paging::items(current, total, more, PAGE_ROWS);
        let last = items
            .iter()
            .filter_map(|i| match i {
                Item::Page(p) | Item::Current(p) => Some(*p),
                Item::Gap => None,
            })
            .max()
            .unwrap_or(1);
        Pager {
            section: id,
            label,
            controls: items
                .into_iter()
                .map(|i| match i {
                    Item::Page(p) => Control {
                        page: Some(p),
                        href: Some(to(p)),
                        keep: p == 1 || (!open && p == last),
                    },
                    Item::Current(p) => Control {
                        page: Some(p),
                        href: None,
                        keep: true,
                    },
                    Item::Gap => Control {
                        page: None,
                        href: None,
                        keep: false,
                    },
                })
                .collect(),
            prev: (current > 1).then(|| to(current - 1)),
            next: (more || (!open && current < last)).then(|| to(current + 1)),
            open,
        }
    }
}

/// A row of "Incoming blocks".
#[derive(Debug, Clone)]
pub struct BlockRow {
    /// The blocker, rendered.
    pub who: String,
    /// The block record's at-uri.
    pub uri: String,
    /// The record viewer's page for it, when one is configured.
    pub view: Option<String>,
    /// The block's stated creation time.
    pub when: Option<Stamp>,
}

/// A row of "Incoming listblocks".
#[derive(Debug, Clone)]
pub struct ListBlockRow {
    /// The list's name, or its at-uri.
    pub list: String,
    /// The list's lookup page.
    pub list_href: String,
    /// Its purpose, in words.
    pub purpose: &'static str,
    /// The blocker, rendered.
    pub who: String,
    /// The listblock's stated creation time.
    pub when: Option<Stamp>,
}

/// A row of "Lists naming this account".
#[derive(Debug, Clone)]
pub struct ListRow {
    /// The list's name, or its at-uri.
    pub list: String,
    /// The list's lookup page.
    pub list_href: String,
    /// Its purpose, in words.
    pub purpose: &'static str,
    /// The owner, rendered.
    pub owner: String,
    /// Counted listblocks on it.
    pub listblocks: String,
    /// When the account was put on it, as the listitem states.
    pub added: Option<Stamp>,
}

/// A row of "Blocking lists": a list the account subscribes to as a
/// block list.
#[derive(Debug, Clone)]
pub struct BlockingRow {
    /// The list's name, or its at-uri.
    pub list: String,
    /// The list's lookup page.
    pub list_href: String,
    /// Its purpose, in words; a dash where Farsight has no record of it.
    pub purpose: &'static str,
    /// The owner, rendered.
    pub owner: String,
    /// The at-uri of the account's listblock record.
    pub uri: String,
    /// The record viewer's page for it, when one is configured.
    pub view: Option<String>,
    /// The listblock's stated creation time.
    pub when: Option<Stamp>,
}

/// One table.
#[derive(Debug, Clone)]
pub struct Table<R> {
    /// The count for the heading; `None` when it could not be read.
    pub count: Option<String>,
    /// Coverage in words.
    pub coverage: String,
    /// What went wrong, if the rows could not be read.
    pub error: Option<String>,
    /// What the filter kept, in words.
    pub note: Option<String>,
    /// The rows of this page.
    pub rows: Vec<R>,
    /// The page controls, rendered; empty for a table of one page.
    pub pager: String,
}

impl<R> Default for Table<R> {
    fn default() -> Self {
        Table {
            count: None,
            coverage: String::new(),
            error: None,
            note: None,
            rows: Vec::new(),
            pager: String::new(),
        }
    }
}

/// A handle or a host the account has had.
#[derive(Debug, Clone)]
pub struct HeldRow {
    /// The handle or host.
    pub value: String,
    /// Since when.
    pub since: Stamp,
    /// It is the one the account has now.
    pub current: bool,
}

fn held_rows(list: &[card::Held]) -> Vec<HeldRow> {
    let last = list.len().saturating_sub(1);
    list.iter()
        .enumerate()
        .rev()
        .map(|(i, h)| HeldRow {
            value: clean(&h.value),
            since: Stamp::of(h.since),
            current: i == last,
        })
        .collect()
}

/// The History tab: the account's earlier handles and hosts from its PLC
/// audit log, and the records naming it that this instance stored and
/// later removed.
pub struct HistoryTab {
    /// Why there are no handles and hosts to show, if so.
    pub note: Option<&'static str>,
    /// Handles, newest first.
    pub handles: Vec<HeldRow>,
    /// Hosts, newest first.
    pub hosts: Vec<HeldRow>,
    /// The removed records, or what to say in their place.
    pub removed: Result<crate::history::DidRemoved, &'static str>,
}

/// The account in view.
pub struct Subject {
    /// The DID.
    pub did: String,
    /// Its verified handle, if cached.
    pub handle: Option<String>,
    /// Its profile-card fragment: the header takes the avatar, the
    /// creation date and the host from it.
    pub card: String,
    /// The History tab, when it is the one asked for.
    pub history: Option<HistoryTab>,
    /// Backfill state: label and value.
    pub backfill: Vec<(&'static str, String)>,
    /// The filter text.
    pub find: String,
    /// The tabs.
    pub tabs: Vec<Tab>,
    /// The id of the table in view.
    pub active: &'static str,
    /// Incoming blocks.
    pub blocks: Table<BlockRow>,
    /// Incoming listblocks.
    pub listblocks: Table<ListBlockRow>,
    /// Lists naming the account.
    pub lists: Table<ListRow>,
    /// Lists the account subscribes to as block lists. Always shown to
    /// an admin; the public page shows it with `show_outgoing_blocks`.
    pub blocking: Table<BlockingRow>,
}

/// The DID lookup page.
#[derive(Template)]
#[template(path = "lookup_did.html")]
pub struct DidPage {
    /// Navigation.
    pub nav: Nav,
    /// The query.
    pub q: String,
    /// Error.
    pub error: Option<String>,
    /// The account, once the query names one.
    pub subject: Option<Subject>,
}

fn count_words(n: i64) -> String {
    if n > COUNT_CAP {
        format!("more than {}", thousands(COUNT_CAP))
    } else {
        thousands(n)
    }
}

fn total_of(n: i64) -> Total {
    if n > COUNT_CAP {
        Total::MoreThan(COUNT_CAP)
    } else {
        Total::Rows(n)
    }
}

fn purpose_words(code: Option<i16>) -> &'static str {
    match code.map(|c| farsight_core::ListPurpose::from_code(c).api_name()) {
        Some("modlist") => "Moderation",
        Some("curatelist") => "Curation",
        Some("referencelist") => "Reference",
        Some(_) => "Other",
        None => "—",
    }
}

fn list_uri(owner: &str, rkey: &str) -> String {
    format!("at://{owner}/app.bsky.graph.list/{rkey}")
}

fn list_href(uri: &str) -> String {
    let q: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("q", uri)
        .finish();
    format!("/admin/lookup/list?{q}")
}

/// The filter as the row queries take it: a DID names its account
/// outright; anything else is matched as part of a DID, of a stored
/// handle or (on the lists table) of a list's name.
async fn finder(st: &WebState, text: &str) -> Option<Find> {
    let text = text.trim_start_matches('@');
    if text.is_empty() {
        return None;
    }
    if let Ok(did) = Did::parse(text) {
        let id = match st.api.pool.acquire().await {
            Ok(mut conn) => farsight_storage::queries::actor(&mut conn, did.as_str())
                .await
                .ok()
                .flatten()
                .map(|a| a.id),
            Err(_) => None,
        };
        return Some(Find {
            ids: id.into_iter().collect(),
            pattern: None,
        });
    }
    Some(Find {
        ids: Vec::new(),
        pattern: Some(Find::containing(text)),
    })
}

fn find_note(find: &str, shown: Option<i64>, what: &str) -> Option<String> {
    if find.is_empty() {
        return None;
    }
    Some(match shown {
        Some(0) => format!("No {what} match “{}”.", clean(find)),
        Some(n) => format!("{} matching “{}”.", thousands(n), clean(find)),
        None => format!("Filtered by “{}”.", clean(find)),
    })
}

/// `GET /admin/lookup/did`.
pub async fn lookup_did(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let query = q.get("q").map(|s| s.trim().to_owned()).unwrap_or_default();
    let mut page = DidPage {
        nav: nav(&Some(s)),
        q: query.clone(),
        error: None,
        subject: None,
    };
    if query.is_empty() {
        return render_private(&page);
    }
    let did = if query.starts_with("did:") {
        Did::parse(&query).map_err(|e| e.to_string())
    } else {
        resolve_handle(&st.safe, &query).await
    };
    let did = match did {
        Ok(d) => d,
        Err(e) => {
            page.error = Some(e);
            return render_private(&page);
        }
    };
    let _permit = match permit(&st).await {
        Ok(p) => p,
        Err(e) => {
            page.error = Some(e);
            return render_private(&page);
        }
    };
    let cfg = st.api.config.current();
    let viewer = cfg.config.public_ui.record_viewer_url.as_str();
    let ask = Ask::read(did.as_str(), &q);
    let mut asked = Asked::new(&cfg.config);
    let find = finder(&st, &ask.find).await;
    let coverage_params = Params::from_pairs(vec![
        ("actor".to_owned(), ask.did.clone()),
        ("limit".to_owned(), "1".to_owned()),
    ]);
    let subject_row = match st.api.pool.acquire().await {
        Ok(mut conn) => farsight_storage::queries::actor(&mut conn, did.as_str())
            .await
            .ok()
            .flatten(),
        Err(_) => None,
    };

    // Incoming blocks: coverage from the API's computation, rows from the
    // table's own query, hidden accounts left out as the API does.
    let mut blocks = Table::<BlockRow>::default();
    match handlers::get_incoming_blocks(&st.api, &coverage_params).await {
        Ok(r) => blocks.coverage = coverage_words(&r.body["freshness"]),
        Err(e) => blocks.error = Some(e.message),
    }
    // Incoming listblocks and the lists: the same.
    let mut listblocks = Table::<ListBlockRow>::default();
    match handlers::get_incoming_list_blocks(&st.api, &coverage_params).await {
        Ok(r) => listblocks.coverage = coverage_words(&r.body["freshness"]),
        Err(e) => listblocks.error = Some(e.message),
    }
    let mut lists = Table::<ListRow>::default();
    let mut blocking = Table::<BlockingRow>::default();
    match handlers::get_lists_naming(&st.api, &coverage_params).await {
        Ok(r) => lists.coverage = coverage_words(&r.body["freshness"]),
        Err(e) => lists.error = Some(e.message),
    }

    if let Some(a) = &subject_row {
        let filter = Filter {
            hide_inactive: true,
            show_suspended: false,
            show_taken_down: false,
            find: find.as_ref(),
            excluded: &[],
        };
        let hidden = ui_rows::hidden_statuses(false, false);
        // --- blocks
        let counted = match st.api.pool.acquire().await {
            Ok(mut conn) => store::bounded_count_hiding(
                &mut conn,
                Counted::IncomingBlocks,
                a.id,
                &[],
                hidden,
                find.as_ref(),
                COUNT_CAP,
            )
            .await
            .ok(),
            Err(_) => None,
        };
        blocks.count = counted.map(count_words);
        blocks.note = find_note(&ask.find, counted, "blockers");
        match rows::numbered(
            &st,
            Rows::IncomingBlocks,
            a.id,
            filter,
            ask.pages[0],
            PAGE_ROWS,
        )
        .await
        {
            Ok(p) => {
                let dids: Vec<String> = p.rows.iter().map(|b| b.did.clone()).collect();
                crate::public::handles::recall(&st, &cfg.config, &dids).await;
                blocks.rows = p
                    .rows
                    .iter()
                    .map(|b| {
                        let record = Record::of(viewer, &b.did, BLOCK, &b.rkey);
                        BlockRow {
                            who: cells::account(&st, &mut asked, &b.did)
                                .render()
                                .unwrap_or_default(),
                            uri: record.uri,
                            view: record.href,
                            when: b.created_at.map(Stamp::of),
                        }
                    })
                    .collect();
                let total = counted.map_or(Total::Unknown, total_of);
                blocks.pager = Pager::new(&ask, 0, total, p.more)
                    .render()
                    .unwrap_or_default();
            }
            Err(e) => blocks.error = Some(e.message),
        }
        // --- listblocks (no filter: the rows are pairs of list and blocker)
        let got = match st.api.pool.acquire().await {
            Ok(mut conn) => {
                let n = farsight_storage::queries::incoming_list_blocks_count(
                    &mut conn, a.id, COUNT_CAP,
                )
                .await
                .ok();
                let rows = farsight_storage::queries::incoming_list_blocks_at(
                    &mut conn,
                    a.id,
                    (ask.pages[1] - 1) * PAGE_ROWS,
                    PAGE_ROWS + 1,
                )
                .await;
                Some((n, rows))
            }
            Err(_) => None,
        };
        match got {
            Some((n, Ok(mut rows))) => {
                let more = rows.len() as i64 > PAGE_ROWS;
                rows.truncate(PAGE_ROWS as usize);
                let dids: Vec<String> = rows.iter().map(|r| r.blocker.clone()).collect();
                crate::public::handles::recall(&st, &cfg.config, &dids).await;
                listblocks.count = n.map(count_words);
                listblocks.rows = rows
                    .iter()
                    .map(|r| {
                        let uri = list_uri(&r.list.owner_did, &r.list.rkey);
                        ListBlockRow {
                            list: r
                                .list
                                .name
                                .as_deref()
                                .map(clean)
                                .filter(|n| !n.is_empty())
                                .unwrap_or_else(|| uri.clone()),
                            list_href: list_href(&uri),
                            purpose: purpose_words(r.list.purpose),
                            who: cells::account(&st, &mut asked, &r.blocker)
                                .render()
                                .unwrap_or_default(),
                            when: r.created_at.map(Stamp::of),
                        }
                    })
                    .collect();
                let total = n.map_or(Total::Unknown, total_of);
                listblocks.pager = Pager::new(&ask, 1, total, more)
                    .render()
                    .unwrap_or_default();
            }
            Some((_, Err(e))) => listblocks.error = Some(e.to_string()),
            None => listblocks.error = Some("The database did not answer.".into()),
        }
        // --- lists
        let counted = match st.api.pool.acquire().await {
            Ok(mut conn) => {
                ui_rows::lists_naming_count(&mut conn, a.id, &[], find.as_ref(), COUNT_CAP)
                    .await
                    .ok()
            }
            Err(_) => None,
        };
        lists.count = counted.map(count_words);
        lists.note = find_note(&ask.find, counted, "lists");
        match rows::numbered_naming(&st, a.id, &[], find.as_ref(), ask.pages[2], PAGE_ROWS).await {
            Ok(p) => {
                let dids: Vec<String> = p.rows.iter().map(|l| l.owner_did.clone()).collect();
                crate::public::handles::recall(&st, &cfg.config, &dids).await;
                lists.rows = p
                    .rows
                    .iter()
                    .map(|l| {
                        let uri = list_uri(&l.owner_did, &l.rkey);
                        ListRow {
                            list: l
                                .name
                                .as_deref()
                                .map(clean)
                                .filter(|n| !n.is_empty())
                                .unwrap_or_else(|| uri.clone()),
                            list_href: list_href(&uri),
                            purpose: purpose_words(l.purpose),
                            owner: cells::account(&st, &mut asked, &l.owner_did)
                                .render()
                                .unwrap_or_default(),
                            listblocks: thousands(i64::from(l.listblock_count)),
                            added: l.added_at.map(Stamp::of),
                        }
                    })
                    .collect();
                let total = counted.map_or(Total::Unknown, total_of);
                lists.pager = Pager::new(&ask, 2, total, p.more)
                    .render()
                    .unwrap_or_default();
            }
            Err(e) => lists.error = Some(e.message),
        }
        // --- lists the account subscribes to: every listblock it has,
        // whatever Farsight knows of the list
        let got = match st.api.pool.acquire().await {
            Ok(mut conn) => {
                let n = ui_rows::lists_blocked_count(
                    &mut conn,
                    a.id,
                    &[],
                    find.as_ref(),
                    true,
                    COUNT_CAP,
                )
                .await
                .ok();
                let rows = ui_rows::lists_blocked(
                    &mut conn,
                    a.id,
                    &[],
                    find.as_ref(),
                    true,
                    (ask.pages[3] - 1) * PAGE_ROWS,
                    PAGE_ROWS + 1,
                )
                .await;
                Some((n, rows))
            }
            Err(_) => None,
        };
        match got {
            Some((n, Ok(mut rows))) => {
                let more = rows.len() as i64 > PAGE_ROWS;
                rows.truncate(PAGE_ROWS as usize);
                let dids: Vec<String> = rows.iter().map(|l| l.owner_did.clone()).collect();
                crate::public::handles::recall(&st, &cfg.config, &dids).await;
                blocking.count = n.map(count_words);
                blocking.note = find_note(&ask.find, n, "lists");
                blocking.rows = rows
                    .iter()
                    .map(|l| {
                        let uri = list_uri(&l.owner_did, &l.rkey);
                        let record = Record::of(viewer, did.as_str(), LISTBLOCK, &l.block_rkey);
                        BlockingRow {
                            list: l
                                .name
                                .as_deref()
                                .map(clean)
                                .filter(|n| !n.is_empty())
                                .unwrap_or_else(|| uri.clone()),
                            list_href: list_href(&uri),
                            purpose: purpose_words(l.purpose),
                            owner: cells::account(&st, &mut asked, &l.owner_did)
                                .render()
                                .unwrap_or_default(),
                            uri: record.uri,
                            view: record.href,
                            when: l.created_at.map(Stamp::of),
                        }
                    })
                    .collect();
                let total = n.map_or(Total::Unknown, total_of);
                blocking.pager = Pager::new(&ask, 3, total, more)
                    .render()
                    .unwrap_or_default();
            }
            Some((_, Err(e))) => blocking.error = Some(e.to_string()),
            None => blocking.error = Some("The database did not answer.".into()),
        }
    } else {
        blocks.count = Some("0".into());
        listblocks.count = Some("0".into());
        lists.count = Some("0".into());
        blocking.count = Some("0".into());
    }
    asked.submit(&st);

    // Backfill state, for the header.
    let mut backfill = Vec::new();
    if let Ok(mut conn) = st.api.pool.acquire().await {
        let discovery = !cfg.config.backfill.backlinks.url.is_empty();
        if let Ok(b) = backfill_api::status(&mut conn, did.as_str(), discovery).await {
            backfill.push(("Repo", b.repo.state.api_name().to_owned()));
            if let Some(t) = b.repo.last_backfilled_at {
                backfill.push((
                    "Last backfilled",
                    t.format("%Y-%m-%d %H:%M UTC").to_string(),
                ));
            }
            if let Some(e) = b.repo.last_error {
                backfill.push(("Last error", e));
            }
            backfill.push(("Discovery", b.discovery.state.api_name().to_owned()));
        }
    }
    // The History tab: one request to the PLC directory, under the card
    // budget, and the history tables.
    let history = if ask.tab == HISTORY {
        let (note, handles, hosts) = match card::history(&st, &cfg.config, &did).await {
            card::History::Log(log) => (None, held_rows(&log.handles), held_rows(&log.hosts)),
            card::History::NoLog => (
                Some(
                    "This account's DID is not registered in the PLC directory, which is where its handles and hosts are read from.",
                ),
                Vec::new(),
                Vec::new(),
            ),
            card::History::Unavailable => (
                Some(
                    "The handles and hosts could not be read from the PLC directory right now. Try again in a moment.",
                ),
                Vec::new(),
                Vec::new(),
            ),
        };
        let cursors = Params::from_pairs(
            ["hb", "hm"]
                .iter()
                .filter_map(|k| q.get(*k).map(|v| ((*k).to_owned(), v.clone())))
                .collect(),
        );
        let lead = format!("{}&", ask.link(HISTORY, None));
        Some(HistoryTab {
            note,
            handles,
            hosts,
            removed: crate::history::did_removed(&st, &did, &cursors, &lead).await,
        })
    } else {
        None
    };
    crate::public::handles::recall(&st, &cfg.config, &[did.to_string()]).await;
    let handle = st
        .public
        .handles
        .lookup_stale(did.as_str())
        .and_then(|(c, _)| match c {
            farsight_storage::handles::Cached::Handle(h) => Some(clean(&h)),
            farsight_storage::handles::Cached::None => None,
        });
    page.subject = Some(Subject {
        did: did.to_string(),
        handle,
        card: cells::admin_card_href(did.as_str()),
        history,
        backfill,
        find: ask.find.clone(),
        tabs: TABLES
            .iter()
            .map(|(id, label, _)| (*id, *label))
            .chain([(HISTORY, "History")])
            .map(|(id, label)| Tab {
                id,
                label,
                href: ask.link(id, None),
                active: id == ask.tab,
            })
            .collect(),
        active: ask.tab,
        blocks,
        listblocks,
        lists,
        blocking,
    });
    render_private(&page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(pairs: &[(&str, &str)]) -> Ask {
        let q: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        Ask::read("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &q)
    }

    #[test]
    fn the_address_names_the_tab_the_filter_and_each_tables_page() {
        let a = ask(&[]);
        assert_eq!(
            (a.tab, a.pages, a.find.as_str()),
            ("blocks", [1, 1, 1, 1], "")
        );
        assert_eq!(
            a.link("blocks", None),
            "/admin/lookup/did?q=did%3Aplc%3Aaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let a = ask(&[
            ("tab", "lists"),
            ("find", "  cat "),
            ("page", "3"),
            ("lists", "2"),
        ]);
        assert_eq!(
            (a.tab, a.pages, a.find.as_str()),
            ("lists", [3, 1, 2, 1], "cat")
        );
        assert_eq!(
            a.link("lists", Some((2, 5))),
            "/admin/lookup/did?q=did%3Aplc%3Aaaaaaaaaaaaaaaaaaaaaaaaa&tab=lists&find=cat&page=3&lists=5"
        );
        // Turning to page 1 drops the parameter; another tab keeps the pages.
        assert_eq!(
            a.link("blocks", Some((0, 1))),
            "/admin/lookup/did?q=did%3Aplc%3Aaaaaaaaaaaaaaaaaaaaaaaaa&find=cat&lists=2"
        );
    }

    #[test]
    fn the_history_tab_is_a_tab_of_its_own() {
        let a = ask(&[("tab", "history"), ("page", "2")]);
        assert_eq!(a.tab, HISTORY);
        assert_eq!(
            a.link(HISTORY, None),
            "/admin/lookup/did?q=did%3Aplc%3Aaaaaaaaaaaaaaaaaaaaaaaaa&tab=history&page=2"
        );
    }

    #[test]
    fn what_is_not_a_tab_or_a_page_is_the_first() {
        let a = ask(&[("tab", "nope"), ("page", "0"), ("lb", "x"), ("lists", "-4")]);
        assert_eq!((a.tab, a.pages), ("blocks", [1, 1, 1, 1]));
    }

    #[test]
    fn counts_and_notes() {
        assert_eq!(count_words(1_234), "1,234");
        assert_eq!(count_words(COUNT_CAP + 1), "more than 5,000,000");
        assert_eq!(find_note("", Some(3), "blockers"), None);
        assert_eq!(
            find_note("cat", Some(0), "blockers").as_deref(),
            Some("No blockers match “cat”.")
        );
        assert_eq!(
            find_note("cat", Some(12), "lists").as_deref(),
            Some("12 matching “cat”.")
        );
    }
}
