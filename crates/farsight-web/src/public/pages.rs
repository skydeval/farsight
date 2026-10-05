//! The public pages: home, search, an account and a list.
//!
//! The four sections that show a creation time — blockers, outgoing
//! blocks, a list's members and its subscribers — read their rows through
//! [`crate::rows`], newest first by shown time once the section's index is
//! ready, with the withheld rule applied in the query. The lists naming an
//! account are read the same way, newest first by the shown time of the
//! listitem. The handlers behind the stable read queries are still called
//! in-process, with default filtering, for every section's freshness.
//! Rows are filtered once more with one batched status lookup for the
//! page's DIDs, so a page can hold fewer rows than its size while more
//! follow; the next page is still offered.
//!
//! Every section holds [`PAGE_ROWS`] rows a page and ends with numbered
//! page controls ([`super::paging`]): plain links, the page in the query.
//!
//! A page prints no coverage level. It ends with one "Last updated" line
//! (see [`super::coverage`]); removed records are an admin page
//! ([`crate::history`]).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use askama::Template;
use axum::http::StatusCode;
use axum::response::Response;
use farsight_api::params::Params;
use farsight_api::{handlers, public_ui};
use farsight_core::{Did, RecordKey};
use farsight_storage::codes::actor_status;
use farsight_storage::public::{self as store, Counted};
use farsight_storage::queries::{self, ActorRef};
use farsight_storage::ui_rows::{Filter, Find, Row, Section as Rows};
use serde_json::Value;

use super::card;
use super::coverage::{EMPTY, last_updated};
use super::handles::{page_handle, recall, take_budget};
use super::paging::{self, Pager, Total};
use super::search::{self, Authority, Target};
use super::text::{Stamp, card_href, clean, did_href, list_href, list_uri, paragraphs, thousands};
use super::warming::{Asked, Known};
use super::{
    COUNT_CAP, Cache, Chrome, Fail, OG_ACCOUNT, OG_INSTANCE, OG_LIST, PAGE_ROWS, Req, Withheld,
    chrome, metrics as m, page, redirect,
};
use crate::pages::resolve_handle;

/// Longest the filter box waits for a handle to resolve.
pub const RESOLVE_FIND_WAIT: Duration = Duration::from_secs(3);
/// Longest a search waits for a handle to resolve.
pub const SEARCH_RESOLVE_WAIT: Duration = Duration::from_secs(10);

/// The text a fresh instance shows on its home page.
pub const DEFAULT_DESCRIPTION: &str = "Farsight is an independent index of public block records \
    on the AT Protocol network. This instance shows who blocks an account, directly and through \
    listblocked lists, as far as it has indexed them. It is run by its operator and is not \
    affiliated with Bluesky.";

// ---------------------------------------------------------------------------
// Shared pieces

/// An account as a row names it: `@handle` when its handle is verified,
/// the DID when the check found none. Either way a link to the account's
/// page whose `title` is the DID, and on which the script opens the
/// profile card. Rendering a row never waits for an outbound request: an
/// account shown as a DID is handed to the warming worker.
#[derive(Debug, Clone, Template)]
#[template(path = "_who.html")]
pub struct Who {
    /// The DID.
    pub did: String,
    /// Its public page.
    pub href: String,
    /// Its profile-card fragment.
    pub card: String,
    /// A verified handle, if cached.
    pub handle: Option<String>,
    /// What a host has done to the account, if anything: `takendown` (taken
    /// down) or `suspended`.
    pub tag: Option<&'static str>,
}

/// `None`: the account has not been checked yet and the worker has been
/// asked; its row is held back until it has (see [`Section::pending`]).
fn who(r: &Req<'_>, asked: &mut Asked, shown: &Shown<'_>, did: &str) -> Option<Who> {
    let tag = shown.tag(did);
    let handle = if tag == Some(TAKEN_DOWN) {
        // A taken-down account's host no longer answers for it: nothing
        // to check, so the row waits for nothing and shows what is known.
        r.st.public.handles.get(did).map(|h| clean(&h))
    } else {
        match asked.known(r.st, did) {
            Known::Handle(h) => Some(clean(&h)),
            Known::NoHandle => None,
            Known::Pending => return None,
        }
    };
    Some(Who {
        did: did.to_owned(),
        href: did_href(did),
        card: card_href(did),
        handle,
        tag,
    })
}

/// The tag of an account its host has taken down.
const TAKEN_DOWN: &str = "takendown";
/// The tag of an account its host has suspended.
const SUSPENDED: &str = "suspended";

impl Who {
    /// What the tag says.
    pub fn tag_text(&self) -> &'static str {
        match self.tag {
            Some(TAKEN_DOWN) => "taken down",
            Some(other) => other,
            None => "",
        }
    }
}

/// The "Show taken down accounts" switch of a table.
#[derive(Debug, Clone)]
pub struct Toggle {
    /// The page with the switch the other way.
    pub href: String,
    /// Taken-down accounts are in the tables now.
    pub on: bool,
}

/// An account row with one author-stated time.
#[derive(Debug, Clone)]
pub struct PartyRow {
    /// The account.
    pub who: Who,
    /// `createdAt` / `addedAt`, as stated by the record's author.
    pub when: Option<Stamp>,
}

/// A section.
#[derive(Debug, Clone)]
pub struct Section<R> {
    /// Bounded record count, shown from 1 up; `None` when it is zero or
    /// was not available.
    pub count: Option<String>,
    /// The count with taken-down accounts included, when that is more.
    pub count_all: Option<String>,
    /// The switch that adds taken-down accounts to the rows; `None` on a
    /// table that does not list accounts by status (the lists).
    pub toggle: Option<Toggle>,
    /// What the table says under its filter box, while it is filtered.
    pub note: Option<String>,
    /// Rows shown.
    pub rows: Vec<R>,
    /// Rows of this page held back because their account's handle has
    /// not been checked yet. The page's script reads the page again
    /// until there are none.
    pub pending: usize,
    /// Page controls.
    pub pager: Pager,
    /// Empty-state line: only on a first page with nothing after it.
    pub empty: Option<&'static str>,
}

/// The page parameters of the account page's sections: "Blocked by", "On
/// lists", "Blocks by this account".
const DID_PAGES: [&str; 3] = ["page", "lists", "out"];
/// Of the list page's: "Members", "Blocked by".
const LIST_PAGES: [&str; 2] = ["page", "subscribers"];
/// The tables of the account page, as its tabs name them; the first is
/// the one in view without a `tab` parameter.
const DID_TABS: [&str; 4] = ["blockers", "lists", "outgoing", "history"];
/// Of the list page.
const LIST_TABS: [&str; 2] = ["members", "subscribers"];
/// The cursor parameters of earlier versions. An address that still
/// carries one is redirected to the first page of its section.
const DID_CURSORS: [&str; 3] = ["bc", "nc", "oc"];
const LIST_CURSORS: [&str; 2] = ["mc", "lc"];

/// A handle or host on the History tab.
#[derive(Debug, Clone)]
pub struct HeldRow {
    /// The handle or host.
    pub value: String,
    /// When the account took it.
    pub since: Stamp,
    /// It is the one the account has now.
    pub current: bool,
}

/// The History tab of an account page: what the PLC directory's log says
/// the account's handles and hosts have been, newest first.
#[derive(Debug, Clone, Default)]
pub struct HistoryView {
    /// Handles, as the account claimed them; not verified.
    pub handles: Vec<HeldRow>,
    /// Hosts.
    pub hosts: Vec<HeldRow>,
    /// Why there is nothing to list, if there is not.
    pub note: Option<&'static str>,
}

fn held_rows(list: &[super::card::Held]) -> Vec<HeldRow> {
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

fn history_view(h: super::card::History) -> HistoryView {
    use super::card::History;
    match h {
        History::Log(log) => HistoryView {
            handles: held_rows(&log.handles),
            hosts: held_rows(&log.hosts),
            note: None,
        },
        History::NoLog => HistoryView {
            note: Some(
                "This account's DID is not registered in the PLC directory, which is where this history is read from.",
            ),
            ..HistoryView::default()
        },
        History::Unavailable => HistoryView {
            note: Some(
                "The history could not be read from the PLC directory right now. Try again in a moment.",
            ),
            ..HistoryView::default()
        },
    }
}

/// A tab of a data page: a link that brings one of the page's tables
/// into view. The script switches in place; without it the link is
/// followed and the server marks the table.
#[derive(Debug, Clone)]
pub struct Tab {
    /// The table's section id.
    pub id: &'static str,
    /// What the tab says.
    pub label: &'static str,
    /// The page with this table in view, every table on its own page.
    pub href: String,
    /// The table in view.
    pub active: bool,
}

/// The tabs for the tables a page has (`shown`, in order), and the id of
/// the one in view: the one `tab` names if the page has it, else the
/// first. `all` is the page's full list, whose first needs no parameter.
fn tabs(
    base: &str,
    q: &Params,
    keys: &[&str],
    all: &[&'static str],
    shown: &[(&'static str, &'static str)],
) -> (Vec<Tab>, &'static str) {
    let asked = q.get(paging::TAB).unwrap_or(all[0]);
    let active = shown
        .iter()
        .map(|(id, _)| *id)
        .find(|id| *id == asked)
        .or_else(|| shown.first().map(|(id, _)| *id))
        .unwrap_or(all[0]);
    let tabs = shown
        .iter()
        .map(|(id, label)| Tab {
            id,
            label,
            href: paging::tab_link(base, q, keys, (*id != all[0]).then_some(*id)),
            active: *id == active,
        })
        .collect();
    (tabs, active)
}

/// The `tab` a link into `section` carries: none for the page's first
/// table.
fn tab_of(all: &[&'static str], section: &'static str) -> Option<&'static str> {
    (section != all[0]).then_some(section)
}

fn count_words(total: Total) -> Option<String> {
    match total {
        Total::Rows(n) if n > 0 => Some(thousands(n)),
        Total::MoreThan(n) => Some(format!("more than {}", thousands(n))),
        _ => None,
    }
}

fn total_of(n: Option<i64>) -> Total {
    match n {
        Some(n) if n > COUNT_CAP => Total::MoreThan(COUNT_CAP),
        Some(n) => Total::Rows(n),
        None => Total::Unknown,
    }
}

fn empty_line<R>(rows: &[R], pending: usize, more: bool, number: i64) -> Option<&'static str> {
    (rows.is_empty() && pending == 0 && !more && number == 1).then_some(EMPTY)
}

/// The same for a table with a filter box: while it is filtered, the
/// note under the box says "No match" and the empty-state line stays out.
fn empty_unless<R>(
    filtered: bool,
    rows: &[R],
    pending: usize,
    more: bool,
    number: i64,
) -> Option<&'static str> {
    if filtered {
        None
    } else {
        empty_line(rows, pending, more, number)
    }
}

pub(crate) fn purpose_words(p: &str) -> &'static str {
    match p {
        "modlist" => "moderation list",
        "curatelist" => "curation list",
        "referencelist" => "reference list",
        _ => "other kind of list",
    }
}

async fn actor_row(r: &Req<'_>, did: &Did) -> Result<Option<ActorRef>, Fail> {
    let mut conn = r.st.api.pool.acquire().await?;
    Ok(queries::actor(&mut conn, did.as_str()).await?)
}

/// The batched status lookup of a page's DIDs (the withheld rule).
struct Shown<'a> {
    withheld: &'a Withheld,
    status: HashMap<String, ActorRef>,
    /// Taken-down accounts are shown (the page's switch is on).
    taken_down: bool,
}

impl Shown<'_> {
    async fn load<'a>(
        r: &Req<'_>,
        withheld: &'a Withheld,
        dids: Vec<String>,
        taken_down: bool,
    ) -> Result<Shown<'a>, Fail> {
        let status = if dids.is_empty() {
            HashMap::new()
        } else {
            let mut tx = r.st.api.read_tx().await?;
            let s = queries::actors(&mut tx, &dids).await?;
            tx.rollback().await?;
            s
        };
        Ok(Shown {
            withheld,
            status,
            taken_down,
        })
    }

    /// Whether a row naming `did`, in any role, may be shown: never an
    /// excluded, deactivated or deleted account; a suspended one always;
    /// a taken-down one while the switch is on.
    fn ok(&self, did: &str) -> bool {
        if self.withheld.excluded(did) {
            return false;
        }
        match self.status.get(did).map(|a| a.status) {
            Some(actor_status::TAKENDOWN) => self.taken_down,
            Some(actor_status::SUSPENDED) => true,
            Some(s) => !actor_status::is_hidden(s),
            None => true,
        }
    }

    /// The tag a row naming `did` carries.
    fn tag(&self, did: &str) -> Option<&'static str> {
        match self.status.get(did).map(|a| a.status) {
            Some(actor_status::TAKENDOWN) => Some(TAKEN_DOWN),
            Some(actor_status::SUSPENDED) => Some(SUSPENDED),
            _ => None,
        }
    }
}

/// A section's length with its filters: the real number of rows (up to
/// [`COUNT_CAP`]). It scans the section's index, so it costs what the
/// section's size costs: about a tenth of a second for 40,000 rows.
/// [`Total::Unknown`] when the query fails or times out; the section
/// still renders.
async fn total(
    r: &Req<'_>,
    what: Counted,
    key: i64,
    w: &Withheld,
    taken_down: bool,
    find: Option<&Find>,
) -> Total {
    let n = async {
        let mut tx = r.st.api.read_tx().await.ok()?;
        let n = store::bounded_count(&mut tx, what, key, &w.ids, taken_down, find, COUNT_CAP)
            .await
            .ok()?;
        let _ = tx.rollback().await;
        Some(n)
    };
    total_of(n.await)
}

/// What a table of accounts says about its length: the count of the rows
/// shown at rest, the count with taken-down accounts when that is more,
/// the switch, and the length the page controls go by.
struct Counts {
    count: Option<String>,
    count_all: Option<String>,
    toggle: Option<Toggle>,
    pages: Total,
    /// What the table says under its filter box, while it is filtered.
    note: Option<String>,
}

#[allow(clippy::too_many_arguments)]
async fn counts(
    r: &Req<'_>,
    key: Option<(Counted, i64)>,
    w: &Withheld,
    taken_down: bool,
    base: &str,
    q: &Params,
    keys: &[&str],
    tab: Option<&str>,
    finder: Option<&Finder>,
) -> Counts {
    let (rest, all) = match key {
        Some((what, key)) => (
            total(r, what, key, w, false, None).await,
            total(r, what, key, w, true, None).await,
        ),
        None => (Total::Rows(0), Total::Rows(0)),
    };
    // The heading keeps the table's whole count; the page controls and
    // the note go by what the filter leaves.
    let matching = match (finder, key) {
        (Some(f), Some((what, key))) => {
            Some(total(r, what, key, w, taken_down, Some(&f.find)).await)
        }
        (Some(_), None) => Some(Total::Rows(0)),
        (None, _) => None,
    };
    let more = match (rest, all) {
        (Total::Rows(a), Total::Rows(b)) => b > a,
        (Total::Rows(_), Total::MoreThan(_)) => true,
        _ => false,
    };
    Counts {
        // The number is the table's heading: zero is said, and only a
        // count that could not be read leaves it out.
        count: match rest {
            Total::Rows(n) => Some(thousands(n)),
            other => count_words(other),
        },
        count_all: more.then(|| count_words(all)).flatten(),
        // Offered on every table of accounts, also where it would add
        // nothing: a switch that comes and goes reads as a fault.
        toggle: Some(Toggle {
            href: paging::taken_down_link(base, q, keys, tab, !taken_down),
            on: taken_down,
        }),
        pages: matching.unwrap_or(if taken_down { all } else { rest }),
        note: matching.and_then(|m| find_note(finder, m)),
    }
}

/// The listblock records on the lists naming an account, added up;
/// `None` when the query fails or times out.
async fn naming_listblocks(r: &Req<'_>, subject: i64, w: &Withheld) -> Option<i64> {
    let mut tx = r.st.api.read_tx().await.ok()?;
    let n = farsight_storage::ui_rows::lists_naming_listblocks(&mut tx, subject, &w.ids)
        .await
        .ok()?;
    let _ = tx.rollback().await;
    Some(n)
}

/// The same for the lists naming an account.
async fn naming_total(r: &Req<'_>, subject: i64, w: &Withheld, find: Option<&Find>) -> Total {
    let n = async {
        let mut tx = r.st.api.read_tx().await.ok()?;
        let n = farsight_storage::ui_rows::lists_naming_count(
            &mut tx, subject, &w.ids, find, COUNT_CAP,
        )
        .await
        .ok()?;
        let _ = tx.rollback().await;
        Some(n)
    };
    total_of(n.await)
}

/// Parameters for a handler that is called for its freshness only: the
/// section's rows and cursor come from [`crate::rows`].
fn freshness_params(pairs: &[(&str, &str)]) -> Params {
    let mut p: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    p.push(("limit".to_owned(), "1".to_owned()));
    Params::from_pairs(p)
}

/// A complete page may be kept for half a minute. One that held rows
/// back is about to change and is not kept at all.
fn cache_for(held: usize) -> Cache {
    if held == 0 {
        Cache::Public(30)
    } else {
        Cache::NoStore
    }
}

/// The withheld rule as the row queries apply it.
fn row_filter<'a>(w: &'a Withheld, taken_down: bool, find: Option<&'a Find>) -> Filter<'a> {
    Filter {
        hide_inactive: true,
        show_suspended: true,
        show_taken_down: taken_down,
        excluded: &w.ids,
        find,
    }
}

/// What the filter box of an account page asks for.
struct Finder {
    /// As typed, for the box.
    text: String,
    /// As the queries take it.
    find: Find,
    /// The text is matched as part of a handle: that finds only accounts
    /// whose handle this instance has verified and stored.
    partial: bool,
}

/// Reads the filter box. A DID names its account outright. Anything
/// else is matched as part of a DID or of a stored handle; and when the
/// visitor pressed Enter (`go=1`) on what reads as a whole handle, the
/// handle is resolved — one lookup from the handle budget — so that an
/// account whose handle was never stored is found too.
async fn finder(r: &Req<'_>, q: &Params) -> Result<Option<Finder>, Fail> {
    let Some(raw) = paging::find(q) else {
        return Ok(None);
    };
    if raw.chars().count() > paging::MAX_FIND {
        return Err(Fail::Bad {
            message: "The filter text is too long.".into(),
            link: None,
        });
    }
    let text = raw.trim_start_matches('@');
    let id_of = |did: Did| async move {
        let mut conn = r.st.api.pool.acquire().await?;
        Ok::<_, Fail>(queries::actor(&mut conn, did.as_str()).await?.map(|a| a.id))
    };
    if let Ok(did) = Did::parse(text) {
        return Ok(Some(Finder {
            text: clean(raw),
            find: Find {
                ids: id_of(did).await?.into_iter().collect(),
                pattern: None,
            },
            partial: false,
        }));
    }
    let mut ids = Vec::new();
    let whole = text.to_ascii_lowercase();
    if q.get("go") == Some("1")
        && whole.contains('.')
        && farsight_core::did::is_valid_hostname(&whole)
        && take_budget(r.st, r.config())
    {
        let found =
            tokio::time::timeout(RESOLVE_FIND_WAIT, resolve_handle(&r.st.safe, &whole)).await;
        if let Ok(Ok(did)) = found {
            ids.extend(id_of(did).await?);
        }
    }
    Ok(Some(Finder {
        text: clean(raw),
        find: Find {
            ids,
            pattern: Some(Find::containing(text)),
        },
        partial: true,
    }))
}

/// What a table says under its filter box.
fn find_note(finder: Option<&Finder>, matching: Total) -> Option<String> {
    let f = finder?;
    let n = match matching {
        Total::Rows(0) => "No match".to_owned(),
        Total::Rows(1) => "1 match".to_owned(),
        Total::Rows(n) => format!("{} matches", thousands(n)),
        Total::MoreThan(n) => format!("More than {} matches", thousands(n)),
        Total::Unknown => "Matches".to_owned(),
    };
    Some(if f.partial {
        format!(
            "{n}. Part of a handle finds only accounts whose handle this instance has verified; \
             press Enter to look up a whole handle."
        )
    } else {
        format!("{n}.")
    })
}

/// The rows of a section that may be shown, as the tables render them,
/// and how many were held back for a handle check.
fn party_rows(
    r: &Req<'_>,
    asked: &mut Asked,
    shown: &Shown<'_>,
    rows: &[Row],
) -> (Vec<PartyRow>, usize) {
    let mut pending = 0;
    let out = rows
        .iter()
        .filter(|b| shown.ok(&b.did))
        .filter_map(|b| match who(r, asked, shown, &b.did) {
            Some(who) => Some(PartyRow {
                who,
                when: b.created_at.map(Stamp::of),
            }),
            None => {
                pending += 1;
                None
            }
        })
        .collect();
    (out, pending)
}

// ---------------------------------------------------------------------------
// Withheld

/// The one notice a withheld account or list gets. It does not name the
/// reason: no stable NSID publishes an account's status, and the notice
/// is the same for an operator's exclusion.
#[derive(Template)]
#[template(path = "public_withheld.html")]
struct WithheldPage {
    c: Chrome,
    /// The DID, or the list's at-uri.
    subject: String,
    /// `account` or `list`.
    what: &'static str,
}

fn withheld_page(r: &Req<'_>, subject: &str, what: &'static str, path: &str) -> Response {
    let cfg = r.config();
    let og = if what == "list" { OG_LIST } else { OG_ACCOUNT };
    page(
        &WithheldPage {
            // The title is the DID or at-uri alone.
            c: chrome(cfg, subject, subject, og, path),
            subject: subject.to_owned(),
            what,
        },
        StatusCode::OK,
        cfg,
        Cache::Public(30),
        false,
    )
}

// ---------------------------------------------------------------------------
// Home

#[derive(Template)]
#[template(path = "public_home.html")]
struct HomePage {
    c: Chrome,
    description: Vec<String>,
    /// What this instance holds, as `getStats` counts it: block records,
    /// tracked lists, accounts seen.
    totals: Vec<(&'static str, String)>,
    updated: Option<Stamp>,
}

/// `/public`.
pub async fn home(r: &Req<'_>) -> Result<Response, Fail> {
    let cfg = r.config();
    let (_slot, _permit) = r.render_slots().await?;
    let stats = handlers::get_stats(&r.st.api, &Params::default())
        .await?
        .body;
    let description = paragraphs(&cfg.public_ui.instance_description);
    let host = &cfg.server.hostname;
    Ok(page(
        &HomePage {
            c: chrome(
                cfg,
                "Block lookup",
                &format!("Farsight at {host}"),
                OG_INSTANCE,
                "/",
            ),
            description: if description.is_empty() {
                vec![DEFAULT_DESCRIPTION.to_owned()]
            } else {
                description
            },
            totals: [
                ("Blocks indexed", "blocks"),
                ("Lists tracked", "trackedLists"),
                ("Accounts seen", "actors"),
            ]
            .into_iter()
            .filter_map(|(label, key)| stats["counts"][key].as_i64().map(|n| (label, thousands(n))))
            .collect(),
            updated: last_updated(&[&stats["freshness"]]),
        },
        StatusCode::OK,
        cfg,
        Cache::Public(60),
        true,
    ))
}

// ---------------------------------------------------------------------------
// Search

#[derive(Template)]
#[template(path = "public_search.html")]
struct SearchPage {
    c: Chrome,
    /// What was typed.
    q: String,
    /// What went wrong.
    message: String,
}

async fn resolve(r: &Req<'_>, handle: &str, typed: &str) -> Result<Did, Fail> {
    let cfg = r.config();
    // The same process-wide budget as the pages' handle verification.
    if !take_budget(r.st, cfg) {
        return Err(Fail::Busy);
    }
    let found = tokio::time::timeout(SEARCH_RESOLVE_WAIT, resolve_handle(&r.st.safe, handle)).await;
    match found {
        Ok(Ok(did)) => Ok(did),
        _ => {
            let t = SearchPage {
                c: chrome(
                    cfg,
                    "Handle not found",
                    &format!("Farsight at {}", cfg.server.hostname),
                    OG_INSTANCE,
                    "/search",
                ),
                q: clean(typed),
                message: format!(
                    "The handle {} could not be resolved to an account. Check the spelling and \
                     try again, or paste the account's DID instead.",
                    clean(handle)
                ),
            };
            Err(Fail::Rendered(Box::new(page(
                &t,
                StatusCode::NOT_FOUND,
                cfg,
                Cache::NoStore,
                false,
            ))))
        }
    }
}

/// `/search?q=…`: resolves the input and redirects under
/// `/public/`. Every request is charged to the lookup rate class.
pub async fn search(r: &Req<'_>, q: &Params) -> Result<Response, Fail> {
    let typed = q.get("q").unwrap_or("").trim().to_owned();
    let target = search::parse(&typed).map_err(|message| Fail::Bad {
        message,
        link: None,
    })?;
    let (authority, rkey) = match target {
        Target::Account(a) => (a, None),
        Target::List(a, k) => (a, Some(k)),
    };
    let did = match authority {
        Authority::Did(d) => d,
        Authority::Handle(h) => resolve(r, &h, &typed).await?,
    };
    let to = match rkey {
        Some(k) => list_href(did.as_str(), k.as_str()),
        None => did_href(did.as_str()),
    };
    Ok(redirect(r.config(), &to))
}

// ---------------------------------------------------------------------------
// Account page

/// A list naming the subject.
#[derive(Debug, Clone)]
pub struct ListRow {
    /// at-uri.
    pub uri: String,
    /// Its public page.
    pub href: String,
    /// Name, if the record has one.
    pub name: Option<String>,
    /// Owner.
    pub owner: Who,
    /// `addedAt`, as stated by the list's owner.
    pub added: Option<Stamp>,
}

#[derive(Template)]
#[template(path = "public_did.html")]
struct DidPage {
    c: Chrome,
    did: String,
    handle: Option<String>,
    /// The account's profile card, from which the script takes the
    /// avatar for the header; `None` for an account this instance has no
    /// row for.
    card: Option<String>,
    /// The page's own address, for the filter box.
    base: String,
    /// What the filter box says.
    find: String,
    /// Taken-down accounts are in the tables (the filter keeps it so).
    taken_down: bool,
    tabs: Vec<Tab>,
    /// The id of the table in view.
    active: &'static str,
    blockers: Section<PartyRow>,
    lists: Section<ListRow>,
    outgoing: Option<Section<PartyRow>>,
    /// The History tab's content: read, and rendered, only when that tab
    /// is the one asked for.
    history: Option<HistoryView>,
    updated: Option<Stamp>,
}

/// `/did/{did}`.
pub async fn did(r: &Req<'_>, did: &Did, q: &Params) -> Result<Response, Fail> {
    let cfg = r.config();
    let base = did_href(did.as_str());
    if let Some(query) = paging::canonical(q, &DID_PAGES, &DID_CURSORS, &DID_TABS) {
        return Ok(crate::common::moved(&base, Some(&query)));
    }
    let b_page = paging::number(q, "page", &base)?;
    let l_page = paging::number(q, "lists", &base)?;
    let o_page = paging::number(q, "out", &base)?;
    let withheld = r.withheld().await?;
    let actor = actor_row(r, did).await?;
    if let Some(reason) = withheld.reason(did.as_str(), actor.map(|a| a.status)) {
        m::withheld(reason);
        return Ok(withheld_page(r, did.as_str(), "account", &base));
    }
    // One resolution per page view at most, only for a known subject, and
    // before the render slot so a slow host holds none.
    let handle = match actor {
        Some(_) => page_handle(r.st, cfg, did).await.map(|h| clean(&h)),
        None => None,
    };
    // The History tab reads the PLC directory: only when it is the tab
    // asked for, only for a known subject, and before the render slot.
    let history = if actor.is_some() && q.get(paging::TAB) == Some("history") {
        Some(history_view(super::card::history(r.st, cfg, did).await))
    } else {
        None
    };
    // The filter box: read (and a whole handle resolved) before the
    // render slot, like the page's own handle.
    let finder = finder(r, q).await?;
    let find = finder.as_ref().map(|f| &f.find);
    let (_slot, _permit) = r.render_slots().await?;
    let api = &r.st.api;
    let taken_down = paging::taken_down(q);
    let filter = row_filter(&withheld, taken_down, find);
    let numbered =
        |section, key, number| crate::rows::numbered(r.st, section, key, filter, number, PAGE_ROWS);

    // Called for its freshness; the rows are read below.
    let blocks = handlers::get_incoming_blocks(api, &freshness_params(&[("actor", did.as_str())]))
        .await?
        .body;
    let block_page = match actor {
        Some(a) => numbered(Rows::IncomingBlocks, a.id, b_page).await?,
        None => Default::default(),
    };
    // Called for its freshness; the rows are read below.
    let naming = handlers::get_lists_naming(api, &freshness_params(&[("actor", did.as_str())]))
        .await?
        .body;
    let naming_page = match actor {
        Some(a) => {
            crate::rows::numbered_naming(r.st, a.id, &withheld.ids, find, l_page, PAGE_ROWS).await?
        }
        None => Default::default(),
    };
    let show_outgoing = cfg.public_ui.show_outgoing_blocks;
    let (out_page, out_fresh) = if show_outgoing {
        let page = match actor {
            Some(a) => numbered(Rows::OutgoingBlocks, a.id, o_page).await?,
            None => Default::default(),
        };
        (page, Some(public_ui::outgoing_freshness(api, did).await?))
    } else {
        (Default::default(), None)
    };

    let mut dids: Vec<String> = block_page.rows.iter().map(|b| b.did.clone()).collect();
    dids.extend(naming_page.rows.iter().map(|l| l.owner_did.clone()));
    dids.extend(out_page.rows.iter().map(|o| o.did.clone()));
    recall(r.st, cfg, &dids).await;
    let shown = Shown::load(r, &withheld, dids, taken_down).await?;
    let mut asked = Asked::new(cfg);
    let pager = |section, label, key, number, total, more| {
        Pager::new(
            section,
            label,
            &base,
            q,
            &DID_PAGES,
            key,
            tab_of(&DID_TABS, section),
            number,
            total,
            more,
            PAGE_ROWS,
        )
    };

    let b = counts(
        r,
        actor.map(|a| (Counted::IncomingBlocks, a.id)),
        &withheld,
        taken_down,
        &base,
        q,
        &DID_PAGES,
        None,
        finder.as_ref(),
    )
    .await;
    let b_total = b.pages;
    if let Some(last) = paging::past_end(b_total, b_page, PAGE_ROWS) {
        return Ok(redirect(
            cfg,
            &paging::link(&base, q, &DID_PAGES, "page", last, None),
        ));
    }
    let (b_rows, b_pending) = party_rows(r, &mut asked, &shown, &block_page.rows);
    let blockers = Section {
        count: b.count,
        count_all: b.count_all,
        toggle: b.toggle,
        note: b.note,
        empty: empty_unless(find.is_some(), &b_rows, b_pending, block_page.more, b_page),
        rows: b_rows,
        pending: b_pending,
        pager: pager(
            "blockers",
            "Blocked by",
            "page",
            b_page,
            b_total,
            block_page.more,
        ),
    };

    let l_total = match actor {
        Some(a) => naming_total(r, a.id, &withheld, find).await,
        None => Total::Rows(0),
    };
    let mut l_pending = 0;
    let l_rows: Vec<ListRow> = naming_page
        .rows
        .iter()
        .filter(|l| shown.ok(&l.owner_did))
        .filter_map(|l| {
            let Some(owner) = who(r, &mut asked, &shown, &l.owner_did) else {
                l_pending += 1;
                return None;
            };
            Some(ListRow {
                uri: list_uri(&l.owner_did, &l.rkey),
                href: list_href(&l.owner_did, &l.rkey),
                name: l.name.as_deref().map(clean).filter(|n| !n.is_empty()),
                owner,
                added: l.added_at.map(Stamp::of),
            })
        })
        .collect();
    if let Some(last) = paging::past_end(l_total, l_page, PAGE_ROWS) {
        return Ok(redirect(
            cfg,
            &paging::link(&base, q, &DID_PAGES, "lists", last, Some("lists")),
        ));
    }
    // The heading: the listblocks on the lists that name the account.
    let l_blocks = match actor {
        Some(a) => naming_listblocks(r, a.id, &withheld).await,
        None => Some(0),
    };
    let lists = Section {
        count: l_blocks.map(thousands),
        count_all: None,
        toggle: None,
        note: find_note(finder.as_ref(), l_total),
        empty: empty_unless(find.is_some(), &l_rows, l_pending, naming_page.more, l_page),
        rows: l_rows,
        pending: l_pending,
        pager: pager(
            "lists",
            "On lists",
            "lists",
            l_page,
            l_total,
            naming_page.more,
        ),
    };

    // The sections this page rendered; the earliest dates the page.
    let mut fresh: Vec<&Value> = vec![&blocks["freshness"], &naming["freshness"]];
    let outgoing = match &out_fresh {
        Some(f) => {
            let o = counts(
                r,
                actor.map(|a| (Counted::OutgoingBlocks, a.id)),
                &withheld,
                taken_down,
                &base,
                q,
                &DID_PAGES,
                Some("outgoing"),
                finder.as_ref(),
            )
            .await;
            let o_total = o.pages;
            if let Some(last) = paging::past_end(o_total, o_page, PAGE_ROWS) {
                return Ok(redirect(
                    cfg,
                    &paging::link(&base, q, &DID_PAGES, "out", last, Some("outgoing")),
                ));
            }
            let (rows, pending) = party_rows(r, &mut asked, &shown, &out_page.rows);
            fresh.push(f);
            Some(Section {
                count: o.count,
                count_all: o.count_all,
                toggle: o.toggle,
                note: o.note,
                empty: empty_unless(find.is_some(), &rows, pending, out_page.more, o_page),
                rows,
                pending,
                pager: pager(
                    "outgoing",
                    "Blocks by this account",
                    "out",
                    o_page,
                    o_total,
                    out_page.more,
                ),
            })
        }
        None => None,
    };

    asked.submit(r.st);
    let og_title = match &handle {
        Some(h) => format!("{h} ({did})"),
        None => did.to_string(),
    };
    let held = blockers.pending + lists.pending + outgoing.as_ref().map_or(0, |o| o.pending);
    let mut shown_tabs = vec![("blockers", "Blocked By"), ("lists", "Blocked By Lists")];
    if outgoing.is_some() {
        shown_tabs.push(("outgoing", "Blocking"));
    }
    if actor.is_some() {
        shown_tabs.push(("history", "History"));
    }
    let (tabs, active) = tabs(&base, q, &DID_PAGES, &DID_TABS, &shown_tabs);
    let t = DidPage {
        tabs,
        active,
        card: actor.map(|_| card_href(did.as_str())),
        history,
        find: finder.map(|f| f.text).unwrap_or_default(),
        taken_down,
        base: base.clone(),
        c: chrome(cfg, did.as_str(), &og_title, OG_ACCOUNT, &base),
        did: did.to_string(),
        handle,
        blockers,
        lists,
        outgoing,
        updated: last_updated(&fresh),
    };
    Ok(page(&t, StatusCode::OK, cfg, cache_for(held), true))
}

// ---------------------------------------------------------------------------
// List page

/// The list's state in words, and whether its members are shown. "No
/// members" is never printed for a list whose members are not indexed.
pub fn state_words(state: &str) -> (String, bool) {
    let w = |s: &str| s.to_owned();
    match state {
        "ready" | "retained" => (w("Indexed."), true),
        "pending" => (w("Being indexed. Members are not available yet."), false),
        "untracked" => (
            w(
                "No account known to this instance blocks this list, so its members are not indexed.",
            ),
            false,
        ),
        "missing" => (
            w("The list record has not been found yet; this instance is still trying."),
            false,
        ),
        "dead" => (
            w("The list record was deleted or could not be found."),
            false,
        ),
        "unavailable" => (
            w("The list could not be read from its owner's server. Members are not shown."),
            false,
        ),
        "deferred" => (
            w("Not indexed: this instance is at a storage limit."),
            false,
        ),
        other => (format!("State: {}.", clean(other)), false),
    }
}

#[derive(Template)]
#[template(path = "public_list.html")]
struct ListPage {
    c: Chrome,
    uri: String,
    name: Option<String>,
    purpose: Option<&'static str>,
    owner: Who,
    state: String,
    capped: bool,
    /// `lists.item_count`: stored, not filtered.
    stored_members: String,
    /// The record's description, line by line: plain text, never marked
    /// up, no links.
    description: Vec<String>,
    /// CID of the record's image, with `public_ui.show_avatars`. The
    /// page's script names the image on the owner's server.
    image: Option<String>,
    tabs: Vec<Tab>,
    /// The id of the table in view.
    active: &'static str,
    members: Option<Section<PartyRow>>,
    blockers: Section<PartyRow>,
    updated: Option<Stamp>,
}

/// Most lines a list's description is shown on; the rest run on in the
/// last.
const DESCRIPTION_LINES: usize = 8;

/// A list's description as lines of [`clean`]ed text: blank lines are
/// dropped, and lines past [`DESCRIPTION_LINES`] join the last one.
fn description_lines(s: &str) -> Vec<String> {
    let mut lines: Vec<String> = s
        .replace("\r\n", "\n")
        .split(['\n', '\r'])
        .map(|l| clean(l).trim().to_owned())
        .filter(|l| !l.is_empty())
        .collect();
    if lines.len() > DESCRIPTION_LINES {
        let rest = lines.split_off(DESCRIPTION_LINES - 1).join(" ");
        lines.push(rest);
    }
    lines
}

/// How long a list whose record could not be read is left alone.
const ABOUT_RETRY: Duration = Duration::from_secs(600);
/// Lists whose record could not be read, and when. Memory only.
static ABOUT_FAILED: Mutex<Option<HashMap<i64, Instant>>> = Mutex::new(None);

/// Whether the record of `list` may be asked for now; `failed` notes an
/// attempt that established nothing.
fn about_due(list: i64, failed: bool) -> bool {
    let mut g = ABOUT_FAILED.lock().unwrap_or_else(|e| e.into_inner());
    let map = g.get_or_insert_with(HashMap::new);
    if failed {
        if map.len() >= 10_000 {
            map.clear();
        }
        map.insert(list, Instant::now());
        return false;
    }
    match map.get(&list) {
        Some(at) if at.elapsed() < ABOUT_RETRY => false,
        _ => {
            map.remove(&list);
            true
        }
    }
}

/// What the list says about itself. A row older than the columns has its
/// record read here, once, on the first view that can reach the owner's
/// server; until then the page has no description.
async fn list_about(
    r: &Req<'_>,
    owner: &Did,
    rkey: &RecordKey,
    info: &queries::ListInfo,
) -> Result<queries::ListAbout, Fail> {
    let about = {
        let mut conn = r.st.api.pool.acquire().await?;
        queries::list_about(&mut conn, info.id).await?
    };
    if about.read || info.record_state != 1 || !about_due(info.id, false) {
        return Ok(about);
    }
    match card::list_about(r.st, r.config(), owner, rkey.as_str()).await {
        Some((description, avatar_cid)) => {
            let mut conn = r.st.api.pool.acquire().await?;
            let stored = queries::list_about_fill(
                &mut conn,
                info.id,
                description.as_deref(),
                avatar_cid.as_deref(),
            )
            .await?;
            if stored {
                Ok(queries::ListAbout {
                    description,
                    avatar_cid,
                    read: true,
                })
            } else {
                // Applied from the network in the meantime: that is newer.
                Ok(queries::list_about(&mut conn, info.id).await?)
            }
        }
        None => {
            about_due(info.id, true);
            Ok(about)
        }
    }
}

/// `/list/{did}/{rkey}`.
pub async fn list(
    r: &Req<'_>,
    owner: &Did,
    rkey: &RecordKey,
    q: &Params,
) -> Result<Response, Fail> {
    let cfg = r.config();
    let base = list_href(owner.as_str(), rkey.as_str());
    let uri = list_uri(owner.as_str(), rkey.as_str());
    if let Some(query) = paging::canonical(q, &LIST_PAGES, &LIST_CURSORS, &LIST_TABS) {
        return Ok(crate::common::moved(&base, Some(&query)));
    }
    let m_page = paging::number(q, "page", &base)?;
    let k_page = paging::number(q, "subscribers", &base)?;
    let withheld = r.withheld().await?;
    let actor = actor_row(r, owner).await?;
    if let Some(reason) = withheld.reason(owner.as_str(), actor.map(|a| a.status)) {
        m::withheld(reason);
        return Ok(withheld_page(r, &uri, "list", &base));
    }
    let info = {
        let mut conn = r.st.api.pool.acquire().await?;
        queries::list_info(&mut conn, owner.as_str(), rkey.as_str()).await?
    };
    let Some(info) = info else {
        return Err(Fail::NotFound {
            title: uri,
            message: "This instance has no record of this list.".into(),
            link: None,
        });
    };
    let handle = page_handle(r.st, cfg, owner).await.map(|h| clean(&h));
    let about = list_about(r, owner, rkey, &info).await?;
    let (_slot, _permit) = r.render_slots().await?;
    let api = &r.st.api;

    // Called for the list's state and its freshness; the rows are read
    // below.
    let listing = handlers::get_list_members(api, &freshness_params(&[("list", uri.as_str())]))
        .await?
        .body;
    let (state, show_members) = state_words(listing["state"].as_str().unwrap_or(""));
    let taken_down = paging::taken_down(q);
    let filter = row_filter(&withheld, taken_down, None);
    let member_page = if show_members {
        crate::rows::numbered(r.st, Rows::ListMembers, info.id, filter, m_page, PAGE_ROWS).await?
    } else {
        Default::default()
    };
    let blocker_page =
        crate::rows::numbered(r.st, Rows::ListBlockers, info.id, filter, k_page, PAGE_ROWS).await?;
    let blockers_fresh = public_ui::listblock_freshness(api)?;

    let mut dids: Vec<String> = member_page.rows.iter().map(|m| m.did.clone()).collect();
    dids.extend(blocker_page.rows.iter().map(|b| b.did.clone()));
    recall(r.st, cfg, &dids).await;
    let shown = Shown::load(r, &withheld, dids, taken_down).await?;
    let mut asked = Asked::new(cfg);
    let pager = |section, label, key, number, total, more| {
        Pager::new(
            section,
            label,
            &base,
            q,
            &LIST_PAGES,
            key,
            tab_of(&LIST_TABS, section),
            number,
            total,
            more,
            PAGE_ROWS,
        )
    };

    let mut fresh: Vec<&Value> = Vec::new();
    let members = if show_members {
        let m = counts(
            r,
            Some((Counted::ListMembers, info.id)),
            &withheld,
            taken_down,
            &base,
            q,
            &LIST_PAGES,
            None,
            None,
        )
        .await;
        let m_total = m.pages;
        if let Some(last) = paging::past_end(m_total, m_page, PAGE_ROWS) {
            return Ok(redirect(
                cfg,
                &paging::link(&base, q, &LIST_PAGES, "page", last, None),
            ));
        }
        let (rows, pending) = party_rows(r, &mut asked, &shown, &member_page.rows);
        fresh.push(&listing["freshness"]);
        Some(Section {
            // The heading states the stored-members counter instead.
            count: None,
            count_all: m.count_all,
            toggle: m.toggle,
            note: None,
            empty: empty_line(&rows, pending, member_page.more, m_page),
            rows,
            pending,
            pager: pager(
                "members",
                "Members",
                "page",
                m_page,
                m_total,
                member_page.more,
            ),
        })
    } else {
        None
    };

    let k = counts(
        r,
        Some((Counted::ListBlockers, info.id)),
        &withheld,
        taken_down,
        &base,
        q,
        &LIST_PAGES,
        Some("subscribers"),
        None,
    )
    .await;
    let k_total = k.pages;
    if let Some(last) = paging::past_end(k_total, k_page, PAGE_ROWS) {
        return Ok(redirect(
            cfg,
            &paging::link(
                &base,
                q,
                &LIST_PAGES,
                "subscribers",
                last,
                Some("subscribers"),
            ),
        ));
    }
    let (rows, pending) = party_rows(r, &mut asked, &shown, &blocker_page.rows);
    asked.submit(r.st);
    fresh.push(&blockers_fresh);
    let blockers = Section {
        count: k.count,
        count_all: k.count_all,
        toggle: k.toggle,
        note: None,
        empty: empty_line(&rows, pending, blocker_page.more, k_page),
        rows,
        pending,
        pager: pager(
            "subscribers",
            "Subscribers",
            "subscribers",
            k_page,
            k_total,
            blocker_page.more,
        ),
    };

    let name = listing["name"]
        .as_str()
        .map(clean)
        .filter(|n| !n.is_empty());
    let og_title = match &name {
        Some(n) => format!("{n} ({uri})"),
        None => uri.clone(),
    };
    let held = blockers.pending + members.as_ref().map_or(0, |m| m.pending);
    let mut shown_tabs = Vec::new();
    if members.is_some() {
        shown_tabs.push(("members", "Members"));
    }
    shown_tabs.push(("subscribers", "Subscribers"));
    let (tabs, active) = tabs(&base, q, &LIST_PAGES, &LIST_TABS, &shown_tabs);
    let t = ListPage {
        tabs,
        active,
        c: chrome(cfg, &uri, &og_title, OG_LIST, &base),
        name,
        purpose: listing["purpose"].as_str().map(purpose_words),
        owner: Who {
            did: owner.to_string(),
            href: did_href(owner.as_str()),
            card: card_href(owner.as_str()),
            handle,
            tag: None,
        },
        state,
        capped: listing["capped"].as_bool().unwrap_or(false),
        stored_members: thousands(i64::from(info.item_count)),
        description: about
            .description
            .as_deref()
            .map(description_lines)
            .unwrap_or_default(),
        image: about.avatar_cid.filter(|_| cfg.public_ui.show_avatars),
        members,
        blockers,
        updated: last_updated(&fresh),
        uri,
    };
    Ok(page(&t, StatusCode::OK, cfg, cache_for(held), true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_description_is_plain_lines() {
        assert_eq!(
            description_lines("one\r\n\r\n  two\u{202E} \n\nthree\u{7}"),
            ["one", "two", "three"]
        );
        let long = (1..=12)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let got = description_lines(&long);
        assert_eq!(got.len(), DESCRIPTION_LINES);
        assert_eq!(got.last().map(String::as_str), Some("8 9 10 11 12"));
        assert!(description_lines(" \n ").is_empty());
    }

    #[test]
    fn every_state_has_its_wording_and_only_two_show_members() {
        for s in ["ready", "retained"] {
            assert_eq!(state_words(s), ("Indexed.".to_owned(), true));
        }
        for s in [
            "pending",
            "untracked",
            "missing",
            "dead",
            "unavailable",
            "deferred",
        ] {
            let (w, members) = state_words(s);
            assert!(!members && !w.is_empty() && !w.starts_with("State:"), "{s}");
        }
        assert_eq!(
            state_words("quarantined"),
            ("State: quarantined.".to_owned(), false)
        );
    }

    #[test]
    fn counts_in_words() {
        // The real number, also past a thousand.
        assert_eq!(count_words(total_of(Some(1_001))).as_deref(), Some("1,001"));
        assert_eq!(
            count_words(total_of(Some(COUNT_CAP))).as_deref(),
            Some("5,000,000")
        );
        assert_eq!(
            count_words(total_of(Some(COUNT_CAP + 1))).as_deref(),
            Some("more than 5,000,000")
        );
        // An empty section's heading shows no count; nor does a failed one.
        assert_eq!(count_words(total_of(Some(0))), None);
        assert_eq!(count_words(total_of(None)), None);
    }
}
