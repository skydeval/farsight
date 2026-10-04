//! The public pages: home, search, an account and a list.
//!
//! The four sections that show a creation time — blockers, outgoing
//! blocks, a list's members and its listblockers — read their rows through
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
use std::time::Duration;

use askama::Template;
use axum::http::StatusCode;
use axum::response::Response;
use farsight_api::params::Params;
use farsight_api::{handlers, public_ui};
use farsight_core::{Did, RecordKey};
use farsight_storage::codes::actor_status;
use farsight_storage::public::{self as store, Counted};
use farsight_storage::queries::{self, ActorRef};
use farsight_storage::ui_rows::{Filter, Row, Section as Rows};
use serde_json::Value;

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
    /// What a host has done to the account, if anything: `banned` (taken
    /// down) or `suspended`.
    pub tag: Option<&'static str>,
}

/// `None`: the account has not been checked yet and the worker has been
/// asked; its row is held back until it has (see [`Section::pending`]).
fn who(r: &Req<'_>, asked: &mut Asked, shown: &Shown<'_>, did: &str) -> Option<Who> {
    let tag = shown.tag(did);
    let handle = if tag == Some(BANNED) {
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
const BANNED: &str = "banned";
/// The tag of an account its host has suspended.
const SUSPENDED: &str = "suspended";

/// The "Show banned accounts" switch of a table.
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
    /// The switch that adds taken-down accounts to the rows; offered when
    /// the table has any, or while it is on.
    pub toggle: Option<Toggle>,
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
const LIST_PAGES: [&str; 2] = ["page", "blockers"];
/// The tables of the account page, as its tabs name them; the first is
/// the one in view without a `tab` parameter.
const DID_TABS: [&str; 3] = ["blockers", "lists", "outgoing"];
/// Of the list page.
const LIST_TABS: [&str; 2] = ["members", "listblockers"];
/// The cursor parameters of earlier versions. An address that still
/// carries one is redirected to the first page of its section.
const DID_CURSORS: [&str; 3] = ["bc", "nc", "oc"];
const LIST_CURSORS: [&str; 2] = ["mc", "lc"];

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
    banned: bool,
}

impl Shown<'_> {
    async fn load<'a>(
        r: &Req<'_>,
        withheld: &'a Withheld,
        dids: Vec<String>,
        banned: bool,
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
            banned,
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
            Some(actor_status::TAKENDOWN) => self.banned,
            Some(actor_status::SUSPENDED) => true,
            Some(s) => !actor_status::is_hidden(s),
            None => true,
        }
    }

    /// The tag a row naming `did` carries.
    fn tag(&self, did: &str) -> Option<&'static str> {
        match self.status.get(did).map(|a| a.status) {
            Some(actor_status::TAKENDOWN) => Some(BANNED),
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
async fn total(r: &Req<'_>, what: Counted, key: i64, w: &Withheld, banned: bool) -> Total {
    let n = async {
        let mut tx = r.st.api.read_tx().await.ok()?;
        let n = store::bounded_count(&mut tx, what, key, &w.ids, banned, COUNT_CAP)
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
}

async fn counts(
    r: &Req<'_>,
    key: Option<(Counted, i64)>,
    w: &Withheld,
    banned: bool,
    base: &str,
    tab: Option<&str>,
) -> Counts {
    let (rest, all) = match key {
        Some((what, key)) => (
            total(r, what, key, w, false).await,
            total(r, what, key, w, true).await,
        ),
        None => (Total::Rows(0), Total::Rows(0)),
    };
    let more = match (rest, all) {
        (Total::Rows(a), Total::Rows(b)) => b > a,
        (Total::Rows(_), Total::MoreThan(_)) => true,
        _ => false,
    };
    Counts {
        // "0 (3 counting banned accounts)": the zero is said when the
        // second number needs it.
        count: count_words(rest).or_else(|| more.then(|| "0".to_owned())),
        count_all: more.then(|| count_words(all)).flatten(),
        toggle: (more || banned).then(|| Toggle {
            href: paging::banned_link(base, tab, !banned),
            on: banned,
        }),
        pages: if banned { all } else { rest },
    }
}

/// The same for the lists naming an account.
async fn naming_total(r: &Req<'_>, subject: i64, w: &Withheld) -> Total {
    let n = async {
        let mut tx = r.st.api.read_tx().await.ok()?;
        let n = farsight_storage::ui_rows::lists_naming_count(&mut tx, subject, &w.ids, COUNT_CAP)
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
fn row_filter(w: &Withheld, banned: bool) -> Filter<'_> {
    Filter {
        hide_inactive: true,
        show_suspended: true,
        show_banned: banned,
        excluded: &w.ids,
    }
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
    hostname: String,
    description: Vec<String>,
    /// `public_ui.contact`, or `server.contact`.
    contact: String,
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
            hostname: host.clone(),
            description: if description.is_empty() {
                vec![DEFAULT_DESCRIPTION.to_owned()]
            } else {
                description
            },
            contact: super::contact(cfg),
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
    /// The maintained listblock counter, as a bare number: it counts
    /// listblock records and does not apply the page's filters.
    pub listblocks: String,
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
    tabs: Vec<Tab>,
    /// The id of the table in view.
    active: &'static str,
    blockers: Section<PartyRow>,
    lists: Section<ListRow>,
    outgoing: Option<Section<PartyRow>>,
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
    let (_slot, _permit) = r.render_slots().await?;
    let api = &r.st.api;
    let banned = paging::banned(q);
    let filter = row_filter(&withheld, banned);
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
            crate::rows::numbered_naming(r.st, a.id, &withheld.ids, l_page, PAGE_ROWS).await?
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
    let shown = Shown::load(r, &withheld, dids, banned).await?;
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
        banned,
        &base,
        None,
    )
    .await;
    let b_total = b.pages;
    let (b_rows, b_pending) = party_rows(r, &mut asked, &shown, &block_page.rows);
    let blockers = Section {
        count: b.count,
        count_all: b.count_all,
        toggle: b.toggle,
        empty: empty_line(&b_rows, b_pending, block_page.more, b_page),
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
        Some(a) => naming_total(r, a.id, &withheld).await,
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
                listblocks: l.listblock_count.to_string(),
                added: l.added_at.map(Stamp::of),
            })
        })
        .collect();
    let lists = Section {
        count: None,
        count_all: None,
        toggle: None,
        empty: empty_line(&l_rows, l_pending, naming_page.more, l_page),
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
                banned,
                &base,
                Some("outgoing"),
            )
            .await;
            let o_total = o.pages;
            let (rows, pending) = party_rows(r, &mut asked, &shown, &out_page.rows);
            fresh.push(f);
            Some(Section {
                count: o.count,
                count_all: o.count_all,
                toggle: o.toggle,
                empty: empty_line(&rows, pending, out_page.more, o_page),
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
    let (tabs, active) = tabs(&base, q, &DID_PAGES, &DID_TABS, &shown_tabs);
    let t = DidPage {
        tabs,
        active,
        card: actor.map(|_| card_href(did.as_str())),
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
    tabs: Vec<Tab>,
    /// The id of the table in view.
    active: &'static str,
    members: Option<Section<PartyRow>>,
    blockers: Section<PartyRow>,
    updated: Option<Stamp>,
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
    let k_page = paging::number(q, "blockers", &base)?;
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
    let (_slot, _permit) = r.render_slots().await?;
    let api = &r.st.api;

    // Called for the list's state and its freshness; the rows are read
    // below.
    let listing = handlers::get_list_members(api, &freshness_params(&[("list", uri.as_str())]))
        .await?
        .body;
    let (state, show_members) = state_words(listing["state"].as_str().unwrap_or(""));
    let banned = paging::banned(q);
    let filter = row_filter(&withheld, banned);
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
    let shown = Shown::load(r, &withheld, dids, banned).await?;
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
            banned,
            &base,
            None,
        )
        .await;
        let m_total = m.pages;
        let (rows, pending) = party_rows(r, &mut asked, &shown, &member_page.rows);
        fresh.push(&listing["freshness"]);
        Some(Section {
            // The heading states the stored-members counter instead.
            count: None,
            count_all: m.count_all,
            toggle: m.toggle,
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
        banned,
        &base,
        Some("listblockers"),
    )
    .await;
    let k_total = k.pages;
    let (rows, pending) = party_rows(r, &mut asked, &shown, &blocker_page.rows);
    asked.submit(r.st);
    fresh.push(&blockers_fresh);
    let blockers = Section {
        count: k.count,
        count_all: k.count_all,
        toggle: k.toggle,
        empty: empty_line(&rows, pending, blocker_page.more, k_page),
        rows,
        pending,
        pager: pager(
            "listblockers",
            "Blocked by",
            "blockers",
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
    shown_tabs.push(("listblockers", "Blocked By"));
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
