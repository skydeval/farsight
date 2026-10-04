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
//! follow; the cursor is still offered.
//!
//! An account's "Blocked by" section holds [`BLOCKER_ROWS`] rows a page
//! and grows in place: its "Load more" link appends the next page's rows.
//! Every other section holds [`PAGE_ROWS`] and replaces itself.
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
use super::handles::{page_handle, take_budget};
use super::search::{self, Authority, Target};
use super::text::{Stamp, card_href, clean, did_href, list_href, list_uri, paragraphs, thousands};
use super::warming::Asked;
use super::{
    BLOCKER_ROWS, COUNT_CAP, Cache, Chrome, Fail, OG_ACCOUNT, OG_INSTANCE, OG_LIST, PAGE_ROWS, Req,
    Withheld, chrome, metrics as m, page, redirect,
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

/// An account as a row names it: `@handle` when a verified handle is
/// already cached, the DID otherwise. Either way a link to the account's
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
}

fn who(r: &Req<'_>, asked: &mut Asked, did: &str) -> Who {
    Who {
        did: did.to_owned(),
        href: did_href(did),
        card: card_href(did),
        handle: asked.handle(r.st, did).map(|h| clean(&h)),
    }
}

/// The "next" link of a section: a plain link that htmx upgrades to an
/// in-place swap of the section, or, where the section grows in place
/// (`more`), to an append of the next page's rows to the table body
/// `#{section}-rows`. The response is always the full page.
#[derive(Debug, Clone, Template)]
#[template(path = "_pagination.html")]
pub struct Pager {
    /// The section's fragment id.
    pub section: &'static str,
    /// Target without the fragment, if there is a next page.
    pub next: Option<String>,
    /// "Load more": the link appends instead of replacing. Its paragraph
    /// `#{section}-more` is rendered even without a link, so that the last
    /// page's response takes the link away.
    pub more: bool,
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
    /// Rows shown.
    pub rows: Vec<R>,
    /// Next link.
    pub pager: Pager,
    /// Empty-state line: only on a first page with nothing after it.
    pub empty: Option<&'static str>,
}

/// The cursor parameters of the pages. Cursors are opaque and unstable.
const DID_CURSORS: [&str; 3] = ["bc", "nc", "oc"];
const LIST_CURSORS: [&str; 2] = ["mc", "lc"];

/// `base?…` keeping the other sections' positions and setting `key`.
pub(crate) fn next_link(base: &str, q: &Params, keys: &[&str], key: &str, value: &str) -> String {
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

fn count_words(n: i64) -> String {
    if n > COUNT_CAP {
        format!("more than {}", thousands(COUNT_CAP))
    } else {
        thousands(n)
    }
}

/// A cursor that no longer parses gets a 400 page linking to the first
/// page; everything else keeps its meaning.
fn or_first_page(f: Fail, base: &str) -> Fail {
    match f {
        Fail::Bad { message, .. } => Fail::Bad {
            message,
            link: Some((base.to_owned(), "Open the first page".to_owned())),
        },
        other => other,
    }
}

fn empty_line<R>(rows: &[R], next: &Option<String>, on_first_page: bool) -> Option<&'static str> {
    (rows.is_empty() && next.is_none() && on_first_page).then_some(EMPTY)
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
}

impl Shown<'_> {
    async fn load<'a>(
        r: &Req<'_>,
        withheld: &'a Withheld,
        dids: Vec<String>,
    ) -> Result<Shown<'a>, Fail> {
        let status = if dids.is_empty() {
            HashMap::new()
        } else {
            let mut tx = r.st.api.read_tx().await?;
            let s = queries::actors(&mut tx, &dids).await?;
            tx.rollback().await?;
            s
        };
        Ok(Shown { withheld, status })
    }

    /// Whether a row naming `did`, in any role, may be shown.
    fn ok(&self, did: &str) -> bool {
        !self.withheld.excluded(did)
            && !self
                .status
                .get(did)
                .is_some_and(|a| actor_status::is_hidden(a.status))
    }
}

/// A count that is left out when it is zero (an empty section's heading
/// shows none) or when its query fails or times out; the section still
/// renders.
async fn count(r: &Req<'_>, what: Counted, key: i64, w: &Withheld) -> Option<String> {
    let mut tx = r.st.api.read_tx().await.ok()?;
    let n = store::bounded_count(&mut tx, what, key, &w.ids, COUNT_CAP)
        .await
        .ok()?;
    let _ = tx.rollback().await;
    (n > 0).then(|| count_words(n))
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

/// The withheld rule as the row queries apply it.
fn row_filter(w: &Withheld) -> Filter<'_> {
    Filter {
        hide_inactive: true,
        excluded: &w.ids,
    }
}

/// The rows of a section that may be shown, as the tables render them.
fn party_rows(r: &Req<'_>, asked: &mut Asked, shown: &Shown<'_>, rows: &[Row]) -> Vec<PartyRow> {
    rows.iter()
        .filter(|b| shown.ok(&b.did))
        .map(|b| PartyRow {
            who: who(r, asked, &b.did),
            when: b.created_at.map(Stamp::of),
        })
        .collect()
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
    blockers: Section<PartyRow>,
    lists: Section<ListRow>,
    outgoing: Option<Section<PartyRow>>,
    updated: Option<Stamp>,
}

/// `/did/{did}`.
pub async fn did(r: &Req<'_>, did: &Did, q: &Params) -> Result<Response, Fail> {
    let cfg = r.config();
    let base = did_href(did.as_str());
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

    // Called for its freshness; the rows are read below.
    let blocks = handlers::get_incoming_blocks(api, &freshness_params(&[("actor", did.as_str())]))
        .await?
        .body;
    let block_page = match actor {
        Some(a) => crate::rows::page(
            r.st,
            Rows::IncomingBlocks,
            a.id,
            row_filter(&withheld),
            q.get("bc"),
            BLOCKER_ROWS,
        )
        .await
        .map_err(|e| or_first_page(e.into(), &base))?,
        None => crate::rows::Page::default(),
    };
    // Called for its freshness; the rows are read below.
    let naming = handlers::get_lists_naming(api, &freshness_params(&[("actor", did.as_str())]))
        .await?
        .body;
    let naming_page = crate::rows::naming_page(
        r.st,
        actor.map(|a| a.id),
        &withheld.ids,
        q.get("nc"),
        PAGE_ROWS,
    )
    .await
    .map_err(|e| or_first_page(e.into(), &base))?;
    let show_outgoing = cfg.public_ui.show_outgoing_blocks;
    let (out_page, out_fresh) = if show_outgoing {
        let page = match actor {
            Some(a) => crate::rows::page(
                r.st,
                Rows::OutgoingBlocks,
                a.id,
                row_filter(&withheld),
                q.get("oc"),
                PAGE_ROWS,
            )
            .await
            .map_err(|e| or_first_page(e.into(), &base))?,
            None => crate::rows::Page::default(),
        };
        (page, Some(public_ui::outgoing_freshness(api, did).await?))
    } else {
        (crate::rows::Page::default(), None)
    };

    let mut dids: Vec<String> = block_page.rows.iter().map(|b| b.did.clone()).collect();
    dids.extend(naming_page.rows.iter().map(|l| l.owner_did.clone()));
    dids.extend(out_page.rows.iter().map(|o| o.did.clone()));
    let shown = Shown::load(r, &withheld, dids).await?;
    let mut asked = Asked::new(cfg);

    let b_next = block_page
        .next
        .as_deref()
        .map(|c| next_link(&base, q, &DID_CURSORS, "bc", c));
    let b_rows = party_rows(r, &mut asked, &shown, &block_page.rows);
    let blockers = Section {
        count: match actor {
            Some(a) => count(r, Counted::IncomingBlocks, a.id, &withheld).await,
            None => None,
        },
        empty: empty_line(&b_rows, &b_next, q.get("bc").is_none()),
        rows: b_rows,
        pager: Pager {
            section: "blockers",
            next: b_next,
            more: true,
        },
    };

    let l_next = naming_page
        .next
        .as_deref()
        .map(|c| next_link(&base, q, &DID_CURSORS, "nc", c));
    let l_rows: Vec<ListRow> = naming_page
        .rows
        .iter()
        .filter(|l| shown.ok(&l.owner_did))
        .map(|l| ListRow {
            uri: list_uri(&l.owner_did, &l.rkey),
            href: list_href(&l.owner_did, &l.rkey),
            name: l.name.as_deref().map(clean).filter(|n| !n.is_empty()),
            owner: who(r, &mut asked, &l.owner_did),
            listblocks: l.listblock_count.to_string(),
            added: l.added_at.map(Stamp::of),
        })
        .collect();
    let lists = Section {
        count: None,
        empty: empty_line(&l_rows, &l_next, q.get("nc").is_none()),
        rows: l_rows,
        pager: Pager {
            section: "lists",
            next: l_next,
            more: false,
        },
    };

    // The sections this page rendered; the earliest dates the page.
    let mut fresh: Vec<&Value> = vec![&blocks["freshness"], &naming["freshness"]];
    let outgoing = match &out_fresh {
        Some(f) => {
            let next = out_page
                .next
                .as_deref()
                .map(|c| next_link(&base, q, &DID_CURSORS, "oc", c));
            let rows = party_rows(r, &mut asked, &shown, &out_page.rows);
            fresh.push(f);
            Some(Section {
                count: match actor {
                    Some(a) => count(r, Counted::OutgoingBlocks, a.id, &withheld).await,
                    None => None,
                },
                empty: empty_line(&rows, &next, q.get("oc").is_none()),
                rows,
                pager: Pager {
                    section: "outgoing",
                    next,
                    more: false,
                },
            })
        }
        None => None,
    };

    asked.submit(r.st);
    let og_title = match &handle {
        Some(h) => format!("@{h} ({did})"),
        None => did.to_string(),
    };
    let t = DidPage {
        c: chrome(cfg, did.as_str(), &og_title, OG_ACCOUNT, &base),
        did: did.to_string(),
        handle,
        blockers,
        lists,
        outgoing,
        updated: last_updated(&fresh),
    };
    Ok(page(&t, StatusCode::OK, cfg, Cache::Public(30), true))
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
    let member_page = if show_members {
        crate::rows::page(
            r.st,
            Rows::ListMembers,
            info.id,
            row_filter(&withheld),
            q.get("mc"),
            PAGE_ROWS,
        )
        .await
        .map_err(|e| or_first_page(e.into(), &base))?
    } else {
        crate::rows::Page::default()
    };
    let blocker_page = crate::rows::page(
        r.st,
        Rows::ListBlockers,
        info.id,
        row_filter(&withheld),
        q.get("lc"),
        PAGE_ROWS,
    )
    .await
    .map_err(|e| or_first_page(e.into(), &base))?;
    let blockers_fresh = public_ui::listblock_freshness(api)?;

    let mut dids: Vec<String> = member_page.rows.iter().map(|m| m.did.clone()).collect();
    dids.extend(blocker_page.rows.iter().map(|b| b.did.clone()));
    let shown = Shown::load(r, &withheld, dids).await?;
    let mut asked = Asked::new(cfg);

    let mut fresh: Vec<&Value> = Vec::new();
    let members = if show_members {
        let next = member_page
            .next
            .as_deref()
            .map(|c| next_link(&base, q, &LIST_CURSORS, "mc", c));
        let rows = party_rows(r, &mut asked, &shown, &member_page.rows);
        fresh.push(&listing["freshness"]);
        Some(Section {
            count: None,
            empty: empty_line(&rows, &next, q.get("mc").is_none()),
            rows,
            pager: Pager {
                section: "members",
                next,
                more: false,
            },
        })
    } else {
        None
    };

    let next = blocker_page
        .next
        .as_deref()
        .map(|c| next_link(&base, q, &LIST_CURSORS, "lc", c));
    let rows = party_rows(r, &mut asked, &shown, &blocker_page.rows);
    asked.submit(r.st);
    fresh.push(&blockers_fresh);
    let blockers = Section {
        count: count(r, Counted::ListBlockers, info.id, &withheld).await,
        empty: empty_line(&rows, &next, q.get("lc").is_none()),
        rows,
        pager: Pager {
            section: "listblockers",
            next,
            more: false,
        },
    };

    let name = listing["name"]
        .as_str()
        .map(clean)
        .filter(|n| !n.is_empty());
    let og_title = match &name {
        Some(n) => format!("{n} ({uri})"),
        None => uri.clone(),
    };
    let t = ListPage {
        c: chrome(cfg, &uri, &og_title, OG_LIST, &base),
        name,
        purpose: listing["purpose"].as_str().map(purpose_words),
        owner: Who {
            did: owner.to_string(),
            href: did_href(owner.as_str()),
            card: card_href(owner.as_str()),
            handle,
        },
        state,
        capped: listing["capped"].as_bool().unwrap_or(false),
        stored_members: thousands(i64::from(info.item_count)),
        members,
        blockers,
        updated: last_updated(&fresh),
        uri,
    };
    Ok(page(&t, StatusCode::OK, cfg, Cache::Public(30), true))
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
    fn links_keep_the_other_sections() {
        let q = Params::parse("bc=AAA&nc=BBB&utm=x");
        assert_eq!(
            next_link("/did/did:plc:x", &q, &DID_CURSORS, "bc", "CCC"),
            "/did/did:plc:x?nc=BBB&bc=CCC"
        );
        assert_eq!(count_words(1_000), "1,000");
        assert_eq!(count_words(1_001), "more than 1,000");
    }
}
