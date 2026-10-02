//! The public pages: home, search, an account and a list.
//!
//! Sections are built from the handlers behind the stable read queries,
//! called in-process with default filtering (the UI never passes
//! `includeInactive`), plus read-only storage queries for the two sections
//! without an NSID. Handler output is filtered afterwards with one batched
//! status lookup for the page's DIDs, so a page can hold fewer than 50
//! rows while more follow; the cursor is still offered.
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
use farsight_api::{cursor, handlers, public_ui};
use farsight_core::{AtUri, Did, RecordKey};
use farsight_storage::codes::actor_status;
use farsight_storage::public::{self as store, Counted};
use farsight_storage::queries::{self, ActorRef};
use serde_json::{Value, json};

use super::coverage::{EMPTY, last_updated};
use super::handles::{page_handle, row_handle, take_budget};
use super::search::{self, Authority, Target};
use super::text::{
    BLOCK, LISTBLOCK, Record, Stamp, card_href, clean, did_href, list_href, list_uri, paragraphs,
    thousands,
};
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

/// An account as a row names it: `@handle` when a verified handle is
/// already cached, the DID otherwise. Either way a link to the account's
/// page whose `title` is the DID, and on which the script opens the
/// profile card. Rendering a row never makes an outbound request.
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

fn who(r: &Req<'_>, did: &str) -> Who {
    Who {
        did: did.to_owned(),
        href: did_href(did),
        card: card_href(did),
        handle: row_handle(r.st, did).map(|h| clean(&h)),
    }
}

/// The "next" link of a section: a plain link that htmx upgrades to an
/// in-place swap of the section. The response is always the full page.
#[derive(Debug, Clone, Template)]
#[template(path = "_pagination.html")]
pub struct Pager {
    /// The section's fragment id.
    pub section: &'static str,
    /// Target without the fragment, if there is a next page.
    pub next: Option<String>,
}

/// An account row with one author-stated time.
#[derive(Debug, Clone)]
pub struct PartyRow {
    /// The account.
    pub who: Who,
    /// The block or listblock record. Member rows have none: the API's
    /// member rows do not carry the listitem record.
    pub record: Option<Record>,
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

fn stamp_of(v: &Value) -> Option<Stamp> {
    v.as_str().and_then(Stamp::parse)
}

fn dids_of<'a>(rows: &'a [Value], key: &str) -> impl Iterator<Item = String> + 'a {
    let key = key.to_owned();
    rows.iter()
        .filter_map(move |v| v[key.as_str()].as_str().map(str::to_owned))
}

fn handler_params(pairs: &[(&str, &str)], cursor: Option<&str>) -> Params {
    let mut p: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    p.push(("limit".to_owned(), PAGE_ROWS.to_string()));
    if let Some(c) = cursor {
        p.push(("cursor".to_owned(), c.to_owned()));
    }
    Params::from_pairs(p)
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
                "/public",
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
                    "/public/search",
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

/// `/public/search?q=…`: resolves the input and redirects under
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

/// `/public/did/{did}`.
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

    let blocks = handlers::get_incoming_blocks(
        api,
        &handler_params(&[("actor", did.as_str())], q.get("bc")),
    )
    .await
    .map_err(|e| or_first_page(e.into(), &base))?
    .body;
    let naming = handlers::get_lists_naming(
        api,
        &handler_params(&[("actor", did.as_str())], q.get("nc")),
    )
    .await
    .map_err(|e| or_first_page(e.into(), &base))?
    .body;
    let show_outgoing = cfg.public_ui.show_outgoing_blocks;
    let out_after = cursor::rkey(q.get("oc")).map_err(|e| or_first_page(e.into(), &base))?;
    let (out_rows, out_fresh) = if show_outgoing {
        let rows = match actor {
            Some(a) => {
                let mut tx = api.read_tx().await?;
                let rows = store::outgoing_blocks(
                    &mut tx,
                    a.id,
                    &withheld.ids,
                    out_after.as_deref(),
                    PAGE_ROWS,
                )
                .await?;
                tx.rollback().await?;
                rows
            }
            None => Vec::new(),
        };
        (rows, Some(public_ui::outgoing_freshness(api, did).await?))
    } else {
        (Vec::new(), None)
    };

    let block_rows = blocks["blocks"].as_array().cloned().unwrap_or_default();
    let list_rows = naming["lists"].as_array().cloned().unwrap_or_default();
    let lists: Vec<(AtUri, &Value)> = list_rows
        .iter()
        .filter_map(|l| Some((AtUri::parse(l["uri"].as_str()?).ok()?, l)))
        .collect();
    let mut dids: Vec<String> = dids_of(&block_rows, "did").collect();
    dids.extend(lists.iter().map(|(u, _)| u.authority.as_str().to_owned()));
    dids.extend(out_rows.iter().map(|o| o.did.clone()));
    let shown = Shown::load(r, &withheld, dids).await?;

    let viewer = cfg.public_ui.record_viewer_url.as_str();
    let b_next = blocks["cursor"]
        .as_str()
        .map(|c| next_link(&base, q, &DID_CURSORS, "bc", c));
    let b_rows: Vec<PartyRow> = block_rows
        .iter()
        .filter_map(|b| {
            let d = b["did"].as_str()?;
            shown.ok(d).then(|| PartyRow {
                who: who(r, d),
                record: b["uri"].as_str().map(|u| Record::of_uri(viewer, u)),
                when: stamp_of(&b["createdAt"]),
            })
        })
        .collect();
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
        },
    };

    let l_next = naming["cursor"]
        .as_str()
        .map(|c| next_link(&base, q, &DID_CURSORS, "nc", c));
    let l_rows: Vec<ListRow> = lists
        .iter()
        .filter(|(u, _)| shown.ok(u.authority.as_str()))
        .map(|(u, l)| ListRow {
            uri: u.to_string(),
            href: list_href(u.authority.as_str(), u.rkey.as_str()),
            name: l["name"].as_str().map(clean).filter(|n| !n.is_empty()),
            owner: who(r, u.authority.as_str()),
            listblocks: l["listblockCount"].as_i64().unwrap_or(0).to_string(),
            added: stamp_of(&l["addedAt"]),
        })
        .collect();
    let lists = Section {
        count: None,
        empty: empty_line(&l_rows, &l_next, q.get("nc").is_none()),
        rows: l_rows,
        pager: Pager {
            section: "lists",
            next: l_next,
        },
    };

    // The sections this page rendered; the earliest dates the page.
    let mut fresh: Vec<&Value> = vec![&blocks["freshness"], &naming["freshness"]];
    let outgoing = match &out_fresh {
        Some(f) => {
            let next = (out_rows.len() as i64 == PAGE_ROWS)
                .then(|| out_rows.last())
                .flatten()
                .map(|o| {
                    next_link(
                        &base,
                        q,
                        &DID_CURSORS,
                        "oc",
                        &cursor::encode(&[json!(o.rkey)]),
                    )
                });
            let rows: Vec<PartyRow> = out_rows
                .iter()
                .filter(|o| shown.ok(&o.did))
                .map(|o| PartyRow {
                    who: who(r, &o.did),
                    record: Some(Record::of(viewer, did.as_str(), BLOCK, &o.rkey)),
                    when: o.created_at.map(Stamp::of),
                })
                .collect();
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
                },
            })
        }
        None => None,
    };

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

/// `/public/list/{did}/{rkey}`.
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

    let listing =
        handlers::get_list_members(api, &handler_params(&[("list", uri.as_str())], q.get("mc")))
            .await
            .map_err(|e| or_first_page(e.into(), &base))?
            .body;
    let (state, show_members) = state_words(listing["state"].as_str().unwrap_or(""));
    let after = cursor::id_rkey(q.get("lc")).map_err(|e| or_first_page(e.into(), &base))?;
    let blocker_rows = {
        let mut tx = api.read_tx().await?;
        let rows = store::list_blockers(
            &mut tx,
            info.id,
            &withheld.ids,
            after.as_ref().map(|(a, k)| (*a, k.as_str())),
            PAGE_ROWS,
        )
        .await?;
        tx.rollback().await?;
        rows
    };
    let blockers_fresh = public_ui::listblock_freshness(api)?;

    let member_rows = listing["members"].as_array().cloned().unwrap_or_default();
    let mut dids: Vec<String> = dids_of(&member_rows, "did").collect();
    dids.extend(blocker_rows.iter().map(|b| b.did.clone()));
    let shown = Shown::load(r, &withheld, dids).await?;

    let viewer = cfg.public_ui.record_viewer_url.as_str();
    let mut fresh: Vec<&Value> = Vec::new();
    let members = if show_members {
        let next = listing["cursor"]
            .as_str()
            .map(|c| next_link(&base, q, &LIST_CURSORS, "mc", c));
        let rows: Vec<PartyRow> = member_rows
            .iter()
            .filter_map(|m| {
                let d = m["did"].as_str()?;
                shown.ok(d).then(|| PartyRow {
                    who: who(r, d),
                    record: None,
                    when: stamp_of(&m["addedAt"]),
                })
            })
            .collect();
        fresh.push(&listing["freshness"]);
        Some(Section {
            count: None,
            empty: empty_line(&rows, &next, q.get("mc").is_none()),
            rows,
            pager: Pager {
                section: "members",
                next,
            },
        })
    } else {
        None
    };

    let next = (blocker_rows.len() as i64 == PAGE_ROWS)
        .then(|| blocker_rows.last())
        .flatten()
        .map(|b| {
            next_link(
                &base,
                q,
                &LIST_CURSORS,
                "lc",
                &cursor::encode(&[json!(b.author_id), json!(b.rkey)]),
            )
        });
    let rows: Vec<PartyRow> = blocker_rows
        .iter()
        .filter(|b| shown.ok(&b.did))
        .map(|b| PartyRow {
            who: who(r, &b.did),
            record: Some(Record::of(viewer, &b.did, LISTBLOCK, &b.rkey)),
            when: b.created_at.map(Stamp::of),
        })
        .collect();
    fresh.push(&blockers_fresh);
    let blockers = Section {
        count: count(r, Counted::ListBlockers, info.id, &withheld).await,
        empty: empty_line(&rows, &next, q.get("lc").is_none()),
        rows,
        pager: Pager {
            section: "listblockers",
            next,
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
            next_link("/public/did/did:plc:x", &q, &DID_CURSORS, "bc", "CCC"),
            "/public/did/did:plc:x?nc=BBB&bc=CCC"
        );
        assert_eq!(count_words(1_000), "1,000");
        assert_eq!(count_words(1_001), "more than 1,000");
    }
}
