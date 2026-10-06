//! The admin list lookup page (`/admin/lookup/list`): the list's facts,
//! and its members and subscribers behind tabs, each turned by page
//! number.
//!
//! Like the DID lookup ([`super::admin_did`]) it shares no surface with
//! the public pages: its template and its stylesheet rules are the
//! admin's own.

use std::collections::HashMap;
use std::sync::Arc;

use askama::Template;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use farsight_api::handlers;
use farsight_api::params::Params;
use farsight_core::Did;
use farsight_storage::public::{self as store, Counted};
use farsight_storage::ui_rows::{Filter, Section as Rows};

use super::admin_did::{COUNT_CAP, PAGE_ROWS, Pager};
use super::{
    Nav, Stat, WebState, count, coverage_words, gate, nav, parse_list_ref, permit, resolve_handle,
    stat,
};
use crate::cells;
use crate::common::render_private;
use crate::public::paging::{self, Total};
use crate::public::text::{LISTBLOCK, LISTITEM, Record, Stamp, clean, thousands};
use crate::public::warming::Asked;
use crate::rows;

/// The page's address.
pub const BASE: &str = "/admin/lookup/list";

/// The tables, in tab order: `(id, label, page parameter)`.
pub const TABLES: [(&str, &str, &str); 2] = [
    ("members", "Members", "page"),
    ("subscribers", "Subscribers", "subs"),
];

/// The status list that leaves no account out: these tables show every
/// row, as they always have.
const NONE_HIDDEN: &str = "(-1)";

/// What the address asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Ask {
    /// The list's at-uri.
    uri: String,
    /// The table in view.
    tab: &'static str,
    /// The page of each table, in [`TABLES`] order.
    pages: [i64; 2],
}

impl Ask {
    fn read(uri: &str, q: &HashMap<String, String>) -> Ask {
        let tab = q
            .get("tab")
            .and_then(|t| TABLES.iter().find(|x| x.0 == t.as_str()))
            .map_or(TABLES[0].0, |x| x.0);
        let page = |key: &str| {
            q.get(key)
                .and_then(|v| v.parse::<i64>().ok())
                .filter(|n| (1..=paging::MAX_PAGE).contains(n))
                .unwrap_or(1)
        };
        Ask {
            uri: uri.to_owned(),
            tab,
            pages: [page(TABLES[0].2), page(TABLES[1].2)],
        }
    }

    /// The address of this view with `tab` in front and table `turn` on
    /// page `to`.
    fn link(&self, tab: &str, turn: Option<(usize, i64)>) -> String {
        let mut s = url::form_urlencoded::Serializer::new(String::new());
        s.append_pair("q", &self.uri);
        if tab != TABLES[0].0 {
            s.append_pair("tab", tab);
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

/// A row of either table: an account, the record that put it there, and
/// when the record says it was made.
#[derive(Debug, Clone)]
pub struct Row {
    /// The member or the subscriber, rendered.
    pub who: String,
    /// The record's at-uri: the listitem (in the owner's repo) or the
    /// listblock (in the subscriber's).
    pub uri: String,
    /// The record viewer's page for it, when one is configured.
    pub view: Option<String>,
    /// The record's stated creation time.
    pub when: Option<Stamp>,
}

/// One table.
#[derive(Debug, Clone, Default)]
pub struct Table {
    /// The count for the heading.
    pub count: Option<String>,
    /// Coverage in words.
    pub coverage: String,
    /// What went wrong, if the rows could not be read.
    pub error: Option<String>,
    /// The rows of this page.
    pub rows: Vec<Row>,
    /// The page controls, rendered.
    pub pager: String,
}

/// The list in view.
pub struct Subject {
    /// Its at-uri.
    pub uri: String,
    /// Its history page.
    pub history: String,
    /// Its facts.
    pub facts: Vec<Stat>,
    /// The tabs.
    pub tabs: Vec<Tab>,
    /// The id of the table in view.
    pub active: &'static str,
    /// Members.
    pub members: Table,
    /// Subscribers: the accounts with a listblock on the list.
    pub subscribers: Table,
}

/// The list lookup page.
#[derive(Template)]
#[template(path = "lookup_list.html")]
pub struct ListPage {
    /// Navigation.
    pub nav: Nav,
    /// The query.
    pub q: String,
    /// Error.
    pub error: Option<String>,
    /// The list, once the query names one.
    pub subject: Option<Subject>,
}

fn total_of(n: Option<i64>) -> Total {
    match n {
        Some(n) if n > COUNT_CAP => Total::MoreThan(COUNT_CAP),
        Some(n) => Total::Rows(n),
        None => Total::Unknown,
    }
}

fn count_words(n: i64) -> String {
    if n > COUNT_CAP {
        format!("more than {}", thousands(COUNT_CAP))
    } else {
        thousands(n)
    }
}

/// Reads one table: its count and the rows of its page.
#[allow(clippy::too_many_arguments)]
async fn table(
    st: &WebState,
    asked: &mut Asked,
    ask: &Ask,
    which: usize,
    list_id: i64,
    viewer: &str,
    owner: &str,
    into: &mut Table,
) {
    let (section, counted, collection) = if which == 0 {
        (Rows::ListMembers, Counted::ListMembers, LISTITEM)
    } else {
        (Rows::ListBlockers, Counted::ListBlockers, LISTBLOCK)
    };
    let n = match st.api.pool.acquire().await {
        Ok(mut conn) => store::bounded_count_hiding(
            &mut conn,
            counted,
            list_id,
            &[],
            NONE_HIDDEN,
            None,
            COUNT_CAP,
        )
        .await
        .ok(),
        Err(_) => None,
    };
    into.count = n.map(count_words);
    match rows::numbered(
        st,
        section,
        list_id,
        Filter::default(),
        ask.pages[which],
        PAGE_ROWS,
    )
    .await
    {
        Ok(p) => {
            let cfg = st.api.config.current();
            let dids: Vec<String> = p.rows.iter().map(|b| b.did.clone()).collect();
            crate::public::handles::recall(st, &cfg.config, &dids).await;
            into.rows = p
                .rows
                .iter()
                .map(|b| {
                    // A listitem is in the owner's repo; a listblock in
                    // the subscriber's.
                    let authority = if which == 0 { owner } else { b.did.as_str() };
                    let record = Record::of(viewer, authority, collection, &b.rkey);
                    Row {
                        who: cells::account(st, asked, &b.did)
                            .render()
                            .unwrap_or_default(),
                        uri: record.uri,
                        view: record.href,
                        when: b.created_at.map(Stamp::of),
                    }
                })
                .collect();
            let (id, label, _) = TABLES[which];
            into.pager = Pager::build(id, label, ask.pages[which], total_of(n), p.more, |to| {
                ask.link(id, Some((which, to)))
            })
            .render()
            .unwrap_or_default();
        }
        Err(e) => into.error = Some(e.message),
    }
}

/// `GET /admin/lookup/list`.
pub async fn lookup_list(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let query = q.get("q").map(|s| s.trim().to_owned()).unwrap_or_default();
    let mut page = ListPage {
        nav: nav(&Some(s)),
        q: query.clone(),
        error: None,
        subject: None,
    };
    if query.is_empty() {
        return render_private(&page);
    }
    let Some((actor, rkey)) = parse_list_ref(&query) else {
        page.error =
            Some("Enter an at://…/app.bsky.graph.list/… URI or a bsky.app list URL.".into());
        return render_private(&page);
    };
    let owner = if actor.starts_with("did:") {
        Did::parse(&actor).map_err(|e| e.to_string())
    } else {
        resolve_handle(&st.safe, &actor).await
    };
    let owner = match owner {
        Ok(d) => d,
        Err(e) => {
            page.error = Some(e);
            return render_private(&page);
        }
    };
    let uri = format!("at://{}/app.bsky.graph.list/{rkey}", owner.as_str());
    let _permit = match permit(&st).await {
        Ok(p) => p,
        Err(e) => {
            page.error = Some(e);
            return render_private(&page);
        }
    };
    let cfg = st.api.config.current();
    let viewer = cfg.config.public_ui.record_viewer_url.as_str();
    let ask = Ask::read(&uri, &q);
    let mut asked = Asked::new(&cfg.config);
    let info = match st.api.pool.acquire().await {
        Ok(mut conn) => farsight_storage::queries::list_info(&mut conn, owner.as_str(), &rkey)
            .await
            .ok()
            .flatten(),
        Err(_) => None,
    };
    let mut facts = Vec::new();
    let mut members = Table::default();
    let mut subscribers = Table::default();
    // Called for the list's facts and its freshness; the rows are read
    // below.
    let p = vec![
        ("list".to_owned(), uri.clone()),
        ("limit".to_owned(), "1".to_owned()),
    ];
    let mut serves = false;
    match handlers::get_list_members(&st.api, &Params::from_pairs(p)).await {
        Ok(r) => {
            let b = &r.body;
            facts.push(stat("Owner", owner.as_str()));
            facts.push(stat("Purpose", b["purpose"].as_str().unwrap_or("—")));
            facts.push(stat("Name", b["name"].as_str().unwrap_or("—")));
            // What the list says about itself, as stored: read from the
            // record when it was applied, or on the first view of the
            // list's public page.
            let about = match (&info, st.api.pool.acquire().await) {
                (Some(i), Ok(mut conn)) => farsight_storage::queries::list_about(&mut conn, i.id)
                    .await
                    .ok(),
                _ => None,
            };
            facts.push(stat(
                "Description",
                match about {
                    Some(a) if a.read => a
                        .description
                        .map(|d| clean(&d.replace(['\r', '\n'], " ")))
                        .filter(|d| !d.trim().is_empty())
                        .unwrap_or_else(|| "—".to_owned()),
                    Some(_) => "Not read yet".to_owned(),
                    None => "—".to_owned(),
                },
            ));
            facts.push(stat("State", b["state"].as_str().unwrap_or("—")));
            facts.push(match b["listblockCount"].as_i64() {
                Some(n) => count("Listblocks", n),
                None => stat("Listblocks", "—"),
            });
            facts.push(stat(
                "Capped",
                if b["capped"].as_bool().unwrap_or(false) {
                    "yes"
                } else {
                    "no"
                },
            ));
            // Members are served as the API serves them: for a list that
            // is indexed, whose owner is shown.
            serves = matches!(b["state"].as_str(), Some("ready" | "retained"))
                && info.as_ref().is_some_and(|i| {
                    !farsight_storage::codes::actor_status::is_hidden(i.owner_status)
                });
            members.coverage = coverage_words(&b["freshness"]);
        }
        Err(e) => members.error = Some(e.message),
    }
    if let Some(info) = &info {
        facts.push(count("Stored items", i64::from(info.item_count)));
        if serves {
            table(
                &st,
                &mut asked,
                &ask,
                0,
                info.id,
                viewer,
                owner.as_str(),
                &mut members,
            )
            .await;
        }
        table(
            &st,
            &mut asked,
            &ask,
            1,
            info.id,
            viewer,
            owner.as_str(),
            &mut subscribers,
        )
        .await;
    }
    asked.submit(&st);
    page.subject = Some(Subject {
        history: crate::public::text::admin_list_history_href(owner.as_str(), &rkey),
        facts,
        tabs: TABLES
            .iter()
            .map(|(id, label, _)| Tab {
                id,
                label,
                href: ask.link(id, None),
                active: *id == ask.tab,
            })
            .collect(),
        active: ask.tab,
        members,
        subscribers,
        uri,
    });
    render_private(&page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_address_names_the_tab_and_each_tables_page() {
        let uri = "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/3k";
        let q: HashMap<String, String> = [
            ("tab", "subscribers"),
            ("page", "2"),
            ("subs", "7"),
            ("mc", "old"),
        ]
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
        let a = Ask::read(uri, &q);
        assert_eq!((a.tab, a.pages), ("subscribers", [2, 7]));
        let link = a.link("subscribers", Some((1, 8)));
        assert!(
            link.starts_with("/admin/lookup/list?q=at%3A%2F%2F"),
            "{link}"
        );
        assert!(link.ends_with("&tab=subscribers&page=2&subs=8"), "{link}");
        // An old cursor parameter is not carried along.
        assert!(!link.contains("mc="));
        let first = Ask::read(uri, &HashMap::new());
        assert_eq!((first.tab, first.pages), ("members", [1, 1]));
        assert!(!first.link("members", None).contains('&'));
    }
}
