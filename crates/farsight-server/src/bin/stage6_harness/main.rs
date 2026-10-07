//! `farsight-stage6-harness`: integration tests of the public UI (sticky
//! bar, `/enter`, admin-only history, one "Last updated" line, profile
//! cards, record links, local times, the theme toggle). Every assertion
//! reads real responses from a real `farsight` process over HTTP and
//! real stored rows; expectations that depend on data are derived from
//! SQL against the same database. The parts only a browser can show are
//! probed in headless Chromium (`--browser`).
//!
//! Sections:
//!
//! - 1: toggle gates, config validation, the first settings save,
//!   the enable-confirmation flow;
//! - 2: routes: `/enter`, the sections against stored rows, the "On
//!   lists" table;
//! - 3: search; 4: what a public page says about coverage;
//! - 5: admin history; 6–8: the withheld rule; 9: OpenGraph;
//! - 10: rate classes and the render bound; 11: cache and security
//!   headers; 12: every `[public_ui]` key, hot; 13: robots;
//! - 14: caller independence, handles, metrics;
//! - 15: record links; 16: profile cards; 17: rules every page keeps;
//! - 18: the browser.
//!
//! `--keep` keeps the Postgres container; `--skip-live` skips the checks
//! that need the network (a real handle, a real account's card);
//! `--browser` runs the browser probes in the Playwright container;
//! `--hold` leaves the seeded server running after the checks, public UI
//! on, until the harness is interrupted.

#[allow(dead_code)]
#[path = "../stage3_harness/seed.rs"]
mod seed;
#[allow(dead_code)]
#[path = "../stage3_harness/support.rs"]
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use serde_json::Value;
use sqlx::PgPool;

use crate::seed::did;
use crate::support::{
    ADMIN_DID, Checks, Http, Pg, Resp, Server, admin_session, csrf_of, enc, free_port, set_cookie,
};

const HOSTNAME: &str = "farsight.test";
const NS: &str = "app.nearhorizon.farsight";
/// A real account with a stable handle, for the live checks.
const LIVE_HANDLE: &str = "bsky.app";
const LIVE_DID: &str = "did:plc:z72i7hdynmk6r22z27h6tvur";
const PLC: &str = "https://plc.directory";
/// A did:web account whose host does not exist.
const WEB_DID: &str = "did:web:farsight-harness-no-such-host.invalid";
/// An address nothing answers from (TEST-NET-1): a PLC directory that
/// never responds.
const SLOW_PLC: &str = "https://192.0.2.1";

const EMPTY: &str = "None on record at this instance.";
const WITHHELD_ACCOUNT: &str = "Data for this account is not shown on this instance.";
const WITHHELD_LIST: &str = "Data for this list is not shown on this instance.";
const SHORT_CARD: &str = "Profile not available right now.";
const VIEWER: &str = "https://viewer.example/at/{authority}/{collection}/{rkey}";
/// What no public page prints.
const COVERAGE_WORDS: [&str; 9] = [
    "class=\"coverage",
    "id=\"coverage\"",
    "<strong>Partial.</strong>",
    "<strong>Complete.</strong>",
    "Best effort.",
    "<details>",
    "Accounts that are not active are not shown.",
    "has chosen not to show some accounts",
    "Removed records",
];
const CSP: &str = "default-src 'none'; style-src 'self'; script-src 'self'; img-src 'self'; \
                   connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";
const CSP_AVATARS: &str = "default-src 'none'; style-src 'self'; script-src 'self'; \
                           img-src 'self' https:; connect-src 'self'; base-uri 'none'; \
                           form-action 'self'; frame-ancestors 'none'";
const BROWSER_IMAGE: &str = "mcr.microsoft.com/playwright:v1.48.0-jammy";
const BROWSER_SCRIPT: &str = include_str!("../../../../../scripts/stage6-browser-probes.mjs");

fn config_toml(
    dsn: &str,
    admin_token: &str,
    metrics_bind: &str,
    plc: &str,
    access: &str,
    public_ui: &str,
) -> String {
    format!(
        r#"[server]
hostname = "{HOSTNAME}"
contact = "mailto:ops@{HOSTNAME}"

[storage]
database_url = "{dsn}"
budget_bytes = 70000000000

[firehose]
urls = ["ws://127.0.0.1:9"]

[backfill]
plc_url = "{plc}"

[access]
reads = "public"
admin_ui = true
admin_did = "{ADMIN_DID}"
{access}

[public_ui]
handle_warming_enabled = false
handle_rps = 2
{public_ui}

[auth]
admin_token_sha256 = "{}"

[metrics]
bind = "{metrics_bind}"
"#,
        farsight_api::auth::hex(&farsight_api::auth::sha256(admin_token))
    )
}

/// The harness's view of one server.
struct H {
    base: String,
    metrics: String,
    pool: PgPool,
    admin: Http,
    cookie: String,
    csrf: String,
    form: BTreeMap<&'static str, String>,
    /// This server's `config.toml`.
    config_path: String,
    /// This server's log.
    next_ip: AtomicU32,
    /// Every public HTML body fetched, for the relative-time check.
    pages: Mutex<Vec<(String, String)>>,
}

impl H {
    /// A client with a source address no request has used yet, so that no
    /// per-address rate bucket carries over between checks.
    fn fresh(&self) -> Http {
        let n = self.next_ip.fetch_add(1, Ordering::Relaxed);
        let ip: IpAddr = format!("127.{}.{}.{}", 1 + n / 62_500, (n / 250) % 250, 1 + n % 250)
            .parse()
            .expect("ip");
        Http::new(Some(ip))
    }

    async fn get_with(
        &self,
        http: &Http,
        path: &str,
        headers: &[(&str, &str)],
    ) -> Result<Resp, String> {
        let r = http.get(&format!("{}{path}", self.base), headers).await?;
        if r.header("content-type")
            .is_some_and(|c| c.starts_with("text/html"))
        {
            self.pages
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((path.to_owned(), r.text.clone()));
        }
        Ok(r)
    }

    /// `GET path` from a fresh address.
    async fn get(&self, path: &str) -> Result<Resp, String> {
        self.get_with(&self.fresh(), path, &[]).await
    }

    /// `GET path` with the admin session.
    async fn admin_get(&self, path: &str) -> Result<Resp, String> {
        self.admin
            .get(&format!("{}{path}", self.base), &[("cookie", &self.cookie)])
            .await
    }

    /// Posts the whole `config.toml` through the raw editor.
    async fn post_config(&self, text: &str) -> Result<Resp, String> {
        self.admin
            .post_form(
                &format!("{}/admin/settings", self.base),
                &[("cookie", &self.cookie)],
                &[("csrf", self.csrf.as_str()), ("config", text)],
            )
            .await
    }

    fn config_text(&self) -> Result<String, String> {
        std::fs::read_to_string(&self.config_path).map_err(|e| e.to_string())
    }

    async fn xrpc(&self, method: &str, q: &str) -> Result<Value, String> {
        let r = self
            .fresh()
            .get(&format!("{}/xrpc/{NS}.{method}?{q}", self.base), &[])
            .await?;
        if r.status != 200 {
            return Err(format!("{method}: {}", r.short()));
        }
        Ok(r.body)
    }

    async fn sql(&self, q: &str) -> Result<u64, String> {
        seed::exec(&self.pool, q).await
    }

    async fn n(&self, q: &str) -> Result<i64, String> {
        sqlx::query_scalar(q)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("{e}: {q}"))
    }

    async fn strings(&self, q: &str) -> Result<BTreeSet<String>, String> {
        let v: Vec<String> = sqlx::query_scalar(q)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("{e}: {q}"))?;
        Ok(v.into_iter().collect())
    }

    /// Lets the server's coverage snapshot catch up with seeded rows.
    async fn refresh(&self) -> Result<(), String> {
        seed::notify(&self.pool).await
    }

    async fn login(&mut self) -> Result<(), String> {
        self.cookie = admin_session(&self.pool, ADMIN_DID).await?;
        let page = self
            .admin
            .get(
                &format!("{}/admin/settings", self.base),
                &[("cookie", &self.cookie)],
            )
            .await?;
        self.csrf = csrf_of(&page.text).ok_or("no csrf on /admin/settings")?;
        Ok(())
    }

    fn form_pairs(&self) -> Vec<(&str, &str)> {
        let mut v: Vec<(&str, &str)> = vec![("csrf", self.csrf.as_str())];
        v.extend(self.form.iter().map(|(k, v)| (*k, v.as_str())));
        v
    }

    /// Posts the Public UI form with `changes` applied (`None` unchecks a
    /// box) and returns the first response: the settings page, or the
    /// confirmation page when the change turns the public UI on.
    async fn post_settings(
        &mut self,
        changes: &[(&'static str, Option<&str>)],
    ) -> Result<Resp, String> {
        for (k, v) in changes {
            match v {
                Some(v) => {
                    self.form.insert(k, (*v).to_owned());
                }
                None => {
                    self.form.remove(k);
                }
            }
        }
        self.admin
            .post_form(
                &format!("{}/admin/settings/public-ui", self.base),
                &[("cookie", &self.cookie)],
                &self.form_pairs(),
            )
            .await
    }

    async fn confirm(&self, token: &str) -> Result<Resp, String> {
        self.admin
            .post_form(
                &format!("{}/admin/settings/public-ui/confirm", self.base),
                &[("cookie", &self.cookie)],
                &[("csrf", self.csrf.as_str()), ("confirm", token)],
            )
            .await
    }

    /// Saves settings and requires the save to be accepted, confirming
    /// when asked.
    async fn set(&mut self, changes: &[(&'static str, Option<&str>)]) -> Result<Resp, String> {
        let mut r = self.post_settings(changes).await?;
        if let Some(t) = confirm_token(&r.text) {
            r = self.confirm(&t).await?;
        }
        if r.status != 200 || !(r.text.contains("Saved.") || r.text.contains("No changes.")) {
            return Err(format!("settings save refused: {}", banner(&r.text)));
        }
        Ok(r)
    }

    async fn metrics_text(&self) -> Result<String, String> {
        Ok(Http::new(None)
            .get(&format!("{}/metrics", self.metrics), &[])
            .await?
            .text)
    }
}

/// Sum of the samples of `name` whose labels include every pair.
fn metric(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    text.lines()
        .filter(|l| {
            l.starts_with(name)
                && l[name.len()..].starts_with(['{', ' '])
                && labels
                    .iter()
                    .all(|(k, v)| l.contains(&format!("{k}=\"{v}\"")))
        })
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

fn confirm_token(html: &str) -> Option<String> {
    let k = "name=\"confirm\" value=\"";
    let i = html.find(k)? + k.len();
    Some(html[i..][..html[i..].find('"')?].to_owned())
}

/// The text of the first banner on an admin page.
fn banner(html: &str) -> String {
    match html.find("class=\"banner") {
        Some(i) => {
            let rest = &html[i..];
            let a = rest.find('>').map_or(0, |x| x + 1);
            let b = rest.find("</div>").unwrap_or(rest.len());
            support::truncate(&rest[a..b], 400)
        }
        None => support::truncate(html, 200),
    }
}

/// The inside of the `<section>` whose id is `id`, whatever other
/// attributes its tag carries.
fn section<'a>(html: &'a str, id: &str) -> Option<&'a str> {
    let open = format!("<section id=\"{id}\"");
    let at = html.find(&open)?;
    let a = at + html[at..].find('>')? + 1;
    let b = html[a..].find("</section>")? + a;
    Some(&html[a..b])
}

/// The text of each `<th>` of `sec`, whatever attributes the cell carries.
fn heads_of(sec: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = sec;
    while let Some(i) = rest.find("<th") {
        rest = &rest[i + 3..];
        // `<thead>` is not a cell.
        if !rest.starts_with(['>', ' ']) {
            continue;
        }
        let (Some(a), Some(b)) = (rest.find('>'), rest.find("</th>")) else {
            break;
        };
        out.push(&rest[a + 1..b]);
        rest = &rest[b..];
    }
    out
}

/// The targets of the section nav's links, in order: one entry per nav.
fn section_navs(html: &str) -> Vec<Vec<&str>> {
    between(html, "aria-label=\"Sections\">", "</nav>")
        .into_iter()
        .map(|n| between(n, "href=\"#", "\""))
        .collect()
}

/// Every did:plc mentioned in `text`.
fn dids_in(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut rest = text;
    while let Some(i) = rest.find("did:plc:") {
        let end = i + 8 + 24;
        if rest.len() >= end && rest.is_char_boundary(end) {
            out.insert(rest[i..end].to_owned());
        }
        rest = &rest[i + 8..];
    }
    out
}

/// The DIDs of a section's rows: the accounts its row links point at.
fn row_dids(sec: &str) -> Vec<String> {
    let k = "href=\"/did/";
    let mut out = Vec::new();
    let mut rest = sec;
    while let Some(i) = rest.find(k) {
        let from = i + k.len();
        if let Some(j) = rest[from..].find('"') {
            let d = &rest[from..from + j];
            // Row links are bare DIDs; a page control's link carries a
            // query, a fragment or both.
            if !d.contains(['?', '/', '#']) {
                out.push(d.to_owned());
            }
        }
        rest = &rest[from..];
    }
    out
}

/// The inside of `<tag … id="id">` of an admin page, up to the next card.
fn card_div<'a>(html: &'a str, id: &str) -> Option<&'a str> {
    let open = format!("<div class=\"card\" id=\"{id}\">");
    let a = html.find(&open)? + open.len();
    let b = html[a..]
        .find("<div class=\"card\"")
        .map_or(html.len(), |x| x + a);
    Some(&html[a..b])
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The DIDs of an admin history section's rows: the accounts its lookup
/// links point at.
fn admin_row_dids(sec: &str) -> Vec<String> {
    let k = "href=\"/admin/lookup/did?q=";
    let mut out = Vec::new();
    let mut rest = sec;
    while let Some(i) = rest.find(k) {
        let from = i + k.len();
        if let Some(j) = rest[from..].find('"') {
            out.push(percent_decode(&rest[from..from + j]));
        }
        rest = &rest[from..];
    }
    out
}

/// Every `href` value of a page.
fn hrefs(html: &str) -> Vec<String> {
    let k = "href=\"";
    let mut out = Vec::new();
    let mut rest = html;
    while let Some(i) = rest.find(k) {
        let from = i + k.len();
        match rest[from..].find('"') {
            Some(j) => {
                out.push(rest[from..from + j].replace("&amp;", "&"));
                rest = &rest[from + j..];
            }
            None => break,
        }
    }
    out
}

/// The text between `open` and the next `close`, for each occurrence.
fn between<'a>(html: &'a str, open: &str, close: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut rest = html;
    while let Some(i) = rest.find(open) {
        let from = i + open.len();
        match rest[from..].find(close) {
            Some(j) => {
                out.push(&rest[from..from + j]);
                rest = &rest[from + j..];
            }
            None => break,
        }
    }
    out
}

/// `YYYY-MM-DDTHH:MM:SSZ`.
fn is_utc_instant(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 20
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            10 => *c == b'T',
            13 | 16 => *c == b':',
            19 => *c == b'Z',
            _ => c.is_ascii_digit(),
        })
}

/// The instant of the page's "Last updated" line.
fn updated_of(html: &str) -> Option<String> {
    let k = "Last updated <time datetime=\"";
    let a = html.find(k)? + k.len();
    Some(html[a..][..html[a..].find('"')?].to_owned())
}

/// The address a link of a public section's page controls leads to,
/// without its fragment: `rel` is `next` or `prev`.
fn step_of(sec: &str, rel: &str) -> Option<String> {
    let i = sec.find("<nav class=\"pager\"")?;
    let k = format!("rel=\"{rel} nofollow\" href=\"");
    let a = sec[i..].find(&k)? + i + k.len();
    let b = sec[a..].find('"')? + a;
    let href = sec[a..b].replace("&amp;", "&");
    Some(href.split('#').next().unwrap_or("").to_owned())
}

/// The next page of a public section, if it has one.
fn next_of(sec: &str) -> Option<String> {
    step_of(sec, "next")
}

/// What a public section's page controls read, in order: `←`, numbers
/// (the current one in brackets), `…`, `→`; an arrow that leads nowhere
/// is in parentheses.
fn controls_of(sec: &str) -> String {
    let Some(nav) = between(sec, "<nav class=\"pager\"", "</nav>")
        .first()
        .copied()
    else {
        return String::new();
    };
    let mut out = Vec::new();
    for part in nav.split('<').skip(1) {
        let Some((tag, text)) = part.split_once('>') else {
            continue;
        };
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        out.push(if tag.contains("aria-current") {
            format!("[{text}]")
        } else if tag.contains("aria-disabled") {
            format!("({text})")
        } else {
            text.to_owned()
        });
    }
    out.join(" ")
}

/// The "Next" link of an admin history section (a cursor, fetched by
/// htmx), if it has one.
fn cursor_next(sec: &str) -> Option<String> {
    let i = sec.find("class=\"pager\"")?;
    let k = "hx-get=\"";
    let a = sec[i..].find(k)? + i + k.len();
    let b = sec[a..].find('"')? + a;
    Some(sec[a..b].replace("&amp;", "&"))
}

fn meta<'a>(html: &'a str, key: &str) -> Option<&'a str> {
    let k = format!("\"{key}\" content=\"");
    let a = html.find(&k)? + k.len();
    Some(&html[a..][..html[a..].find('"')?])
}

/// A page with everything that legitimately differs between two renders
/// removed: times and the raw freshness blocks.
fn normalized(html: &str) -> String {
    let strip = |s: &str, open: &str, close: &str| {
        let mut out = String::new();
        let mut rest = s;
        while let Some(a) = rest.find(open) {
            out.push_str(&rest[..a]);
            match rest[a..].find(close) {
                Some(b) => rest = &rest[a + b + close.len()..],
                None => {
                    rest = "";
                    break;
                }
            }
        }
        out.push_str(rest);
        out
    };
    strip(&strip(html, "<time", "</time>"), "<pre>", "</pre>")
}

fn did_sql(prefix: &str, col: &str) -> String {
    format!(
        "'did:plc:{prefix}' || translate(lpad({col}::text, 21, '0'), '0123456789', 'abcdefghij')"
    )
}

fn path_did(d: &str) -> String {
    format!("/did/{d}")
}

/// Whether `path` is an address of the public UI: its pages at the root,
/// or the card fragment.
fn is_public(path: &str) -> bool {
    path == "/"
        || ["/?", "/search", "/did/", "/list/", "/card/"]
            .iter()
            .any(|p| path.starts_with(p))
}

/// Whether a link on a public page stays inside the public UI: its pages,
/// its assets, or an anchor.
fn own_href(href: &str) -> bool {
    href == "/"
        || ["/search", "/did/", "/list/", "/static/", "#"]
            .iter()
            .any(|p| href.starts_with(p))
}

/// Keeps `firehose_state` fresh as a connected v2 stream unless paused.
fn spawn_firehose_keeper(pool: PgPool, paused: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if !paused.load(Ordering::Relaxed) {
                let _ = sqlx::query(
                    "INSERT INTO firehose_state (id, source_url, protocol, applied_through, first_applied_at, connected)
                     VALUES (1, 'ws://harness', 2, now(), now() - interval '2 days', true)
                     ON CONFLICT (id) DO UPDATE SET applied_through = now(), connected = true, protocol = 2",
                )
                .execute(&pool)
                .await;
                let _ = sqlx::query("INSERT INTO firehose_clock (server_at, witness_at) VALUES (now(), now()) ON CONFLICT DO NOTHING")
                    .execute(&pool)
                    .await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

/// The seeded accounts and lists.
struct World {
    s: String,
    e: String,
    o1: String,
    o2: String,
    hidden: Vec<String>,
    unknown: String,
    list: String,
    partial: String,
    /// An account blocked by [`LIVE_DID`]: its page has a row whose card
    /// can be complete.
    card: String,
    /// An account on the three lists of [`SORT_OWNER`], added at different
    /// times, one of them with a stated date in the future.
    sorted: String,
}

const LIST: &str = "modlist1";
/// Owner of the lists naming [`World::sorted`].
const SORT_OWNER: (&str, u64) = ("own", 3);
const LIST_NAME: &str = "<script>alert(1)</script> & friends";

async fn seed_world(h: &H) -> Result<World, String> {
    let p = &h.pool;
    let w = World {
        s: did("sub", 1),
        e: did("exc", 1),
        o1: did("own", 1),
        o2: did("own", 2),
        hidden: (1..=4).map(|i| did("hid", i)).collect(),
        unknown: did("unk", 1),
        list: format!("/list/{}/{LIST}", did("own", 1)),
        partial: did("sub", 3),
        card: did("sub", 4),
        sorted: did("sub", 5),
    };
    let s = seed::actor(p, &w.s).await?;
    let e = seed::actor(p, &w.e).await?;
    let o1 = seed::actor(p, &w.o1).await?;
    let o2 = seed::actor(p, &w.o2).await?;
    seed::actor(p, &w.partial).await?;
    let card = seed::actor(p, &w.card).await?;
    let live = seed::actor(p, LIVE_DID).await?;
    seed::block(p, live, "3lblive", card).await?;
    let web = seed::actor(p, WEB_DID).await?;
    seed::block(p, web, "3lbweb", card).await?;
    let mut hid = Vec::new();
    for (i, d) in w.hidden.iter().enumerate() {
        let id = seed::actor(p, d).await?;
        h.sql(&format!(
            "UPDATE actors SET status = {} WHERE id = {id}",
            i + 1
        ))
        .await?;
        hid.push(id);
    }
    for (prefix, n) in [
        ("blk", 60),
        ("mem", 60),
        ("lbk", 3),
        ("tgt", 3),
        ("hau", 100),
        ("hmm", 100),
        ("hlb", 3),
    ] {
        h.sql(&format!(
            "INSERT INTO actors (did) SELECT {} FROM generate_series(1, {n}) g ON CONFLICT DO NOTHING",
            did_sql(prefix, "g")
        ))
        .await?;
    }
    // Blocks naming S: 60 active blockers, one per hidden status, and E.
    h.sql(&format!(
        "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
         SELECT a.id, '3lb' || lpad(g::text, 9, '0'), {s}, now() - interval '3 days', 1,
                now() - interval '3 days', now() - interval '3 days'
         FROM generate_series(1, 60) g JOIN actors a ON a.did = {}",
        did_sql("blk", "g")
    ))
    .await?;
    for id in hid.iter().chain([&e]) {
        seed::block(p, *id, "3lbx", s).await?;
    }
    // Blocks by S: three active targets, one hidden, and E.
    h.sql(&format!(
        "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
         SELECT {s}, '3lo' || lpad(g::text, 9, '0'), a.id, now() - interval '3 days', 1,
                now() - interval '3 days', now() - interval '3 days'
         FROM generate_series(1, 3) g JOIN actors a ON a.did = {}",
        did_sql("tgt", "g")
    ))
    .await?;
    seed::block(p, s, "3lohid", hid[0]).await?;
    seed::block(p, s, "3loexc", e).await?;

    // The list: ready, a modlist whose name carries markup.
    let l = seed::list(p, o1, LIST, 2, 1, 3, false, Some(3600)).await?;
    sqlx::query("UPDATE lists SET name = $2, purpose = 1 WHERE id = $1")
        .bind(l)
        .bind(LIST_NAME)
        .execute(p)
        .await
        .map_err(|e| e.to_string())?;
    seed::item(p, o1, "3li0subject", l, s).await?;
    h.sql(&format!(
        "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev, first_seen, last_seen)
         SELECT {o1}, '3lm' || lpad(g::text, 9, '0'), {l}, a.id, now() - interval '3 days', 1,
                now() - interval '3 days', now() - interval '3 days'
         FROM generate_series(1, 60) g JOIN actors a ON a.did = {}",
        did_sql("mem", "g")
    ))
    .await?;
    for (i, id) in hid.iter().chain([&e]).enumerate() {
        seed::item(p, o1, &format!("3lix{i}"), l, *id).await?;
    }
    h.sql(&format!(
        "UPDATE lists SET item_count = (SELECT count(*) FROM list_items WHERE list_id = {l}) WHERE id = {l}"
    ))
    .await?;
    h.sql(&format!(
        "INSERT INTO list_blocks (author_id, rkey, list_id, counted, witnessed_at, created_at, rev, first_seen, last_seen)
         SELECT a.id, '3lk' || g, {l}, true, now() - interval '3 days', now() - interval '3 days', 1,
                now() - interval '3 days', now() - interval '3 days'
         FROM generate_series(1, 3) g JOIN actors a ON a.did = {}",
        did_sql("lbk", "g")
    ))
    .await?;
    for id in hid.iter().chain([&e]) {
        seed::listblock(p, *id, "3lkx", l, Some(86_400)).await?;
    }
    // Lists naming S owned by a hidden account and by E.
    let lh = seed::list(p, hid[1], "hiddenowned", 2, 1, 1, false, Some(3600)).await?;
    seed::item(p, hid[1], "3lih", lh, s).await?;
    let le = seed::list(p, e, "excluded", 2, 1, 1, false, Some(3600)).await?;
    seed::item(p, e, "3lie", le, s).await?;

    // Lists naming one account, stored in an order that is neither the
    // order of their stated dates nor of their shown times. "srt-spoof"
    // states the year 9999 and was stored five days ago.
    let sorted = seed::actor(p, &w.sorted).await?;
    let so = seed::actor(p, &did(SORT_OWNER.0, SORT_OWNER.1)).await?;
    for (rkey, stated, seen) in [
        (
            "srt-old",
            "now() - interval '10 days'",
            "now() - interval '10 days'",
        ),
        (
            "srt-spoof",
            "'9999-12-31T00:00:00Z'::timestamptz",
            "now() - interval '5 days'",
        ),
        (
            "srt-new",
            "now() - interval '1 day'",
            "now() - interval '1 day'",
        ),
    ] {
        let id = seed::list(p, so, rkey, 2, 1, 1, false, Some(3600)).await?;
        h.sql(&format!(
            "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev, first_seen, last_seen)
             VALUES ({so}, '3li{rkey}', {id}, {sorted}, {stated}, 1, {seen}, now())"
        ))
        .await?;
    }

    // One list per state, and a capped one.
    for (rkey, state, record, count, capped) in [
        ("st-pending", 1, 1, 1, false),
        ("st-ready", 2, 1, 1, false),
        ("st-retained", 3, 1, 0, false),
        ("st-unavailable", 4, 1, 1, false),
        ("st-missing", 6, 0, 1, false),
        ("st-dead", 7, 2, 0, false),
        ("st-deferred", 8, 1, 1, false),
        ("st-untracked", 0, 1, 0, false),
        ("st-capped", 2, 1, 1, true),
    ] {
        let fetched = matches!(state, 2 | 3).then_some(3600);
        seed::list(p, o2, rkey, state, record, count, capped, fetched).await?;
    }

    // History naming S. 100 removals share one removed_at (one reconcile);
    // others are spread, hidden, excluded, older than the retention, or
    // mark a blocker that blocks again.
    h.sql(&format!(
        "INSERT INTO blocks_history (author_id, rkey, subject_id, created_at, first_seen, last_seen, removed_at, removed_rev, cause)
         SELECT a.id, '3hb' || g, {s}, now() - interval '9 days', now() - interval '8 days',
                now() - interval '3 days', date_trunc('second', now()) - interval '2 days', NULL, 4
         FROM generate_series(1, 100) g JOIN actors a ON a.did = {}",
        did_sql("hau", "g")
    ))
    .await?;
    for (i, id) in hid.iter().chain([&e]).enumerate() {
        h.sql(&format!(
            "INSERT INTO blocks_history (author_id, rkey, subject_id, removed_at, removed_rev, cause)
             VALUES ({id}, '3hbx', {s}, now() - interval '{} hours', 7, 1)",
            i + 1
        ))
        .await?;
    }
    // A row without the bounds; its blocker (blk 1) blocks S now.
    h.sql(&format!(
        "INSERT INTO blocks_history (author_id, rkey, subject_id, created_at, first_seen, last_seen, removed_at, removed_rev, cause)
         SELECT a.id, '3hbold', {s}, now() - interval '30 days', NULL, NULL, now() - interval '5 hours', 9, 1
         FROM actors a WHERE a.did = '{}'",
        did("blk", 1)
    ))
    .await?;
    // Older than the 365-day retention, not yet pruned.
    h.sql(&format!(
        "INSERT INTO blocks_history (author_id, rkey, subject_id, removed_at, cause)
         SELECT a.id, '3hbancient', {s}, now() - interval '400 days', 1 FROM actors a WHERE a.did = '{}'",
        did("lbk", 3)
    ))
    .await?;
    // Removed memberships naming S: the live list, a list with no row,
    // lists of a hidden owner and of E.
    for (owner, rkey, cause) in [
        (o1, LIST, 1),
        (o2, "vanished", 5),
        (hid[1], "hiddenowned", 1),
        (e, "excluded", 4),
    ] {
        h.sql(&format!(
            "INSERT INTO list_items_history (owner_id, rkey, list_rkey, subject_id, created_at, first_seen, last_seen, removed_at, cause)
             VALUES ({owner}, '3him', '{rkey}', {s}, now() - interval '9 days', now() - interval '8 days',
                     now() - interval '4 days', now() - interval '6 hours', {cause})"
        ))
        .await?;
    }
    // History of the list: removed listblocks and removed members (100 at
    // one removed_at, as a drain batch gives them).
    h.sql(&format!(
        "INSERT INTO list_blocks_history (author_id, rkey, list_owner_id, list_rkey, created_at, first_seen, last_seen, removed_at, removed_rev, cause)
         SELECT a.id, '3hl' || g, {o1}, '{LIST}', now() - interval '9 days', now() - interval '8 days',
                now() - interval '3 days', now() - interval '1 day' - g * interval '1 minute', 5, 1
         FROM generate_series(1, 3) g JOIN actors a ON a.did = {}",
        did_sql("hlb", "g")
    ))
    .await?;
    for id in hid.iter().chain([&e]) {
        h.sql(&format!(
            "INSERT INTO list_blocks_history (author_id, rkey, list_owner_id, list_rkey, removed_at, cause)
             VALUES ({id}, '3hlx', {o1}, '{LIST}', now() - interval '7 hours', 1)"
        ))
        .await?;
    }
    h.sql(&format!(
        "INSERT INTO list_blocks_history (author_id, rkey, list_owner_id, list_rkey, removed_at, cause)
         SELECT a.id, '3hlagain', {o1}, '{LIST}', now() - interval '8 hours', 2 FROM actors a WHERE a.did = '{}'",
        did("lbk", 1)
    ))
    .await?;
    h.sql(&format!(
        "INSERT INTO list_items_history (owner_id, rkey, list_rkey, subject_id, created_at, first_seen, last_seen, removed_at, cause)
         SELECT {o1}, '3hm' || g, '{LIST}', a.id, now() - interval '9 days', now() - interval '8 days',
                now() - interval '3 days', date_trunc('second', now()) - interval '1 day', 5
         FROM generate_series(1, 100) g JOIN actors a ON a.did = {}",
        did_sql("hmm", "g")
    ))
    .await?;
    for id in hid.iter().chain([&e]) {
        h.sql(&format!(
            "INSERT INTO list_items_history (owner_id, rkey, list_rkey, subject_id, removed_at, cause)
             VALUES ({o1}, '3hmx{id}', '{LIST}', {id}, now() - interval '9 hours', 1)"
        ))
        .await?;
    }
    h.sql(&format!(
        "INSERT INTO list_items_history (owner_id, rkey, list_rkey, subject_id, removed_at, cause)
         SELECT {o1}, '3hmagain', '{LIST}', a.id, now() - interval '10 hours', 1 FROM actors a WHERE a.did = '{}'",
        did("mem", 1)
    ))
    .await?;
    // Removed blocks S itself authored: no page shows them; the by-author
    // query is exercised directly.
    h.sql(&format!(
        "INSERT INTO blocks_history (author_id, rkey, subject_id, removed_at, cause)
         SELECT {s}, '3ha' || g, a.id, date_trunc('second', now()) - interval '3 days', 4
         FROM generate_series(1, 20) g JOIN actors a ON a.did = {}",
        did_sql("hmm", "g")
    ))
    .await?;
    h.sql(&format!(
        "INSERT INTO blocks_history (author_id, rkey, subject_id, removed_at, cause)
         VALUES ({s}, '3hahid', {}, now() - interval '3 days', 1)",
        hid[2]
    ))
    .await?;
    // An earlier, closed recording window.
    h.sql("INSERT INTO history_windows (from_at, to_at) VALUES (now() - interval '30 days', now() - interval '20 days')")
        .await?;
    // Baseline: a completed full sweep over every collection.
    h.sql("INSERT INTO sweep_cycles (kind, source, collections, started_at, effective_start, effective_start_witness, enumerated_at, completed_at, completed_witness)
        VALUES (1, 'relay_collections', '{1,2,3,4}', now() - interval '2 days', now() - interval '2 days', now() - interval '2 days', now() - interval '1 day', now() - interval '1 day', now() - interval '1 day')").await?;
    Ok(w)
}

// ------------------------------------------------------------- 1. gates

async fn check_gates_off(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("1. toggle gates");
    let unknown = h.get("/no-such-route").await?;
    let mut same = true;
    let mut detail = Vec::new();
    for p in [
        "/search?q=x.example".to_owned(),
        path_did(&w.s),
        w.list.clone(),
        format!("/card/{}", w.s),
    ] {
        let r = h.get(&p).await?;
        let ok = r.status == 404
            && r.text == unknown.text
            && r.header("cache-control") == unknown.header("cache-control")
            && r.header("content-security-policy").is_none();
        same &= ok;
        if !ok {
            detail.push(format!("{p}: {}", r.short()));
        }
    }
    c.check(
        "public_ui = false ⇒ every public route, the card route included, answers 404, byte-identical to an unknown route",
        same && unknown.status == 404,
        if detail.is_empty() {
            unknown.short()
        } else {
            detail.join("; ")
        },
    );
    let root = h.get("/").await?;
    c.check(
        "public_ui = false with the admin UI on ⇒ / is a 303 to /admin, no-store",
        root.status == 303
            && root.header("location").as_deref() == Some("/admin")
            && root
                .header("cache-control")
                .is_some_and(|v| v.contains("no-store")),
        format!(
            "{} → {:?}, {:?}",
            root.status,
            root.header("location"),
            root.header("cache-control")
        ),
    );
    let mut bad = Vec::new();
    for p in [
        "/static/farsight.css",
        "/static/public.css",
        "/static/public.js",
        "/static/htmx.min.js",
        "/static/og-default.png",
    ] {
        let r = h.get(p).await?;
        if r.status != 200
            || r.header("cache-control").as_deref() != Some("public, max-age=3600")
            || r.header("x-content-type-options").as_deref() != Some("nosniff")
        {
            bad.push(format!("{p}: {} {:?}", r.status, r.header("cache-control")));
        }
    }
    c.check(
        "the five static assets are served whatever the switches say (200, public, max-age=3600, nosniff)",
        bad.is_empty(),
        bad.join("; "),
    );
    let robots = h.get("/robots.txt").await?;
    c.check(
        "robots.txt with the public UI off: Disallow: /, max-age=300",
        robots.status == 200
            && robots.text == "User-agent: *\nDisallow: /\n"
            && robots.header("cache-control").as_deref() == Some("public, max-age=300"),
        robots.short(),
    );
    // The loader refuses the public UI over gated reads, with the admin
    // UI on or off (the public UI does not depend on it).
    let mut refused = Vec::new();
    for (reads, ui) in [
        ("api_key", true),
        ("disabled", true),
        ("api_key", false),
        ("disabled", false),
    ] {
        let text = minimal_config(&format!(
            "[access]\nreads = \"{reads}\"\nadmin_ui = {ui}\npublic_ui = true\n"
        ));
        let on = farsight_core::config::load_from_parts(Some(&text), &[]);
        let off = farsight_core::config::load_from_parts(
            Some(&text.replace("public_ui = true", "public_ui = false")),
            &[],
        );
        let msg = on
            .as_ref()
            .err()
            .map(ToString::to_string)
            .unwrap_or_default();
        if !(on.is_err() && off.is_ok() && msg.contains("access.reads")) {
            refused.push(format!("{reads}/{ui}: {msg}"));
        }
    }
    for ui in [true, false] {
        let text = minimal_config(&format!(
            "[access]\nreads = \"public\"\nadmin_ui = {ui}\npublic_ui = true\n"
        ));
        if let Err(e) = farsight_core::config::load_from_parts(Some(&text), &[]) {
            refused.push(format!("public/{ui} refused: {e}"));
        }
    }
    c.check(
        "config load refuses public_ui = true without reads = public, naming the key, and accepts it with the admin UI on or off",
        refused.is_empty(),
        refused.join("; "),
    );
    Ok(())
}

fn minimal_config(rest: &str) -> String {
    format!(
        "[server]\nhostname = \"h.test\"\ncontact = \"mailto:x@h.test\"\n[storage]\ndatabase_url = \"postgres://x\"\n\
         [auth]\nadmin_token_sha256 = \"{}\"\n{rest}",
        "0".repeat(64)
    )
}

/// Runs `farsight` on a config and returns whether it exited by itself
/// with an error, and what it wrote.
fn run_once(tag: &str, config: &str) -> Result<(bool, String), String> {
    let dir = std::env::temp_dir().join(format!("farsight-stage6-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let cfg = dir.join("config.toml");
    std::fs::write(&cfg, config).map_err(|e| e.to_string())?;
    let out = std::process::Command::new(support::farsight_bin())
        .env("FARSIGHT_CONFIG", &cfg)
        .output()
        .map_err(|e| e.to_string())?;
    let err = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    let _ = std::fs::remove_dir_all(&dir);
    Ok((!out.status.success(), err))
}

/// Config validation: at load in the real binary, and in the loader.
fn check_validation(c: &mut Checks, dsn: &str) -> Result<(), String> {
    c.section("1a. config validation");
    let (failed, err) = run_once(
        "invalid-access",
        &config_toml(
            dsn,
            "fsa_x",
            "127.0.0.1:1",
            "https://127.0.0.1:9",
            "public_ui = true",
            "",
        )
        .replace("reads = \"public\"", "reads = \"disabled\""),
    )?;
    c.check(
        "farsight exits non-zero on public_ui = true with reads = disabled",
        failed && err.contains("access.public_ui"),
        support::truncate(&err, 300),
    );
    let (failed, err) = run_once(
        "invalid-viewer",
        &config_toml(
            dsn,
            "fsa_x",
            "127.0.0.1:1",
            "https://127.0.0.1:9",
            "public_ui = false",
            "record_viewer_url = \"https://viewer.example/at/{authority}/{rkey}\"",
        ),
    )?;
    c.check(
        "farsight exits non-zero on a record_viewer_url without its three placeholders, naming the key and the missing one",
        failed
            && err.contains("public_ui.record_viewer_url")
            && err.contains("`{collection}` is missing"),
        support::truncate(&err, 300),
    );
    let load = |pu: &str| {
        farsight_core::config::load_from_parts(
            Some(&minimal_config(&format!(
                "[access]\nadmin_did = \"{ADMIN_DID}\"\n[public_ui]\n{pu}\n"
            ))),
            &[],
        )
    };
    let mut bad = Vec::new();
    for (value, ok) in [
        ("", true),
        (VIEWER, true),
        (
            "https://viewer.example/?u=at://{authority}/{collection}/{rkey}",
            true,
        ),
        ("https://viewer.example/", false),
        ("https://viewer.example/{authority}/{collection}", false),
        ("https://{authority}.example/{collection}/{rkey}", false),
        (
            "https://viewer.example/{authority}/{collection}/{rkey}/{other}",
            false,
        ),
        ("javascript:alert('{authority}{collection}{rkey}')", false),
        (
            "https://user:pw@viewer.example/{authority}/{collection}/{rkey}",
            false,
        ),
    ] {
        let r = load(&format!("record_viewer_url = \"{value}\""));
        if r.is_ok() != ok {
            bad.push(format!("{value}: {:?}", r.err().map(|e| e.to_string())));
        }
    }
    c.check(
        "the loader accepts an empty or well-formed record_viewer_url and refuses one with a missing or stray placeholder, a placeholder in the host, a non-http scheme or credentials",
        bad.is_empty(),
        bad.join("; "),
    );
    let clamped = load("card_rps = 9\ncard_burst = 2").map_err(|e| e.to_string())?;
    let fine = load("card_rps = 4\ncard_burst = 8").map_err(|e| e.to_string())?;
    let limit = farsight_api::ratelimit::Class::PublicCardBudget.limit(&clamped.config, None);
    c.check(
        "card_burst below card_rps loads with a warning and is clamped up to the rate; the defaults load without one",
        clamped.warnings.iter().any(|x| x.contains("card_burst"))
            && clamped.config.public_ui.effective_card_burst() == 9
            && (limit.rate, limit.burst) == (9.0, 9.0)
            && fine.warnings.is_empty()
            && load("card_rps = 0").is_err(),
        format!("{:?}; limit {limit:?}", clamped.warnings),
    );
    let defaults = &fine.config.public_ui;
    c.check(
        "defaults: record_viewer_url empty, show_avatars on, card_rps 4, card_burst 8",
        farsight_core::config::Config::default()
            .public_ui
            .record_viewer_url
            .is_empty()
            && farsight_core::config::Config::default()
                .public_ui
                .show_avatars
            && (defaults.card_rps, defaults.card_burst) == (4, 8),
        format!("{} / {}", defaults.card_rps, defaults.card_burst),
    );
    Ok(())
}

/// Once the public UI is on and the Public UI settings have been saved
/// once: the file holds the form's keys.
async fn check_first_save(c: &mut Checks, h: &H) -> Result<(), String> {
    c.section("1b. after the first save of the Public UI settings");
    let file = h.config_text()?;
    c.check(
        "the first save of the Public UI settings wrote the form's keys to config.toml, those the file did not have included",
        file.contains("record_viewer_url = \"\"")
            && file.contains("show_avatars = true")
            && file.contains("card_rps = 4")
            && file.contains("card_burst = 8"),
        support::truncate(
            &file
                .lines()
                .filter(|l| l.contains("show_") || l.contains("card_") || l.contains("record_"))
                .collect::<Vec<_>>()
                .join(" | "),
            300,
        ),
    );
    let reloaded =
        farsight_core::config::load_from_parts(Some(&file), &[]).map_err(|e| e.to_string())?;
    c.check(
        "the rewritten file loads without a warning",
        reloaded.warnings.is_empty(),
        format!("{:?}", reloaded.warnings),
    );
    Ok(())
}

async fn check_enable_flow(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("1c. enable-confirmation flow and the Settings controls");
    let settings = h.admin_get("/admin/settings").await?;
    let controls = [
        "enabled",
        "instance_description",
        "contact",
        "show_outgoing_blocks",
        "record_viewer_url",
        "show_avatars",
        "card_rps",
        "card_burst",
        "show_opengraph_image",
        "dark_mode_default",
        "crawlable",
        "rate_limit_rps",
        "rate_limit_burst",
        "query_concurrency",
        "handle_cache_ttl",
        "excluded_dids",
    ];
    let missing: Vec<&str> = controls
        .iter()
        .copied()
        .filter(|k| !settings.text.contains(&format!("name=\"{k}\"")))
        .collect();
    c.check(
        "Settings has a control for the toggle and every [public_ui] key",
        missing.is_empty()
            && settings.text.contains("href=\"/\" target=\"_blank\"")
            && settings.text.contains(
                "placeholder=\"https://viewer.example/at/{authority}/{collection}/{rkey}\"",
            )
            && settings
                .text
                .contains("fetches it from the account's own server"),
        format!("missing: {missing:?}"),
    );
    // A direct second POST, with no first: nothing happens.
    let direct = h.confirm("never-issued").await?;
    c.check(
        "a confirm POST without a pending confirmation is refused (400) and changes nothing",
        direct.status == 400 && h.get("/").await?.status == 303,
        banner(&direct.text),
    );
    let first = h.post_settings(&[("enabled", Some("on"))]).await?;
    let token = confirm_token(&first.text);
    let lists_all = [
        HOSTNAME,
        "mailto:ops@",
        "the public pages do not show it",
        "Incoming blocks",
        "Lists naming any account",
        "List memberships",
        "Profile cards",
        "fetches from the account",
        "Removed records are <strong>not</strong> public",
    ]
    .iter()
    .all(|s| first.text.contains(s));
    c.check(
        "first POST turning public_ui on renders the confirmation page: every public category, the avatar disclosure, and that removed records are not public",
        first.status == 200
            && token.is_some()
            && lists_all
            && !first.text.contains("Historical data")
            && !first.text.contains("Coverage status"),
        support::truncate(&first.text, 200),
    );
    c.check(
        "…and writes nothing: / is still the redirect to /admin and the config still says off",
        h.get("/").await?.status == 303 && !h.config_text()?.contains("public_ui = true"),
        "not written",
    );
    let token = token.ok_or("no confirmation token")?;
    let bogus = h.confirm(&format!("{token}x")).await?;
    c.check(
        "a confirm POST with a wrong token is refused and the public UI stays off",
        bogus.status == 400 && h.get("/").await?.status == 303,
        banner(&bogus.text),
    );
    let second = h.confirm(&token).await?;
    let home = h.get("/").await?;
    c.check(
        "second POST with the form token commits: the settings are saved and / serves the public home, with no restart",
        second.status == 200
            && second.text.contains("Saved.")
            && second.text.contains("access.public_ui")
            && home.status == 200,
        format!("{}; / {}", banner(&second.text), home.status),
    );
    let replay = h.confirm(&token).await?;
    c.check(
        "a confirmation token works once",
        replay.status == 400,
        banner(&replay.text),
    );
    let _ = w;
    Ok(())
}

// ------------------------------------------------------------ 2. routes

async fn check_login_route(c: &mut Checks, h: &H) -> Result<(), String> {
    c.section("2a. the admin sign-in is at /enter");
    let enter = h.get("/enter").await?;
    c.check(
        "/enter serves the admin sign-in form, posting to /enter, never cached",
        enter.status == 200
            && enter.text.contains(
                "<form class=\"stack card enter-card\" method=\"post\" action=\"/enter\"",
            )
            && enter.header("cache-control").as_deref() == Some("no-store, private"),
        enter.short(),
    );
    // A form posted to /enter signs nobody in (the flow itself is the
    // stage-7 harness's subject).
    let posted = h
        .fresh()
        .post_form(
            &format!("{}/enter", h.base),
            &[],
            &[("admin_did", "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")],
        )
        .await?;
    c.check(
        "POST /enter reads no form field: whatever is posted, no session cookie comes back",
        set_cookie(&posted, "farsight_admin").is_none() && posted.status != 200,
        format!("{}", posted.status),
    );
    // There is no anonymous dashboard: it redirects like every admin page,
    // and the header of the sign-in page has the brand and no link.
    let gated = h.get("/admin/settings").await?;
    let dash = h.get("/admin").await?;
    c.check(
        "an admin page without a session — the dashboard included — redirects to /enter; the sign-in page's header has the brand and no nav link",
        gated.status == 303
            && gated.header("location").as_deref() == Some("/enter")
            && dash.status == 303
            && dash.header("location").as_deref() == Some("/enter")
            && enter.text.contains("<span class=\"brand\">")
            && !enter.text.contains("<nav")
            && !enter.text.contains("Dashboard"),
        format!(
            "{} → {:?}; {} → {:?}",
            gated.status,
            gated.header("location"),
            dash.status,
            dash.header("location")
        ),
    );
    Ok(())
}

async fn check_routes(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("2. routes");
    let mut bad = Vec::new();
    for p in [
        "/".to_owned(),
        path_did(&w.s),
        w.list.clone(),
        format!("/card/{}", w.s),
        "/static/public.css".to_owned(),
        "/static/public.js".to_owned(),
        "/static/htmx.min.js".to_owned(),
        "/static/og-default.png".to_owned(),
    ] {
        let r = h.get(&p).await?;
        if r.status != 200 {
            bad.push(format!("{p}: {}", r.short()));
        }
    }
    c.check(
        "every page, the card fragment and every asset renders against seeded data (200)",
        bad.is_empty(),
        bad.join("; "),
    );
    let png = h
        .fresh()
        .get(&format!("{}/static/og-default.png", h.base), &[])
        .await?;
    c.check(
        "the preview image is a PNG served as image/png",
        png.header("content-type").as_deref() == Some("image/png") && png.text.contains("PNG"),
        png.header("content-type").unwrap_or_default(),
    );
    let unknown = h.get("/no-such-route").await?;
    let mut bad = Vec::new();
    for p in [
        "/about".to_owned(),
        "/nonsense".to_owned(),
        "/static/nothing.css".to_owned(),
        format!("{}/more", path_did(&w.s)),
        format!("{}/more", w.list),
    ] {
        let r = h.get(&p).await?;
        if r.status != 404
            || r.text != unknown.text
            || r.header("cache-control") != unknown.header("cache-control")
            || r.header("content-security-policy").is_some()
        {
            bad.push(format!("{p}: {}", support::truncate(&r.short(), 80)));
        }
    }
    c.check(
        "an unknown path — /about, a static file that does not exist, a path below an account or list page — is the bare 404 with the public UI on, not a redirect and not the public not-found page",
        bad.is_empty() && unknown.status == 404 && unknown.text == "not found",
        bad.join("; "),
    );
    let mut bad = Vec::new();
    let long = "a".repeat(600);
    for p in [
        "/did/not-a-did".to_owned(),
        "/did/alice.example".to_owned(),
        "/did/did:plc:short".to_owned(),
        format!("/did/did:web:{long}.example"),
        format!("/list/{}/bad%20key", w.o1),
        format!("/list/alice.example/{LIST}"),
        format!("{}?page=0", path_did(&w.s)),
        format!("{}?lists=x", path_did(&w.s)),
        format!("{}?page=2&out=-1", path_did(&w.s)),
        format!("{}?subscribers=1000001", w.list),
        format!("/search?q={long}"),
        "/card/alice.example".to_owned(),
        "/card/did:plc:short".to_owned(),
    ] {
        let r = h.get(&p).await?;
        if r.status != 400 || r.header("cache-control").as_deref() != Some("no-store") {
            bad.push(format!(
                "{}: {} {:?}",
                support::truncate(&p, 60),
                r.status,
                r.header("cache-control")
            ));
        }
    }
    c.check(
        "malformed DID, list key, page number and over-long input ⇒ 400, no-store (pages and the card route)",
        bad.is_empty(),
        bad.join("; "),
    );
    let cur = h.get(&format!("{}?page=0", path_did(&w.s))).await?;
    c.check(
        "a page number that cannot be read links to the first page",
        cur.text.contains(&format!("href=\"{}\"", path_did(&w.s))),
        support::truncate(&cur.text, 120),
    );
    let mut bad = Vec::new();
    for (from, to) in [
        // Page 1 has one address: the one without the parameter.
        (format!("{}?page=1", path_did(&w.s)), path_did(&w.s)),
        (
            format!("{}?page=2&lists=1", path_did(&w.s)),
            format!("{}?page=2", path_did(&w.s)),
        ),
        (format!("{}?subscribers=1", w.list), w.list.clone()),
    ] {
        let r = h.get(&from).await?;
        if r.status != 301 || r.header("location").as_deref() != Some(to.as_str()) {
            bad.push(format!("{from}: {} {:?}", r.status, r.header("location")));
        }
    }
    c.check(
        "an address with a page parameter of 1 ⇒ 301 to the address without it; the other sections keep their page",
        bad.is_empty(),
        bad.join("; "),
    );
    let u = h.get(&path_did(&w.unknown)).await?;
    let actors_before = h.n("SELECT count(*) FROM actors").await?;
    c.check(
        "an unknown DID renders its sections empty with the one empty-section string and a count of 0 (never 'not found'), and is not interned",
        u.status == 200
            && section(&u.text, "blockers").is_some_and(|s| s.contains(&format!("<p class=\"empty\">{EMPTY}</p>")) && s.contains("<span class=\"count count-big\">0</span>"))
            && section(&u.text, "lists").is_some_and(|s| s.contains(EMPTY))
            && !u.text.contains("None found")
            && !u.text.contains("No blockers")
            && h.n(&format!("SELECT count(*) FROM actors WHERE did = '{}'", w.unknown)).await? == 0
            && actors_before == h.n("SELECT count(*) FROM actors").await?,
        u.short(),
    );
    let ul = h.get(&format!("/list/{}/nosuchlist", w.o1)).await?;
    c.check(
        "an unknown list ⇒ 404 page saying this instance has no record of it, with no link to a history page",
        ul.status == 404
            && ul
                .text
                .contains("This instance has no record of this list.")
            && !ul.text.contains("/history"),
        ul.short(),
    );
    Ok(())
}

// ------------------------------------------------------ sections

async fn check_sections(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("2b. sections against stored rows");
    let sid = h
        .n(&format!("SELECT id FROM actors WHERE did = '{}'", w.s))
        .await?;
    // The public tables leave out deactivated (1), taken-down (2) and
    // deleted (4) accounts; a suspended one (3) is shown, tagged.
    let shown = "a.status NOT IN (1, 2, 4)";
    let page = h.get(&path_did(&w.s)).await?;
    let navs = section_navs(&page.text);
    c.check(
        "the account page's header is the DID alone: no copy button (copying an account's DID takes selecting it), no stat tiles, no section nav, no status pill, no avatar",
        navs.is_empty()
            && !page.text.contains("stat-card")
            && !page.text.contains("section-pill")
            && !page.text.contains("status-pill")
            && !page.text.contains("avatar-badge")
            && !page.text.contains("data-copy")
            && !page.text.contains("copy-btn")
            && page.text.contains(&format!("<code class=\"did-code\">{}</code>", w.s)),
        format!("{navs:?}"),
    );
    let ids: Vec<&str> = between(&page.text, "<section id=\"", "\"");
    c.check(
        "the account page has the two sections and nothing else: no Coverage section, no removed-records link",
        ids == ["blockers", "lists"],
        format!("{ids:?}"),
    );
    let want = h
        .strings(&format!(
            "SELECT a.did FROM blocks b JOIN actors a ON a.id = b.author_id WHERE b.subject_id = {sid} AND {shown}"
        ))
        .await?;
    let (rows, pages, _) = walk(h, &path_did(&w.s), "blockers").await?;
    let (ok, d) = exactly_once(&rows, &want);
    c.check(
        "Blocked by: paging through the section returns every blocker with an active account exactly once, 50 a page",
        ok && pages == 2 && want.len() == 62,
        format!("{d}; {pages} pages"),
    );
    let sec = section(&page.text, "blockers").unwrap_or("");
    let second = h.get(&format!("{}?page=2", path_did(&w.s))).await?;
    let sec2 = section(&second.text, "blockers").unwrap_or("");
    c.check(
        "Blocked by holds 50 rows a page with numbered page controls: 62 blockers are two pages; the controls are plain links (no htmx) that carry the page in the query and the section as the fragment, the current page is marked, and an arrow that leads nowhere is not a link",
        row_dids(sec).len() == 50
            && row_dids(sec2).len() == 12
            && controls_of(sec) == "(←) [1] 2 →"
            && controls_of(sec2) == "← 1 [2] (→)"
            && sec.contains(&format!(
                "rel=\"next nofollow\" href=\"{}?page=2#blockers\"",
                path_did(&w.s)
            ))
            && step_of(sec2, "prev") == Some(path_did(&w.s))
            && sec2.contains("aria-current=\"page\"")
            && !sec.contains("hx-")
            && !sec.contains("Load more"),
        format!("{:?} / {:?}", controls_of(sec), controls_of(sec2)),
    );
    let lists_sec = section(&page.text, "lists").unwrap_or("");
    c.check(
        "a section that fits on one page has no page controls",
        !lists_sec.contains("<nav class=\"pager\""),
        support::truncate(lists_sec.rsplit("</table>").next().unwrap_or(""), 120),
    );
    c.check(
        "row times on the account page are marked to read as the instant alone (no relative part)",
        sec.matches("<time ").count() == row_dids(sec).len()
            && sec.matches(" data-abs>").count() == row_dids(sec).len(),
        format!("{} times", sec.matches("<time ").count()),
    );
    c.check(
        "the bounded count equals the stored count with the page's filters",
        sec.contains(&format!(
            "<span class=\"count count-big\">{}</span>",
            want.len()
        )),
        support::truncate(sec, 120),
    );
    // An ordinary account's row (a suspended one carries a tag as well).
    let first = row_dids(sec)
        .into_iter()
        .find(|d| d.starts_with("did:plc:blk"))
        .unwrap_or_default();
    c.check(
        "a row names the account as a link to its page whose title is the DID and which carries its card address; with no handle cached it shows the DID",
        sec.contains(&format!(
            "<span class=\"who-wrap\"><a class=\"who\" href=\"/did/{first}\" title=\"{first}\" data-card=\"/card/{first}\"><code>{first}</code></a></span>"
        )),
        first,
    );
    let lists = section(&page.text, "lists").unwrap_or("");
    let heads: Vec<&str> = heads_of(lists);
    c.check(
        "Blocked By Lists: the columns are List, Owner, Added — no Purpose column, no per-list count",
        heads == ["List", "Owner", "Added"]
            && !lists.contains("Purpose")
            && !lists.contains("moderation list</td>"),
        format!("{heads:?}"),
    );
    let stored_count = h
        .n(&format!(
            "SELECT COALESCE(sum(l.listblock_count), 0)::bigint FROM lists l JOIN actors o ON o.id = l.owner_id
             WHERE l.id IN (SELECT list_id FROM list_items WHERE subject_id = {sid})
               AND l.track_state IN (2, 3) AND l.record_state = 1 AND o.status NOT IN (1, 2, 4)"
        ))
        .await?;
    c.check(
        "Blocked By Lists: the heading is the listblock records on the lists shown, added up, with the description under it; the list links to its page; a hidden owner's list is left out and not added in",
        lists.contains(&format!("<span class=\"count count-big\" title=\"listblock records on the lists below\">{stored_count}</span>"))
            && lists.contains("Moderation Lists which this user has been added to.")
            && !lists.contains("listblock-badge")
            && lists.contains(&format!("href=\"{}\"", w.list))
            && lists.contains(&format!("data-card=\"/card/{}\"", w.o1))
            && !lists.contains("hiddenowned"),
        format!("counter {stored_count}; {}", support::truncate(lists, 160)),
    );
    let sorted = h.get(&path_did(&w.sorted)).await?;
    let so = did(SORT_OWNER.0, SORT_OWNER.1);
    let order: Vec<&str> = between(
        section(&sorted.text, "lists").unwrap_or(""),
        &format!("href=\"/list/{so}/"),
        "\"",
    );
    c.check(
        "On lists is newest first by the earlier of the stated date and the date this instance stored the listitem: a listitem dated in the year 9999 sits where it arrived, between the newer and the older one, and still shows its stated date",
        order == ["srt-new", "srt-spoof", "srt-old"]
            && sorted.text.contains("<time datetime=\"9999-12-31T00:00:00Z\""),
        format!("{order:?}"),
    );
    c.check(
        "a list name carrying markup renders as text",
        lists.contains("&lt;script&gt;alert(1)&lt;/script&gt; &amp; friends")
            && !page.text.contains("<script>alert(1)"),
        "escaped",
    );

    // The list page.
    let lid = h
        .n(&format!(
            "SELECT l.id FROM lists l JOIN actors o ON o.id = l.owner_id WHERE o.did = '{}' AND l.rkey = '{LIST}'",
            w.o1
        ))
        .await?;
    let lp = h.get(&w.list).await?;
    let stored = h
        .n(&format!(
            "SELECT item_count::bigint FROM lists WHERE id = {lid}"
        ))
        .await?;
    c.check(
        "list page header: at-uri, name (as text), purpose, owner, state in words, the stored-members counter; no copy button",
        lp.text.contains(&format!("at://{}/app.bsky.graph.list/{LIST}", w.o1))
            && lp.text.contains("&lt;script&gt;alert(1)&lt;/script&gt;")
            && lp.text.contains(">moderation list</span>")
            && !lp.text.contains("status-pill")
            && !lp.text.contains("avatar-badge")
            && !lp.text.contains("data-copy")
            && !lp.text.contains("copy-btn")
            && lp.text.contains(&format!("href=\"{}\"", path_did(&w.o1)))
            && lp.text.contains("State: Indexed.")
            && lp.text.contains(&format!("{stored} stored members")),
        support::truncate(&lp.text, 160),
    );
    let navs = section_navs(&lp.text);
    let ids: Vec<&str> = between(&lp.text, "<section id=\"", "\"");
    c.check(
        "the list page has the two sections, Members and Blocked by, and like the account page no stat tiles and no section nav",
        navs.is_empty()
            && !lp.text.contains("stat-card")
            && !lp.text.contains("section-pill")
            && ids == ["members", "subscribers"],
        format!("{navs:?} {ids:?}"),
    );
    let want = h
        .strings(&format!(
            "SELECT a.did FROM list_items li JOIN actors a ON a.id = li.subject_id WHERE li.list_id = {lid} AND {shown}"
        ))
        .await?;
    let (rows, pages, short) = walk(h, &w.list, "members").await?;
    let (ok, d) = exactly_once(&rows, &want);
    c.check(
        "Members: every active member exactly once across pages; inactive members are left out",
        ok && want.len() == 63,
        format!("{d}; {pages} pages"),
    );
    let sec = section(&lp.text, "members").unwrap_or("");
    c.check(
        "the withheld rule runs in the section's query, so it shortens no page and the page count is that of the rows shown: 50 rows while more follow, two pages for 63 members",
        short == 0 && pages == 2 && controls_of(sec) == "(←) [1] 2 →" && !sec.contains("hx-"),
        format!("{short} short pages with a next link; {pages} pages; {:?}", controls_of(sec)),
    );
    let times = |sec: &str| {
        sec.matches("<time ").count() == row_dids(sec).len()
            && sec.matches(" data-abs>").count() == row_dids(sec).len()
    };
    c.check(
        "row times on the list page are marked to read as the instant alone (no relative part), in both tables",
        times(sec) && times(section(&lp.text, "subscribers").unwrap_or("")),
        format!("{} times in Members", sec.matches("<time ").count()),
    );
    let want = h
        .strings(&format!(
            "SELECT a.did FROM list_blocks b JOIN actors a ON a.id = b.author_id WHERE b.list_id = {lid} AND {shown}"
        ))
        .await?;
    let (rows, _, _) = walk(h, &w.list, "subscribers").await?;
    let (ok, d) = exactly_once(&rows, &want);
    let sec = section(&lp.text, "subscribers").unwrap_or("");
    c.check(
        "Blocked by (list): every active listblocker exactly once, and the bounded count",
        ok && want.len() == 5
            && sec.contains(&format!("<span class=\"count\">{}</span>", want.len())),
        d,
    );

    // List states: what stays, because without it a page would state
    // something false.
    let mut bad = Vec::new();
    for (rkey, words, members) in [
        ("st-ready", "Indexed.", true),
        ("st-retained", "Indexed.", true),
        (
            "st-pending",
            "Being indexed. Members are not available yet.",
            false,
        ),
        (
            "st-untracked",
            "No account known to this instance blocks this list, so its members are not indexed.",
            false,
        ),
        (
            "st-missing",
            "The list record has not been found yet; this instance is still trying.",
            false,
        ),
        (
            "st-dead",
            "The list record was deleted or could not be found.",
            false,
        ),
        (
            "st-unavailable",
            "The list could not be read from its owner&#x27;s server. Members are not shown.",
            false,
        ),
        (
            "st-deferred",
            "Not indexed: this instance is at a storage limit.",
            false,
        ),
    ] {
        let r = h.get(&format!("/list/{}/{rkey}", w.o2)).await?;
        let has = section(&r.text, "members").is_some();
        if r.status != 200
            || !r.text.contains(&format!("State: {words}"))
            || has != members
            || r.text.contains("No members")
        {
            bad.push(format!("{rkey}: {} members={has}", r.status));
        }
    }
    let capped = h.get(&format!("/list/{}/st-capped", w.o2)).await?;
    c.check(
        "each getListMembers state keeps its wording; the Members section only for ready and retained; a capped list says so",
        bad.is_empty()
            && capped
                .text
                .contains("This instance stores only part of this list."),
        bad.join("; "),
    );
    Ok(())
}

async fn walk(h: &H, first: &str, id: &str) -> Result<(Vec<String>, usize, usize), String> {
    let mut rows = Vec::new();
    let mut url = first.to_owned();
    let (mut pages, mut short_with_next) = (0, 0);
    loop {
        let r = h.get(&url).await?;
        if r.status != 200 {
            return Err(format!("{url}: {}", r.short()));
        }
        let sec = section(&r.text, id).ok_or_else(|| format!("{url}: no #{id}"))?;
        let here = row_dids(sec);
        pages += 1;
        let next = next_of(sec);
        if next.is_some() && here.len() < 50 {
            short_with_next += 1;
        }
        rows.extend(here);
        match next {
            Some(n) if pages < 40 => url = n,
            _ => break,
        }
    }
    Ok((rows, pages, short_with_next))
}

fn exactly_once(rows: &[String], want: &BTreeSet<String>) -> (bool, String) {
    let got: BTreeSet<String> = rows.iter().cloned().collect();
    (
        got == *want && rows.len() == want.len(),
        format!(
            "{} rows, {} distinct, {} expected, {} missing, {} unexpected",
            rows.len(),
            got.len(),
            want.len(),
            want.difference(&got).count(),
            got.difference(want).count()
        ),
    )
}

// ------------------------------------------------------------ 3. search

async fn check_search(c: &mut Checks, h: &H, live: Option<&H>, w: &World) -> Result<(), String> {
    c.section("3. search");
    let list_uri = format!("at://{}/app.bsky.graph.list/{LIST}", w.o1);
    let cases = [
        (w.s.clone(), path_did(&w.s)),
        (format!("  {}  ", w.s), path_did(&w.s)),
        (format!("at://{}", w.s), path_did(&w.s)),
        (list_uri.clone(), w.list.clone()),
        (format!("https://bsky.app/profile/{}", w.s), path_did(&w.s)),
        (
            format!("https://bsky.app/profile/{}/lists/{LIST}", w.o1),
            w.list.clone(),
        ),
    ];
    let mut bad = Vec::new();
    for (q, want) in &cases {
        let r = h.get(&format!("/search?q={}", enc(q))).await?;
        if r.status != 303
            || r.header("location").as_deref() != Some(want.as_str())
            || r.header("cache-control").as_deref() != Some("no-store")
        {
            bad.push(format!("{q}: {} → {:?}", r.status, r.header("location")));
        }
    }
    c.check(
        "DID, at:// account, at:// list and bsky.app links redirect (303, relative, no-store) to the right page",
        bad.is_empty(),
        bad.join("; "),
    );
    let mut bad = Vec::new();
    for q in [
        format!("at://{}/app.bsky.graph.block/3kabc", w.s),
        format!("at://{}/App.Bsky.Graph.List/{LIST}", w.o1),
        format!("{list_uri}/extra"),
        "https://example.com/profile/alice.example".to_owned(),
        "http://bsky.app/profile/alice.example".to_owned(),
        "https://bsky.app/profile/alice.example/post/3k".to_owned(),
        "http://169.254.169.254/latest/meta-data".to_owned(),
        "did:plc:nope".to_owned(),
        "not a handle".to_owned(),
        String::new(),
    ] {
        let r = h.get(&format!("/search?q={}", enc(&q))).await?;
        if r.status != 400 || r.elapsed > Duration::from_secs(2) {
            bad.push(format!("{q}: {} in {:?}", r.status, r.elapsed));
        }
    }
    c.check(
        "other collections, extra segments, non-bsky.app URLs and junk ⇒ 400 at once (nothing is fetched)",
        bad.is_empty(),
        bad.join("; "),
    );
    let only = h
        .get(&format!(
            "/search?q={}",
            enc(&format!("at://{}/app.bsky.feed.post/3k", w.s))
        ))
        .await?;
    c.check(
        "a rejected collection says only accounts and lists can be looked up",
        only.text
            .contains("Only accounts and lists can be looked up."),
        support::truncate(&only.text, 100),
    );
    let fail = h.get("/search?q=no-such-handle.invalid").await?;
    c.check(
        "a handle that does not resolve ⇒ the 404 search page suggesting a retry or the DID, no-store, noindex",
        fail.status == 404
            && fail.text.contains("could not be resolved")
            && fail.text.contains("paste the account&#x27;s DID")
            && fail.text.contains("value=\"no-such-handle.invalid\"")
            && fail.header("cache-control").as_deref() == Some("no-store")
            && fail.header("x-robots-tag").as_deref() == Some("noindex, nofollow"),
        fail.short(),
    );
    match live {
        None => c.unverified(
            "a real handle resolves and redirects to its DID page",
            "--skip-live",
        ),
        Some(l) => {
            let r = l.get(&format!("/search?q={LIVE_HANDLE}")).await?;
            let want = path_did(LIVE_DID);
            if r.status == 303 && r.header("location").as_deref() == Some(want.as_str()) {
                c.check("a real handle (DNS / well-known, through the safe client) redirects to its DID page", true, want);
            } else {
                c.unverified(
                    "a real handle resolves and redirects to its DID page",
                    format!("needs the network: {}", r.short()),
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------- 4. coverage

fn coverage_words_in(html: &str) -> Vec<&'static str> {
    COVERAGE_WORDS
        .iter()
        .copied()
        .filter(|w| html.contains(w))
        .collect()
}

fn secs(iso: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(iso)
        .ok()
        .map(|t| t.timestamp())
}

async fn check_coverage(
    c: &mut Checks,
    h: &mut H,
    w: &World,
    paused: &AtomicBool,
) -> Result<(), String> {
    c.section("4. what a public page says about coverage");
    h.refresh().await?;
    let q = |d: &str| format!("actor={}", enc(d));
    let page = h.get(&path_did(&w.s)).await?;
    let lp = h.get(&w.list).await?;
    let home = h.get("/").await?;
    let found: Vec<&str> = [&page, &lp, &home]
        .iter()
        .flat_map(|r| coverage_words_in(&r.text))
        .collect();
    c.check(
        "no coverage section, no per-section line, no level word, no raw freshness, no 'not shown' sentence on the account page, the list page or home",
        found.is_empty(),
        format!("{found:?}"),
    );
    let mut bad = Vec::new();
    for (name, r) in [("account", &page), ("list", &lp), ("home", &home)] {
        let n = r.text.matches("Last updated <time datetime=\"").count();
        let stamp = updated_of(&r.text).unwrap_or_default();
        let text_ok = r.text.contains(&format!(
            "<time datetime=\"{stamp}\">{} {} UTC</time>.",
            stamp.get(..10).unwrap_or(""),
            stamp.get(11..19).unwrap_or("")
        ));
        if n != 1 || !is_utc_instant(&stamp) || !text_ok {
            bad.push(format!("{name}: {n} lines, {stamp:?}, text {text_ok}"));
        }
    }
    c.check(
        "each data page and home ends with exactly one \"Last updated\" line: a <time> whose datetime is YYYY-MM-DDTHH:MM:SSZ and whose text is the same instant in UTC",
        bad.is_empty(),
        bad.join("; "),
    );
    let main = page.text.find("</main>").unwrap_or(usize::MAX);
    let foot = page.text.find("</footer>").unwrap_or(0);
    c.check(
        "the line is in the page's footer; an error page's footer has none",
        page.text
            .find("Last updated <time")
            .is_some_and(|i| i > main && i < foot)
            && [&page, &lp, &home]
                .iter()
                .all(|r| !r.text.contains("ATProto Block Graph Index")),
        "in the footer",
    );

    // The time is the earliest indexedAt among the sections rendered.
    // With the stream stopped it stands still, so it can be compared
    // exactly with what the API reports for the same queries.
    paused.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    h.refresh().await?;
    let page = h.get(&path_did(&w.s)).await?;
    let blocks = h.xrpc("query.getIncomingBlocks", &q(&w.s)).await?;
    let naming = h.xrpc("query.getListsNaming", &q(&w.s)).await?;
    let stats = h.xrpc("query.getStats", "").await?;
    let home = h.get("/").await?;
    let api_min = [&blocks, &naming]
        .iter()
        .filter_map(|v| v["freshness"]["indexedAt"].as_str().and_then(secs))
        .min();
    let shown = updated_of(&page.text).as_deref().and_then(secs);
    let home_shown = updated_of(&home.text).as_deref().and_then(secs);
    c.check(
        "the time is the earliest indexedAt of the freshness the API reports for the page's sections; on home it is getStats's",
        shown.is_some()
            && shown == api_min
            && home_shown == stats["freshness"]["indexedAt"].as_str().and_then(secs),
        format!("page {shown:?}, api {api_min:?}, home {home_shown:?}"),
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let again = h.get(&path_did(&w.s)).await?;
    c.check(
        "the server writes the instant, never a relative phrase: two renders a moment apart carry the same line",
        updated_of(&again.text) == updated_of(&page.text) && !again.text.contains(" ago"),
        format!("{:?}", updated_of(&again.text)),
    );
    paused.store(false, Ordering::Relaxed);

    // A level the API reports as partial changes nothing on the page.
    h.sql("UPDATE sweep_cycles SET completed_at = NULL WHERE kind = 1")
        .await?;
    h.refresh().await?;
    let page = h.get(&path_did(&w.partial)).await?;
    let api = h.xrpc("query.getIncomingBlocks", &q(&w.partial)).await?;
    let sec = section(&page.text, "blockers").unwrap_or("");
    c.check(
        "while the API reports partial coverage for the same query, the page prints no banner and an empty section says only what this instance holds",
        api["freshness"]["coverage"]["level"] == "partial"
            && coverage_words_in(&page.text).is_empty()
            && !page.text.contains("Partial")
            && !page.text.contains("not complete")
            && sec.contains(&format!("<p class=\"empty\">{EMPTY}</p>"))
            && sec.contains("<span class=\"count count-big\">0</span>"),
        format!("api level {}", api["freshness"]["coverage"]["level"]),
    );
    h.sql("UPDATE sweep_cycles SET completed_at = now() - interval '1 day' WHERE kind = 1")
        .await?;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    h.refresh().await?;

    // Outgoing: off by default; on, a third section and a third nav link.
    let page = h.get(&path_did(&w.s)).await?;
    c.check(
        "the outgoing sections (the account's blocks, and the lists it subscribes to as block lists) are off by default",
        section(&page.text, "outgoing").is_none()
            && section(&page.text, "blockinglists").is_none()
            && !page.text.contains("data-tab=\"blockinglists\"")
            && !page.text.contains("Accounts this user has blocked."),
        "absent",
    );
    h.set(&[("show_outgoing_blocks", Some("on"))]).await?;
    let sid = h
        .n(&format!("SELECT id FROM actors WHERE did = '{}'", w.s))
        .await?;
    seed::debt(&h.pool, sid, 2).await?;
    h.refresh().await?;
    let page = h.get(&path_did(&w.s)).await?;
    let out = section(&page.text, "outgoing").unwrap_or("");
    let want = h
        .strings(&format!(
            "SELECT s.did FROM blocks b JOIN actors s ON s.id = b.subject_id
             WHERE b.author_id = {sid} AND s.status NOT IN (1, 2, 3, 4)"
        ))
        .await?;
    c.check(
        "show_outgoing_blocks = true, without restart: the section lists the account's blocks (inactive targets left out), after the other two",
        row_dids(out).into_iter().collect::<BTreeSet<_>>() == want
            && want.len() == 4
            && between(&page.text, "<section id=\"", "\"")
                == ["blockers", "lists", "outgoing", "blockinglists"]
            && page.text.contains("data-tab=\"blockinglists\"")
            && section(&page.text, "blockinglists")
                .is_some_and(|s| s.contains("Lists this user subscribes to as block lists."))
            && out.contains("Accounts this user has blocked."),
        format!("{} rows, {} expected", row_dids(out).len(), want.len()),
    );
    c.check(
        "the outgoing section prints no coverage either, though its own freshness is lower (this account's records need re-reading)",
        coverage_words_in(&page.text).is_empty() && !out.contains("re-reading") && updated_of(&page.text).is_some(),
        "no banner",
    );
    h.sql(&format!("DELETE FROM relist_debt WHERE actor_id = {sid}"))
        .await?;
    h.set(&[("show_outgoing_blocks", None)]).await?;
    h.refresh().await?;
    let page = h.get(&path_did(&w.s)).await?;
    c.check(
        "show_outgoing_blocks = false: both sections disappear without restart",
        section(&page.text, "outgoing").is_none() && section(&page.text, "blockinglists").is_none(),
        "absent",
    );
    Ok(())
}

// ------------------------------------------------------ 5. admin history

async fn admin_walk(h: &H, first: &str, id: &str) -> Result<(Vec<String>, usize), String> {
    let mut rows = Vec::new();
    let mut url = first.to_owned();
    let mut pages = 0;
    loop {
        let r = h.admin_get(&url).await?;
        if r.status != 200 {
            return Err(format!("{url}: {}", r.short()));
        }
        let sec = card_div(&r.text, id).ok_or_else(|| format!("{url}: no #{id}"))?;
        rows.extend(admin_row_dids(sec));
        pages += 1;
        match cursor_next(sec) {
            Some(n) if pages < 40 => url = n,
            _ => break,
        }
    }
    Ok((rows, pages))
}

async fn check_admin_history(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("5. admin history");
    let sid = h
        .n(&format!("SELECT id FROM actors WHERE did = '{}'", w.s))
        .await?;
    let oid = h
        .n(&format!("SELECT id FROM actors WHERE did = '{}'", w.o1))
        .await?;
    let base = format!("/admin/did/{}/history", w.s);
    let lbase = format!("/admin/list/{}/{LIST}/history", w.o1);
    let mut bad = Vec::new();
    for p in [&base, &lbase] {
        let r = h.get(p).await?;
        if r.status != 303
            || r.header("location").as_deref() != Some("/enter")
            || r.text.contains("did:plc:")
        {
            bad.push(format!("{p}: {}", r.short()));
        }
        // The tables' "Next" links are fetched by htmx, which would swap
        // in whatever a redirect leads to.
        let hx = h.get_with(&h.fresh(), p, &[("hx-request", "true")]).await?;
        if hx.status != 404 || hx.text != "not found" || hx.header("location").is_some() {
            bad.push(format!("{p} (HX-Request): {}", hx.short()));
        }
    }
    c.check(
        "the admin history routes need a session: without one they redirect to /enter like every admin page and show nothing; a request made by htmx gets the bare 404 instead",
        bad.is_empty(),
        bad.join("; "),
    );
    let page = h.admin_get(&base).await?;
    c.check(
        "a logged-in admin gets the account's history page, in the admin layout, never cached",
        page.status == 200
            && page.header("cache-control").as_deref() == Some("no-store, private")
            && page
                .text
                .contains("<a href=\"/admin/settings\">Settings</a>")
            && page
                .text
                .contains(&format!("Removed records naming <code>{}</code>", w.s))
            && page
                .header("content-security-policy")
                .is_some_and(|p| p.contains("script-src 'self'") && !p.contains("unsafe")),
        page.short(),
    );
    let tied = h
        .n(&format!(
            "SELECT max(n) FROM (SELECT count(*) n FROM blocks_history WHERE subject_id = {sid} GROUP BY removed_at) x"
        ))
        .await?;
    let want = h
        .strings(&format!(
            "SELECT a.did FROM blocks_history hh JOIN actors a ON a.id = hh.author_id
             WHERE hh.subject_id = {sid} AND a.status NOT IN (1, 2, 3, 4)
               AND hh.removed_at >= now() - interval '365 days'"
        ))
        .await?;
    let (rows, pages) = admin_walk(h, &base, "removed-blocks").await?;
    let (ok, d) = exactly_once(&rows, &want);
    c.check(
        "removed blocks: walking the cursor across 100 rows with one removed_at returns no row twice and skips none",
        ok && tied == 100 && pages == 3 && want.len() == 102,
        format!("{d}; {tied} tied; {pages} pages"),
    );
    let sec = card_div(&page.text, "removed-blocks").unwrap_or("");
    c.check(
        "the first page is the newest removals, 50 rows, with a next link on the admin route carrying its own cursor",
        admin_row_dids(sec).len() == 50
            && cursor_next(sec).is_some_and(|n| n.starts_with(&base) && n.contains("hb=")),
        format!("{} rows; next {:?}", admin_row_dids(sec).len(), cursor_next(sec)),
    );
    let leaked: Vec<&String> = w.hidden.iter().filter(|d| rows.contains(d)).collect();
    c.check(
        "the display filter binds the admin page too: rows whose author is hidden (all four statuses) are not shown, and a row older than the retention is not shown",
        leaked.is_empty()
            && h.n(&format!("SELECT count(*) FROM blocks_history hh JOIN actors a ON a.id = hh.author_id WHERE hh.subject_id = {sid} AND a.status IN (1, 2, 3, 4)")).await? == 4
            && !rows.contains(&did("lbk", 3)),
        format!("leaked {leaked:?}"),
    );
    c.check(
        "live-row mark and cause wording, with the uncertainty of a reconcile",
        sec.contains("blocks this account again")
            && sec.contains("Block deleted.")
            && sec.contains("Found missing when the author&#x27;s records were re-read. Removed some time between &#x27;last seen&#x27; and this time."),
        "marks present",
    );
    let mem = card_div(&page.text, "removed-memberships").unwrap_or("");
    c.check(
        "removed memberships: the list by name with 'on this list now'; a list with no row by its at-uri; a hidden owner's list left out",
        mem.contains("&lt;script&gt;alert(1)&lt;/script&gt;")
            && mem.contains("on this list now")
            && mem.contains(&format!("at://{}/app.bsky.graph.list/vanished", w.o2))
            && mem.contains("The list was deleted.")
            && mem.contains("Removed from the list.")
            && !mem.contains("hiddenowned"),
        support::truncate(mem, 200),
    );
    let lim = card_div(&page.text, "limits").unwrap_or("");
    c.check(
        "the limits block states both recording windows, the retention and the limits",
        lim.contains("This instance recorded removals from <time datetime=\"20")
            && lim.contains("This instance has recorded removals since <time datetime=\"20")
            && lim.contains("Removals older than 365 days are deleted.")
            && lim.contains("This is most of what is missing.")
            && lim.matches("<li>").count() >= 10,
        support::truncate(lim, 160),
    );
    c.check(
        "rows link into the admin UI (the lookup pages), not to the public pages",
        page.text
            .contains("href=\"/admin/lookup/did?q=did%3Aplc%3A")
            && page
                .text
                .contains("href=\"/admin/lookup/list?q=at%3A%2F%2F")
            && !page.text.contains("href=\"/did/")
            && !page.text.contains("href=\"/list/"),
        "lookup links",
    );

    // The list's history.
    let want = h
        .strings(&format!(
            "SELECT a.did FROM list_items_history hh JOIN actors a ON a.id = hh.subject_id
             WHERE hh.owner_id = {oid} AND hh.list_rkey = '{LIST}' AND a.status NOT IN (1, 2, 3, 4)"
        ))
        .await?;
    let (rows, pages) = admin_walk(h, &lbase, "removed-members").await?;
    let (ok, d) = exactly_once(&rows, &want);
    c.check(
        "removed members of a list: 100 rows at one removed_at page without loss or repeat; inactive members left out",
        ok && pages == 3 && want.len() == 103,
        format!("{d}; {pages} pages"),
    );
    let want = h
        .strings(&format!(
            "SELECT a.did FROM list_blocks_history hh JOIN actors a ON a.id = hh.author_id
             WHERE hh.list_owner_id = {oid} AND hh.list_rkey = '{LIST}' AND a.status NOT IN (1, 2, 3, 4)"
        ))
        .await?;
    let (rows, _) = admin_walk(h, &lbase, "removed-listblocks").await?;
    let lp = h.admin_get(&lbase).await?;
    c.check(
        "removed listblocks of a list: every active former listblocker; 'blocks this list again'; listblock wording",
        rows.iter().cloned().collect::<BTreeSet<_>>() == want
            && want.len() == 5
            && lp.text.contains("blocks this list again")
            && lp.text.contains("Listblock deleted.")
            && lp.text.contains("Record changed to block a different list.")
            && card_div(&lp.text, "removed-members").is_some_and(|s| s.contains("on this list now")),
        format!("{} rows, {} expected", rows.len(), want.len()),
    );
    let dead = h
        .admin_get(&format!("/admin/list/{}/st-dead/history", w.o2))
        .await?;
    let none = h
        .admin_get(&format!("/admin/list/{}/never-seen/history", w.o2))
        .await?;
    let hidden_owner = h
        .admin_get(&format!("/admin/list/{}/hiddenowned/history", w.hidden[1]))
        .await?;
    c.check(
        "a list's history page needs no lists row and says so when nothing was recorded; a hidden owner's list shows no row",
        dead.status == 200
            && none.status == 200
            && none.text.matches("No removals recorded.").count() == 2
            && hidden_owner.status == 200
            && admin_row_dids(&hidden_owner.text).is_empty(),
        format!("{} / {} / {}", dead.status, none.status, hidden_owner.status),
    );
    let mut bad = Vec::new();
    for p in [
        "/admin/did/alice.example/history".to_owned(),
        format!("/admin/list/{}/bad%20key/history", w.o1),
        format!("{base}?hb=!!!"),
    ] {
        let r = h.admin_get(&p).await?;
        if r.status != 400 {
            bad.push(format!("{p}: {}", r.status));
        }
    }
    c.check(
        "a malformed DID, list key or cursor on the admin routes ⇒ 400",
        bad.is_empty(),
        bad.join("; "),
    );

    // Reaching them: links on the lookup pages, which need a session.
    let did_lookup = format!("/admin/lookup/did?q={}", enc(&w.s));
    let list_lookup = format!(
        "/admin/lookup/list?q={}",
        enc(&format!("at://{}/app.bsky.graph.list/{LIST}", w.o1))
    );
    let with = h.admin_get(&did_lookup).await?;
    let without = h.get(&did_lookup).await?;
    let lwith = h.admin_get(&list_lookup).await?;
    let lwithout = h.get(&list_lookup).await?;
    let to_enter = |r: &Resp| {
        r.status == 303
            && r.header("location").as_deref() == Some("/enter")
            && !r.text.contains("/history")
            && !r.text.contains("did:plc:")
    };
    c.check(
        "for a logged-in admin the DID lookup page has a History tab and the list lookup page links to \"View history\"; without a session there is no lookup page, only the redirect to /enter",
        with.text.contains("data-tab=\"history\"")
            && with.text.contains("&amp;tab=history\"")
            && lwith.text.contains(&format!("<a href=\"{lbase}\""))
            && lwith.text.contains("View history")
            && to_enter(&without)
            && to_enter(&lwithout),
        format!(
            "{} / {}; without a session {} / {}",
            with.status, lwith.status, without.status, lwithout.status
        ),
    );
    Ok(())
}

// ------------------------------------------------------- 6–8. withheld

/// Every page of every section of the two data pages, concatenated.
async fn all_text(h: &H, pages: &[String; 2]) -> Result<String, String> {
    let mut text = String::new();
    for (p, ids) in [
        (&pages[0], vec!["blockers", "lists", "outgoing"]),
        (&pages[1], vec!["members", "subscribers"]),
    ] {
        for id in ids {
            let mut url = p.clone();
            for _ in 0..10 {
                let r = h.get(&url).await?;
                text.push_str(&r.text);
                match section(&r.text, id).and_then(next_of) {
                    Some(n) => url = n,
                    None => break,
                }
            }
        }
    }
    Ok(text)
}

async fn check_withheld(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("6. hidden-status filtering");
    let pages = [path_did(&w.s), w.list.clone()];
    h.set(&[("show_outgoing_blocks", Some("on"))]).await?;
    let text = all_text(h, &pages).await?;
    let seen = dids_in(&text);
    // w.hidden is one account per status 1–4: deactivated, taken down,
    // suspended, deleted.
    let (suspended, taken_down) = (&w.hidden[2], &w.hidden[1]);
    let leaked: Vec<&String> = w
        .hidden
        .iter()
        .filter(|d| *d != suspended && seen.contains(*d))
        .collect();
    c.check(
        "deactivated, taken-down and deleted accounts appear in no row of any section of any page — as author, member, owner or target; the suspended account is shown, with a \"suspended\" tag",
        leaked.is_empty()
            && seen.contains(&w.e)
            && seen.contains(suspended)
            // Each page's guide names both tags once; any more are rows.
            && text.matches("<span class=\"acct-tag acct-suspended\">suspended</span>").count()
                > text.matches("<details class=\"guide").count()
            && text.matches("acct-takendown").count()
                == text.matches("<details class=\"guide").count()
            && seen.len() > 120,
        format!("{} DIDs on the pages; leaked: {leaked:?}", seen.len()),
    );
    // "Show taken down accounts": taken-down accounts, on request.
    let rest = h.get(&path_did(&w.s)).await?;
    let on = h.get(&format!("{}?takendown=1", path_did(&w.s))).await?;
    let odd = h
        .get(&format!("{}?takendown=yes&page=2", path_did(&w.s)))
        .await?;
    let (rsec, osec) = (
        section(&rest.text, "blockers").unwrap_or(""),
        section(&on.text, "blockers").unwrap_or(""),
    );
    let lon = h.get(&format!("{}?takendown=1", w.list)).await?;
    let lrest = h.get(&w.list).await?;
    c.check(
        "the heading states both numbers — the blockers shown, and the number counting taken-down accounts — and offers the switch; with ?takendown=1 the taken-down blocker is a row with a \"taken down\" tag, the deactivated and deleted ones still are not, the page links keep the switch and the switch leads back; any other value of the parameter is redirected away",
        rsec.contains("<span class=\"count count-big\">62</span> <span class=\"count-all\">(63 counting taken down accounts)</span>")
            && rsec.contains(&format!("href=\"{}?takendown=1#blockers\"", path_did(&w.s)))
            && rsec.contains("role=\"checkbox\" aria-checked=\"false\"")
            && !row_dids(rsec).contains(taken_down)
            && osec.contains("<span class=\"count count-big\">62</span> <span class=\"count-all\">(63 counting taken down accounts)</span>")
            && osec.contains("aria-checked=\"true\"")
            && osec.contains(&format!("href=\"{}#blockers\"", path_did(&w.s)))
            && osec.contains("<span class=\"acct-tag acct-takendown\">taken down</span>")
            && step_of(osec, "next") == Some(format!("{}?takendown=1&page=2", path_did(&w.s)))
            && {
                let two = h.get(&format!("{}?takendown=1&page=2", path_did(&w.s))).await?;
                let mut all = row_dids(osec);
                all.extend(row_dids(section(&two.text, "blockers").unwrap_or("")));
                all.len() == 63
                    && all.contains(taken_down)
                    && !all.contains(&w.hidden[0])
                    && !all.contains(&w.hidden[3])
            }
            && odd.status == 301
            && odd.header("location").as_deref() == Some(format!("{}?page=2", path_did(&w.s)).as_str())
            && section(&lrest.text, "subscribers").is_some_and(|s| s.contains("(6 counting taken down accounts)") && !row_dids(s).contains(taken_down))
            && section(&lrest.text, "members").is_some_and(|s| s.contains("(64 counting taken down accounts)"))
            && section(&lon.text, "subscribers").is_some_and(|s| row_dids(s).contains(taken_down) && s.contains("acct-takendown")),
        support::truncate(rsec, 300),
    );
    c.check(
        "…while the API still returns rows naming them (the UI is stricter than the API)",
        h.xrpc(
            "query.getListMembers",
            &format!(
                "list={}&limit=1000",
                enc(&format!("at://{}/app.bsky.graph.list/{LIST}", w.o1))
            ),
        )
        .await?["members"]
            .as_array()
            .is_some_and(|m| {
                w.hidden
                    .iter()
                    .all(|d| m.iter().any(|x| x["did"] == d.as_str()))
            }),
        "getListMembers includes the four",
    );

    c.section("7. operator exclusion");
    let before = h.metrics_text().await?;
    let epage = h.get(&path_did(&w.e)).await?;
    c.check(
        "before the exclusion the account has an ordinary page",
        epage.status == 200 && section(&epage.text, "blockers").is_some(),
        epage.short(),
    );
    let bad_list = h
        .post_settings(&[("excluded_dids", Some("alice.example"))])
        .await?;
    c.check(
        "an excluded_dids entry that is not a DID is refused on submit, naming the line",
        bad_list.text.contains("line 1") && bad_list.text.contains("is not a DID"),
        banner(&bad_list.text),
    );
    let many: String = (0..10_001)
        .map(|i| format!("did:web:h{i}.example\n"))
        .collect();
    let too_many = h.post_settings(&[("excluded_dids", Some(&many))]).await?;
    c.check(
        "more than 10,000 excluded DIDs are refused",
        too_many.text.contains("at most 10000"),
        banner(&too_many.text),
    );
    h.set(&[("excluded_dids", Some(&format!("{}\n", w.e)))])
        .await?;
    let text = all_text(h, &pages).await?;
    c.check(
        "excluded_dids takes effect without restart: the account is in no row of any section of any page",
        !dids_in(&text).contains(&w.e),
        "absent",
    );
    let spage = h.get(&path_did(&w.s)).await?;
    c.check(
        "a list the excluded account owns is left out of 'On lists', and the page says nothing about what it leaves out",
        section(&spage.text, "lists").is_some_and(|s| !s.contains("/excluded\""))
            && coverage_words_in(&spage.text).is_empty(),
        "no sentence",
    );
    c.check(
        "the exclusion changes what the public pages show, nothing else: the API still returns the account's rows",
        h.xrpc("query.getIncomingBlocks", &format!("actor={}&limit=1000", enc(&w.s)))
            .await?["blocks"]
            .as_array()
            .is_some_and(|b| b.iter().any(|x| x["did"] == w.e.as_str())),
        "getIncomingBlocks includes it",
    );
    let (rows, _) = admin_walk(h, &format!("/admin/did/{}/history", w.s), "removed-blocks").await?;
    c.check(
        "excluded_dids governs the public pages only: the admin history page still shows the excluded account's removed block",
        rows.contains(&w.e),
        format!("{} rows", rows.len()),
    );

    c.section("8. withheld unification");
    let e_page = h.get(&path_did(&w.e)).await?;
    let e_list = h.get(&format!("/list/{}/excluded", w.e)).await?;
    let only_notice = |name: &str, r: &Resp, notice: &str| -> Vec<String> {
        [
            ("status 200", r.status == 200),
            ("the notice", r.text.contains(notice)),
            ("the bar", r.text.contains("<nav class=\"public-nav\"")),
            ("no section", !r.text.contains("<section")),
            (
                "no link to a data page",
                !r.text.contains("href=\"/did/") && !r.text.contains("href=\"/list/"),
            ),
            ("no table", !r.text.contains("<table")),
            ("no Last updated line", !r.text.contains("Last updated")),
            (
                "max-age=30",
                r.header("cache-control").as_deref() == Some("public, max-age=30"),
            ),
        ]
        .iter()
        .filter(|(_, ok)| !ok)
        .map(|(what, _)| format!("{name}: {what}"))
        .collect()
    };
    let mut wrong = only_notice("page", &e_page, WITHHELD_ACCOUNT);
    wrong.extend(only_notice("list", &e_list, WITHHELD_LIST));
    c.check(
        "an excluded account's page and the page of a list it owns show the bar, the notice and nothing else",
        wrong.is_empty(),
        wrong.join("; "),
    );
    c.check(
        "the preview tags of a withheld page carry the DID alone",
        meta(&e_page.text, "og:title") == Some(w.e.as_str())
            && meta(&e_page.text, "twitter:title") == Some(w.e.as_str()),
        format!("{:?}", meta(&e_page.text, "og:title")),
    );
    let mut identical = true;
    let mut detail = Vec::new();
    let canon = |text: &str, d: &str| text.replace(d, "DID");
    for (i, d) in w.hidden.iter().enumerate() {
        let p = h.get(&path_did(d)).await?;
        let ok = canon(&p.text, d) == canon(&e_page.text, &w.e)
            && p.status == e_page.status
            && p.header("cache-control") == e_page.header("cache-control")
            && p.header("x-robots-tag") == e_page.header("x-robots-tag");
        identical &= ok;
        detail.push(format!(
            "status {}: {}",
            i + 1,
            if ok { "identical" } else { "DIFFERS" }
        ));
    }
    let hl = h.get(&format!("/list/{}/hiddenowned", w.hidden[1])).await?;
    identical &= canon(&hl.text, &w.hidden[1]).replace("hiddenowned", "L")
        == canon(&e_list.text, &w.e).replace("excluded", "L");
    c.check(
        "hidden-status and operator-excluded accounts get byte-identical responses (body and headers), for all four statuses and for lists they own",
        identical,
        detail.join(", "),
    );
    // Cards: one 404 for every account this instance does not show.
    let unknown = h.get(&format!("/card/{}", w.unknown)).await?;
    let mut same = unknown.status == 404
        && unknown.header("cache-control").as_deref() == Some("no-store")
        && !unknown.text.contains("did:");
    for d in w.hidden.iter().chain([&w.e]) {
        let r = h.get(&format!("/card/{d}")).await?;
        same &= r.status == 404
            && r.text == unknown.text
            && r.header("cache-control") == unknown.header("cache-control")
            && r.header("x-robots-tag") == unknown.header("x-robots-tag");
    }
    c.check(
        "a card for an unknown, a hidden or an excluded account is the same 404, no-store",
        same,
        unknown.short(),
    );
    let after = h.metrics_text().await?;
    let delta = |reason: &str| {
        metric(
            &after,
            "farsight_public_ui_withheld_total",
            &[("reason", reason)],
        ) - metric(
            &before,
            "farsight_public_ui_withheld_total",
            &[("reason", reason)],
        )
    };
    c.check(
        "farsight_public_ui_withheld_total counts both reasons separately (the pages never show which)",
        delta("operator_excluded") >= 2.0 && delta("hidden_status") >= 5.0,
        format!("operator_excluded +{}, hidden_status +{}", delta("operator_excluded"), delta("hidden_status")),
    );
    h.set(&[("show_outgoing_blocks", None)]).await?;
    Ok(())
}

// --------------------------------------------- 9–13. settings and headers

async fn check_opengraph(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("9. OpenGraph");
    let p = h
        .get_with(&h.fresh(), &path_did(&w.s), &[("host", "evil.example")])
        .await?;
    let want_url = format!("https://{HOSTNAME}{}", path_did(&w.s));
    let image = format!("https://{HOSTNAME}/static/og-default.png");
    let site = format!("Farsight at {HOSTNAME}");
    c.check(
        "text tags present; og:url and og:image come from server.hostname, not the Host header",
        meta(&p.text, "og:title") == Some(w.s.as_str())
            && meta(&p.text, "og:description")
                .is_some_and(|d| d.starts_with("Block records for this account"))
            && meta(&p.text, "og:url") == Some(want_url.as_str())
            && meta(&p.text, "og:type") == Some("website")
            && meta(&p.text, "og:site_name") == Some(site.as_str())
            && meta(&p.text, "og:image") == Some(image.as_str())
            && meta(&p.text, "twitter:card") == Some("summary_large_image")
            && !p.text.contains("evil.example"),
        format!(
            "{:?} {:?}",
            meta(&p.text, "og:url"),
            meta(&p.text, "og:image")
        ),
    );
    c.check(
        "no data in the tags: the description is fixed text with no count",
        meta(&p.text, "og:description").is_some_and(|d| !d.chars().any(|ch| ch.is_ascii_digit())),
        meta(&p.text, "og:description").unwrap_or("").to_owned(),
    );
    let l = h.get(&w.list).await?;
    c.check(
        "a list's preview names the list and its at-uri",
        meta(&l.text, "og:title").is_some_and(|t| {
            t.contains("alert(1)") && t.contains("app.bsky.graph.list") && !t.contains('<')
        }),
        meta(&l.text, "og:title").unwrap_or("").to_owned(),
    );
    h.set(&[("show_opengraph_image", None)]).await?;
    let p = h.get(&path_did(&w.s)).await?;
    c.check(
        "show_opengraph_image = false: og:image is gone, the other tags stay, twitter:card is summary",
        meta(&p.text, "og:image").is_none()
            && meta(&p.text, "twitter:image").is_none()
            && meta(&p.text, "og:title").is_some()
            && meta(&p.text, "og:url").is_some()
            && meta(&p.text, "twitter:card") == Some("summary"),
        format!("{:?}", meta(&p.text, "twitter:card")),
    );
    h.set(&[("show_opengraph_image", Some("on"))]).await?;
    Ok(())
}

async fn check_cache_and_headers(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("11. cache and security headers");
    let mut bad = Vec::new();
    for (p, want) in [
        ("/".to_owned(), "public, max-age=60"),
        (path_did(&w.s), "public, max-age=30"),
        (w.list.clone(), "public, max-age=30"),
        (format!("/search?q={}", w.s), "no-store"),
        ("/search?q=".to_owned(), "no-store"),
        ("/did/nope".to_owned(), "no-store"),
        // A card whose fetch failed (the harness's PLC is unreachable).
        (format!("/card/{}", w.s), "no-store"),
        (format!("/card/{}", w.unknown), "no-store"),
        ("/robots.txt".to_owned(), "public, max-age=300"),
    ] {
        let r = h.get(&p).await?;
        if r.header("cache-control").as_deref() != Some(want) {
            bad.push(format!("{p}: {:?}", r.header("cache-control")));
        }
        if is_public(&p)
            && (r.header("content-security-policy").as_deref() != Some(CSP_AVATARS)
                || r.header("x-content-type-options").as_deref() != Some("nosniff")
                || r.header("referrer-policy").as_deref() != Some("same-origin")
                || r.header("set-cookie").is_some()
                || r.header("access-control-allow-origin").is_some()
                || r.header("ratelimit").is_some())
        {
            bad.push(format!("{p}: security headers"));
        }
    }
    c.check(
        "Cache-Control per class (home 60, data pages 30, search, errors and incomplete cards no-store, robots 300); CSP, nosniff and Referrer-Policy on every response; no cookie, CORS or RateLimit header",
        bad.is_empty(),
        bad.join("; "),
    );
    c.check(
        "with show_avatars on the CSP differs from the strict one in img-src alone, and still allows no inline or foreign script",
        CSP_AVATARS.replace("img-src 'self' https:", "img-src 'self'") == CSP
            && CSP_AVATARS.contains("script-src 'self';")
            && !CSP_AVATARS.contains("unsafe"),
        "img-src 'self' https:",
    );
    Ok(())
}

async fn check_independence(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("14b. responses do not depend on the caller");
    let p = path_did(&w.s);
    let plain = h.get(&p).await?;
    let cookie = h.get_with(&h.fresh(), &p, &[("cookie", &h.cookie)]).await?;
    let hx = h
        .get_with(
            &h.fresh(),
            &p,
            &[("hx-request", "true"), ("hx-target", "blockers")],
        )
        .await?;
    let host = h
        .get_with(
            &h.fresh(),
            &p,
            &[
                ("host", "evil.example"),
                ("x-forwarded-host", "evil.example"),
                ("accept-language", "fr"),
            ],
        )
        .await?;
    let n = normalized(&plain.text);
    c.check(
        "the same bytes with and without an admin cookie, HX-Request, and a forged Host (times aside)",
        n == normalized(&cookie.text) && n == normalized(&hx.text) && n == normalized(&host.text) && n.len() > 2_000,
        format!("{} bytes compared", n.len()),
    );
    c.check(
        "a logged-in admin gets a cacheable public response like anyone else",
        cookie.header("cache-control").as_deref() == Some("public, max-age=30")
            && cookie.header("set-cookie").is_none(),
        format!("{:?}", cookie.header("cache-control")),
    );
    Ok(())
}

async fn check_keys(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("12. each public_ui key, without restart");
    // dark_mode_default
    let sys = h.get("/").await?;
    h.set(&[("dark_mode_default", Some("dark"))]).await?;
    let dark = h.get("/").await?;
    h.set(&[("dark_mode_default", Some("light"))]).await?;
    let light = h.get("/").await?;
    h.set(&[("dark_mode_default", Some("system"))]).await?;
    c.check(
        "dark_mode_default sets the initial data-theme on <html>: none for system, dark, light",
        sys.text
            .contains("<html lang=\"en\" data-theme-default=\"system\">")
            && dark
                .text
                .contains("<html lang=\"en\" data-theme=\"dark\" data-theme-default=\"dark\">")
            && light
                .text
                .contains("<html lang=\"en\" data-theme=\"light\" data-theme-default=\"light\">"),
        "three values",
    );
    let css = h.get("/static/public.css").await?;
    let js = h.get("/static/public.js").await?;
    let toggle = between(&sys.text, "<div class=\"theme-toggle\"", "</div>");
    c.check(
        "the toggle is in the bar: three labelled buttons (light, dark, system), hidden until the script shows it",
        toggle.len() == 1
            && toggle[0].contains(" hidden>")
            && ["light", "dark", "system"].iter().all(|t| {
                toggle[0].contains(&format!("data-theme-choice=\"{t}\""))
            })
            && toggle[0].matches("aria-label=\"").count() == 4,
        support::truncate(toggle.first().copied().unwrap_or(""), 200),
    );
    c.check(
        "one stylesheet with color variables, a prefers-color-scheme rule and [data-theme] rules; the script stores the choice in localStorage under farsight-theme and knows \"system\"; no external font",
        css.text.contains("@media (prefers-color-scheme: dark)")
            && css.text.contains(":root:not([data-theme=\"light\"])")
            && css.text.contains(":root[data-theme=\"dark\"]")
            && css.text.contains("--bg:")
            && !css.text.contains("@font-face")
            && !css.text.contains("url(")
            && js.text.contains("var KEY = \"farsight-theme\";")
            && js.text.contains("localStorage.setItem(KEY, choice)")
            && js.text.contains("\"system\""),
        "css and js",
    );
    c.check(
        "the bar is sticky and cards are hover-only in the stylesheet: position: sticky; top: 0, and the card rule sits under @media (hover: hover)",
        between(&css.text, "nav.public-nav {", "}")
            .first()
            .is_some_and(|r| r.contains("position: sticky;") && r.contains("top: 0;"))
            && css.text.contains("scroll-margin-top")
            && css.text.contains(".profile-card { display: none; }")
            && between(&css.text, "@media (hover: hover) {", "}\n}")
                .first()
                .is_some_and(|r| {
                    r.contains(".who-wrap.open .profile-card {") && r.contains("display: block;")
                })
            && js.text.contains("window.matchMedia(\"(hover: hover)\").matches"),
        "css rules",
    );
    // instance_description, contact
    let default = h.get("/").await?;
    h.set(&[
        (
            "instance_description",
            Some("First paragraph <b>plain</b>.\r\n\r\nSecond paragraph."),
        ),
        ("contact", Some("mailto:public@farsight.test")),
    ])
    .await?;
    let custom = h.get("/").await?;
    let account = h.get(&path_did(&w.s)).await?;
    c.check(
        "instance_description (escaped plain text, blank line = paragraph) replaces the default on home; no public page shows the contact, set or not",
        default.text.contains("Farsight is an independent index of public block records")
            && !default.text.contains("mailto:")
            && custom.text.contains(">First paragraph &lt;b&gt;plain&lt;/b&gt;.</p>")
            && custom.text.contains(">Second paragraph.</p>")
            && !custom.text.contains("Farsight is an independent index of public block records")
            && !custom.text.contains("mailto:")
            && !account.text.contains("mailto:"),
        "home",
    );
    c.check(
        "home: instance name, the description, a search form, the totals, the guide and the Last updated line; no label above the name, no link to About",
        custom.text.contains("<h1>Farsight</h1>")
            && !custom.text.contains("Accepts a handle")
            && !custom.text.contains("hero-badge")
            && custom.text.contains("<dl class=\"home-totals\">")
            && ["Blocks indexed", "Lists tracked", "Accounts seen"]
                .iter()
                .all(|t| custom.text.contains(&format!("<dt>{t}</dt><dd>")))
            && custom.text.contains("<details class=\"guide home-guide\">")
            && custom.text.contains("<span>How to read a page</span></summary>")
            && custom.text.contains("acct-takendown\">taken down</span>")
            && custom.text.matches("action=\"/search\"").count() == 1
            && updated_of(&custom.text).is_some()
            && !custom.text.contains("/about")
            && !custom.text.contains("Instance-wide"),
        "home",
    );
    let icon = h.get("/static/favicon.svg").await?;
    c.check(
        "the tab icon is an SVG this instance serves",
        icon.status == 200
            && icon.header("content-type").as_deref() == Some("image/svg+xml")
            && icon.text.starts_with("<svg "),
        format!("{} {:?}", icon.status, icon.header("content-type")),
    );
    h.set(&[("instance_description", Some("")), ("contact", Some(""))])
        .await?;

    // show_avatars: the CSP follows it at once.
    h.set(&[("show_avatars", None)]).await?;
    let strict = h.get(&path_did(&w.s)).await?;
    h.set(&[("show_avatars", Some("on"))]).await?;
    let loose = h.get(&path_did(&w.s)).await?;
    c.check(
        "show_avatars = false, without restart: img-src goes back to 'self'; on again: 'self' https:",
        strict.header("content-security-policy").as_deref() == Some(CSP)
            && loose.header("content-security-policy").as_deref() == Some(CSP_AVATARS),
        format!("{:?}", strict.header("content-security-policy")),
    );

    c.section("13. robots.txt and X-Robots-Tag");
    let robots = h.get("/robots.txt").await?;
    let page = h.get(&path_did(&w.s)).await?;
    c.check(
        "crawlable = false: Disallow: / and noindex on every page",
        robots.text == "User-agent: *\nDisallow: /\n"
            && page.header("x-robots-tag").as_deref() == Some("noindex, nofollow")
            && h.get("/").await?.header("x-robots-tag").as_deref() == Some("noindex, nofollow"),
        robots.text.replace('\n', " / "),
    );
    h.set(&[("crawlable", Some("on"))]).await?;
    let robots = h.get("/robots.txt").await?;
    let mut bad = Vec::new();
    for (p, indexable) in [
        ("/".to_owned(), true),
        (path_did(&w.s), true),
        (w.list.clone(), true),
        (format!("/card/{}", w.s), false),
        (format!("/search?q={}", w.s), false),
        ("/did/nope".to_owned(), false),
        (path_did(&w.e), false),
    ] {
        let r = h.get(&p).await?;
        let tagged = r.header("x-robots-tag").as_deref() == Some("noindex, nofollow");
        if tagged == indexable {
            bad.push(format!("{p}: {:?}", r.header("x-robots-tag")));
        }
    }
    c.check(
        "crawlable = true: robots.txt keeps crawlers out of the admin UI, sign-in, the wizard, the API, search and cards and the health endpoints, and allows the rest; cards, search, error and withheld pages stay noindex",
        robots.text
            == "User-agent: *\nDisallow: /admin\nDisallow: /enter\nDisallow: /setup\nDisallow: /xrpc/\nDisallow: /search\nDisallow: /card/\nDisallow: /health\nDisallow: /livez\nAllow: /\n"
            && bad.is_empty(),
        if bad.is_empty() { robots.text.replace('\n', " / ") } else { bad.join("; ") },
    );
    h.set(&[("crawlable", None)]).await?;
    c.check(
        "crawlable = false again: robots.txt is back to Disallow: /",
        h.get("/robots.txt").await?.text == "User-agent: *\nDisallow: /\n",
        "restored",
    );
    Ok(())
}

// ------------------------------------------------------ 15. record links

/// The record cells of the DID lookup's block table: the at-uri each
/// "Copy at:// URL" button holds, and the viewer link beside it, if any.
fn did_records(html: &str) -> Vec<(String, Option<String>)> {
    between(html, "<td class=\"td-record\">", "</td>")
        .into_iter()
        .filter_map(|cell| {
            let rest =
                cell.strip_prefix("<button type=\"button\" class=\"copy-uri\" data-copy=\"")?;
            let (uri, tail) = rest.split_once('"')?;
            let tail = tail.strip_prefix(&format!(" title=\"{uri}\">Copy at:// URL</button>"))?;
            let link = match tail.strip_prefix(" <a class=\"record-view\" href=\"") {
                Some(x) => {
                    let (href, end) = x.split_once('"')?;
                    (end == " target=\"_blank\" rel=\"noopener noreferrer nofollow\">View</a>")
                        .then(|| href.to_owned())?
                        .into()
                }
                None if tail.is_empty() => None,
                None => return None,
            };
            Some((uri.to_owned(), link))
        })
        .collect()
}

/// The viewer address the template gives for an at-uri.
fn viewer_href(uri: &str) -> String {
    format!(
        "https://viewer.example/at/{}",
        uri.trim_start_matches("at://")
    )
}

async fn check_record_links(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("15. record links");
    h.set(&[("show_outgoing_blocks", Some("on"))]).await?;
    let sid = h
        .n(&format!("SELECT id FROM actors WHERE did = '{}'", w.s))
        .await?;
    let lid = h
        .n(&format!(
            "SELECT l.id FROM lists l JOIN actors o ON o.id = l.owner_id WHERE o.did = '{}' AND l.rkey = '{LIST}'",
            w.o1
        ))
        .await?;
    // What the lookup pages (read with the admin session: there is no
    // anonymous lookup) list: incoming blocks without hidden blockers
    // (as the API), every listblock and every item of the list.
    let incoming = h
        .strings(&format!(
            "SELECT 'at://' || a.did || '/app.bsky.graph.block/' || b.rkey FROM blocks b JOIN actors a ON a.id = b.author_id
             WHERE b.subject_id = {sid} AND a.status NOT IN (1, 2, 3, 4)"
        ))
        .await?;
    let on_list = h
        .strings(&format!(
            "SELECT 'at://' || a.did || '/app.bsky.graph.listblock/' || b.rkey FROM list_blocks b JOIN actors a ON a.id = b.author_id
             WHERE b.list_id = {lid}
             UNION ALL
             SELECT 'at://{}/app.bsky.graph.listitem/' || li.rkey FROM list_items li WHERE li.list_id = {lid}",
            w.o1
        ))
        .await?;
    let did_lookup = format!("/admin/lookup/did?q={}", enc(&w.s));
    let list_lookup = format!(
        "/admin/lookup/list?q={}",
        enc(&format!("at://{}/app.bsky.graph.list/{LIST}", w.o1))
    );
    let heads = |html: &str, id: &str| -> Vec<String> {
        heads_of(section(html, id).unwrap_or(""))
            .into_iter()
            .map(str::to_owned)
            .collect()
    };
    // No public table has a Record column, whatever the viewer setting.
    let public_clean = |page: &Resp, lp: &Resp| {
        let block_heads = ["Account", "Created"];
        heads(&page.text, "blockers") == block_heads
            && heads(&page.text, "outgoing") == block_heads
            && heads(&lp.text, "subscribers") == block_heads
            && heads(&lp.text, "members") == ["Account", "Added"]
            && !heads(&page.text, "lists").contains(&"Record".to_owned())
            && [page, lp].iter().all(|r| {
                !r.text.contains("class=\"record\"")
                    && !r.text.contains("target=\"_blank\"")
                    && !r.text.contains("/app.bsky.graph.block/")
                    && !r.text.contains("/app.bsky.graph.listblock/")
            })
    };
    let page = h.get(&path_did(&w.s)).await?;
    let lp = h.get(&w.list).await?;
    c.check(
        "no public table has a Record column: the three block tables are Account and Created, and no public page carries a block or listblock at-uri",
        public_clean(&page, &lp),
        format!("{:?}", heads(&page.text, "blockers")),
    );
    let dl = h.admin_get(&did_lookup).await?;
    let ll = h.admin_get(&list_lookup).await?;
    let (b, l) = (did_records(&dl.text), did_records(&ll.text));
    c.check(
        "record_viewer_url empty: on the lookup pages every record cell is a button holding the stored record's at-uri, which is not printed in a cell — no link anywhere",
        b.len() == 50
            && b.iter().all(|(uri, link)| incoming.contains(uri) && link.is_none())
            && !dl.text.contains(">at://")
            && !l.is_empty()
            && l.iter().all(|(uri, link)| on_list.contains(uri) && link.is_none())
            && l.iter().any(|(uri, _)| uri.contains("/app.bsky.graph.listitem/"))
            && l.iter().any(|(uri, _)| uri.contains("/app.bsky.graph.listblock/"))
            && !dl.text.contains("target=\"_blank\"")
            && !ll.text.contains("target=\"_blank\""),
        format!("{} / {} cells; first {:?}", b.len(), l.len(), b.first()),
    );
    let refused = h
        .post_settings(&[(
            "record_viewer_url",
            Some("https://viewer.example/at/{authority}/{collection}"),
        )])
        .await?;
    c.check(
        "Settings refuses a viewer URL without its placeholders, naming the missing one under the form, and writes nothing",
        banner(&refused.text).contains("Record viewer URL: `{rkey}` is missing.")
            && h.config_text()?.contains("record_viewer_url = \"\""),
        banner(&refused.text),
    );
    h.set(&[("record_viewer_url", Some(VIEWER))]).await?;
    let dl = h.admin_get(&did_lookup).await?;
    let ll = h.admin_get(&list_lookup).await?;
    let (b, l) = (did_records(&dl.text), did_records(&ll.text));
    c.check(
        "record_viewer_url set, without restart: on the lookup pages a View link stands beside every button, built from the template with the record's authority, collection and rkey, opening in a new tab with rel=noopener noreferrer nofollow, its text the at-uri",
        b.len() == 50
            && b.iter().all(|(uri, link)| {
                incoming.contains(uri)
                    && uri.contains("/app.bsky.graph.block/")
                    && link.as_deref() == Some(viewer_href(uri).as_str())
            })
            && !l.is_empty()
            && l.iter().all(|(uri, link)| {
                on_list.contains(uri) && link.as_deref() == Some(viewer_href(uri).as_str())
            }),
        format!("first {:?}", b.first()),
    );
    let page = h.get(&path_did(&w.s)).await?;
    let lp = h.get(&w.list).await?;
    let outside: Vec<String> = hrefs(&page.text)
        .into_iter()
        .chain(hrefs(&lp.text))
        .filter(|x| !own_href(x))
        .collect();
    c.check(
        "with a viewer set the public pages are unchanged: no Record column, and no link to anywhere outside the public UI",
        public_clean(&page, &lp) && outside.is_empty(),
        format!("{outside:?}"),
    );
    h.set(&[
        ("record_viewer_url", Some("")),
        ("show_outgoing_blocks", None),
    ])
    .await?;
    let dl = h.admin_get(&did_lookup).await?;
    let again = did_records(&dl.text);
    c.check(
        "record_viewer_url emptied again: the buttons alone again",
        again.len() == 50
            && again
                .iter()
                .all(|(uri, link)| incoming.contains(uri) && link.is_none()),
        "no link",
    );
    Ok(())
}

// -------------------------------------------------------- 16. cards

fn cards_metric(text: &str, outcome: &str) -> f64 {
    metric(
        text,
        "farsight_public_ui_cards_total",
        &[("outcome", outcome)],
    )
}

async fn check_cards(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("16. profile cards");
    let resolutions = |t: &str| metric(t, "farsight_public_ui_handle_resolutions_total", &[]);
    // Let the budget fill.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let m0 = h.metrics_text().await?;
    let target = did("blk", 7);
    let r = h.get(&format!("/card/{target}")).await?;
    let m1 = h.metrics_text().await?;
    c.check(
        "the card is an HTML fragment: no document, no bar, no footer, no preview tags; never indexed",
        r.status == 200
            && r.header("content-type").is_some_and(|t| t.starts_with("text/html"))
            && r.text.starts_with("<div class=\"pc\">")
            && !r.text.contains("<html")
            && !r.text.contains("<nav")
            && !r.text.contains("<footer")
            && !r.text.contains("og:")
            && !r.text.contains("<script")
            && r.header("x-robots-tag").as_deref() == Some("noindex, nofollow"),
        r.short(),
    );
    c.check(
        "when the PLC directory cannot be read (here: an address the safe client refuses) the card has the DID, no image, \"DID created: unavailable\" and no age; it is not cached; outcome plc_timeout",
        r.text.contains(&format!("<code class=\"pc-did\">{target}</code>"))
            && r.text.contains("<dt>DID created</dt><dd>unavailable</dd>")
            && r.text.contains(SHORT_CARD)
            && !r.text.contains("<img")
            && !r.text.contains("pc-age")
            && r.header("cache-control").as_deref() == Some("no-store")
            && cards_metric(&m1, "plc_timeout") - cards_metric(&m0, "plc_timeout") == 1.0,
        format!("plc_timeout +{}", cards_metric(&m1, "plc_timeout") - cards_metric(&m0, "plc_timeout")),
    );
    // Nothing is fetched for an account this instance does not hold or
    // does not show.
    let unknown = h.get(&format!("/card/{}", w.unknown)).await?;
    let hidden = h.get(&format!("/card/{}", w.hidden[0])).await?;
    let m2 = h.metrics_text().await?;
    let total = |t: &str| {
        [
            "served",
            "rate_limited",
            "plc_timeout",
            "pds_failed",
            "avatars_disabled",
        ]
        .iter()
        .map(|o| cards_metric(t, o))
        .sum::<f64>()
    };
    c.check(
        "an unknown and a hidden account get the same 404 at once, and nothing is fetched or counted for them",
        unknown.status == 404
            && hidden.status == 404
            && unknown.text == hidden.text
            && unknown.elapsed < Duration::from_secs(1)
            && total(&m2) == total(&m1)
            && resolutions(&m2) == resolutions(&m1)
            && h.n(&format!("SELECT count(*) FROM actors WHERE did = '{}'", w.unknown)).await? == 0,
        format!("{} / {} in {:?}", unknown.status, hidden.status, unknown.elapsed),
    );

    // The process-wide budget at its defaults: 4 a second, burst 8.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let before = h.metrics_text().await?;
    let mut tasks = Vec::new();
    for i in 1..=16u64 {
        let (http, url) = (h.fresh(), format!("{}/card/{}", h.base, did("mem", i)));
        tasks.push(tokio::spawn(async move { http.get(&url, &[]).await }));
    }
    let mut fetched = 0;
    let mut short = 0;
    let mut other = Vec::new();
    for t in tasks {
        let r = t.await.map_err(|e| e.to_string())??;
        if r.status == 200 && r.text.contains("<dt>DID created</dt>") {
            fetched += 1;
        } else if r.status == 200
            && r.text.contains(SHORT_CARD)
            && r.text.contains("did:plc:")
            && r.header("cache-control").as_deref() == Some("no-store")
        {
            short += 1;
        } else {
            other.push(r.short());
        }
    }
    let after = h.metrics_text().await?;
    let limited = cards_metric(&after, "rate_limited") - cards_metric(&before, "rate_limited");
    c.check(
        "the card budget at its defaults (card_rps 4, card_burst 8): of 16 cards asked at once from 16 addresses, the burst is fetched and the rest get the short card — the DID and \"Profile not available right now.\" — with nothing fetched; outcome rate_limited",
        (8..=10).contains(&fetched) && short == 16 - fetched && other.is_empty() && limited == f64::from(short),
        format!("{fetched} fetched, {short} short, rate_limited +{limited}; {other:?}"),
    );
    // Hot: a lower rate applies at once.
    h.set(&[("card_rps", Some("1")), ("card_burst", Some("2"))])
        .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut kinds = Vec::new();
    for i in 20..=25u64 {
        let r = h.get(&format!("/card/{}", did("mem", i))).await?;
        kinds.push(if r.text.contains("<dt>DID created</dt>") {
            'f'
        } else {
            's'
        });
    }
    c.check(
        "card_rps = 1, card_burst = 2, without restart: of six cards in a row the first two are fetched and the others are short",
        kinds[..2] == ['f', 'f'] && kinds[2..].iter().filter(|k| **k == 's').count() >= 3,
        kinds.iter().collect::<String>(),
    );
    let raised = h
        .post_settings(&[("card_rps", Some("6")), ("card_burst", Some("2"))])
        .await?;
    c.check(
        "a burst below the rate is saved raised to the rate, and the save says so",
        raised.text.contains("was raised to 6")
            && h.config_text()?.contains("card_rps = 6")
            && h.config_text()?.contains("card_burst = 6"),
        banner(&raised.text),
    );
    h.set(&[("card_rps", Some("4")), ("card_burst", Some("8"))])
        .await?;

    // The per-address class: 2 a second, burst 20, before anything else.
    let before = h.metrics_text().await?;
    let one = h.fresh();
    let mut codes = Vec::new();
    let mut limited = None;
    for _ in 0..26 {
        let r = h
            .get_with(&one, &format!("/card/{}", w.unknown), &[])
            .await?;
        codes.push(r.status);
        if r.status == 429 {
            limited = Some(r);
        }
    }
    let elsewhere = h.get(&format!("/card/{}", w.unknown)).await?;
    let after = h.metrics_text().await?;
    let class = |t: &str| {
        metric(
            t,
            "farsight_rate_limited_total",
            &[("class", "public_ui_card")],
        )
    };
    c.check(
        "cards have their own per-address class (burst 20): the 21st quick request from one address is 429 with Retry-After, no-store; another address is unaffected; farsight_rate_limited_total{class=public_ui_card} counts it",
        codes[..20].iter().all(|s| *s == 404)
            && codes.iter().filter(|s| **s == 429).count() >= 4
            && limited.as_ref().is_some_and(|r| {
                r.header("retry-after").is_some_and(|v| v.parse::<u64>().is_ok_and(|n| n >= 1))
                    && r.header("cache-control").as_deref() == Some("no-store")
            })
            && elsewhere.status == 404
            && class(&after) - class(&before) >= 4.0,
        format!("{codes:?}"),
    );
    let page_after = h.get_with(&one, &path_did(&w.s), &[]).await?;
    c.check(
        "…and it does not spend the visitor's page budget: the same address still gets its page",
        page_after.status == 200,
        page_after.status.to_string(),
    );
    Ok(())
}

/// Cards against the real network: a real account's PLC log, handle and
/// profile record.
async fn check_cards_live(c: &mut Checks, l: &mut H, w: &World) -> Result<(), String> {
    c.section("16b. profile cards, live");
    // What the harness itself reads from the PLC directory.
    let log = Http::new(None)
        .get(&format!("{PLC}/{LIVE_DID}/log/audit"), &[])
        .await;
    let (created, pds) = match &log {
        Ok(r) if r.status == 200 => {
            let entries = r.body.as_array().cloned().unwrap_or_default();
            let created = entries
                .first()
                .and_then(|e| e["createdAt"].as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|t| {
                    t.with_timezone(&chrono::Utc)
                        .format("%Y-%m-%dT%H:%M:%SZ")
                        .to_string()
                });
            let pds = entries
                .iter()
                .rev()
                .find(|e| e["nullified"] != true)
                .and_then(|e| e["operation"]["services"]["atproto_pds"]["endpoint"].as_str())
                .map(|s| s.trim_end_matches('/').to_owned());
            (created, pds)
        }
        _ => (None, None),
    };
    let (Some(created), Some(pds)) = (created, pds) else {
        c.unverified(
            "a real account's card (creation date, handle, avatar)",
            "needs the network: the PLC directory could not be read from here",
        );
        return Ok(());
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    let m0 = l.metrics_text().await?;
    let r = l.get(&format!("/card/{LIVE_DID}")).await?;
    let m1 = l.metrics_text().await?;
    let img = between(&r.text, "<img class=\"pc-avatar\" src=\"", "\"")
        .first()
        .map(|s| s.replace("&amp;", "&"))
        .unwrap_or_default();
    let cid = img.split("&cid=").nth(1).unwrap_or("").to_owned();
    c.check(
        "a did:plc account's card: the verified handle, the DID, and the creation date — the createdAt of the first entry of its PLC audit log, as the harness reads it from the directory",
        r.status == 200
            && r.text.contains(&format!("<p class=\"handle\">{LIVE_HANDLE}</p>"))
            && r.text.contains(&format!("<code class=\"pc-did\">{LIVE_DID}</code>"))
            && r.text.contains(&format!("<dt>DID created</dt><dd><time datetime=\"{created}\">"))
            && r.text.contains(" UTC</time></dd>")
            && r.text.matches("class=\"pc-age\" hidden").count() == 2
            && !r.text.contains(SHORT_CARD),
        format!("created {created}; {}", support::truncate(&r.text, 200)),
    );
    c.check(
        "show_avatars = true: the card names the avatar blob on the account's own PDS — the endpoint in its PLC log — and Farsight serves no image itself",
        img.starts_with(&format!("{pds}/xrpc/com.atproto.sync.getBlob?did=did%3Aplc%3A"))
            && cid.starts_with("baf")
            && cid.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            && r.text.contains("alt=\"\" width=\"")
            && r.text.contains("loading=\"lazy\" referrerpolicy=\"no-referrer\">")
            && !img.contains(HOSTNAME)
            && !img.starts_with(&l.base),
        img.clone(),
    );
    c.check(
        "a complete card is cacheable for 300 s and counted as served",
        r.header("cache-control").as_deref() == Some("public, max-age=300")
            && r.header("content-security-policy").as_deref() == Some(CSP_AVATARS)
            && cards_metric(&m1, "served") - cards_metric(&m0, "served") == 1.0,
        format!("{:?}", r.header("cache-control")),
    );
    // The blob is the browser's to fetch: it exists where the card says.
    let blob = Http::new(None).get(&img, &[]).await;
    match blob {
        Ok(b) if b.status == 200 => {
            c.check(
                "the address the card names answers with an image when fetched directly from the PDS",
                b.header("content-type").is_some_and(|t| t.starts_with("image/")),
                b.header("content-type").unwrap_or_default(),
            );
        }
        other => c.unverified(
            "the address the card names answers with an image",
            format!("needs the network: {:?}", other.map(|b| b.status)),
        ),
    }
    // A card verifies the handle and writes the cache rows read.
    let row_page = l.get(&path_did(&w.card)).await?;
    c.check(
        "the card's verification filled the handle cache: the row now shows @handle alone, as a link whose title is the DID",
        row_page.text.contains(&format!(
            "<a class=\"who\" href=\"/did/{LIVE_DID}\" title=\"{LIVE_DID}\" data-card=\"/card/{LIVE_DID}\">{LIVE_HANDLE}</a>"
        )),
        "row",
    );
    l.set(&[("show_avatars", None)]).await?;
    let off = l.get(&format!("/card/{LIVE_DID}")).await?;
    let m2 = l.metrics_text().await?;
    c.check(
        "show_avatars = false, without restart: the card has no image and no placeholder, keeps the handle, DID and date, and is counted avatars_disabled; the CSP allows no foreign image",
        off.status == 200
            && !off.text.contains("<img")
            && !off.text.contains("pc-avatar")
            && off.text.contains(LIVE_HANDLE)
            && off.text.contains(&format!("<time datetime=\"{created}\">"))
            && off.header("content-security-policy").as_deref() == Some(CSP)
            && off.header("cache-control").as_deref() == Some("public, max-age=300")
            && cards_metric(&m2, "avatars_disabled") - cards_metric(&m1, "avatars_disabled") == 1.0,
        support::truncate(&off.text, 200),
    );
    l.set(&[("show_avatars", Some("on"))]).await?;

    // did:web: no creation date to read.
    let web = l.get(&format!("/card/{WEB_DID}")).await?;
    let m3 = l.metrics_text().await?;
    c.check(
        "a did:web account's card says the creation date is unknown and has no age (here the host does not exist: the document cannot be read, outcome pds_failed, not cached)",
        web.status == 200
            && web.text.contains(&format!("<code class=\"pc-did\">{WEB_DID}</code>"))
            && web.text.contains("<dt>DID created</dt><dd>unknown</dd>")
            && !web.text.contains("pc-age")
            && !web.text.contains("<time")
            && web.header("cache-control").as_deref() == Some("no-store")
            && cards_metric(&m3, "pds_failed") - cards_metric(&m2, "pds_failed") == 1.0,
        support::truncate(&web.text, 200),
    );

    // A PLC directory that does not answer: the card gives up at 3 s.
    let file = l.config_text()?;
    let slow = l
        .post_config(&file.replace(
            &format!("plc_url = \"{PLC}\""),
            &format!("plc_url = \"{SLOW_PLC}\""),
        ))
        .await?;
    if !slow.text.contains("Saved.") {
        return Err(format!("plc_url change refused: {}", banner(&slow.text)));
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let m4 = l.metrics_text().await?;
    let r = l.get(&format!("/card/{}", did("blk", 9))).await?;
    let m5 = l.metrics_text().await?;
    let text_ok = r.status == 200
        && r.text.contains("<dt>DID created</dt><dd>unavailable</dd>")
        && r.header("cache-control").as_deref() == Some("no-store")
        && cards_metric(&m5, "plc_timeout") - cards_metric(&m4, "plc_timeout") == 1.0;
    if r.elapsed >= Duration::from_millis(2900) {
        c.check(
            "a PLC directory that does not answer (backfill.plc_url pointed at an address nothing answers from, hot): the card comes back at the 3 s deadline with \"DID created: unavailable\"; farsight_public_ui_cards_total{outcome=plc_timeout} increments",
            text_ok && r.elapsed < Duration::from_millis(4500),
            format!("{} after {:?}", r.status, r.elapsed),
        );
    } else {
        c.check(
            "a PLC directory that cannot be reached: the card shows \"DID created: unavailable\" and counts plc_timeout",
            text_ok,
            format!("{} after {:?}", r.status, r.elapsed),
        );
        c.unverified(
            "the card waits no longer than its 3 s deadline for a silent PLC directory",
            format!(
                "this network refuses {SLOW_PLC} at once ({:?}) instead of leaving it unanswered",
                r.elapsed
            ),
        );
    }
    let back = l
        .post_config(&l.config_text()?.replace(
            &format!("plc_url = \"{SLOW_PLC}\""),
            &format!("plc_url = \"{PLC}\""),
        ))
        .await?;
    if !back.text.contains("Saved.") {
        return Err(format!("plc_url restore refused: {}", banner(&back.text)));
    }
    Ok(())
}

async fn check_limits(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("10. rate classes and the render bound");
    let before = h.metrics_text().await?;
    h.set(&[
        ("rate_limit_rps", Some("1")),
        ("rate_limit_burst", Some("3")),
    ])
    .await?;
    let one = h.fresh();
    let mut codes = Vec::new();
    let mut limited = None;
    for _ in 0..8 {
        let r = h.get_with(&one, &path_did(&w.unknown), &[]).await?;
        codes.push(r.status);
        if r.status == 429 {
            limited = Some(r);
        }
    }
    let other = h.get(&path_did(&w.unknown)).await?;
    c.check(
        "page views faster than public_ui.rate_limit_rps from one address ⇒ 429 (new values apply without restart); another address is unaffected",
        codes[..3].iter().all(|s| *s == 200)
            && codes.iter().filter(|s| **s == 429).count() >= 4
            && codes.iter().all(|s| *s == 200 || *s == 429)
            && other.status == 200,
        format!("{codes:?}"),
    );
    if let Some(r) = &limited {
        c.check(
            "429 carries Retry-After, is no-store, and has no RateLimit headers",
            r.header("retry-after")
                .is_some_and(|v| v.parse::<u64>().is_ok_and(|n| n >= 1))
                && r.header("cache-control").as_deref() == Some("no-store")
                && r.header("ratelimit").is_none()
                && r.text.contains("Too many requests"),
            r.short(),
        );
    }
    h.set(&[
        ("rate_limit_rps", Some("5")),
        ("rate_limit_burst", Some("20")),
    ])
    .await?;
    let one = h.fresh();
    let mut codes = Vec::new();
    for _ in 0..8 {
        codes.push(
            h.get_with(&one, &format!("/search?q={}", w.s), &[])
                .await?
                .status,
        );
    }
    c.check(
        "search is on the ui_lookup class: 5 pass, then 429",
        codes.iter().filter(|s| **s == 429).count() >= 2 && codes[..5].iter().all(|s| *s != 429),
        format!("{codes:?}"),
    );
    let one = h.fresh();
    let mut codes = Vec::new();
    for _ in 0..12 {
        codes.push(h.get_with(&one, "/", &[]).await?.status);
    }
    c.check(
        "home is on the public_ui class (12 quick views pass at burst 20)",
        codes.iter().all(|s| *s == 200),
        format!("{codes:?}"),
    );
    let after = h.metrics_text().await?;
    let d = |class: &str| {
        metric(&after, "farsight_rate_limited_total", &[("class", class)])
            - metric(&before, "farsight_rate_limited_total", &[("class", class)])
    };
    c.check(
        "farsight_rate_limited_total counts the classes public_ui and ui_lookup",
        d("public_ui") >= 4.0 && d("ui_lookup") >= 2.0,
        format!(
            "public_ui +{}, ui_lookup +{}",
            d("public_ui"),
            d("ui_lookup")
        ),
    );

    // Render bound: with one slot held by a page whose query is blocked,
    // the next page waits 2 s and gets 503.
    h.set(&[("query_concurrency", Some("1"))]).await?;
    let mut lock = h.pool.begin().await.map_err(|e| e.to_string())?;
    sqlx::query("LOCK TABLE blocks IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .map_err(|e| e.to_string())?;
    let (base, first_http) = (h.base.clone(), h.fresh());
    let path = path_did(&w.s);
    let first = {
        let url = format!("{base}{path}");
        tokio::spawn(async move { first_http.get(&url, &[]).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let second = h.get(&path).await?;
    lock.rollback().await.map_err(|e| e.to_string())?;
    let first = first.await.map_err(|e| e.to_string())??;
    c.check(
        "render concurrency cap (query_concurrency = 1): a page that cannot get a slot within 2 s ⇒ 503, Retry-After: 1, no-store",
        second.status == 503
            && second.header("retry-after").as_deref() == Some("1")
            && second.header("cache-control").as_deref() == Some("no-store")
            && second.elapsed >= Duration::from_millis(1900)
            && second.elapsed < Duration::from_secs(4),
        format!("{} after {:?}", second.status, second.elapsed),
    );
    c.check(
        "the page holding the slot completes once its query can run",
        first.status == 200,
        format!("{} after {:?}", first.status, first.elapsed),
    );
    // A query that outlasts rate_limit.query_timeout ⇒ 503.
    let mut lock = h.pool.begin().await.map_err(|e| e.to_string())?;
    sqlx::query("LOCK TABLE blocks IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .map_err(|e| e.to_string())?;
    let slow = h.get(&path).await?;
    lock.rollback().await.map_err(|e| e.to_string())?;
    c.check(
        "a page query that exceeds rate_limit.query_timeout ⇒ 503, Retry-After: 1",
        slow.status == 503
            && slow.header("retry-after").as_deref() == Some("1")
            && slow.elapsed >= Duration::from_secs(4),
        format!("{} after {:?}", slow.status, slow.elapsed),
    );
    let too_many = h
        .post_settings(&[("query_concurrency", Some("33"))])
        .await?;
    c.check(
        "query_concurrency above rate_limit.query_concurrency is refused",
        too_many.text.contains("between 1 and 32"),
        banner(&too_many.text),
    );
    h.set(&[("query_concurrency", Some("8"))]).await?;
    Ok(())
}

async fn check_handles(c: &mut Checks, h: &H, live: Option<&H>, w: &World) -> Result<(), String> {
    c.section("14c. handles");
    let total = |t: &str, outcome: &str| {
        metric(
            t,
            "farsight_public_ui_handle_resolutions_total",
            &[("outcome", outcome)],
        )
    };
    let all = |t: &str| {
        ["cached", "resolved", "unverified", "failed", "skipped"]
            .iter()
            .map(|o| total(t, o))
            .sum::<f64>()
    };
    // Let the process-wide budget refill.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let m0 = h.metrics_text().await?;
    h.get(&path_did(&did("unk", 77))).await?;
    let m1 = h.metrics_text().await?;
    c.check(
        "no resolution is attempted for a DID with no actors row",
        all(&m1) == all(&m0),
        format!("{} → {}", all(&m0), all(&m1)),
    );
    // A known DID whose document cannot be read (the harness's PLC URL is
    // not reachable through the safe client): failed once, then cached.
    let d = did("mem", 40);
    let p1 = h.get(&path_did(&d)).await?;
    let m2 = h.metrics_text().await?;
    let p2 = h.get(&path_did(&d)).await?;
    let m3 = h.metrics_text().await?;
    c.check(
        "a known DID whose document cannot be read renders with the DID alone; the failure is cached (failed once, then cached)",
        p1.status == 200
            && !p1.text.contains("class=\"handle\"")
            && total(&m2, "failed") - total(&m1, "failed") == 1.0
            && total(&m3, "cached") - total(&m2, "cached") == 1.0
            && total(&m3, "failed") == total(&m2, "failed")
            && p2.status == 200,
        format!("failed +{}, cached +{}", total(&m2, "failed") - total(&m1, "failed"), total(&m3, "cached") - total(&m2, "cached")),
    );
    // Rows never trigger a resolution: a page with 50 rows costs at most
    // the one resolution of its subject.
    let m4 = h.metrics_text().await?;
    h.get(&path_did(&w.s)).await?;
    let m5 = h.metrics_text().await?;
    c.check(
        "rows never trigger a resolution: a page with 50 rows resolves at most its subject",
        all(&m5) - all(&m4) <= 1.0,
        format!("+{}", all(&m5) - all(&m4)),
    );
    // Budget: more first views than the burst, at once.
    let mut ok = true;
    for i in 1..=16 {
        let r = h.get(&path_did(&did("hau", i))).await?;
        ok &= r.status == 200 && !r.text.contains("class=\"handle\"");
    }
    let m6 = h.metrics_text().await?;
    c.check(
        "budget exhaustion (process-wide, 2/s burst 10) skips the resolution and renders the DID",
        ok && total(&m6, "skipped") - total(&m5, "skipped") >= 4.0,
        format!("skipped +{}", total(&m6, "skipped") - total(&m5, "skipped")),
    );
    match live {
        None => c.unverified("a verified handle is shown beside the DID", "--skip-live"),
        Some(l) => {
            seed::actor(&l.pool, LIVE_DID).await?;
            let r = l.get(&path_did(LIVE_DID)).await?;
            let m = l.metrics_text().await?;
            if r.text
                .contains(&format!("<span class=\"handle\">{LIVE_HANDLE}</span>"))
                && r.text
                    .contains(&format!("<code class=\"did-code\">{LIVE_DID}</code>"))
            {
                c.check(
                    "a handle verified in both directions (DID document, then forward resolution) is shown beside the DID, and in og:title",
                    total(&m, "resolved") >= 1.0
                        && meta(&r.text, "og:title") == Some(format!("{LIVE_HANDLE} ({LIVE_DID})").as_str()),
                    format!("resolved {}", total(&m, "resolved")),
                );
                let stored: Vec<(String,)> = sqlx::query_as(
                    "SELECT handle FROM handle_cache WHERE did = $1 AND resolved_at > now() - interval '5 minutes'",
                )
                .bind(LIVE_DID)
                .fetch_all(&l.pool)
                .await
                .map_err(|e| e.to_string())?;
                c.check(
                    "the verified handle is stored: handle_cache holds one row for the DID, with the handle and the time of the verification",
                    stored.len() == 1 && stored[0].0 == LIVE_HANDLE,
                    format!("{stored:?}"),
                );
                let again = l.get(&path_did(LIVE_DID)).await?;
                let m2 = l.metrics_text().await?;
                c.check(
                    "the verified handle is served from the cache on the next view",
                    again.text.contains(LIVE_HANDLE)
                        && total(&m2, "cached") > total(&m, "cached")
                        && total(&m2, "resolved") == total(&m, "resolved"),
                    format!("cached {}", total(&m2, "cached")),
                );
            } else {
                c.unverified(
                    "a verified handle is shown beside the DID",
                    format!(
                        "needs the network: failed {}, unverified {}",
                        total(&m, "failed"),
                        total(&m, "unverified")
                    ),
                );
            }
        }
    }
    c.unverified(
        "an alsoKnownAs handle that does not resolve back is not shown",
        "needs a DID document under the harness's control, which the safe client (correctly) cannot reach on loopback; covered by the unit tests of claimed_handle and by code review only",
    );
    Ok(())
}

async fn check_settings_refusals(c: &mut Checks, h: &mut H) -> Result<(), String> {
    c.section("1b. Settings refuses what the loader refuses");
    let path = h.config_path.clone();
    let before = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let mut bad = Vec::new();
    for (from, to) in [
        ("reads = \"public\"", "reads = \"api_key\""),
        ("reads = \"public\"", "reads = \"disabled\""),
    ] {
        let text = before.replace(from, to);
        let r = h
            .admin
            .post_form(
                &format!("{}/admin/settings", h.base),
                &[("cookie", &h.cookie)],
                &[("csrf", h.csrf.as_str()), ("config", text.as_str())],
            )
            .await?;
        let b = banner(&r.text);
        if !b.contains("access.reads")
            || std::fs::read_to_string(&path).map_err(|e| e.to_string())? != before
        {
            bad.push(format!("{to}: {b}"));
        }
    }
    c.check(
        "changing reads while the public UI is on is rejected with a message naming the key; nothing is written",
        bad.is_empty() && h.get("/").await?.status == 200 && before.contains("public_ui = true"),
        bad.join("; "),
    );
    Ok(())
}

async fn check_off_and_raw_editor(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("1c. turning it off, and on again through the raw editor");
    h.set(&[("enabled", None)]).await?;
    let off = h.get(&path_did(&w.s)).await?;
    let unknown = h.get("/no-such-route").await?;
    c.check(
        "toggled off from admin: the public pages are 404 again, like any unknown route, and / is the redirect to /admin, without restart",
        off.status == 404 && off.text == unknown.text && h.get("/").await?.status == 303,
        off.short(),
    );
    let path = h.config_path.clone();
    let text = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let r = h
        .admin
        .post_form(
            &format!("{}/admin/settings", h.base),
            &[("cookie", &h.cookie)],
            &[
                ("csrf", h.csrf.as_str()),
                (
                    "config",
                    text.replace("public_ui = false", "public_ui = true")
                        .as_str(),
                ),
            ],
        )
        .await?;
    let token = confirm_token(&r.text);
    c.check(
        "the raw config editor cannot turn the public UI on without the same confirmation",
        token.is_some() && h.get("/").await?.status == 303,
        support::truncate(&r.text, 120),
    );
    if let Some(t) = token {
        let done = h.confirm(&t).await?;
        c.check(
            "…and the confirmation commits the edited file",
            done.text.contains("Saved.") && h.get("/").await?.status == 200,
            banner(&done.text),
        );
        h.form.insert("enabled", "on".to_owned());
    }
    Ok(())
}

async fn check_metrics(c: &mut Checks, h: &H) -> Result<(), String> {
    c.section("14d. metrics");
    let m = h.metrics_text().await?;
    let req = |page: &str, status: &str| {
        metric(
            &m,
            "farsight_public_ui_requests_total",
            &[("page", page), ("status", status)],
        )
    };
    let pages = ["home", "search", "did", "list", "card", "robots"];
    let missing: Vec<&str> = pages
        .iter()
        .copied()
        .filter(|p| req(p, "2xx") + req(p, "3xx") + req(p, "4xx") == 0.0)
        .collect();
    let seen: BTreeSet<&str> = m
        .lines()
        .filter(|l| l.starts_with("farsight_public_ui_requests_total{"))
        .filter_map(|l| l.split("page=\"").nth(1)?.split('"').next())
        .collect();
    c.check(
        "farsight_public_ui_requests_total has exactly the page labels home, search, did, list, card, robots — one per page, nothing for an unknown path — with bucketed statuses",
        missing.is_empty()
            && seen == pages.iter().copied().collect::<BTreeSet<_>>()
            && req("search", "3xx") > 0.0
            && req("did", "4xx") > 0.0
            && req("did", "5xx") > 0.0
            && req("card", "2xx") > 0.0
            && req("card", "4xx") > 0.0,
        format!("missing {missing:?}; seen {seen:?}"),
    );
    let labels: BTreeSet<&str> = m
        .lines()
        .filter(|l| l.starts_with("farsight_public_ui_requests_total{"))
        .filter_map(|l| l.split("status=\"").nth(1)?.split('"').next())
        .collect();
    c.check(
        "status labels are the four classes only; page labels never carry a path",
        labels
            .iter()
            .all(|s| ["2xx", "3xx", "4xx", "5xx"].contains(s))
            && !m.contains("page=\"/"),
        format!("{labels:?}"),
    );
    let outcomes: BTreeSet<&str> = m
        .lines()
        .filter(|l| l.starts_with("farsight_public_ui_cards_total{"))
        .filter_map(|l| l.split("outcome=\"").nth(1)?.split('"').next())
        .collect();
    c.check(
        "farsight_public_ui_cards_total has the outcomes served, rate_limited, plc_timeout, pds_failed, avatars_disabled, and no other",
        outcomes
            == ["served", "rate_limited", "plc_timeout", "pds_failed", "avatars_disabled"]
                .into_iter()
                .collect::<BTreeSet<_>>()
            && cards_metric(&m, "rate_limited") > 0.0
            && cards_metric(&m, "plc_timeout") > 0.0,
        format!("{outcomes:?}"),
    );
    c.check(
        "farsight_public_ui_duration_seconds, handle_resolutions and withheld series exist",
        m.contains("farsight_public_ui_duration_seconds")
            && m.contains("farsight_public_ui_handle_resolutions_total{outcome=\"skipped\"}")
            && m.contains("farsight_public_ui_withheld_total{reason=\"operator_excluded\"}")
            && m.contains("farsight_block_history_written_total"),
        "present",
    );
    Ok(())
}

// ------------------------------------------- 17. rules every page keeps

fn check_page_rules(c: &mut Checks, h: &H) {
    c.section("17. rules every page keeps");
    let pages = h.pages.lock().unwrap_or_else(|e| e.into_inner());
    let public: Vec<&(String, String)> = pages.iter().filter(|(p, _)| is_public(p)).collect();
    let documents: Vec<&&(String, String)> =
        public.iter().filter(|(_, t)| t.contains("<html")).collect();
    let mut inline = Vec::new();
    for (path, html) in &public {
        let scripts = between(html, "<script", ">");
        // ` src="/static/<name>.js[?v=<fingerprint>]"`, alone or with `defer`.
        let external = scripts.iter().all(|s| {
            let Some(rest) = s.strip_prefix(" src=\"/static/") else {
                return false;
            };
            let Some((url, tail)) = rest.split_once('"') else {
                return false;
            };
            let (file, query) = url.split_once('?').unwrap_or((url, ""));
            file.ends_with(".js")
                && !file.contains(['/', ':'])
                && (query.is_empty()
                    || query
                        .strip_prefix("v=")
                        .is_some_and(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_hexdigit())))
                && (tail.is_empty() || tail == " defer")
        });
        let handlers = [
            " onclick=",
            " onload=",
            " onerror=",
            " onmouseover=",
            " onfocus=",
            "javascript:",
        ]
        .iter()
        .any(|x| html.contains(x));
        if !external || handlers || html.contains("<style") || html.contains(" style=\"") {
            inline.push(path.clone());
        }
    }
    c.check(
        "no inline script, handler or style on any public response: every <script> loads /static/*.js",
        inline.is_empty() && public.len() > 100,
        format!("{} responses scanned; offenders: {inline:?}", public.len()),
    );
    let mut bad = Vec::new();
    for (path, html) in &documents {
        let nav = between(html, "<nav class=\"public-nav\"", "</nav>");
        // Home has its own search form and guide: its bar is the brand
        // and the theme toggle.
        if html.contains("<details class=\"guide home-guide\">") {
            let ok = nav.len() == 1
                && nav[0].contains(
                    "<a class=\"brand\" href=\"/\" aria-label=\"Farsight\" title=\"Farsight\">",
                )
                && !nav[0].contains("<form")
                && !nav[0].contains("nav-guide")
                && nav[0].contains("class=\"theme-toggle\"");
            if !ok {
                bad.push(path.clone());
            }
            continue;
        }
        let ok = nav.len() == 1
            && nav[0].contains(
                "<a class=\"brand\" href=\"/\" aria-label=\"Farsight\" title=\"Farsight\">",
            )
            && !nav[0].contains("<span>Farsight</span>")
            && nav[0].contains(
                "<form class=\"search\" action=\"/search\" method=\"get\" role=\"search\">",
            )
            && nav[0].contains("<input type=\"search\" name=\"q\"")
            && !nav[0].contains(" value=\"")
            && nav[0].contains("<details class=\"guide nav-guide\">")
            && nav[0].contains("<dt>Blocked By</dt>")
            && nav[0].contains("class=\"theme-toggle\"");
        if !ok {
            bad.push(path.clone());
        }
    }
    c.check(
        "the bar is on every public page — data pages, the withheld notice, error pages: brand link to /, a GET search form to /search with one empty field q, the guide, the theme toggle; on home, which has its own search form and guide, the brand and the toggle alone",
        bad.is_empty() && documents.len() > 100,
        format!("{} pages scanned; offenders: {bad:?}", documents.len()),
    );
    let untitled: Vec<&String> = documents
        .iter()
        .filter(|(_, html)| {
            !html.contains("<title>Farsight</title>")
                || !html.contains(
                    "<link rel=\"icon\" type=\"image/svg+xml\" href=\"/static/favicon.svg?v=",
                )
        })
        .map(|(path, _)| path)
        .collect();
    c.check(
        "every public page's tab is titled \"Farsight\" and nothing else — not the account, the list or the hostname — and names the same icon",
        untitled.is_empty(),
        format!("offenders: {untitled:?}"),
    );
    let mut bad = Vec::new();
    for (path, html) in &public {
        let out: Vec<String> = hrefs(html)
            .into_iter()
            .filter(|x| !(own_href(x) || x.starts_with("https://viewer.example/")))
            .collect();
        let lower = html.to_ascii_lowercase();
        let names = [
            "/enter",
            "/settings",
            "/lookup",
            "/admin",
            "/ops",
            "log in",
            "dashboard",
        ]
        .iter()
        .any(|x| lower.contains(x));
        if !out.is_empty() || names || html.contains("action=\"/enter") {
            bad.push(format!("{path}: {out:?}"));
        }
    }
    c.check(
        "no link out of the public UI on any public response (the configured record viewer aside): no login link, no admin route named",
        bad.is_empty(),
        support::truncate(&bad.join("; "), 400),
    );
    let mut bad = Vec::new();
    let mut times = 0;
    for (path, html) in &public {
        for t in between(html, "<time ", "</time>") {
            times += 1;
            // The instant, then whatever other attributes the tag carries.
            let ok = t
                .strip_prefix("datetime=\"")
                .and_then(|x| x.split_once('"'))
                .and_then(|(iso, rest)| rest.split_once('>').map(|(_, text)| (iso, text)))
                .is_some_and(|(iso, text)| {
                    // A table row's time (data-abs) leaves the zone to
                    // the page's "All times are in UTC" line.
                    let bare = format!("{} {}", &iso[..10], &iso[11..19]);
                    is_utc_instant(iso)
                        && (text == format!("{bare} UTC")
                            || (t.contains(" data-abs") && text == bare))
                });
            if !ok {
                bad.push(format!("{path}: {t}"));
            }
        }
    }
    c.check(
        "every <time> on every public response has datetime = YYYY-MM-DDTHH:MM:SSZ and the same instant as UTC text (what a visitor without script reads)",
        bad.is_empty() && times > 1_000,
        format!("{times} times; offenders: {}", support::truncate(&bad.join("; "), 200)),
    );
    let mut bad = Vec::new();
    for (path, html) in &public {
        let found = coverage_words_in(html);
        if !found.is_empty() {
            bad.push(format!("{path}: {found:?}"));
        }
    }
    c.check(
        "no per-section coverage banner, coverage section or removed-records link on any public response",
        bad.is_empty(),
        support::truncate(&bad.join("; "), 300),
    );
}

// ------------------------------------------------------- 18. the browser

/// Runs the browser probes in the Playwright container against `base`.
fn check_browser(
    c: &mut Checks,
    base: &str,
    account: &str,
    operator_default: &str,
    live: Option<&str>,
) -> Result<(), String> {
    c.section("18. in a browser (headless Chromium)");
    let dir = std::env::temp_dir().join("farsight-stage6-browser");
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    std::fs::write(dir.join("probes.mjs"), BROWSER_SCRIPT).map_err(|e| e.to_string())?;
    let mut script = format!(
        "cd /work && ([ -d node_modules/playwright ] || npm install --no-save --no-audit --no-fund playwright@1.48.0 >npm.log 2>&1) && node probes.mjs '{base}' '{account}' '{operator_default}'"
    );
    if let Some(p) = live {
        script.push_str(&format!(" '{p}' '{LIVE_DID}' '{LIVE_HANDLE}'"));
    }
    let out = std::process::Command::new("docker")
        .args(["run", "--rm", "--network", "host", "-v"])
        .arg(format!("{}:/work", dir.display()))
        .args([BROWSER_IMAGE, "sh", "-c", &script])
        .output()
        .map_err(|e| format!("docker: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut n = 0;
    for line in stdout.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let (Some(what), Some(ok)) = (v["what"].as_str(), v["ok"].as_bool()) else {
            continue;
        };
        n += 1;
        c.check(what, ok, v["detail"].as_str().unwrap_or(""));
    }
    if n == 0 || !out.status.success() {
        c.check(
            "the browser probes ran to completion",
            false,
            format!(
                "exit {:?}: {} {}",
                out.status.code(),
                support::truncate(&stdout, 200),
                support::truncate(&String::from_utf8_lossy(&out.stderr), 300)
            ),
        );
    }
    Ok(())
}

fn check_no_relative_time(c: &mut Checks, h: &H) {
    let pages = h.pages.lock().unwrap_or_else(|e| e.into_inner());
    let mut bad = Vec::new();
    for (path, html) in pages.iter().filter(|(p, _)| is_public(p)) {
        let lower = html.to_ascii_lowercase();
        if [
            " ago",
            "seconds ago",
            "minutes ago",
            "just now",
            "yesterday",
        ]
        .iter()
        .any(|w| lower.contains(w))
        {
            bad.push(path.clone());
        }
    }
    c.check(
        "no relative time in any server-rendered page (a cached copy would keep saying it)",
        bad.is_empty() && pages.len() > 100,
        format!("{} pages scanned; offenders: {bad:?}", pages.len()),
    );
}

async fn start(
    pg: &Pg,
    db: &str,
    name: &str,
    admin: &str,
    plc: &str,
) -> Result<(Server, H), String> {
    let dsn = pg.url(db);
    let (port, mport) = (free_port()?, free_port()?);
    let cfg = config_toml(
        &dsn,
        admin,
        &format!("127.0.0.1:{mport}"),
        plc,
        "public_ui = false",
        "",
    );
    let server = Server::start(name, Some(&cfg), &format!("127.0.0.1:{port}"), &[])?;
    let http = Http::new(Some("127.0.0.250".parse().expect("ip")));
    server.wait_live(&http, Duration::from_secs(90)).await?;
    let mut form: BTreeMap<&'static str, String> = BTreeMap::new();
    for (k, v) in [
        ("show_opengraph_image", "on"),
        ("show_avatars", "on"),
        ("record_viewer_url", ""),
        ("card_rps", "4"),
        ("card_burst", "8"),
        ("dark_mode_default", "system"),
        ("rate_limit_rps", "5"),
        ("rate_limit_burst", "20"),
        ("query_concurrency", "8"),
        ("handle_cache_ttl", "1h"),
        ("instance_description", ""),
        ("contact", ""),
        ("excluded_dids", ""),
    ] {
        form.insert(k, v.to_owned());
    }
    let h = H {
        base: server.base.clone(),
        metrics: format!("http://127.0.0.1:{mport}"),
        pool: pg.pool(db, 6).await?,
        admin: http,
        cookie: String::new(),
        csrf: String::new(),
        form,
        config_path: server.config_path().display().to_string(),
        next_ip: AtomicU32::new(match name {
            "live" => 30_000,
            _ => 0,
        }),
        pages: Mutex::new(Vec::new()),
    };
    Ok((server, h))
}

async fn phase_ui(
    c: &mut Checks,
    pg: &Pg,
    skip_live: bool,
    browser: bool,
    hold: bool,
) -> Result<(), String> {
    pg.create_db("stage6_ui").await?;
    let admin = farsight_api::auth::generate(farsight_api::auth::ADMIN_PREFIX);
    // The harness's PLC URL is one the safe client refuses (loopback), so
    // synthetic DIDs never reach the real directory.
    let (_server, mut h) = start(pg, "stage6_ui", "ui", &admin, "https://127.0.0.1:9").await?;
    let paused = Arc::new(AtomicBool::new(false));
    let keeper = spawn_firehose_keeper(h.pool.clone(), paused.clone());
    tokio::time::sleep(Duration::from_secs(3)).await;
    let w = seed_world(&h).await?;
    h.refresh().await?;
    h.login().await?;

    check_gates_off(c, &h, &w).await?;
    check_validation(c, &pg.url("stage6_ui"))?;
    check_enable_flow(c, &mut h, &w).await?;
    check_first_save(c, &h).await?;
    check_settings_refusals(c, &mut h).await?;
    check_login_route(c, &h).await?;
    check_routes(c, &h, &w).await?;
    check_sections(c, &h, &w).await?;
    check_admin_history(c, &h, &w).await?;
    check_coverage(c, &mut h, &w, &paused).await?;
    check_cache_and_headers(c, &h, &w).await?;
    check_independence(c, &h, &w).await?;
    check_opengraph(c, &mut h, &w).await?;
    check_record_links(c, &mut h, &w).await?;
    check_cards(c, &mut h, &w).await?;

    // A second server on the same database with the real PLC directory,
    // for the checks that need the network.
    let mut live = if skip_live {
        None
    } else {
        let (server, mut l) = start(pg, "stage6_ui", "live", &admin, PLC).await?;
        l.login().await?;
        l.set(&[("enabled", Some("on"))]).await?;
        Some((server, l))
    };
    check_search(c, &h, live.as_ref().map(|(_, l)| l), &w).await?;
    check_handles(c, &h, live.as_ref().map(|(_, l)| l), &w).await?;
    match live.as_mut() {
        Some((_, l)) => check_cards_live(c, l, &w).await?,
        None => c.unverified(
            "a real account's card (creation date, handle, avatar), and the 3 s PLC deadline",
            "--skip-live",
        ),
    }

    check_withheld(c, &mut h, &w).await?;
    check_keys(c, &mut h, &w).await?;
    check_limits(c, &mut h, &w).await?;
    check_off_and_raw_editor(c, &mut h, &w).await?;
    check_metrics(c, &h).await?;
    check_page_rules(c, &h);
    c.section("17b. rendered times");
    check_no_relative_time(c, &h);

    if browser {
        // The operator's default is dark for this run, so that a first
        // visit can be told apart from the browser's own preference.
        let account = path_did(&w.s);
        match live.as_mut() {
            Some((_, l)) => {
                l.set(&[("dark_mode_default", Some("dark"))]).await?;
                check_browser(c, &l.base, &account, "dark", Some(&path_did(&w.card)))?;
                l.set(&[("dark_mode_default", Some("system"))]).await?;
            }
            None => {
                h.set(&[("dark_mode_default", Some("dark"))]).await?;
                check_browser(c, &h.base, &account, "dark", None)?;
                h.set(&[("dark_mode_default", Some("system"))]).await?;
            }
        }
    } else {
        c.section("18. in a browser (headless Chromium)");
        c.unverified(
            "theme toggle and its persistence, times in the visitor's timezone, the sticky bar, cards on hover and focus, no cards on touch",
            "run with --browser (needs the Playwright container)",
        );
    }
    if hold {
        h.set(&[("show_outgoing_blocks", Some("on"))]).await?;
        println!(
            "== holding: {} (admin DID {ADMIN_DID}; sessions are made by the harness)",
            h.base
        );
        println!("   account {}/did/{}", h.base, w.s);
        println!("   list    {}{}", h.base, w.list);
        println!("   withheld {}/did/{}", h.base, w.e);
        println!("   history {}/admin/did/{}/history", h.base, w.s);
        if let Some((_, l)) = &live {
            println!("   live cards {}/did/{}", l.base, w.card);
        }
        let _ = tokio::signal::ctrl_c().await;
    }
    drop(live);
    keeper.abort();
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let flag = |f: &str| std::env::args().any(|a| a == f);
    let (keep, skip_live, browser, hold) = (
        flag("--keep"),
        flag("--skip-live"),
        flag("--browser"),
        flag("--hold"),
    );
    println!("== farsight stage-6 harness: public UI");
    let pg = match Pg::start(keep) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("postgres: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    let mut c = Checks::default();
    let result = async {
        pg.wait_ready(Duration::from_secs(60)).await?;
        phase_ui(&mut c, &pg, skip_live, browser, hold).await
    }
    .await;
    if let Err(e) = &result {
        c.check("harness ran to completion", false, e.clone());
    }
    pg.stop();
    let (p, f, u) = c.counts();
    for i in c
        .items
        .iter()
        .filter(|i| i.verdict != support::Verdict::Pass)
    {
        println!(
            "  {} {} — {}",
            support::tag(i.verdict),
            i.what,
            support::truncate(&i.detail, 300)
        );
    }
    println!(
        "RESULT: {} ({p} passed, {f} failed, {u} unverified)",
        if f == 0 { "PASS" } else { "FAIL" }
    );
    if f == 0 {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(1)
    }
}
