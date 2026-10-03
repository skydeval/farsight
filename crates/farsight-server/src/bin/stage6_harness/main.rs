//! `farsight-stage6-harness`: Phase B Mode A — the public UI as updated
//! by stage 6 (sticky bar, `/enter`, admin-only history, one "Last
//! updated" line, profile cards, record links, local times, the theme
//! toggle). Every assertion reads real responses from a real `farsight`
//! process over HTTP and real stored rows; expectations that depend on
//! data are derived from SQL against the same database. The parts only a
//! browser can show are probed in headless Chromium (`--browser`).
//!
//! Sections:
//!
//! - 1: toggle gates, the retired `show_history` key, config validation,
//!   the enable-confirmation flow;
//! - 2: routes: `/enter`, the withdrawn paths, the sections against
//!   stored rows, the "On lists" table;
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
const RETIRED_WARNING: &str = "no longer has any effect; history pages are admin-only";
const VIEWER: &str = "https://viewer.example/at/{authority}/{collection}/{rkey}";
/// What no public page prints any more.
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
ui = "public_read"
admin_did = "{ADMIN_DID}"
{access}

[public_ui]
handle_warming_enabled = false
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
    log_path: String,
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
                &format!("{}/settings", self.base),
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
                &format!("{}/settings", self.base),
                &[("cookie", &self.cookie)],
            )
            .await?;
        self.csrf = csrf_of(&page.text).ok_or("no csrf on /settings")?;
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
                &format!("{}/settings/public-ui", self.base),
                &[("cookie", &self.cookie)],
                &self.form_pairs(),
            )
            .await
    }

    async fn confirm(&self, token: &str) -> Result<Resp, String> {
        self.admin
            .post_form(
                &format!("{}/settings/public-ui/confirm", self.base),
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

/// The inside of `<section id="…">`.
fn section<'a>(html: &'a str, id: &str) -> Option<&'a str> {
    let open = format!("<section id=\"{id}\">");
    let a = html.find(&open)? + open.len();
    let b = html[a..].find("</section>")? + a;
    Some(&html[a..b])
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
    let k = "href=\"/public/did/";
    let mut out = Vec::new();
    let mut rest = sec;
    while let Some(i) = rest.find(k) {
        let from = i + k.len();
        if let Some(j) = rest[from..].find('"') {
            let d = &rest[from..from + j];
            // Row links are bare DIDs; the pager's link carries a query.
            if !d.contains(['?', '/']) {
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
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The DIDs of an admin history section's rows: the accounts its lookup
/// links point at.
fn admin_row_dids(sec: &str) -> Vec<String> {
    let k = "href=\"/lookup/did?q=";
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
    let k = "<p class=\"updated\">Last updated <time datetime=\"";
    let a = html.find(k)? + k.len();
    Some(html[a..][..html[a..].find('"')?].to_owned())
}

/// The "Next" link of a section, if it has one.
fn next_of(sec: &str) -> Option<String> {
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
    format!("/public/did/{d}")
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
}

const LIST: &str = "modlist1";
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
        list: format!("/public/list/{}/{LIST}", did("own", 1)),
        partial: did("sub", 3),
        card: did("sub", 4),
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
        "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev)
         SELECT a.id, '3lb' || lpad(g::text, 9, '0'), {s}, now() - interval '3 days', 1
         FROM generate_series(1, 60) g JOIN actors a ON a.did = {}",
        did_sql("blk", "g")
    ))
    .await?;
    for id in hid.iter().chain([&e]) {
        seed::block(p, *id, "3lbx", s).await?;
    }
    // Blocks by S: three active targets, one hidden, and E.
    h.sql(&format!(
        "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev)
         SELECT {s}, '3lo' || lpad(g::text, 9, '0'), a.id, now() - interval '3 days', 1
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
        "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev)
         SELECT {o1}, '3lm' || lpad(g::text, 9, '0'), {l}, a.id, now() - interval '3 days', 1
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
        "INSERT INTO list_blocks (author_id, rkey, list_id, counted, witnessed_at, created_at, rev)
         SELECT a.id, '3lk' || g, {l}, true, now() - interval '3 days', now() - interval '3 days', 1
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
    // Stored before the bounds existed; its blocker (blk 1) blocks S now.
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
        "/public".to_owned(),
        "/public/about".to_owned(),
        "/public/search?q=x.example".to_owned(),
        path_did(&w.s),
        format!("{}/history", path_did(&w.s)),
        w.list.clone(),
        format!("{}/history", w.list),
        format!("/public/card/{}", w.s),
        "/public/static/public.css".to_owned(),
        "/public/static/public.js".to_owned(),
        "/public/static/og-default.png".to_owned(),
        "/public/anything/else".to_owned(),
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
        "public_ui = false ⇒ every /public/* route, the card route included, answers 404, byte-identical to an unknown route",
        same && unknown.status == 404,
        if detail.is_empty() {
            unknown.short()
        } else {
            detail.join("; ")
        },
    );
    let robots = h.get("/robots.txt").await?;
    c.check(
        "robots.txt with the public UI off: Disallow: /, max-age=300",
        robots.status == 200
            && robots.text == "User-agent: *\nDisallow: /\n"
            && robots.header("cache-control").as_deref() == Some("public, max-age=300"),
        robots.short(),
    );
    // The loader refuses the public UI over gated reads, whatever the
    // admin UI mode (the public UI no longer depends on it).
    let mut refused = Vec::new();
    for (reads, ui) in [
        ("api_key", "public_read"),
        ("disabled", "public_read"),
        ("api_key", "auth_all"),
        ("disabled", "disabled"),
    ] {
        let text = minimal_config(&format!(
            "[access]\nreads = \"{reads}\"\nui = \"{ui}\"\npublic_ui = true\n"
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
    for ui in ["public_read", "auth_all", "disabled"] {
        let text = minimal_config(&format!(
            "[access]\nreads = \"public\"\nui = \"{ui}\"\npublic_ui = true\n"
        ));
        if let Err(e) = farsight_core::config::load_from_parts(Some(&text), &[]) {
            refused.push(format!("public/{ui} refused: {e}"));
        }
    }
    c.check(
        "config load refuses public_ui = true without reads = public, naming the key, and accepts it with every ui mode",
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

/// The retired key, first half: a config that still has it loads, with
/// one warning, and the value does nothing.
async fn check_retired_key_loads(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("1b. the retired show_history key");
    let file = h.config_text()?;
    let log = std::fs::read_to_string(&h.log_path).unwrap_or_default();
    let warnings = log.matches(RETIRED_WARNING).count();
    c.check(
        "a config file with show_history = true loads: the server is up, and logged the retirement warning once",
        file.contains("show_history = true") && warnings == 1 && h.get("/livez").await?.status == 200,
        format!("{warnings} warning lines"),
    );
    let env = vec![(
        "FARSIGHT__PUBLIC_UI__SHOW_HISTORY".to_owned(),
        "true".to_owned(),
    )];
    let from_env = farsight_core::config::load_from_parts(Some(&minimal_config("")), &env)
        .map_err(|e| e.to_string())?;
    c.check(
        "the environment form of the key is accepted the same way, and locks nothing",
        from_env
            .warnings
            .iter()
            .any(|x| x.contains(RETIRED_WARNING))
            && from_env.env_keys.is_empty(),
        format!("{:?}", from_env.warnings),
    );
    let _ = w;
    Ok(())
}

/// Second half, once the public UI is on and the Public UI settings have
/// been saved once: the value did nothing, and the key is gone from the
/// file.
async fn check_retired_key_removed(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("1b. the retired show_history key (after a save)");
    let hist = h.get(&format!("{}/history", path_did(&w.s))).await?;
    c.check(
        "show_history = true does nothing: the public history path is 404 with the public UI on",
        hist.status == 404,
        hist.short(),
    );
    let file = h.config_text()?;
    c.check(
        "the first save of the Public UI settings removed the key from config.toml and wrote the new keys",
        !file.contains("show_history")
            && file.contains("record_viewer_url = \"\"")
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
        "the rewritten file loads without the warning",
        reloaded.warnings.is_empty(),
        format!("{:?}", reloaded.warnings),
    );
    Ok(())
}

async fn check_enable_flow(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("1c. enable-confirmation flow and the Settings controls");
    let settings = h.admin_get("/settings").await?;
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
        "Settings has a control for the toggle and every [public_ui] key — the four new ones included — and none for show_history",
        missing.is_empty()
            && !settings.text.contains("name=\"show_history\"")
            && settings.text.contains("href=\"/public\" target=\"_blank\"")
            && settings.text.contains("placeholder=\"https://viewer.example/at/{authority}/{collection}/{rkey}\"")
            && settings.text.contains("fetches it from the account's own server"),
        format!("missing: {missing:?}"),
    );
    // A direct second POST, with no first: nothing happens.
    let direct = h.confirm("never-issued").await?;
    c.check(
        "a confirm POST without a pending confirmation is refused (400) and changes nothing",
        direct.status == 400 && h.get("/public").await?.status == 404,
        banner(&direct.text),
    );
    let first = h.post_settings(&[("enabled", Some("on"))]).await?;
    let token = confirm_token(&first.text);
    let lists_all = [
        HOSTNAME,
        "mailto:ops@",
        "(shown on the home page)",
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
        "…and writes nothing: /public is still 404 and the config still says off",
        h.get("/public").await?.status == 404 && !h.config_text()?.contains("public_ui = true"),
        "not written",
    );
    let token = token.ok_or("no confirmation token")?;
    let bogus = h.confirm(&format!("{token}x")).await?;
    c.check(
        "a confirm POST with a wrong token is refused and the public UI stays off",
        bogus.status == 400 && h.get("/public").await?.status == 404,
        banner(&bogus.text),
    );
    let second = h.confirm(&token).await?;
    let home = h.get("/public").await?;
    c.check(
        "second POST with the form token commits: the settings are saved and /public serves, with no restart",
        second.status == 200
            && second.text.contains("Saved.")
            && second.text.contains("access.public_ui")
            && home.status == 200,
        format!("{}; /public {}", banner(&second.text), home.status),
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
    let unknown = h.get("/no-such-route").await?;
    let get = h.get("/login").await?;
    let post = h
        .fresh()
        .post_form(&format!("{}/login", h.base), &[], &[("password", "x")])
        .await?;
    let with_cookie = h.admin_get("/login").await?;
    let same = |r: &Resp| {
        r.status == 404
            && r.text == unknown.text
            && r.header("cache-control") == unknown.header("cache-control")
            && r.header("location").is_none()
            && r.header("set-cookie").is_none()
    };
    c.check(
        "/login is no longer a route with the public UI on: GET and POST get the 404 any unknown path gets, byte for byte; no redirect, no cookie",
        same(&get) && same(&post) && same(&with_cookie),
        format!("GET {} POST {}", get.status, post.status),
    );
    let enter = h.get("/enter").await?;
    c.check(
        "/enter serves the admin sign-in form, posting to /enter, with no password field, never cached",
        enter.status == 200
            && enter
                .text
                .contains("<form class=\"stack card\" method=\"post\" action=\"/enter\">")
            && !enter.text.contains("type=\"password\"")
            && enter.header("cache-control").as_deref() == Some("no-store, private"),
        enter.short(),
    );
    // A password posted to /enter signs nobody in (the flow itself is the
    // stage-7 harness's subject).
    let posted = h
        .fresh()
        .post_form(&format!("{}/enter", h.base), &[], &[("password", "nope")])
        .await?;
    c.check(
        "POST /enter takes no password: whatever is posted, no session cookie comes back",
        set_cookie(&posted, "farsight_admin").is_none() && posted.status != 200,
        format!("{}", posted.status),
    );
    let gated = h.get("/settings").await?;
    let dash = h.get("/").await?;
    c.check(
        "an admin page without a session redirects to /enter, and the admin header's link points there",
        gated.status == 303
            && gated.header("location").as_deref() == Some("/enter")
            && dash.text.contains("<a href=\"/enter\">Sign in</a>")
            && !dash.text.contains("/login"),
        format!("{} → {:?}", gated.status, gated.header("location")),
    );
    Ok(())
}

async fn check_routes(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("2. routes");
    let mut bad = Vec::new();
    for p in [
        "/public".to_owned(),
        path_did(&w.s),
        w.list.clone(),
        format!("/public/card/{}", w.s),
        "/public/static/public.css".to_owned(),
        "/public/static/public.js".to_owned(),
        "/public/static/htmx.min.js".to_owned(),
        "/public/static/og-default.png".to_owned(),
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
        .get(&format!("{}/public/static/og-default.png", h.base), &[])
        .await?;
    c.check(
        "the preview image is a PNG served as image/png",
        png.header("content-type").as_deref() == Some("image/png") && png.text.contains("PNG"),
        png.header("content-type").unwrap_or_default(),
    );
    // The withdrawn paths.
    let other = h.get("/public/nothing/here").await?;
    let mut bad = Vec::new();
    for p in [
        "/public/about".to_owned(),
        format!("{}/history", path_did(&w.s)),
        format!("{}/history?hb=AAAA", path_did(&w.s)),
        format!("{}/history", w.list),
        format!("{}/history", path_did(&w.unknown)),
    ] {
        for r in [h.get(&p).await?, h.admin_get(&p).await?] {
            if r.status != 404
                || r.text != other.text
                || r.header("cache-control").as_deref() != Some("no-store")
                || r.header("location").is_some()
                || r.header("x-robots-tag").as_deref() != Some("noindex, nofollow")
            {
                bad.push(format!("{p}: {}", support::truncate(&r.short(), 80)));
            }
        }
    }
    c.check(
        "/public/about and both public history paths answer the 404 page any unknown /public/… path gets — no-store, no redirect, with or without an admin session",
        bad.is_empty() && other.status == 404 && other.text.contains("There is no page at this address."),
        bad.join("; "),
    );
    let mut bad = Vec::new();
    let long = "a".repeat(600);
    for p in [
        "/public/did/not-a-did".to_owned(),
        "/public/did/alice.example".to_owned(),
        "/public/did/did:plc:short".to_owned(),
        format!("/public/did/did:web:{long}.example"),
        format!("/public/list/{}/bad%20key", w.o1),
        format!("/public/list/alice.example/{LIST}"),
        format!("{}?bc=!!!", path_did(&w.s)),
        format!("{}?nc=bm9wZQ", path_did(&w.s)),
        format!("{}?lc=!!!", w.list),
        format!("/public/search?q={long}"),
        "/public/card/alice.example".to_owned(),
        "/public/card/did:plc:short".to_owned(),
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
        "malformed DID, list key, cursor and over-long input ⇒ 400, no-store (pages and the card route)",
        bad.is_empty(),
        bad.join("; "),
    );
    let cur = h.get(&format!("{}?bc=!!!", path_did(&w.s))).await?;
    c.check(
        "a cursor that no longer parses links to the first page",
        cur.text.contains(&format!("href=\"{}\"", path_did(&w.s))),
        support::truncate(&cur.text, 120),
    );
    let u = h.get(&path_did(&w.unknown)).await?;
    let actors_before = h.n("SELECT count(*) FROM actors").await?;
    c.check(
        "an unknown DID renders its sections empty with the one empty-section string and no count (never 'not found'), and is not interned",
        u.status == 200
            && section(&u.text, "blockers").is_some_and(|s| s.contains(&format!("<p class=\"empty\">{EMPTY}</p>")) && !s.contains("class=\"count\""))
            && section(&u.text, "lists").is_some_and(|s| s.contains(EMPTY))
            && !u.text.contains("None found")
            && !u.text.contains("No blockers")
            && h.n(&format!("SELECT count(*) FROM actors WHERE did = '{}'", w.unknown)).await? == 0
            && actors_before == h.n("SELECT count(*) FROM actors").await?,
        u.short(),
    );
    let ul = h.get(&format!("/public/list/{}/nosuchlist", w.o1)).await?;
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
    let shown = "a.status NOT IN (1, 2, 3, 4)";
    let page = h.get(&path_did(&w.s)).await?;
    let navs = between(
        &page.text,
        "<nav class=\"sections\" aria-label=\"Sections\">",
        "</nav>",
    );
    c.check(
        "account page section nav: exactly two links, Blocked by and On lists (outgoing is off)",
        navs.len() == 1
            && navs[0] == "<a href=\"#blockers\">Blocked by</a><a href=\"#lists\">On lists</a>",
        navs.join(" || "),
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
        "Blocked by: paging through the section returns every blocker with an active account exactly once",
        ok && pages == 2 && want.len() == 61,
        format!("{d}; {pages} pages"),
    );
    let sec = section(&page.text, "blockers").unwrap_or("");
    c.check(
        "the bounded count equals the stored count with the page's filters",
        sec.contains(&format!("<span class=\"count\">{}</span>", want.len())),
        support::truncate(sec, 120),
    );
    let first = row_dids(sec).first().cloned().unwrap_or_default();
    c.check(
        "a row names the account as a link to its page whose title is the DID and which carries its card address; with no handle cached it shows the DID",
        sec.contains(&format!(
            "<span class=\"who-wrap\"><a class=\"who\" href=\"/public/did/{first}\" title=\"{first}\" data-card=\"/public/card/{first}\"><code>{first}</code></a></span>"
        )),
        first,
    );
    let lists = section(&page.text, "lists").unwrap_or("");
    let heads: Vec<&str> = between(lists, "<th>", "</th>");
    c.check(
        "On lists: the columns are List, Owner, Blocked by, Added — no Purpose column",
        heads
            == [
                "List",
                "Owner",
                "Blocked by",
                "Added (as stated by the list's owner)",
            ]
            && !lists.contains("Purpose")
            && !lists.contains("moderation list</td>"),
        format!("{heads:?}"),
    );
    let stored_count = h
        .n(&format!(
            "SELECT l.listblock_count::bigint FROM lists l JOIN actors o ON o.id = l.owner_id WHERE o.did = '{}' AND l.rkey = '{LIST}'",
            w.o1
        ))
        .await?;
    c.check(
        "On lists: \"Blocked by\" is the listblock counter as a bare number, titled as a count of records; the list links to its page; a hidden owner's list is left out",
        lists.contains(&format!(
            "<td class=\"num\" title=\"listblock records\">{stored_count}</td>"
        )) && !lists.contains("listblocks</td>")
            && lists.contains(&format!("href=\"{}\"", w.list))
            && lists.contains(&format!("data-card=\"/public/card/{}\"", w.o1))
            && !lists.contains("hiddenowned"),
        format!("counter {stored_count}; {}", support::truncate(lists, 160)),
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
        "list page header: at-uri, name (as text), purpose, owner, state in words, the stored-members counter",
        lp.text.contains(&format!("at://{}/app.bsky.graph.list/{LIST}", w.o1))
            && lp.text.contains("&lt;script&gt;alert(1)&lt;/script&gt;")
            && lp.text.contains("<dd>moderation list</dd>")
            && lp.text.contains(&format!("href=\"{}\"", path_did(&w.o1)))
            && lp.text.contains("<dd>Indexed.</dd>")
            && lp.text.contains(&format!("{stored} stored members")),
        support::truncate(&lp.text, 160),
    );
    let navs = between(
        &lp.text,
        "<nav class=\"sections\" aria-label=\"Sections\">",
        "</nav>",
    );
    let ids: Vec<&str> = between(&lp.text, "<section id=\"", "\"");
    c.check(
        "list page section nav: exactly two links, Members and Blocked by; two sections",
        navs.len() == 1
            && navs[0]
                == "<a href=\"#members\">Members</a><a href=\"#listblockers\">Blocked by</a>"
            && ids == ["members", "listblockers"],
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
        ok && want.len() == 62,
        format!("{d}; {pages} pages"),
    );
    c.check(
        "the withheld rule runs in the section's query, so it shortens no page: 50 rows while more follow, two pages for 62 members",
        short == 0 && pages == 2,
        format!("{short} short pages with a next link; {pages} pages"),
    );
    let want = h
        .strings(&format!(
            "SELECT a.did FROM list_blocks b JOIN actors a ON a.id = b.author_id WHERE b.list_id = {lid} AND {shown}"
        ))
        .await?;
    let (rows, _, _) = walk(h, &w.list, "listblockers").await?;
    let (ok, d) = exactly_once(&rows, &want);
    let sec = section(&lp.text, "listblockers").unwrap_or("");
    c.check(
        "Blocked by (list): every active listblocker exactly once, and the bounded count",
        ok && want.len() == 4
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
        let r = h.get(&format!("/public/list/{}/{rkey}", w.o2)).await?;
        let has = section(&r.text, "members").is_some();
        let nav_has = r.text.contains("<a href=\"#members\">Members</a>");
        if r.status != 200
            || !r.text.contains(&format!("<dd>{words}"))
            || has != members
            || nav_has != members
            || r.text.contains("No members")
        {
            bad.push(format!("{rkey}: {} members={has} nav={nav_has}", r.status));
        }
    }
    let capped = h.get(&format!("/public/list/{}/st-capped", w.o2)).await?;
    c.check(
        "each getListMembers state keeps its wording; the Members section and its nav link only for ready and retained; a capped list says so",
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
        let r = h.get(&format!("/public/search?q={}", enc(q))).await?;
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
        let r = h.get(&format!("/public/search?q={}", enc(&q))).await?;
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
            "/public/search?q={}",
            enc(&format!("at://{}/app.bsky.feed.post/3k", w.s))
        ))
        .await?;
    c.check(
        "a rejected collection says only accounts and lists can be looked up",
        only.text
            .contains("Only accounts and lists can be looked up."),
        support::truncate(&only.text, 100),
    );
    let fail = h.get("/public/search?q=no-such-handle.invalid").await?;
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
            let r = l.get(&format!("/public/search?q={LIVE_HANDLE}")).await?;
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
    let home = h.get("/public").await?;
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
        let n = r
            .text
            .matches("<p class=\"updated\">Last updated <time datetime=\"")
            .count();
        let stamp = updated_of(&r.text).unwrap_or_default();
        let text_ok = r.text.contains(&format!(
            "<time datetime=\"{stamp}\">{} {} UTC</time>.</p>",
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
    let main = page.text.find("</main>").unwrap_or(0);
    c.check(
        "the line is the last thing in the page's content, after the sections",
        page.text
            .find("<p class=\"updated\">")
            .is_some_and(|i| i > page.text.rfind("</section>").unwrap_or(usize::MAX) && i < main),
        "after the last section",
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
    let home = h.get("/public").await?;
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
            && !sec.contains("class=\"count\""),
        format!("api level {}", api["freshness"]["coverage"]["level"]),
    );
    h.sql("UPDATE sweep_cycles SET completed_at = now() - interval '1 day' WHERE kind = 1")
        .await?;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    h.refresh().await?;

    // Outgoing: off by default; on, a third section and a third nav link.
    let page = h.get(&path_did(&w.s)).await?;
    c.check(
        "the outgoing section is off by default",
        section(&page.text, "outgoing").is_none() && !page.text.contains("Blocks by this account"),
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
        "show_outgoing_blocks = true, without restart: the section lists the account's blocks (inactive targets left out), and the nav gains its link",
        row_dids(out).into_iter().collect::<BTreeSet<_>>() == want
            && want.len() == 4
            && page.text.contains("<a href=\"#lists\">On lists</a><a href=\"#outgoing\">Blocks by this account</a></nav>"),
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
        "show_outgoing_blocks = false: the section disappears without restart",
        section(&page.text, "outgoing").is_none(),
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
        match next_of(sec) {
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
    }
    c.check(
        "the admin history routes need a session: without one they redirect to /enter like every admin page and show nothing",
        bad.is_empty(),
        bad.join("; "),
    );
    let page = h.admin_get(&base).await?;
    c.check(
        "a logged-in admin gets the account's history page, in the admin layout, never cached",
        page.status == 200
            && page.header("cache-control").as_deref() == Some("no-store, private")
            && page.text.contains("<a href=\"/settings\">Settings</a>")
            && page
                .text
                .contains(&format!("Removed records naming <code>{}</code>", w.s))
            && page.header("content-security-policy").is_none(),
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
            && next_of(sec).is_some_and(|n| n.starts_with(&base) && n.contains("hb=")),
        format!("{} rows; next {:?}", admin_row_dids(sec).len(), next_of(sec)),
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
        "live-row mark, NULL first_seen wording and cause wording, with the uncertainty of a reconcile",
        sec.contains("blocks this account again")
            && sec.contains("before this instance kept dates")
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
        lim.contains("This instance recorded removals from 20")
            && lim.contains("This instance has recorded removals since 20")
            && lim.contains("Removals older than 365 days are deleted.")
            && lim.contains("This is most of what is missing.")
            && lim.matches("<li>").count() >= 10,
        support::truncate(lim, 160),
    );
    c.check(
        "rows link into the admin UI (the lookup pages), not to /public",
        page.text.contains("href=\"/lookup/did?q=did%3Aplc%3A")
            && page.text.contains("href=\"/lookup/list?q=at%3A%2F%2F")
            && !page.text.contains("href=\"/public"),
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

    // Reaching them: links on the lookup pages, for a session only.
    let did_lookup = format!("/lookup/did?q={}", enc(&w.s));
    let list_lookup = format!(
        "/lookup/list?q={}",
        enc(&format!("at://{}/app.bsky.graph.list/{LIST}", w.o1))
    );
    let with = h.admin_get(&did_lookup).await?;
    let without = h.get(&did_lookup).await?;
    let lwith = h.admin_get(&list_lookup).await?;
    let lwithout = h.get(&list_lookup).await?;
    c.check(
        "the DID and list lookup pages link to \"View history\" for a logged-in admin, and not otherwise",
        with.text.contains(&format!("<a href=\"{base}\">View history</a>"))
            && lwith.text.contains(&format!("<a href=\"{lbase}\">View history</a>"))
            && without.status == 200
            && !without.text.contains("/history")
            && lwithout.status == 200
            && !lwithout.text.contains("/history"),
        format!("{} / {}", with.status, lwith.status),
    );

    // The storage queries directly: the two no page uses, too.
    let mut conn = h.pool.acquire().await.map_err(|e| e.to_string())?;
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let rows = farsight_storage::public::blocks_history_by_author(
            &mut conn,
            sid,
            farsight_storage::public::HistoryArgs {
                excluded: &[],
                horizon: None,
                after,
                limit: 7,
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        after = farsight_storage::public::next_cursor(&rows, 7);
        all.extend(rows.into_iter().map(|r| r.id));
        if after.is_none() {
            break;
        }
    }
    let stored = h.n(&format!("SELECT count(*) FROM blocks_history hh JOIN actors a ON a.id = hh.subject_id WHERE hh.author_id = {sid} AND a.status NOT IN (1,2,3,4)")).await?;
    c.check(
        "the by-author history query (not shown by any page) still pages",
        all.len() as i64 == stored
            && stored == 20
            && all.iter().collect::<BTreeSet<_>>().len() == 20,
        format!("{} by-author block rows", all.len()),
    );
    Ok(())
}

// ------------------------------------------------------- 6–8. withheld

/// Every page of every section of the two data pages, concatenated.
async fn all_text(h: &H, pages: &[String; 2]) -> Result<String, String> {
    let mut text = String::new();
    for (p, ids) in [
        (&pages[0], vec!["blockers", "lists", "outgoing"]),
        (&pages[1], vec!["members", "listblockers"]),
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
    let leaked: Vec<&String> = w.hidden.iter().filter(|d| seen.contains(*d)).collect();
    c.check(
        "accounts with each hidden status (deactivated, takendown, suspended, deleted) appear in no row of any section of any page — as author, member, owner or target",
        leaked.is_empty() && seen.contains(&w.e) && seen.len() > 120,
        format!("{} DIDs on the pages; leaked: {leaked:?}", seen.len()),
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
        "excluded_dids governs /public/* only: the admin history page still shows the excluded account's removed block",
        rows.contains(&w.e),
        format!("{} rows", rows.len()),
    );

    c.section("8. withheld unification");
    let e_page = h.get(&path_did(&w.e)).await?;
    let e_list = h.get(&format!("/public/list/{}/excluded", w.e)).await?;
    let only_notice = |name: &str, r: &Resp, notice: &str| -> Vec<String> {
        [
            ("status 200", r.status == 200),
            ("the notice", r.text.contains(notice)),
            ("the bar", r.text.contains("<nav class=\"public-nav\"")),
            ("no section", !r.text.contains("<section")),
            (
                "no link to a data page",
                !r.text.contains("href=\"/public/did/") && !r.text.contains("href=\"/public/list/"),
            ),
            ("no table", !r.text.contains("<table")),
            (
                "no Last updated line",
                !r.text.contains("class=\"updated\""),
            ),
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
    let hl = h
        .get(&format!("/public/list/{}/hiddenowned", w.hidden[1]))
        .await?;
    identical &= canon(&hl.text, &w.hidden[1]).replace("hiddenowned", "L")
        == canon(&e_list.text, &w.e).replace("excluded", "L");
    c.check(
        "hidden-status and operator-excluded accounts get byte-identical responses (body and headers), for all four statuses and for lists they own",
        identical,
        detail.join(", "),
    );
    // Cards: one 404 for every account this instance does not show.
    let unknown = h.get(&format!("/public/card/{}", w.unknown)).await?;
    let mut same = unknown.status == 404
        && unknown.header("cache-control").as_deref() == Some("no-store")
        && !unknown.text.contains("did:");
    for d in w.hidden.iter().chain([&w.e]) {
        let r = h.get(&format!("/public/card/{d}")).await?;
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
    let image = format!("https://{HOSTNAME}/public/static/og-default.png");
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
        ("/public".to_owned(), "public, max-age=60"),
        (path_did(&w.s), "public, max-age=30"),
        (w.list.clone(), "public, max-age=30"),
        (format!("/public/search?q={}", w.s), "no-store"),
        ("/public/search?q=".to_owned(), "no-store"),
        ("/public/did/nope".to_owned(), "no-store"),
        ("/public/nothing".to_owned(), "no-store"),
        ("/public/about".to_owned(), "no-store"),
        (format!("{}/history", path_did(&w.s)), "no-store"),
        // A card whose fetch failed (the harness's PLC is unreachable).
        (format!("/public/card/{}", w.s), "no-store"),
        (format!("/public/card/{}", w.unknown), "no-store"),
        ("/robots.txt".to_owned(), "public, max-age=300"),
    ] {
        let r = h.get(&p).await?;
        if r.header("cache-control").as_deref() != Some(want) {
            bad.push(format!("{p}: {:?}", r.header("cache-control")));
        }
        if p.starts_with("/public")
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
        "Cache-Control per class (home 60, data pages 30, search, errors, removed paths and incomplete cards no-store, robots 300); CSP, nosniff and Referrer-Policy on every response; no cookie, CORS or RateLimit header",
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
    let sys = h.get("/public").await?;
    h.set(&[("dark_mode_default", Some("dark"))]).await?;
    let dark = h.get("/public").await?;
    h.set(&[("dark_mode_default", Some("light"))]).await?;
    let light = h.get("/public").await?;
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
    let css = h.get("/public/static/public.css").await?;
    let js = h.get("/public/static/public.js").await?;
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
        css.text.contains("nav.public-nav { position: sticky; top: 0;")
            && css.text.contains("scroll-margin-top")
            && css.text.contains(".profile-card { display: none; }")
            && between(&css.text, "@media (hover: hover) {", "}\n}")
                .first()
                .is_some_and(|r| r.contains(".who-wrap.open .profile-card { display: block;"))
            && js.text.contains("window.matchMedia(\"(hover: hover)\").matches"),
        "css rules",
    );
    // instance_description, contact
    let default = h.get("/public").await?;
    h.set(&[
        (
            "instance_description",
            Some("First paragraph <b>plain</b>.\r\n\r\nSecond paragraph."),
        ),
        ("contact", Some("mailto:public@farsight.test")),
    ])
    .await?;
    let custom = h.get("/public").await?;
    let account = h.get(&path_did(&w.s)).await?;
    c.check(
        "instance_description (escaped plain text, blank line = paragraph) and contact replace the defaults on home; the contact is on home only",
        default.text.contains("Farsight is an independent index of public block records")
            && default.text.contains("mailto:ops@farsight.test")
            && custom.text.contains("<p>First paragraph &lt;b&gt;plain&lt;/b&gt;.</p>")
            && custom.text.contains("<p>Second paragraph.</p>")
            && !custom.text.contains("Farsight is an independent index of public block records")
            && custom.text.contains("mailto:public@farsight.test")
            && !custom.text.contains("mailto:ops@farsight.test")
            && !account.text.contains("mailto:"),
        "home",
    );
    c.check(
        "home: instance name, the description, the contact, a search form and the Last updated line; no link to About",
        custom.text.contains(&format!("<h1>Farsight at {HOSTNAME}</h1>"))
            && custom.text.matches("action=\"/public/search\"").count() == 2
            && updated_of(&custom.text).is_some()
            && !custom.text.contains("/public/about")
            && !custom.text.contains("Instance-wide"),
        "home",
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
            && h.get("/public").await?.header("x-robots-tag").as_deref()
                == Some("noindex, nofollow"),
        robots.text.replace('\n', " / "),
    );
    h.set(&[("crawlable", Some("on"))]).await?;
    let robots = h.get("/robots.txt").await?;
    let mut bad = Vec::new();
    for (p, indexable) in [
        ("/public".to_owned(), true),
        (path_did(&w.s), true),
        (w.list.clone(), true),
        (format!("/public/card/{}", w.s), false),
        (format!("{}/history", path_did(&w.s)), false),
        ("/public/about".to_owned(), false),
        (format!("/public/search?q={}", w.s), false),
        ("/public/did/nope".to_owned(), false),
        (path_did(&w.e), false),
    ] {
        let r = h.get(&p).await?;
        let tagged = r.header("x-robots-tag").as_deref() == Some("noindex, nofollow");
        if tagged == indexable {
            bad.push(format!("{p}: {:?}", r.header("x-robots-tag")));
        }
    }
    c.check(
        "crawlable = true: robots.txt allows /public but not search or cards, the narrower rules first; cards, search, error and withheld pages stay noindex",
        robots.text
            == "User-agent: *\nDisallow: /public/search\nDisallow: /public/card\nAllow: /public\nDisallow: /\n"
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

/// The Record cells of a section, in row order.
fn record_cells(sec: &str) -> Vec<&str> {
    let mut out = Vec::new();
    for row in between(sec, "<tr><td>", "</tr>") {
        let cells: Vec<&str> = row.split("</td><td>").collect();
        if cells.len() == 3 {
            out.push(cells[1]);
        }
    }
    out
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
    // What the lookup pages list: incoming blocks without hidden blockers
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
    let did_lookup = format!("/lookup/did?q={}", enc(&w.s));
    let list_lookup = format!(
        "/lookup/list?q={}",
        enc(&format!("at://{}/app.bsky.graph.list/{LIST}", w.o1))
    );
    let heads = |html: &str, id: &str| -> Vec<String> {
        between(section(html, id).unwrap_or(""), "<th>", "</th>")
            .into_iter()
            .map(str::to_owned)
            .collect()
    };
    let cells =
        |html: &str| -> Vec<String> { record_cells(html).into_iter().map(str::to_owned).collect() };
    let plain = |cell: &String, set: &BTreeSet<String>| {
        cell.strip_prefix("<code class=\"record\">")
            .and_then(|x| x.strip_suffix("</code>"))
            .is_some_and(|uri| set.contains(uri))
    };
    let linked = |cell: &String, set: &BTreeSet<String>| {
        let Some(rest) = cell.strip_prefix("<a class=\"record\" href=\"") else {
            return false;
        };
        let Some((href, tail)) = rest.split_once('"') else {
            return false;
        };
        let Some(uri) = tail
            .strip_prefix(" target=\"_blank\" rel=\"noopener noreferrer nofollow\"><code>")
            .and_then(|x| x.strip_suffix("</code></a>"))
        else {
            return false;
        };
        // at://{authority}/{collection}/{rkey} ⇒ the template with each part.
        let parts: Vec<&str> = uri.trim_start_matches("at://").splitn(3, '/').collect();
        set.contains(uri)
            && parts.len() == 3
            && href
                == format!(
                    "https://viewer.example/at/{}/{}/{}",
                    parts[0], parts[1], parts[2]
                )
    };
    // No public table has a Record column, whatever the viewer setting.
    let public_clean = |page: &Resp, lp: &Resp| {
        let block_heads = ["Account", "Created (as stated by the author)"];
        heads(&page.text, "blockers") == block_heads
            && heads(&page.text, "outgoing") == block_heads
            && heads(&lp.text, "listblockers") == block_heads
            && heads(&lp.text, "members") == ["Account", "Added (as stated by the list's owner)"]
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
    let dl = h.get(&did_lookup).await?;
    let ll = h.get(&list_lookup).await?;
    let (b, l) = (cells(&dl.text), cells(&ll.text));
    c.check(
        "record_viewer_url empty: on the lookup pages every record cell is the stored record's at-uri as plain text — no link anywhere",
        b.len() == 50
            && b.iter().all(|x| plain(x, &incoming))
            && !l.is_empty()
            && l.iter().all(|x| plain(x, &on_list))
            && l.iter().any(|x| x.contains("/app.bsky.graph.listitem/"))
            && l.iter().any(|x| x.contains("/app.bsky.graph.listblock/"))
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
    let dl = h.get(&did_lookup).await?;
    let ll = h.get(&list_lookup).await?;
    let (b, l) = (cells(&dl.text), cells(&ll.text));
    c.check(
        "record_viewer_url set, without restart: on the lookup pages every record cell is a link built from the template with the record's authority, collection and rkey, opening in a new tab with rel=noopener noreferrer nofollow, its text the at-uri",
        b.len() == 50
            && b.iter().all(|x| linked(x, &incoming))
            && b.iter().all(|x| x.contains("/app.bsky.graph.block/"))
            && !l.is_empty()
            && l.iter().all(|x| linked(x, &on_list)),
        format!("first {:?}", b.first()),
    );
    let page = h.get(&path_did(&w.s)).await?;
    let lp = h.get(&w.list).await?;
    let outside: Vec<String> = hrefs(&page.text)
        .into_iter()
        .chain(hrefs(&lp.text))
        .filter(|x| !(x.starts_with("/public") || x.starts_with('#')))
        .collect();
    c.check(
        "with a viewer set the public pages are unchanged: no Record column, and no link to anywhere outside /public",
        public_clean(&page, &lp) && outside.is_empty(),
        format!("{outside:?}"),
    );
    h.set(&[
        ("record_viewer_url", Some("")),
        ("show_outgoing_blocks", None),
    ])
    .await?;
    let dl = h.get(&did_lookup).await?;
    c.check(
        "record_viewer_url emptied again: plain text again",
        cells(&dl.text).iter().all(|x| plain(x, &incoming)),
        "plain",
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
    let r = h.get(&format!("/public/card/{target}")).await?;
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
    let unknown = h.get(&format!("/public/card/{}", w.unknown)).await?;
    let hidden = h.get(&format!("/public/card/{}", w.hidden[0])).await?;
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
        let (http, url) = (
            h.fresh(),
            format!("{}/public/card/{}", h.base, did("mem", i)),
        );
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
        let r = h.get(&format!("/public/card/{}", did("mem", i))).await?;
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
            .get_with(&one, &format!("/public/card/{}", w.unknown), &[])
            .await?;
        codes.push(r.status);
        if r.status == 429 {
            limited = Some(r);
        }
    }
    let elsewhere = h.get(&format!("/public/card/{}", w.unknown)).await?;
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
    let r = l.get(&format!("/public/card/{LIVE_DID}")).await?;
    let m1 = l.metrics_text().await?;
    let img = between(&r.text, "<img class=\"pc-avatar\" src=\"", "\"")
        .first()
        .map(|s| s.replace("&amp;", "&"))
        .unwrap_or_default();
    let cid = img.split("&cid=").nth(1).unwrap_or("").to_owned();
    c.check(
        "a did:plc account's card: the verified handle, the DID, and the creation date — the createdAt of the first entry of its PLC audit log, as the harness reads it from the directory",
        r.status == 200
            && r.text.contains(&format!("<p class=\"handle\">@{LIVE_HANDLE}</p>"))
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
            && r.text.contains("alt=\"\" width=\"64\" height=\"64\" loading=\"lazy\" referrerpolicy=\"no-referrer\">")
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
            "<a class=\"who\" href=\"/public/did/{LIVE_DID}\" title=\"{LIVE_DID}\" data-card=\"/public/card/{LIVE_DID}\">@{LIVE_HANDLE}</a>"
        )),
        "row",
    );
    l.set(&[("show_avatars", None)]).await?;
    let off = l.get(&format!("/public/card/{LIVE_DID}")).await?;
    let m2 = l.metrics_text().await?;
    c.check(
        "show_avatars = false, without restart: the card has no image and no placeholder, keeps the handle, DID and date, and is counted avatars_disabled; the CSP allows no foreign image",
        off.status == 200
            && !off.text.contains("<img")
            && !off.text.contains("pc-avatar")
            && off.text.contains(&format!("@{LIVE_HANDLE}"))
            && off.text.contains(&format!("<time datetime=\"{created}\">"))
            && off.header("content-security-policy").as_deref() == Some(CSP)
            && off.header("cache-control").as_deref() == Some("public, max-age=300")
            && cards_metric(&m2, "avatars_disabled") - cards_metric(&m1, "avatars_disabled") == 1.0,
        support::truncate(&off.text, 200),
    );
    l.set(&[("show_avatars", Some("on"))]).await?;

    // did:web: no creation date to read.
    let web = l.get(&format!("/public/card/{WEB_DID}")).await?;
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
    let r = l.get(&format!("/public/card/{}", did("blk", 9))).await?;
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
            h.get_with(&one, &format!("/public/search?q={}", w.s), &[])
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
        codes.push(h.get_with(&one, "/public", &[]).await?.status);
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
            if r.text.contains(&format!(
                "<span class=\"handle\">@{LIVE_HANDLE}</span> <code>{LIVE_DID}</code>"
            )) {
                c.check(
                    "a handle verified in both directions (DID document, then forward resolution) is shown beside the DID, and in og:title",
                    total(&m, "resolved") >= 1.0
                        && meta(&r.text, "og:title") == Some(format!("@{LIVE_HANDLE} ({LIVE_DID})").as_str()),
                    format!("resolved {}", total(&m, "resolved")),
                );
                let again = l.get(&path_did(LIVE_DID)).await?;
                let m2 = l.metrics_text().await?;
                c.check(
                    "the verified handle is served from the cache on the next view",
                    again.text.contains(&format!("@{LIVE_HANDLE}"))
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
                &format!("{}/settings", h.base),
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
        "changing reads while the public UI is on is rejected with a message naming the key; nothing is written (the ui mode is free: stage 7)",
        bad.is_empty() && h.get("/public").await?.status == 200 && before.contains("public_ui = true"),
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
        "toggled off from admin: /public/* is 404 again, like any unknown route, without restart",
        off.status == 404 && off.text == unknown.text && h.get("/public").await?.status == 404,
        off.short(),
    );
    let path = h.config_path.clone();
    let text = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let r = h
        .admin
        .post_form(
            &format!("{}/settings", h.base),
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
        token.is_some() && h.get("/public").await?.status == 404,
        support::truncate(&r.text, 120),
    );
    if let Some(t) = token {
        let done = h.confirm(&t).await?;
        c.check(
            "…and the confirmation commits the edited file",
            done.text.contains("Saved.") && h.get("/public").await?.status == 200,
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
    let pages = ["home", "search", "did", "list", "card", "robots", "other"];
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
        "farsight_public_ui_requests_total has exactly the page labels home, search, did, list, card, robots, other — no about, did_history or list_history — with bucketed statuses; the removed paths are counted under other",
        missing.is_empty()
            && seen == pages.iter().copied().collect::<BTreeSet<_>>()
            && req("search", "3xx") > 0.0
            && req("did", "4xx") > 0.0
            && req("did", "5xx") > 0.0
            && req("card", "2xx") > 0.0
            && req("card", "4xx") > 0.0
            && req("other", "4xx") > 0.0,
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
    let public: Vec<&(String, String)> = pages
        .iter()
        .filter(|(p, _)| p.starts_with("/public"))
        .collect();
    let documents: Vec<&&(String, String)> =
        public.iter().filter(|(_, t)| t.contains("<html")).collect();
    let mut inline = Vec::new();
    for (path, html) in &public {
        let scripts = between(html, "<script", ">");
        let external = scripts.iter().all(|s| {
            s.starts_with(" src=\"/public/static/")
                && (s.ends_with(".js\"") || s.ends_with(".js\" defer"))
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
        "no inline script, handler or style on any public response: every <script> loads /public/static/*.js",
        inline.is_empty() && public.len() > 100,
        format!("{} responses scanned; offenders: {inline:?}", public.len()),
    );
    let mut bad = Vec::new();
    for (path, html) in &documents {
        let nav = between(html, "<nav class=\"public-nav\"", "</nav>");
        let ok = nav.len() == 1
            && nav[0].contains("<a class=\"brand\" href=\"/public\">Farsight</a>")
            && nav[0].contains(
                "<form class=\"search\" action=\"/public/search\" method=\"get\" role=\"search\">",
            )
            && nav[0].contains("<input type=\"search\" name=\"q\"")
            && !nav[0].contains(" value=\"")
            && nav[0].contains("class=\"theme-toggle\"");
        if !ok {
            bad.push(path.clone());
        }
    }
    c.check(
        "the bar is on every public page — data pages, the withheld notice, error pages: brand link to /public, a GET search form to /public/search with one empty field q, the theme toggle",
        bad.is_empty() && documents.len() > 100,
        format!("{} pages scanned; offenders: {bad:?}", documents.len()),
    );
    let mut bad = Vec::new();
    for (path, html) in &public {
        let out: Vec<String> = hrefs(html)
            .into_iter()
            .filter(|x| {
                !(x.starts_with("/public")
                    || x.starts_with('#')
                    || x.starts_with("https://viewer.example/"))
            })
            .collect();
        let lower = html.to_ascii_lowercase();
        let names = [
            "/enter",
            "/login",
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
        "no link out of /public/* on any public response (the configured record viewer aside): no login link, no admin route named",
        bad.is_empty(),
        support::truncate(&bad.join("; "), 400),
    );
    let mut bad = Vec::new();
    let mut times = 0;
    for (path, html) in &public {
        for t in between(html, "<time ", "</time>") {
            times += 1;
            let ok = t
                .strip_prefix("datetime=\"")
                .and_then(|x| x.split_once("\">"))
                .is_some_and(|(iso, text)| {
                    is_utc_instant(iso) && text == format!("{} {} UTC", &iso[..10], &iso[11..19])
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
    for (path, html) in pages.iter().filter(|(p, _)| p.starts_with("/public")) {
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
    // A file as the previous version's Settings page left it: the retired
    // key is still there.
    let cfg = config_toml(
        &dsn,
        admin,
        &format!("127.0.0.1:{mport}"),
        plc,
        "public_ui = false",
        "show_history = true",
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
        log_path: server.dir.join("server.log").display().to_string(),
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
    check_retired_key_loads(c, &h, &w).await?;
    check_enable_flow(c, &mut h, &w).await?;
    check_retired_key_removed(c, &h, &w).await?;
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
        println!("   account {}/public/did/{}", h.base, w.s);
        println!("   list    {}{}", h.base, w.list);
        println!("   withheld {}/public/did/{}", h.base, w.e);
        println!("   history {}/admin/did/{}/history", h.base, w.s);
        if let Some((_, l)) = &live {
            println!("   live cards {}/public/did/{}", l.base, w.card);
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
    println!("== farsight stage-6 harness: Mode A (public UI)");
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
