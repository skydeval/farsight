//! `farsight-stage5-harness`: Phase B Mode A — the public UI (stage-5
//! kickoff). Every assertion reads real responses from a real `farsight`
//! process over HTTP and real stored rows; expectations that depend on
//! data are derived from SQL against the same database, and expectations
//! about coverage from what the API returns for the same query.
//!
//! Sections:
//!
//! - 0: the history write path, through the real apply path (its own DB);
//! - 1: toggle gates and the enable-confirmation flow;
//! - 2: routes; 3: search; 4: coverage wording; 5: history paging;
//! - 6–8: the withheld rule (hidden statuses, operator exclusion, one
//!   notice);
//! - 9: OpenGraph; 10: rate classes and the render bound; 11: cache
//!   headers; 12: every `[public_ui]` key, hot; 13: robots;
//! - 14: caller independence, handles, untrusted text, metrics.
//!
//! `--keep` keeps the Postgres container; `--skip-live` skips the two
//! checks that resolve a real handle over the network; `--hold` leaves
//! the seeded server running after the checks, public UI on, until the
//! harness is interrupted (for looking at the pages in a browser).

#[allow(dead_code)]
#[path = "../stage3_harness/seed.rs"]
mod seed;
mod store;
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
use crate::support::{Checks, Http, Pg, Resp, Server, csrf_of, enc, free_port, set_cookie};

const PASSWORD: &str = "harness-password-123";
const HOSTNAME: &str = "farsight.test";
const NS: &str = "app.nearhorizon.farsight";
/// A real account with a stable handle, for the two live checks.
const LIVE_HANDLE: &str = "bsky.app";
const LIVE_DID: &str = "did:plc:z72i7hdynmk6r22z27h6tvur";

const COMPLETE: &str = "Complete.";
const ASSISTED: &str = "Best effort. This instance has not finished indexing the whole network; \
                        it used a backlink index to find records naming this account.";
const PARTIAL: &str = "Partial.";
const WITHHELD_ACCOUNT: &str = "Data for this account is not shown on this instance.";
const WITHHELD_LIST: &str = "Data for this list is not shown on this instance.";
const INACTIVE_LINE: &str = "Accounts that are not active are not shown.";
const OPERATOR_LINE: &str =
    "This instance's operator has chosen not to show some accounts on these pages.";
const CSP: &str = "default-src 'none'; style-src 'self'; script-src 'self'; img-src 'self'; \
                   connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

fn config_toml(dsn: &str, admin_token: &str, metrics_bind: &str, plc: &str, extra: &str) -> String {
    let bcrypt = bcrypt::hash(PASSWORD, 4).expect("bcrypt");
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
{extra}

[auth]
admin_token_sha256 = "{}"
admin_password_bcrypt = "{bcrypt}"

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
        let r = self
            .admin
            .post_form(
                &format!("{}/login", self.base),
                &[],
                &[("password", PASSWORD)],
            )
            .await?;
        self.cookie =
            set_cookie(&r, "farsight_admin").ok_or_else(|| format!("login: {}", r.short()))?;
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
    let k = "<a href=\"/public/did/";
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
    assisted: String,
    partial: String,
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
        assisted: did("sub", 2),
        partial: did("sub", 3),
    };
    let s = seed::actor(p, &w.s).await?;
    let e = seed::actor(p, &w.e).await?;
    let o1 = seed::actor(p, &w.o1).await?;
    let o2 = seed::actor(p, &w.o2).await?;
    seed::actor(p, &w.assisted).await?;
    seed::actor(p, &w.partial).await?;
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
        "/public/static/public.css".to_owned(),
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
        "public_ui = false ⇒ every /public/* route answers 404, byte-identical to an unknown route",
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
    // The loader refuses every other access combination.
    let mut refused = Vec::new();
    for (reads, ui) in [
        ("api_key", "public_read"),
        ("disabled", "public_read"),
        ("public", "auth_all"),
        ("public", "disabled"),
    ] {
        let text = format!(
            "[server]\nhostname = \"h.test\"\ncontact = \"mailto:x@h.test\"\n[storage]\ndatabase_url = \"postgres://x\"\n\
             [auth]\nadmin_token_sha256 = \"{}\"\nadmin_password_bcrypt = \"$2b$04$x\"\n\
             [access]\nreads = \"{reads}\"\nui = \"{ui}\"\npublic_ui = true\n",
            "0".repeat(64)
        );
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
        if !(on.is_err()
            && off.is_ok()
            && msg.contains("access.reads")
            && msg.contains("access.ui"))
        {
            refused.push(format!("{reads}/{ui}: {msg}"));
        }
    }
    c.check(
        "config load refuses public_ui = true without reads = public and ui = public_read, naming both keys",
        refused.is_empty(),
        refused.join("; "),
    );
    Ok(())
}

/// A `farsight` process given an invalid config exits non-zero.
fn check_exit_on_invalid(c: &mut Checks, dsn: &str) -> Result<(), String> {
    let dir = std::env::temp_dir().join(format!("farsight-stage5-invalid-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let cfg = dir.join("config.toml");
    let text = config_toml(
        dsn,
        "fsa_x",
        "127.0.0.1:1",
        "https://127.0.0.1:9",
        "public_ui = true",
    )
    .replace("reads = \"public\"", "reads = \"disabled\"");
    std::fs::write(&cfg, text).map_err(|e| e.to_string())?;
    let out = std::process::Command::new(support::farsight_bin())
        .env("FARSIGHT_CONFIG", &cfg)
        .output()
        .map_err(|e| e.to_string())?;
    let err = String::from_utf8_lossy(&out.stderr);
    c.check(
        "farsight exits non-zero on public_ui = true with reads = disabled",
        !out.status.success() && err.contains("access.public_ui"),
        format!(
            "exit {:?}: {}",
            out.status.code(),
            support::truncate(&err, 300)
        ),
    );
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

async fn check_enable_flow(c: &mut Checks, h: &mut H, w: &World) -> Result<(), String> {
    c.section("14. enable-confirmation flow");
    let settings = h
        .admin
        .get(&format!("{}/settings", h.base), &[("cookie", &h.cookie)])
        .await?;
    let controls = [
        "enabled",
        "instance_description",
        "contact",
        "show_outgoing_blocks",
        "show_history",
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
        "Settings has a Public UI subsection with a control for the toggle and every [public_ui] key, and a preview link",
        missing.is_empty() && settings.text.contains("href=\"/public\" target=\"_blank\""),
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
        "Coverage status",
        "Incoming blocks",
        "Lists naming any account",
        "List memberships",
        "Historical data",
        "not available from any other public source",
    ]
    .iter()
    .all(|s| first.text.contains(s));
    c.check(
        "first POST turning public_ui on renders the confirmation page listing every data category",
        first.status == 200 && token.is_some() && lists_all,
        support::truncate(&first.text, 200),
    );
    c.check(
        "…and writes nothing: /public is still 404 and the config still says off",
        h.get("/public").await?.status == 404
            && !std::fs::read_to_string(&h.config_path)
                .unwrap_or_default()
                .contains("public_ui = true"),
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
        second.status == 200 && second.text.contains("Saved.") && second.text.contains("access.public_ui") && home.status == 200,
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

async fn check_routes(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("2. routes");
    let mut bad = Vec::new();
    for p in [
        "/public".to_owned(),
        "/public/about".to_owned(),
        path_did(&w.s),
        format!("{}/history", path_did(&w.s)),
        w.list.clone(),
        format!("{}/history", w.list),
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
        "every page and asset renders against seeded data (200)",
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
        format!("{}/history?hb=!!!", path_did(&w.s)),
        format!("{}?lc=!!!", w.list),
        format!("/public/search?q={long}"),
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
        "malformed DID, list key, cursor and over-long input ⇒ 400, no-store",
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
        "an unknown DID renders its sections empty with their coverage (never 'not found'), and is not interned",
        u.status == 200
            && section(&u.text, "blockers").is_some_and(|s| s.contains("None found.") && s.contains(COMPLETE))
            && section(&u.text, "lists").is_some_and(|s| s.contains("None found."))
            && h.n(&format!("SELECT count(*) FROM actors WHERE did = '{}'", w.unknown)).await? == 0
            && actors_before == h.n("SELECT count(*) FROM actors").await?,
        u.short(),
    );
    let ul = h.get(&format!("/public/list/{}/nosuchlist", w.o1)).await?;
    c.check(
        "an unknown list ⇒ 404 page saying this instance has no record of it",
        ul.status == 404
            && ul
                .text
                .contains("This instance has no record of this list."),
        ul.short(),
    );
    let other = h.get("/public/nothing/here").await?;
    c.check(
        "an unknown path under /public/ ⇒ 404",
        other.status == 404,
        other.short(),
    );
    Ok(())
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

/// The phrases a page must print for the API's reason codes.
fn phrase(code: &str) -> &'static str {
    match code {
        "sync_events_unavailable" => "the event stream in use does not report repository resyncs",
        "storage_refusal" => "the storage limit was reached",
        "firehose_disconnected" => "disconnected from the event stream",
        "firehose_lagging" => "behind the event stream",
        "firehose_gap" => "a gap in the event stream is being repaired",
        "sweep_incomplete" => "the first full pass over the network is not finished",
        "list_pending" | "list_pending_historical" => "some lists are still being indexed",
        "list_capped" => "a list is stored only in part",
        "list_unavailable" | "list_missing" | "list_deferred" => "a list could not be indexed",
        "list_not_tracked" => "the list is not indexed because nothing blocks it",
        "discovery_truncated" => "the backlink search was cut short",
        "party_debt" => "this account&#x27;s own records need re-reading",
        _ => "another limitation",
    }
}

fn level_words(level: &str) -> &'static str {
    match level {
        "complete" => COMPLETE,
        "assisted" => ASSISTED,
        _ => PARTIAL,
    }
}

/// Whether a section's coverage line says what the API's `freshness` for
/// the same query says.
fn line_matches(sec: &str, fresh: &Value) -> (bool, String) {
    let cov = &fresh["coverage"];
    let level = cov["level"].as_str().unwrap_or("");
    let mut ok = sec.contains(&format!("<strong>{}</strong>", level_words(level)));
    let mut detail = format!("level {level}");
    for r in cov["reasons"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        ok &= sec.contains(phrase(r));
        detail.push_str(&format!(", {r}"));
    }
    (ok, detail)
}

async fn check_coverage(
    c: &mut Checks,
    h: &mut H,
    w: &World,
    paused: &AtomicBool,
) -> Result<(), String> {
    c.section("4. coverage in plain English");
    h.refresh().await?;
    let q = |d: &str| format!("actor={}", enc(d));
    // complete
    let page = h.get(&path_did(&w.s)).await?;
    let api = h.xrpc("query.getIncomingBlocks", &q(&w.s)).await?;
    let (ok, d) = line_matches(
        section(&page.text, "blockers").unwrap_or(""),
        &api["freshness"],
    );
    c.check(
        "complete: the section prints \"Complete.\" as the API reports for the same query",
        ok && api["freshness"]["coverage"]["level"] == "complete",
        d,
    );
    let cov = section(&page.text, "coverage").unwrap_or("");
    c.check(
        "the line states indexedAt, asOf and completeSince as absolute UTC <time> elements",
        section(&page.text, "blockers").is_some_and(|s| {
            s.contains("Reflects what this instance saw up to <time datetime=\"")
                && s.contains("Page built <time datetime=\"")
                && s.contains("This has held since <time datetime=\"")
                && s.contains(" UTC</time>")
        }),
        "three sentences",
    );
    c.check(
        "the coverage panel carries the page summary, the inactive-accounts line and each section's raw freshness",
        cov.contains(COMPLETE)
            && cov.contains(INACTIVE_LINE)
            && cov.matches("<details>").count() == 2
            && cov.contains("&quot;level&quot;: &quot;complete&quot;"),
        support::truncate(cov, 200),
    );
    let home = h.get("/public").await?;
    let stats = h.xrpc("query.getStats", "").await?;
    let (ok, d) = line_matches(
        section(&home.text, "coverage").unwrap_or(""),
        &stats["freshness"],
    );
    c.check(
        "home: the instance-wide line matches getStats and says each page states its own",
        ok && home.text.contains("<h2>Instance-wide</h2>")
            && home.text.contains(
                "Each account and list page states its own coverage, which can be lower.",
            ),
        d,
    );

    // complete with a reason: a capped list.
    let capped = h.get(&format!("/public/list/{}/st-capped", w.o2)).await?;
    let api = h
        .xrpc(
            "query.getListMembers",
            &format!(
                "list={}",
                enc(&format!("at://{}/app.bsky.graph.list/st-capped", w.o2))
            ),
        )
        .await?;
    let sec = section(&capped.text, "members").unwrap_or("");
    let (ok, d) = line_matches(sec, &api["freshness"]);
    c.check(
        "complete with a reason shows the reason: a capped list is \"Complete.\" and \"a list is stored only in part\"",
        ok && api["freshness"]["coverage"]["level"] == "complete"
            && sec.contains("a list is stored only in part")
            && capped.text.contains("This instance stores only part of this list."),
        d,
    );

    // assisted and partial: no completed baseline.
    h.sql("UPDATE sweep_cycles SET completed_at = NULL WHERE kind = 1")
        .await?;
    let a = h
        .n(&format!(
            "SELECT id FROM actors WHERE did = '{}'",
            w.assisted
        ))
        .await?;
    h.sql(&format!(
        "INSERT INTO discovery_state (actor_id, state, source, started_at, discovered_witness, completed_at)
         VALUES ({a}, 3, 'https://backlinks.test', now() - interval '10 minutes', now() - interval '10 minutes', now() - interval '9 minutes')"
    ))
    .await?;
    h.sql(&format!(
        "INSERT INTO subject_coverage (actor_id, scope, confirmed_at, refs_found) VALUES ({a}, 1, now(), 0), ({a}, 2, now(), 0)"
    ))
    .await?;
    h.refresh().await?;
    let page = h.get(&path_did(&w.assisted)).await?;
    let api = h.xrpc("query.getIncomingBlocks", &q(&w.assisted)).await?;
    let sec = section(&page.text, "blockers").unwrap_or("");
    let (ok, d) = line_matches(sec, &api["freshness"]);
    c.check(
        "assisted: \"Best effort. … it used a backlink index …\", with its reasons, and an empty section says coverage is not complete",
        ok && api["freshness"]["coverage"]["level"] == "assisted"
            && sec.contains(ASSISTED)
            && sec.contains("None found so far — coverage is not complete."),
        d,
    );
    let page = h.get(&path_did(&w.partial)).await?;
    let api = h.xrpc("query.getIncomingBlocks", &q(&w.partial)).await?;
    let sec = section(&page.text, "blockers").unwrap_or("");
    let (ok, d) = line_matches(sec, &api["freshness"]);
    c.check(
        "partial + sweep_incomplete: \"Partial.\" and \"the first full pass over the network is not finished\"; no 'held since'",
        ok && api["freshness"]["coverage"]["level"] == "partial"
            && sec.contains("the first full pass over the network is not finished")
            && !sec.contains("This has held since")
            && sec.contains("href=\"/public/about#reason-sweep_incomplete\""),
        d,
    );

    // firehose_disconnected.
    paused.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    h.sql("UPDATE firehose_state SET connected = false WHERE id = 1")
        .await?;
    h.refresh().await?;
    let page = h.get(&path_did(&w.s)).await?;
    let api = h.xrpc("query.getIncomingBlocks", &q(&w.s)).await?;
    let (ok, d) = line_matches(
        section(&page.text, "blockers").unwrap_or(""),
        &api["freshness"],
    );
    c.check(
        "partial + firehose_disconnected renders \"disconnected from the event stream\"",
        ok && page.text.contains("disconnected from the event stream"),
        d,
    );
    paused.store(false, Ordering::Relaxed);
    h.sql("UPDATE sweep_cycles SET completed_at = now() - interval '1 day' WHERE kind = 1")
        .await?;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    h.refresh().await?;

    // Outgoing: off by default; on, its own coverage, lower than the
    // incoming sections for an account with a debt.
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
    let inc = section(&page.text, "blockers").unwrap_or("");
    let cov = section(&page.text, "coverage").unwrap_or("");
    let want = h
        .strings(&format!(
            "SELECT s.did FROM blocks b JOIN actors s ON s.id = b.subject_id
             WHERE b.author_id = {sid} AND s.status NOT IN (1, 2, 3, 4)"
        ))
        .await?;
    c.check(
        "show_outgoing_blocks = true, without restart: the section lists the account's blocks (inactive targets left out)",
        row_dids(out).into_iter().collect::<BTreeSet<_>>() == want && want.len() == 4,
        format!("{} rows, {} expected", row_dids(out).len(), want.len()),
    );
    c.check(
        "a page whose sections differ: outgoing is \"Partial.\" (this account's own records need re-reading) while incoming is \"Complete.\", and the summary is the lowest",
        out.contains(&format!("<strong>{PARTIAL}</strong>"))
            && out.contains("this account&#x27;s own records need re-reading")
            && inc.contains(&format!("<strong>{COMPLETE}</strong>"))
            && cov.contains(&format!("<strong>{PARTIAL}</strong>"))
            && !cov[..cov.find("<details>").unwrap_or(cov.len())].contains(&format!("<strong>{COMPLETE}</strong>"))
            && cov.matches("<details>").count() == 3,
        "blockers complete, outgoing partial, summary partial",
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

// ------------------------------------------------------ live sections

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

async fn check_live_sections(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("2b. live sections against stored rows");
    let sid = h
        .n(&format!("SELECT id FROM actors WHERE did = '{}'", w.s))
        .await?;
    let shown = "a.status NOT IN (1, 2, 3, 4)";
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
    let page = h.get(&path_did(&w.s)).await?;
    let sec = section(&page.text, "blockers").unwrap_or("");
    c.check(
        "the bounded count equals the stored count with the page's filters",
        sec.contains(&format!("<span class=\"count\">{}</span>", want.len())),
        support::truncate(sec, 120),
    );
    let lists = section(&page.text, "lists").unwrap_or("");
    c.check(
        "On listblocked lists: the list with its purpose, owner, listblock counter and 'added' label; a hidden owner's list is left out",
        lists.contains(&format!("href=\"{}\"", w.list))
            && lists.contains("moderation list")
            && lists.contains("3 listblocks")
            && lists.contains("Added (as stated by the list's owner)")
            && lists.contains("An account that blocks a moderation list blocks everyone on it.")
            && !lists.contains("hiddenowned"),
        support::truncate(lists, 200),
    );
    c.check(
        "a list name carrying markup renders as text",
        lists.contains("&lt;script&gt;alert(1)&lt;/script&gt; &amp; friends")
            && !page.text.contains("<script>alert(1)"),
        "escaped",
    );
    c.check(
        "the page links to the history page while show_history is on",
        page.text
            .contains(&format!("href=\"{}/history\"", path_did(&w.s))),
        "linked",
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
        "a page shortened by the filter still offers \"next\"",
        short >= 1,
        format!("{short} short pages with a next link"),
    );
    let want = h
        .strings(&format!(
            "SELECT a.did FROM list_blocks b JOIN actors a ON a.id = b.author_id WHERE b.list_id = {lid} AND {shown}"
        ))
        .await?;
    let (rows, _, _) = walk(h, &w.list, "listblockers").await?;
    let (ok, d) = exactly_once(&rows, &want);
    let sec = section(&lp.text, "listblockers").unwrap_or("");
    let api = h.xrpc("query.getStats", "").await?;
    c.check(
        "Blocked by (list): every active listblocker exactly once, the bounded count, and network-scope coverage",
        ok && want.len() == 4
            && sec.contains(&format!("<span class=\"count\">{}</span>", want.len()))
            && sec.contains(level_words(api["freshness"]["coverage"]["level"].as_str().unwrap_or(""))),
        d,
    );

    // List states.
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
        let api = h
            .xrpc(
                "query.getListMembers",
                &format!(
                    "list={}",
                    enc(&format!("at://{}/app.bsky.graph.list/{rkey}", w.o2))
                ),
            )
            .await?;
        let has = section(&r.text, "members").is_some();
        if r.status != 200
            || !r.text.contains(&format!("<dd>{words}"))
            || has != members
            || r.text.contains("No members")
        {
            bad.push(format!(
                "{rkey} (api state {}): {} members={has}",
                api["state"], r.status
            ));
        }
    }
    c.check(
        "each getListMembers state renders its wording; members only for ready and retained",
        bad.is_empty(),
        bad.join("; "),
    );
    Ok(())
}

// ----------------------------------------------------------- 5. history

async fn check_history(c: &mut Checks, h: &H, w: &World) -> Result<(), String> {
    c.section("5. historical queries");
    let sid = h
        .n(&format!("SELECT id FROM actors WHERE did = '{}'", w.s))
        .await?;
    let oid = h
        .n(&format!("SELECT id FROM actors WHERE did = '{}'", w.o1))
        .await?;
    let base = format!("{}/history", path_did(&w.s));
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
    let (rows, pages, _) = walk(h, &base, "removed-blocks").await?;
    let (ok, d) = exactly_once(&rows, &want);
    c.check(
        "removed blocks: walking the cursor across 100 rows with one removed_at returns no row twice and skips none",
        ok && tied == 100 && pages == 3 && want.len() == 102,
        format!("{d}; {tied} tied; {pages} pages"),
    );
    let page = h.get(&base).await?;
    let sec = section(&page.text, "removed-blocks").unwrap_or("");
    let p2 = next_of(sec).map(|n| n.contains("hb=")).unwrap_or(false);
    c.check(
        "the first page is the newest removals, 50 rows, with a next link carrying its own cursor",
        row_dids(sec).len() == 50 && p2,
        format!("{} rows", row_dids(sec).len()),
    );
    c.check(
        "a row older than the retention but not yet pruned is not shown",
        h.n(&format!("SELECT count(*) FROM blocks_history WHERE subject_id = {sid} AND removed_at < now() - interval '365 days'")).await? == 1
            && !rows.contains(&did("lbk", 3)),
        "400-day-old row stored, not shown",
    );
    c.check(
        "live-row mark, NULL first_seen wording and cause wording",
        sec.contains("blocks this account again")
            && sec.contains("before this instance kept dates")
            && sec.contains("Block deleted."),
        "marks present",
    );
    c.check(
        "the reconcile cause is worded with its uncertainty",
        sec.contains("Found missing when the author&#x27;s records were re-read. Removed some time between &#x27;last seen&#x27; and this time."),
        "reconcile rows",
    );
    let mem = section(&page.text, "removed-memberships").unwrap_or("");
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
    let lim = section(&page.text, "limits").unwrap_or("");
    c.check(
        "the limits block states both recording windows, the retention and the limits",
        lim.contains("This instance recorded removals from <time")
            && lim.contains("This instance has recorded removals since <time")
            && lim.contains("Removals older than 365 days are deleted.")
            && lim.contains("This is most of what is missing.")
            && lim.matches("<li>").count() >= 10,
        support::truncate(lim, 160),
    );
    c.check(
        "history is outside the coverage contract: no coverage panel, none of the coverage words",
        section(&page.text, "coverage").is_none()
            && !page.text.contains(COMPLETE)
            && !page.text.contains(PARTIAL)
            && !page.text.contains("Best effort"),
        "none",
    );
    c.check(
        "history pages are public, max-age=30, and never offered to search engines",
        page.header("cache-control").as_deref() == Some("public, max-age=30")
            && page.header("x-robots-tag").as_deref() == Some("noindex, nofollow"),
        format!("{:?}", page.header("cache-control")),
    );

    // The list's history.
    let lbase = format!("{}/history", w.list);
    let want = h
        .strings(&format!(
            "SELECT a.did FROM list_items_history hh JOIN actors a ON a.id = hh.subject_id
             WHERE hh.owner_id = {oid} AND hh.list_rkey = '{LIST}' AND a.status NOT IN (1, 2, 3, 4)"
        ))
        .await?;
    let (rows, pages, _) = walk(h, &lbase, "removed-members").await?;
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
    let (rows, _, _) = walk(h, &lbase, "removed-listblocks").await?;
    let lp = h.get(&lbase).await?;
    c.check(
        "removed listblocks of a list: every active former listblocker; 'blocks this list again'; listblock wording",
        rows.iter().cloned().collect::<BTreeSet<_>>() == want
            && want.len() == 5
            && lp.text.contains("blocks this list again")
            && lp.text.contains("Listblock deleted.")
            && lp.text.contains("Record changed to block a different list.")
            && section(&lp.text, "removed-members").is_some_and(|s| s.contains("on this list now")),
        format!("{} rows, {} expected", rows.len(), want.len()),
    );
    let dead = h
        .get(&format!("/public/list/{}/st-dead/history", w.o2))
        .await?;
    let none = h
        .get(&format!("/public/list/{}/never-seen/history", w.o2))
        .await?;
    c.check(
        "a list's history page needs no lists row and says so when nothing was recorded",
        dead.status == 200
            && none.status == 200
            && none.text.matches("No removals recorded.").count() == 2,
        format!("{} / {}", dead.status, none.status),
    );

    // The six storage queries directly: the two the UI does not use too.
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
    let lb = farsight_storage::public::list_blocks_history_by_author(
        &mut conn,
        h.n(&format!(
            "SELECT id FROM actors WHERE did = '{}'",
            did("hlb", 1)
        ))
        .await?,
        farsight_storage::public::HistoryArgs {
            excluded: &[],
            horizon: None,
            after: None,
            limit: 50,
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    c.check(
        "the by-author history queries (not shown by any page) page and resolve the list",
        all.len() as i64 == stored
            && stored == 20
            && all.iter().collect::<BTreeSet<_>>().len() == 20
            && lb.len() == 1
            && lb[0]
                .list
                .as_ref()
                .is_some_and(|l| l.rkey == LIST && l.owner_did == w.o1 && l.name.is_some()),
        format!(
            "{} by-author block rows, {} listblock rows",
            all.len(),
            lb.len()
        ),
    );
    Ok(())
}

// ------------------------------------------------------- 6–8. withheld

/// Every page of every section of the four data pages, concatenated.
async fn all_text(h: &H, pages: &[String; 4]) -> Result<String, String> {
    let mut text = String::new();
    for (p, ids) in [
        (&pages[0], vec!["blockers", "lists", "outgoing"]),
        (&pages[1], vec!["removed-blocks", "removed-memberships"]),
        (&pages[2], vec!["members", "listblockers"]),
        (&pages[3], vec!["removed-listblocks", "removed-members"]),
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
    let pages = [
        path_did(&w.s),
        format!("{}/history", path_did(&w.s)),
        w.list.clone(),
        format!("{}/history", w.list),
    ];
    h.set(&[("show_outgoing_blocks", Some("on"))]).await?;
    let text = all_text(h, &pages).await?;
    let seen = dids_in(&text);
    let leaked: Vec<&String> = w.hidden.iter().filter(|d| seen.contains(*d)).collect();
    c.check(
        "accounts with each hidden status (deactivated, takendown, suspended, deleted) appear in no row of any section of any page — as author, member, owner or target",
        leaked.is_empty() && seen.contains(&w.e) && seen.len() > 300,
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
        "before the exclusion the account has an ordinary page, and no operator line is shown",
        epage.status == 200
            && section(&epage.text, "blockers").is_some()
            && !h.get(&path_did(&w.s)).await?.text.contains(OPERATOR_LINE),
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
        "a list the excluded account owns is left out of 'on listblocked lists', and both coverage-panel lines show",
        section(&spage.text, "lists").is_some_and(|s| !s.contains("/excluded\""))
            && spage.text.contains(INACTIVE_LINE)
            && spage.text.contains(OPERATOR_LINE),
        "panel lines",
    );
    c.check(
        "the exclusion changes what the public pages show, nothing else: the API still returns the account's rows",
        h.xrpc("query.getIncomingBlocks", &format!("actor={}&limit=1000", enc(&w.s)))
            .await?["blocks"]
            .as_array()
            .is_some_and(|b| b.iter().any(|x| x["did"] == w.e.as_str())),
        "getIncomingBlocks includes it",
    );

    c.section("8. withheld unification");
    let e_page = h.get(&path_did(&w.e)).await?;
    let e_hist = h.get(&format!("{}/history", path_did(&w.e))).await?;
    let e_list = h.get(&format!("/public/list/{}/excluded", w.e)).await?;
    let e_list_hist = h
        .get(&format!("/public/list/{}/excluded/history", w.e))
        .await?;
    let only_notice = |name: &str, r: &Resp, notice: &str| -> Vec<String> {
        [
            ("status 200", r.status == 200),
            ("the notice", r.text.contains(notice)),
            ("no section", !r.text.contains("<section")),
            (
                "no link to a data or history page",
                !r.text.contains("<a href=\"/public/did/")
                    && !r.text.contains("<a href=\"/public/list/"),
            ),
            ("no table", !r.text.contains("<table")),
            ("no coverage line", !r.text.contains("class=\"coverage")),
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
    wrong.extend(only_notice("history", &e_hist, WITHHELD_ACCOUNT));
    wrong.extend(only_notice("list", &e_list, WITHHELD_LIST));
    wrong.extend(only_notice("list history", &e_list_hist, WITHHELD_LIST));
    c.check(
        "an excluded account's page, history page, list page and list history show the notice and nothing else",
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
        let hp = h.get(&format!("{}/history", path_did(d))).await?;
        let ok = canon(&p.text, d) == canon(&e_page.text, &w.e)
            && canon(&hp.text, d) == canon(&e_hist.text, &w.e)
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
        delta("operator_excluded") >= 4.0 && delta("hidden_status") >= 9.0,
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
        ("/public/about".to_owned(), "public, max-age=60"),
        (path_did(&w.s), "public, max-age=30"),
        (w.list.clone(), "public, max-age=30"),
        (format!("{}/history", path_did(&w.s)), "public, max-age=30"),
        (format!("{}/history", w.list), "public, max-age=30"),
        (format!("/public/search?q={}", w.s), "no-store"),
        ("/public/search?q=".to_owned(), "no-store"),
        ("/public/did/nope".to_owned(), "no-store"),
        ("/public/nothing".to_owned(), "no-store"),
        ("/robots.txt".to_owned(), "public, max-age=300"),
    ] {
        let r = h.get(&p).await?;
        if r.header("cache-control").as_deref() != Some(want) {
            bad.push(format!("{p}: {:?}", r.header("cache-control")));
        }
        if p.starts_with("/public")
            && (r.header("content-security-policy").as_deref() != Some(CSP)
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
        "Cache-Control per class (home/about 60, data pages 30, search and errors no-store, robots 300); CSP, nosniff and Referrer-Policy on every page; no cookie, CORS or RateLimit header",
        bad.is_empty(),
        bad.join("; "),
    );
    let page = h.get(&path_did(&w.s)).await?;
    c.check(
        "pages hold no inline script or style (the CSP allows none)",
        !page.text.contains("<style")
            && !page.text.contains(" style=\"")
            && !page.text.contains("<script>")
            && !page.text.contains("onclick="),
        "none",
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
    // show_history
    h.set(&[("show_history", None)]).await?;
    let page = h.get(&path_did(&w.s)).await?;
    let unknown = h.get("/no-such-route").await?;
    let hist = h.get(&format!("{}/history", path_did(&w.s))).await?;
    let lhist = h.get(&format!("{}/history", w.list)).await?;
    c.check(
        "show_history = false: the history link disappears and both history routes answer like an unknown route",
        !page.text.contains("/history\"")
            && hist.status == 404
            && hist.text == unknown.text
            && lhist.status == 404
            && !h.get(&w.list).await?.text.contains("/history\""),
        format!("{} / {}", hist.status, lhist.status),
    );
    let about = h.get("/public/about").await?;
    c.check(
        "the About page follows the settings (no history section while history is off)",
        section(&about.text, "history").is_none()
            && about.text.contains("id=\"reason-sweep_incomplete\""),
        "about",
    );
    h.set(&[("show_history", Some("on"))]).await?;
    c.check(
        "show_history = true again: the page is back",
        h.get(&format!("{}/history", path_did(&w.s))).await?.status == 200,
        "200",
    );
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
    c.check(
        "one stylesheet with color variables, a prefers-color-scheme rule and a [data-theme=\"dark\"] rule; the toggle persists in localStorage; no external font",
        css.text.contains("@media (prefers-color-scheme: dark)")
            && css.text.contains(":root[data-theme=\"dark\"]")
            && css.text.contains("--bg:")
            && !css.text.contains("@font-face")
            && !css.text.contains("url(")
            && js.text.contains("localStorage.setItem")
            && js.text.contains("data-theme")
            && sys.text.contains("id=\"theme-toggle\""),
        "css and js",
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
    c.check(
        "instance_description (escaped plain text, blank line = paragraph) and contact replace the defaults",
        default.text.contains("Farsight is an independent index of public block records")
            && default.text.contains("mailto:ops@farsight.test")
            && custom.text.contains("<p>First paragraph &lt;b&gt;plain&lt;/b&gt;.</p>")
            && custom.text.contains("<p>Second paragraph.</p>")
            && !custom.text.contains("Farsight is an independent index of public block records")
            && custom.text.contains("mailto:public@farsight.test")
            && !custom.text.contains("mailto:ops@farsight.test"),
        "home",
    );
    h.set(&[("instance_description", Some("")), ("contact", Some(""))])
        .await?;

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
        ("/public/about".to_owned(), true),
        (path_did(&w.s), true),
        (w.list.clone(), true),
        (format!("{}/history", path_did(&w.s)), false),
        (format!("{}/history", w.list), false),
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
        "crawlable = true: robots.txt allows /public (not search), and only history, search, error and withheld pages stay noindex",
        robots.text.contains("Allow: /public\n")
            && robots.text.contains("Disallow: /public/search\n")
            && robots.text.ends_with("Disallow: /\n")
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
    for (what, path) in [
        ("search", format!("/public/search?q={}", w.s)),
        (
            "an account's history",
            format!("{}/history", path_did(&w.s)),
        ),
        ("a list's history", format!("{}/history", w.list)),
    ] {
        let one = h.fresh();
        let mut codes = Vec::new();
        for _ in 0..8 {
            codes.push(h.get_with(&one, &path, &[]).await?.status);
        }
        c.check(
            format!("{what} is on the ui_lookup class: 5 pass, then 429"),
            codes.iter().filter(|s| **s == 429).count() >= 2
                && codes[..5].iter().all(|s| *s != 429),
            format!("{codes:?}"),
        );
    }
    let one = h.fresh();
    let mut codes = Vec::new();
    for _ in 0..12 {
        codes.push(h.get_with(&one, "/public/about", &[]).await?.status);
    }
    c.check(
        "other pages are on the public_ui class (12 quick views pass at burst 20)",
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
        d("public_ui") >= 4.0 && d("ui_lookup") >= 6.0,
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
        ("ui = \"public_read\"", "ui = \"auth_all\""),
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
        if !(b.contains("access.reads") && b.contains("access.ui"))
            || std::fs::read_to_string(&path).map_err(|e| e.to_string())? != before
        {
            bad.push(format!("{to}: {b}"));
        }
    }
    c.check(
        "changing reads or ui while the public UI is on is rejected with a message naming both keys; nothing is written",
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
    c.section("metrics");
    let m = h.metrics_text().await?;
    let req = |page: &str, status: &str| {
        metric(
            &m,
            "farsight_public_ui_requests_total",
            &[("page", page), ("status", status)],
        )
    };
    let pages = [
        "home",
        "about",
        "search",
        "did",
        "did_history",
        "list",
        "list_history",
        "robots",
        "other",
    ];
    let missing: Vec<&str> = pages
        .iter()
        .copied()
        .filter(|p| req(p, "2xx") + req(p, "3xx") + req(p, "4xx") == 0.0)
        .collect();
    c.check(
        "farsight_public_ui_requests_total has every page label, with bucketed statuses",
        missing.is_empty()
            && req("search", "3xx") > 0.0
            && req("did", "4xx") > 0.0
            && req("did", "5xx") > 0.0
            && req("did", "2xx") > 0.0,
        format!("missing {missing:?}"),
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
    let cfg = config_toml(
        &dsn,
        admin,
        &format!("127.0.0.1:{mport}"),
        plc,
        "public_ui = false",
    );
    let server = Server::start(name, Some(&cfg), &format!("127.0.0.1:{port}"), &[])?;
    let http = Http::new(Some("127.0.0.250".parse().expect("ip")));
    server.wait_live(&http, Duration::from_secs(90)).await?;
    let mut form: BTreeMap<&'static str, String> = BTreeMap::new();
    for (k, v) in [
        ("show_history", "on"),
        ("show_opengraph_image", "on"),
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

async fn phase_ui(c: &mut Checks, pg: &Pg, skip_live: bool, hold: bool) -> Result<(), String> {
    pg.create_db("stage5_ui").await?;
    let admin = farsight_api::auth::generate(farsight_api::auth::ADMIN_PREFIX);
    // The harness's PLC URL is one the safe client refuses (loopback), so
    // synthetic DIDs never reach the real directory.
    let (_server, mut h) = start(pg, "stage5_ui", "ui", &admin, "https://127.0.0.1:9").await?;
    let paused = Arc::new(AtomicBool::new(false));
    let keeper = spawn_firehose_keeper(h.pool.clone(), paused.clone());
    tokio::time::sleep(Duration::from_secs(3)).await;
    let w = seed_world(&h).await?;
    h.refresh().await?;
    h.login().await?;

    check_gates_off(c, &h, &w).await?;
    check_exit_on_invalid(c, &pg.url("stage5_ui"))?;
    check_enable_flow(c, &mut h, &w).await?;
    check_settings_refusals(c, &mut h).await?;
    check_routes(c, &h, &w).await?;
    check_live_sections(c, &h, &w).await?;
    check_history(c, &h, &w).await?;
    check_coverage(c, &mut h, &w, &paused).await?;
    check_cache_and_headers(c, &h, &w).await?;
    check_independence(c, &h, &w).await?;
    check_opengraph(c, &mut h, &w).await?;

    // A second server on the same database with the real PLC directory,
    // for the two checks that need a real handle.
    let live = if skip_live {
        None
    } else {
        let (server, mut l) =
            start(pg, "stage5_ui", "live", &admin, "https://plc.directory").await?;
        l.login().await?;
        l.set(&[("enabled", Some("on"))]).await?;
        Some((server, l))
    };
    let live_h = live.as_ref().map(|(_, l)| l);
    check_search(c, &h, live_h, &w).await?;
    check_handles(c, &h, live_h, &w).await?;
    drop(live);

    check_withheld(c, &mut h, &w).await?;
    check_keys(c, &mut h, &w).await?;
    check_limits(c, &mut h, &w).await?;
    check_off_and_raw_editor(c, &mut h, &w).await?;
    check_metrics(c, &h).await?;
    c.section("4b. rendered times");
    check_no_relative_time(c, &h);
    if hold {
        h.set(&[("show_outgoing_blocks", Some("on"))]).await?;
        println!("== holding: {} (admin password {PASSWORD})", h.base);
        println!("   account {}/public/did/{}", h.base, w.s);
        println!("   list    {}{}", h.base, w.list);
        println!("   withheld {}/public/did/{}", h.base, w.e);
        let _ = tokio::signal::ctrl_c().await;
    }
    keeper.abort();
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let keep = std::env::args().any(|a| a == "--keep");
    let skip_live = std::env::args().any(|a| a == "--skip-live");
    let hold = std::env::args().any(|a| a == "--hold");
    let only: Option<String> = std::env::args().skip_while(|a| a != "--only").nth(1);
    println!("== farsight stage-5 harness: Mode A (public UI)");
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
        if only.as_deref().is_none_or(|o| o == "store") {
            store::run(&mut c, &pg).await?;
        }
        if only.as_deref().is_none_or(|o| o == "ui") {
            phase_ui(&mut c, &pg, skip_live, hold).await?;
        }
        Ok::<(), String>(())
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
