//! `farsight-stage8-harness`: integration tests of tables that sort by
//! their rows' shown time, the four sort indexes and their background
//! build, queue-driven handle warming, and the admin tables (sticky
//! header, handles, profile cards, record cells, columns).
//!
//! Sections:
//!
//! - 1–6: sort key and clamp (order, future dates, missing dates, paging
//!   through ties, index use, page numbers);
//! - 7–8: public pages (no Record column; handles after warming);
//! - 9–15: admin pages (sticky header and cards in a browser, handles,
//!   `/admin/card/{did}` with and without a session, record cells, columns,
//!   the shared card budget);
//! - 16–20: handle warming (queue, drain, cap, toggle, no duplicates),
//!   and 20b: handles stored in `handle_cache`;
//! - 21–23: the index build (in the background, held by the storage
//!   budget, released);
//! - 24: what a v2 Jetstream's `time` field is (reported, never a
//!   failure);
//! - 27–28: write cost of the four indexes; plans of representative
//!   queries.
//!
//! Sessions are created in the database, as in the stage-6 harness; the
//! sign-in itself is the stage-7 harness's subject.
//!
//! Flags: `--browser` runs `scripts/stage8-browser-probes.mjs` in the
//! Playwright image; `--skip-live` leaves out what needs the live network
//! (verified handles come from the real PLC directory and DNS);
//! `--jetstream URL` names the v2 Jetstream section 24 reads from (default
//! `ws://127.0.0.1:16008`); `--keep` keeps the Postgres container.

mod plc;
#[allow(dead_code)]
#[path = "../stage3_harness/seed.rs"]
mod seed;
#[allow(dead_code)]
#[path = "../stage3_harness/support.rs"]
mod support;

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use farsight_core::record::BlockRecord;
use farsight_core::{Collection, Did, Record, RecordKey};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Write, WriteAction};
use farsight_storage::counters::CounterSink;
use farsight_storage::ids::{ActorId, ListId, Stamp};
use farsight_storage::keys::Limits;
use farsight_storage::txn::Gates;
use farsight_storage::ui_rows::{self, Filter, Order, Section, SectionKey};
use serde_json::Value;
use sqlx::PgPool;

use crate::plc::Plc;
use crate::seed::did;
use crate::support::{
    ADMIN_DID, Checks, Http, Pg, Resp, admin_session, csrf_of, enc, farsight_bin, free_port,
};

const HOSTNAME: &str = "farsight.test";
/// TEST-NET-2: an address the safe outbound client treats as public.
const STANDIN_SUBNET: &str = "198.51.100.0/24";
const STANDIN_ADDR: &str = "198.51.100.1";
const LIVE_PLC: &str = "https://plc.directory";
/// Accounts of the live network with long-standing handles.
const LIVE: [(&str, &str); 3] = [
    ("did:plc:z72i7hdynmk6r22z27h6tvur", "bsky.app"),
    ("did:plc:ewvi7nxzyoun6zhxrhs64oiz", "atproto.com"),
    ("did:plc:ragtjsm2j2vknwkz3zp4oxrd", "pfrazee.com"),
];
const VIEWER: &str = "https://viewer.example/at/{authority}/{collection}/{rkey}";
const JETSTREAM: &str = "ws://127.0.0.1:16008";
const BROWSER_IMAGE: &str = "mcr.microsoft.com/playwright:v1.48.0-jammy";
const BROWSER_SCRIPT: &str = include_str!("../../../../../scripts/stage8-browser-probes.mjs");
/// The instant the seeded rows are dated around.
const BASE: &str = "'2026-09-01T00:00:00Z'::timestamptz";
const SHORT_CARD: &str = "Profile not available right now.";

/// SQL for [`did`] of a generate_series column.
fn did_sql(prefix: &str, col: &str) -> String {
    format!(
        "'did:plc:{prefix}' || translate(lpad({col}::text, 21, '0'), '0123456789', 'abcdefghij')"
    )
}

// ------------------------------------------------------------------ servers

struct Cfg<'a> {
    dsn: &'a str,
    plc: &'a str,
    budget: u64,
    public_ui: &'a str,
}

/// A `config.toml` and the metrics address it names.
fn config_toml(c: &Cfg<'_>) -> (String, String) {
    let metrics = format!("127.0.0.1:{}", free_port().unwrap_or(0));
    let text = format!(
        r#"[server]
hostname = "{HOSTNAME}"
contact = "mailto:ops@farsight.test"

[storage]
database_url = "{}"
budget_bytes = {}

[firehose]
urls = ["ws://127.0.0.1:9"]

[backfill]
plc_url = "{}"

[net]
allow_http_hosts = ["{STANDIN_ADDR}"]

[access]
reads = "public"
admin_ui = true
admin_did = "{ADMIN_DID}"
public_ui = true

[public_ui]
show_outgoing_blocks = true
rate_limit_rps = 1000
rate_limit_burst = 10000
handle_rps = 2
{}

[auth]
admin_token_sha256 = "{}"

[metrics]
bind = "{metrics}"
"#,
        c.dsn,
        c.budget,
        c.plc,
        c.public_ui,
        farsight_api::auth::hex(&farsight_api::auth::sha256("stage8-admin-token")),
    );
    (text, format!("http://{metrics}"))
}

/// One `farsight` process and the harness's ways into it.
struct Srv {
    base: String,
    metrics: String,
    dir: PathBuf,
    child: std::process::Child,
    /// How long it took from spawn to a live answer.
    came_up: Duration,
    next_ip: AtomicU32,
    admin: Http,
}

impl Srv {
    async fn start(
        name: &str,
        (text, metrics): (String, String),
        env: &[(&str, &str)],
    ) -> Result<Srv, String> {
        let dir =
            std::env::temp_dir().join(format!("farsight-stage8-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        std::fs::write(dir.join("config.toml"), text).map_err(|e| e.to_string())?;
        Srv::launch(&dir, metrics, env).await
    }

    /// Starts `farsight` on `dir` as it is.
    async fn launch(dir: &Path, metrics: String, env: &[(&str, &str)]) -> Result<Srv, String> {
        let port = free_port()?;
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("server.log"))
            .map_err(|e| e.to_string())?;
        let log2 = log.try_clone().map_err(|e| e.to_string())?;
        let mut cmd = Command::new(farsight_bin());
        cmd.env("FARSIGHT_CONFIG", dir.join("config.toml"))
            .env("FARSIGHT__SERVER__BIND", format!("127.0.0.1:{port}"))
            .env(
                "RUST_LOG",
                "info,sqlx=warn,hyper=warn,reqwest=warn,hickory=warn,rustls=warn",
            )
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log2));
        for (k, v) in env {
            cmd.env(k, v);
        }
        let started = Instant::now();
        let child = cmd.spawn().map_err(|e| format!("spawning farsight: {e}"))?;
        let mut s = Srv {
            base: format!("http://127.0.0.1:{port}"),
            metrics,
            dir: dir.to_owned(),
            child,
            came_up: Duration::ZERO,
            next_ip: AtomicU32::new(0),
            admin: Http::new(Some("127.0.0.250".parse().expect("ip"))),
        };
        let http = Http::new(None);
        loop {
            if let Ok(r) = http.get(&format!("{}/livez", s.base), &[]).await {
                if r.status == 200 {
                    s.came_up = started.elapsed();
                    return Ok(s);
                }
            }
            if started.elapsed() > Duration::from_secs(90) {
                return Err(format!("farsight did not come up: {}", s.log_tail(15)));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default()
    }

    fn log_tail(&self, n: usize) -> String {
        let l = self.log();
        let lines: Vec<&str> = l.lines().collect();
        lines[lines.len().saturating_sub(n)..].join("\n")
    }

    /// A client with a source address no request of this server has used.
    fn fresh(&self) -> Http {
        let n = self.next_ip.fetch_add(1, Ordering::Relaxed);
        let ip: IpAddr = format!("127.{}.{}.{}", 8 + n / 62_500, (n / 250) % 250, 1 + n % 250)
            .parse()
            .expect("ip");
        Http::new(Some(ip))
    }

    /// `GET path` anonymously, from a fresh address.
    async fn get(&self, path: &str) -> Result<Resp, String> {
        self.fresh().get(&format!("{}{path}", self.base), &[]).await
    }

    /// `GET path` with a session cookie.
    async fn admin_get(&self, cookie: &str, path: &str) -> Result<Resp, String> {
        self.admin
            .get(&format!("{}{path}", self.base), &[("cookie", cookie)])
            .await
    }

    async fn metrics_text(&self) -> Result<String, String> {
        Ok(Http::new(None)
            .get(&format!("{}/metrics", self.metrics), &[])
            .await?
            .text)
    }

    async fn gauge(&self, name: &str) -> Result<f64, String> {
        Ok(metric(&self.metrics_text().await?, name, &[]))
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Srv {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Bridge(String);

impl Bridge {
    /// A bridge network that puts [`STANDIN_ADDR`] on this host.
    fn create() -> Result<Bridge, String> {
        let name = format!("farsight-stage8-{}", std::process::id());
        support::run(Command::new("docker").args([
            "network",
            "create",
            "--subnet",
            STANDIN_SUBNET,
            "--gateway",
            STANDIN_ADDR,
            &name,
        ]))?;
        Ok(Bridge(name))
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["network", "rm", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

// ------------------------------------------------------------------ parsing

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

/// Warming outcomes that mean the worker finished with a DID.
fn processed(text: &str) -> f64 {
    ["resolved", "unverified", "failed", "cached"]
        .iter()
        .map(|o| metric(text, "farsight_handle_warming_total", &[("outcome", o)]))
        .sum()
}

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

/// The inside of the `<section>` of a public page whose id is `id`,
/// whatever other attributes its tag carries.
fn section<'a>(html: &'a str, id: &str) -> Option<&'a str> {
    let open = format!("<section id=\"{id}\"");
    let at = html.find(&open)?;
    let a = at + html[at..].find('>')? + 1;
    let b = html[a..].find("</section>")? + a;
    Some(&html[a..b])
}

/// The card of an admin lookup page whose heading is `title`.
fn admin_section<'a>(html: &'a str, title: &str) -> Option<&'a str> {
    // The DID lookup page keeps its tables in sections behind tabs.
    let id = match title {
        "Incoming blocks" => Some("blocks"),
        "Incoming listblocks" => Some("listblocks"),
        "Lists naming this account" => Some("lists"),
        // The list lookup page's.
        "Members" => Some("members"),
        "Subscribers" => Some("subscribers"),
        _ => None,
    };
    if let Some(found) = id.and_then(|id| section(html, id)) {
        return Some(found);
    }
    let open = format!("<h2>{title}</h2>");
    let a = html.find(&open)? + open.len();
    let b = html[a..]
        .find("<div class=\"card\">")
        .map_or(html.len(), |x| x + a);
    Some(&html[a..b])
}

/// The accounts of a section's rows, in row order: the `title` of each
/// account link (public and admin rows alike).
fn row_dids(sec: &str) -> Vec<String> {
    between(sec, "<a class=\"who\" href=\"", ">")
        .into_iter()
        .filter_map(|a| {
            let i = a.find("title=\"")? + 7;
            let j = a[i..].find('"')? + i;
            Some(a[i..j].to_owned())
        })
        .collect()
}

/// The next page of a public section (the arrow of its page controls),
/// without the fragment.
fn next_of(sec: &str) -> Option<String> {
    let i = sec.find("<nav class=\"pager\"")?;
    let k = "rel=\"next nofollow\" href=\"";
    let a = sec[i..].find(k)? + i + k.len();
    let b = sec[a..].find('"')? + a;
    let href = sec[a..b].replace("&amp;", "&");
    Some(href.split('#').next().unwrap_or("").to_owned())
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

/// The next page of an admin section: the arrow of its page controls
/// (the DID lookup), or its "Next page" link (the list lookup).
fn admin_next(sec: &str) -> Option<String> {
    if sec.contains("<nav class=\"pager\"") {
        return next_of(sec);
    }
    let b = sec.find("\">Next page")?;
    let a = sec[..b].rfind("href=\"")? + 6;
    Some(sec[a..b].replace("&amp;", "&"))
}

/// The text of each `<th>` of `sec`, whatever attributes the cell carries.
fn heads(sec: &str) -> Vec<String> {
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
        out.push(rest[a + 1..b].to_owned());
        rest = &rest[b..];
    }
    out
}

/// Walks a public section through its pages.
async fn walk(s: &Srv, first: &str, id: &str) -> Result<(Vec<String>, usize), String> {
    let mut rows = Vec::new();
    let mut url = first.to_owned();
    let mut pages = 0;
    loop {
        let r = s.get(&url).await?;
        if r.status != 200 {
            return Err(format!("{url}: {}", r.short()));
        }
        let sec = section(&r.text, id).ok_or_else(|| format!("{url}: no #{id}"))?;
        rows.extend(row_dids(sec));
        pages += 1;
        match next_of(sec) {
            Some(n) if pages < 80 => url = n,
            _ => break,
        }
    }
    Ok((rows, pages))
}

/// Walks a section of an admin lookup page through its "Next page" links.
async fn admin_walk(
    s: &Srv,
    cookie: &str,
    first: &str,
    title: &str,
) -> Result<(Vec<String>, usize), String> {
    let mut rows = Vec::new();
    let mut url = first.to_owned();
    let mut pages = 0;
    loop {
        let r = s.admin_get(cookie, &url).await?;
        if r.status != 200 {
            return Err(format!("{url}: {}", r.short()));
        }
        let sec = admin_section(&r.text, title).ok_or_else(|| format!("{url}: no {title}"))?;
        rows.extend(row_dids(sec));
        pages += 1;
        match admin_next(sec) {
            Some(n) if pages < 80 => url = n,
            _ => break,
        }
    }
    Ok((rows, pages))
}

fn exactly_once(rows: &[String], want: usize) -> bool {
    rows.len() == want && rows.iter().collect::<BTreeSet<_>>().len() == want
}

// ----------------------------------------------------------------- the world

type Stored = (
    String,
    i64,
    String,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);

/// The accounts a section should list, in shown-time order, computed here
/// from the stored columns — not by the expression the server sorts by.
async fn expected(
    pool: &PgPool,
    section: Section,
    key: i64,
    visible_only: bool,
) -> Result<Vec<String>, String> {
    let (table, key_col, party) = match section {
        Section::IncomingBlocks => ("blocks", "subject_id", "author_id"),
        Section::OutgoingBlocks => ("blocks", "author_id", "subject_id"),
        Section::ListBlockers => ("list_blocks", "list_id", "author_id"),
        Section::ListMembers => ("list_items", "list_id", "subject_id"),
    };
    let hidden = if visible_only {
        "AND a.status NOT IN (1, 2, 3, 4)"
    } else {
        ""
    };
    let mut rows: Vec<Stored> = sqlx::query_as(&format!(
        "SELECT a.did, b.{party}, b.rkey, b.created_at, b.first_seen
         FROM {table} b JOIN actors a ON a.id = b.{party} WHERE b.{key_col} = $1 {hidden}"
    ))
    .bind(key)
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())?;
    let shown = |r: &Stored| match (r.3, r.4) {
        (Some(c), Some(f)) => Some(c.min(f)),
        (c, f) => c.or(f),
    };
    let by_party = section.ties_by_party();
    rows.sort_by(|x, y| {
        // Newest first; then the rest of the primary key, descending.
        shown(y)
            .cmp(&shown(x))
            .then_with(|| {
                if by_party {
                    y.1.cmp(&x.1)
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .then_with(|| y.2.as_bytes().cmp(x.2.as_bytes()))
    });
    Ok(rows.into_iter().map(|r| r.0).collect())
}

/// The seeded accounts and lists of the main database.
struct World {
    /// Subject with 305 blocks of mixed dates.
    s: String,
    s_id: i64,
    /// Subject of the future-date records.
    t2: String,
    t2_id: i64,
    spam: String,
    /// Subject blocked by 120 accounts at one instant.
    t4: String,
    t4_id: i64,
    /// Author of 120 blocks at one instant.
    o4: String,
    o4_id: i64,
    /// A list with 120 members and 120 listblocks at one instant.
    l4_path: String,
    l4_uri: String,
    l4_id: i64,
    /// Subjects for the warming probes.
    w16: String,
    w18: String,
    w18_id: i64,
    w19: String,
    w20: String,
    w21: String,
    /// An author of 3,000 blocks; a list with 20,000 members and listblocks.
    dense_author_id: i64,
    filler_list_id: i64,
    filler_path: String,
    sparse_id: i64,
}

async fn n(pool: &PgPool, q: &str) -> Result<i64, String> {
    sqlx::query_scalar(q)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("{e}: {q}"))
}

async fn seed_list(pool: &PgPool, owner_id: i64, rkey: &str, items: i64) -> Result<i64, String> {
    sqlx::query_scalar(
        "INSERT INTO lists (owner_id, rkey, record_state, purpose, name, listblock_count,
                            track_state, item_count, admitted_at, fetched_at, fetched_witness)
         VALUES ($1, $2, 1, 1, $2, $3, 2, $3, now() - interval '1 hour', now() - interval '1 hour',
                 now() - interval '1 hour') RETURNING id",
    )
    .bind(owner_id)
    .bind(rkey)
    .bind(items as i32)
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())
}

/// `count` accounts with `prefix`, each blocking `subject` once at `when`
/// (an SQL expression over `g`), stored at `seen`.
async fn seed_blockers(
    pool: &PgPool,
    prefix: &str,
    count: i64,
    subject: i64,
    when: &str,
    seen: &str,
) -> Result<(), String> {
    seed::exec(
        pool,
        &format!(
            "INSERT INTO actors (did) SELECT {} FROM generate_series(1, {count}) g ON CONFLICT DO NOTHING",
            did_sql(prefix, "g")
        ),
    )
    .await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             SELECT a.id, '3k{prefix}' || lpad(g::text, 8, '0'), {subject}, {when}, 1, {seen}, now()
             FROM generate_series(1, {count}) g JOIN actors a ON a.did = {}",
            did_sql(prefix, "g")
        ),
    )
    .await?;
    Ok(())
}

async fn seed_world(pool: &PgPool) -> Result<World, String> {
    let actor = |d: String| async move {
        let id = seed::actor(pool, &d).await?;
        Ok::<(String, i64), String>((d, id))
    };
    // S: five kinds of row — no stated date and stored lately; a date in
    // the future; an honest date; a date long before it was stored; no
    // stated date and stored long ago.
    let (s, s_id) = actor(did("sub", 1)).await?;
    let when = format!(
        "CASE g % 5 WHEN 0 THEN NULL
                    WHEN 1 THEN '9999-12-31T00:00:00Z'::timestamptz
                    WHEN 2 THEN {BASE} - make_interval(mins => g) - interval '1 day'
                    WHEN 3 THEN {BASE} - make_interval(hours => g)
                    ELSE NULL END"
    );
    let seen = format!(
        "CASE g % 5 WHEN 3 THEN {BASE}
                    WHEN 4 THEN {BASE} - interval '20 days' - make_interval(mins => g)
                    ELSE {BASE} - make_interval(mins => g) END"
    );
    seed_blockers(pool, "sbl", 300, s_id, &when, &seen).await?;
    // One account with several records naming S, at one instant.
    seed::exec(
        pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             SELECT a.id, '3kdup' || g, {s_id}, {BASE} - interval '2 hours', 1, {BASE} - interval '1 hour', now()
             FROM generate_series(1, 5) g JOIN actors a ON a.did = '{}'",
            did("sbl", 7)
        ),
    )
    .await?;

    // `t2`: its future-date records go through the real write path later.
    let (t2, t2_id) = actor(did("tfu", 1)).await?;
    let spam = did("spm", 1);

    // Ties: one instant for every row.
    let (t4, t4_id) = actor(did("tie", 1)).await?;
    seed_blockers(pool, "tib", 120, t4_id, BASE, BASE).await?;
    let (o4, o4_id) = actor(did("out", 1)).await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO actors (did) SELECT {} FROM generate_series(1, 120) g ON CONFLICT DO NOTHING",
            did_sql("ots", "g")
        ),
    )
    .await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             SELECT {o4_id}, '3kout' || lpad(g::text, 6, '0'), a.id, {BASE}, 1, {BASE}, now()
             FROM generate_series(1, 120) g JOIN actors a ON a.did = {}",
            did_sql("ots", "g")
        ),
    )
    .await?;
    let (lo, lo_id) = actor(did("lso", 1)).await?;
    let l4_id = seed_list(pool, lo_id, "ties", 120).await?;
    for (prefix, n) in [("lim", 120), ("llb", 120)] {
        seed::exec(
            pool,
            &format!(
                "INSERT INTO actors (did) SELECT {} FROM generate_series(1, {n}) g ON CONFLICT DO NOTHING",
                did_sql(prefix, "g")
            ),
        )
        .await?;
    }
    seed::exec(
        pool,
        &format!(
            "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev, first_seen, last_seen)
             SELECT {lo_id}, '3kitm' || lpad(g::text, 6, '0'), {l4_id}, a.id, {BASE}, 1, {BASE}, now()
             FROM generate_series(1, 120) g JOIN actors a ON a.did = {}",
            did_sql("lim", "g")
        ),
    )
    .await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO list_blocks (author_id, rkey, list_id, counted, created_at, rev, first_seen, last_seen)
             SELECT a.id, '3klbk' || lpad(g::text, 6, '0'), {l4_id}, true, {BASE}, 1, {BASE}, now()
             FROM generate_series(1, 120) g JOIN actors a ON a.did = {}",
            did_sql("llb", "g")
        ),
    )
    .await?;

    // Warming subjects: accounts no page has shown yet.
    let recent = format!("{BASE} - make_interval(secs => g)");
    let (w16, w16_id) = actor(did("wsx", 16)).await?;
    seed_blockers(pool, "wxa", 50, w16_id, &recent, &recent).await?;
    let (w18, w18_id) = actor(did("wsx", 18)).await?;
    seed_blockers(pool, "wxb", 2300, w18_id, &recent, &recent).await?;
    let (w19, w19_id) = actor(did("wsx", 19)).await?;
    seed_blockers(pool, "wxc", 20, w19_id, &recent, &recent).await?;
    let (w20, w20_id) = actor(did("wsx", 20)).await?;
    seed_blockers(pool, "wxd", 10, w20_id, &recent, &recent).await?;
    // Stored handles: one verified today, one verified eight days ago.
    let (w21, w21_id) = actor(did("wsx", 21)).await?;
    seed_blockers(pool, "wxe", 3, w21_id, &recent, &recent).await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO handle_cache (did, handle, resolved_at) VALUES
             ('{}', 'stored-fresh.example', now() - interval '1 day'),
             ('{}', 'stored-stale.example', now() - interval '8 days')",
            did("wxe", 1),
            did("wxe", 2)
        ),
    )
    .await?;
    // The same account twice on one page.
    seed::exec(
        pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             SELECT a.id, '3ktwice', {w20_id}, {BASE}, 1, {BASE}, now() FROM actors a WHERE a.did = '{}'",
            did("wxd", 1)
        ),
    )
    .await?;

    // Filler, so that the planner has tables worth an index: 50,000 blocks
    // among 5,000 authors and 500 subjects, a dense author, a sparse
    // subject, and a list with 20,000 members and 20,000 listblocks.
    for (prefix, n) in [("fla", 5000), ("fls", 500), ("flm", 20_000)] {
        seed::exec(
            pool,
            &format!(
                "INSERT INTO actors (did) SELECT {} FROM generate_series(1, {n}) g ON CONFLICT DO NOTHING",
                did_sql(prefix, "g")
            ),
        )
        .await?;
    }
    seed::exec(
        pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             SELECT a.id, '3kfil' || lpad(g::text, 8, '0'), s.id, {BASE} - make_interval(secs => g), 1,
                    {BASE} - make_interval(secs => g), now()
             FROM generate_series(1, 50000) g
             JOIN actors a ON a.did = {}
             JOIN actors s ON s.did = {}",
            did_sql("fla", "(1 + g % 5000)"),
            did_sql("fls", "(1 + g % 500)")
        ),
    )
    .await?;
    let (_, dense_author_id) = actor(did("dns", 1)).await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             SELECT {dense_author_id}, '3kdns' || lpad(g::text, 8, '0'), s.id, {BASE} - make_interval(secs => g), 1,
                    {BASE} - make_interval(secs => g), now()
             FROM generate_series(1, 3000) g JOIN actors s ON s.did = {}",
            did_sql("fls", "(1 + g % 500)")
        ),
    )
    .await?;
    let (_, sparse_id) = actor(did("spr", 1)).await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             SELECT a.id, '3ksparse', {sparse_id}, {BASE}, 1, {BASE}, now() FROM actors a WHERE a.did = '{}'",
            did("fla", 1)
        ),
    )
    .await?;
    let (_, fo_id) = actor(did("flo", 1)).await?;
    let filler_list_id = seed_list(pool, fo_id, "filler", 20_000).await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO list_items (owner_id, rkey, list_id, subject_id, created_at, rev, first_seen, last_seen)
             SELECT {fo_id}, '3kfit' || lpad(g::text, 8, '0'), {filler_list_id}, a.id,
                    {BASE} - make_interval(secs => g), 1, {BASE} - make_interval(secs => g), now()
             FROM generate_series(1, 20000) g JOIN actors a ON a.did = {}",
            did_sql("flm", "g")
        ),
    )
    .await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO list_blocks (author_id, rkey, list_id, counted, created_at, rev, first_seen, last_seen)
             SELECT a.id, '3kflb' || lpad(g::text, 8, '0'), {filler_list_id}, true,
                    {BASE} - make_interval(secs => g), 1, {BASE} - make_interval(secs => g), now()
             FROM generate_series(1, 20000) g JOIN actors a ON a.did = {}",
            did_sql("flm", "g")
        ),
    )
    .await?;
    seed::exec(pool, "ANALYZE").await?;
    Ok(World {
        s,
        s_id,
        t2,
        t2_id,
        spam,
        t4,
        t4_id,
        o4,
        o4_id,
        l4_path: format!("/list/{lo}/ties"),
        l4_uri: format!("at://{lo}/app.bsky.graph.list/ties"),
        l4_id,
        w16,
        w18,
        w18_id,
        w19,
        w20,
        w21,
        dense_author_id,
        filler_list_id,
        filler_path: format!("/list/{}/filler", did("flo", 1)),
        sparse_id,
    })
}

fn public_did(d: &str) -> String {
    format!("/did/{d}")
}

fn lookup_did(d: &str) -> String {
    format!("/admin/lookup/did?q={}", enc(d))
}

fn lookup_list(uri: &str) -> String {
    format!("/admin/lookup/list?q={}", enc(uri))
}

// ------------------------------------------------- 1–6. sort key and clamp

async fn check_order(
    c: &mut Checks,
    a: &Srv,
    pool: &PgPool,
    cookie: &str,
    w: &World,
) -> Result<(), String> {
    c.section("1. rows sort by shown time: LEAST(created_at, first_seen), newest first");
    let want = expected(pool, Section::IncomingBlocks, w.s_id, true).await?;
    let (got, pages) = walk(a, &public_did(&w.s), "blockers").await?;
    c.check(
        "the public \"Blocked by\" table lists 305 records with stated dates in the future, in the past and missing in exactly the order of the shown time, then blocker id, then record key, all descending",
        got == want && got.len() == 305,
        format!("{} rows over {pages} pages; first differing position: {:?}", got.len(), got.iter().zip(&want).position(|(x, y)| x != y)),
    );
    let first = a.get(&public_did(&w.s)).await?;
    let sec = section(&first.text, "blockers").unwrap_or("");
    let at = |n: i64| format!("{}?page={n}", public_did(&w.s));
    let middle = a.get(&at(4)).await?;
    let last = a.get(&at(7)).await?;
    let past = a.get(&at(8)).await?;
    let (mid_sec, last_sec, past_sec) = (
        section(&middle.text, "blockers").unwrap_or(""),
        section(&last.text, "blockers").unwrap_or(""),
        section(&past.text, "blockers").unwrap_or(""),
    );
    c.check(
        "\"Blocked by\" is 50 rows a page with numbered page controls, plain links with the page in the query: 305 records are seven pages; the controls list the pages (a run of up to twenty-five around the current one, with the first and last page and a gap where pages are left out; the page's script hides what does not fit the row) and arrows that are not links at the ends; page 4 is rows 151–200 of the order; a page past the end is answered with the last page",
        pages == 7
            && row_dids(sec).len() == 50
            && controls_of(sec) == "(←) [1] 2 3 4 5 6 7 →"
            && next_of(sec) == Some(at(2))
            && controls_of(mid_sec) == "← 1 2 3 [4] 5 6 7 →"
            && row_dids(mid_sec) == want[150..200]
            && controls_of(last_sec) == "← 1 2 3 4 5 6 [7] (→)"
            && row_dids(last_sec).len() == 5
            && past.status == 303
            && past.header("location").as_deref() == Some(at(7).as_str())
            && past_sec.is_empty()
            && !sec.contains("hx-")
            && !sec.contains("Load more"),
        format!("{pages} pages; {:?} / {:?} / {:?} / {:?}", controls_of(sec), controls_of(mid_sec), controls_of(last_sec), controls_of(past_sec)),
    );
    let dense = a.get(&format!("{}?page=20", w.filler_path)).await?;
    let deep = a.get(&format!("{}?page=400", w.filler_path)).await?;
    let (dense_sec, deep_sec) = (
        section(&dense.text, "members").unwrap_or(""),
        section(&deep.text, "members").unwrap_or(""),
    );
    c.check(
        "a long section shows its real last page and stays reachable to the end: on page 20 of a 20,000-member list the controls name page 400, and page 400 holds the last 50 rows and closes the list",
        row_dids(dense_sec).len() == 50
            && controls_of(dense_sec)
                == "← 1 … 8 9 10 11 12 13 14 15 16 17 18 19 [20] 21 22 23 24 25 26 27 28 29 30 31 32 … 400 →"
            && row_dids(deep_sec).len() == 50
            && controls_of(deep_sec)
                == "← 1 … 376 377 378 379 380 381 382 383 384 385 386 387 388 389 390 391 392 393 394 395 396 397 398 399 [400] (→)",
        format!("{:?} / {:?}", controls_of(dense_sec), controls_of(deep_sec)),
    );
    let (admin, _) = admin_walk(a, cookie, &lookup_did(&w.s), "Incoming blocks").await?;
    c.check(
        "the admin \"Incoming blocks\" table is in the same order",
        admin == want,
        format!("{} rows", admin.len()),
    );
    let honest = did("sbl", 2);
    let undated = did("sbl", 5);
    let long_ago = did("sbl", 4);
    let pos = |d: &str| got.iter().position(|x| x == d);
    c.section("3. a missing created_at falls back to first_seen");
    c.check(
        "a record that states no createdAt sorts at its first_seen: among the dated rows when it was stored lately, and after all of them when it was stored before any of their dates (the last 60 rows)",
        pos(&undated) < pos(&long_ago)
            && pos(&undated).is_some()
            && pos(&honest).is_some()
            && got.len() >= 60
            && got[got.len() - 60..].iter().cloned().collect::<BTreeSet<_>>()
                == (1..=300u64)
                    .filter(|g| g % 5 == 4)
                    .map(|g| did("sbl", g))
                    .collect::<BTreeSet<_>>(),
        format!("undated at {:?}, stored long ago at {:?}", pos(&undated), pos(&long_ago)),
    );
    Ok(())
}

async fn check_future_dates(
    c: &mut Checks,
    a: &Srv,
    pool: &PgPool,
    w: &World,
) -> Result<(), String> {
    c.section("2. a future createdAt does not pin a page (through the real write path)");
    let limits = Limits::defaults();
    let counters = CounterSink::new(8);
    let ctx = ApplyCtx {
        limits: &limits,
        gates: Gates::default(),
        counters: &counters,
    };
    let now = Utc::now();
    let far: DateTime<Utc> = "9999-12-31T00:00:00Z".parse().map_err(|_| "date")?;
    let stamp = now.timestamp_micros();
    let subject = Did::parse(&w.t2).map_err(|e| e.to_string())?;
    let block = |author: &str, rkey: String, i: i64, created: DateTime<Utc>, witness| Write {
        author: Did::parse(author).expect("did"),
        collection: Collection::Block,
        rkey: RecordKey::parse(&rkey).expect("rkey"),
        stamp: Stamp::new(stamp + i),
        witness: Some(witness),
        action: WriteAction::Upsert(Record::Block(BlockRecord {
            subject: subject.clone(),
            created_at: Some(created),
        })),
    };
    let mut writes = Vec::new();
    // 20 honest blocks three hours ago, then one account's 100 records
    // dated in the year 9999 two hours ago, then 30 honest blocks since.
    for i in 0..20 {
        let t = now - ChronoDuration::hours(3) - ChronoDuration::minutes(i);
        writes.push(block(
            &did("hoa", i as u64 + 1),
            format!("3khoa{i:06}"),
            i,
            t,
            t,
        ));
    }
    let arrived = now - ChronoDuration::hours(2);
    for i in 0..100 {
        writes.push(block(
            &w.spam,
            format!("3kspm{i:06}"),
            100 + i,
            far,
            arrived,
        ));
    }
    for i in 0..30 {
        let t = now - ChronoDuration::minutes(60 - i);
        writes.push(block(
            &did("hob", i as u64 + 1),
            format!("3khob{i:06}"),
            300 + i,
            t,
            t,
        ));
    }
    let mut b = Batch::new(Origin::Firehose);
    b.writes = writes;
    apply::apply(pool, &ctx, &b)
        .await
        .map_err(|e| e.to_string())?;
    let stored = n(
        pool,
        &format!(
            "SELECT count(*) FROM blocks b JOIN actors a ON a.id = b.author_id
             WHERE b.subject_id = {} AND a.did = '{}' AND b.created_at > now() + interval '1000 years'
               AND b.first_seen < now() - interval '119 minutes'",
            w.t2_id, w.spam
        ),
    )
    .await?;
    let (got, _) = walk(a, &public_did(&w.t2), "blockers").await?;
    let spam_at: Vec<usize> = got
        .iter()
        .enumerate()
        .filter(|(_, d)| **d == w.spam)
        .map(|(i, _)| i)
        .collect();
    let want = expected(pool, Section::IncomingBlocks, w.t2_id, true).await?;
    c.check(
        "100 block records from one account dated 9999-12-31 are stored with their stated date and the witness time as first_seen, and sit where they arrived: after the 30 blocks recorded since, before the 20 older ones — not at the top",
        stored == 100
            && got == want
            && got.len() == 150
            && spam_at == (30..130).collect::<Vec<_>>()
            && got[0] == did("hob", 30),
        format!("{stored} stored; spam rows at {:?}..{:?}; first row {}", spam_at.first(), spam_at.last(), got.first().map_or("", String::as_str)),
    );
    let page = a.get(&public_did(&w.t2)).await?;
    c.check(
        "the Created cell still shows what the author stated (the year 9999): only the order is clamped",
        section(&page.text, "blockers").is_some_and(|s| s.contains("<time datetime=\"9999-12-31T00:00:00Z\"")),
        "stated value shown",
    );
    Ok(())
}

async fn check_ties(c: &mut Checks, a: &Srv, pool: &PgPool, w: &World) -> Result<(), String> {
    c.section("4. paging through rows with one shown time");
    let mut ok = true;
    let mut detail = Vec::new();
    for (name, first, id, section_, key, want_pages) in [
        (
            "Blocked by (account)",
            public_did(&w.t4),
            "blockers",
            Section::IncomingBlocks,
            w.t4_id,
            3,
        ),
        (
            "Blocks by this account",
            public_did(&w.o4),
            "outgoing",
            Section::OutgoingBlocks,
            w.o4_id,
            3,
        ),
        (
            "Members",
            w.l4_path.clone(),
            "members",
            Section::ListMembers,
            w.l4_id,
            3,
        ),
        (
            "Blocked by (list)",
            w.l4_path.clone(),
            "subscribers",
            Section::ListBlockers,
            w.l4_id,
            3,
        ),
    ] {
        let (got, pages) = walk(a, &first, id).await?;
        let want = expected(pool, section_, key, true).await?;
        let good = exactly_once(&got, 120) && pages == want_pages && got == want;
        ok &= good;
        detail.push(format!(
            "{name}: {} rows, {} distinct, {pages} pages, {}",
            got.len(),
            got.iter().collect::<BTreeSet<_>>().len(),
            match got.iter().zip(&want).position(|(x, y)| x != y) {
                None if good => "exact".to_owned(),
                at => format!("WRONG (first differing position: {at:?})"),
            }
        ));
    }
    c.check(
        "120 rows with the same shown time in each of the four sections: every row exactly once, in the order of the tiebreak, across three pages of 50",
        ok,
        detail.join("; "),
    );
    Ok(())
}

/// The key of `section` for an id read from the database as a number.
fn section_key(section: Section, id: i64) -> SectionKey {
    match section {
        Section::IncomingBlocks | Section::OutgoingBlocks => ActorId::new(id).into(),
        Section::ListBlockers | Section::ListMembers => ListId::new(id).into(),
    }
}

async fn check_plans(c: &mut Checks, pool: &PgPool, w: &World) -> Result<(), String> {
    c.section("5. each section's query runs on its expression index");
    let mut conn = pool.acquire().await.map_err(|e| e.to_string())?;
    let mut ok = true;
    let mut detail = Vec::new();
    for (section_, key) in [
        (Section::IncomingBlocks, w.w18_id),
        (Section::OutgoingBlocks, w.dense_author_id),
        (Section::ListBlockers, w.filler_list_id),
        (Section::ListMembers, w.filler_list_id),
    ] {
        let filter = Filter {
            hide_inactive: true,
            show_suspended: false,
            show_taken_down: false,
            find: None,
            excluded: &[],
        };
        for offset in [0, 50] {
            let plan = ui_rows::explain(
                &mut conn,
                section_,
                section_key(section_, key),
                Order::Shown,
                filter,
                offset,
                50,
            )
            .await
            .map_err(|e| e.to_string())?;
            let text = plan.join("\n");
            let good = uses_index(&text, section_);
            ok &= good;
            detail.push(format!(
                "{}{}: {}",
                section_.index(),
                if offset > 0 { " (page 2)" } else { "" },
                if good { order_of(&text) } else { text.as_str() }
            ));
        }
    }
    c.check(
        "EXPLAIN ANALYZE of the four section queries, first page and second: each reads its table through the section's shown-time index, never by a sequential scan",
        ok,
        detail.join("; "),
    );
    Ok(())
}

/// Whether a plan reads `section`'s table through its sort index and
/// never by a sequential scan.
fn uses_index(plan: &str, section: Section) -> bool {
    plan.contains(&format!("using {} on {}", section.index(), section.table()))
        && !plan.contains(&format!("Seq Scan on {}", section.table()))
}

/// How a plan gets its order: from the index, or by sorting what the
/// index returned.
fn order_of(plan: &str) -> &'static str {
    if plan.contains("Sort Key") {
        "index scan, then a sort"
    } else {
        "index scan, in index order"
    }
}

async fn check_pages(c: &mut Checks, a: &Srv, cookie: &str, w: &World) -> Result<(), String> {
    c.section("6. page numbers");
    // The lookups page by number, each page with its own address.
    let filler_uri = format!("at://{}/app.bsky.graph.list/filler", did("flo", 1));
    let page = a.admin_get(cookie, &lookup_list(&filler_uri)).await?;
    let msec = admin_section(&page.text, "Members").unwrap_or("");
    let ssec = admin_section(&page.text, "Subscribers").unwrap_or("");
    let subs2 = a
        .admin_get(
            cookie,
            &format!("{}&tab=subscribers&subs=2", lookup_list(&filler_uri)),
        )
        .await?;
    let s2 = admin_section(&subs2.text, "Subscribers").unwrap_or("");
    c.check(
        "the admin list lookup pages by number: a list of 20,000 members and 20,000 subscribers has 400 pages of each, the next page of Members is &page=2 and of Subscribers &tab=subscribers&subs=2, which holds the next 50 and opens with the Subscribers tab in front",
        admin_next(msec).is_some_and(|n| n.ends_with("&page=2"))
            && admin_next(ssec).is_some_and(|n| n.ends_with("&tab=subscribers&subs=2"))
            && controls_of(msec).ends_with("… 400 →")
            && controls_of(ssec).ends_with("… 400 →")
            && row_dids(msec).len() == 50
            && row_dids(ssec).len() == 50
            && row_dids(s2).len() == 50
            && row_dids(s2)[0] != row_dids(ssec)[0]
            && subs2.text.contains("<div class=\"tabbed\" data-active=\"subscribers\">"),
        format!("{:?} / {:?}", controls_of(msec), controls_of(ssec)),
    );
    let public = a.get(&public_did(&w.s)).await?;
    let public_next = section(&public.text, "blockers")
        .and_then(next_of)
        .unwrap_or_default();
    let zero = a.get(&format!("{}?page=0", public_did(&w.s))).await?;
    c.check(
        "the public tables page by number: the next page is ?page=2; a page number that is not one gets the 400 page with its \"Open the first page\" link — never a 500",
        public_next == format!("{}?page=2", public_did(&w.s))
            && zero.status == 400
            && zero.text.contains("Open the first page")
            && zero.text.contains(&format!("href=\"{}\"", public_did(&w.s))),
        format!("{public_next}; {}", zero.status),
    );
    let did_page = a.admin_get(cookie, &lookup_did(&w.s)).await?;
    let dsec = admin_section(&did_page.text, "Incoming blocks").unwrap_or("");
    let second = a
        .admin_get(cookie, &format!("{}&page=2", lookup_did(&w.s)))
        .await?;
    let ssec = admin_section(&second.text, "Incoming blocks").unwrap_or("");
    c.check(
        "the admin DID lookup pages by number like the public tables, with its own address: 305 blocks are seven pages, the next page is &page=2 and holds the next 50",
        admin_next(dsec).is_some_and(|n| n.ends_with("&page=2"))
            && controls_of(dsec) == "(←) [1] 2 3 4 5 6 7 →"
            && row_dids(dsec).len() == 50
            && controls_of(ssec) == "← 1 [2] 3 4 5 6 7 →"
            && row_dids(ssec).len() == 50
            && row_dids(ssec)[0] != row_dids(dsec)[0],
        format!("{:?} / {:?}", controls_of(dsec), controls_of(ssec)),
    );
    Ok(())
}

// ------------------------------------------------- 7b. the History tab

async fn check_history_tab(c: &mut Checks, a: &Srv, plc: &Plc, w: &World) -> Result<(), String> {
    c.section("7b. the History tab reads the PLC log only when asked");
    let before = plc.audit_total();
    let plain = a.get(&public_did(&w.s)).await?;
    let after_plain = plc.audit_total();
    let asked = a.get(&format!("{}?tab=history", public_did(&w.s))).await?;
    let after_asked = plc.audit_total();
    let unknown = a
        .get(&format!("{}?tab=history", public_did(&did("unk", 404))))
        .await?;
    let sec = section(&asked.text, "history").unwrap_or("");
    c.check(
        "the account page offers a History tab as a link (?tab=history) and reads nothing for it; asked for, the page reads the account's PLC audit log once and shows the History table with the tab marked — here a log that names no handle and no host; an account this instance does not hold has no such tab and causes no request",
        plain.status == 200
            && plain.text.contains(&format!(
                "href=\"{}?tab=history\" data-tab=\"history\"",
                public_did(&w.s)
            ))
            && section(&plain.text, "history").is_none()
            && after_plain == before
            && asked.status == 200
            && asked.text.contains("<div class=\"tabbed\" data-active=\"history\">")
            && sec.contains("<h2>History</h2>")
            && sec.matches("None recorded.").count() == 2
            && after_asked == before + 1
            && unknown.status == 200
            && section(&unknown.text, "history").is_none()
            && !unknown.text.contains("data-tab=\"history\"")
            && plc.audit_total() == after_asked,
        format!(
            "audit requests: {before} → {after_plain} → {after_asked} → {}",
            plc.audit_total()
        ),
    );
    c.section("7c. the filter box of an account page's tables");
    let base = public_did(&w.w21);
    let whole = a.get(&base).await?;
    let (fresh, stale, none) = (did("wxe", 1), did("wxe", 2), did("wxe", 3));
    let by_handle = a.get(&format!("{base}?find=Stored-FR")).await?;
    let by_did = a.get(&format!("{base}?find={none}")).await?;
    let nothing = a.get(&format!("{base}?find=no%25such_thing")).await?;
    let empty = a.get(&format!("{base}?find=&page=1")).await?;
    let padded = a.get(&format!("{base}?find=+stored+")).await?;
    let sec = |r: &Resp| section(&r.text, "blockers").unwrap_or("").to_owned();
    let (hs, ds, ns) = (sec(&by_handle), sec(&by_did), sec(&nothing));
    c.check(
        "each table has a filter box; part of a handle (any case) keeps the rows of accounts whose stored handle contains it, a DID keeps that account's rows, and text that matches nothing — its % and _ taken literally — leaves no row and says so instead of \"none on record\"; the heading keeps the table's whole count, the note states the matches, and an empty or padded filter is redirected to the plain or trimmed address",
        sec(&whole).contains("<form class=\"find\" method=\"get\"")
            && sec(&whole).contains("name=\"find\" value=\"\"")
            && row_dids(&sec(&whole)).len() == 3
            && row_dids(&hs) == [fresh.clone()]
            && hs.contains("name=\"find\" value=\"Stored-FR\"")
            && hs.contains("<p class=\"find-note\" role=\"status\">1 match. Part of a handle")
            && hs.contains("<span class=\"count count-big\">3</span>")
            && row_dids(&ds) == [none.clone()]
            && ds.contains("<p class=\"find-note\" role=\"status\">1 match.</p>")
            && row_dids(&ns).is_empty()
            && ns.contains(">No match.")
            && !ns.contains("class=\"empty\"")
            && !row_dids(&hs).contains(&stale)
            && empty.status == 301
            && empty.header("location").as_deref() == Some(base.as_str())
            && padded.status == 301
            && padded.header("location").as_deref() == Some(format!("{base}?find=stored").as_str()),
        format!(
            "{:?} / {:?} / {:?}; {} {}",
            row_dids(&hs),
            row_dids(&ds),
            row_dids(&ns),
            empty.status,
            padded.status
        ),
    );
    Ok(())
}

// --------------------------------------------------------- 7. public pages

/// The admin DID lookup's History tab: the PLC log's handles and hosts
/// and the removed records, read only when the tab is asked for.
async fn check_admin_history_tab(
    c: &mut Checks,
    a: &Srv,
    pool: &PgPool,
    cookie: &str,
    plc: &Plc,
    w: &World,
) -> Result<(), String> {
    c.section("7c2. the admin DID lookup's History tab");
    let before = plc.audit_total();
    let plain = a.admin_get(cookie, &lookup_did(&w.s)).await?;
    let after_plain = plc.audit_total();
    let asked = a
        .admin_get(cookie, &format!("{}&tab=history", lookup_did(&w.s)))
        .await?;
    let after_asked = plc.audit_total();
    let sec = section(&asked.text, "history").unwrap_or("");
    let titles: Vec<&str> = between(sec, "<h3 class=\"history-title\">", "</h3>");
    let tabs: Vec<&str> = between(&plain.text, " data-tab=\"", "\"");
    let standalone = a
        .admin_get(cookie, &format!("/admin/did/{}/history", w.s))
        .await?;
    // An account with a history in its log: two handles and two hosts.
    let storied = did("abh", 1);
    seed::exec(
        pool,
        &format!("INSERT INTO actors (did) VALUES ('{storied}') ON CONFLICT DO NOTHING"),
    )
    .await?;
    plc.claim(&storied, "storied.example");
    let told = a
        .admin_get(cookie, &format!("{}&tab=history", lookup_did(&storied)))
        .await?;
    let tsec = section(&told.text, "history").unwrap_or("");
    c.check(
        "an account whose log has two operations: both handles and both hosts are listed, newest first, the newest marked current, the two tables in one pair so that they stand side by side",
        tsec.contains("<div class=\"history-pair\">")
            && tsec.matches("<div class=\"history-part\">").count() == 2
            && tsec.find("storied.example").zip(tsec.find("earlier-storied.example")).is_some_and(|(new, old)| new < old)
            && tsec.contains("earlier-host.example")
            && tsec.matches("held-current").count() == 2,
        format!("{} handles named", tsec.matches("storied.example").count()),
    );
    c.check(
        "the lookup page offers History as its last tab and reads nothing for it; asked for (&tab=history), the page reads the account's PLC audit log once and shows, in this order, Handle history, PDS history, Removed blocks and Removed lists — without the \"What this page covers\" block, which the account's own history page still has",
        tabs == ["blocks", "listblocks", "lists", "blockinglists", "history"]
            && section(&plain.text, "history").is_none()
            && after_plain == before
            && asked.status == 200
            && asked.text.contains("<div class=\"tabbed\" data-active=\"history\">")
            && titles == ["Handle history", "PDS history", "Removed blocks", "Removed lists"]
            && sec.contains("id=\"removed-blocks\"")
            && sec.contains("id=\"removed-memberships\"")
            && !asked.text.contains("What this page covers")
            && !asked.text.contains("id=\"limits\"")
            && after_asked == before + 1
            && standalone.status == 200
            && standalone.text.contains("id=\"limits\""),
        format!("tabs {tabs:?}; titles {titles:?}; audit requests {before} → {after_plain} → {after_asked}"),
    );
    Ok(())
}

/// A list's page shows the description and the image its record states,
/// from the stored row.
async fn check_list_about(c: &mut Checks, a: &Srv, pool: &PgPool, plc: &Plc) -> Result<(), String> {
    c.section("7d. a list's description and image");
    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let owner = did("abo", 1);
    seed::exec(
        pool,
        &format!("INSERT INTO actors (did) VALUES ('{owner}') ON CONFLICT DO NOTHING"),
    )
    .await?;
    let oid = n(
        pool,
        &format!("SELECT id FROM actors WHERE did = '{owner}'"),
    )
    .await?;
    let told = seed_list(pool, oid, "told", 0).await?;
    seed_list(pool, oid, "blank", 0).await?;
    sqlx::query("UPDATE lists SET description = $2, avatar_cid = $3 WHERE id = $1")
        .bind(told)
        .bind("First line <b>bold</b>\n\nhttps://example.com second\u{202e}")
        .bind(CID)
        .execute(pool)
        .await
        .map_err(|e| e.to_string())?;
    let page = |rkey: &str| format!("/list/{owner}/{rkey}");
    let r0 = plc.record_total();
    let first = a.get(&page("told")).await?;
    let blank = a.get(&page("blank")).await?;
    let r1 = plc.record_total();
    let want = "<p class=\"list-description\">First line &lt;b&gt;bold&lt;/b&gt;<br>https://example.com second</p>";
    let image = format!("data-list-image=\"{CID}\" data-owner=\"{owner}\"");
    c.check(
        "a list's page shows the description its record states as escaped plain text, line by line, with no link made of an address in it, and names the image by its CID for the page's script; a list whose record states neither has neither; both come from the stored row, and no record is read for the page",
        first.status == 200
            && first.text.contains(want)
            && first.text.contains(&image)
            && !first.text.contains("href=\"https://example.com")
            && blank.status == 200
            && !blank.text.contains("list-description")
            && !blank.text.contains("data-list-image")
            && r1 == r0,
        format!("getRecord {r0} → {r1}"),
    );
    Ok(())
}

async fn check_public_columns(c: &mut Checks, a: &Srv, w: &World) -> Result<(), String> {
    c.section("7. public tables have no Record column");
    let page = a.get(&public_did(&w.t4)).await?;
    let out = a.get(&public_did(&w.o4)).await?;
    let list = a.get(&w.l4_path).await?;
    let two = ["Account", "Created"];
    let clean = |r: &Resp| {
        !r.text.contains("class=\"record\"")
            && !r.text.contains("/app.bsky.graph.block/")
            && !r.text.contains("/app.bsky.graph.listblock/")
            && !r.text.contains(">Record<")
    };
    c.check(
        "\"Blocked by\" and \"Blocks by this account\" on the account page and \"Blocked by\" on the list page are Account and Created; Members is Account and Added; no public page carries a block or listblock at-uri",
        heads(section(&page.text, "blockers").unwrap_or("")) == two
            && heads(section(&out.text, "outgoing").unwrap_or("")) == two
            && heads(section(&list.text, "subscribers").unwrap_or("")) == two
            && heads(section(&list.text, "members").unwrap_or(""))
                == ["Account", "Added"]
            && [&page, &out, &list].iter().all(|r| r.status == 200 && clean(r)),
        format!("{:?}", heads(section(&page.text, "blockers").unwrap_or(""))),
    );
    Ok(())
}

// ------------------------------------------------- 8, 10. live handles

async fn check_live(c: &mut Checks, pg: &Pg, skip: bool) -> Result<(), String> {
    c.section("8. handles appear after warming (live network)");
    if skip {
        c.unverified(
            "a fresh instance renders bare DIDs, then handles once the worker has verified them",
            "--skip-live",
        );
        c.section("10. admin tables: handles for cached accounts, DIDs otherwise");
        c.unverified(
            "admin rows show @handle for accounts in the cache and the DID for the rest",
            "--skip-live",
        );
        return Ok(());
    }
    pg.create_db("s8live").await?;
    let dsn = pg.url("s8live");
    let l = Srv::start(
        "live",
        config_toml(&Cfg {
            dsn: &dsn,
            plc: LIVE_PLC,
            budget: 70_000_000_000,
            public_ui: "",
        }),
        &[],
    )
    .await?;
    let pool = pg.pool("s8live", 2).await?;
    let (subject, sid) = (did("lsb", 1), seed::actor(&pool, &did("lsb", 1)).await?);
    for (i, (d, _)) in LIVE.iter().enumerate() {
        let id = seed::actor(&pool, d).await?;
        seed::exec(
            &pool,
            &format!(
                "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
                 VALUES ({id}, '3klive{i}', {sid}, now(), 1, now(), now())"
            ),
        )
        .await?;
    }
    // An account the live directory does not know: it stays a DID.
    let ghost = did("gho", 1);
    let gid = seed::actor(&pool, &ghost).await?;
    seed::exec(
        &pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             VALUES ({gid}, '3kghost', {sid}, now() - interval '1 hour', 1, now() - interval '1 hour', now())"
        ),
    )
    .await?;
    let started = Instant::now();
    let first = l.get(&public_did(&subject)).await?;
    let took = started.elapsed();
    let sec = section(&first.text, "blockers").unwrap_or("").to_owned();
    let held = LIVE.iter().all(|(d, h)| {
        !sec.contains(&format!("title=\"{d}\"")) && !sec.contains(&format!(">{h}</a>"))
    });
    c.check(
        "a new instance with an empty cache renders the page at once, waiting for no outbound request, and shows no account it has not checked: every row is held back and the table says how many",
        first.status == 200
            && held
            && row_dids(&sec).is_empty()
            && pending_of(&sec) == LIVE.len() + 1
            && first.header("cache-control").as_deref() == Some("no-store")
            && took < Duration::from_secs(3),
        format!("{} in {took:?}; {} held back", first.status, pending_of(&sec)),
    );
    // The harness holds for the worker, 15 s at most.
    let mut shown = 0;
    let mut ghost_as_did = false;
    let mut left = usize::MAX;
    for _ in 0..15 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let second = l.get(&public_did(&subject)).await?;
        let s = section(&second.text, "blockers").unwrap_or("");
        shown = LIVE
            .iter()
            .filter(|(_, h)| s.contains(&format!(">{h}</a>")))
            .count();
        ghost_as_did = shown_as(s, &ghost) == format!("<code>{ghost}</code>");
        left = pending_of(s);
        if shown == LIVE.len() && ghost_as_did {
            break;
        }
    }
    let m = l.metrics_text().await?;
    let resolved = metric(
        &m,
        "farsight_handle_warming_total",
        &[("outcome", "resolved")],
    );
    if shown == 0 {
        c.unverified(
            "the next view shows handles",
            format!("no handle verified within 15 s (the live PLC directory or DNS did not answer from here); resolved = {resolved}"),
        );
    } else {
        c.check(
            "within 15 s the same page shows @handle for the accounts the worker verified against the live PLC directory and DNS (both directions), with no card opened and no visit to their pages; the account the directory does not know is shown as its DID; nothing is held back any more",
            shown == LIVE.len() && resolved >= LIVE.len() as f64 && ghost_as_did && left == 0,
            format!("{shown} of {} handles shown; warming resolved = {resolved}; ghost as DID: {ghost_as_did}; {left} held back", LIVE.len()),
        );
    }
    c.section("10. admin tables: handles for cached accounts, DIDs otherwise");
    let cookie = admin_session(&pool, ADMIN_DID).await?;
    let mut admin = l.admin_get(&cookie, &lookup_did(&subject)).await?;
    // The ghost's failed verification is cached within a few seconds.
    for _ in 0..10 {
        let m = l.metrics_text().await?;
        if metric(
            &m,
            "farsight_handle_warming_total",
            &[("outcome", "failed")],
        ) >= 1.0
        {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        admin = l.admin_get(&cookie, &lookup_did(&subject)).await?;
    }
    let sec = admin_section(&admin.text, "Incoming blocks").unwrap_or("");
    let handles = LIVE.iter().filter(|(d, h)| {
        sec.contains(&format!(
            "<a class=\"who\" href=\"/admin/lookup/did?q={}\" title=\"{d}\" data-card=\"/admin/card/{d}\" data-card-session>{h}</a>",
            enc(d)
        ))
    }).count();
    if shown == 0 {
        c.unverified(
            "admin rows show @handle for cached accounts",
            "nothing was verified (see 8)",
        );
    } else {
        c.check(
            "the admin \"Incoming blocks\" table mixes both: @handle for the accounts in the cache — a link to the DID lookup whose title is the DID — and the bare DID for the one account that has no verified handle",
            handles == shown && sec.contains(&format!("<code>{ghost}</code>")) && !sec.contains(&format!("<code>{}</code>", LIVE[0].0)),
            format!("{handles} handles, ghost as DID: {}", sec.contains(&format!("<code>{ghost}</code>"))),
        );
    }
    let anon = l.get(&lookup_did(&subject)).await?;
    c.check(
        "without a session there is no lookup page: the answer is the 303 to /enter, with no handle, no DID and no card attribute in it",
        anon.status == 303
            && anon.header("location").as_deref() == Some("/enter")
            && !anon.text.contains("data-card")
            && !anon.text.contains("did:plc:")
            && !anon.text.contains(LIVE[0].1),
        format!("{} → {:?}", anon.status, anon.header("location")),
    );
    Ok(())
}

// ------------------------------------------------- 11–15. admin pages

async fn check_admin_card(
    c: &mut Checks,
    a: &Srv,
    b: &Srv,
    cookie: &str,
    plc: &Plc,
    w: &World,
) -> Result<(), String> {
    let known = did("sbl", 1);
    let card = |d: &str| format!("/admin/card/{d}");
    let nowhere = b.get("/no/such/route").await?;
    c.section("11–12. /admin/card/{did} with and without a session");
    let anon = b.get(&card(&known)).await?;
    let settings = b.get("/admin/settings").await?;
    c.check(
        "anonymous: the bare 404 of a path that does not exist — same status, same body — never the 303 to /enter that the admin pages answer with: there is no redirect for a script to follow into the card",
        anon.status == 404
            && anon.text == nowhere.text
            && anon.header("location").is_none()
            && settings.status == 303
            && settings.header("location").as_deref() == Some("/enter"),
        format!("card {} / settings {}", anon.status, settings.status),
    );
    // B's card budget is 1 a second (section 15): leave it full.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let before = plc.audit_total();
    let admin = b.admin_get(cookie, &card(&known)).await?;
    c.check(
        "signed-in admin: 200 with the card fragment (the DID, \"DID created\"), fetched through the same code as a public card, and Cache-Control: no-store, private",
        admin.status == 200
            && admin.text.contains("<div class=\"pc\">")
            && admin.text.contains(&format!("<code class=\"pc-did\">{known}</code>"))
            && admin.text.contains("<dt>DID created</dt>")
            && !admin.text.contains("<html")
            && admin.header("cache-control").as_deref() == Some("no-store, private")
            && plc.audit_total() == before + 1,
        format!("{} {:?}", admin.status, admin.header("cache-control")),
    );
    let unknown = b.admin_get(cookie, &card(&did("nop", 1))).await?;
    let bad = b.admin_get(cookie, "/admin/card/not-a-did").await?;
    let wrong = b
        .admin_get("farsight_admin=not-a-session", &card(&known))
        .await?;
    c.check(
        "an account this instance does not hold is a 404 with nothing fetched; a path that is not a DID is a 400; a cookie that is not a session is the anonymous 404; all no-store, private where a session answered",
        unknown.status == 404
            && bad.status == 400
            && wrong.status == 404
            && wrong.text == nowhere.text
            && unknown.header("cache-control").as_deref() == Some("no-store, private")
            && plc.audit_total() == before + 1,
        format!("{} / {} / {}", unknown.status, bad.status, wrong.status),
    );
    let js = a.get("/static/admin.js").await?;
    let public_js = a.get("/static/public.js").await?;
    let page = a.admin_get(cookie, &lookup_did(&w.s)).await?;
    c.check(
        "what the browser gets: admin pages load /static/admin.js and not /static/public.js; the admin script asks for a card with credentials \"same-origin\" only for a link marked data-card-session, the public script always without cookies; neither injects an answer that is not a 200 or was reached through a redirect, and neither names an admin path",
        // The admin pages have their own script, which changes separately
        // from the public one.
        js.status == 200
            && public_js.status == 200
            && page.text.contains("<script src=\"/static/admin.js?v=")
            && !page.text.contains("/static/public.js")
            && js.text.contains("link.hasAttribute(\"data-card-session\")")
            && js.text.contains("credentials: session ? \"same-origin\" : \"omit\"")
            && public_js.text.contains("fetch(link.getAttribute(\"data-card\"), { credentials: \"omit\" })")
            && !public_js.text.contains("data-card-session")
            && [&js, &public_js].iter().all(|j| {
                j.text.contains("r.status !== 200 || r.redirected") && !j.text.contains("/admin/")
            }),
        format!("admin.js {} / public.js {}", js.status, public_js.status),
    );
    Ok(())
}

async fn check_admin_columns(
    c: &mut Checks,
    a: &Srv,
    b: &Srv,
    cookie: &str,
    w: &World,
) -> Result<(), String> {
    c.section("13. admin record cells");
    let plain = a.admin_get(cookie, &lookup_did(&w.t4)).await?;
    let linked = b.admin_get(cookie, &lookup_did(&w.t4)).await?;
    let first = did("tib", 120);
    let rkey = "3ktib00000120";
    let uri = format!("at://{first}/app.bsky.graph.block/{rkey}");
    let copy = format!(
        "<button type=\"button\" class=\"copy-uri\" data-copy=\"{uri}\" title=\"{uri}\">Copy at:// URL</button>"
    );
    c.check(
        "record_viewer_url empty: the DID lookup's record cell is a button that copies the at-uri; the address is not printed, and there is no link",
        plain.text.contains(&format!("<td class=\"td-record\">{copy}</td>"))
            && !plain.text.contains(&format!(">{uri}<"))
            && !plain.text.contains("target=\"_blank\""),
        "copy button",
    );
    c.check(
        "record_viewer_url set: beside the button, a link built from the template with the record's authority, collection and rkey, target=_blank rel=\"noopener noreferrer nofollow\"",
        linked.text.contains(&format!(
            "<td class=\"td-record\">{copy} <a class=\"record-view\" href=\"https://viewer.example/at/{first}/app.bsky.graph.block/{rkey}\" target=\"_blank\" rel=\"noopener noreferrer nofollow\">View</a></td>"
        )),
        "button and link",
    );
    let list = b.admin_get(cookie, &lookup_list(&w.l4_uri)).await?;
    let owner = did("lso", 1);
    c.check(
        "the list lookup links listitem records (in the owner's repo) and listblock records (in the blocker's) the same way",
        list.text.contains(&format!("href=\"https://viewer.example/at/{owner}/app.bsky.graph.listitem/3kitm000120\""))
            && list.text.contains(&format!("href=\"https://viewer.example/at/{}/app.bsky.graph.listblock/3klbk000120\"", did("llb", 120))),
        "listitem and listblock links",
    );

    c.section("14. the lookup pages' columns; no first_seen on a lookup page or a public page");
    let admin = a.admin_get(cookie, &lookup_did(&w.s)).await?;
    let asec = admin_section(&admin.text, "Incoming blocks").unwrap_or("");
    c.check(
        "signed-in admin on /admin/lookup/did: Blocker, Record, Created — no \"First seen\" column and no first_seen value; a date is the instant without its zone (the table states the zone once) with a second line for how long ago, which the page's script writes",
        heads(asec) == ["Blocker", "Record", "Created"]
            && !admin.text.contains("First seen")
            && !admin.text.contains("2026-08-31T23:5")
            && asec.contains("class=\"time-badge\" data-abs>")
            && asec.contains("</time><span class=\"ago\" data-ago></span></td>")
            && !asec.contains(" UTC</time>")
            && asec.contains("All times are in <span data-zone>UTC</span>."),
        format!("{:?}", heads(asec)),
    );
    let ladmin = a.admin_get(cookie, &lookup_list(&w.l4_uri)).await?;
    c.check(
        "the list lookup the same: Member, Record, Added and Subscriber, Record, Created, with no \"First seen\" column",
        heads(admin_section(&ladmin.text, "Members").unwrap_or("")) == ["Member", "Record", "Added"]
            && heads(admin_section(&ladmin.text, "Subscribers").unwrap_or(""))
                == ["Subscriber", "Record", "Created"]
            && !ladmin.text.contains("First seen"),
        "list lookup",
    );
    let anon = a.get(&lookup_did(&w.s)).await?;
    let lanon = a.get(&lookup_list(&w.l4_uri)).await?;
    c.check(
        "without a session there is no lookup page to read the column from: /admin/lookup/did and /admin/lookup/list answer 303 to /enter, with no row, no \"First seen\" and not one first_seen value in the response",
        [&anon, &lanon].iter().all(|r| {
            r.status == 303
                && r.header("location").as_deref() == Some("/enter")
                && !r.text.contains("First seen")
                && !r.text.contains("did:plc:")
                && !r.text.contains("2026-08-31T23:5")
                && !r.text.contains("2026-08-31 23:5")
        }),
        format!("{} / {}", anon.status, lanon.status),
    );
    let public = a.get(&public_did(&w.s)).await?;
    c.check(
        "no public page shows first_seen",
        !public.text.contains("First seen")
            && !public.text.contains("2026-08-31T23:5")
            && !public.text.contains("2026-08-31 23:5"),
        "public",
    );
    Ok(())
}

async fn check_shared_budget(
    c: &mut Checks,
    b: &Srv,
    cookie: &str,
    plc: &Plc,
) -> Result<(), String> {
    c.section("15. public and admin cards draw on one budget");
    let limited = |t: &str| {
        metric(
            t,
            "farsight_public_ui_cards_total",
            &[("outcome", "rate_limited")],
        )
    };
    let full = |r: &Resp| {
        r.status == 200 && r.text.contains("<dt>DID created</dt>") && !r.text.contains(SHORT_CARD)
    };
    let short = |r: &Resp| r.status == 200 && r.text.contains(SHORT_CARD);
    // card_rps = 1, card_burst = 1.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let m0 = b.metrics_text().await?;
    let fetched = plc.audit_total();
    let p1 = b.get(&format!("/card/{}", did("sbl", 11))).await?;
    let a1 = b
        .admin_get(cookie, &format!("/admin/card/{}", did("sbl", 12)))
        .await?;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let a2 = b
        .admin_get(cookie, &format!("/admin/card/{}", did("sbl", 13)))
        .await?;
    let p2 = b.get(&format!("/card/{}", did("sbl", 14))).await?;
    let m1 = b.metrics_text().await?;
    c.check(
        "with card_rps = 1: a public card takes the token and the admin card right after it is the short card; a second later the admin card takes it and the public one is short — two cards fetched, two refused, in farsight_public_ui_cards_total",
        full(&p1) && short(&a1) && full(&a2) && short(&p2)
            && limited(&m1) - limited(&m0) == 2.0
            && plc.audit_total() == fetched + 2
            && a1.header("cache-control").as_deref() == Some("no-store, private"),
        format!(
            "public {} / admin {} / admin {} / public {}; rate_limited +{}",
            if full(&p1) { "full" } else { "short" },
            if full(&a1) { "full" } else { "short" },
            if full(&a2) { "full" } else { "short" },
            if full(&p2) { "full" } else { "short" },
            limited(&m1) - limited(&m0)
        ),
    );
    Ok(())
}

// ------------------------------------------------- 16–20. handle warming

/// The Public UI form as the server has it, with warming on or off.
fn warming_form(csrf: &str, warming: bool) -> Vec<(&'static str, String)> {
    let mut f: Vec<(&'static str, String)> = vec![
        ("csrf", csrf.to_owned()),
        ("enabled", "on".into()),
        ("show_outgoing_blocks", "on".into()),
        ("show_opengraph_image", "on".into()),
        ("show_avatars", "on".into()),
        ("record_viewer_url", String::new()),
        ("card_rps", "4".into()),
        ("card_burst", "8".into()),
        ("dark_mode_default", "system".into()),
        ("rate_limit_rps", "1000".into()),
        ("rate_limit_burst", "10000".into()),
        ("query_concurrency", "8".into()),
        ("handle_cache_ttl", "1h".into()),
        ("instance_description", String::new()),
        ("contact", String::new()),
        ("excluded_dids", String::new()),
    ];
    if warming {
        f.push(("handle_warming_enabled", "on".into()));
    }
    f
}

async fn set_warming(a: &Srv, cookie: &str, on: bool) -> Result<Resp, String> {
    let page = a.admin_get(cookie, "/admin/settings").await?;
    let csrf = csrf_of(&page.text).ok_or("no csrf on /admin/settings")?;
    let form = warming_form(&csrf, on);
    let pairs: Vec<(&str, &str)> = form.iter().map(|(k, v)| (*k, v.as_str())).collect();
    a.admin
        .post_form(
            &format!("{}/admin/settings/public-ui", a.base),
            &[("cookie", cookie)],
            &pairs,
        )
        .await
}

async fn set_pass(a: &Srv, cookie: &str, rps: u32) -> Result<Resp, String> {
    let page = a.admin_get(cookie, "/admin/settings").await?;
    let csrf = csrf_of(&page.text).ok_or("no csrf on /admin/settings")?;
    let mut form = warming_form(&csrf, true);
    form.push(("handle_pass_rps", rps.to_string()));
    let pairs: Vec<(&str, &str)> = form.iter().map(|(k, v)| (*k, v.as_str())).collect();
    a.admin
        .post_form(
            &format!("{}/admin/settings/public-ui", a.base),
            &[("cookie", cookie)],
            &pairs,
        )
        .await
}

/// One table of a top list on the home page: `(DID, count)` per row.
fn top_rows(table: &str) -> Vec<(String, i64)> {
    table
        .split("<tr class=\"data-row")
        .skip(1)
        .map(|row| {
            let did = between(row, "title=\"", "\"")
                .first()
                .copied()
                .unwrap_or("");
            let count = between(row, "td-count\">", "</td>")
                .first()
                .map(|n| n.replace(',', ""))
                .and_then(|n| n.parse().ok())
                .unwrap_or(-1);
            (did.to_owned(), count)
        })
        .collect()
}

/// The two tables of a period's tab, top blockers then most blocked,
/// each with the rows behind "Show more" after its first ten.
fn top_tables(home: &str, id: &str) -> Vec<Vec<(String, i64)>> {
    let Some(i) = home.find(&format!("<section id=\"{id}\"")) else {
        return Vec::new();
    };
    let rest = &home[i..];
    let group = &rest[..rest.find("</section>").unwrap_or(rest.len())];
    group
        .split("<div class=\"top-list\">")
        .skip(1)
        .map(top_rows)
        .collect()
}

/// The home page's top lists: off by default, counted in the background
/// once a switch is on, for the day that ended at 05:00 EST.
async fn check_top_lists(
    c: &mut Checks,
    a: &Srv,
    pool: &PgPool,
    cookie: &str,
) -> Result<(), String> {
    c.section("19e. the home page's top lists (show_top_blockers, show_top_blocked)");
    let home = a.get("/").await?;
    let stored = n(pool, "SELECT count(*) FROM top_lists").await?;
    c.check(
        "off by default: the home page has no top list and nothing is counted",
        home.status == 200 && !home.text.contains("top-group") && stored == 0,
        format!("{stored} stored lists"),
    );
    // The seeded blocks were written past the apply path: give the
    // authors their counts. Then, around the day that ended at the last
    // 10:00 UTC: 400 of the blocks are logged inside it, 300 others
    // after it, and 900 rows inside it, under one author, have no stored
    // block.
    const DAY_END: &str = "(date_trunc('day', now() - interval '10 hours') + interval '10 hours')";
    seed::exec(
        pool,
        "UPDATE actors a SET authored_blocks = t.n
         FROM (SELECT author_id, count(*)::int AS n FROM blocks GROUP BY 1) t WHERE a.id = t.author_id",
    )
    .await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO block_recent (at, author_id, rkey, subject_id)
             SELECT {DAY_END} - interval '1 hour', author_id, rkey, subject_id FROM blocks
             ORDER BY subject_id, author_id, rkey LIMIT 400"
        ),
    )
    .await?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO block_recent (at, author_id, rkey, subject_id)
             SELECT {DAY_END} + interval '1 second', author_id, rkey, subject_id FROM blocks
             ORDER BY subject_id DESC, author_id, rkey LIMIT 300"
        ),
    )
    .await?;
    let phantom = did("hpq", 2);
    seed::exec(
        pool,
        &format!(
            "INSERT INTO block_recent (at, author_id, rkey, subject_id)
             SELECT {DAY_END} - interval '2 hours', (SELECT id FROM actors WHERE did = '{phantom}'), 'gone' || g, 1
             FROM generate_series(1, 900) g"
        ),
    )
    .await?;
    let save = |on: bool| async move {
        let page = a.admin_get(cookie, "/admin/settings").await?;
        let csrf = csrf_of(&page.text).ok_or("no csrf on /admin/settings")?;
        let mut form = warming_form(&csrf, false);
        if on {
            form.push(("show_top_blockers", "on".into()));
            form.push(("show_top_blocked", "on".into()));
        }
        let pairs: Vec<(&str, &str)> = form.iter().map(|(k, v)| (*k, v.as_str())).collect();
        a.admin
            .post_form(
                &format!("{}/admin/settings/public-ui", a.base),
                &[("cookie", cookie)],
                &pairs,
            )
            .await
    };
    let saved = save(true).await?;
    let started = Instant::now();
    while n(pool, "SELECT count(*) FROM top_lists").await? < 4
        && started.elapsed() < Duration::from_secs(150)
    {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let lists = n(
        pool,
        &format!("SELECT count(*) FROM top_lists WHERE computed_at = {DAY_END}"),
    )
    .await?;
    let home = a.get("/").await?;
    let other = a.get("/?tab=alltime").await?;
    let day = top_tables(&home.text, "lastday");
    let all = top_tables(&home.text, "alltime");
    let ranked = |t: &Vec<(String, i64)>| {
        !t.is_empty() && t.len() <= 20 && t.windows(2).all(|w| w[0].1 >= w[1].1) && t[0].1 > 0
    };
    let first = |sql: String| async move {
        sqlx::query_as::<_, (String, i64)>(&sql)
            .fetch_one(pool)
            .await
            .map_err(|e| e.to_string())
    };
    let of_day = |by: &str| {
        format!(
            "SELECT a.did, t.n FROM (
               SELECT r.{by} AS id, count(*) AS n FROM block_recent r
               JOIN blocks b ON b.author_id = r.author_id AND b.rkey = r.rkey AND b.subject_id = r.subject_id
               WHERE r.at >= {DAY_END} - interval '24 hours' AND r.at < {DAY_END} GROUP BY 1) t
             JOIN actors a ON a.id = t.id WHERE a.status IN (0, 3)
             ORDER BY t.n DESC, a.id LIMIT 1"
        )
    };
    let day_blocks = first(of_day("author_id")).await?;
    let day_blocked = first(of_day("subject_id")).await?;
    let all_blocks = first(
        "SELECT did, authored_blocks::bigint FROM actors WHERE status IN (0, 3)
         ORDER BY authored_blocks DESC, id LIMIT 1"
            .into(),
    )
    .await?;
    let all_blocked = first(
        "SELECT a.did, t.n FROM (SELECT subject_id, count(*) AS n FROM blocks GROUP BY 1) t
         JOIN actors a ON a.id = t.subject_id WHERE a.status IN (0, 3)
         ORDER BY t.n DESC, a.id LIMIT 1"
            .into(),
    )
    .await?;
    c.check(
        "switched on (saved from Settings, no restart): within the task's next look the four lists are stored for the day that ended at 05:00 EST; the home page has a tab for each period, \"Last 24H\" in view and ?tab=alltime the other; each tab has two tables, top blockers and most blocked, at most 20 rows each, largest count first, with no description and no line about when they are counted",
        saved.status < 400
            && lists == 4
            && day.len() == 2
            && all.len() == 2
            && day.iter().chain(all.iter()).all(ranked)
            && home.text.contains("data-active=\"lastday\"")
            && other.text.contains("data-active=\"alltime\"")
            && home.text.contains(">Last 24H</a>")
            && home.text.contains(">All Time</a>")
            && !home.text.contains("top-note")
            && !home.text.contains("top-asof"),
        format!(
            "{lists} lists after {} s; rows {:?} and {:?}",
            started.elapsed().as_secs(),
            day.iter().map(Vec::len).collect::<Vec<_>>(),
            all.iter().map(Vec::len).collect::<Vec<_>>()
        ),
    );
    let lead = |t: &[Vec<(String, i64)>], i: usize| t.get(i).and_then(|t| t.first()).cloned();
    c.check(
        "each table leads with the right account and number: the most blocks made and received in that day, and of all time; a block logged after the day ended does not count, nor one that is no longer stored",
        lead(&day, 0) == Some(day_blocks.clone())
            && lead(&day, 1) == Some(day_blocked.clone())
            && lead(&all, 0) == Some(all_blocks.clone())
            && lead(&all, 1) == Some(all_blocked.clone())
            && !day.iter().flatten().any(|r| r.0 == phantom),
        format!(
            "day {:?} / {:?}, expected {day_blocks:?} / {day_blocked:?}; all time {:?} / {:?}, expected {all_blocks:?} / {all_blocked:?}",
            lead(&day, 0),
            lead(&day, 1),
            lead(&all, 0),
            lead(&all, 1)
        ),
    );
    let long = day.iter().chain(all.iter()).any(|t| t.len() > 10);
    c.check(
        "a table shows ten rows; the rest are behind \"Show more\", which only a table of more than ten has",
        long == home.text.contains("class=\"top-more-toggle")
            && home
                .text
                .split("<div class=\"top-list\">")
                .skip(1)
                .all(|t| top_rows(t.split("top-more-toggle").next().unwrap_or("")).len() <= 10),
        format!("a table of more than ten: {long}"),
    );
    let off = save(false).await?;
    let home = a.get("/").await?;
    c.check(
        "switched off again, the home page has no top list",
        off.status < 400 && !home.text.contains("top-group"),
        format!("save {}", off.status),
    );
    Ok(())
}

/// The lists an account subscribes to as block lists: always a tab of the
/// admin lookup; a tab of the public page with `show_outgoing_blocks`.
async fn check_blocking_lists(
    c: &mut Checks,
    a: &Srv,
    pool: &PgPool,
    cookie: &str,
    w: &World,
) -> Result<(), String> {
    c.section("19d. the lists an account subscribes to (Blocking lists)");
    let owner = did("abo", 1);
    let oid = n(
        pool,
        &format!("SELECT id FROM actors WHERE did = '{owner}'"),
    )
    .await?;
    let sid = n(
        pool,
        &format!("SELECT id FROM actors WHERE did = '{}'", w.s),
    )
    .await?;
    // One list Farsight serves, and one it only knows from the listblock.
    let served = seed_list(pool, oid, "subscribed", 0).await?;
    let unknown: i64 = sqlx::query_scalar(
        "INSERT INTO lists (owner_id, rkey) VALUES ($1, 'unknownlist') RETURNING id",
    )
    .bind(oid)
    .fetch_one(pool)
    .await
    .map_err(|e| e.to_string())?;
    seed::exec(
        pool,
        &format!(
            "INSERT INTO list_blocks (author_id, rkey, list_id, counted, created_at, rev, first_seen, last_seen)
             VALUES ({sid}, '3ksubscr00001', {served}, true, '2026-01-02T03:04:05Z', 1, now(), now()),
                    ({sid}, '3ksubscr00002', {unknown}, false, '2026-01-03T03:04:05Z', 1, now(), now())"
        ),
    )
    .await?;
    let admin = a
        .admin_get(cookie, &format!("{}&tab=blockinglists", lookup_did(&w.s)))
        .await?;
    let asec = section(&admin.text, "blockinglists").unwrap_or("");
    let copy = |rkey: &str| format!("data-copy=\"at://{}/app.bsky.graph.listblock/{rkey}\"", w.s);
    c.check(
        "the admin lookup always has a Blocking lists tab: every listblock the account has, newest first, each with the list (by name, or by address where Farsight has no record of it), its owner, a button holding the listblock's at-uri, and the date",
        admin.status == 200
            && admin.text.contains("<div class=\"tabbed\" data-active=\"blockinglists\">")
            && heads(asec) == ["List", "Purpose", "Owner", "Record", "Subscribed"]
            && asec.contains(&copy("3ksubscr00001"))
            && asec.contains(&copy("3ksubscr00002"))
            && asec.find(&copy("3ksubscr00002")) < asec.find(&copy("3ksubscr00001"))
            && asec.contains(">subscribed</span>")
            && asec.contains(&format!("at://{owner}/app.bsky.graph.list/unknownlist"))
            && asec.contains("<span class=\"count count-big\">2</span>"),
        format!("{:?}", heads(asec)),
    );
    // The main instance has show_outgoing_blocks on by now.
    let public = a
        .get(&format!("{}?tab=blockinglists", public_did(&w.s)))
        .await?;
    let psec = section(&public.text, "blockinglists").unwrap_or("");
    c.check(
        "the public page, with show_outgoing_blocks: a Blocking Lists tab after Blocking, listing only the lists the public pages serve (the list Farsight has no record of is left out), linked to the list's page, with no record address",
        public.status == 200
            && public.text.contains("data-tab=\"blockinglists\"")
            && heads(psec) == ["List", "Owner", "Subscribed"]
            && psec.contains(&format!("href=\"/list/{owner}/subscribed\""))
            && !psec.contains("unknownlist")
            && !psec.contains("app.bsky.graph.listblock")
            && psec.contains("<span class=\"count count-big\">1</span>"),
        format!("{:?}; {}", heads(psec), public.status),
    );
    Ok(())
}

/// `avatar_thumbnails`: cards and list pages name the image as a
/// thumbnail on Bluesky's image service, from the CID stored for the
/// account.
async fn check_thumbnails(
    c: &mut Checks,
    a: &Srv,
    pool: &PgPool,
    cookie: &str,
    plc: &Plc,
) -> Result<(), String> {
    c.section("19c. avatar thumbnails (avatar_thumbnails)");
    const LIST_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const CID: &str = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    // The list's owner had a card opened by the browser probes, before
    // it had a profile here: that answer ("no avatar") is stored for a
    // day. The card is read for an account nothing has asked about.
    let owner = did("abo", 1);
    let subject = did("abp", 1);
    seed::exec(
        pool,
        &format!("INSERT INTO actors (did) VALUES ('{subject}') ON CONFLICT DO NOTHING"),
    )
    .await?;
    plc.host(&subject);
    plc.set_profile(
        &subject,
        serde_json::json!({
            "$type": "app.bsky.actor.profile",
            "avatar": {"$type": "blob", "ref": {"$link": CID}, "mimeType": "image/jpeg", "size": 1}
        }),
    );
    let set = |on: bool| async move {
        let page = a.admin_get(cookie, "/admin/settings").await?;
        let csrf = csrf_of(&page.text).ok_or("no csrf on /admin/settings")?;
        let mut form = warming_form(&csrf, true);
        if on {
            form.push(("avatar_thumbnails", "on".into()));
        }
        let pairs: Vec<(&str, &str)> = form.iter().map(|(k, v)| (*k, v.as_str())).collect();
        a.admin
            .post_form(
                &format!("{}/admin/settings/public-ui", a.base),
                &[("cookie", cookie)],
                &pairs,
            )
            .await
    };
    let thumb = |who: &str, cid: &str| {
        format!("https://cdn.bsky.app/img/avatar_thumbnail/plain/{who}/{cid}@jpeg")
    };
    let card = format!("/card/{subject}");
    let list = format!("/list/{owner}/told");
    let stored =
        format!("SELECT count(*) FROM avatar_cache WHERE did = '{subject}' AND cid = '{CID}'");

    set(true).await?;
    let r0 = plc.record_total();
    let first = a.get(&card).await?;
    let r1 = plc.record_total();
    let again = a.get(&card).await?;
    let r2 = plc.record_total();
    let lp = a.get(&list).await?;
    let src = format!("<img class=\"pc-avatar\" src=\"{}\"", thumb(&subject, CID));
    c.check(
        "on (saved from Settings, no restart): a card names the avatar as a thumbnail on the image service, by the account's DID and the CID of its profile's avatar; the CID is read from the profile record once and stored, and the next card asks nobody; a list page names its image the same way, itself",
        first.status == 200
            && first.text.contains(&src)
            && r1 == r0 + 1
            && again.text.contains(&src)
            && r2 == r1
            && n(pool, &stored).await? == 1
            && lp.text.contains(&format!("data-list-image-src=\"{}\"", thumb(&owner, LIST_CID))),
        format!(
            "getRecord {r0} → {r1} → {r2}; stored rows {}; card has the thumbnail: {}",
            n(pool, &stored).await?,
            first.text.contains(&src)
        ),
    );
    set(false).await?;
    let off = a.get(&card).await?;
    let lp_off = a.get(&list).await?;
    c.check(
        "off again: no page names the image service; the stored CID is still what a card goes by (no new read of the profile)",
        off.status == 200
            && !off.text.contains("cdn.bsky.app")
            && !lp_off.text.contains("cdn.bsky.app")
            && lp_off.text.contains(&format!("data-list-image=\"{LIST_CID}\""))
            && plc.record_total() == r2,
        format!("getRecord total {}", plc.record_total()),
    );
    Ok(())
}

/// The handle pass: off by default; switched on, it serves queued
/// identity changes first, then walks the accounts; switched off, it
/// stops.
async fn check_pass(
    c: &mut Checks,
    a: &Srv,
    pool: &PgPool,
    cookie: &str,
    plc: &Plc,
) -> Result<(), String> {
    c.section("19b. the handle pass (handle_pass_rps)");
    let stored = "SELECT count(*) FROM handle_cache";
    let before = n(pool, stored).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let settings = a.admin_get(cookie, "/admin/settings").await?;
    let dash_off = a.admin_get(cookie, "/admin/dashboard/fragment").await?;
    let caught = |t: &str, label: &str| {
        between(
            t,
            &format!("<div class=\"l\">{label}</div><div class=\"v\">"),
            "</div>",
        )
        .first()
        .map(|v| v.to_string())
    };
    c.check(
        "off by default: Settings shows handle_pass_rps = 0, nothing is checked in the background and the dashboard's \"Catching up\" has no line for handles",
        settings.text.contains("name=\"handle_pass_rps\" min=\"0\" max=\"200\" value=\"0\"")
            && dash_off.status == 200
            && caught(&dash_off.text, "Handles").is_none()
            && n(pool, stored).await? == before,
        format!("{before} stored answers"),
    );
    seed::exec(
        pool,
        &format!(
            "INSERT INTO actors (did) SELECT {} FROM generate_series(1, 30) g ON CONFLICT DO NOTHING",
            did_sql("hpq", "g")
        ),
    )
    .await?;
    let (kept, queued_only) = (did("hpq", 1), "did LIKE 'did:plc:hpq%'");
    seed::exec(
        pool,
        &format!("INSERT INTO handle_cache (did, handle, resolved_at) VALUES ('{kept}', 'old.example', now())"),
    )
    .await?;
    seed::exec(
        pool,
        &format!("INSERT INTO handle_due (did) SELECT did FROM actors WHERE {queued_only}"),
    )
    .await?;
    let saved = set_pass(a, cookie, 200).await?;
    let waiting = format!("SELECT count(*) FROM handle_due WHERE {queued_only}");
    let started = Instant::now();
    while n(pool, &waiting).await? > 0 && started.elapsed() < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let left = n(pool, &waiting).await?;
    let asked: Vec<u32> = (1..=30).map(|i| plc.documents(&did("hpq", i))).collect();
    let answered = n(
        pool,
        &format!("SELECT count(*) FROM handle_cache WHERE {queued_only} AND handle = ''"),
    )
    .await?;
    let m = a.metrics_text().await?;
    let gone = metric(&m, "farsight_handle_pass_total", &[("outcome", "gone")]);
    c.check(
        "switched on (saved from Settings, no restart): accounts queued by identity changes are checked first — each one's document read once, the queue emptied, the answer stored; an account whose document no longer names the handle stored for it loses that handle",
        saved.status < 400
            && left == 0
            && asked.iter().all(|k| *k == 1)
            && answered == 30
            && gone >= 30.0,
        format!("{left} left in the queue, documents asked {asked:?}, {answered} stored as no handle, gone = {gone}"),
    );
    let n0 = n(pool, stored).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let n1 = n(pool, stored).await?;
    let position = metric(
        &a.metrics_text().await?,
        "farsight_handle_pass_position",
        &[],
    );
    let dash_on = a.admin_get(cookie, "/admin/dashboard/fragment").await?;
    let line = caught(&dash_on.text, "Handles").unwrap_or_default();
    c.check(
        "while it walks, the dashboard's \"Catching up\" block says how far it has got and about how long the rest takes",
        dash_on.text.contains("<span>Catching up</span>")
            && line.contains("% of accounts checked, about ")
            && line.ends_with(" left"),
        format!("Handles: {line:?}"),
    );
    set_pass(a, cookie, 0).await?;
    tokio::time::sleep(Duration::from_secs(8)).await;
    let n2 = n(pool, stored).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let n3 = n(pool, stored).await?;
    c.check(
        "then it walks the accounts in id order at its own rate, storing an answer for each (hundreds in five seconds at 200 a second); set back to 0 it stops",
        n1 >= n0 + 300 && position > 0.0 && n3 == n2,
        format!("stored answers {n0} → {n1} in 5 s, position {position}; after switching off {n2} → {n3}"),
    );
    Ok(())
}

/// How many rows a public section says it holds back for a handle check.
fn pending_of(sec: &str) -> usize {
    between(sec, "data-pending=\"", "\"")
        .first()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// What the row naming `did` shows inside its link.
fn shown_as<'a>(sec: &'a str, did: &str) -> &'a str {
    let Some(i) = sec.find(&format!("title=\"{did}\"")) else {
        return "";
    };
    let rest = &sec[i..];
    match (rest.find('>'), rest.find("</a>")) {
        (Some(a), Some(b)) if a < b => &rest[a + 1..b],
        _ => "",
    }
}

async fn check_warming(
    c: &mut Checks,
    a: &Srv,
    pool: &PgPool,
    cookie: &str,
    plc: &Plc,
    w: &World,
) -> Result<(), String> {
    let queue = "farsight_handle_warming_queue";
    let outcome = |t: &str, o: &str| metric(t, "farsight_handle_warming_total", &[("outcome", o)]);
    // The earlier sections rendered about a thousand accounts, minutes of
    // work at 2 a second. Off and on again empties the queue; the handle
    // budget refills meanwhile.
    set_warming(a, cookie, false).await?;
    tokio::time::sleep(Duration::from_secs(7)).await;
    set_warming(a, cookie, true).await?;
    tokio::time::sleep(Duration::from_secs(6)).await;

    c.section("16. a rendered page puts its uncached accounts on the queue");
    let m0 = a.metrics_text().await?;
    let docs0 = plc.document_total();
    let page = a.get(&public_did(&w.w16)).await?;
    let m1 = a.metrics_text().await?;
    let waiting = metric(&m1, queue, &[]);
    let done = processed(&m1) - processed(&m0);
    c.check(
        "a page with 50 accounts not in the cache holds all 50 rows back and says so, and is not to be cached; right after the render the queue gauge plus what the worker has already taken accounts for all 50 — most still waiting, since the worker gets only what the handle budget has to spare",
        page.status == 200
            && row_dids(section(&page.text, "blockers").unwrap_or("")).is_empty()
            && pending_of(section(&page.text, "blockers").unwrap_or("")) == 50
            && page.header("cache-control").as_deref() == Some("no-store")
            && metric(&m0, queue, &[]) == 0.0
            && (40.0..=50.0).contains(&waiting)
            && waiting + done <= 50.0
            // Up to five can be in flight: what the bucket holds above
            // its reserve.
            && waiting + done >= 45.0,
        format!("queue {waiting}, finished {done}"),
    );

    c.section("17. the queue drains and the cache fills");
    let mut left = waiting;
    let started = Instant::now();
    while left > 0.0 && started.elapsed() < Duration::from_secs(45) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        left = a.gauge(queue).await?;
    }
    tokio::time::sleep(Duration::from_secs(6)).await;
    let m2 = a.metrics_text().await?;
    let failed = outcome(&m2, "failed") - outcome(&m0, "failed");
    let asked = plc.document_total() - docs0;
    let took = started.elapsed();
    c.check(
        "with handle_warming_enabled = true the 50 are worked off at the handle budget's pace (about 2 a second): the gauge falls to 0, each account's document was requested from the PLC source, and each outcome is counted (the stand-in names no handle: `failed`)",
        left == 0.0 && failed == 50.0 && (50..=51).contains(&asked) && took >= Duration::from_secs(15),
        format!("drained in {took:?}; failed +{failed}; {asked} document requests"),
    );
    let again = a.get(&public_did(&w.w16)).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let m3 = a.metrics_text().await?;
    c.check(
        "the answers are in the cache: rendering the page again queues nothing and requests nothing (a failure is remembered for 10 minutes); the 50 accounts, checked and without a handle, are now rows showing their DIDs, nothing is held back and the page may be cached again",
        again.status == 200
            && row_dids(section(&again.text, "blockers").unwrap_or("")).len() == 50
            && pending_of(section(&again.text, "blockers").unwrap_or("")) == 0
            && again.header("cache-control").as_deref() == Some("public, max-age=30")
            && metric(&m3, queue, &[]) == 0.0
            && processed(&m3) == processed(&m2)
            && plc.document_total() - docs0 == asked,
        format!("queue {}", metric(&m3, queue, &[])),
    );

    c.section("20. one resolution per account");
    let twice = did("wxd", 1);
    let p1 = a.get(&public_did(&w.w20)).await?;
    let p2 = a.get(&public_did(&w.w20)).await?;
    let held = pending_of(section(&p1.text, "blockers").unwrap_or(""));
    let started = Instant::now();
    while a.gauge(queue).await? > 0.0 && started.elapsed() < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    tokio::time::sleep(Duration::from_secs(6)).await;
    let p3 = a.get(&public_did(&w.w20)).await?;
    let rows = row_dids(section(&p3.text, "blockers").unwrap_or(""));
    tokio::time::sleep(Duration::from_secs(2)).await;
    let counts: Vec<u32> = (1..=10).map(|i| plc.documents(&did("wxd", i))).collect();
    c.check(
        "an account that is on a page twice, on a page rendered twice before the worker reached it, and rendered again afterwards: its document was requested exactly once — and so was every other account's",
        p2.status == 200
            && held == 11
            && rows.iter().filter(|d| **d == twice).count() == 2
            && rows.len() == 11
            && counts.iter().all(|n| *n == 1),
        format!("{counts:?}"),
    );

    c.section("20b. handles stored in handle_cache");
    let (fresh, stale, none) = (did("wxe", 1), did("wxe", 2), did("wxe", 3));
    let first = a.get(&public_did(&w.w21)).await?;
    let sec = section(&first.text, "blockers").unwrap_or("");
    c.check(
        "a page nobody has rendered since the server started shows the stored handles at once — the one verified a day ago and the one verified eight days ago — and holds back the one account that has never been checked",
        first.status == 200
            && shown_as(sec, &fresh) == "stored-fresh.example"
            && shown_as(sec, &stale) == "stored-stale.example"
            && shown_as(sec, &none).is_empty()
            && pending_of(sec) == 1,
        format!(
            "{:?} {:?} {:?}",
            shown_as(sec, &fresh),
            shown_as(sec, &stale),
            support::truncate(shown_as(sec, &none), 60)
        ),
    );
    let started = Instant::now();
    while (plc.documents(&stale) == 0 || plc.documents(&none) == 0)
        && started.elapsed() < Duration::from_secs(30)
    {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    tokio::time::sleep(Duration::from_secs(6)).await;
    let again = a.get(&public_did(&w.w21)).await?;
    let sec = section(&again.text, "blockers").unwrap_or("");
    let kept = n(
        pool,
        &format!(
            "SELECT count(*) FROM handle_cache WHERE (did = '{fresh}' AND handle = 'stored-fresh.example')
                OR (did = '{stale}' AND handle = 'stored-stale.example' AND resolved_at < now() - interval '7 days')"
        ),
    )
    .await?;
    let rows = n(pool, "SELECT count(*) FROM handle_cache WHERE handle <> ''").await?;
    let checked = n(
        pool,
        &format!("SELECT count(*) FROM handle_cache WHERE did = '{none}' AND handle = ''"),
    )
    .await?;
    c.check(
        "only the handle older than seven days is verified again, in the background (its document is requested once, the fresh one's never); the verification fails here (the stand-in names no handle), so the old handle stays on the page and in the table; the account that was held back has been checked, is now a row showing its DID, and its check is stored as \"no handle\" without touching a stored handle",
        plc.documents(&fresh) == 0
            && plc.documents(&stale) == 1
            && plc.documents(&none) == 1
            && shown_as(sec, &fresh) == "stored-fresh.example"
            && shown_as(sec, &stale) == "stored-stale.example"
            && shown_as(sec, &none) == format!("<code>{none}</code>")
            && pending_of(sec) == 0
            && kept == 2
            && rows == 2
            && checked == 1,
        format!(
            "documents: fresh {}, stale {}, none {}; {kept} of 2 rows unchanged, {rows} with a handle, {checked} stored as none",
            plc.documents(&fresh),
            plc.documents(&stale),
            plc.documents(&none)
        ),
    );

    c.section("18. the queue is capped");
    let m4 = a.metrics_text().await?;
    let (seen, pages) = admin_walk(a, cookie, &lookup_did(&w.w18), "Incoming blocks").await?;
    let m5 = a.metrics_text().await?;
    let dropped = outcome(&m5, "dropped") - outcome(&m4, "dropped");
    let waiting = metric(&m5, queue, &[]);
    c.check(
        "2,300 uncached accounts rendered in under a minute (46 admin pages): the queue stops at 2,000 and the oldest are dropped, counted as outcome=\"dropped\"",
        seen.len() == 2300 && (46..=47).contains(&pages) && (1900.0..=2000.0).contains(&waiting) && (200.0..=300.0).contains(&dropped),
        format!("queue {waiting}, dropped +{dropped}"),
    );

    c.section("19. handle_warming_enabled = false");
    let off = set_warming(a, cookie, false).await?;
    let cfg = std::fs::read_to_string(a.dir.join("config.toml")).unwrap_or_default();
    // The worker reads the setting every 5 s.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let m6 = a.metrics_text().await?;
    let page = a.get(&public_did(&w.w19)).await?;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let m7 = a.metrics_text().await?;
    c.check(
        "switched off in Settings, without a restart: the queue is emptied, the worker takes nothing more, and a page with 20 uncached accounts queues nothing and causes no request — the gauge stays flat at 0",
        off.status < 400
            && cfg.contains("handle_warming_enabled = false")
            && metric(&m6, queue, &[]) == 0.0
            && page.status == 200
            && metric(&m7, queue, &[]) == 0.0
            && processed(&m7) == processed(&m6)
            && (1..=20).all(|i| plc.documents(&did("wxc", i)) == 0),
        format!("save {}; queue {} → {}", off.status, metric(&m6, queue, &[]), metric(&m7, queue, &[])),
    );
    let on = set_warming(a, cookie, true).await?;
    tokio::time::sleep(Duration::from_secs(6)).await;
    let restarted_empty = a.gauge(queue).await? == 0.0;
    let _ = a.get(&public_did(&w.w19)).await?;
    let started = Instant::now();
    let mut done = 0;
    while done < 20 && started.elapsed() < Duration::from_secs(40) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        done = (1..=20)
            .filter(|i| plc.documents(&did("wxc", *i)) == 1)
            .count();
    }
    c.check(
        "switched on again: it starts from an empty queue (none of the 2,000 dropped accounts comes back) and the next render's 20 accounts are verified",
        on.status < 400
            && restarted_empty
            && done == 20
            && a.gauge(queue).await? == 0.0,
        format!("{done} of 20 verified in {:?}", started.elapsed()),
    );
    let m8 = a.metrics_text().await?;
    c.check(
        "the two metrics exist with the documented outcomes",
        ["resolved", "unverified", "failed", "cached", "dropped"]
            .iter()
            .all(|o| m8.contains(&format!("farsight_handle_warming_total{{outcome=\"{o}\"}}")))
            && m8.contains("farsight_handle_warming_queue ")
            && m8.contains("farsight_ui_sort_indexes_ready 4"),
        "farsight_handle_warming_total, farsight_handle_warming_queue",
    );
    Ok(())
}

// ------------------------------------------------- 21–23. the index build

async fn valid_indexes(pool: &PgPool) -> Result<Vec<String>, String> {
    sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
         WHERE i.indisvalid AND c.relname LIKE '%\\_created' ORDER BY 1",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())
}

async fn wait_indexes(pool: &PgPool, want: usize, within: Duration) -> Result<usize, String> {
    let started = Instant::now();
    loop {
        let have = valid_indexes(pool).await?.len();
        if have >= want || started.elapsed() > within {
            return Ok(have);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn check_index_build(c: &mut Checks, pg: &Pg) -> Result<(), String> {
    c.section("21. the indexes are built in the background, after the server serves");
    pg.create_db("s8idx").await?;
    let dsn = pg.url("s8idx");
    let roomy = |extra: &'static str| Cfg {
        dsn: &dsn,
        plc: "https://127.0.0.1:9",
        budget: 70_000_000_000,
        public_ui: extra,
    };
    let env = [("FARSIGHT_HARNESS_SORT_RETRY_SECS", "3")];
    let mut e = Srv::start(
        "idx",
        config_toml(&roomy("handle_warming_enabled = false")),
        &env,
    )
    .await?;
    let pool = pg.pool("s8idx", 3).await?;
    let have = wait_indexes(&pool, 4, Duration::from_secs(300)).await?;
    // `/health` also wants a connected firehose, which the harness has
    // none of: a page is the evidence that the server serves.
    let health = e.get("/").await?;
    let log = e.log();
    let serving = log.find("normal mode: serving");
    let building = log.find("sort index: building");
    let migrations: i64 = n(&pool, "SELECT count(*) FROM _sqlx_migrations").await?;
    c.check(
        "a fresh database: the server is live in its normal start-up time, the thirteen migrations create no sort index, and the four appear afterwards, built by the server task (the log's first \"building\" line comes after \"serving\")",
        have == 4
            && health.status == 200
            && e.came_up < Duration::from_secs(60)
            && migrations == 13
            && serving.is_some()
            && building.is_some()
            && serving < building
            && log.matches("sort index: built").count() == 4
            && log.contains("sort indexes: all four ready"),
        format!("live after {:?}; {have} of 4 valid; / {}", e.came_up, health.status),
    );
    c.check(
        "each build is logged at INFO with its duration and the resulting index size, and farsight_ui_sort_indexes_ready reads 4",
        log.lines().filter(|l| l.contains("sort index: built") && l.contains("took_ms") && l.contains("index_bytes") && l.contains("INFO")).count() == 4
            && e.gauge("farsight_ui_sort_indexes_ready").await? == 4.0,
        support::truncate(log.lines().find(|l| l.contains("sort index: built")).unwrap_or(""), 300),
    );
    // A start on a database that has them: nothing to do.
    e.stop();
    let metrics = e.metrics.clone();
    let mut e = Srv::launch(&e.dir.clone(), metrics, &env).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let log = e.log();
    c.check(
        "idempotent: a restart finds the four valid, builds nothing, and the sections sort by them from the first request",
        log.matches("sort index: building").count() == 4
            && log.matches("sort indexes: all four ready").count() == 2
            && e.gauge("farsight_ui_sort_indexes_ready").await? == 4.0,
        "second start",
    );
    // An interrupted build leaves an index that is present and not valid.
    e.stop();
    seed::exec(&pool, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'blocks_by_author_created'::regclass").await?;
    let metrics = e.metrics.clone();
    let mut e = Srv::launch(&e.dir.clone(), metrics, &env).await?;
    let have = wait_indexes(&pool, 4, Duration::from_secs(60)).await?;
    c.check(
        "an index left invalid by an interrupted build is dropped and built again at the next start",
        have == 4 && e.log().contains("sort index: dropping an interrupted build"),
        format!("{have} of 4 valid"),
    );
    e.stop();

    c.section("22. the build is held while the storage budget has no room");
    for s in Section::ALL {
        seed::exec(&pool, &format!("DROP INDEX {}", s.index())).await?;
    }
    let subject = did("hsb", 1);
    let sid = seed::actor(&pool, &subject).await?;
    seed_blockers(
        &pool,
        "hbl",
        250,
        sid,
        &format!("{BASE} - make_interval(mins => 251 - g)"),
        &format!("{BASE} - make_interval(mins => 251 - g)"),
    )
    .await?;
    seed::exec(&pool, &format!("INSERT INTO actors (did) SELECT {} FROM generate_series(1, 3000) g ON CONFLICT DO NOTHING", did_sql("hfl", "g"))).await?;
    seed::exec(
        &pool,
        &format!(
            "INSERT INTO blocks (author_id, rkey, subject_id, created_at, rev, first_seen, last_seen)
             SELECT a.id, '3khfl' || lpad(g::text, 8, '0'), s.id, {BASE}, 1, {BASE}, now()
             FROM generate_series(1, 300000) g
             JOIN actors a ON a.did = {} JOIN actors s ON s.did = {}",
            did_sql("hfl", "(1 + g % 3000)"),
            did_sql("hfl", "(1 + (g / 3000) % 3000)")
        ),
    )
    .await?;
    seed::exec(&pool, "ANALYZE").await?;
    let size = n(&pool, "SELECT pg_database_size(current_database())").await? as u64;
    let estimate = 300_250 * ui_rows::ENTRY_BYTES;
    // Room for the two list indexes (their tables are empty), none for an
    // index on `blocks`.
    let budget = size + 2_000_000;
    let tight = Cfg {
        dsn: &dsn,
        plc: "https://127.0.0.1:9",
        budget,
        public_ui: "handle_warming_enabled = false",
    };
    let h = Srv::start("held", config_toml(&tight), &env).await?;
    tokio::time::sleep(Duration::from_secs(8)).await;
    let valid = valid_indexes(&pool).await?;
    let log = h.log();
    let cookie = admin_session(&pool, ADMIN_DID).await?;
    let dash = h.admin_get(&cookie, "/admin/alerts").await?;
    let anon = h.get("/admin").await?;
    let home = h.get("/").await?;
    c.check(
        "budget_bytes = database size + 2 MB, an index on `blocks` estimated at ~18.6 MB: the server starts and serves, the two indexes that fit are built, the two on `blocks` are not, the log says so at WARN on every check, and the gauge reads 2",
        valid == ["list_blocks_by_list_created", "list_items_by_list_created"]
            && log.lines().filter(|l| l.contains("WARN") && l.contains("the storage budget has no room") && l.contains("blocks_by_subject_created")).count() >= 2
            && !log.lines().any(|l| l.contains("sort index: building") && l.contains("\"blocks_by_"))
            && h.gauge("farsight_ui_sort_indexes_ready").await? == 2.0,
        format!("valid: {valid:?}; database {size} B, budget {budget} B, estimate {estimate} B"),
    );
    c.check(
        "the admin pages' Alerts tell a signed-in admin — \"2 of 4 indexes ready … the storage budget has no room (needs ~18.6 MB)\" — and nobody else: without a session the dashboard is the 303 to /enter, and the public home says nothing of it",
        dash.text.contains("Sorting by creation time: 2 of 4 indexes ready")
            && dash.text.contains("the storage budget has no room (needs ~18.6 MB)")
            && anon.status == 303
            && anon.header("location").as_deref() == Some("/enter")
            && !anon.text.contains("Sorting by creation time")
            && home.status == 200
            && !home.text.contains("Sorting by creation time"),
        support::truncate(between(&dash.text, "Sorting by creation time", "</div>").first().unwrap_or(&""), 200),
    );
    // The section without its index is in blocker id order, then record
    // key, ascending.
    let stored: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT a.did FROM blocks b JOIN actors a ON a.id = b.author_id WHERE b.subject_id = {sid} ORDER BY b.author_id, b.rkey"
    ))
    .fetch_all(&pool)
    .await
    .map_err(|e| e.to_string())?;
    let (walked, _) = walk(&h, &public_did(&subject), "blockers").await?;
    let admin = h.admin_get(&cookie, &lookup_did(&subject)).await?;
    let (admin_walked, _) =
        admin_walk(&h, &cookie, &lookup_did(&subject), "Incoming blocks").await?;
    c.check(
        "until its index is valid a section lists its rows by account, with the same columns and filters: the public table and the admin table both page in that order",
        walked == stored
            && walked.len() == 250
            && admin_walked == stored
            && heads(admin_section(&admin.text, "Incoming blocks").unwrap_or("")) == ["Blocker", "Record", "Created"],
        format!("{} public rows, {} admin rows", walked.len(), admin_walked.len()),
    );

    c.section("23. raising the budget releases the build");
    let settings = h.admin_get(&cookie, "/admin/settings").await?;
    let csrf = csrf_of(&settings.text).ok_or("no csrf on /admin/settings")?;
    let text = std::fs::read_to_string(h.dir.join("config.toml")).map_err(|e| e.to_string())?;
    let raised = text.replace(
        &format!("budget_bytes = {budget}"),
        "budget_bytes = 70000000000",
    );
    let save = h
        .admin
        .post_form(
            &format!("{}/admin/settings", h.base),
            &[("cookie", &cookie)],
            &[("csrf", csrf.as_str()), ("config", raised.as_str())],
        )
        .await?;
    let have = wait_indexes(&pool, 4, Duration::from_secs(120)).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let log = h.log();
    let dash = h.admin_get(&cookie, "/admin/alerts").await?;
    c.check(
        "storage.budget_bytes raised in Settings, without a restart: on its next check the task builds the two held indexes, the gauge reads 4 and the warning is gone from Alerts",
        save.status < 400
            && raised != text
            && have == 4
            && log.contains("sort indexes: all four ready")
            && h.gauge("farsight_ui_sort_indexes_ready").await? == 4.0
            && !dash.text.contains("Sorting by creation time"),
        format!("save {}; {have} of 4 valid", save.status),
    );
    let want = expected(&pool, Section::IncomingBlocks, sid, true).await?;
    let (walked, _) = walk(&h, &public_did(&subject), "blockers").await?;
    let (admin_walked, _) =
        admin_walk(&h, &cookie, &lookup_did(&subject), "Incoming blocks").await?;
    c.check(
        "the section switched by itself: the public table and the admin table now page by shown time",
        walked == want && walked != stored && admin_walked == want,
        format!("{} admin rows", admin_walked.len()),
    );
    Ok(())
}

// ------------------------------------------------- 24. Jetstream `time`

async fn check_jetstream(c: &mut Checks, url: &str) {
    c.section("24. what a v2 Jetstream's `time` field is (reported, not a gate)");
    use farsight_ingest::conn;
    use farsight_ingest::frame::Protocol;
    use farsight_ingest::resume::Cursor;
    let mut session = match conn::connect(url, Protocol::V2, Cursor::Live, false).await {
        Ok(s) => s,
        Err(e) => {
            c.unverified("frames of a live v2 Jetstream", format!("{url}: {e}"));
            return;
        }
    };
    let micros = |v: &Value| v.as_str().and_then(farsight_ingest::frame::parse_time_us);
    let (mut commits, mut with_witness, mut equal) = (0u32, 0u32, 0u32);
    let (mut max_skew, mut max_created_gap) = (0i64, 0i64);
    let mut sample = String::new();
    let started = Instant::now();
    while commits < 300 && started.elapsed() < Duration::from_secs(25) {
        let raw = match tokio::time::timeout(Duration::from_secs(5), session.next_raw()).await {
            Ok(Some(Ok(raw))) => raw,
            _ => break,
        };
        let now = Utc::now().timestamp_micros();
        let Ok(v) = serde_json::from_slice::<Value>(&raw) else {
            continue;
        };
        let p = &v["payload"];
        if !p["$type"].as_str().is_some_and(|t| t.ends_with("#commit")) {
            continue;
        }
        let Some(time) = micros(&p["time"]) else {
            continue;
        };
        commits += 1;
        max_skew = max_skew.max((now - time).abs());
        if let Some(w) = micros(&p["witnessedAt"]) {
            with_witness += 1;
            if w == time {
                equal += 1;
            }
        }
        if let Some(created) = micros(&p["record"]["createdAt"]) {
            max_created_gap = max_created_gap.max((created - time).abs());
        }
        if sample.is_empty() {
            let mut keys: Vec<&str> = p
                .as_object()
                .map(|o| o.keys().map(String::as_str).collect())
                .unwrap_or_default();
            keys.sort_unstable();
            sample = format!(
                "payload keys {keys:?}; time {} witnessedAt {}",
                p["time"], p["witnessedAt"]
            );
        }
    }
    session.close().await;
    if commits == 0 {
        c.unverified(
            "frames of a live v2 Jetstream",
            format!("{url}: no commit frame in 25 s"),
        );
        return;
    }
    c.check(
        "live v2 frames: every commit carries `time`, within seconds of this machine's clock whatever the record's own createdAt says — a time stamped where the event is received, not one taken from the record or its author's server",
        max_skew < 120_000_000,
        format!(
            "{commits} commits from {url}; `witnessedAt` on {with_witness}, equal to `time` on {equal}; largest |now - time| {:.1} s; largest |createdAt - time| {:.1} s; {sample}",
            max_skew as f64 / 1e6,
            max_created_gap as f64 / 1e6
        ),
    );
    println!(
        "   This source sends `witnessedAt` on {with_witness} of {commits} commit frames, so ingest's fallback to `time` is {}. Upstream source (jetstream, commit 3fa54fd): `time` is the display timestamp — the instance's witnessed time unless its operator ran a timestamp import (segment/event.go DisplayTimeUS); `witnessedAt` is never altered. Neither is taken from the PDS.",
        if with_witness == commits {
            "not used here"
        } else {
            "used for some frames"
        }
    );
}

// ------------------------------------------------- 27. write cost

async fn ingest(pool: &PgPool, ctx: &ApplyCtx<'_>, from: i64, count: i64) -> Result<f64, String> {
    let now = Utc::now();
    let stamp = now.timestamp_micros();
    let started = Instant::now();
    let mut i = from;
    while i < from + count {
        let mut b = Batch::new(Origin::Firehose);
        b.writes = (i..(i + 500).min(from + count))
            .map(|k| Write {
                author: Did::parse(&did("wca", 1 + (k % 200) as u64)).expect("did"),
                collection: Collection::Block,
                rkey: RecordKey::parse(&format!("3kw{k:010}")).expect("rkey"),
                stamp: Stamp::new(stamp + k),
                witness: Some(now),
                action: WriteAction::Upsert(Record::Block(BlockRecord {
                    subject: Did::parse(&did("wcs", 1 + (k % 2000) as u64)).expect("did"),
                    created_at: Some(now),
                })),
            })
            .collect();
        apply::apply(pool, ctx, &b)
            .await
            .map_err(|e| e.to_string())?;
        i += 500;
    }
    Ok(count as f64 / started.elapsed().as_secs_f64())
}

async fn check_write_cost(c: &mut Checks, pg: &Pg) -> Result<(), String> {
    c.section("27. write cost of the four indexes (block ingest through the apply path)");
    pg.create_db("s8write").await?;
    let pool = pg.pool("s8write", 4).await?;
    farsight_storage::migrate(&pool)
        .await
        .map_err(|e| e.to_string())?;
    for (prefix, count) in [("wca", 200), ("wcs", 2000)] {
        seed::exec(
            &pool,
            &format!(
                "INSERT INTO actors (did) SELECT {} FROM generate_series(1, {count}) g",
                did_sql(prefix, "g")
            ),
        )
        .await?;
    }
    let limits = Limits::defaults();
    let counters = CounterSink::new(7);
    let ctx = ApplyCtx {
        limits: &limits,
        gates: Gates::default(),
        counters: &counters,
    };
    const N: i64 = 30_000;
    let build = |on: bool| {
        let pool = pool.clone();
        async move {
            let mut conn = pool.acquire().await.map_err(|e| e.to_string())?;
            for s in Section::ALL {
                if on {
                    ui_rows::create_index(&mut conn, s)
                        .await
                        .map_err(|e| e.to_string())?;
                } else {
                    ui_rows::drop_index(&mut conn, s)
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
            Ok::<(), String>(())
        }
    };
    // Warm-up, then without / with / without / with: the table grows the
    // whole time, so each state is measured at two sizes.
    ingest(&pool, &ctx, 0, 5_000).await?;
    let mut rates = [0.0f64; 4];
    for (round, rate) in rates.iter_mut().enumerate() {
        build(round % 2 == 1).await?;
        *rate = ingest(&pool, &ctx, 5_000 + round as i64 * N, N).await?;
    }
    let stored = n(&pool, "SELECT count(*) FROM blocks").await?;
    let without = (rates[0] + rates[2]) / 2.0;
    let with = (rates[1] + rates[3]) / 2.0;
    let loss = 1.0 - with / without;
    let mut conn = pool.acquire().await.map_err(|e| e.to_string())?;
    let mut sizes = Vec::new();
    for s in Section::ALL {
        let bytes = ui_rows::index_bytes(&mut conn, s)
            .await
            .map_err(|e| e.to_string())?;
        sizes.push(format!("{} {} kB", s.index(), bytes / 1000));
    }
    c.check(
        "block ingest with the four indexes is within 20% of ingest without them",
        stored == 5_000 + 4 * N && loss <= 0.20,
        format!(
            "{N} blocks a round in batches of 500: without {:.0} and {:.0} rows/s, with {:.0} and {:.0} rows/s; mean {without:.0} → {with:.0} rows/s ({:+.1}%); at {stored} rows: {}",
            rates[0], rates[2], rates[1], rates[3], -loss * 100.0, sizes.join(", ")
        ),
    );
    Ok(())
}

// ------------------------------------------------- 28. representative plans

async fn check_representative(c: &mut Checks, pool: &PgPool, w: &World) -> Result<(), String> {
    c.section("28. plans of representative queries");
    let mut conn = pool.acquire().await.map_err(|e| e.to_string())?;
    let excluded: Vec<ActorId> = sqlx::query_scalar(&format!(
        "SELECT author_id FROM blocks WHERE subject_id = {} ORDER BY author_id LIMIT 500",
        w.w18_id
    ))
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| e.to_string())?;
    let shown = Filter {
        hide_inactive: true,
        show_suspended: false,
        show_taken_down: false,
        find: None,
        excluded: &[],
    };
    let cases: [(&str, Section, i64, Filter<'_>, i64); 5] = [
        (
            "dense subject (2,300 blockers)",
            Section::IncomingBlocks,
            w.w18_id,
            shown,
            0,
        ),
        (
            "sparse subject (1 blocker)",
            Section::IncomingBlocks,
            w.sparse_id,
            shown,
            0,
        ),
        (
            "dense author (3,000 blocks)",
            Section::OutgoingBlocks,
            w.dense_author_id,
            shown,
            0,
        ),
        (
            "paginated (page 21 of the dense subject)",
            Section::IncomingBlocks,
            w.w18_id,
            shown,
            1000,
        ),
        (
            "filtered (hidden statuses and 500 excluded accounts)",
            Section::IncomingBlocks,
            w.w18_id,
            Filter {
                hide_inactive: true,
                show_suspended: false,
                show_taken_down: false,
                find: None,
                excluded: &excluded,
            },
            0,
        ),
    ];
    let mut ok = true;
    let mut detail = Vec::new();
    for (name, section_, key, filter, offset) in cases {
        let plan = ui_rows::explain(
            &mut conn,
            section_,
            section_key(section_, key),
            Order::Shown,
            filter,
            offset,
            50,
        )
        .await
        .map_err(|e| e.to_string())?;
        let text = plan.join("\n");
        // A page deep into a section may read the section's rows through
        // another index on its key and sort them; it never scans the
        // table.
        let by_index = uses_index(&text, section_);
        let good = if offset > 0 {
            !text.contains(&format!("Seq Scan on {}", section_.table()))
        } else {
            by_index
        };
        ok &= good;
        let time = plan
            .iter()
            .find(|l| l.starts_with("Execution Time"))
            .cloned()
            .unwrap_or_default();
        detail.push(format!(
            "{name}: {} — {time}",
            if by_index {
                format!("{}, {}", section_.index(), order_of(&text))
            } else if good {
                "an index on the section's key, then a sort".to_owned()
            } else {
                text.clone()
            }
        ));
    }
    c.check(
        "four realistic first pages each read the table through the shown-time index, and a page deep into a dense section (page 21) reads it through an index too: never a sequential scan",
        ok,
        detail.join("; "),
    );
    Ok(())
}

// ------------------------------------------------- 9. the browser

fn check_browser(c: &mut Checks, a: &Srv, cookie: &str, w: &World) -> Result<(), String> {
    c.section("9. in a browser: the sticky header, cards on admin pages, local times");
    let dir = std::env::temp_dir().join("farsight-stage8-browser");
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    std::fs::write(dir.join("probes.mjs"), BROWSER_SCRIPT).map_err(|e| e.to_string())?;
    let value = cookie.trim_start_matches("farsight_admin=");
    let script = format!(
        "cd /work && ([ -d node_modules/playwright ] || npm install --no-save --no-audit --no-fund playwright@1.48.0 >npm.log 2>&1) && node probes.mjs '{}' '{value}' '{}' '{}'",
        a.base, w.s, w.t4
    );
    let out = Command::new("docker")
        .args(["run", "--rm", "--network", "host", "-v"])
        .arg(format!("{}:/work", dir.display()))
        .args([BROWSER_IMAGE, "sh", "-c", &script])
        .output()
        .map_err(|e| format!("docker: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut reported = 0;
    for line in stdout.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let (Some(what), Some(ok)) = (v["what"].as_str(), v["ok"].as_bool()) else {
            continue;
        };
        reported += 1;
        c.check(what, ok, v["detail"].as_str().unwrap_or(""));
    }
    if !out.status.success() || reported == 0 {
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

// ------------------------------------------------------------------ main

struct Args {
    browser: bool,
    skip_live: bool,
    jetstream: String,
}

async fn run(c: &mut Checks, pg: &Pg, args: &Args) -> Result<(), String> {
    let plc = Plc::start(STANDIN_ADDR).await?;
    println!("   stand-in PLC directory at {}", plc.base);
    pg.create_db("s8").await?;
    let dsn = pg.url("s8");
    // A runs the migrations and builds the indexes on the empty tables;
    // the pool and the seeding come after.
    let a = Srv::start(
        "a",
        config_toml(&Cfg {
            dsn: &dsn,
            plc: &plc.base,
            budget: 70_000_000_000,
            public_ui: "handle_warming_enabled = false",
        }),
        &[],
    )
    .await?;
    let pool = pg.pool("s8", 4).await?;
    if wait_indexes(&pool, 4, Duration::from_secs(120)).await? != 4 {
        return Err(format!(
            "the sort indexes were not built: {}",
            a.log_tail(10)
        ));
    }
    let w = seed_world(&pool).await?;
    let cookie = admin_session(&pool, ADMIN_DID).await?;
    // B: a record viewer, a card budget of one a second.
    let b = Srv::start(
        "b",
        config_toml(&Cfg {
            dsn: &dsn,
            plc: &plc.base,
            budget: 70_000_000_000,
            public_ui: &format!("record_viewer_url = \"{VIEWER}\"\ncard_rps = 1\ncard_burst = 1\nhandle_warming_enabled = false"),
        }),
        &[],
    )
    .await?;

    check_order(c, &a, &pool, &cookie, &w).await?;
    check_future_dates(c, &a, &pool, &w).await?;
    check_ties(c, &a, &pool, &w).await?;
    check_plans(c, &pool, &w).await?;
    check_pages(c, &a, &cookie, &w).await?;
    check_public_columns(c, &a, &w).await?;
    check_history_tab(c, &a, &plc, &w).await?;
    check_admin_history_tab(c, &a, &pool, &cookie, &plc, &w).await?;
    check_list_about(c, &a, &pool, &plc).await?;
    check_live(c, pg, args.skip_live).await?;
    if args.browser {
        check_browser(c, &a, &cookie, &w)?;
    } else {
        c.section("9. in a browser: the sticky header, cards on admin pages, local times");
        c.unverified("browser probes", "run with --browser");
    }
    check_admin_card(c, &a, &b, &cookie, &plc, &w).await?;
    check_admin_columns(c, &a, &b, &cookie, &w).await?;
    check_shared_budget(c, &b, &cookie, &plc).await?;
    check_warming(c, &a, &pool, &cookie, &plc, &w).await?;
    check_pass(c, &a, &pool, &cookie, &plc).await?;
    check_thumbnails(c, &a, &pool, &cookie, &plc).await?;
    check_top_lists(c, &a, &pool, &cookie).await?;
    check_blocking_lists(c, &a, &pool, &cookie, &w).await?;
    check_representative(c, &pool, &w).await?;
    drop(b);
    drop(a);
    check_index_build(c, pg).await?;
    check_jetstream(c, &args.jetstream).await;
    check_write_cost(c, pg).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let flag = |f: &str| argv.iter().any(|a| a == f);
    let args = Args {
        browser: flag("--browser"),
        skip_live: flag("--skip-live"),
        jetstream: argv
            .iter()
            .position(|a| a == "--jetstream")
            .and_then(|i| argv.get(i + 1).cloned())
            .unwrap_or_else(|| JETSTREAM.to_owned()),
    };
    println!("== farsight stage-8 harness: tables, sort indexes, handle warming");
    let bridge = match Bridge::create() {
        Ok(b) => b,
        Err(e) => {
            eprintln!("stand-in address: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    let pg = match Pg::start(flag("--keep")) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("postgres: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    let mut c = Checks::default();
    let result = async {
        pg.wait_ready(Duration::from_secs(120)).await?;
        run(&mut c, &pg, &args).await
    }
    .await;
    if let Err(e) = &result {
        c.check("harness ran to completion", false, e.clone());
    }
    pg.stop();
    drop(bridge);
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
