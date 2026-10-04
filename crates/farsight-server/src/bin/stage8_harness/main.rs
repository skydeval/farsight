//! `farsight-stage8-harness`: Phase B Mode A for UI v2.4.3 — tables that
//! sort by their rows' shown time, the four sort indexes and their
//! background build, queue-driven handle warming, and the admin tables
//! (sticky header, handles, profile cards, record cells, "First seen").
//!
//! Sections, numbered as the stage kickoff numbers its probes:
//!
//! - 1–6: sort key and clamp (order, future dates, missing dates, paging
//!   through ties, index use, cursors);
//! - 7–8: public pages (no Record column; handles after warming);
//! - 9–15: admin pages (sticky header and cards in a browser, handles,
//!   `/admin/card/{did}` with and without a session, record cells, "First seen",
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
//! Probes 25 and 26 are the stage-7 and stage-6 harnesses, run again.
//!
//! Sessions are created in the database, as in the stage-6 harness; the
//! sign-in itself is the stage-7 harness's subject.
//!
//! Flags: `--browser` runs `scripts/stage8-browser-probes.mjs` in the
//! Playwright image; `--skip-live` leaves out what needs the live network
//! (verified handles come from the real PLC directory and DNS);
//! `--jetstream URL` names the v2 Jetstream probe 24 reads from (default
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

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use farsight_core::record::BlockRecord;
use farsight_core::{Collection, Did, Record, RecordKey};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Write, WriteAction};
use farsight_storage::counters::CounterSink;
use farsight_storage::keys::Limits;
use farsight_storage::txn::Gates;
use farsight_storage::ui_rows::{self, Filter, Order, Position, Section};
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
const STALE_LINK: &str = "This link carries a position that can no longer be read";
const NO_FIRST_SEEN: &str = "Stored before Farsight kept this date";
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

/// The "Next page" link of an admin section.
fn admin_next(sec: &str) -> Option<String> {
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

/// The value of query parameter `key` of a relative link (cursors are
/// base64url and need no decoding).
fn query_of(link: &str, key: &str) -> Option<String> {
    link.split_once('?')?
        .1
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v.to_owned())
}

fn cursor_json(raw: &str) -> Option<Value> {
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(raw).ok()?).ok()
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
        // Newest first; a row with neither time last; then the rest of the
        // primary key, descending.
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
    // S: five kinds of row.
    let (s, s_id) = actor(did("sub", 1)).await?;
    let when = format!(
        "CASE g % 5 WHEN 0 THEN NULL
                    WHEN 1 THEN '9999-12-31T00:00:00Z'::timestamptz
                    WHEN 2 THEN {BASE} - make_interval(mins => g) - interval '1 day'
                    WHEN 3 THEN {BASE} - make_interval(hours => g)
                    ELSE NULL END"
    );
    let seen = format!("CASE WHEN g % 5 IN (0, 1, 2) THEN {BASE} - make_interval(mins => g) END");
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

    // T2: the future-date records go through the real write path later.
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
        "the public \"Blocked by\" table lists 305 records with stated dates in the future, in the past, missing, and rows stored before first_seen existed in exactly the order of the shown time, then blocker id, then record key, all descending",
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
        "\"Blocked by\" is 50 rows a page with numbered page controls, plain links with the page in the query: 305 records are seven pages; the controls show the first and last page, the current one and its neighbours, a gap where pages are left out, and arrows that are not links at the ends; page 4 is rows 151–200 of the order; a page past the end is an empty table that still leads back",
        pages == 7
            && row_dids(sec).len() == 50
            && controls_of(sec) == "(←) [1] 2 3 … 7 →"
            && next_of(sec) == Some(at(2))
            && controls_of(mid_sec) == "← 1 2 3 [4] 5 6 7 →"
            && row_dids(mid_sec) == want[150..200]
            && controls_of(last_sec) == "← 1 … 5 6 [7] (→)"
            && row_dids(last_sec).len() == 5
            && past.status == 200
            && row_dids(past_sec).is_empty()
            && controls_of(past_sec).starts_with("← 1 ")
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
            && controls_of(dense_sec) == "← 1 … 18 19 [20] 21 22 … 400 →"
            && row_dids(deep_sec).len() == 50
            && controls_of(deep_sec) == "← 1 … 398 399 [400] (→)",
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
    let neither = did("sbl", 4);
    let pos = |d: &str| got.iter().position(|x| x == d);
    c.section("3. a missing created_at falls back to first_seen");
    c.check(
        "a record that states no createdAt sorts at its first_seen — among the dated rows, ahead of every row that has neither time — and rows with neither time come last",
        pos(&undated) < pos(&neither)
            && pos(&undated).is_some()
            && pos(&honest).is_some()
            && got.len() >= 60
            && got[got.len() - 60..].iter().cloned().collect::<BTreeSet<_>>()
                == (1..=300u64)
                    .filter(|g| g % 5 == 4)
                    .map(|g| did("sbl", g))
                    .collect::<BTreeSet<_>>(),
        format!("undated at {:?}, neither at {:?}", pos(&undated), pos(&neither)),
    );
    // The database's own reading of the expression: never NULL.
    let nulls = n(
        pool,
        &format!(
            "SELECT count(*) FROM blocks WHERE subject_id = {} AND {} IS NULL",
            w.s_id,
            ui_rows::SHOWN_TIME
        ),
    )
    .await?;
    let last = n(
        pool,
        &format!(
            "SELECT count(*) FROM blocks WHERE subject_id = {} AND {} = '-infinity'",
            w.s_id,
            ui_rows::SHOWN_TIME
        ),
    )
    .await?;
    c.check(
        "the sort expression is never NULL: LEAST ignores a NULL argument, and a row with neither time is '-infinity' (60 such rows)",
        nulls == 0 && last == 60,
        format!("{nulls} NULL, {last} at -infinity"),
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
        stamp: stamp + i,
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
    c.section("4. keyset paging through rows with one shown time");
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
            "listblockers",
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
            show_banned: false,
            excluded: &[],
        };
        let first = ui_rows::rows(&mut conn, section_, key, Order::Shown, filter, None, 50)
            .await
            .map_err(|e| e.to_string())?;
        let after = first.last().map(farsight_storage::ui_rows::Row::position);
        for cursor in [None, after.as_ref()] {
            let plan = ui_rows::explain(&mut conn, section_, key, Order::Shown, filter, cursor, 50)
                .await
                .map_err(|e| e.to_string())?;
            let text = plan.join("\n");
            let good = uses_index(&text, section_);
            ok &= good;
            detail.push(format!(
                "{}{}: {}",
                section_.index(),
                if cursor.is_some() { " (cursor)" } else { "" },
                if good { order_of(&text) } else { text.as_str() }
            ));
        }
    }
    c.check(
        "EXPLAIN ANALYZE of the four section queries, first page and with a cursor: each reads its table through the section's shown-time index, never by a sequential scan",
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

async fn check_cursors(c: &mut Checks, a: &Srv, cookie: &str, w: &World) -> Result<(), String> {
    c.section("6. cursors (admin tables) and page numbers (public tables)");
    let page = a.admin_get(cookie, &lookup_did(&w.s)).await?;
    let next = admin_section(&page.text, "Incoming blocks")
        .and_then(admin_next)
        .unwrap_or_default();
    let raw = query_of(&next, "bc").unwrap_or_default();
    let v = cursor_json(&raw).unwrap_or(Value::Null);
    c.check(
        "a cursor of the new order (admin tables) is the codebase's opaque format — base64url of a JSON array — tagged \"t\": [\"t\", microseconds, blocker id, record key]",
        v[0] == "t" && v[1].is_i64() && v[2].is_i64() && v[3].is_string() && v.as_array().is_some_and(|x| x.len() == 4),
        v.to_string(),
    );
    let out = a.admin_get(cookie, &lookup_list(&w.l4_uri)).await?;
    let onext = admin_section(&out.text, "Members")
        .and_then(admin_next)
        .unwrap_or_default();
    let ov = cursor_json(&query_of(&onext, "mc").unwrap_or_default()).unwrap_or(Value::Null);
    c.check(
        "sections that list one author's or one list's records carry [\"t\", microseconds, record key]",
        ov[0] == "t" && ov[1].is_i64() && ov[2].is_string() && ov.as_array().is_some_and(|x| x.len() == 3),
        ov.to_string(),
    );
    // A cursor of the order before the indexes: [blocker id, record key].
    let old = URL_SAFE_NO_PAD.encode(br#"[42,"3ksbl00000007"]"#);
    let public = a.get(&public_did(&w.s)).await?;
    let public_next = section(&public.text, "blockers")
        .and_then(next_of)
        .unwrap_or_default();
    let stale = a.get(&format!("{}?bc={old}", public_did(&w.s))).await?;
    let garbage = a.get(&format!("{}?bc=%21%21", public_did(&w.s))).await?;
    let zero = a.get(&format!("{}?page=0", public_did(&w.s))).await?;
    c.check(
        "the public tables carry no cursor: the next page is ?page=2; an address that still has a cursor of an earlier version, readable or not, is redirected (301) to the first page of its section; a page number that is not one gets the 400 page with its \"Open the first page\" link — never a 500",
        public_next == format!("{}?page=2", public_did(&w.s))
            && [&stale, &garbage].iter().all(|r| {
                r.status == 301 && r.header("location").as_deref() == Some(public_did(&w.s).as_str())
            })
            && zero.status == 400
            && zero.text.contains("Open the first page")
            && zero.text.contains(&format!("href=\"{}\"", public_did(&w.s))),
        format!("{public_next}; {} / {} / {}", stale.status, garbage.status, zero.status),
    );
    let admin = a
        .admin_get(cookie, &format!("{}&bc={old}", lookup_did(&w.s)))
        .await?;
    let sec = admin_section(&admin.text, "Incoming blocks").unwrap_or("");
    c.check(
        "on the admin lookup page an old cursor gives the section's error line and no rows; the other sections still render",
        admin.status == 200
            && sec.contains(STALE_LINK)
            && row_dids(sec).is_empty()
            && admin.text.contains("<h2>Incoming listblocks</h2>"),
        support::truncate(sec, 200),
    );
    Ok(())
}

// --------------------------------------------------------- 7. public pages

async fn check_public_columns(c: &mut Checks, a: &Srv, w: &World) -> Result<(), String> {
    c.section("7. public tables have no Record column");
    let page = a.get(&public_did(&w.t4)).await?;
    let out = a.get(&public_did(&w.o4)).await?;
    let list = a.get(&w.l4_path).await?;
    let two = ["Account", "Created (Author Claim)"];
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
            && heads(section(&list.text, "listblockers").unwrap_or("")) == two
            && heads(section(&list.text, "members").unwrap_or(""))
                == ["Account", "Added (Owner Claim)"]
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
    // The harness holds for the worker; 15 s is the kickoff's bound.
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
        "anonymous: the bare 404 of a path that does not exist — same status, same body — never the 303 to /enter that the admin pages answer with: there is no redirect for a script to follow into the card (v2.4.3 §3.3)",
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
    let js = a.get("/static/public.js").await?;
    let old_js = a.get("/static/farsight.js").await?;
    let page = a.admin_get(cookie, &lookup_did(&w.s)).await?;
    c.check(
        "the three fixes of v2.4.3 §3.3 are in what the browser gets: admin pages load /static/public.js (the one UI script, ungated; its second name /static/farsight.js is gone); the script asks with credentials \"same-origin\" only for a link marked data-card-session and never injects an answer that is not a 200 or was reached through a redirect; it names no admin path",
        js.status == 200
            && old_js.status == 404
            && page.text.contains("<script src=\"/static/public.js?v=")
            && !page.text.contains("/static/farsight.js")
            && js.text.contains("link.hasAttribute(\"data-card-session\")")
            && js.text.contains("credentials: session ? \"same-origin\" : \"omit\"")
            && js.text.contains("r.status !== 200 || r.redirected")
            && !js.text.contains("/admin/")
            && !js.text.contains("/lookup"),
        format!("public.js {} / farsight.js {}", js.status, old_js.status),
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
    c.check(
        "record_viewer_url empty: the record cell is the at-uri as plain text",
        plain
            .text
            .contains(&format!("<td><code class=\"record\">{uri}</code></td>"))
            && !plain.text.contains("target=\"_blank\""),
        "plain text",
    );
    c.check(
        "record_viewer_url set: the cell is a link built from the template with the record's authority, collection and rkey, target=_blank rel=\"noopener noreferrer nofollow\", its text the at-uri",
        linked.text.contains(&format!(
            "<td><a class=\"record\" href=\"https://viewer.example/at/{first}/app.bsky.graph.block/{rkey}\" target=\"_blank\" rel=\"noopener noreferrer nofollow\"><code>{uri}</code></a></td>"
        )),
        "link",
    );
    let list = b.admin_get(cookie, &lookup_list(&w.l4_uri)).await?;
    let owner = did("lso", 1);
    c.check(
        "the list lookup links listitem records (in the owner's repo) and listblock records (in the blocker's) the same way",
        list.text.contains(&format!("href=\"https://viewer.example/at/{owner}/app.bsky.graph.listitem/3kitm000120\""))
            && list.text.contains(&format!("href=\"https://viewer.example/at/{}/app.bsky.graph.listblock/3klbk000120\"", did("llb", 120))),
        "listitem and listblock links",
    );

    c.section("14. \"First seen\" is an admin column: on the lookup pages, which need a session, and on no public page");
    let admin = a.admin_get(cookie, &lookup_did(&w.s)).await?;
    let asec = admin_section(&admin.text, "Incoming blocks").unwrap_or("");
    // Rows stored before the date was kept sort further down: look for
    // their cell on the following pages.
    let dash = format!("<td><span class=\"muted\" title=\"{NO_FIRST_SEEN}\">—</span></td>");
    let mut undated = asec.contains(&dash);
    let mut next = admin_next(asec);
    for _ in 0..7 {
        let Some(url) = next else { break };
        let r = a.admin_get(cookie, &url).await?;
        let sec = admin_section(&r.text, "Incoming blocks").unwrap_or("");
        undated |= sec.contains(&dash);
        next = admin_next(sec);
    }
    // Row g of S was first seen at BASE - g minutes: 23:5x on 2026-08-31,
    // minutes no stated createdAt of these rows falls in.
    c.check(
        "signed-in admin on /admin/lookup/did: a \"First seen\" column, last, with the stored first_seen in a <time> element, and \"—\" with its title for rows stored before the date was kept",
        heads(asec) == ["Blocker", "Record", "Created", "First seen"]
            && asec.contains("<td><time datetime=\"2026-08-31T23:59:00Z\">2026-08-31 23:59:00 UTC</time></td></tr>")
            && undated,
        format!("{:?}; undated cell seen: {undated}", heads(asec)),
    );
    let ladmin = a.admin_get(cookie, &lookup_list(&w.l4_uri)).await?;
    c.check(
        "the list lookup the same: Members and Inbound listblocks have \"First seen\"",
        heads(admin_section(&ladmin.text, "Members").unwrap_or(""))
            == ["Member", "Listitem", "Added", "First seen"]
            && heads(admin_section(&ladmin.text, "Inbound listblocks").unwrap_or(""))
                == ["Blocker", "Listblock", "Created", "First seen"],
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
                && !r.text.contains(NO_FIRST_SEEN)
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
        "a fresh database: the server is live in its normal start-up time, the nine migrations create no sort index, and the four appear afterwards, built by the server task (the log's first \"building\" line comes after \"serving\")",
        have == 4
            && health.status == 200
            && e.came_up < Duration::from_secs(60)
            && migrations == 9
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
    let dash = h.admin_get(&cookie, "/admin").await?;
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
        "the dashboard tells a signed-in admin — \"2 of 4 indexes ready … the storage budget has no room (needs ~18.6 MB)\" — and nobody else: without a session the dashboard is the 303 to /enter, and the public home says nothing of it",
        dash.text.contains("Sorting by creation time: 2 of 4 indexes ready")
            && dash.text.contains("the storage budget has no room (needs ~18.6 MB)")
            && anon.status == 303
            && anon.header("location").as_deref() == Some("/enter")
            && !anon.text.contains("Sorting by creation time")
            && home.status == 200
            && !home.text.contains("Sorting by creation time"),
        support::truncate(between(&dash.text, "Sorting by creation time", "</div>").first().unwrap_or(&""), 200),
    );
    // The section without its index keeps the order it had: blocker id,
    // then record key, ascending — and the cursor of that order.
    let stored: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT a.did FROM blocks b JOIN actors a ON a.id = b.author_id WHERE b.subject_id = {sid} ORDER BY b.author_id, b.rkey"
    ))
    .fetch_all(&pool)
    .await
    .map_err(|e| e.to_string())?;
    let (walked, _) = walk(&h, &public_did(&subject), "blockers").await?;
    let admin = h.admin_get(&cookie, &lookup_did(&subject)).await?;
    let old_next = admin_section(&admin.text, "Incoming blocks")
        .and_then(admin_next)
        .unwrap_or_default();
    let old_cursor =
        cursor_json(&query_of(&old_next, "bc").unwrap_or_default()).unwrap_or(Value::Null);
    c.check(
        "until its index is valid a section keeps its previous order and cursors, with the same columns and filters: the public table pages in the old order, the admin table with an untagged [id, rkey] cursor, and the admin table still has its \"First seen\" column",
        walked == stored
            && walked.len() == 250
            && old_cursor[0].is_i64()
            && old_cursor.as_array().is_some_and(|x| x.len() == 2)
            && heads(admin_section(&admin.text, "Incoming blocks").unwrap_or("")) == ["Blocker", "Record", "Created", "First seen"],
        format!("cursor {old_cursor}"),
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
    let dash = h.admin_get(&cookie, "/admin").await?;
    c.check(
        "storage.budget_bytes raised in Settings, without a restart: on its next check the task builds the two held indexes, the gauge reads 4 and the dashboard warning is gone",
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
    let stale = h.admin_get(&cookie, &old_next).await?;
    let stale_sec = admin_section(&stale.text, "Incoming blocks").unwrap_or("");
    c.check(
        "the section switched by itself: the public table now pages by shown time, and the admin link made before the switch gives the section's error line and no rows",
        walked == want
            && walked != stored
            && stale.status == 200
            && stale_sec.contains(STALE_LINK)
            && row_dids(stale_sec).is_empty(),
        format!("stale link: {}", stale.status),
    );
    Ok(())
}

// ------------------------------------------------- 24. Jetstream `time`

async fn check_jetstream(c: &mut Checks, url: &str) {
    c.section("24. what a v2 Jetstream's `time` field is (T15; reported, not a gate)");
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
        "   T15: this source sends `witnessedAt` on {with_witness} of {commits} commit frames, so ingest's fallback to `time` is {}. Upstream source (jetstream, commit 3fa54fd): `time` is the display timestamp — the instance's witnessed time unless its operator ran a timestamp import (segment/event.go DisplayTimeUS); `witnessedAt` is never altered. Neither is taken from the PDS.",
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
                stamp: stamp + k,
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
    let excluded: Vec<i64> = sqlx::query_scalar(&format!(
        "SELECT author_id FROM blocks WHERE subject_id = {} ORDER BY author_id LIMIT 500",
        w.w18_id
    ))
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| e.to_string())?;
    let shown = Filter {
        hide_inactive: true,
        show_suspended: false,
        show_banned: false,
        excluded: &[],
    };
    let deep = ui_rows::rows(
        &mut conn,
        Section::IncomingBlocks,
        w.w18_id,
        Order::Shown,
        shown,
        None,
        1000,
    )
    .await
    .map_err(|e| e.to_string())?;
    let middle: Option<Position> = deep.last().map(farsight_storage::ui_rows::Row::position);
    let cases: [(&str, Section, i64, Filter<'_>, Option<&Position>); 5] = [
        (
            "dense subject (2,300 blockers)",
            Section::IncomingBlocks,
            w.w18_id,
            shown,
            None,
        ),
        (
            "sparse subject (1 blocker)",
            Section::IncomingBlocks,
            w.sparse_id,
            shown,
            None,
        ),
        (
            "dense author (3,000 blocks)",
            Section::OutgoingBlocks,
            w.dense_author_id,
            shown,
            None,
        ),
        (
            "paginated (page 21 of the dense subject)",
            Section::IncomingBlocks,
            w.w18_id,
            shown,
            middle.as_ref(),
        ),
        (
            "filtered (hidden statuses and 500 excluded accounts)",
            Section::IncomingBlocks,
            w.w18_id,
            Filter {
                hide_inactive: true,
                show_suspended: false,
                show_banned: false,
                excluded: &excluded,
            },
            None,
        ),
    ];
    let mut ok = true;
    let mut detail = Vec::new();
    for (name, section_, key, filter, after) in cases {
        let plan = ui_rows::explain(&mut conn, section_, key, Order::Shown, filter, after, 50)
            .await
            .map_err(|e| e.to_string())?;
        let text = plan.join("\n");
        let good = uses_index(&text, section_);
        ok &= good;
        let time = plan
            .iter()
            .find(|l| l.starts_with("Execution Time"))
            .cloned()
            .unwrap_or_default();
        detail.push(format!(
            "{name}: {} — {time}",
            if good {
                format!("{}, {}", section_.index(), order_of(&text))
            } else {
                text.clone()
            }
        ));
    }
    c.check(
        "five realistic queries each read the table through the shown-time index, never by a sequential scan",
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
    check_cursors(c, &a, &cookie, &w).await?;
    check_public_columns(c, &a, &w).await?;
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
    println!("== farsight stage-8 harness: Mode A (UI v2.4.3)");
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
