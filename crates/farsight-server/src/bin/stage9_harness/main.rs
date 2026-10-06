//! `farsight-stage9-harness`: Phase B Mode A for UI v2.5.2 — the public
//! UI at the root, the admin UI under `/admin` behind a session, the two
//! switches (`access.public_ui`, `access.admin_ui`), the retired
//! `access.ui`, that no page has a second address, the static assets,
//! `robots.txt`, and the wizard's access step.
//!
//! Sections, with the numbers the stage kickoff gives its probes:
//!
//! - 1: routing in the four switch combinations (1–4), sessions (10–12);
//! - 2: config (5–9);
//! - 3: no second address for any page (13–16);
//! - 4: static assets (17–21);
//! - 5: `/` of an API-only instance (22–23);
//! - 6: the wizard (24–27);
//! - 7: `robots.txt` (28–30), the client metadata (31–32);
//! - 8: what the routing relies on from axum (33–34), metric labels (36),
//!   the admin card (38);
//! - 9: in a browser, with `--browser`: the access step without script
//!   (25), htmx and a refused poll (35), times on the new paths (37), the
//!   admin card on hover (38).
//!
//! Probes 39–43 are the stage-6, stage-7 and stage-8 harnesses, run again
//! with their paths moved.
//!
//! Sessions are created in the database, as in the stage-6 harness; the
//! sign-in itself is the stage-7 harness's subject.
//!
//! Flags: `--browser` runs `scripts/stage9-browser-probes.mjs` in the
//! Playwright image; `--keep` keeps the Postgres container.

#[allow(dead_code)]
#[path = "../stage3_harness/seed.rs"]
mod seed;
#[allow(dead_code)]
#[path = "../stage3_harness/support.rs"]
mod support;

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::support::{
    ADMIN_DID, Checks, Http, Pg, Resp, Server, admin_session, csrf_of, enc, farsight_bin,
    free_port, set_cookie,
};

const HOSTNAME: &str = "farsight.test";
/// A well-formed admin token: `fsa_` and 43 base64url characters.
const ADMIN_TOKEN: &str = "fsa_stage9harnessadmintokenAAAAAAAAAAAAAAAAAAAA";
const NS: &str = "app.nearhorizon.farsight";
const BROWSER_IMAGE: &str = "mcr.microsoft.com/playwright:v1.48.0-jammy";
const BROWSER_SCRIPT: &str = include_str!("../../../../../scripts/stage9-browser-probes.mjs");
/// The body of every route that does not exist, or does not exist in
/// this configuration.
const BARE: &str = "not found";
const RETIRED: &str = "`access.ui` is retired";
const NEEDS_RESTART: &str = "`access.admin_ui` requires a restart";
const STATIC: [(&str, &str); 6] = [
    ("/static/farsight.css", "text/css"),
    ("/static/public.css", "text/css"),
    ("/static/public.js", "text/javascript"),
    ("/static/admin.js", "text/javascript"),
    ("/static/htmx.min.js", "text/javascript"),
    ("/static/og-default.png", "image/png"),
];
/// Admin routes a browser navigates to.
const ADMIN_PAGES: [&str; 6] = [
    "/admin",
    "/admin/lookup/did",
    "/admin/lookup/list",
    "/admin/ops",
    "/admin/settings",
    "/admin/reset",
];

// ------------------------------------------------------------------ servers

/// The `[access]` part of a config under test.
struct Access<'a> {
    /// Lines of `[access]` besides `reads`.
    lines: &'a str,
    reads: &'a str,
    crawlable: bool,
}

/// A `config.toml` and the metrics address it names.
fn config_toml(dsn: &str, a: &Access<'_>) -> (String, String) {
    let metrics = format!("127.0.0.1:{}", free_port().unwrap_or(0));
    let text = format!(
        r#"[server]
hostname = "{HOSTNAME}"
contact = "mailto:ops@farsight.test"

[storage]
database_url = "{dsn}"

[firehose]
urls = ["ws://127.0.0.1:9"]

[backfill]
plc_url = "http://127.0.0.1:9"

[access]
reads = "{}"
{}

[public_ui]
crawlable = {}
handle_warming_enabled = false
rate_limit_rps = 1000
rate_limit_burst = 10000

[auth]
admin_token_sha256 = "{}"

[metrics]
bind = "{metrics}"
"#,
        a.reads,
        a.lines,
        a.crawlable,
        farsight_api::auth::hex(&farsight_api::auth::sha256(ADMIN_TOKEN)),
    );
    (text, format!("http://{metrics}"))
}

/// One `farsight` process in normal mode.
struct Srv {
    base: String,
    metrics: String,
    dir: PathBuf,
    child: std::process::Child,
    next_ip: AtomicU32,
    admin: Http,
}

impl Srv {
    fn spawn(name: &str, text: &str) -> Result<(PathBuf, std::process::Child, u16), String> {
        let dir =
            std::env::temp_dir().join(format!("farsight-stage9-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        std::fs::write(dir.join("config.toml"), text).map_err(|e| e.to_string())?;
        let port = free_port()?;
        let log = std::fs::File::create(dir.join("server.log")).map_err(|e| e.to_string())?;
        let log2 = log.try_clone().map_err(|e| e.to_string())?;
        let child = Command::new(farsight_bin())
            .env("FARSIGHT_CONFIG", dir.join("config.toml"))
            .env("FARSIGHT__SERVER__BIND", format!("127.0.0.1:{port}"))
            .env("RUST_LOG", "info,sqlx=warn,hyper=warn,reqwest=warn")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log2))
            .spawn()
            .map_err(|e| format!("spawning farsight: {e}"))?;
        Ok((dir, child, port))
    }

    async fn start(name: &str, dsn: &str, access: &Access<'_>) -> Result<Srv, String> {
        let (text, metrics) = config_toml(dsn, access);
        let (dir, child, port) = Srv::spawn(name, &text)?;
        let s = Srv {
            base: format!("http://127.0.0.1:{port}"),
            metrics,
            dir,
            child,
            next_ip: AtomicU32::new(0),
            admin: Http::new(Some("127.0.0.250".parse().expect("ip"))),
        };
        let http = Http::new(None);
        let started = Instant::now();
        loop {
            if let Ok(r) = http.get(&format!("{}/livez", s.base), &[]).await {
                if r.status == 200 {
                    return Ok(s);
                }
            }
            if started.elapsed() > Duration::from_secs(90) {
                return Err(format!(
                    "farsight {name} did not come up: {}",
                    s.log_tail(15)
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn config_path(&self) -> PathBuf {
        self.dir.join("config.toml")
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
        let ip: std::net::IpAddr =
            format!("127.{}.{}.{}", 8 + n / 62_500, (n / 250) % 250, 1 + n % 250)
                .parse()
                .expect("ip");
        Http::new(Some(ip))
    }

    /// `GET path` anonymously, from a fresh address.
    async fn get(&self, path: &str) -> Result<Resp, String> {
        self.fresh().get(&format!("{}{path}", self.base), &[]).await
    }

    /// `GET path` anonymously with extra headers.
    async fn get_with(&self, path: &str, headers: &[(&str, &str)]) -> Result<Resp, String> {
        self.fresh()
            .get(&format!("{}{path}", self.base), headers)
            .await
    }

    /// `GET path` with a session cookie.
    async fn admin_get(&self, cookie: &str, path: &str) -> Result<Resp, String> {
        self.admin
            .get(&format!("{}{path}", self.base), &[("cookie", cookie)])
            .await
    }

    async fn post(
        &self,
        path: &str,
        headers: &[(&str, &str)],
        form: &[(&str, &str)],
    ) -> Result<Resp, String> {
        self.admin
            .post_form(&format!("{}{path}", self.base), headers, form)
            .await
    }

    /// Pauses or resumes the sweep over XRPC: an in-process config edit
    /// that has nothing to do with the admin UI.
    async fn pause_sweep(&self, paused: bool) -> Result<Resp, String> {
        self.admin
            .post_json(
                &format!("{}/xrpc/{NS}.admin.pauseSweep", self.base),
                &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
                &json!({ "paused": paused }),
            )
            .await
    }

    async fn metrics_text(&self) -> Result<String, String> {
        Ok(Http::new(None)
            .get(&format!("{}/metrics", self.metrics), &[])
            .await?
            .text)
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

// ------------------------------------------------------------------ helpers

fn loc(r: &Resp) -> String {
    r.header("location").unwrap_or_default()
}

fn cc(r: &Resp) -> String {
    r.header("cache-control").unwrap_or_default()
}

/// The answer of a path that does not exist: the same bytes whatever the
/// reason.
fn bare(r: &Resp) -> bool {
    r.status == 404 && r.text == BARE && cc(r) == "no-store" && r.header("location").is_none()
}

/// A redirect to the sign-in page.
fn to_enter(r: &Resp) -> bool {
    r.status == 303 && loc(r) == "/enter"
}

fn brief(r: &Resp) -> String {
    format!(
        "{} loc={:?} cc={:?} {}",
        r.status,
        loc(r),
        cc(r),
        support::truncate(&r.text.replace('\n', " "), 60)
    )
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

/// Checks `path` on `s` for every `(path, ok)` and reports them as one
/// check, naming the ones that failed.
async fn all(
    c: &mut Checks,
    what: &str,
    s: &Srv,
    cookie: Option<&str>,
    paths: &[&str],
    ok: impl Fn(&Resp) -> bool,
) -> Result<(), String> {
    let mut bad = Vec::new();
    for p in paths {
        let r = match cookie {
            Some(k) => s.admin_get(k, p).await?,
            None => s.get(p).await?,
        };
        if !ok(&r) {
            bad.push(format!("{p}: {}", brief(&r)));
        }
    }
    c.check(
        what,
        bad.is_empty(),
        if bad.is_empty() {
            paths.join(" ")
        } else {
            bad.join(" | ")
        },
    );
    Ok(())
}

// ------------------------------------------------------- 1. routing by switch

struct World {
    /// An account with a block on record.
    subject: String,
    /// The account that blocks it.
    blocker: String,
}

async fn seed_world(pool: &sqlx::PgPool) -> Result<World, String> {
    let w = World {
        subject: seed::did("sub", 1),
        blocker: seed::did("blk", 1),
    };
    let s = seed::actor(pool, &w.subject).await?;
    let b = seed::actor(pool, &w.blocker).await?;
    seed::block(pool, b, "3kaaaaaaaaaa2", s).await?;
    Ok(w)
}

/// Public UI on, admin UI on.
async fn check_both(c: &mut Checks, a: &Srv, cookie: &str, w: &World) -> Result<(), String> {
    c.section("1a. public UI on, admin UI on (probes 1, 10–12)");
    let home = a.get("/").await?;
    c.check(
        "/ is the public home: 200, cacheable, its search form posts to /search, no admin or sign-in link",
        home.status == 200
            && cc(&home) == "public, max-age=60"
            && home.text.contains("action=\"/search\"")
            && !home.text.contains("/admin")
            && !home.text.contains("/enter")
            && !home.text.contains("/public/")
            && !home.text.contains("\"/public\""),
        brief(&home),
    );
    let did = a.get(&format!("/did/{}", w.subject)).await?;
    c.check(
        "/did/{did} is the public account page, with the blocker linked at /did/…",
        did.status == 200
            && did.text.contains(&format!("href=\"/did/{}\"", w.blocker))
            && did.text.contains("/static/public.css")
            && did.text.contains("/static/public.js")
            && !did.text.contains("/public/"),
        brief(&did),
    );
    // The fingerprint the page asks for its stylesheet under.
    let version = did
        .text
        .split("/static/public.css?v=")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or("")
        .to_owned();
    let stamped = a.get(&format!("/static/public.css?v={version}")).await?;
    let plain = a.get("/static/public.css").await?;
    let dash = a.admin_get(cookie, "/admin").await?;
    c.check(
        "pages name their stylesheet and scripts with this build's fingerprint (?v=, ten hex digits, the same on public and admin pages), so a new build's files are fetched at once instead of after the hour they may be cached; the address with the fingerprint and the one without serve the same file",
        version.len() == 10
            && version.bytes().all(|b| b.is_ascii_hexdigit())
            && did.text.contains(&format!("/static/public.js?v={version}\""))
            && did.text.contains(&format!("/static/htmx.min.js?v={version}\""))
            && dash.text.contains(&format!("/static/farsight.css?v={version}\""))
            && dash.text.contains(&format!("/static/admin.js?v={version}\""))
            && stamped.status == 200
            && stamped.text == plain.text
            && cc(&stamped) == "public, max-age=3600",
        format!("v={version}; {} / {}", brief(&stamped), brief(&plain)),
    );
    let search = a.get(&format!("/search?q={}", enc(&w.subject))).await?;
    c.check(
        "/search redirects to the account's page at the root",
        search.status == 303 && loc(&search) == format!("/did/{}", w.subject),
        brief(&search),
    );
    let card = a.get(&format!("/card/{}", w.subject)).await?;
    c.check(
        "/card/{did} answers with a card fragment",
        card.status == 200 && !card.text.contains("<html") && !card.text.contains("/public/"),
        brief(&card),
    );
    all(
        c,
        "every admin page without a session: 303 to /enter, never the page (D1)",
        a,
        None,
        &ADMIN_PAGES,
        to_enter,
    )
    .await?;
    let lookup = format!("/admin/lookup/did?q={}", enc(&w.subject));
    let hist = format!("/admin/did/{}/history", w.subject);
    let anon = a.get(&lookup).await?;
    let anon_hist = a.get(&hist).await?;
    c.check(
        "an anonymous lookup with a query, and a history page: 303 to /enter, nothing of the data",
        to_enter(&anon) && to_enter(&anon_hist) && !anon.text.contains(&w.blocker),
        format!("{} | {}", brief(&anon), brief(&anon_hist)),
    );
    all(
        c,
        "every admin page with a session: 200, no-store, private",
        a,
        Some(cookie),
        &ADMIN_PAGES,
        |r| r.status == 200 && cc(r) == "no-store, private",
    )
    .await?;
    let mut loose = Vec::new();
    for path in ADMIN_PAGES
        .iter()
        .copied()
        .chain(["/admin/dashboard/fragment", "/admin/alerts"])
    {
        let r = a.admin_get(cookie, path).await?;
        let csp = r.header("content-security-policy").unwrap_or_default();
        if r.status != 200
            || !csp.contains("default-src 'none'")
            || !csp.contains("script-src 'self'")
            || !csp.contains("style-src 'self'")
            || !csp.contains("frame-ancestors 'none'")
            || csp.contains("unsafe")
            || r.text.contains(" style=\"")
            || r.text.contains("<style")
            || r.text.contains(" onclick=")
        {
            loose.push(format!("{path}: {} {csp:?}", r.status));
        }
    }
    let enter = a.get("/enter").await?;
    let enter_csp = enter.header("content-security-policy").unwrap_or_default();
    c.check(
        "every admin page and fragment carries a Content-Security-Policy that allows no inline script or style and no framing, and has neither a style attribute nor a style element; the sign-in page's differs only in letting its form be answered by a redirect to an https origin",
        loose.is_empty()
            && enter_csp.contains("form-action 'self' https:")
            && enter_csp.contains("script-src 'self'")
            && !enter.text.contains(" style=\""),
        format!("{loose:?}; /enter: {enter_csp:?}"),
    );
    let dash = a.admin_get(cookie, "/admin").await?;
    c.check(
        "the dashboard's links, poll and logout form all name /admin paths; the page loads the admin pages' own script, /static/admin.js",
        dash.text.contains("hx-get=\"/admin/dashboard/fragment\"")
            && dash.text.contains("href=\"/admin/lookup/did\"")
            && dash.text.contains("href=\"/admin/ops\"")
            && dash.text.contains("href=\"/admin/settings\"")
            && dash.text.contains("action=\"/admin/logout\"")
            && dash.text.contains("src=\"/static/admin.js?v=")
            && !dash.text.contains("/static/public.js")
            && !dash.text.contains("farsight.js"),
        support::truncate(&dash.text, 120),
    );
    let page = a.admin_get(cookie, &lookup).await?;
    c.check(
        "the signed-in lookup shows the blocker, a button that copies its record's address, a card link and the History tab",
        page.status == 200
            && page.text.contains(&w.blocker)
            && page.text.contains("class=\"copy-uri\" data-copy=\"at://")
            && page.text.contains("data-card-session")
            && page.text.contains("data-tab=\"history\"")
            && page
                .text
                .contains(&format!("/admin/lookup/did?q={}", enc(&w.blocker))),
        brief(&page),
    );
    let frag_anon = a
        .get_with("/admin/dashboard/fragment", &[("hx-request", "true")])
        .await?;
    let frag_nav = a.get("/admin/dashboard/fragment").await?;
    let frag = a.admin_get(cookie, "/admin/dashboard/fragment").await?;
    c.check(
        "the dashboard poll without a session: the bare 404 when htmx asks, 303 for a navigation; 200 with a session",
        bare(&frag_anon) && to_enter(&frag_nav) && frag.status == 200 && frag.text.contains("id=\"dash\""),
        format!("{} | {} | {}", brief(&frag_anon), brief(&frag_nav), frag.status),
    );
    let slash = a.get("/admin/").await?;
    c.check(
        "/admin/ redirects to /admin",
        slash.status == 303 && loc(&slash) == "/admin",
        brief(&slash),
    );
    let enter = a.get("/enter").await?;
    let signed = a.admin_get(cookie, "/enter").await?;
    c.check(
        "/enter is the sign-in page (header: brand only); a signed-in visitor is sent to /admin",
        enter.status == 200
            && enter.text.contains("action=\"/enter\"")
            && !enter.text.contains(">Dashboard<")
            && signed.status == 303
            && loc(&signed) == "/admin",
        format!("{} | {}", brief(&enter), brief(&signed)),
    );
    let csrf = csrf_of(&dash.text).unwrap_or_default();
    let nocookie = a.post("/admin/logout", &[], &[]).await?;
    c.check(
        "POST /admin/logout without a session: 303 to /enter",
        to_enter(&nocookie),
        brief(&nocookie),
    );
    let out = a
        .post("/admin/logout", &[("cookie", cookie)], &[("csrf", &csrf)])
        .await?;
    let after = a.admin_get(cookie, "/admin").await?;
    c.check(
        "POST /admin/logout with the session: 303 to /enter, and the session is over",
        to_enter(&out) && to_enter(&after),
        format!("{} | {}", brief(&out), brief(&after)),
    );
    Ok(())
}

/// Public UI on, admin UI off (and an admin DID in the file).
async fn check_public_only(c: &mut Checks, b: &Srv, cookie: &str, w: &World) -> Result<(), String> {
    c.section("1b. public UI on, admin UI off (probes 2, 9)");
    let home = b.get("/").await?;
    let did = b.get(&format!("/did/{}", w.subject)).await?;
    c.check(
        "/ and /did/{did} are the public pages",
        home.status == 200 && home.text.contains("action=\"/search\"") && did.status == 200,
        format!("{} | {}", brief(&home), did.status),
    );
    let mut gone: Vec<&str> = ADMIN_PAGES.to_vec();
    let card = format!("/admin/card/{}", w.subject);
    let hist = format!("/admin/did/{}/history", w.subject);
    gone.extend([
        "/admin/",
        "/admin/anything",
        "/admin/dashboard/fragment",
        card.as_str(),
        hist.as_str(),
        "/enter",
        "/enter/callback",
        "/.well-known/atproto-oauth-client-metadata",
        "/lookup/did",
        "/lookup/list",
        "/ops",
        "/settings",
        "/reset",
    ]);
    all(
        c,
        "every admin page, the sign-in, its callback, the client metadata and the old admin addresses: the bare 404",
        b,
        None,
        &gone,
        bare,
    )
    .await?;
    all(
        c,
        "the same with a session cookie that would be valid if the admin UI were on (the admin DID is in the file and ignored)",
        b,
        Some(cookie),
        &gone,
        bare,
    )
    .await?;
    let meta = b
        .get_with(
            "/.well-known/atproto-oauth-client-metadata",
            &[("host", HOSTNAME)],
        )
        .await?;
    c.check(
        "the client metadata asked for under the instance's own hostname: still the bare 404 (probe 32)",
        bare(&meta),
        brief(&meta),
    );
    let logout = b.post("/admin/logout", &[("cookie", cookie)], &[]).await?;
    c.check(
        "POST /admin/logout: the bare 404",
        bare(&logout),
        brief(&logout),
    );
    c.check(
        "the server started with admin_ui = false and an admin_did, and says nothing about either",
        !b.log().contains("admin sign-in"),
        b.log_tail(3),
    );
    Ok(())
}

/// Public UI off, admin UI on, reads gated.
async fn check_admin_only(c: &mut Checks, s: &Srv, cookie: &str, w: &World) -> Result<(), String> {
    c.section("1c. public UI off, admin UI on, reads = api_key (probes 3, 11)");
    let root = s.get("/").await?;
    c.check(
        "/ redirects to /admin: 303, no-store (not a permanent redirect: the public UI can be turned on)",
        root.status == 303 && loc(&root) == "/admin" && cc(&root).contains("no-store"),
        brief(&root),
    );
    let did = format!("/did/{}", w.subject);
    let card = format!("/card/{}", w.subject);
    let list = format!("/list/{}/3kaaaaaaaaaa2", w.subject);
    let old = format!("/public/did/{}", w.subject);
    all(
        c,
        "every public page, the old public addresses and an unknown path: the bare 404",
        s,
        None,
        &[
            did.as_str(),
            card.as_str(),
            list.as_str(),
            "/search?q=x",
            "/public",
            "/public/search?q=x",
            old.as_str(),
            "/public/static/public.css",
            "/public/nonsense",
            "/nonsense",
        ],
        bare,
    )
    .await?;
    all(
        c,
        "admin pages without a session: 303 to /enter, also the lookups while reads are gated",
        s,
        None,
        &ADMIN_PAGES,
        to_enter,
    )
    .await?;
    let lookup = s
        .admin_get(cookie, &format!("/admin/lookup/did?q={}", enc(&w.subject)))
        .await?;
    let dash = s.admin_get(cookie, "/admin").await?;
    c.check(
        "with a session the dashboard renders and the lookup reads, whatever `reads` is",
        dash.status == 200 && lookup.status == 200 && lookup.text.contains(&w.blocker),
        format!("{} | {}", dash.status, brief(&lookup)),
    );
    let enter = s.get("/enter").await?;
    c.check(
        "/enter is the sign-in page",
        enter.status == 200,
        brief(&enter),
    );
    let old_admin = s.get("/settings").await?;
    c.check(
        "an admin page has no address at the root: /settings is an unknown path",
        bare(&old_admin) && old_admin.header("location").is_none(),
        brief(&old_admin),
    );
    Ok(())
}

/// Neither UI; the admin UI switched off by the retired key alone.
async fn check_api_only(c: &mut Checks, d: &Srv, w: &World) -> Result<(), String> {
    c.section("1d. neither UI: API only (probes 4, 22, 23)");
    let root = d.get("/").await?;
    c.check(
        "/ is a short text page: 200, text/plain, public, max-age=300, nosniff",
        root.status == 200
            && root
                .header("content-type")
                .is_some_and(|t| t.starts_with("text/plain"))
            && cc(&root) == "public, max-age=300"
            && root.header("x-content-type-options").as_deref() == Some("nosniff"),
        brief(&root),
    );
    c.check(
        "it names Farsight and the protocol's site, and nothing of the instance: no link, no path, no hostname, no contact",
        root.text.starts_with("Farsight\n")
            && root.text.contains("https://atproto.com")
            && !root.text.contains(HOSTNAME)
            && !root.text.contains("ops@")
            && !root.text.contains("/admin")
            && !root.text.contains("/xrpc")
            && root.text.len() < 200,
        root.text.replace('\n', " / "),
    );
    let did = format!("/did/{}", w.subject);
    let card = format!("/admin/card/{}", w.subject);
    let mut gone: Vec<&str> = ADMIN_PAGES.to_vec();
    gone.extend([
        "/enter",
        "/enter/callback",
        "/.well-known/atproto-oauth-client-metadata",
        card.as_str(),
        did.as_str(),
        "/search?q=x",
        "/public",
        "/lookup/did",
        "/settings",
    ]);
    all(
        c,
        "every admin and public path: the bare 404",
        d,
        None,
        &gone,
        bare,
    )
    .await?;
    let livez = d.get("/livez").await?;
    let health = d.get("/health").await?;
    let stats = d.get(&format!("/xrpc/{NS}.query.getStats")).await?;
    let admin = d
        .admin
        .get(
            &format!("{}/xrpc/{NS}.admin.listErrors", d.base),
            &[("authorization", &format!("Bearer {ADMIN_TOKEN}"))],
        )
        .await?;
    let noauth = d.get(&format!("/xrpc/{NS}.admin.listErrors")).await?;
    c.check(
        "/livez 200; /xrpc answers (getStats 200, an admin method 200 with the token and 401 without); /health is its own answer",
        livez.status == 200 && stats.status == 200 && admin.status == 200 && noauth.status == 401,
        format!(
            "livez {} | getStats {} | admin {} / {} | health {} (503 here: the harness has no firehose)",
            livez.status, stats.status, admin.status, noauth.status, health.status
        ),
    );
    Ok(())
}

// ------------------------------------------------------------------ 2. config

async fn check_config(
    c: &mut Checks,
    pg: &Pg,
    a: &Srv,
    cookie: &str,
    d: &Srv,
) -> Result<(), String> {
    c.section("2. config: the retired access.ui, admin_ui at restart only (probes 5–9)");
    let warned = |s: &Srv| s.log().matches(RETIRED).count();
    c.check(
        "a file with ui = \"public_read\" and no admin_ui loads with the admin UI on and one warning, which says the dashboard is no longer public",
        warned(a) == 1 && a.log().contains("no longer public"),
        format!("{} warning(s)", warned(a)),
    );
    c.check(
        "ui = \"disabled\" and no admin_ui: one warning, and the admin UI is off (section 1d ran on this server)",
        warned(d) == 1 && d.log().contains("access.admin_ui = false"),
        format!("{} warning(s)", warned(d)),
    );

    // Any string loads; no admin DID: unconfigured, and it loads.
    pg.create_db("s9e").await?;
    let e = Srv::start(
        "e",
        &pg.url("s9e"),
        &Access {
            lines: "ui = \"garbage\"",
            reads: "public",
            crawlable: false,
        },
    )
    .await?;
    let enter = e.get("/enter").await?;
    let dash = e.get("/admin").await?;
    c.check(
        "ui = \"garbage\" loads with one warning; with the admin UI on and no admin DID the instance is unconfigured: /enter says so, /admin redirects there (probe 7)",
        warned(&e) == 1
            && e.log().contains("never one of its values")
            && e.log().contains("admin sign-in is not configured")
            && enter.status == 200
            && enter.text.contains("set-admin-did")
            && to_enter(&dash),
        format!("{} warning(s) | {} | {}", warned(&e), brief(&enter), brief(&dash)),
    );
    drop(e);

    // The public UI over gated reads does not load.
    pg.create_db("s9f").await?;
    let (text, _) = config_toml(
        &pg.url("s9f"),
        &Access {
            lines: "public_ui = true",
            reads: "api_key",
            crawlable: false,
        },
    );
    let (dir, mut child, _) = Srv::spawn("f", &text)?;
    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break Some(s),
            Ok(None) if started.elapsed() > Duration::from_secs(30) => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(_) => break None,
        }
    };
    let log = std::fs::read_to_string(dir.join("server.log")).unwrap_or_default();
    c.check(
        "public_ui = true with reads = \"api_key\": the process exits non-zero naming access.public_ui (probe 8)",
        status.is_some_and(|s| !s.success()) && log.contains("access.public_ui"),
        format!("exit {status:?}: {}", support::truncate(log.trim(), 200)),
    );

    // The editor cannot switch the admin UI off.
    let before = std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())?;
    let page = a.admin_get(cookie, "/admin/settings").await?;
    let csrf = csrf_of(&page.text).unwrap_or_default();
    c.check(
        "Settings says how the admin UI is switched and no longer names access.ui in its hints",
        page.text
            .contains("<code>access.admin_ui</code> in config.toml, at restart")
            && !page.text.contains("access.ui = "),
        String::new(),
    );
    let off = before.replace("[access]\n", "[access]\nadmin_ui = false\n");
    let r = a
        .post(
            "/admin/settings",
            &[("cookie", cookie)],
            &[("csrf", &csrf), ("config", &off)],
        )
        .await?;
    let after = std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())?;
    let still = a.admin_get(cookie, "/admin").await?;
    c.check(
        "a Settings save with admin_ui = false is refused with the restart message; config.toml is untouched and the admin UI is still there (probe 6, D4)",
        r.status == 200 && r.text.contains(NEEDS_RESTART) && after == before && still.status == 200,
        format!("{} | file unchanged: {} | /admin {}", r.status, after == before, still.status),
    );
    // Replacing the retired key by the new one, same effect: allowed.
    let tidy = before.replace("ui = \"public_read\"\n", "admin_ui = true\n");
    let r = a
        .post(
            "/admin/settings",
            &[("cookie", cookie)],
            &[("csrf", &csrf), ("config", &tidy)],
        )
        .await?;
    let tidied = std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())?;
    c.check(
        "replacing ui = \"public_read\" by admin_ui = true in the editor is accepted: the effective value does not change",
        r.status == 200 && !r.text.contains(NEEDS_RESTART) && tidied.contains("admin_ui = true") && !tidied.contains("ui = \"public_read\""),
        r.status.to_string(),
    );
    std::fs::write(a.config_path(), &before).map_err(|e| e.to_string())?;

    // An unrelated in-process edit works, and rewrites neither key.
    let p = a.pause_sweep(true).await?;
    let file = std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())?;
    c.check(
        "pausing the sweep over XRPC works, and that edit leaves ui = \"public_read\" in the file and adds no admin_ui (D10)",
        p.status == 200 && file.contains("ui = \"public_read\"") && !file.contains("admin_ui"),
        p.short(),
    );
    // A hand edit waiting for a restart does not go live through it.
    let pending = file.replace("[access]\n", "[access]\nadmin_ui = false\n");
    std::fs::write(a.config_path(), &pending).map_err(|e| e.to_string())?;
    let p = a.pause_sweep(false).await?;
    let held = std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())?;
    let still = a.admin_get(cookie, "/admin").await?;
    c.check(
        "with admin_ui = false edited into the file by hand and no restart, the same edit is refused with the restart message, the file is left as the operator wrote it, and the admin UI stays on",
        p.status == 400 && p.text.contains("requires a restart") && held == pending && still.status == 200,
        format!("{} | /admin {}", p.short(), still.status),
    );
    std::fs::write(a.config_path(), &file).map_err(|e| e.to_string())?;
    let p = a.pause_sweep(false).await?;
    c.check(
        "with the hand edit undone, the edit works again",
        p.status == 200,
        p.short(),
    );
    Ok(())
}

// ----------------------------------------------------------- 3. old addresses

/// The pages have one address each. The addresses they had before
/// anything was released (`/public/…`, and the admin pages at the root)
/// are not routes: they get what any unknown path gets.
async fn check_old_paths(c: &mut Checks, a: &Srv, cookie: &str, w: &World) -> Result<(), String> {
    c.section("3. no second address for any page");
    let s = &w.subject;
    let paths: Vec<String> = vec![
        "/public".into(),
        "/public?x=1".into(),
        "/public/".into(),
        format!("/public/search?q={}", enc(s)),
        format!("/public/did/{s}"),
        format!("/public/did/{s}/history"),
        format!("/public/list/{s}/3kaaaaaaaaaa2?mc=x"),
        format!("/public/card/{s}"),
        "/public/about".into(),
        "/public/nonsense".into(),
        "/public/static/public.css".into(),
        "/public/static/public.js".into(),
        "/public/static/htmx.min.js".into(),
        "/public/static/og-default.png".into(),
        "/lookup/did".into(),
        "/lookup/did?q=did%3Aplc%3Ax".into(),
        "/lookup/list?q=at%3A%2F%2Fx&mc=y".into(),
        "/ops".into(),
        "/settings".into(),
        "/reset".into(),
        "/dashboard/fragment".into(),
        "/logout".into(),
        "/ops/backfill".into(),
        "/settings/token".into(),
        "/static/farsight.js".into(),
        "/nonsense".into(),
        // Paths that would name another host if a prefix were stripped.
        "/public//evil.example.com/x".into(),
        "//evil.example.com/x".into(),
        "/public/%2F%2Fevil.example.com".into(),
        "/public/did/%2F%2Fevil.example.com".into(),
        "/public/did/a%0D%0ALocation:%20https:%2F%2Fevil.example.com".into(),
        "/lookup//evil.example.com".into(),
    ];
    let mut bad = Vec::new();
    for p in &paths {
        for r in [a.get(p).await?, a.admin_get(cookie, p).await?] {
            if !(bare(&r) && r.header("location").is_none()) {
                bad.push(format!("{p}: {}", brief(&r)));
            }
        }
    }
    c.check(
        "every address a page might once have had — under /public, or an admin page at the root — is an unknown path: the bare 404, no Location, with or without a session",
        bad.is_empty(),
        if bad.is_empty() {
            format!("{} paths", paths.len())
        } else {
            bad.join(" | ")
        },
    );
    let hop = a.get(&format!("/did/{s}?bc=abc")).await?;
    let target = a.get(&format!("/did/{s}")).await?;
    c.check(
        "a retired cursor parameter on a page's own address is dropped with one 301, to the page itself",
        hop.status == 301 && loc(&hop) == format!("/did/{s}") && target.status == 200,
        format!("{} then {}", brief(&hop), brief(&target)),
    );
    Ok(())
}

// ------------------------------------------------------------------ 4. static

async fn check_static(c: &mut Checks, servers: &[(&str, &Srv)]) -> Result<(), String> {
    c.section("4. static assets: five files at /static, in every configuration (probes 17–21)");
    let mut bad = Vec::new();
    let mut sizes = Vec::new();
    for (name, s) in servers {
        for (path, ctype) in STATIC {
            let r = s.get(path).await?;
            let ok = r.status == 200
                && r.header("content-type")
                    .is_some_and(|t| t.starts_with(ctype))
                && cc(&r) == "public, max-age=3600"
                && r.header("x-content-type-options").as_deref() == Some("nosniff");
            if !ok {
                bad.push(format!("{name} {path}: {}", brief(&r)));
            }
            if *name == "A" {
                sizes.push(format!("{path} {}", r.text.len()));
            }
        }
        let js = s.get("/static/farsight.js").await?;
        if !bare(&js) {
            bad.push(format!("{name} /static/farsight.js: {}", brief(&js)));
        }
    }
    c.check(
        "the five files answer 200 (type, public, max-age=3600, nosniff) on all four servers, the API-only one included; /static/farsight.js is gone (D7, D9)",
        bad.is_empty(),
        if bad.is_empty() { sizes.join(", ") } else { bad.join(" | ") },
    );
    Ok(())
}

// ------------------------------------------------------------------ 6. wizard

/// A setup-mode server and a verified wizard session.
struct Wiz {
    srv: Server,
    http: Http,
    cookie: String,
    csrf: String,
}

impl Wiz {
    async fn start(name: &str) -> Result<Wiz, String> {
        let bind = format!("127.0.0.1:{}", free_port()?);
        let srv = Server::start(name, None, &bind, &[])?;
        let http = Http::new(None);
        srv.wait_live(&http, Duration::from_secs(90)).await?;
        let token = std::fs::read_to_string(srv.token_path())
            .unwrap_or_default()
            .lines()
            .next()
            .unwrap_or("")
            .to_owned();
        let r = http
            .post_form(&format!("{}/setup", srv.base), &[], &[("token", &token)])
            .await?;
        let cookie = set_cookie(&r, "farsight_setup")
            .ok_or_else(|| format!("setup token not accepted: {}", r.short()))?;
        let page = http
            .get(
                &format!("{}/setup/welcome", srv.base),
                &[("cookie", &cookie)],
            )
            .await?;
        let csrf = csrf_of(&page.text).ok_or("no csrf on the wizard's first page")?;
        Ok(Wiz {
            srv,
            http,
            cookie,
            csrf,
        })
    }

    async fn get(&self, path: &str) -> Result<Resp, String> {
        self.http
            .get(
                &format!("{}{path}", self.srv.base),
                &[("cookie", &self.cookie)],
            )
            .await
    }

    async fn post(&self, path: &str, fields: &[(&str, &str)]) -> Result<Resp, String> {
        let mut form = vec![("csrf", self.csrf.as_str())];
        form.extend_from_slice(fields);
        self.http
            .post_form(
                &format!("{}{path}", self.srv.base),
                &[("cookie", &self.cookie)],
                &form,
            )
            .await
    }

    /// Steps 2–5, up to the access step.
    async fn to_access(&self) -> Result<(), String> {
        for (step, fields) in [
            ("welcome", vec![]),
            (
                "identity",
                vec![
                    ("hostname", HOSTNAME),
                    ("contact", "mailto:ops@farsight.test"),
                ],
            ),
            ("firehose", vec![("urls", "ws://127.0.0.1:9")]),
            (
                "backfill",
                vec![
                    ("source", "relay_collections"),
                    ("per_host_rps", "10"),
                    ("concurrency", "4"),
                    ("max_repos_per_hour", "0"),
                    ("plc_url", "http://127.0.0.1:9"),
                    ("disk_gb", "500"),
                    ("backlinks_url", ""),
                ],
            ),
        ] {
            let r = self.post(&format!("/setup/{step}"), &fields).await?;
            if r.status != 303 {
                return Err(format!(
                    "wizard step {step}: {} {}",
                    r.status,
                    support::truncate(&r.text, 300)
                ));
            }
        }
        Ok(())
    }

    /// Steps 7 and 8.
    async fn past_access(&self, dsn: &str) -> Result<(), String> {
        let r = self
            .post("/setup/proxy", &[("proxy_choice", "none")])
            .await?;
        if r.status != 303 {
            return Err(format!("wizard step proxy: {}", r.short()));
        }
        let t = self.post("/setup/storage/test", &[("dsn", dsn)]).await?;
        let r = self.post("/setup/storage", &[("dsn", dsn)]).await?;
        if r.status != 303 {
            return Err(format!(
                "wizard step storage: {} after test {}",
                r.short(),
                support::truncate(t.text.split("<pre>").nth(1).unwrap_or(""), 200)
            ));
        }
        Ok(())
    }

    /// Waits for the in-process switch to normal mode.
    async fn normal(&self) -> Result<(), String> {
        let started = Instant::now();
        loop {
            if let Ok(r) = self
                .http
                .get(&format!("{}/setup", self.srv.base), &[])
                .await
            {
                if r.status == 404 {
                    return Ok(());
                }
            }
            if started.elapsed() > Duration::from_secs(60) {
                return Err(format!(
                    "no switch to normal mode: {}",
                    self.srv.log_tail(10)
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

async fn check_wizard(c: &mut Checks, pg: &Pg) -> Result<(), String> {
    c.section("6. the wizard's access step (probes 24–27)");
    pg.create_db("s9w1").await?;
    let w = Wiz::start("w1").await?;
    w.to_access().await?;
    let page = w.get("/setup/access").await?;
    c.check(
        "the step has the two boxes, neither ticked, and no Web UI select (probe 24, D5)",
        page.status == 200
            && page.text.contains("id=\"public_ui\" name=\"public_ui\">")
            && page.text.contains("id=\"admin_ui\" name=\"admin_ui\">")
            && !page.text.contains(" checked")
            && !page.text.contains("name=\"ui\""),
        brief(&page),
    );
    let css = w.get("/static/farsight.css").await?;
    let box_at = page.text.find("class=\"switch reveals\" id=\"admin_ui\"");
    let field_at = page
        .text
        .find("<div class=\"revealed\" id=\"admin-did-field\">");
    let did_at = page.text.find("name=\"admin_did\"");
    c.check(
        "the admin DID field is in the page, after the admin box, inside the block a stylesheet rule hides while the box is unticked; the page has no script (probe 25)",
        matches!((box_at, field_at, did_at), (Some(b), Some(f), Some(d)) if b < f && f < d)
            && css.text.contains("input.reveals:not(:checked) ~ .revealed { display: none; }")
            && !page.text.contains("<script"),
        format!("{box_at:?} {field_at:?} {did_at:?}"),
    );
    c.check(
        "the public UI box says what it publishes, the avatar fetch included; the admin token is on the step",
        page.text.contains("each visitor's browser fetches from the account's own server")
            && page.text.contains("name=\"token_saved\""),
        String::new(),
    );
    let r = w
        .post(
            "/setup/access",
            &[
                ("reads", "api_key"),
                ("public_ui", "on"),
                ("token_saved", "on"),
            ],
        )
        .await?;
    c.check(
        "the public UI with gated reads is refused on the step itself",
        r.status == 200 && r.text.contains("needs public read queries"),
        brief(&r),
    );
    let both = [
        ("reads", "public"),
        ("public_ui", "on"),
        ("admin_ui", "on"),
        ("admin_did", ADMIN_DID),
        ("token_saved", "on"),
    ];
    let bad_did = w
        .post(
            "/setup/access",
            &[
                ("reads", "public"),
                ("admin_ui", "on"),
                ("admin_did", "alice.example"),
                ("token_saved", "on"),
            ],
        )
        .await?;
    let r1 = w.post("/setup/access", &both).await?;
    c.check(
        "with the admin box ticked the DID is required and looked up: a handle is refused; a DID that does not resolve asks for \"Use this DID anyway\"",
        bad_did.status == 200
            && bad_did.text.contains("A handle will not do")
            && r1.status == 200
            && r1.text.contains("id=\"admin-not-found\"")
            && r1.text.contains("name=\"public_ui\" checked")
            && r1.text.contains("name=\"admin_ui\" checked"),
        format!("{} | {}", bad_did.status, r1.status),
    );
    let mut anyway = both.to_vec();
    anyway.push(("use_anyway", "on"));
    let confirm = w.post("/setup/access", &anyway).await?;
    c.check(
        "with the public UI ticked the step answers with the confirmation page: what becomes public (hostname, contact, blocks, lists, members, cards and the avatar fetch), a confirm button and a way back (probe 26)",
        confirm.status == 200
            && confirm.text.contains("id=\"confirm-public\"")
            && confirm.text.contains(&format!("https://{HOSTNAME}/"))
            && confirm.text.contains("mailto:ops@farsight.test")
            && confirm.text.contains("Incoming blocks")
            && confirm.text.contains("List memberships")
            && confirm.text.contains("each visitor's browser fetches from the")
            && confirm.text.contains("name=\"confirm_public\"")
            && confirm.text.contains("Enable public UI")
            && confirm.text.contains("href=\"/setup/access\""),
        brief(&confirm),
    );
    w.past_access(&pg.url("s9w1")).await?;
    let early = w.post("/setup/finish", &[]).await?;
    c.check(
        "until it is confirmed nothing can be written: finishing sends the operator back to the access step, and no config exists",
        early.status == 303 && loc(&early) == "/setup/access" && !w.srv.config_path().exists(),
        brief(&early),
    );
    let ok = w.post("/setup/access", &[("confirm_public", "1")]).await?;
    c.check(
        "the confirmation's own POST completes the step",
        ok.status == 303 && loc(&ok) == "/setup/proxy",
        brief(&ok),
    );
    let done = w.post("/setup/finish", &[]).await?;
    let text = std::fs::read_to_string(w.srv.config_path()).unwrap_or_default();
    c.check(
        "done page, both on: the admin UI at /admin, sign-in at /enter with the DID, the public UI at / (probe 27)",
        done.status == 200
            && done.text.contains("The admin UI is at <code>/admin</code>")
            && done.text.contains(ADMIN_DID)
            && done.text.contains("href=\"/enter\"")
            && done.text.contains("The public UI is at <code>/</code>"),
        brief(&done),
    );
    c.check(
        "the written config has public_ui = true, admin_ui = true, the admin DID, and no ui key",
        text.contains("public_ui = true")
            && text.contains("admin_ui = true")
            && text.contains(ADMIN_DID)
            && !text.contains("\nui ="),
        support::truncate(text.split("[access]").nth(1).unwrap_or(""), 160),
    );
    w.normal().await?;
    let root = w.http.get(&format!("{}/", w.srv.base), &[]).await?;
    let admin = w.http.get(&format!("{}/admin", w.srv.base), &[]).await?;
    c.check(
        "after the switch to normal mode: / is the public home, /admin asks for sign-in, no setup token is left",
        root.status == 200
            && root.text.contains("action=\"/search\"")
            && to_enter(&admin)
            && !w.srv.token_path().exists(),
        format!("{} | {}", brief(&root), brief(&admin)),
    );
    drop(w);

    // Neither box ticked: an API-only instance.
    pg.create_db("s9w2").await?;
    let w = Wiz::start("w2").await?;
    w.to_access().await?;
    let r = w
        .post(
            "/setup/access",
            &[
                ("reads", "public"),
                ("admin_did", ADMIN_DID),
                ("token_saved", "on"),
            ],
        )
        .await?;
    c.check(
        "with neither box ticked the step advances at once: no DID round, no confirmation",
        r.status == 303 && loc(&r) == "/setup/proxy",
        brief(&r),
    );
    w.past_access(&pg.url("s9w2")).await?;
    let done = w.post("/setup/finish", &[]).await?;
    let text = std::fs::read_to_string(w.srv.config_path()).unwrap_or_default();
    c.check(
        "done page, both off: API-only, /xrpc and /livez named, no sign-in link; the config has admin_ui = false, public_ui = false and no admin DID although one was typed",
        done.status == 200
            && done.text.contains("API-only")
            && done.text.contains("/livez")
            && !done.text.contains("href=\"/enter\"")
            && text.contains("admin_ui = false")
            && text.contains("public_ui = false")
            && !text.contains("admin_did"),
        brief(&done),
    );
    w.normal().await?;
    let root = w.http.get(&format!("{}/", w.srv.base), &[]).await?;
    let admin = w.http.get(&format!("{}/admin", w.srv.base), &[]).await?;
    let enter = w.http.get(&format!("{}/enter", w.srv.base), &[]).await?;
    c.check(
        "after the switch: / is the text page, /admin and /enter are the bare 404",
        root.status == 200 && root.text.starts_with("Farsight\n") && bare(&admin) && bare(&enter),
        format!("{} | {} | {}", brief(&root), brief(&admin), brief(&enter)),
    );
    drop(w);

    // One box each: the other two done pages.
    pg.create_db("s9w3").await?;
    let w = Wiz::start("w3").await?;
    w.to_access().await?;
    let admin_only = [
        ("reads", "api_key"),
        ("admin_ui", "on"),
        ("admin_did", ADMIN_DID),
        ("token_saved", "on"),
        ("use_anyway", "on"),
    ];
    w.post("/setup/access", &admin_only).await?;
    let r = w.post("/setup/access", &admin_only).await?;
    w.past_access(&pg.url("s9w3")).await?;
    let done = w.post("/setup/finish", &[]).await?;
    c.check(
        "done page, admin UI only: /admin and /enter, no word of a public UI",
        r.status == 303
            && done.status == 200
            && done.text.contains("The admin UI is at <code>/admin</code>")
            && !done.text.contains("public UI is at"),
        format!("{} | {}", brief(&r), brief(&done)),
    );
    drop(w);
    pg.create_db("s9w4").await?;
    let w = Wiz::start("w4").await?;
    w.to_access().await?;
    let public_only = [
        ("reads", "public"),
        ("public_ui", "on"),
        ("token_saved", "on"),
    ];
    let confirm = w.post("/setup/access", &public_only).await?;
    let ok = w.post("/setup/access", &[("confirm_public", "1")]).await?;
    w.past_access(&pg.url("s9w4")).await?;
    let done = w.post("/setup/finish", &[]).await?;
    c.check(
        "done page, public UI only: the public UI at /, no admin UI, the admin token for the API; no sign-in link",
        confirm.text.contains("id=\"confirm-public\"")
            && ok.status == 303
            && done.status == 200
            && done.text.contains("The public UI is at <code>/</code>")
            && done.text.contains("There is no admin UI")
            && !done.text.contains("href=\"/enter\""),
        format!("{} | {} | {}", confirm.status, brief(&ok), brief(&done)),
    );
    Ok(())
}

// ------------------------------------------------- 7. robots, client metadata

async fn check_robots_and_metadata(
    c: &mut Checks,
    a: &Srv,
    b: &Srv,
    s: &Srv,
    d: &Srv,
) -> Result<(), String> {
    c.section("7. robots.txt and the client metadata (probes 28–32)");
    let ra = a.get("/robots.txt").await?;
    let lines: Vec<&str> = ra.text.lines().collect();
    let closed = [
        "/admin", "/enter", "/setup", "/xrpc/", "/search", "/card/", "/health", "/livez",
    ];
    c.check(
        "public UI on and crawlable: the admin UI, sign-in, wizard, API, search, cards and health are closed; Allow: / comes last (probe 28)",
        ra.status == 200
            && cc(&ra) == "public, max-age=300"
            && lines.first() == Some(&"User-agent: *")
            && closed.iter().all(|p| lines.contains(&format!("Disallow: {p}").as_str()))
            && lines.last() == Some(&"Allow: /")
            && !lines.contains(&"Disallow: /"),
        ra.text.replace('\n', " / "),
    );
    let mut same = Vec::new();
    for (name, srv) in [
        ("B: public UI, not crawlable", b),
        ("C: admin UI only", s),
        ("D: neither", d),
    ] {
        let r = srv.get("/robots.txt").await?;
        if !(r.status == 200 && r.text == "User-agent: *\nDisallow: /\n") {
            same.push(format!("{name}: {}", brief(&r)));
        }
    }
    c.check(
        "every other configuration, the API-only one included: 200 and Disallow: / (probes 29, 30)",
        same.is_empty(),
        same.join(" | "),
    );
    let path = "/.well-known/atproto-oauth-client-metadata";
    let ma = a.get_with(path, &[("host", HOSTNAME)]).await?;
    let ms = s.get_with(path, &[("host", HOSTNAME)]).await?;
    let root = format!("https://{HOSTNAME}/");
    c.check(
        "the client metadata is the same document with the public UI on or off: client_uri is the hostname's root, the callback is /enter/callback (probe 31, D8)",
        ma.status == 200
            && ma.text == ms.text
            && ma.body["client_uri"] == root.as_str()
            && ma.body["redirect_uris"][0] == format!("https://{HOSTNAME}/enter/callback").as_str()
            && ma.body["client_id"] == format!("https://{HOSTNAME}{path}").as_str(),
        support::truncate(&ma.text, 200),
    );
    Ok(())
}

// ------------------------------------- 8. axum's part, metric labels, the card

async fn check_contracts(
    c: &mut Checks,
    a: &Srv,
    d: &Srv,
    cookie: &str,
    w: &World,
) -> Result<(), String> {
    c.section("8. what the routing relies on (probes 33, 34, 36, 38)");
    let post = |s: &Srv, path: &str| {
        let url = format!("{}{path}", s.base);
        let http = s.fresh();
        async move { http.post_form(&url, &[], &[]).await }
    };
    let p1 = post(a, &format!("/did/{}", w.subject)).await?;
    let p2 = post(a, "/admin/lookup/did").await?;
    let p3 = post(d, "/admin").await?;
    let g = d.get("/admin/logout").await?;
    c.check(
        "a method a route does not serve is a 405 with Allow, in every configuration: POST /did/{did}, POST /admin/lookup/did, and — on the API-only server — POST /admin and GET /admin/logout (probe 33)",
        [&p1, &p2, &p3, &g].iter().all(|r| r.status == 405 && r.header("allow").is_some()),
        format!(
            "{} {:?} | {} | {} | {}",
            p1.status,
            p1.header("allow"),
            p2.status,
            p3.status,
            g.status
        ),
    );
    // Metric labels: one per page; an unknown path is not a public UI
    // request.
    let before = a.metrics_text().await?;
    a.get("/").await?;
    a.get("/public").await?;
    a.get("/public/nonsense").await?;
    a.get("/robots.txt").await?;
    let after = a.metrics_text().await?;
    let name = "farsight_public_ui_requests_total";
    let delta = |page: &str, status: &str| {
        metric(&after, name, &[("page", page), ("status", status)])
            - metric(&before, name, &[("page", page), ("status", status)])
    };
    let mut labels: Vec<&str> = after
        .lines()
        .filter(|l| l.starts_with(name))
        .filter_map(|l| l.split("page=\"").nth(1)?.split('"').next())
        .collect();
    labels.sort_unstable();
    labels.dedup();
    c.check(
        "farsight_public_ui_requests_total has one label per page; / counts as home, robots as robots, and an unknown path such as /public is counted under none (probe 36)",
        labels == ["card", "did", "home", "list", "robots", "search"]
            && delta("home", "2xx") == 1.0
            && delta("robots", "2xx") == 1.0,
        format!(
            "{labels:?} home {} robots {}",
            delta("home", "2xx"),
            delta("robots", "2xx")
        ),
    );
    let before = d.metrics_text().await?;
    d.get("/").await?;
    let after = d.metrics_text().await?;
    c.check(
        "the text page of an API-only instance is not counted as a public UI request",
        metric(&after, name, &[("page", "home")]) == metric(&before, name, &[("page", "home")]),
        String::new(),
    );
    // The admin card.
    let path = format!("/admin/card/{}", w.subject);
    let anon = a.get(&path).await?;
    let hx = a.get_with(&path, &[("hx-request", "true")]).await?;
    let card = a.admin_get(cookie, &path).await?;
    c.check(
        "/admin/card/{did}: the bare 404 without a session, never a redirect; a fragment with one, no-store, private (probe 38)",
        bare(&anon) && bare(&hx) && card.status == 200 && cc(&card) == "no-store, private" && !card.text.contains("<html"),
        format!("{} | {}", brief(&anon), brief(&card)),
    );
    Ok(())
}

// ----------------------------------------------------------- 9. the browser

async fn check_browser(c: &mut Checks, a: &Srv, cookie: &str, w: &World) -> Result<(), String> {
    c.section("9. in a browser: the access step without script, a refused poll, times, the admin card (probes 25, 35, 37, 38)");
    // A wizard session parked on the access step.
    let wiz = Wiz::start("wb").await?;
    wiz.to_access().await?;
    let dir = std::env::temp_dir().join("farsight-stage9-browser");
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    std::fs::write(dir.join("probes.mjs"), BROWSER_SCRIPT).map_err(|e| e.to_string())?;
    let session = cookie.trim_start_matches("farsight_admin=");
    let setup = wiz.cookie.trim_start_matches("farsight_setup=");
    let script = format!(
        "cd /work && ([ -d node_modules/playwright ] || npm install --no-save --no-audit --no-fund playwright@1.48.0 >npm.log 2>&1) && node probes.mjs '{}' '{session}' '{}' '{}' '{setup}'",
        a.base, w.subject, wiz.srv.base
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

async fn run(c: &mut Checks, pg: &Pg, browser: bool) -> Result<(), String> {
    // A: both on, crawlable, with the retired key as an upgraded file has it.
    pg.create_db("s9a").await?;
    let a = Srv::start(
        "a",
        &pg.url("s9a"),
        &Access {
            lines: &format!("ui = \"public_read\"\nadmin_did = \"{ADMIN_DID}\"\npublic_ui = true"),
            reads: "public",
            crawlable: true,
        },
    )
    .await?;
    // B: the public UI only; an admin DID in the file, unused.
    pg.create_db("s9b").await?;
    let b = Srv::start(
        "b",
        &pg.url("s9b"),
        &Access {
            lines: &format!("admin_ui = false\nadmin_did = \"{ADMIN_DID}\"\npublic_ui = true"),
            reads: "public",
            crawlable: false,
        },
    )
    .await?;
    // C: the admin UI only, reads gated.
    pg.create_db("s9c").await?;
    let s = Srv::start(
        "c",
        &pg.url("s9c"),
        &Access {
            lines: &format!("admin_ui = true\nadmin_did = \"{ADMIN_DID}\""),
            reads: "api_key",
            crawlable: true,
        },
    )
    .await?;
    // D: neither; the admin UI is off by the retired key alone.
    pg.create_db("s9d").await?;
    let d = Srv::start(
        "d",
        &pg.url("s9d"),
        &Access {
            lines: &format!("ui = \"disabled\"\nadmin_did = \"{ADMIN_DID}\""),
            reads: "public",
            crawlable: true,
        },
    )
    .await?;
    let mut world = None;
    let mut cookies = Vec::new();
    for db in ["s9a", "s9b", "s9c", "s9d"] {
        let pool = pg.pool(db, 2).await?;
        world = Some(seed_world(&pool).await?);
        cookies.push(admin_session(&pool, ADMIN_DID).await?);
        // A second session on A: the first is ended by the logout check.
        if db == "s9a" {
            cookies.push(admin_session(&pool, ADMIN_DID).await?);
        }
    }
    let w = world.ok_or("no database seeded")?;
    let (a_first, a_cookie, b_cookie, s_cookie) =
        (&cookies[0], &cookies[1], &cookies[2], &cookies[3]);

    check_both(c, &a, a_first, &w).await?;
    check_public_only(c, &b, b_cookie, &w).await?;
    check_admin_only(c, &s, s_cookie, &w).await?;
    check_api_only(c, &d, &w).await?;
    check_config(c, pg, &a, a_cookie, &d).await?;
    check_old_paths(c, &a, a_cookie, &w).await?;
    check_static(c, &[("A", &a), ("B", &b), ("C", &s), ("D", &d)]).await?;
    check_wizard(c, pg).await?;
    check_robots_and_metadata(c, &a, &b, &s, &d).await?;
    check_contracts(c, &a, &d, a_cookie, &w).await?;
    if browser {
        check_browser(c, &a, a_cookie, &w).await?;
    } else {
        c.unverified("browser probes (25, 35, 37, 38)", "not run: pass --browser");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let flag = |f: &str| argv.iter().any(|a| a == f);
    println!("== farsight stage-9 harness: Mode A (UI v2.5.2)");
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
        run(&mut c, &pg, flag("--browser")).await
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
