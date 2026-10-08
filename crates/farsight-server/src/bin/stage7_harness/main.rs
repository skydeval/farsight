//! `farsight-stage7-harness`: integration tests of the admin sign-in through
//! ATProto OAuth. Every assertion reads real responses from a real
//! `farsight` process over HTTP, real rows, real files and real log lines.
//! The authorization server is a stand-in ([`standin`]) on a TEST-NET-2
//! address the harness puts on a local bridge, which the safe outbound
//! client treats as public; the harness itself plays the browser: it follows
//! the redirects by hand and carries the cookies.
//!
//! Sections:
//!
//! - 1: config: admin DID syntax, the three states, the public UI's
//!   independence of `admin_ui`;
//! - 2: client modes and the client metadata document;
//! - 3: a loopback sign-in end to end, and the requests it made;
//! - 4: a hosted sign-in end to end (client metadata without
//!   `refresh_token`);
//! - 5: DPoP nonces; 6: the callback's checks, in order;
//! - 7: flow lifetime; 8: `gate` and `/robots.txt` by configuration;
//! - 9: rate limits; 10: Settings; 10b: the fresh sign-in that sensitive
//!   actions ask for, the directory pinned at start, sessions ended by a
//!   changed admin token;
//! - 11: no admin DID; 12: the CLI, and a changed admin DID;
//! - 13: what is logged and what never is;
//! - 14: the browser (`--browser`): the session cookie after the callback
//!   page, and signing in again before a sensitive action, in Chromium,
//!   Firefox and WebKit.
//!
//! `--keep` keeps the Postgres container.

#[allow(dead_code)]
#[path = "../stage3_harness/support.rs"]
mod support;

// Handlers return early with a ready `Response` as the error value.
#[allow(clippy::result_large_err)]
mod standin;

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use farsight_core::config::{self, AdminAuth};
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::standin::{Knobs, Standin};
use crate::support::{Checks, Http, Pg, Resp, csrf_of, farsight_bin, free_port, set_cookie};

const HOSTNAME: &str = "farsight.test";
/// TEST-NET-2: documentation space, routed nowhere, public to the safe
/// client. The harness creates a bridge that owns `.1`.
const STANDIN_SUBNET: &str = "198.51.100.0/24";
const STANDIN_ADDR: &str = "198.51.100.1";
const D1: &str = "did:plc:adminoneaaaaaaaaaaaaaaaa";
const D2: &str = "did:plc:admintwoaaaaaaaaaaaaaaaa";
const OTHER: &str = "did:plc:someoneelseaaaaaaaaaaaaa";
const UNKNOWN: &str = "did:plc:unknownaaaaaaaaaaaaaaaaa";
const REFUSED: &str = "Sign-in did not complete. Start again.";
const BROWSER_IMAGE: &str = "mcr.microsoft.com/playwright:v1.48.0-jammy";
const BROWSER_SCRIPT: &str = include_str!("../../../../../scripts/stage7-browser-probes.mjs");

fn hex(b: &[u8]) -> String {
    farsight_api::auth::hex(b)
}

/// A `config.toml` for the harness.
fn config_toml(dsn: &str, hostname: &str, standin: &str, access: &str) -> String {
    format!(
        r#"[server]
hostname = "{hostname}"
contact = "mailto:ops@farsight.test"

[storage]
database_url = "{dsn}"
budget_bytes = 70000000000

[firehose]
urls = ["ws://127.0.0.1:9"]

[backfill]
plc_url = "{standin}"

[net]
allow_http_hosts = ["{STANDIN_ADDR}"]

[access]
reads = "public"
{access}

[auth]
admin_token_sha256 = "{}"

[metrics]
bind = "127.0.0.1:{}"
"#,
        hex(&farsight_api::auth::sha256("stage7-admin-token")),
        free_port().unwrap_or(0),
    )
}

/// One `farsight` process on a config directory the harness keeps.
struct Srv {
    base: String,
    port: u16,
    dir: PathBuf,
    child: std::process::Child,
}

impl Srv {
    fn dir(name: &str) -> Result<PathBuf, String> {
        let dir =
            std::env::temp_dir().join(format!("farsight-stage7-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        Ok(dir)
    }

    /// Starts `farsight` on `dir` (its `config.toml`, if any, is left as
    /// it is) and waits until it serves.
    async fn launch(dir: &Path, env: &[(&str, &str)]) -> Result<Srv, String> {
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
                "debug,sqlx=warn,hyper=warn,reqwest=warn,hickory=warn,rustls=warn",
            )
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log2));
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().map_err(|e| format!("spawning farsight: {e}"))?;
        let s = Srv {
            base: format!("http://127.0.0.1:{port}"),
            port,
            dir: dir.to_owned(),
            child,
        };
        let http = Http::new(None);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(r) = http.get(&format!("{}/livez", s.base), &[]).await
                && r.status == 200
            {
                return Ok(s);
            }
            if Instant::now() > deadline {
                return Err(format!("farsight did not come up: {}", s.log_tail(15)));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn with_config(name: &str, text: &str, env: &[(&str, &str)]) -> Result<Srv, String> {
        let dir = Srv::dir(name)?;
        write_private(&dir.join("config.toml"), text)?;
        Srv::launch(&dir, env).await
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

    /// The loopback `Host` of this server.
    fn loopback(&self) -> String {
        format!("127.0.0.1:{}", self.port)
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

fn write_private(path: &Path, text: &str) -> Result<(), String> {
    use std::os::unix::fs::OpenOptionsExt;
    let _ = std::fs::remove_file(path);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| e.to_string())?;
    std::io::Write::write_all(&mut f, text.as_bytes()).map_err(|e| e.to_string())
}

/// Runs the `farsight` CLI on a config directory.
fn cli(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> (bool, String) {
    let mut cmd = Command::new(farsight_bin());
    cmd.args(args)
        .env("FARSIGHT_CONFIG", dir.join("config.toml"))
        .env_remove("FARSIGHT__ACCESS__ADMIN_DID");
    for (k, v) in env {
        cmd.env(k, v);
    }
    match cmd.output() {
        Ok(o) => (
            o.status.success(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            ),
        ),
        Err(e) => (false, e.to_string()),
    }
}

/// What the harness shares across sections.
struct Ctx {
    standin: Arc<Standin>,
    /// Requests to the stand-in (the "browser" visiting it).
    web: Http,
    pool: PgPool,
    dsn: String,
    next_ip: AtomicU32,
    /// When the last sign-in was started (the process-wide bucket admits
    /// one a second).
    last_start: Mutex<Option<Instant>>,
    /// Every secret seen: states, codes, tokens, cookie values.
    secrets: Mutex<Vec<String>>,
    /// Logs of servers already stopped.
    logs: Mutex<Vec<String>>,
}

impl Ctx {
    /// A client with a source address no request has used yet.
    fn fresh(&self) -> Http {
        let n = self.next_ip.fetch_add(1, Ordering::Relaxed);
        let ip: IpAddr = format!("127.{}.{}.{}", 7 + n / 62_500, (n / 250) % 250, 2 + n % 250)
            .parse()
            .expect("ip");
        Http::new(Some(ip))
    }

    fn secret(&self, s: &str) {
        if !s.is_empty() {
            self.secrets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(s.to_owned());
        }
    }

    fn config(&self, hostname: &str, access: &str) -> String {
        config_toml(&self.dsn, hostname, &self.standin.base, access)
    }

    fn retire(&self, mut s: Srv) {
        s.stop();
        self.logs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(s.log());
    }

    /// Waits so that sign-in starts stay inside the process-wide bucket.
    async fn pace(&self) {
        let wait = {
            let mut l = self.last_start.lock().unwrap_or_else(|e| e.into_inner());
            let wait = l.map_or(Duration::ZERO, |t| {
                Duration::from_millis(1100).saturating_sub(t.elapsed())
            });
            *l = Some(Instant::now() + wait);
            wait
        };
        tokio::time::sleep(wait).await;
    }

    /// `POST /enter` on `host`: the start of a sign-in.
    async fn start(&self, s: &Srv, http: &Http, host: &str) -> Result<Started, String> {
        self.pace().await;
        let r = http
            .post_form(&format!("{}/enter", s.base), &[("host", host)], &[])
            .await?;
        let cookie = set_cookie(&r, "farsight_flow");
        if let Some(c) = &cookie {
            self.secret(c.split_once('=').map_or("", |x| x.1));
        }
        Ok(Started {
            location: r.header("location"),
            cookie,
            resp: r,
        })
    }

    /// Visits the authorization URL: the stand-in "signs in" and sends the
    /// browser back. Returns the callback URL.
    async fn approve(&self, authorize: &str) -> Result<String, String> {
        let r = self.web.get(authorize, &[]).await?;
        let to = r
            .header("location")
            .ok_or_else(|| format!("the stand-in did not redirect: {}", r.short()))?;
        if let Ok(u) = reqwest::Url::parse(&to) {
            for (k, v) in u.query_pairs() {
                if k == "code" || k == "state" {
                    self.secret(&v);
                }
            }
        }
        Ok(to)
    }

    /// Follows a callback URL to Farsight: its path and query, on the
    /// `Host` it names, with the flow cookie (if given).
    async fn callback(
        &self,
        s: &Srv,
        http: &Http,
        url: &str,
        cookie: Option<&str>,
    ) -> Result<Resp, String> {
        let u = reqwest::Url::parse(url).map_err(|e| format!("{url}: {e}"))?;
        let host = match u.port() {
            Some(p) => format!("{}:{p}", u.host_str().unwrap_or_default()),
            None => u.host_str().unwrap_or_default().to_owned(),
        };
        let mut headers = vec![("host", host.as_str())];
        if let Some(c) = cookie {
            headers.push(("cookie", c));
        }
        let r = http
            .get(
                &format!("{}{}?{}", s.base, u.path(), u.query().unwrap_or_default()),
                &headers,
            )
            .await?;
        if let Some(c) = set_cookie(&r, "farsight_admin") {
            self.secret(c.split_once('=').map_or("", |x| x.1));
        }
        Ok(r)
    }

    /// A whole sign-in on `host`. Returns the callback's response.
    async fn sign_in(&self, s: &Srv, http: &Http, host: &str) -> Result<Resp, String> {
        let st = self.start(s, http, host).await?;
        let (Some(loc), Some(cookie)) = (st.location, st.cookie) else {
            return Err(format!("start: {}", st.resp.short()));
        };
        let cb = self.approve(&loc).await?;
        self.callback(s, http, &cb, Some(&cookie)).await
    }

    /// A flow taken as far as the callback URL, not yet followed.
    async fn pending(&self, s: &Srv, http: &Http, host: &str) -> Result<(String, String), String> {
        let st = self.start(s, http, host).await?;
        let (Some(loc), Some(cookie)) = (st.location, st.cookie) else {
            return Err(format!("start: {}", st.resp.short()));
        };
        Ok((self.approve(&loc).await?, cookie))
    }

    async fn n(&self, q: &str) -> Result<i64, String> {
        sqlx::query_scalar(q)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("{e}: {q}"))
    }

    async fn has_session_key(&self, key: &[u8]) -> Result<bool, String> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM admin_sessions WHERE id_sha256 = $1")
            .bind(key)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(n == 1)
    }
}

struct Started {
    resp: Resp,
    location: Option<String>,
    cookie: Option<String>,
}

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

fn replace_query(url: &str, key: &str, value: Option<&str>) -> String {
    let mut u = reqwest::Url::parse(url).expect("url");
    let pairs: Vec<(String, String)> = u
        .query_pairs()
        .into_owned()
        .filter(|(k, _)| k != key)
        .collect();
    {
        let mut q = u.query_pairs_mut();
        q.clear();
        q.extend_pairs(pairs);
        if let Some(v) = value {
            q.append_pair(key, v);
        }
    }
    u.to_string()
}

fn query_of(url: &str, key: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()?
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

// ------------------------------------------------------------- 1. config

fn check_config(c: &mut Checks, ctx: &Ctx) -> Result<(), String> {
    c.section("1. config: the admin DID and the three states");
    let load = |access: &str, env: &[(&str, &str)]| {
        let env: Vec<(String, String)> = env
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        config::load_from_parts(Some(&ctx.config(HOSTNAME, access)), &env)
    };
    let bad: Vec<&str> = [
        "alice.example",
        "did:plc:short",
        "did:plc:AAAAAAAAAAAAAAAAAAAAAAAA",
        "did:web:alice.example%3A8080",
        "did:web:alice.example:path",
        "did:web:localhost",
        "did:key:z6Mk",
    ]
    .into_iter()
    .filter(|d| {
        !load(&format!("admin_did = \"{d}\""), &[])
            .err()
            .is_some_and(|e| e.to_string().contains("access.admin_did"))
    })
    .collect();
    let good: Vec<&str> = [D1, "did:web:alice.example"]
        .into_iter()
        .filter(|d| load(&format!("admin_did = \"{d}\""), &[]).is_err())
        .collect();
    c.check(
        "a malformed access.admin_did fails the load, naming the key (a handle, a short or upper-case did:plc, a did:web with a port or a path, another method); did:plc and did:web load",
        bad.is_empty() && good.is_empty(),
        format!("accepted {bad:?}; refused {good:?}"),
    );
    let unconfigured = load("", &[]);
    let configured = load(&format!("admin_did = \"{D1}\""), &[]);
    let from_env = load("", &[("FARSIGHT__ACCESS__ADMIN_DID", D1)]);
    let disabled = load("admin_ui = false", &[]);
    let state = |r: &Result<config::LoadedConfig, config::ConfigError>| {
        r.as_ref().ok().map(|l| l.admin_auth())
    };
    c.check(
        "a file without an admin DID loads, unconfigured, with a warning; with the admin UI off (admin_ui = false) the admin DID does not matter",
        state(&unconfigured) == Some(AdminAuth::Unconfigured)
            && unconfigured.as_ref().is_ok_and(|l| l.warnings.iter().any(|w| w.contains("not configured")))
            && state(&disabled) == Some(AdminAuth::Disabled),
        format!("{:?} / {:?}", state(&unconfigured), state(&disabled)),
    );
    c.check(
        "an admin DID, in the file or in the environment, configures sign-in as that DID, with no warning",
        state(&configured) == Some(AdminAuth::Configured(D1.into()))
            && state(&from_env) == Some(AdminAuth::Configured(D1.into()))
            && [&configured, &from_env]
                .iter()
                .all(|r| r.as_ref().is_ok_and(|l| l.warnings.is_empty())),
        format!("{:?} / {:?}", state(&configured), state(&from_env)),
    );
    let env_only = config::load_from_parts(
        None,
        &[
            ("FARSIGHT__SERVER__HOSTNAME", "h.test"),
            ("FARSIGHT__SERVER__CONTACT", "mailto:x@h.test"),
            ("FARSIGHT__STORAGE__DATABASE_URL", "postgres://x"),
            ("FARSIGHT__AUTH__ADMIN_TOKEN_SHA256", &"0".repeat(64)),
        ]
        .map(|(k, v)| (k.to_owned(), v.to_owned())),
    );
    c.check(
        "an env-only config without an admin DID loads and is unconfigured",
        state(&env_only) == Some(AdminAuth::Unconfigured),
        format!(
            "{:?}",
            env_only
                .as_ref()
                .map(|l| l.admin_auth())
                .map_err(ToString::to_string)
        ),
    );
    let mut combos = Vec::new();
    for admin_ui in [true, false] {
        if let Err(e) = load(
            &format!("admin_ui = {admin_ui}\npublic_ui = true\nadmin_did = \"{D1}\""),
            &[],
        ) {
            combos.push(format!("admin_ui = {admin_ui}: {e}"));
        }
    }
    for reads in ["api_key", "disabled"] {
        let text = ctx
            .config(HOSTNAME, "admin_ui = true\npublic_ui = true")
            .replace("reads = \"public\"", &format!("reads = \"{reads}\""));
        if !config::load_from_parts(Some(&text), &[])
            .err()
            .is_some_and(|e| e.to_string().contains("access.reads"))
        {
            combos.push(format!("reads = {reads} accepted"));
        }
    }
    c.check(
        "public_ui = true loads with the admin UI on or off and still needs reads = public",
        combos.is_empty(),
        combos.join("; "),
    );
    Ok(())
}

/// A malformed DID makes the real process exit; a missing one does not.
async fn check_process_start(c: &mut Checks, ctx: &Ctx) -> Result<(), String> {
    let dir = Srv::dir("badcfg")?;
    write_private(
        &dir.join("config.toml"),
        &ctx.config(HOSTNAME, "admin_did = \"alice.example\""),
    )?;
    let out = Command::new("timeout")
        .arg("20")
        .arg(farsight_bin())
        .env("FARSIGHT_CONFIG", dir.join("config.toml"))
        .env(
            "FARSIGHT__SERVER__BIND",
            format!("127.0.0.1:{}", free_port()?),
        )
        .output()
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&out.stderr).into_owned();
    c.check(
        "the process exits non-zero on a malformed admin DID, naming access.admin_did",
        out.status.code() == Some(1) && text.contains("access.admin_did"),
        format!(
            "exit {:?}: {}",
            out.status.code(),
            support::truncate(&text, 200)
        ),
    );
    Ok(())
}

// ------------------------------------------------- 2. modes and metadata

async fn check_modes(c: &mut Checks, ctx: &Ctx, a: &Srv) -> Result<(), String> {
    c.section("2. client modes and the client metadata document");
    let http = ctx.fresh();
    let get = |host: &'static str, path: &'static str| {
        let http = http.clone();
        let base = a.base.clone();
        async move { http.get(&format!("{base}{path}"), &[("host", host)]).await }
    };
    let meta = get(HOSTNAME, "/.well-known/atproto-oauth-client-metadata").await?;
    let m = &meta.body;
    let id = format!("https://{HOSTNAME}/.well-known/atproto-oauth-client-metadata");
    c.check(
        "the client metadata is served on the instance's hostname: JSON, public max-age=3600, every field of the profile, authorization_code only",
        meta.status == 200
            && meta.header("content-type").as_deref() == Some("application/json")
            && meta.header("cache-control").as_deref() == Some("public, max-age=3600")
            && m["client_id"] == id.as_str()
            && m["redirect_uris"] == serde_json::json!([format!("https://{HOSTNAME}/enter/callback")])
            && m["grant_types"] == serde_json::json!(["authorization_code"])
            && m["response_types"] == serde_json::json!(["code"])
            && m["token_endpoint_auth_method"] == "none"
            && m["scope"] == "atproto"
            && m["application_type"] == "web"
            && m["dpop_bound_access_tokens"] == true
            && m["client_uri"] == format!("https://{HOSTNAME}/").as_str(),
        meta.short(),
    );
    let on_443 = http
        .get(
            &format!("{}/.well-known/atproto-oauth-client-metadata", a.base),
            &[("host", "farsight.test:443")],
        )
        .await?;
    let other = get(
        "other.example",
        "/.well-known/atproto-oauth-client-metadata",
    )
    .await?;
    let loopback = http
        .get(
            &format!("{}/.well-known/atproto-oauth-client-metadata", a.base),
            &[],
        )
        .await?;
    c.check(
        "the metadata answers on Host farsight.test:443 too, and is 404 on another Host and on the loopback address",
        on_443.status == 200 && other.status == 404 && loopback.status == 404,
        format!("{} / {} / {}", on_443.status, other.status, loopback.status),
    );
    let hosted = get(HOSTNAME, "/enter").await?;
    let loop_page = http.get(&format!("{}/enter", a.base), &[]).await?;
    let localhost = http
        .get(&format!("{}/enter", a.base), &[("host", "localhost:8080")])
        .await?;
    let elsewhere = get("other.example", "/enter").await?;
    let button =
        |r: &Resp| r.text.contains("Sign in with ATProto") && r.text.contains("action=\"/enter\"");
    c.check(
        "/enter has the sign-in button on the hostname and on 127.0.0.1; one button, no input field; never cached",
        hosted.status == 200
            && loop_page.status == 200
            && button(&hosted)
            && button(&loop_page)
            && !hosted.text.contains("<input")
            && hosted.header("cache-control").as_deref() == Some("no-store, private")
            && hosted.text.contains("does not sign you out of your ATProto account"),
        format!("{} / {}", hosted.status, loop_page.status),
    );
    c.check(
        "/enter on localhost or on another Host has no button: it says where to sign in (the hostname's URL) and gives the loopback instructions with 127.0.0.1, not localhost",
        [&localhost, &elsewhere].iter().all(|r| {
            r.status == 200
                && !button(r)
                && r.text.contains("https://farsight.test/enter")
                && r.text.contains("http://127.0.0.1:PORT/enter")
                && r.text.contains("not <code>localhost</code>")
        }),
        format!("{} / {}", localhost.status, elsewhere.status),
    );
    let posted = ctx.start(a, &ctx.fresh(), "other.example").await?;
    c.check(
        "POST /enter on a Host that is neither is 400 with the same page; nothing is started",
        posted.resp.status == 400
            && posted.location.is_none()
            && posted.cookie.is_none()
            && !button(&posted.resp),
        posted.resp.short(),
    );
    let cross = ctx
        .fresh()
        .post_form(
            &format!("{}/enter", a.base),
            &[("origin", "https://evil.example")],
            &[],
        )
        .await?;
    c.check(
        "a cross-origin POST /enter is refused (403) before anything else",
        cross.status == 403 && cross.header("location").is_none(),
        cross.short(),
    );
    Ok(())
}

/// Loopback mode is for local clients, and outbound requests ignore a
/// proxy named in the environment. The server trusts loopback as a proxy,
/// so the harness can present a request as forwarded for a public
/// address; its environment names a proxy nothing listens on.
async fn check_local_only(c: &mut Checks, ctx: &Ctx) -> Result<(), String> {
    c.section("2b. loopback mode and the client's address; proxies in the environment");
    let cfg = format!(
        "{}\n[proxy]\nmode = \"forwarded\"\ntrusted = [\"127.0.0.0/8\"]\n",
        ctx.config(HOSTNAME, &format!("admin_ui = true\nadmin_did = \"{D1}\""))
    );
    let dead = "http://127.0.0.1:1";
    let s = Srv::with_config(
        "local",
        &cfg,
        &[
            ("HTTP_PROXY", dead),
            ("HTTPS_PROXY", dead),
            ("ALL_PROXY", dead),
            ("http_proxy", dead),
            ("https_proxy", dead),
            ("all_proxy", dead),
        ],
    )
    .await?;
    let host = s.loopback();
    let remote = [("host", host.as_str()), ("x-forwarded-for", "203.0.113.9")];
    let button =
        |r: &Resp| r.text.contains("Sign in with ATProto") && r.text.contains("action=\"/enter\"");
    let http = ctx.fresh();
    let page = http.get(&format!("{}/enter", s.base), &remote).await?;
    let local_page = http
        .get(&format!("{}/enter", s.base), &[("host", host.as_str())])
        .await?;
    let _ = ctx.standin.take_events();
    let posted = http
        .post_form(&format!("{}/enter", s.base), &remote, &[])
        .await?;
    let sent = ctx.standin.take_events().len();
    c.check(
        "Host 127.0.0.1 from a client at a public address is not loopback mode: no button, POST /enter is 400, no flow cookie, no request to the account's server",
        page.status == 200
            && !button(&page)
            && posted.status == 400
            && posted.header("location").is_none()
            && set_cookie(&posted, "farsight_flow").is_none()
            && sent == 0,
        format!(
            "page {} button {}; POST {} location {:?}; requests the account's server received: {sent}",
            page.status,
            button(&page),
            posted.status,
            posted.header("location")
        ),
    );
    let signed = ctx.sign_in(&s, &ctx.fresh(), &host).await?;
    c.check(
        "the same Host from a local client has the button and a sign-in completes — with HTTP_PROXY, HTTPS_PROXY and ALL_PROXY naming a proxy nothing listens on, so no outbound request went through it",
        button(&local_page)
            && signed.status == 200
            && set_cookie(&signed, "farsight_admin").is_some(),
        format!("page button {}; sign-in {}", button(&local_page), signed.short()),
    );
    ctx.retire(s);
    Ok(())
}

async fn check_unhostable(c: &mut Checks, ctx: &Ctx) -> Result<(), String> {
    let mut detail = Vec::new();
    let mut ok = true;
    for hostname in ["203.0.113.7", "farsight", "farsight.test:8443"] {
        let s = Srv::with_config(
            "ip",
            &ctx.config(hostname, &format!("admin_did = \"{D1}\"")),
            &[],
        )
        .await?;
        let http = ctx.fresh();
        let meta = http
            .get(
                &format!("{}/.well-known/atproto-oauth-client-metadata", s.base),
                &[("host", hostname)],
            )
            .await?;
        let page = http
            .get(&format!("{}/enter", s.base), &[("host", hostname)])
            .await?;
        let loop_page = http.get(&format!("{}/enter", s.base), &[]).await?;
        let signed = ctx.sign_in(&s, &ctx.fresh(), &s.loopback()).await?;
        let good = meta.status == 404
            && page.status == 200
            && !page.text.contains("Sign in with ATProto")
            && page.text.contains("cannot be used for ATProto sign-in")
            && !page.text.contains("Sign in at <a")
            && loop_page.text.contains("Sign in with ATProto")
            && signed.status == 200
            && set_cookie(&signed, "farsight_admin").is_some();
        ok &= good;
        detail.push(format!(
            "{hostname}: metadata {} page {} sign-in {}",
            meta.status, page.status, signed.status
        ));
        ctx.retire(s);
    }
    c.check(
        "a hostname that is an IP address, a single label or has a port cannot be a hosted client: no metadata (404), no button on that Host — only the loopback instructions — and the button on 127.0.0.1, where a sign-in completes",
        ok,
        detail.join("; "),
    );
    Ok(())
}

// ------------------------------------------------------ 3. loopback flow

async fn check_loopback_flow(c: &mut Checks, ctx: &Ctx, a: &Srv) -> Result<String, String> {
    c.section("3. a loopback sign-in, end to end");
    ctx.standin.take_events();
    let http = ctx.fresh();
    let host = a.loopback();
    let st = ctx.start(a, &http, &host).await?;
    let loc = st.location.clone().unwrap_or_default();
    let cookie_header = st
        .resp
        .headers
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find(|v| v.starts_with("farsight_flow="))
        .unwrap_or_default()
        .to_owned();
    let redirect = format!("http://{host}/enter/callback");
    let client_id = query_of(&loc, "client_id").unwrap_or_default();
    c.check(
        "POST /enter on 127.0.0.1 answers 303 to the authorization endpoint with only client_id and request_uri, no-store",
        st.resp.status == 303
            && loc.starts_with(&format!("{}/oauth/authorize?", ctx.standin.base))
            && reqwest::Url::parse(&loc).is_ok_and(|u| {
                let mut keys: Vec<String> = u.query_pairs().map(|(k, _)| k.into_owned()).collect();
                keys.sort();
                keys == ["client_id", "request_uri"]
            })
            && st.resp.header("cache-control").as_deref() == Some("no-store, private"),
        format!("{} {}", st.resp.status, support::truncate(&loc, 160)),
    );
    c.check(
        "the loopback client_id is http://localhost with the redirect_uri (this Host, port included) and scope=atproto in its query",
        reqwest::Url::parse(&client_id).is_ok_and(|u| {
            u.scheme() == "http"
                && u.host_str() == Some("localhost")
                && u.port().is_none()
                && u.path() == "/"
                && u.query_pairs().any(|(k, v)| k == "redirect_uri" && v == redirect.as_str())
                && u.query_pairs().any(|(k, v)| k == "scope" && v == "atproto")
        }),
        client_id.clone(),
    );
    c.check(
        "the flow cookie: farsight_flow, 256 random bits, Path=/enter, HttpOnly, SameSite=Lax, Max-Age=600 (no Secure on plain HTTP)",
        cookie_header.split("; ").collect::<Vec<_>>()[1..]
            == ["Path=/enter", "HttpOnly", "SameSite=Lax", "Max-Age=600"]
            && st.cookie.as_deref().is_some_and(|c| c.len() == "farsight_flow=".len() + 43),
        cookie_header.split(';').skip(1).collect::<Vec<_>>().join(";"),
    );
    let events = ctx.standin.take_events();
    let pars: Vec<_> = events.iter().filter(|e| e.kind == "par").collect();
    let last = pars.last();
    let f = |k: &str| {
        last.and_then(|e| e.form.get(k))
            .cloned()
            .unwrap_or_default()
    };
    c.check(
        "the pushed authorization request carries client_id, redirect_uri, response_type=code, scope=atproto, a 256-bit state, an S256 challenge and login_hint = the admin DID",
        f("client_id") == client_id
            && f("redirect_uri") == redirect
            && f("response_type") == "code"
            && f("scope") == "atproto"
            && f("state").len() == 43
            && f("code_challenge").len() == 43
            && f("code_challenge_method") == "S256"
            && f("login_hint") == D1
            && last.is_some_and(|e| e.status == 201),
        format!("{:?}", last.map(|e| e.form.keys().collect::<Vec<_>>())),
    );
    ctx.secret(&f("state"));
    let proof = last.and_then(|e| e.proof.clone());
    c.check(
        "the PAR request has a DPoP proof: typ dpop+jwt, ES256, a public P-256 jwk, htm POST, htu the endpoint, a jti, an iat (the stand-in verified its signature)",
        proof.as_ref().is_some_and(|(h, cl)| {
            h["typ"] == "dpop+jwt"
                && h["alg"] == "ES256"
                && h["jwk"]["crv"] == "P-256"
                && h["jwk"].get("d").is_none()
                && cl["htm"] == "POST"
                && cl["htu"] == format!("{}/oauth/par", ctx.standin.base).as_str()
                && cl["jti"].is_string()
                && cl["iat"].is_i64()
        }),
        format!("{:?}", proof.as_ref().map(|p| &p.1)),
    );
    c.check(
        "DPoP nonce: the first PAR (no nonce) is answered use_dpop_nonce, and Farsight repeats it once with the server's nonce",
        pars.len() == 2
            && pars[0].error.as_deref() == Some("dpop")
            && pars[0].proof.as_ref().is_some_and(|(_, cl)| cl.get("nonce").is_none())
            && pars[1].proof.as_ref().is_some_and(|(_, cl)| cl["nonce"].is_string())
            && pars[0].proof.as_ref().map(|p| &p.1["jti"]) != pars[1].proof.as_ref().map(|p| &p.1["jti"]),
        format!("{:?}", pars.iter().map(|e| e.status).collect::<Vec<_>>()),
    );
    let cb = ctx.approve(&loc).await?;
    c.check(
        "the authorization server sends the browser to http://127.0.0.1:<port>/enter/callback with code, state and iss",
        cb.starts_with(&format!("{redirect}?"))
            && query_of(&cb, "code").is_some()
            && query_of(&cb, "state") == Some(f("state"))
            && query_of(&cb, "iss").as_deref() == Some(ctx.standin.base.as_str()),
        support::truncate(cb.split('?').next().unwrap_or_default(), 120),
    );
    let before = ctx.n("SELECT count(*) FROM admin_sessions").await?;
    let done = ctx.callback(a, &http, &cb, st.cookie.as_deref()).await?;
    let admin_cookie = set_cookie(&done, "farsight_admin").unwrap_or_default();
    let cookies: Vec<String> = done
        .headers
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_owned))
        .collect();
    c.check(
        "the callback answers 200 (not a redirect) with a page that continues to /admin by meta refresh and a Continue link; no-store, Referrer-Policy: no-referrer",
        done.status == 200
            && done.header("location").is_none()
            && done.text.contains("<meta http-equiv=\"refresh\" content=\"0;url=/admin\">")
            && done.text.contains("<a class=\"button\" href=\"/admin\">Continue</a>")
            && done.header("cache-control").as_deref() == Some("no-store, private")
            && done.header("referrer-policy").as_deref() == Some("no-referrer"),
        done.short(),
    );
    c.check(
        "it sets farsight_admin (Path=/, HttpOnly, SameSite=Strict) and clears farsight_flow",
        cookies.iter().any(|c| {
            c.starts_with("farsight_admin=") && c.ends_with("; Path=/; HttpOnly; SameSite=Strict")
        }) && cookies
            .iter()
            .any(|c| c.starts_with("farsight_flow=;") && c.contains("Max-Age=0")),
        format!(
            "{:?}",
            cookies
                .iter()
                .map(|c| c.split(';').skip(1).collect::<String>())
                .collect::<Vec<_>>()
        ),
    );
    let events = ctx.standin.take_events();
    let tok = events.iter().find(|e| e.kind == "token");
    c.check(
        "the token request: authorization_code, the code, the PKCE verifier, the same client_id and redirect_uri, a DPoP proof under the PAR key with the nonce — accepted first time",
        tok.is_some_and(|e| {
            e.status == 200
                && e.form.get("grant_type").map(String::as_str) == Some("authorization_code")
                && e.form.get("code_verifier").is_some_and(|v| v.len() == 43)
                && e.form.get("client_id") == Some(&client_id)
                && e.form.get("redirect_uri") == Some(&redirect)
                && e.proof.as_ref().map(|p| &p.0["jwk"]) == proof.as_ref().map(|p| &p.0["jwk"])
                && e.proof.as_ref().is_some_and(|p| p.1["htu"] == format!("{}/oauth/token", ctx.standin.base).as_str())
        }) && events.iter().filter(|e| e.kind == "token").count() == 1,
        format!("{:?}", tok.map(|e| (e.status, &e.error))),
    );
    if let Some(v) = tok.and_then(|e| e.form.get("code_verifier")) {
        ctx.secret(v);
    }
    let raw = admin_cookie.split_once('=').map_or("", |x| x.1);
    let key = farsight_web::pages::oauth_session_key(raw, D1);
    let mut manual = Sha256::new();
    manual.update(raw.as_bytes());
    manual.update([0u8]);
    manual.update(D1.as_bytes());
    let manual: [u8; 32] = manual.finalize().into();
    let after = ctx.n("SELECT count(*) FROM admin_sessions").await?;
    c.check(
        "one admin_sessions row is created, keyed SHA-256(cookie ‖ 0x00 ‖ did); no row has the plain SHA-256(cookie)",
        after == before + 1
            && key == manual
            && ctx.has_session_key(&manual).await?
            && !ctx.has_session_key(&Sha256::digest(raw.as_bytes())).await?,
        format!("{before} → {after}"),
    );
    let settings = http
        .get(
            &format!("{}/admin/settings", a.base),
            &[("cookie", &admin_cookie)],
        )
        .await?;
    let enter = http
        .get(&format!("{}/enter", a.base), &[("cookie", &admin_cookie)])
        .await?;
    c.check(
        "the session works (Settings 200), and /enter with a session redirects to /admin",
        settings.status == 200
            && enter.status == 303
            && enter.header("location").as_deref() == Some("/admin"),
        format!("{} / {}", settings.status, enter.status),
    );
    let schema = ctx
        .n("SELECT count(*) FROM information_schema.columns WHERE table_name = 'admin_sessions'")
        .await?;
    let migrations = ctx.n("SELECT max(version) FROM _sqlx_migrations").await?;
    c.check(
        "admin_sessions has six columns, and one migration creates the schema",
        schema == 6 && migrations == 1,
        format!("{schema} columns, migration {migrations}"),
    );
    Ok(admin_cookie)
}

// -------------------------------------------------------- 4. hosted flow

async fn check_hosted_flow(c: &mut Checks, ctx: &Ctx, a: &Srv) -> Result<(), String> {
    c.section("4. a hosted sign-in, end to end");
    ctx.standin.client_metadata_at(&a.base, HOSTNAME);
    ctx.standin.take_events();
    let fetched_before = ctx.standin.fetched_metadata().len();
    let http = ctx.fresh();
    let st = ctx.start(a, &http, HOSTNAME).await?;
    let loc = st.location.clone().unwrap_or_default();
    let id = format!("https://{HOSTNAME}/.well-known/atproto-oauth-client-metadata");
    c.check(
        "POST /enter on the hostname starts a flow as the hosted client: client_id is the metadata URL",
        st.resp.status == 303 && query_of(&loc, "client_id").as_deref() == Some(id.as_str()),
        st.resp.short(),
    );
    let fetched = ctx.standin.fetched_metadata();
    c.check(
        "the stand-in fetched that client_id's document from Farsight and accepted it under the profile's client rules, with grant_types = [authorization_code] and no refresh_token (against the stand-in)",
        fetched.len() > fetched_before
            && fetched.last().is_some_and(|m| m["grant_types"] == serde_json::json!(["authorization_code"]))
            && ctx.standin.take_events().iter().any(|e| e.kind == "par" && e.status == 201),
        format!("{} fetched", fetched.len() - fetched_before),
    );
    let cb = ctx.approve(&loc).await?;
    let done = ctx.callback(a, &http, &cb, st.cookie.as_deref()).await?;
    let admin = set_cookie(&done, "farsight_admin").unwrap_or_default();
    let ops = http
        .get(
            &format!("{}/admin/ops", a.base),
            &[("cookie", &admin), ("host", HOSTNAME)],
        )
        .await?;
    c.check(
        "the callback on https://farsight.test/enter/callback completes: 200, session cookie, and the session opens an admin page",
        cb.starts_with(&format!("https://{HOSTNAME}/enter/callback?"))
            && done.status == 200
            && !admin.is_empty()
            && ops.status == 200,
        format!("{} / {}", done.status, ops.status),
    );
    Ok(())
}

// ------------------------------------------------------------ 5. nonces

async fn check_nonces(c: &mut Checks, ctx: &Ctx, a: &Srv) -> Result<(), String> {
    c.section("5. DPoP nonces");
    let host = a.loopback();
    ctx.standin.take_events();
    ctx.standin.set(Knobs {
        nonce_never_accepted: true,
        ..Knobs::default()
    });
    let st = ctx.start(a, &ctx.fresh(), &host).await?;
    let pars = ctx.standin.take_events();
    c.check(
        "a server that keeps answering use_dpop_nonce gets exactly one retry; the start then fails with 502, a generic message, no cookie and no flow",
        st.resp.status == 502
            && pars.iter().filter(|e| e.kind == "par").count() == 2
            && st.cookie.is_none()
            && st.location.is_none()
            && banner(&st.resp.text).contains("could not be reached"),
        format!("{} after {} PAR requests", st.resp.status, pars.len()),
    );
    ctx.standin.reset();
    Ok(())
}

// ---------------------------------------------------- 6. callback checks

async fn check_callback(c: &mut Checks, ctx: &Ctx, a: &Srv) -> Result<(), String> {
    c.section("6. the callback's checks, in order");
    let host = a.loopback();
    let refused = |r: &Resp, status: u16| {
        r.status == status
            && r.text.contains(REFUSED)
            && set_cookie(r, "farsight_admin").is_none()
            && r.header("cache-control").as_deref() == Some("no-store, private")
            && r.header("referrer-policy").as_deref() == Some("no-referrer")
    };
    // Flow 1: cookie and state presence, the wrong cookie, then success
    // and replay.
    let http = ctx.fresh();
    let (cb, cookie) = ctx.pending(a, &http, &host).await?;
    let no_cookie = ctx.callback(a, &ctx.fresh(), &cb, None).await?;
    let no_state = ctx
        .callback(
            a,
            &ctx.fresh(),
            &replace_query(&cb, "state", None),
            Some(&cookie),
        )
        .await?;
    let unknown_state = ctx
        .callback(
            a,
            &ctx.fresh(),
            &replace_query(
                &cb,
                "state",
                Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            ),
            Some(&cookie),
        )
        .await?;
    let wrong_cookie = ctx
        .callback(
            a,
            &ctx.fresh(),
            &cb,
            Some("farsight_flow=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
        )
        .await?;
    c.check(
        "no flow cookie ⇒ 400; no state ⇒ 400; an unknown state ⇒ 400; the real state with another cookie ⇒ 400 — each the same page, no session",
        [&no_cookie, &no_state, &unknown_state, &wrong_cookie]
            .iter()
            .all(|r| refused(r, 400)),
        format!(
            "{} {} {} {}",
            no_cookie.status, no_state.status, unknown_state.status, wrong_cookie.status
        ),
    );
    let tokens_before = ctx.standin.take_events();
    drop(tokens_before);
    let good = ctx.callback(a, &ctx.fresh(), &cb, Some(&cookie)).await?;
    c.check(
        "none of those spent the flow: the same callback with the right cookie then signs in (a caller without the cookie cannot spend someone else's flow)",
        good.status == 200 && set_cookie(&good, "farsight_admin").is_some(),
        good.short(),
    );
    let replay = ctx.callback(a, &ctx.fresh(), &cb, Some(&cookie)).await?;
    let events = ctx.standin.take_events();
    c.check(
        "a replay of the completed callback ⇒ 400: the record was removed, and no second token request is made",
        refused(&replay, 400) && events.iter().filter(|e| e.kind == "token").count() == 1,
        format!("{} with {} token requests", replay.status, events.len()),
    );
    // iss.
    ctx.standin.set(Knobs {
        wrong_iss: true,
        ..Knobs::default()
    });
    let (cb, cookie) = ctx.pending(a, &ctx.fresh(), &host).await?;
    ctx.standin.reset();
    let wrong_iss = ctx.callback(a, &ctx.fresh(), &cb, Some(&cookie)).await?;
    let again = ctx
        .callback(
            a,
            &ctx.fresh(),
            &replace_query(&cb, "iss", Some(&ctx.standin.base)),
            Some(&cookie),
        )
        .await?;
    let (cb2, cookie2) = ctx.pending(a, &ctx.fresh(), &host).await?;
    let no_iss = ctx
        .callback(
            a,
            &ctx.fresh(),
            &replace_query(&cb2, "iss", None),
            Some(&cookie2),
        )
        .await?;
    let events = ctx.standin.take_events();
    c.check(
        "an iss that is not the flow's issuer, or no iss ⇒ 400 with no token request; the state is spent by it (the corrected callback is 400 too)",
        refused(&wrong_iss, 400)
            && refused(&no_iss, 400)
            && refused(&again, 400)
            && !events.iter().any(|e| e.kind == "token"),
        format!("{} {} {}", wrong_iss.status, no_iss.status, again.status),
    );
    // error.
    ctx.standin.set(Knobs {
        deny: true,
        ..Knobs::default()
    });
    let (cb, cookie) = ctx.pending(a, &ctx.fresh(), &host).await?;
    ctx.standin.reset();
    let denied = ctx.callback(a, &ctx.fresh(), &cb, Some(&cookie)).await?;
    let (cb2, cookie2) = ctx.pending(a, &ctx.fresh(), &host).await?;
    let no_code = ctx
        .callback(
            a,
            &ctx.fresh(),
            &replace_query(&cb2, "code", None),
            Some(&cookie2),
        )
        .await?;
    let events = ctx.standin.take_events();
    c.check(
        "an error callback (access_denied), or one without a code ⇒ 400, no token request; the server's error value is not shown",
        refused(&denied, 400)
            && refused(&no_code, 400)
            && !denied.text.contains("access_denied")
            && !events.iter().any(|e| e.kind == "token"),
        format!("{} {}", denied.status, no_code.status),
    );
    // A code that is not the flow's: the token endpoint refuses.
    let (cb, cookie) = ctx.pending(a, &ctx.fresh(), &host).await?;
    let bad_code = ctx
        .callback(
            a,
            &ctx.fresh(),
            &replace_query(&cb, "code", Some("cod-forged")),
            Some(&cookie),
        )
        .await?;
    c.check(
        "a forged code ⇒ 502 (the token endpoint refuses it), no session",
        refused(&bad_code, 502),
        format!("{}", bad_code.status),
    );
    // Token response shape.
    let mut shapes = Vec::new();
    for (what, knobs) in [
        (
            "token_type Bearer",
            Knobs {
                token_type: Some("Bearer".into()),
                ..Knobs::default()
            },
        ),
        (
            "scope without atproto",
            Knobs {
                drop_scope: true,
                ..Knobs::default()
            },
        ),
        (
            "empty sub",
            Knobs {
                sub: Some(String::new()),
                ..Knobs::default()
            },
        ),
    ] {
        ctx.standin.set(knobs);
        let r = ctx.sign_in(a, &ctx.fresh(), &host).await?;
        ctx.standin.reset();
        if !refused(&r, 502) {
            shapes.push(format!("{what}: {}", r.status));
        }
    }
    c.check(
        "a token response that is not DPoP-bound, does not grant atproto, or has no sub ⇒ 502, no session",
        shapes.is_empty(),
        shapes.join("; "),
    );
    // sub.
    let sessions = ctx.n("SELECT count(*) FROM admin_sessions").await?;
    ctx.standin.set(Knobs {
        sub: Some(OTHER.into()),
        ..Knobs::default()
    });
    let other = ctx.sign_in(a, &ctx.fresh(), &host).await?;
    let other2 = ctx.sign_in(a, &ctx.fresh(), &host).await?;
    ctx.standin.reset();
    c.check(
        "a flow completed by another account (sub ≠ the admin DID) ⇒ 403, same page, no session row",
        refused(&other, 403)
            && refused(&other2, 403)
            && ctx.n("SELECT count(*) FROM admin_sessions").await? == sessions,
        format!("{} {}", other.status, other2.status),
    );
    let log = a.log();
    let warns: Vec<&str> = log
        .lines()
        .filter(|l| l.contains("the flow completed for another account"))
        .collect();
    c.check(
        "the mismatch is logged at WARN with the sub DID, at most once a minute (two mismatches, one line)",
        warns.len() == 1 && warns[0].contains("\"WARN\"") && warns[0].contains(OTHER),
        format!("{} lines", warns.len()),
    );
    c.unverified(
        "sub equals the flow's DID but the configured admin DID changed mid-flow ⇒ 403",
        "not reachable over HTTP: the admin DID changes only at a restart, which drops every flow. The comparison is in the callback next to the sub check (enter.rs) and was read, not exercised",
    );
    // Two tabs.
    let http = ctx.fresh();
    let first = ctx.start(a, &http, &host).await?;
    let second = ctx.start(a, &http, &host).await?;
    let cb1 = ctx
        .approve(first.location.as_deref().unwrap_or_default())
        .await?;
    let cb2 = ctx
        .approve(second.location.as_deref().unwrap_or_default())
        .await?;
    let tab1 = ctx
        .callback(a, &http, &cb1, second.cookie.as_deref())
        .await?;
    let tab2 = ctx
        .callback(a, &http, &cb2, second.cookie.as_deref())
        .await?;
    c.check(
        "two tabs: the second start replaces the cookie; the first tab's callback is refused (400), the second signs in",
        refused(&tab1, 400) && tab2.status == 200,
        format!("{} / {}", tab1.status, tab2.status),
    );
    Ok(())
}

// ------------------------------------------------------- 7. flow lifetime

async fn check_lifetime(c: &mut Checks, ctx: &Ctx, cfg: &str) -> Result<(), String> {
    c.section("7. flow lifetime");
    let s = Srv::with_config("ttl", cfg, &[("FARSIGHT_HARNESS_FLOW_TTL_SECS", "2")]).await?;
    let host = s.loopback();
    let (cb, cookie) = ctx.pending(&s, &ctx.fresh(), &host).await?;
    tokio::time::sleep(Duration::from_millis(3200)).await;
    ctx.standin.take_events();
    let late = ctx.callback(&s, &ctx.fresh(), &cb, Some(&cookie)).await?;
    let events = ctx.standin.take_events();
    let (cb2, cookie2) = ctx.pending(&s, &ctx.fresh(), &host).await?;
    let in_time = ctx.callback(&s, &ctx.fresh(), &cb2, Some(&cookie2)).await?;
    c.check(
        "a callback after the flow's lifetime (2 s in this server, by the harness hook; 10 min in a release build) ⇒ 400 with no token request, though the sweeper (every minute) has not run; one inside it signs in",
        late.status == 400
            && late.text.contains(REFUSED)
            && !events.iter().any(|e| e.kind == "token")
            && in_time.status == 200,
        format!("{} / {}", late.status, in_time.status),
    );
    c.unverified(
        "at most 256 flows are held and a start beyond that evicts the oldest; 10-minute lifetime as a constant",
        "not over HTTP: 256 starts take over four minutes through the process-wide bucket. Covered by the unit tests oauth::tests::flow_store_evicts_the_oldest_when_full and flow_store_single_use_cookie_and_lifetime (cargo test), which exercise the same FlowStore with MAX_FLOWS and FLOW_TTL",
    );
    ctx.retire(s);
    Ok(())
}

// ------------------------------------------------------------- 8. gates

async fn check_gates(c: &mut Checks, ctx: &Ctx) -> Result<(), String> {
    c.section("8. gate and /robots.txt by configuration");
    const GATED: [&str; 9] = [
        "/admin",
        "/admin/dashboard/fragment",
        "/admin/lookup/did",
        "/admin/lookup/list",
        "/admin/ops",
        "/admin/settings",
        "/admin/reset",
        "/admin/did/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/history",
        "/admin/list/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/3kabc/history",
    ];
    let to_enter = |r: &Resp| r.status == 303 && r.header("location").as_deref() == Some("/enter");
    // Both UIs on.
    let s = Srv::with_config(
        "both",
        &ctx.config(
            HOSTNAME,
            &format!("admin_ui = true\npublic_ui = true\nadmin_did = \"{D1}\""),
        ),
        &[],
    )
    .await?;
    let http = ctx.fresh();
    let unknown = http.get(&format!("{}/no-such-route", s.base), &[]).await?;
    let bare = |r: &Resp| {
        r.status == 404
            && r.text == unknown.text
            && r.header("cache-control") == unknown.header("cache-control")
            && r.header("location").is_none()
    };
    let mut bad = Vec::new();
    for p in GATED {
        let r = http.get(&format!("{}{p}", s.base), &[]).await?;
        if !to_enter(&r) {
            bad.push(format!("{p}: {}", r.short()));
        }
        // What htmx sends (the dashboard's poll, the history tables'
        // "Next" links): a redirect would be followed and swapped in.
        let hx = http
            .get(&format!("{}{p}", s.base), &[("hx-request", "true")])
            .await?;
        if !bare(&hx) {
            bad.push(format!("{p} (HX-Request): {}", hx.short()));
        }
    }
    let post = http
        .post_form(
            &format!("{}/admin/settings", s.base),
            &[],
            &[("config", "x")],
        )
        .await?;
    c.check(
        "both UIs on, no session: every admin page (dashboard, fragment, lookups, operations, settings, reset, both history pages) redirects (303) to /enter — none answers 404 to hide itself; the same request made by htmx (HX-Request) gets the bare 404 of an unknown path, byte for byte, with no redirect; a POST redirects too",
        bad.is_empty()
            && unknown.status == 404
            && unknown.text == "not found"
            && unknown.header("cache-control").as_deref() == Some("no-store")
            && to_enter(&post),
        bad.join("; "),
    );
    let enter = http.get(&format!("{}/enter", s.base), &[]).await?;
    let cb = http
        .get(&format!("{}/enter/callback?state=x", s.base), &[])
        .await?;
    let meta = http
        .get(
            &format!("{}/.well-known/atproto-oauth-client-metadata", s.base),
            &[("host", HOSTNAME)],
        )
        .await?;
    let css = http
        .get(&format!("{}/static/farsight.css", s.base), &[])
        .await?;
    let public = http.get(&format!("{}/", s.base), &[]).await?;
    c.check(
        "both UIs on: /enter renders the sign-in page with the brand and no nav link, /enter/callback answers (400 without a flow), the client metadata and the stylesheet are served, and the public UI is up at / next to the admin pages",
        enter.status == 200
            && enter.text.contains("Sign in with ATProto")
            && enter.text.contains("<span class=\"brand\">")
            && !enter.text.contains("<nav")
            && cb.status == 400
            && cb.text.contains(REFUSED)
            && meta.status == 200
            && css.status == 200
            && public.status == 200
            && public.text.contains("<nav class=\"public-nav\""),
        format!(
            "{} {} {} {} {}",
            enter.status, cb.status, meta.status, css.status, public.status
        ),
    );
    let slash = http.get(&format!("{}/admin/", s.base), &[]).await?;
    c.check(
        "/admin/ redirects to /admin",
        slash.status == 303 && slash.header("location").as_deref() == Some("/admin"),
        format!("/admin/ {} → {:?}", slash.status, slash.header("location")),
    );
    let signed = ctx.sign_in(&s, &http, &s.loopback()).await?;
    let cookie = set_cookie(&signed, "farsight_admin").unwrap_or_default();
    let mut closed = Vec::new();
    for p in [
        "/admin",
        "/admin/dashboard/fragment",
        "/admin/lookup/did",
        "/admin/ops",
        "/admin/settings",
        "/admin/reset",
    ] {
        let r = http
            .get(&format!("{}{p}", s.base), &[("cookie", &cookie)])
            .await?;
        if r.status != 200 {
            closed.push(format!("{p}: {}", r.status));
        }
    }
    let dash = http
        .get(&format!("{}/admin", s.base), &[("cookie", &cookie)])
        .await?;
    let csrf = csrf_of(&dash.text).unwrap_or_default();
    let logout = http
        .post_form(
            &format!("{}/admin/logout", s.base),
            &[("cookie", &cookie)],
            &[("csrf", &csrf)],
        )
        .await?;
    let after = http
        .get(&format!("{}/admin", s.base), &[("cookie", &cookie)])
        .await?;
    c.check(
        "both UIs on: signing in works and opens every page; POST /admin/logout redirects to /enter and the session is gone (/admin redirects to /enter again)",
        signed.status == 200
            && closed.is_empty()
            && to_enter(&logout)
            && to_enter(&after),
        format!("{closed:?} logout {} then {}", logout.status, after.status),
    );
    let robots = http.get(&format!("{}/robots.txt", s.base), &[]).await?;
    ctx.retire(s);
    // The admin UI alone: nothing of it is public.
    let s = Srv::with_config(
        "adminonly",
        &ctx.config(HOSTNAME, &format!("admin_ui = true\nadmin_did = \"{D1}\"")),
        &[],
    )
    .await?;
    let http = ctx.fresh();
    let root = http.get(&format!("{}/", s.base), &[]).await?;
    let mut open = Vec::new();
    for p in GATED {
        let r = http.get(&format!("{}{p}", s.base), &[]).await?;
        if !to_enter(&r) {
            open.push(format!("{p}: {}", r.status));
        }
    }
    let enter = http.get(&format!("{}/enter", s.base), &[]).await?;
    c.check(
        "the admin UI alone (public UI off): / is a 303 to /admin, no-store; there is no anonymous dashboard, fragment or lookup — every admin page redirects (303) to /enter; the sign-in page's header has no \"Dashboard\" or \"Sign in\" link",
        root.status == 303
            && root.header("location").as_deref() == Some("/admin")
            && root
                .header("cache-control")
                .is_some_and(|v| v.contains("no-store"))
            && open.is_empty()
            && enter.status == 200
            && !enter.text.contains("<nav")
            && !enter.text.contains("Dashboard")
            && !enter.text.contains("<a href=\"/enter\">Sign in</a>"),
        format!(
            "/ {} → {:?}; {open:?}",
            root.status,
            root.header("location")
        ),
    );
    let robots_admin = http.get(&format!("{}/robots.txt", s.base), &[]).await?;
    ctx.retire(s);
    // The admin UI off, with and without the public UI.
    let mut disabled = Vec::new();
    let mut off = Vec::new();
    let mut api_only = None;
    for public_ui in [true, false] {
        let s = Srv::with_config(
            "adminoff",
            &ctx.config(
                HOSTNAME,
                &format!("admin_ui = false\npublic_ui = {public_ui}"),
            ),
            &[],
        )
        .await?;
        let http = ctx.fresh();
        let unknown = http.get(&format!("{}/no-such-route", s.base), &[]).await?;
        for (p, host) in [
            ("/admin", ""),
            ("/admin/", ""),
            ("/admin/dashboard/fragment", ""),
            ("/admin/lookup/did", ""),
            ("/admin/ops", ""),
            ("/admin/settings", ""),
            ("/admin/reset", ""),
            (GATED[7], ""),
            ("/admin/card/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", ""),
            ("/enter", ""),
            ("/enter/callback?state=x", ""),
            ("/.well-known/atproto-oauth-client-metadata", HOSTNAME),
        ] {
            let headers: Vec<(&str, &str)> = if host.is_empty() {
                vec![]
            } else {
                vec![("host", host)]
            };
            let r = http.get(&format!("{}{p}", s.base), &headers).await?;
            if r.status != 404 || r.text != unknown.text || r.header("location").is_some() {
                disabled.push(format!("public_ui={public_ui} {p}: {}", r.status));
            }
        }
        for p in ["/enter", "/admin/logout", "/admin/settings"] {
            let post = ctx
                .fresh()
                .post_form(&format!("{}{p}", s.base), &[], &[])
                .await?;
            if post.status != 404 {
                disabled.push(format!("public_ui={public_ui} POST {p}: {}", post.status));
            }
        }
        let robots = http.get(&format!("{}/robots.txt", s.base), &[]).await?;
        let root = http.get(&format!("{}/", s.base), &[]).await?;
        let css = http
            .get(&format!("{}/static/farsight.css", s.base), &[])
            .await?;
        off.push((
            public_ui,
            robots.status,
            robots.text == "User-agent: *\nDisallow: /\n",
            root.status,
            css.status,
        ));
        if public_ui {
            if !root.text.contains("<nav class=\"public-nav\"") {
                disabled.push("public_ui=true /: not the public home".to_owned());
            }
        } else {
            api_only = Some(root);
        }
        ctx.retire(s);
    }
    c.check(
        "admin_ui = false: every admin page, /admin/card, /enter (GET and POST), /enter/callback, the client metadata and logout are the bare 404, with or without the public UI",
        disabled.is_empty(),
        disabled.join("; "),
    );
    c.check(
        "neither UI: / is a short text page — 200, text/plain, starting \"Farsight\", public, max-age=300 — so that an API-only instance does not look broken",
        api_only.as_ref().is_some_and(|r| {
            r.status == 200
                && r.header("content-type")
                    .is_some_and(|t| t.starts_with("text/plain"))
                && r.text.starts_with("Farsight")
                && r.header("cache-control").as_deref() == Some("public, max-age=300")
        }),
        api_only.as_ref().map(Resp::short).unwrap_or_default(),
    );
    c.check(
        "/robots.txt is served in every configuration: 200 with both UIs on and with the admin UI alone; with the admin UI off, 200 and \"Disallow: /\" with the public UI on (crawlable is off) and with it off too; / and the admin stylesheet answer 200 in both",
        robots.status == 200
            && robots_admin.status == 200
            && off == [(true, 200, true, 200, 200), (false, 200, true, 200, 200)],
        format!("{} {} {off:?}", robots.status, robots_admin.status),
    );
    Ok(())
}

// -------------------------------------------------------- 9. rate limits

async fn check_rates(c: &mut Checks, ctx: &Ctx, cfg: &str) -> Result<(), String> {
    c.section("9. rate limits");
    // A fresh process: full buckets, nobody signed in.
    let s = Srv::with_config("rates", cfg, &[]).await?;
    let host = s.loopback();
    // One address signs in first (and so becomes exempt from the
    // process-wide bucket). That is start 1.
    let admin_http = ctx.fresh();
    let signed = ctx.sign_in(&s, &admin_http, &host).await?;
    // Per address: 5/min over starts and callbacks together.
    let one = ctx.fresh();
    let mut statuses = Vec::new();
    for _ in 0..7 {
        let r = one
            .post_form(&format!("{}/enter", s.base), &[("host", &host)], &[])
            .await?;
        statuses.push(r.status);
    }
    let retry = one
        .post_form(&format!("{}/enter", s.base), &[("host", &host)], &[])
        .await?;
    c.check(
        "POST /enter: ui_login per address — five starts, then 429 with Retry-After and no flow",
        statuses[..5].iter().all(|x| *x == 303)
            && statuses[5..].iter().all(|x| *x == 429)
            && retry.header("retry-after").is_some()
            && retry.header("location").is_none(),
        format!("{statuses:?}"),
    );
    let cb_http = ctx.fresh();
    let mut cbs = Vec::new();
    for _ in 0..7 {
        let r = cb_http
            .get(
                &format!("{}/enter/callback?state=x", s.base),
                &[("cookie", "farsight_flow=x")],
            )
            .await?;
        cbs.push(r.status);
    }
    c.check(
        "GET /enter/callback: ui_login per address — five answers (400), then 429",
        cbs[..5].iter().all(|x| *x == 400) && cbs[5..].iter().all(|x| *x == 429),
        format!("{cbs:?}"),
    );
    // Process-wide: 1/s, burst 10. Six starts were charged above (1 + 5)
    // over a few seconds; a burst from fresh addresses empties the rest.
    let begun = Instant::now();
    let mut burst = Vec::new();
    for _ in 0..14 {
        let r = ctx
            .fresh()
            .post_form(&format!("{}/enter", s.base), &[("host", &host)], &[])
            .await?;
        burst.push(r.status);
    }
    let admitted = burst.iter().filter(|x| **x == 303).count();
    let limited = burst.iter().filter(|x| **x == 429).count();
    let ceiling = 10.0 + begun.elapsed().as_secs_f64() + 6.0;
    c.check(
        "the process-wide bucket (1/s, burst 10): fourteen starts from fourteen fresh addresses — each inside its own limit — are not all admitted; the rest get 429",
        limited >= 1 && admitted + limited == 14 && (admitted as f64) <= ceiling,
        format!("{burst:?}"),
    );
    let exempt = admin_http
        .post_form(&format!("{}/enter", s.base), &[("host", &host)], &[])
        .await?;
    let stranger = ctx
        .fresh()
        .post_form(&format!("{}/enter", s.base), &[("host", &host)], &[])
        .await?;
    c.check(
        "with the bucket empty, the address that signed in earlier still starts (303): it is not charged; a fresh address at the same moment gets 429",
        signed.status == 200 && exempt.status == 303 && stranger.status == 429,
        format!("{} / {}", exempt.status, stranger.status),
    );
    let metrics = Http::new(None);
    drop(metrics);
    *ctx.last_start.lock().unwrap_or_else(|e| e.into_inner()) = None;
    ctx.retire(s);
    Ok(())
}

// ---------------------------------------------------------- 10. settings

/// The session a completed sign-in made opens every admin surface, the
/// profile-card route of the admin tables included (stage 8).
async fn check_admin_card(c: &mut Checks, ctx: &Ctx, a: &Srv, cookie: &str) -> Result<(), String> {
    c.section("9b. the signed-in session and /admin/card/{did}");
    sqlx::query("INSERT INTO actors (did) VALUES ($1) ON CONFLICT (did) DO NOTHING")
        .bind(OTHER)
        .execute(&ctx.pool)
        .await
        .map_err(|e| e.to_string())?;
    let url = format!("{}/admin/card/{OTHER}", a.base);
    let with = ctx.fresh().get(&url, &[("cookie", cookie)]).await?;
    let without = ctx.fresh().get(&url, &[]).await?;
    let nowhere = ctx
        .fresh()
        .get(&format!("{}/no/such/route", a.base), &[])
        .await?;
    c.check(
        "the session the OAuth sign-in created opens /admin/card/{did} — 200, the card fragment, no-store, private; without it the route is the bare 404 of an unknown path, never a redirect to /enter, unlike the pages",
        with.status == 200
            && with.text.contains(&format!("<code class=\"pc-did\">{OTHER}</code>"))
            && with.header("cache-control").as_deref() == Some("no-store, private")
            && without.status == 404
            && without.text == nowhere.text
            && without.header("location").is_none(),
        format!("{} / {}", with.status, without.status),
    );
    Ok(())
}

async fn check_settings(c: &mut Checks, ctx: &Ctx, a: &Srv, cookie: &str) -> Result<(), String> {
    c.section("10. Settings");
    let http = ctx.fresh();
    let page = http
        .get(&format!("{}/admin/settings", a.base), &[("cookie", cookie)])
        .await?;
    let csrf = csrf_of(&page.text).unwrap_or_default();
    c.check(
        "Settings shows the admin DID read-only with the CLI to change it",
        page.status == 200
            && page
                .text
                .contains(&format!("Sign-in is as <code>{D1}</code>"))
            && page.text.contains("farsight set-admin-did"),
        page.short(),
    );
    let before = std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())?;
    let mut refusals = Vec::new();
    for (what, text) in [
        ("another DID", before.replace(D1, D2)),
        (
            "no DID",
            before.replace(&format!("admin_did = \"{D1}\"\n"), ""),
        ),
    ] {
        let r = http
            .post_form(
                &format!("{}/admin/settings", a.base),
                &[("cookie", cookie)],
                &[("csrf", csrf.as_str()), ("config", text.as_str())],
            )
            .await?;
        if !(banner(&r.text).contains("cannot be changed here")
            && std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())? == before)
        {
            refusals.push(format!("{what}: {}", banner(&r.text)));
        }
    }
    let harmless = http
        .post_form(
            &format!("{}/admin/settings", a.base),
            &[("cookie", cookie)],
            &[
                ("csrf", csrf.as_str()),
                (
                    "config",
                    before.replace("mailto:ops@", "mailto:admin@").as_str(),
                ),
            ],
        )
        .await?;
    c.check(
        "a Settings save that changes or removes access.admin_did is refused, pointing to the CLI, and writes nothing; a save that leaves it alone is stored",
        refusals.is_empty() && harmless.text.contains("Saved."),
        format!("{refusals:?} / {}", banner(&harmless.text)),
    );
    Ok(())
}

// ------------------------------------------ 10b. a fresh sign-in first

/// What a POST that was refused for want of a fresh sign-in looks like:
/// `303` to the sign-in page, naming the page to come back to.
fn asks_again(r: &Resp, page: &str) -> bool {
    r.status == 303 && r.header("location").as_deref() == Some(&format!("/enter?again={page}"))
}

async fn check_step_up(c: &mut Checks, ctx: &Ctx, cfg: &str) -> Result<(), String> {
    c.section("10b. a fresh sign-in before sensitive actions");
    let s = Srv::with_config("stepup", cfg, &[]).await?;
    let host = s.loopback();
    let http = ctx.fresh();
    let signed = ctx.sign_in(&s, &http, &host).await?;
    let cookie = set_cookie(&signed, "farsight_admin").unwrap_or_default();
    let key_of = |cookie: &str| {
        farsight_web::pages::oauth_session_key(cookie.split_once('=').map_or("", |x| x.1), D1)
    };
    let old_key = key_of(&cookie);
    let settings = format!("{}/admin/settings", s.base);
    let csrf_now = |cookie: String| {
        let (http, settings) = (http.clone(), settings.clone());
        async move {
            let page = http.get(&settings, &[("cookie", &cookie)]).await?;
            Ok::<_, String>(csrf_of(&page.text).unwrap_or_default())
        }
    };
    let file = || std::fs::read_to_string(s.config_path()).map_err(|e| e.to_string());
    let keys = || ctx.n("SELECT count(*) FROM api_tokens");
    let post = |path: &'static str, cookie: String, form: Vec<(&'static str, String)>| {
        let (http, base) = (http.clone(), s.base.clone());
        async move {
            let form: Vec<(&str, &str)> = form.iter().map(|(k, v)| (*k, v.as_str())).collect();
            http.post_form(&format!("{base}{path}"), &[("cookie", &cookie)], &form)
                .await
        }
    };

    // A session that has just signed in does everything.
    let csrf = csrf_now(cookie.clone()).await?;
    let keys_before = keys().await?;
    let made = post(
        "/admin/ops/keys-create",
        cookie.clone(),
        vec![
            ("csrf", csrf.clone()),
            ("name", "fresh-session".into()),
            ("scope", "read".into()),
        ],
    )
    .await?;
    c.check(
        "a session that signed in just now creates an API key: 200, the key shown once, one row more",
        made.status == 200
            && made.text.contains("created.")
            && made.text.contains("fsk_")
            && keys().await? == keys_before + 1,
        made.short(),
    );

    // Eleven minutes later it is the same session, no longer fresh.
    sqlx::query(
        "UPDATE admin_sessions SET created_at = now() - interval '11 minutes' WHERE id_sha256 = $1",
    )
    .bind(&old_key[..])
    .execute(&ctx.pool)
    .await
    .map_err(|e| e.to_string())?;
    let before = file()?;
    let raw = |text: String| {
        post(
            "/admin/settings",
            cookie.clone(),
            vec![("csrf", csrf.clone()), ("config", text)],
        )
    };
    let harmless = raw(before.replace("mailto:ops@", "mailto:later@")).await?;
    let narrowed = raw(file()?.replace("reads = \"public\"", "reads = \"api_key\"")).await?;
    c.check(
        "eleven minutes after signing in the session still saves ordinary settings, and one that narrows access (reads public → api_key)",
        harmless.status == 200
            && banner(&harmless.text).contains("Saved. Changed: server.contact")
            && narrowed.status == 200
            && banner(&narrowed.text).contains("Saved. Changed: access.reads"),
        format!("{} / {}", banner(&harmless.text), banner(&narrowed.text)),
    );
    let base = file()?;
    let other_hash = hex(&farsight_api::auth::sha256("stage7-another-admin-token"));
    let old_hash = hex(&farsight_api::auth::sha256("stage7-admin-token"));
    let keys_before = keys().await?;
    let mut wrong = Vec::new();
    for (what, text) in [
        (
            "backfill.plc_url",
            base.replace(
                &format!("plc_url = \"{}\"", ctx.standin.base),
                "plc_url = \"https://plc.attacker.example\"",
            ),
        ),
        (
            "backfill.relay_url",
            base.replace(
                "[backfill]\n",
                "[backfill]\nrelay_url = \"https://relay.attacker.example\"\n",
            ),
        ),
        (
            "net.allow_http_hosts",
            base.replace(
                &format!("allow_http_hosts = [\"{STANDIN_ADDR}\"]"),
                &format!("allow_http_hosts = [\"{STANDIN_ADDR}\", \"198.51.100.2\"]"),
            ),
        ),
        (
            "auth.admin_token_sha256",
            base.replace(&old_hash, &other_hash),
        ),
        (
            "access.reads widened",
            base.replace("reads = \"api_key\"", "reads = \"public\""),
        ),
    ] {
        if text == base {
            wrong.push(format!(
                "{what}: the edit changed nothing in the harness's config"
            ));
            continue;
        }
        let r = raw(text).await?;
        if !(asks_again(&r, "settings") && file()? == base) {
            wrong.push(format!("{what}: {}", r.short()));
        }
    }
    c.check(
        "the same session's raw-editor save that changes backfill.plc_url, backfill.relay_url, net.allow_http_hosts or auth.admin_token_sha256, or widens access.reads ⇒ 303 to /enter?again=settings, and config.toml is byte for byte what it was",
        wrong.is_empty(),
        format!("{wrong:?}"),
    );
    let rotate = post(
        "/admin/settings/token",
        cookie.clone(),
        vec![("csrf", csrf.clone())],
    )
    .await?;
    let create = post(
        "/admin/ops/keys-create",
        cookie.clone(),
        vec![
            ("csrf", csrf.clone()),
            ("name", "stale-session".into()),
            ("scope", "read".into()),
        ],
    )
    .await?;
    c.check(
        "rotating the admin token ⇒ 303 to /enter?again=settings with the token unchanged and the session kept; creating an API key ⇒ 303 to /enter?again=ops and no row",
        asks_again(&rotate, "settings")
            && file()? == base
            && ctx.has_session_key(&old_key).await?
            && asks_again(&create, "ops")
            && keys().await? == keys_before
            && !rotate.text.contains("fsa_")
            && !create.text.contains("fsk_"),
        format!("{} / {}", rotate.short(), create.short()),
    );
    // Without a form token the answer is the form check's, before anything else.
    let forged = post(
        "/admin/settings/token",
        cookie.clone(),
        vec![("csrf", "0".repeat(64))],
    )
    .await?;
    c.check(
        "the form token is checked first: a rotation with a wrong token is 403, not a redirect to sign in",
        forged.status == 403,
        forged.short(),
    );

    // The sign-in page says why.
    let enter = |query: &'static str, cookie: Option<String>| {
        let (http, base) = (http.clone(), s.base.clone());
        async move {
            let headers: Vec<(&str, &str)> = match &cookie {
                Some(c) => vec![("cookie", c.as_str())],
                None => Vec::new(),
            };
            http.get(&format!("{base}/enter{query}"), &headers).await
        }
    };
    let again = enter("?again=settings", Some(cookie.clone())).await?;
    let again_ops = enter("?again=ops", Some(cookie.clone())).await?;
    let elsewhere = enter("?again=//evil.example", Some(cookie.clone())).await?;
    let anonymous = enter("?again=settings", None).await?;
    c.check(
        "/enter?again=settings with the session: 200, the banner \"Sign in again to change this setting.\" with \"Nothing was changed\", a form that carries next=settings and a \"Sign in again with ATProto\" button, under the page policy (no inline script or style)",
        again.status == 200
            && again.text.contains("<strong>Sign in again to change this setting.</strong> Nothing was changed.")
            && again.text.contains("you are back on Settings")
            && again.text.contains("<input type=\"hidden\" name=\"next\" value=\"settings\">")
            && again.text.contains("Sign in again with ATProto")
            && again_ops.text.contains("<input type=\"hidden\" name=\"next\" value=\"ops\">")
            && again_ops.text.contains("you are back on Operations")
            && again.header("content-security-policy").is_some_and(|p| {
                p.contains("script-src 'self'") && p.contains("style-src 'self'") && !p.contains("unsafe")
            })
            && !again.text.contains("<style")
            && !again.text.contains(" style=\""),
        again.short(),
    );
    c.check(
        "a page name that is not one of the two is ignored (the session goes to /admin as before), and without a session /enter?again=… is the plain sign-in page",
        elsewhere.status == 303
            && elsewhere.header("location").as_deref() == Some("/admin")
            && anonymous.status == 200
            && !anonymous.text.contains("Sign in again")
            && !anonymous.text.contains("name=\"next\"")
            && anonymous.text.contains("Sign in with ATProto"),
        format!("{} / {}", elsewhere.short(), anonymous.status),
    );

    // Signing in again: the same flow, started by the session.
    ctx.pace().await;
    ctx.standin.take_events();
    let sessions_before = ctx.n("SELECT count(*) FROM admin_sessions").await?;
    let started = http
        .post_form(
            &format!("{}/enter", s.base),
            &[("host", host.as_str()), ("cookie", cookie.as_str())],
            &[("next", "settings")],
        )
        .await?;
    let flow_cookie = set_cookie(&started, "farsight_flow").unwrap_or_default();
    ctx.secret(flow_cookie.split_once('=').map_or("", |x| x.1));
    let authorize = started.header("location").unwrap_or_default();
    let par = ctx
        .standin
        .take_events()
        .into_iter()
        .rfind(|e| e.kind == "par");
    c.check(
        "POST /enter with the session and next=settings starts an ordinary flow: 303 to the authorization server, a pushed request with login_hint = the admin DID, a flow cookie",
        started.status == 303
            && authorize.starts_with(&format!("{}/oauth/authorize?", ctx.standin.base))
            && par.as_ref().is_some_and(|e| {
                e.status == 201 && e.form.get("login_hint").map(String::as_str) == Some(D1)
            })
            && !flow_cookie.is_empty(),
        started.short(),
    );
    if let Some(e) = &par {
        ctx.secret(e.form.get("state").map_or("", String::as_str));
    }
    let cb = ctx.approve(&authorize).await?;
    // The callback is a cross-site navigation: the browser sends the flow
    // cookie and not the SameSite=Strict session cookie.
    let done = ctx.callback(&s, &http, &cb, Some(&flow_cookie)).await?;
    let new_cookie = set_cookie(&done, "farsight_admin").unwrap_or_default();
    let new_key = key_of(&new_cookie);
    c.check(
        "the callback answers 200 with a page that continues to /admin/settings (meta refresh and Continue link), and sets a new session cookie",
        done.status == 200
            && done.text.contains("<meta http-equiv=\"refresh\" content=\"0;url=/admin/settings\">")
            && done.text.contains("<a class=\"button\" href=\"/admin/settings\">Continue</a>")
            && !new_cookie.is_empty()
            && new_cookie != cookie,
        done.short(),
    );
    let old_page = http.get(&settings, &[("cookie", &cookie)]).await?;
    c.check(
        "the new session replaces the one that asked: its row is gone, the new one is there, the count is unchanged, and the old cookie is sent to /enter",
        !ctx.has_session_key(&old_key).await?
            && ctx.has_session_key(&new_key).await?
            && ctx.n("SELECT count(*) FROM admin_sessions").await? == sessions_before
            && old_page.status == 303
            && old_page.header("location").as_deref() == Some("/enter"),
        format!("old cookie: {}", old_page.status),
    );

    // Back on the page, the action is repeated and now goes through.
    let csrf2 = csrf_now(new_cookie.clone()).await?;
    let widened = post(
        "/admin/settings",
        new_cookie.clone(),
        vec![
            ("csrf", csrf2.clone()),
            (
                "config",
                file()?.replace("reads = \"api_key\"", "reads = \"public\""),
            ),
        ],
    )
    .await?;
    let keys_before = keys().await?;
    let created = post(
        "/admin/ops/keys-create",
        new_cookie.clone(),
        vec![
            ("csrf", csrf2.clone()),
            ("name", "after-step-up".into()),
            ("scope", "read".into()),
        ],
    )
    .await?;
    c.check(
        "repeated in the new session, the same changes are made: access.reads is public again, and the API key is created",
        widened.status == 200
            && banner(&widened.text).contains("Saved. Changed: access.reads")
            && file()?.contains("reads = \"public\"")
            && created.status == 200
            && created.text.contains("fsk_")
            && keys().await? == keys_before + 1,
        format!("{} / {}", banner(&widened.text), created.status),
    );
    // A posted form is never replayed: nothing named `stale-session` exists.
    let replayed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM api_tokens WHERE name = 'stale-session'")
            .fetch_one(&ctx.pool)
            .await
            .map_err(|e| e.to_string())?;
    c.check(
        "the form that was refused is not replayed after the sign-in: no key of that name exists",
        replayed == 0,
        format!("{replayed}"),
    );
    ctx.retire(s);

    // A second server, whose sign-in has not looked anything up yet.
    let pin = Srv::with_config("pin", cfg, &[]).await?;
    let admin = support::admin_session(&ctx.pool, D1).await?;
    let page = ctx
        .fresh()
        .get(
            &format!("{}/admin/settings", pin.base),
            &[("cookie", &admin)],
        )
        .await?;
    let csrf = csrf_of(&page.text).unwrap_or_default();
    let pin_file = std::fs::read_to_string(pin.config_path()).map_err(|e| e.to_string())?;
    let moved = ctx
        .fresh()
        .post_form(
            &format!("{}/admin/settings", pin.base),
            &[("cookie", &admin)],
            &[
                ("csrf", csrf.as_str()),
                (
                    "config",
                    pin_file
                        .replace(
                            &format!("plc_url = \"{}\"", ctx.standin.base),
                            &format!("plc_url = \"http://{STANDIN_ADDR}:9\""),
                        )
                        .as_str(),
                ),
            ],
        )
        .await?;
    let start = ctx.start(&pin, &ctx.fresh(), &pin.loopback()).await?;
    c.check(
        "the admin DID is resolved through the PLC directory named at start: after a fresh session saved another backfill.plc_url (one that answers nothing), a first sign-in on that server still reaches the admin's own authorization server",
        banner(&moved.text).contains("Saved. Changed: backfill.plc_url")
            && start.resp.status == 303
            && start
                .location
                .as_deref()
                .is_some_and(|l| l.starts_with(&format!("{}/oauth/authorize?", ctx.standin.base))),
        format!("{} / {}", banner(&moved.text), start.resp.short()),
    );
    // The admin token's hash written by hand in the raw editor.
    let pin_file = std::fs::read_to_string(pin.config_path()).map_err(|e| e.to_string())?;
    let page = ctx
        .fresh()
        .get(
            &format!("{}/admin/settings", pin.base),
            &[("cookie", &admin)],
        )
        .await?;
    let csrf = csrf_of(&page.text).unwrap_or_default();
    let sessions = ctx.n("SELECT count(*) FROM admin_sessions").await?;
    let rehashed = ctx
        .fresh()
        .post_form(
            &format!("{}/admin/settings", pin.base),
            &[("cookie", &admin)],
            &[
                ("csrf", csrf.as_str()),
                ("config", pin_file.replace(&old_hash, &other_hash).as_str()),
            ],
        )
        .await?;
    let cleared: Vec<String> = rehashed
        .headers
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_owned))
        .collect();
    let after = ctx
        .fresh()
        .get(
            &format!("{}/admin/settings", pin.base),
            &[("cookie", &admin)],
        )
        .await?;
    c.check(
        "a raw-editor save that changes auth.admin_token_sha256 ends every session, like the rotate button: the save says so, admin_sessions is empty, the cookie is cleared, and the session that saved is sent to /enter",
        sessions >= 1
            && rehashed.status == 200
            && banner(&rehashed.text).contains("Saved. Changed: auth.admin_token_sha256.")
            && banner(&rehashed.text).contains("every session was signed out")
            && ctx.n("SELECT count(*) FROM admin_sessions").await? == 0
            && cleared.iter().any(|c| c.starts_with("farsight_admin=;") && c.contains("Max-Age=0"))
            && after.status == 303
            && after.header("location").as_deref() == Some("/enter"),
        format!("{sessions} sessions before; {}", banner(&rehashed.text)),
    );
    ctx.retire(pin);
    Ok(())
}

// ------------------------------------------------------- 11. no admin DID

async fn check_unconfigured(c: &mut Checks, ctx: &Ctx) -> Result<(), String> {
    c.section("11. no admin DID");
    // A session stored for an admin DID the config does not name.
    let stored = "unconfigured-session-cookie";
    farsight_storage::auth::create_session(
        &ctx.pool,
        &farsight_web::pages::oauth_session_key(stored, D1),
        &[9u8; 32],
        None,
        None,
    )
    .await
    .map_err(|e| e.to_string())?;
    let bare = ctx.config(HOSTNAME, "admin_ui = true");
    let s = Srv::with_config("unconfigured", &bare, &[]).await?;
    let http = ctx.fresh();
    let page = http.get(&format!("{}/enter", s.base), &[]).await?;
    let post = ctx
        .fresh()
        .post_form(&format!("{}/enter", s.base), &[], &[])
        .await?;
    let settings = http
        .get(
            &format!("{}/admin/settings", s.base),
            &[("cookie", &format!("farsight_admin={stored}"))],
        )
        .await?;
    let cb = ctx
        .fresh()
        .get(
            &format!("{}/enter/callback?state=x", s.base),
            &[("cookie", "farsight_flow=x")],
        )
        .await?;
    let health = http.get(&format!("{}/livez", s.base), &[]).await?;
    c.check(
        "unconfigured (no admin DID): the process runs; /enter says sign-in is not configured and how to set it, with no button; POST /enter is 400 and starts nothing; /enter/callback is 400; no session is accepted, a stored one included",
        health.status == 200
            && page.status == 200
            && page.text.contains("id=\"unconfigured\"")
            && page.text.contains("FARSIGHT__ACCESS__ADMIN_DID")
            && !page.text.contains("type=\"submit\"")
            && !page.text.contains("<form")
            && post.status == 400
            && post.header("location").is_none()
            && cb.status == 400
            && settings.status == 303
            && s.log().contains("admin sign-in is not configured"),
        format!("{} {} {} {}", page.status, post.status, cb.status, settings.status),
    );
    ctx.retire(s);
    Ok(())
}

// --------------------------------------------------------------- 12. CLI

async fn check_cli(c: &mut Checks, ctx: &Ctx, a: Srv, cookie: &str) -> Result<(), String> {
    c.section("12. the CLI, and a changed admin DID");
    let dir = a.dir.clone();
    let before = std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())?;
    let (ok, out) = cli(&dir, &["admin-did"], &[]);
    let (ok_env, out_env) = cli(&dir, &["admin-did"], &[("FARSIGHT__ACCESS__ADMIN_DID", D2)]);
    c.check(
        "farsight admin-did prints the DID and its source: the file, or the environment when the variable is set",
        ok && out.starts_with(D1)
            && out.contains("config.toml")
            && ok_env
            && out_env.starts_with(D2)
            && out_env.contains("from the environment"),
        format!("{} / {}", out.trim().replace('\n', " "), out_env.trim().replace('\n', " ")),
    );
    let mut refused = Vec::new();
    for (what, args, env, needle) in [
        (
            "a handle",
            vec!["set-admin-did", "alice.example"],
            vec![],
            "is not an admin DID",
        ),
        (
            "an unresolvable DID",
            vec!["set-admin-did", UNKNOWN],
            vec![],
            "could not be resolved",
        ),
        (
            "the variable set",
            vec!["set-admin-did", D2],
            vec![("FARSIGHT__ACCESS__ADMIN_DID", D1)],
            "overrides the file",
        ),
        ("no argument", vec!["set-admin-did"], vec![], "usage"),
    ] {
        let (ok, out) = cli(&dir, &args, &env);
        if ok || !out.contains(needle) {
            refused.push(format!("{what}: {ok} {}", support::truncate(&out, 120)));
        }
    }
    let empty = Srv::dir("noconfig")?;
    let (ok_none, out_none) = cli(&empty, &["set-admin-did", D2], &[]);
    c.check(
        "set-admin-did refuses, changing nothing: a handle; a DID that does not resolve (without --force); the admin DID set by environment variable; no config file (it names the variable to use)",
        refused.is_empty()
            && !ok_none
            && out_none.contains("FARSIGHT__ACCESS__ADMIN_DID")
            && std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())? == before,
        format!("{refused:?} / {}", support::truncate(&out_none, 120)),
    );
    let (ok, out) = cli(&dir, &["set-admin-did", D2], &[]);
    let after = std::fs::read_to_string(a.config_path()).map_err(|e| e.to_string())?;
    let http = ctx.fresh();
    let still = http
        .get(&format!("{}/admin/settings", a.base), &[("cookie", cookie)])
        .await?;
    c.check(
        "set-admin-did <did> resolves the DID, writes it to config.toml, prints it with the handle line and \"Restart farsight to apply\"; it does not restart anything — the running server still serves the old DID's session",
        ok && out.contains(&format!("Admin DID set to {D2}"))
            && out.contains("Handle: none verified")
            && out.contains("Restart farsight to apply")
            && after.contains(&format!("admin_did = \"{D2}\""))
            && !after.contains(D1)
            && still.status == 200,
        support::truncate(&out.replace('\n', " "), 200),
    );
    let old_key =
        farsight_web::pages::oauth_session_key(cookie.split_once('=').map_or("", |x| x.1), D1);
    let row_before = ctx.has_session_key(&old_key).await?;
    ctx.retire(a);
    let a = Srv::launch(&dir, &[]).await?;
    let http = ctx.fresh();
    let dead = http
        .get(&format!("{}/admin/settings", a.base), &[("cookie", cookie)])
        .await?;
    c.check(
        "after the restart the old DID's session no longer authenticates (303 to /enter), although its row is still in the table: no revocation step was needed",
        dead.status == 303 && row_before && ctx.has_session_key(&old_key).await?,
        format!("{}", dead.status),
    );
    ctx.standin.take_events();
    let signed = ctx.sign_in(&a, &http, &a.loopback()).await?;
    let hint = ctx
        .standin
        .take_events()
        .iter()
        .rev()
        .find(|e| e.kind == "par")
        .and_then(|e| e.form.get("login_hint").cloned());
    let new_cookie = set_cookie(&signed, "farsight_admin").unwrap_or_default();
    let works = http
        .get(
            &format!("{}/admin/settings", a.base),
            &[("cookie", &new_cookie)],
        )
        .await?;
    ctx.standin.set(Knobs {
        sub: Some(D1.into()),
        ..Knobs::default()
    });
    let old_admin = ctx.sign_in(&a, &ctx.fresh(), &a.loopback()).await?;
    ctx.standin.reset();
    c.check(
        "sign-in is now as the new DID (login_hint, session, Settings shows it); the previous admin completing a flow is refused (403)",
        signed.status == 200
            && hint.as_deref() == Some(D2)
            && works.text.contains(&format!("Sign-in is as <code>{D2}</code>"))
            && old_admin.status == 403,
        format!("{} {:?} {}", signed.status, hint, old_admin.status),
    );
    let (ok, out) = cli(&dir, &["set-admin-did", UNKNOWN, "--force"], &[]);
    c.check(
        "set-admin-did --force sets a DID that does not resolve, with a warning",
        ok && out.contains("could not be resolved")
            && out.contains(&format!("Admin DID set to {UNKNOWN}"))
            && std::fs::read_to_string(a.config_path())
                .map_err(|e| e.to_string())?
                .contains(UNKNOWN),
        support::truncate(&out.replace('\n', " "), 160),
    );
    // Repairing a file that names no admin at all.
    let bare_dir = Srv::dir("repair")?;
    write_private(
        &bare_dir.join("config.toml"),
        &ctx.config(HOSTNAME, "admin_ui = true"),
    )?;
    let (ok, _) = cli(&bare_dir, &["set-admin-did", D1], &[]);
    let repaired = std::fs::read_to_string(bare_dir.join("config.toml")).unwrap_or_default();
    c.check(
        "set-admin-did also sets the DID in a file that has none (the unconfigured state's repair)",
        ok && config::load_from_parts(Some(&repaired), &[])
            .is_ok_and(|l| l.admin_auth() == AdminAuth::Configured(D1.into())),
        "",
    );
    ctx.retire(a);
    Ok(())
}

// ----------------------------------------------------------- 13. logging

fn check_logging(c: &mut Checks, ctx: &Ctx) {
    c.section("13. what is logged, and what never is");
    let logs = ctx.logs.lock().unwrap_or_else(|e| e.into_inner());
    let all = logs.join("\n");
    let signed: Vec<&str> = all
        .lines()
        .filter(|l| l.contains("admin signed in"))
        .collect();
    c.check(
        "every successful sign-in is logged at INFO with the DID and the address",
        !signed.is_empty()
            && signed.iter().all(|l| {
                l.contains("\"INFO\"") && l.contains("did:plc:") && l.contains("\"ip\":\"127.")
            }),
        format!(
            "{} lines, e.g. {}",
            signed.len(),
            support::truncate(signed.first().copied().unwrap_or_default(), 200)
        ),
    );
    let refusals = all
        .lines()
        .filter(|l| {
            l.contains("sign-in callback")
                || l.contains("was not completed at the authorization server")
        })
        .collect::<Vec<_>>();
    c.check(
        "ordinary callback refusals (no cookie, unknown flow, wrong cookie, wrong issuer, denied) are DEBUG lines",
        !refusals.is_empty() && refusals.iter().all(|l| l.contains("\"DEBUG\"")),
        format!("{} lines", refusals.len()),
    );
    let mut secrets = ctx
        .secrets
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    secrets.extend(ctx.standin.issued());
    secrets.sort();
    secrets.dedup();
    let leaked: Vec<String> = secrets
        .iter()
        .filter(|s| s.len() >= 12 && all.contains(s.as_str()))
        .map(|s| support::truncate(s, 8))
        .collect();
    c.check(
        "no state, code, PKCE verifier, access token, flow cookie or session cookie value seen in this run appears in any server log (debug level on); nor does the word of any DPoP private key field",
        leaked.is_empty() && secrets.len() > 40 && !all.contains("\"d\":"),
        format!("{} secrets checked across {} logs; leaked: {leaked:?}", secrets.len(), logs.len()),
    );
}

// ----------------------------------------------------------- 14. browser

async fn check_browser(c: &mut Checks, ctx: &Ctx, cfg: &str) -> Result<(), String> {
    c.section("14. the browser: the session cookie after the callback page");
    let dir = std::env::temp_dir().join("farsight-stage7-browser");
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    std::fs::write(dir.join("probes.mjs"), BROWSER_SCRIPT).map_err(|e| e.to_string())?;
    let mut n = 0;
    // One configuration: the admin UI is on or it is not there.
    {
        // The sign-in stops being fresh after 12 s on this server, so the
        // probes can watch it happen.
        let s =
            Srv::with_config("browser", cfg, &[("FARSIGHT_HARNESS_STEP_UP_SECS", "12")]).await?;
        let script = format!(
            "cd /work && ([ -d node_modules/playwright ] || npm install --no-save --no-audit --no-fund playwright@1.48.0 >npm.log 2>&1) && node probes.mjs '{}' '{}'",
            s.base, ctx.standin.base
        );
        let out = Command::new("docker")
            .args(["run", "--rm", "--network", "host", "-v"])
            .arg(format!("{}:/work", dir.display()))
            .args([BROWSER_IMAGE, "sh", "-c", &script])
            .output()
            .map_err(|e| format!("docker: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        for line in stdout.lines() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let (Some(what), Some(ok)) = (v["what"].as_str(), v["ok"].as_bool()) else {
                continue;
            };
            n += 1;
            c.check(what, ok, v["detail"].as_str().unwrap_or(""));
        }
        if !out.status.success() {
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
        ctx.retire(s);
    }
    if n == 0 {
        c.check("the browser probes reported results", false, "none");
    }
    Ok(())
}

// ------------------------------------------------------------------ main

struct Bridge(String);

impl Bridge {
    /// A bridge network that puts [`STANDIN_ADDR`] on this host.
    fn create() -> Result<Bridge, String> {
        let name = format!("farsight-stage7-{}", std::process::id());
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

async fn run(c: &mut Checks, pg: &Pg, browser: bool) -> Result<(), String> {
    pg.create_db("s7").await?;
    let dsn = pg.url("s7");
    let standin = Standin::start(STANDIN_ADDR).await?;
    println!("   stand-in authorization server at {}", standin.base);
    // The first server runs the migrations; the pool comes after.
    let ctx_dsn = dsn.clone();
    let cfg_a = config_toml(
        &ctx_dsn,
        HOSTNAME,
        &standin.base,
        &format!("admin_ui = true\nadmin_did = \"{D1}\""),
    );
    let a = Srv::with_config("a", &cfg_a, &[]).await?;
    let ctx = Ctx {
        standin,
        web: Http::new(None),
        pool: pg.pool("s7", 4).await?,
        dsn,
        next_ip: AtomicU32::new(0),
        last_start: Mutex::new(None),
        secrets: Mutex::new(Vec::new()),
        logs: Mutex::new(Vec::new()),
    };
    check_config(c, &ctx)?;
    check_process_start(c, &ctx).await?;
    check_modes(c, &ctx, &a).await?;
    let admin_cookie = check_loopback_flow(c, &ctx, &a).await?;
    check_hosted_flow(c, &ctx, &a).await?;
    check_nonces(c, &ctx, &a).await?;
    check_callback(c, &ctx, &a).await?;
    check_admin_card(c, &ctx, &a, &admin_cookie).await?;
    check_settings(c, &ctx, &a, &admin_cookie).await?;
    // The CLI section restarts A on its directory and retires it.
    check_cli(c, &ctx, a, &admin_cookie).await?;
    check_step_up(c, &ctx, &cfg_a).await?;
    check_unhostable(c, &ctx).await?;
    check_local_only(c, &ctx).await?;
    check_lifetime(c, &ctx, &cfg_a).await?;
    check_gates(c, &ctx).await?;
    check_rates(c, &ctx, &cfg_a).await?;
    check_unconfigured(c, &ctx).await?;
    if browser {
        check_browser(c, &ctx, &cfg_a).await?;
    }
    check_logging(c, &ctx);
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |f: &str| args.iter().any(|a| a == f);
    println!("== farsight stage-7 harness: admin sign-in");
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
        pg.wait_ready(Duration::from_secs(60)).await?;
        run(&mut c, &pg, flag("--browser")).await
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
