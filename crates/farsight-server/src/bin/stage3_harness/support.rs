//! Harness plumbing: check recorder, the Postgres container, `farsight`
//! server processes and an HTTP client.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use reqwest::header::HeaderMap;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

// ------------------------------------------------------------------ checks

/// Outcome of one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
    Unverified,
}

#[derive(Debug, Clone)]
pub struct Check {
    pub verdict: Verdict,
    #[allow(dead_code)]
    pub section: String,
    pub what: String,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct Checks {
    pub items: Vec<Check>,
    pub section: String,
}

pub fn tag(v: Verdict) -> &'static str {
    match v {
        Verdict::Pass => "PASS",
        Verdict::Fail => "FAIL",
        Verdict::Unverified => "UNVERIFIED",
    }
}

impl Checks {
    pub fn section(&mut self, s: &str) {
        self.section = s.to_owned();
        println!("== {s}");
    }

    pub fn check(&mut self, what: impl Into<String>, ok: bool, detail: impl Into<String>) -> bool {
        let c = Check {
            verdict: if ok { Verdict::Pass } else { Verdict::Fail },
            section: self.section.clone(),
            what: what.into(),
            detail: detail.into(),
        };
        let d = truncate(&c.detail, 600);
        println!("  [{}] {} — {}", tag(c.verdict), c.what, d);
        self.items.push(c);
        ok
    }

    #[allow(dead_code)]
    pub fn unverified(&mut self, what: impl Into<String>, detail: impl Into<String>) {
        let c = Check {
            verdict: Verdict::Unverified,
            section: self.section.clone(),
            what: what.into(),
            detail: detail.into(),
        };
        println!("  [{}] {} — {}", tag(c.verdict), c.what, c.detail);
        self.items.push(c);
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        let n = |v| self.items.iter().filter(|c| c.verdict == v).count();
        (n(Verdict::Pass), n(Verdict::Fail), n(Verdict::Unverified))
    }
}

/// At most `n` bytes of `s`, cut at a character boundary.
pub fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_owned();
    }
    let mut i = n;
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    format!("{}…", &s[..i])
}

// --------------------------------------------------------------- postgres

pub fn run(cmd: &mut Command) -> Result<String, String> {
    let out = cmd.output().map_err(|e| format!("{cmd:?}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "{cmd:?} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// A throwaway Postgres 16 container.
pub struct Pg {
    pub port: u16,
    name: String,
    keep: bool,
}

pub fn free_port() -> Result<u16, String> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .map_err(|e| e.to_string())
}

impl Pg {
    pub fn start(keep: bool) -> Result<Pg, String> {
        let name = format!(
            "farsight-stage3-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp()
        );
        let port = free_port()?;
        run(Command::new("docker").args([
            "run",
            "-d",
            "--name",
            &name,
            "--label",
            "farsight.stage3-harness=1",
            "--shm-size=1g",
            "-e",
            "POSTGRES_PASSWORD=harness",
            "-e",
            "POSTGRES_USER=harness",
            "-e",
            "POSTGRES_DB=postgres",
            "-p",
            &format!("127.0.0.1:{port}:5432"),
            "postgres:16",
        ]))?;
        Ok(Pg { port, name, keep })
    }

    pub fn url(&self, db: &str) -> String {
        format!("postgres://harness:harness@127.0.0.1:{}/{db}", self.port)
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<(), String> {
        let start = Instant::now();
        let mut ok = 0;
        let mut last = String::new();
        while start.elapsed() < timeout {
            match PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect(&self.url("postgres"))
                .await
            {
                Ok(p) => match sqlx::query("SELECT 1").execute(&p).await {
                    Ok(_) => {
                        ok += 1;
                        if ok >= 2 {
                            return Ok(());
                        }
                    }
                    Err(e) => last = e.to_string(),
                },
                Err(e) => {
                    ok = 0;
                    last = e.to_string();
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(format!("postgres not ready after {timeout:?}: {last}"))
    }

    pub async fn create_db(&self, db: &str) -> Result<(), String> {
        let p = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.url("postgres"))
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&p)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn pool(&self, db: &str, n: u32) -> Result<PgPool, String> {
        PgPoolOptions::new()
            .max_connections(n)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&self.url(db))
            .await
            .map_err(|e| e.to_string())
    }

    pub fn stop(&self) {
        if self.keep {
            println!("keeping container {}", self.name);
            return;
        }
        if let Err(e) = run(Command::new("docker").args(["rm", "-f", "-v", &self.name])) {
            eprintln!("warning: {e}");
        }
    }
}

// ----------------------------------------------------------------- server

/// A `farsight` child process with its own config directory.
pub struct Server {
    pub base: String,
    pub dir: PathBuf,
    child: Child,
}

/// The `farsight` binary next to this harness binary.
pub fn farsight_bin() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("farsight")))
        .unwrap_or_else(|| PathBuf::from("farsight"))
}

impl Server {
    /// Starts `farsight` with `config.toml` written into a fresh directory
    /// (or no config: setup mode), listening on `bind`.
    pub fn start(
        name: &str,
        config: Option<&str>,
        bind: &str,
        extra_env: &[(&str, &str)],
    ) -> Result<Server, String> {
        let dir =
            std::env::temp_dir().join(format!("farsight-stage3-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let cfg = dir.join("config.toml");
        if let Some(text) = config {
            std::fs::write(&cfg, text).map_err(|e| e.to_string())?;
        }
        let log = std::fs::File::create(dir.join("server.log")).map_err(|e| e.to_string())?;
        let log2 = log.try_clone().map_err(|e| e.to_string())?;
        let mut cmd = Command::new(farsight_bin());
        cmd.env("FARSIGHT_CONFIG", &cfg)
            .env("FARSIGHT__SERVER__BIND", bind)
            .env("FARSIGHT_SETUP_BIND", bind)
            .env("RUST_LOG", "info,sqlx=warn")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log2));
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().map_err(|e| format!("spawning farsight: {e}"))?;
        Ok(Server {
            base: format!("http://{bind}"),
            dir,
            child,
        })
    }

    pub fn config_path(&self) -> PathBuf {
        self.dir.join("config.toml")
    }

    pub fn token_path(&self) -> PathBuf {
        self.dir.join(".setup-token")
    }

    pub fn log_tail(&self, n: usize) -> String {
        let text = std::fs::read_to_string(self.dir.join("server.log")).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(n)..].join("\n")
    }

    pub async fn wait_live(&self, http: &Http, timeout: Duration) -> Result<(), String> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Ok(r) = http.get(&format!("{}/livez", self.base), &[]).await
                && r.status == 200
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        Err(format!(
            "server at {} not live after {timeout:?}; log:\n{}",
            self.base,
            self.log_tail(20)
        ))
    }

    pub fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

// ------------------------------------------------------------------- http

/// A response.
#[derive(Debug, Clone)]
pub struct Resp {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Value,
    pub text: String,
    pub elapsed: Duration,
}

impl Resp {
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    }

    pub fn error_name(&self) -> Option<&str> {
        self.body.get("error").and_then(Value::as_str)
    }

    pub fn short(&self) -> String {
        format!("HTTP {} {}", self.status, truncate(&self.text, 300))
    }
}

/// An HTTP client bound to one local source address.
#[derive(Clone)]
pub struct Http {
    client: reqwest::Client,
}

impl Http {
    pub fn new(local: Option<std::net::IpAddr>) -> Http {
        let mut b = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(ip) = local {
            b = b.local_address(ip);
        }
        Http {
            client: b.build().expect("client"),
        }
    }

    async fn finish(rb: reqwest::RequestBuilder) -> Result<Resp, String> {
        let start = Instant::now();
        let r = rb.send().await.map_err(|e| e.to_string())?;
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        let text = r.text().await.map_err(|e| e.to_string())?;
        let body = serde_json::from_str(&text).unwrap_or(Value::Null);
        Ok(Resp {
            status,
            headers,
            body,
            text,
            elapsed: start.elapsed(),
        })
    }

    pub async fn get(&self, url: &str, headers: &[(&str, &str)]) -> Result<Resp, String> {
        let mut rb = self.client.get(url);
        for (k, v) in headers {
            rb = rb.header(*k, *v);
        }
        Http::finish(rb).await
    }

    pub async fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Resp, String> {
        let mut rb = self.client.post(url).json(body);
        for (k, v) in headers {
            rb = rb.header(*k, *v);
        }
        Http::finish(rb).await
    }

    pub async fn post_form(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        form: &[(&str, &str)],
    ) -> Result<Resp, String> {
        let mut rb = self.client.post(url).form(form);
        for (k, v) in headers {
            rb = rb.header(*k, *v);
        }
        Http::finish(rb).await
    }
}

/// The admin DID the harness configs name. Nothing resolves it: harness
/// sessions are created in the database, not by signing in.
pub const ADMIN_DID: &str = "did:plc:harnessadminaaaaaaaaaaaa";

/// Creates an admin session for `did` the way a completed sign-in does
/// (the row is keyed by the cookie value and the DID) and returns the
/// `Cookie` header value for it.
pub async fn admin_session(pool: &PgPool, did: &str) -> Result<String, String> {
    let raw = farsight_web::common::random_id();
    farsight_storage::auth::create_session(
        pool,
        &farsight_web::pages::oauth_session_key(&raw, did),
        &farsight_api::auth::random_bytes::<32>(),
        None,
        Some("farsight-harness"),
    )
    .await
    .map_err(|e| format!("creating a session: {e}"))?;
    Ok(format!("farsight_admin={raw}"))
}

/// The `name=value` of a `Set-Cookie` header.
pub fn set_cookie(r: &Resp, name: &str) -> Option<String> {
    r.headers
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find(|v| v.starts_with(&format!("{name}=")))
        .and_then(|v| v.split(';').next())
        .map(str::to_owned)
}

/// The value of `name="csrf" value="…"` in an HTML page.
pub fn csrf_of(html: &str) -> Option<String> {
    let i = html.find("name=\"csrf\" value=\"")? + "name=\"csrf\" value=\"".len();
    let rest = &html[i..];
    Some(rest[..rest.find('"')?].to_owned())
}

/// Percent-encodes a query value.
pub fn enc(s: &str) -> String {
    url_encode(s)
}

fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
