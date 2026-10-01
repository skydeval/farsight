//! Harness plumbing: check recorder, the Postgres container and child
//! processes (`farsight`, `farsight-backfill`).

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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
            "farsight-stage4-{}-{}",
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
            "farsight.stage4-harness=1",
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

// -------------------------------------------------------------- processes

/// A child process of a binary next to this harness, logging to a file.
pub struct Proc {
    pub dir: PathBuf,
    name: String,
    child: Child,
}

/// A binary next to this harness binary.
pub fn sibling(name: &str) -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join(name)))
        .unwrap_or_else(|| PathBuf::from(name))
}

impl Proc {
    /// Starts `bin` with `FARSIGHT_CONFIG` pointing at `dir/config.toml`.
    pub fn start(bin: &str, dir: &std::path::Path, env: &[(&str, &str)]) -> Result<Proc, String> {
        let log =
            std::fs::File::create(dir.join(format!("{bin}.log"))).map_err(|e| e.to_string())?;
        let log2 = log.try_clone().map_err(|e| e.to_string())?;
        let mut cmd = Command::new(sibling(bin));
        cmd.env("FARSIGHT_CONFIG", dir.join("config.toml"))
            .env("RUST_LOG", "info,sqlx=warn")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log2));
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().map_err(|e| format!("spawning {bin}: {e}"))?;
        Ok(Proc {
            dir: dir.to_owned(),
            name: bin.to_owned(),
            child,
        })
    }

    pub fn log_tail(&self, n: usize) -> String {
        let text = std::fs::read_to_string(self.dir.join(format!("{}.log", self.name)))
            .unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(n)..].join("\n")
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join(format!("{}.log", self.name))).unwrap_or_default()
    }

    pub fn running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.stop();
    }
}
