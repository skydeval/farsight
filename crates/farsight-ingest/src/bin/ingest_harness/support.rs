//! Harness plumbing: Postgres container, check recorder, metrics scraping.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ------------------------------------------------------------------ checks

/// Outcome of one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
    /// The run could not establish what the check claims.
    Unverified,
}

#[derive(Debug, Clone)]
pub struct Check {
    pub verdict: Verdict,
    pub what: String,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct Checks {
    pub items: Vec<Check>,
}

impl Checks {
    pub fn check(&mut self, what: impl Into<String>, ok: bool, detail: impl Into<String>) -> bool {
        let c = Check {
            verdict: if ok { Verdict::Pass } else { Verdict::Fail },
            what: what.into(),
            detail: detail.into(),
        };
        println!("  [{}] {} — {}", tag(c.verdict), c.what, c.detail);
        self.items.push(c);
        ok
    }

    pub fn unverified(&mut self, what: impl Into<String>, detail: impl Into<String>) {
        let c = Check {
            verdict: Verdict::Unverified,
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

pub fn tag(v: Verdict) -> &'static str {
    match v {
        Verdict::Pass => "PASS",
        Verdict::Fail => "FAIL",
        Verdict::Unverified => "UNVERIFIED",
    }
}

// --------------------------------------------------------------- postgres

fn run(cmd: &mut Command) -> Result<String, String> {
    let out = cmd.output().map_err(|e| format!("{cmd:?}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "{cmd:?} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// A throwaway Postgres 16 container (or an existing server).
pub struct Pg {
    pub url: String,
    container: Option<(String, String)>,
    keep: bool,
}

impl Pg {
    pub fn start_docker(keep: bool) -> Result<Pg, String> {
        let tag = format!("{}-{}", std::process::id(), chrono::Utc::now().timestamp());
        let name = format!("farsight-stage2-{tag}");
        let volume = format!("farsight-stage2-{tag}");
        // A fixed host port: `docker restart` re-assigns ephemeral ports,
        // which would strand every pool after the restart injection.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map_err(|e| e.to_string())?
            .port();
        run(Command::new("docker").args([
            "run",
            "-d",
            "--name",
            &name,
            "--label",
            "farsight.stage2-harness=1",
            // The whole-run model comparison runs parallel hash joins that
            // outgrow Docker's 64 MB /dev/shm default.
            "--shm-size=1g",
            "-e",
            "POSTGRES_PASSWORD=harness",
            "-e",
            "POSTGRES_USER=harness",
            "-e",
            "POSTGRES_DB=farsight",
            "-p",
            &format!("127.0.0.1:{port}:5432"),
            "-v",
            &format!("{volume}:/var/lib/postgresql/data"),
            "postgres:16",
        ]))?;
        Ok(Pg {
            url: format!("postgres://harness:harness@127.0.0.1:{port}/farsight"),
            container: Some((name, volume)),
            keep,
        })
    }

    pub fn existing(url: &str) -> Pg {
        Pg {
            url: url.to_owned(),
            container: None,
            keep: true,
        }
    }

    pub fn can_restart(&self) -> bool {
        self.container.is_some()
    }

    /// `docker restart` (fault injection: Postgres restart mid-batch).
    pub fn restart(&self) -> Result<(), String> {
        let Some((name, _)) = &self.container else {
            return Err("not a harness-owned container".into());
        };
        run(Command::new("docker").args(["restart", "-t", "1", name])).map(|_| ())
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<(), String> {
        let start = Instant::now();
        let mut ok = 0;
        let mut last = String::new();
        while start.elapsed() < timeout {
            match PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect(&self.url)
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

    pub async fn pool(&self, n: u32) -> Result<PgPool, sqlx::Error> {
        PgPoolOptions::new()
            .max_connections(n)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&self.url)
            .await
    }

    pub fn stop(&self) {
        let Some((name, volume)) = &self.container else {
            return;
        };
        if self.keep {
            println!("keeping container {name} (volume {volume})");
            return;
        }
        if let Err(e) = run(Command::new("docker").args(["rm", "-f", "-v", name])) {
            eprintln!("warning: {e}");
        }
        if let Err(e) = run(Command::new("docker").args(["volume", "rm", "-f", volume])) {
            eprintln!("warning: {e}");
        }
    }
}

// ----------------------------------------------------------------- metrics

/// One scrape: series (name + labels, as printed) → value.
pub type Scrape = BTreeMap<String, f64>;

/// `GET http://addr/metrics` without an HTTP client dependency.
pub async fn scrape(addr: &str) -> Result<Scrape, String> {
    let mut s = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| e.to_string())?;
    s.write_all(format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut body = String::new();
    s.read_to_string(&mut body)
        .await
        .map_err(|e| e.to_string())?;
    let text = body.split("\r\n\r\n").nth(1).unwrap_or("");
    let mut out = Scrape::new();
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        if let Some((series, value)) = line.rsplit_once(' ')
            && let Ok(v) = value.parse::<f64>()
        {
            out.insert(series.to_owned(), v);
        }
    }
    Ok(out)
}

/// The metric name of a series (`name{…}` → `name`).
pub fn series_name(series: &str) -> &str {
    series.split('{').next().unwrap_or(series)
}

/// Sum of all series of `name` (all label sets), or `None` if absent.
pub fn sum_of(s: &Scrape, name: &str) -> Option<f64> {
    let v: Vec<f64> = s
        .iter()
        .filter(|(k, _)| series_name(k) == name)
        .map(|(_, v)| *v)
        .collect();
    if v.is_empty() {
        None
    } else {
        Some(v.iter().sum())
    }
}
