//! Outbound requests (see `docs/design/backfill.md` and
//! `docs/design/security.md`): every request to a network-learned address
//! goes through the safe client, a per-host token bucket with a
//! concurrency limit, `429` / `RateLimit-Remaining: 0` / `Retry-After`
//! handling and a circuit breaker; the PLC directory has its own limiter
//! with half reserved for the resolver.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use farsight_core::net::{OutboundClient, OutboundError, OutboundResponse, SafeClient};
use serde_json::Value;
use tokio::sync::Notify;
use url::Url;

use crate::metrics as m;

/// Why a request did not produce a usable response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetError {
    /// The host is cooling down (429, breaker); retry after `until`.
    #[error("host {host} cooling down for {secs} s")]
    Cooling {
        /// The host.
        host: String,
        /// Seconds left.
        secs: u64,
    },
    /// An XRPC error response (`{"error": name}`) or another HTTP status.
    #[error("HTTP {status} {name}")]
    Http {
        /// Status.
        status: u16,
        /// XRPC error name (`RepoNotFound`, …) or empty.
        name: String,
    },
    /// Refused by the safe client, a transport failure, a timeout or an
    /// oversized body.
    #[error("{0}")]
    Transport(String),
    /// The body was not the expected JSON.
    #[error("bad response: {0}")]
    Decode(String),
}

impl NetError {
    /// The XRPC error name, if this is one.
    pub fn xrpc_name(&self) -> Option<&str> {
        match self {
            NetError::Http { name, .. } if !name.is_empty() => Some(name),
            _ => None,
        }
    }

    /// The metric `kind` label.
    pub fn kind(&self) -> &'static str {
        match self {
            NetError::Cooling { .. } => "cooling",
            NetError::Http { status, .. } if *status == 429 => "rate_limited",
            NetError::Http { status, .. } if *status >= 500 => "server",
            NetError::Http { .. } => "client",
            NetError::Transport(_) => "transport",
            NetError::Decode(_) => "decode",
        }
    }
}

/// The outbound client: the production safe client, or (harness builds
/// only) a plain client for loopback fake services.
#[derive(Clone)]
pub enum Client {
    /// The safe client.
    Safe(SafeClient),
    /// Plain HTTP for the harness's loopback fakes.
    #[cfg(feature = "harness")]
    Plain(reqwest::Client),
}

impl Client {
    async fn get(&self, url: &Url) -> Result<OutboundResponse, OutboundError> {
        match self {
            Client::Safe(c) => c.get(url).await,
            #[cfg(feature = "harness")]
            Client::Plain(c) => {
                let r = c
                    .get(url.clone())
                    .send()
                    .await
                    .map_err(|e| OutboundError::Transport(e.to_string()))?;
                let status = r.status().as_u16();
                let headers = r
                    .headers()
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().to_ascii_lowercase(),
                            String::from_utf8_lossy(v.as_bytes()).into_owned(),
                        )
                    })
                    .collect();
                let body = r
                    .bytes()
                    .await
                    .map_err(|e| OutboundError::Transport(e.to_string()))?
                    .to_vec();
                Ok(OutboundResponse {
                    status,
                    headers,
                    body,
                    final_url: url.clone(),
                })
            }
        }
    }
}

#[derive(Debug)]
struct HostState {
    tokens: f64,
    last: Instant,
    inflight: u32,
    cooldown_until: Option<Instant>,
    consecutive_failures: u32,
    trips: u32,
    last_trip: Option<Instant>,
}

/// Circuit-breaker threshold: consecutive failures that trip it.
pub const BREAKER_FAILURES: u32 = 5;
/// First trip: 1 minute.
pub const BREAKER_FIRST: Duration = Duration::from_secs(60);
/// Repeat trip (within an hour of the last): 1 hour.
pub const BREAKER_REPEAT: Duration = Duration::from_secs(3600);
/// Cooldown after a 429 without `Retry-After`.
pub const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(60);
/// Longest a request waits for a per-host slot before giving up.
pub const MAX_SLOT_WAIT: Duration = Duration::from_secs(120);

/// Per-host limits: token bucket, concurrency, cooldown, breaker.
#[derive(Debug)]
pub struct HostLimiter {
    rps: f64,
    concurrency: u32,
    hosts: Mutex<HashMap<String, HostState>>,
    freed: Notify,
}

impl HostLimiter {
    /// A limiter with `rps` and `concurrency` per host.
    pub fn new(rps: u32, concurrency: u32) -> HostLimiter {
        HostLimiter {
            rps: f64::from(rps.max(1)),
            concurrency: concurrency.max(1),
            hosts: Mutex::new(HashMap::new()),
            freed: Notify::new(),
        }
    }

    fn state<'a>(
        map: &'a mut HashMap<String, HostState>,
        host: &str,
        burst: f64,
    ) -> &'a mut HostState {
        map.entry(host.to_owned()).or_insert(HostState {
            tokens: burst,
            last: Instant::now(),
            inflight: 0,
            cooldown_until: None,
            consecutive_failures: 0,
            trips: 0,
            last_trip: None,
        })
    }

    /// Whether a request to `host` could start now (blocked-head skip of
    /// the list-job lanes).
    pub fn has_capacity(&self, host: &str) -> bool {
        let mut map = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        let burst = self.rps;
        let s = Self::state(&mut map, host, burst);
        let now = Instant::now();
        if s.cooldown_until.is_some_and(|u| u > now) {
            return false;
        }
        s.inflight < self.concurrency
    }

    /// Seconds `host` is still cooling down, if it is.
    pub fn cooling(&self, host: &str) -> Option<u64> {
        let map = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        map.get(host)
            .and_then(|s| s.cooldown_until)
            .filter(|u| *u > now)
            .map(|u| u.duration_since(now).as_secs().max(1))
    }

    /// Waits for a slot on `host` (bucket token + concurrency).
    pub async fn acquire(&self, host: &str) -> Result<(), NetError> {
        let start = Instant::now();
        loop {
            let wait = {
                let mut map = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
                let (rps, conc) = (self.rps, self.concurrency);
                let s = Self::state(&mut map, host, rps);
                let now = Instant::now();
                if let Some(u) = s.cooldown_until.filter(|u| *u > now) {
                    return Err(NetError::Cooling {
                        host: host.to_owned(),
                        secs: u.duration_since(now).as_secs().max(1),
                    });
                }
                let el = now.saturating_duration_since(s.last).as_secs_f64();
                s.tokens = (s.tokens + el * rps).min(rps);
                s.last = now;
                if s.inflight < conc && s.tokens >= 1.0 {
                    s.tokens -= 1.0;
                    s.inflight += 1;
                    return Ok(());
                }
                if s.tokens < 1.0 {
                    Duration::from_secs_f64(((1.0 - s.tokens) / rps).max(0.005))
                } else {
                    Duration::from_millis(250)
                }
            };
            if start.elapsed() > MAX_SLOT_WAIT {
                return Err(NetError::Cooling {
                    host: host.to_owned(),
                    secs: 1,
                });
            }
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.freed.notified() => {}
            }
        }
    }

    /// Releases a slot and records the result for the breaker.
    pub fn release(&self, host: &str, outcome: Result<(), Option<Duration>>) {
        let mut map = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        let rps = self.rps;
        let s = Self::state(&mut map, host, rps);
        s.inflight = s.inflight.saturating_sub(1);
        let now = Instant::now();
        match outcome {
            Ok(()) => s.consecutive_failures = 0,
            Err(Some(retry_after)) => {
                // 429 / RateLimit-Remaining: 0: honour Retry-After.
                s.cooldown_until = Some(now + retry_after);
            }
            Err(None) => {
                s.consecutive_failures += 1;
                if s.consecutive_failures >= BREAKER_FAILURES {
                    let repeat = s
                        .last_trip
                        .is_some_and(|t| now.duration_since(t) < BREAKER_REPEAT);
                    let d = if repeat {
                        BREAKER_REPEAT
                    } else {
                        BREAKER_FIRST
                    };
                    s.cooldown_until = Some(now + d);
                    s.trips += 1;
                    s.last_trip = Some(now);
                    s.consecutive_failures = 0;
                    tracing::warn!(
                        host,
                        cooldown_s = d.as_secs(),
                        "host circuit breaker tripped"
                    );
                }
            }
        }
        drop(map);
        self.freed.notify_waiters();
    }
}

/// The PLC directory limiter: `plc_rps` in total, half of it reserved
/// for the resolver.
#[derive(Debug)]
pub struct PlcLimiter {
    reserved: Mutex<(f64, Instant)>,
    shared: Mutex<(f64, Instant)>,
    half: f64,
}

/// Who is calling the PLC directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlcUse {
    /// DID resolution: may use the reserved half and the shared half.
    Resolver,
    /// Export enumeration: the shared half only.
    Export,
}

impl PlcLimiter {
    /// A limiter for `rps` requests per second.
    pub fn new(rps: u32) -> PlcLimiter {
        let half = (f64::from(rps.max(2))) / 2.0;
        PlcLimiter {
            reserved: Mutex::new((half, Instant::now())),
            shared: Mutex::new((half, Instant::now())),
            half,
        }
    }

    fn take(bucket: &Mutex<(f64, Instant)>, rate: f64) -> Option<Duration> {
        let mut b = bucket.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        b.0 = (b.0 + now.saturating_duration_since(b.1).as_secs_f64() * rate).min(rate);
        b.1 = now;
        if b.0 >= 1.0 {
            b.0 -= 1.0;
            None
        } else {
            Some(Duration::from_secs_f64((1.0 - b.0) / rate))
        }
    }

    /// Waits for a PLC token.
    pub async fn acquire(&self, use_: PlcUse) {
        loop {
            if use_ == PlcUse::Resolver {
                match Self::take(&self.reserved, self.half) {
                    None => return,
                    Some(w) => match Self::take(&self.shared, self.half) {
                        None => return,
                        Some(w2) => tokio::time::sleep(w.min(w2)).await,
                    },
                }
            } else {
                match Self::take(&self.shared, self.half) {
                    None => return,
                    Some(w) => tokio::time::sleep(w).await,
                }
            }
        }
    }
}

/// The network layer shared by every job.
pub struct Net {
    client: Client,
    /// Per-host limits.
    pub hosts: HostLimiter,
    /// PLC directory limits.
    pub plc: PlcLimiter,
    plc_host: String,
}

/// The host part of a URL used as the limiter key (`host[:port]`).
pub fn host_key(url: &Url) -> String {
    let h = url.host_str().unwrap_or("").to_ascii_lowercase();
    match url.port() {
        Some(p) => format!("{h}:{p}"),
        None => h,
    }
}

fn retry_after(r: &OutboundResponse) -> Option<Duration> {
    let h = |name: &str| {
        r.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.trim().to_owned())
    };
    let remaining_zero = h("ratelimit-remaining").is_some_and(|v| v == "0");
    let ra = h("retry-after")
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs);
    if r.status == 429 || remaining_zero {
        Some(ra.unwrap_or(DEFAULT_RETRY_AFTER))
    } else {
        None
    }
}

/// The XRPC error name of an error body. Reference PDSes answer a repo
/// they do not host with `InvalidRequest` "Could not find repo: <did>";
/// that is `RepoNotFound` for the repo-level rules.
pub fn error_name(body: &[u8]) -> String {
    let v = serde_json::from_slice::<Value>(body).ok();
    let field = |k: &str| v.as_ref().and_then(|v| v.get(k)).and_then(Value::as_str);
    let name = field("error").unwrap_or_default();
    if name == "InvalidRequest"
        && field("message").is_some_and(|m| m.starts_with("Could not find repo"))
    {
        return "RepoNotFound".to_owned();
    }
    name.to_owned()
}

impl Net {
    /// A network layer.
    pub fn new(
        client: Client,
        per_host_rps: u32,
        per_host_concurrency: u32,
        plc_rps: u32,
        plc_url: &str,
    ) -> Net {
        let plc_host = Url::parse(plc_url)
            .map(|u| host_key(&u))
            .unwrap_or_default();
        Net {
            client,
            hosts: HostLimiter::new(per_host_rps, per_host_concurrency),
            plc: PlcLimiter::new(plc_rps),
            plc_host,
        }
    }

    /// `GET url` as JSON through the per-host limiter. `method` labels the
    /// metrics (`describeRepo`, `listRecords`, …).
    pub async fn get_json(&self, url: &Url, method: &'static str) -> Result<Value, NetError> {
        let body = self.get_body(url, method).await?;
        serde_json::from_slice(&body).map_err(|e| NetError::Decode(e.to_string()))
    }

    /// `GET url` as text (the PLC export's JSON lines).
    pub async fn get_text(&self, url: &Url) -> Result<String, NetError> {
        let body = self.get_body(url, "export").await?;
        String::from_utf8(body).map_err(|e| NetError::Decode(e.to_string()))
    }

    async fn get_body(&self, url: &Url, method: &'static str) -> Result<Vec<u8>, NetError> {
        let host = host_key(url);
        let is_plc = host == self.plc_host;
        if !is_plc {
            self.hosts.acquire(&host).await?;
        }
        let started = Instant::now();
        let r = self.client.get(url).await;
        let elapsed = started.elapsed().as_secs_f64();
        let label = m::host_label(&host);
        if is_plc {
            metrics::histogram!(m::PLC_REQUEST_SECONDS).record(elapsed);
        } else {
            metrics::histogram!(m::PDS_REQUEST_SECONDS, "host" => label.clone(), "method" => method)
                .record(elapsed);
        }
        let (result, outcome) = match r {
            Err(e) => (Err(NetError::Transport(e.to_string())), Err(None)),
            Ok(resp) => {
                let ra = retry_after(&resp);
                if resp.status == 200 {
                    let outcome = if ra.is_some() { Err(ra) } else { Ok(()) };
                    (Ok(resp.body), outcome)
                } else {
                    let name = error_name(&resp.body);
                    // 4xx answers are healthy hosts saying no; only 5xx, 429
                    // and transport errors count toward the breaker.
                    let outcome = if ra.is_some() {
                        Err(ra)
                    } else if resp.status >= 500 {
                        Err(None)
                    } else {
                        Ok(())
                    };
                    (
                        Err(NetError::Http {
                            status: resp.status,
                            name,
                        }),
                        outcome,
                    )
                }
            }
        };
        if !is_plc {
            self.hosts.release(&host, outcome);
        }
        if let Err(e) = &result {
            metrics::counter!(m::PDS_ERRORS, "host" => label, "kind" => e.kind()).increment(1);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_repo_is_repo_not_found() {
        let b = br#"{"error":"InvalidRequest","message":"Could not find repo: did:plc:uraielgolztbeqrrv5c7qbce"}"#;
        assert_eq!(error_name(b), "RepoNotFound");
        assert_eq!(
            error_name(br#"{"error":"InvalidRequest","message":"bad cursor"}"#),
            "InvalidRequest"
        );
        assert_eq!(
            error_name(br#"{"error":"RecordNotFound"}"#),
            "RecordNotFound"
        );
        assert_eq!(error_name(b"not json"), "");
    }

    #[tokio::test]
    async fn concurrency_and_breaker() {
        let l = HostLimiter::new(100, 2);
        l.acquire("h").await.unwrap();
        l.acquire("h").await.unwrap();
        assert!(!l.has_capacity("h"));
        l.release("h", Ok(()));
        assert!(l.has_capacity("h"));
        l.release("h", Ok(()));
        for _ in 0..BREAKER_FAILURES {
            l.acquire("h").await.unwrap();
            l.release("h", Err(None));
        }
        assert!(l.cooling("h").is_some_and(|s| s <= 60));
        assert!(matches!(
            l.acquire("h").await,
            Err(NetError::Cooling { .. })
        ));
    }

    #[tokio::test]
    async fn retry_after_cools_host() {
        let l = HostLimiter::new(100, 2);
        l.acquire("x").await.unwrap();
        l.release("x", Err(Some(Duration::from_secs(30))));
        assert!(l.cooling("x").is_some_and(|s| s > 20));
    }
}
