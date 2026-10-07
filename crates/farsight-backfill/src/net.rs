//! Outbound requests (see `docs/design/backfill.md` and
//! `docs/design/security.md`): every request to a network-learned address
//! goes through the safe client, a per-host token bucket with a
//! concurrency limit, `429` / `RateLimit-Remaining: 0` / `Retry-After`
//! handling and a circuit breaker; the PLC directory has its own limiter
//! with half reserved for the resolver.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use farsight_core::bucket::{Bucket, Rate};
use farsight_core::net::{OutboundClient, OutboundError, OutboundResponse, SafeClient};
use serde_json::Value;
use tokio::sync::Notify;
use url::Url;

use crate::metrics as m;

/// XRPC error name: the repo is not on this host.
pub const REPO_NOT_FOUND: &str = "RepoNotFound";
/// XRPC error name: the repo has no such record.
pub const RECORD_NOT_FOUND: &str = "RecordNotFound";

/// Why a request did not produce a usable response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetError {
    /// The host is cooling down (429, breaker); retry after `secs`.
    #[error("host {host} cooling down for {secs} s")]
    Cooling {
        /// The limiter key of the host (`host[:port]`).
        host: String,
        /// Whole seconds of cooldown left when the request was refused;
        /// at least 1.
        secs: u64,
    },
    /// An XRPC error response (`{"error": name}`) or another HTTP status.
    #[error("HTTP {status} {name}")]
    Http {
        /// The HTTP status; never 200.
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

    /// Whether the server says it does not have the method asked for:
    /// the XRPC error `MethodNotImplemented`, or a status that means the
    /// route does not exist (`404`, `405`, `501`). Asked of a relay's
    /// `listReposByCollection`, which takes no repo, so a `404` there is
    /// never about a repo.
    pub fn method_missing(&self) -> bool {
        match self {
            NetError::Http { status, name } => {
                name == "MethodNotImplemented" || matches!(status, 404 | 405 | 501)
            }
            _ => false,
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
    /// The safe client: every request to an address learned from the
    /// network goes through its checks.
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
    bucket: Bucket,
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
/// Longest cooldown a host can ask for with `Retry-After`: the header is
/// the host's own word, and a larger value would shelve the host for as
/// long as it likes.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(3600);
/// Longest a request waits for a per-host slot before giving up.
pub const MAX_SLOT_WAIT: Duration = Duration::from_secs(120);

/// How a request that held a slot ended, for the breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotOutcome {
    /// The host answered (a `4xx` is a healthy host saying no).
    Healthy,
    /// `429` or `RateLimit-Remaining: 0`: cool down this long.
    RateLimited(Duration),
    /// A `5xx`, a transport error or a timeout: counts toward the breaker.
    Failed,
}

/// A slot on a host, from [`HostLimiter::acquire`]. [`Slot::release`]
/// frees it with the request's outcome; a slot dropped without one (the
/// request's future was cancelled, or it panicked) is freed all the same
/// and leaves the breaker as it was.
#[derive(Debug)]
pub struct Slot<'a> {
    limiter: &'a HostLimiter,
    host: String,
    released: bool,
}

impl Slot<'_> {
    /// Frees the slot and records the result for the breaker.
    pub fn release(mut self, outcome: SlotOutcome) {
        self.released = true;
        self.limiter.release(&self.host, Some(outcome));
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.limiter.release(&self.host, None);
        }
    }
}

/// `now + d`, or `now` where the sum cannot be represented.
fn after(now: Instant, d: Duration) -> Instant {
    now.checked_add(d).unwrap_or(now)
}

/// How long a request that got no slot sleeps before it looks again
/// (a freed slot wakes it sooner): until the bucket has a token, at least
/// 5 ms, or 250 ms when it is the concurrency limit that is in the way.
fn slot_wait(bucket: &Bucket, rate: Rate) -> Duration {
    if bucket.tokens() < 1.0 {
        Duration::from_secs_f64(bucket.secs_until_token(rate).max(0.005))
    } else {
        Duration::from_millis(250)
    }
}

/// Per-host limits: token bucket, concurrency, cooldown, breaker.
#[derive(Debug)]
pub struct HostLimiter {
    /// `rps` a second, and room for one second of it.
    rate: Rate,
    concurrency: u32,
    hosts: Mutex<HashMap<String, HostState>>,
    freed: Notify,
}

impl HostLimiter {
    /// A limiter with `rps` and `concurrency` per host.
    pub fn new(rps: u32, concurrency: u32) -> HostLimiter {
        HostLimiter {
            rate: Rate::one_second(f64::from(rps.max(1))),
            concurrency: concurrency.max(1),
            hosts: Mutex::new(HashMap::new()),
            freed: Notify::new(),
        }
    }

    fn state<'a>(
        map: &'a mut HashMap<String, HostState>,
        host: &str,
        rate: Rate,
    ) -> &'a mut HostState {
        map.entry(host.to_owned()).or_insert_with(|| HostState {
            bucket: Bucket::full(rate, Instant::now()),
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
        let s = Self::state(&mut map, host, self.rate);
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
    pub async fn acquire(&self, host: &str) -> Result<Slot<'_>, NetError> {
        let start = Instant::now();
        loop {
            let wait = {
                let mut map = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
                let (rate, conc) = (self.rate, self.concurrency);
                let s = Self::state(&mut map, host, rate);
                let now = Instant::now();
                if let Some(u) = s.cooldown_until.filter(|u| *u > now) {
                    return Err(NetError::Cooling {
                        host: host.to_owned(),
                        secs: u.duration_since(now).as_secs().max(1),
                    });
                }
                s.bucket.refill(now, rate);
                if s.inflight < conc && s.bucket.try_take() {
                    s.inflight += 1;
                    return Ok(Slot {
                        limiter: self,
                        host: host.to_owned(),
                        released: false,
                    });
                }
                slot_wait(&s.bucket, rate)
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

    /// Frees a slot; with an outcome, records it for the breaker.
    fn release(&self, host: &str, outcome: Option<SlotOutcome>) {
        let mut map = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        let s = Self::state(&mut map, host, self.rate);
        s.inflight = s.inflight.saturating_sub(1);
        let now = Instant::now();
        match outcome {
            None => {}
            Some(SlotOutcome::Healthy) => s.consecutive_failures = 0,
            Some(SlotOutcome::RateLimited(retry_after)) => {
                // 429 / RateLimit-Remaining: 0: honour Retry-After, up to
                // the bound.
                s.cooldown_until = Some(after(now, retry_after.min(MAX_RETRY_AFTER)));
            }
            Some(SlotOutcome::Failed) => {
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
                    s.cooldown_until = Some(after(now, d));
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
    reserved: Mutex<Bucket>,
    shared: Mutex<Bucket>,
    /// Each half's rate, with room for one second of it.
    half: Rate,
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
        let half = Rate::one_second((f64::from(rps.max(2))) / 2.0);
        PlcLimiter {
            reserved: Mutex::new(Bucket::full(half, Instant::now())),
            shared: Mutex::new(Bucket::full(half, Instant::now())),
            half,
        }
    }

    /// Takes a token, or says how long until there is one.
    fn take(bucket: &Mutex<Bucket>, rate: Rate) -> Option<Duration> {
        let mut b = bucket.lock().unwrap_or_else(|e| e.into_inner());
        Self::take_at(&mut b, rate, Instant::now())
    }

    fn take_at(b: &mut Bucket, rate: Rate, now: Instant) -> Option<Duration> {
        if b.take(now, rate) {
            None
        } else {
            Some(Duration::from_secs_f64(b.secs_until_token(rate)))
        }
    }

    /// Waits for a PLC token. The resolver tries the reserved half, then
    /// the shared half; the export waits on the shared half alone.
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
    /// Limits of every host but the PLC directory, keyed by
    /// `host[:port]`: `backfill.per_host_rps` and
    /// `backfill.per_host_concurrency`, cooldowns and the breaker.
    pub hosts: HostLimiter,
    /// The PLC directory's own limiter. A caller takes a token from it
    /// before a request to the directory; requests to that host skip
    /// `hosts`.
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
    // Seconds only (the date form gets the default), and never more than
    // the bound: the number is the peer's.
    let ra = h("retry-after")
        .and_then(|v| v.parse::<u64>().ok())
        .map(|secs| Duration::from_secs(secs).min(MAX_RETRY_AFTER));
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
        return REPO_NOT_FOUND.to_owned();
    }
    name.to_owned()
}

impl Net {
    /// A network layer over `client`. `plc_url` names the host that is
    /// exempt from the per-host limits and timed as the PLC directory.
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
        // Held across the request: if this future is dropped at the await
        // below, the slot is freed with it.
        let slot = if is_plc {
            None
        } else {
            Some(self.hosts.acquire(&host).await?)
        };
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
            Err(e) => (Err(NetError::Transport(e.to_string())), SlotOutcome::Failed),
            Ok(resp) => {
                let ra = retry_after(&resp);
                if resp.status == 200 {
                    let outcome = ra.map_or(SlotOutcome::Healthy, SlotOutcome::RateLimited);
                    (Ok(resp.body), outcome)
                } else {
                    let name = error_name(&resp.body);
                    // 4xx answers are healthy hosts saying no; only 5xx, 429
                    // and transport errors count toward the breaker.
                    let outcome = match ra {
                        Some(d) => SlotOutcome::RateLimited(d),
                        None if resp.status >= 500 => SlotOutcome::Failed,
                        None => SlotOutcome::Healthy,
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
        if let Some(slot) = slot {
            slot.release(outcome);
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
        let a = l.acquire("h").await.unwrap();
        let b = l.acquire("h").await.unwrap();
        assert!(!l.has_capacity("h"));
        a.release(SlotOutcome::Healthy);
        assert!(l.has_capacity("h"));
        b.release(SlotOutcome::Healthy);
        for _ in 0..BREAKER_FAILURES {
            l.acquire("h").await.unwrap().release(SlotOutcome::Failed);
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
        l.acquire("x")
            .await
            .unwrap()
            .release(SlotOutcome::RateLimited(Duration::from_secs(30)));
        assert!(l.cooling("x").is_some_and(|s| s > 20));
    }

    fn response(status: u16, headers: &[(&str, &str)]) -> OutboundResponse {
        OutboundResponse {
            status,
            headers: headers
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            body: Vec::new(),
            final_url: Url::parse("https://pds.example/").unwrap(),
        }
    }

    #[test]
    fn retry_after_is_bounded() {
        let ra = |v: &str| retry_after(&response(429, &[("retry-after", v)]));
        assert_eq!(ra("30"), Some(Duration::from_secs(30)));
        assert_eq!(ra("86400"), Some(MAX_RETRY_AFTER));
        assert_eq!(ra("18446744073709551615"), Some(MAX_RETRY_AFTER));
        // Not a number of seconds: the default.
        assert_eq!(ra("99999999999999999999999"), Some(DEFAULT_RETRY_AFTER));
        assert_eq!(ra("-5"), Some(DEFAULT_RETRY_AFTER));
        assert_eq!(
            ra("Wed, 21 Oct 2026 07:28:00 GMT"),
            Some(DEFAULT_RETRY_AFTER)
        );
        assert_eq!(retry_after(&response(200, &[("retry-after", "5")])), None);
        assert_eq!(
            retry_after(&response(200, &[("ratelimit-remaining", "0")])),
            Some(DEFAULT_RETRY_AFTER)
        );
    }

    #[tokio::test]
    async fn a_huge_cooldown_neither_panics_nor_outlasts_the_bound() {
        let l = HostLimiter::new(100, 2);
        // What `Retry-After: 18446744073709551615` would ask for, handed
        // to the limiter unclamped.
        l.acquire("x")
            .await
            .unwrap()
            .release(SlotOutcome::RateLimited(Duration::from_secs(u64::MAX)));
        assert!(
            l.cooling("x")
                .is_some_and(|s| s <= MAX_RETRY_AFTER.as_secs())
        );
        assert!(l.has_capacity("y"));
    }

    #[tokio::test]
    async fn a_dropped_slot_is_freed_and_is_not_a_failure() {
        let l = HostLimiter::new(100, 1);
        for _ in 0..(BREAKER_FAILURES * 2) {
            // A request whose future is dropped while it holds the slot.
            let slot = l.acquire("h").await.unwrap();
            assert!(!l.has_capacity("h"));
            drop(slot);
            assert!(l.has_capacity("h"));
        }
        assert_eq!(l.cooling("h"), None);
        // The same when the holder panics.
        let r = farsight_core::task::catch(async {
            let _slot = l.acquire("h").await.unwrap();
            if std::hint::black_box(true) {
                panic!("job failed");
            }
        })
        .await;
        assert!(r.is_err());
        assert!(l.has_capacity("h"));
    }

    #[test]
    fn missing_method_is_told_from_other_failures() {
        let http = |status: u16, name: &str| NetError::Http {
            status,
            name: name.to_owned(),
        };
        assert!(http(501, "MethodNotImplemented").method_missing());
        assert!(http(400, "MethodNotImplemented").method_missing());
        assert!(http(404, "").method_missing());
        assert!(http(405, "").method_missing());
        assert!(http(501, "").method_missing());
        // A relay that is down, busy or refusing is not one without the
        // method.
        assert!(!http(500, "").method_missing());
        assert!(!http(502, "").method_missing());
        assert!(!http(429, "").method_missing());
        assert!(!http(400, "InvalidRequest").method_missing());
        assert!(!NetError::Transport("connection refused".into()).method_missing());
        assert!(!NetError::Decode("no repos".into()).method_missing());
        assert!(
            !NetError::Cooling {
                host: "relay".into(),
                secs: 5
            }
            .method_missing()
        );
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        fn header_value() -> impl Strategy<Value = String> {
            prop_oneof![
                any::<String>(),
                any::<u64>().prop_map(|n| n.to_string()),
                (0u64..10_000).prop_map(|n| format!(" {n}\t")),
                "[+-]?[0-9]{1,40}(\\.[0-9]{1,5})?",
                "[0-9]{1,5}[eE][0-9]{1,5}",
                Just("Wed, 21 Oct 2026 07:28:00 GMT".to_owned()),
                Just(String::new()),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Whatever a host answers with: a cool-down is asked for
            /// exactly on a `429` or `RateLimit-Remaining: 0`, it is the
            /// `Retry-After` seconds when that is a whole number and the
            /// default otherwise, and it is never longer than the bound.
            #[test]
            fn retry_after_is_total_and_bounded(
                status in prop_oneof![Just(200u16), Just(429), Just(503), any::<u16>()],
                value in prop::option::of(header_value()),
                remaining in prop::option::of(prop_oneof![
                    Just("0".to_owned()),
                    Just(" 0 ".to_owned()),
                    Just("00".to_owned()),
                    header_value(),
                ]),
            ) {
                let mut headers = Vec::new();
                if let Some(v) = &value {
                    headers.push(("retry-after", v.as_str()));
                }
                if let Some(v) = &remaining {
                    headers.push(("ratelimit-remaining", v.as_str()));
                }
                let got = retry_after(&response(status, &headers));
                let limited = status == 429 || remaining.as_deref().is_some_and(|v| v.trim() == "0");
                prop_assert_eq!(got.is_some(), limited);
                if let Some(d) = got {
                    prop_assert!(d <= MAX_RETRY_AFTER);
                    let secs = value.as_deref().and_then(|v| v.trim().parse::<u64>().ok());
                    let expected = secs.map_or(DEFAULT_RETRY_AFTER, |s| {
                        Duration::from_secs(s.min(MAX_RETRY_AFTER.as_secs()))
                    });
                    prop_assert_eq!(d, expected);
                }
            }

            /// Any cool-down handed to the limiter, in range or not: the
            /// host cools for no longer than the bound, and for as long
            /// as asked when that is less.
            #[test]
            fn a_cooldown_never_outlasts_the_bound(
                d in prop_oneof![
                    (0u64..7200).prop_map(Duration::from_secs),
                    (any::<u64>(), 0u32..1_000_000_000).prop_map(|(s, n)| Duration::new(s, n)),
                    Just(Duration::MAX),
                ],
            ) {
                let now = Instant::now();
                prop_assert!(after(now, d) >= now);
                let l = HostLimiter::new(100, 2);
                l.release("h", Some(SlotOutcome::RateLimited(d)));
                let left = l.cooling("h");
                prop_assert!(left.is_none_or(|s| (1..=MAX_RETRY_AFTER.as_secs()).contains(&s)));
                prop_assert!(left.is_none_or(|s| s <= d.as_secs().max(1)));
                if d >= Duration::from_secs(2) {
                    prop_assert!(left.is_some() && !l.has_capacity("h"));
                }
                prop_assert!(l.has_capacity("other"));
            }

            /// The wait of a request without a slot is between 5 ms and
            /// the time the rate needs for one token, or the fixed 250 ms
            /// when a token is there; the PLC wait is the time to one
            /// token, and nothing when one was taken.
            #[test]
            fn waits_for_a_token_are_bounded(
                rps in 1u32..500,
                spent in 0u32..600,
                ms in 0u64..3_000,
            ) {
                let rate = Rate::one_second(f64::from(rps));
                let t0 = Instant::now();
                let mut b = Bucket::full(rate, t0);
                for _ in 0..spent {
                    b.try_take();
                }
                let now = t0 + Duration::from_millis(ms);
                b.refill(now, rate);
                let wait = slot_wait(&b, rate);
                if b.tokens() < 1.0 {
                    prop_assert!(wait >= Duration::from_millis(5));
                    prop_assert!(wait.as_secs_f64() <= (1.0 / f64::from(rps)).max(0.005) + 1e-9);
                } else {
                    prop_assert_eq!(wait, Duration::from_millis(250));
                }
                let had = b.tokens();
                match PlcLimiter::take_at(&mut b, rate, now) {
                    None => prop_assert!(had >= 1.0 && b.tokens() == had - 1.0),
                    Some(w) => {
                        prop_assert!(had < 1.0 && b.tokens() == had);
                        prop_assert!(w > Duration::ZERO);
                        prop_assert!(w.as_secs_f64() <= 1.0 / f64::from(rps) + 1e-9);
                    }
                }
            }

            /// An error body of any bytes has a name, without a panic.
            #[test]
            fn error_names_are_total(body in prop_oneof![
                prop::collection::vec(any::<u8>(), 0..128),
                ("\\PC{0,20}", "\\PC{0,40}").prop_map(|(e, m)| {
                    serde_json::to_vec(&serde_json::json!({"error": e, "message": m})).unwrap()
                }),
            ]) {
                let name = error_name(&body);
                prop_assert!(name.len() <= body.len());
            }
        }
    }
}
