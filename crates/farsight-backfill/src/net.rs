//! Outbound requests (see `docs/design/backfill.md` and
//! `docs/design/security.md`): every request to a network-learned address
//! goes through the safe client, a per-host token bucket with a
//! concurrency limit, a second bucket and limit shared by every host of
//! one registrable domain, `429` / `RateLimit-Remaining: 0` /
//! `Retry-After` handling and a circuit breaker; the PLC directory has
//! its own limiter with half reserved for the resolver.
//!
//! One [`Net`] serves the process for its lifetime: a rebuild after a
//! config change gives it the new client and limits and keeps what it
//! knows of every host (cooldowns, breaker trips, requests in flight).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use farsight_core::Config;

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
    /// The body is larger than the client reads; carries the bound in
    /// bytes. Asking for the same thing again gives the same answer, so
    /// a caller that can ask for less does.
    #[error("response body exceeds {0} bytes")]
    TooLarge(u64),
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
            NetError::Transport(_) | NetError::TooLarge(_) => "transport",
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
    /// Requests that wait for a slot here: the host's rate or its
    /// request limit is in their way.
    waiting: u32,
    cooldown_until: Option<Instant>,
    consecutive_failures: u32,
    trips: u32,
    last_trip: Option<Instant>,
    last_used: Instant,
}

/// What the hosts of one registrable domain share.
#[derive(Debug)]
struct DomainState {
    bucket: Bucket,
    inflight: u32,
    last_used: Instant,
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
/// Hosts (and domains) remembered before the idle ones are forgotten.
pub const REMEMBERED: usize = 10_000;
/// A host or domain not asked for this long, with nothing in flight and
/// no cooldown or recent breaker trip, is idle.
pub const IDLE_AFTER: Duration = Duration::from_secs(600);

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
    domain: Option<String>,
    released: bool,
}

impl Slot<'_> {
    /// Frees the slot and records the result for the breaker.
    pub fn release(mut self, outcome: SlotOutcome) {
        self.released = true;
        self.limiter
            .release(&self.host, self.domain.as_deref(), Some(outcome));
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.limiter
                .release(&self.host, self.domain.as_deref(), None);
        }
    }
}

/// A request that waits for a slot on a host ([`HostLimiter::acquire`]),
/// counted in the host's `waiting` until it is dropped.
struct Waiting<'a> {
    limiter: &'a HostLimiter,
    host: &'a str,
    counted: bool,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if self.counted {
            let mut s = self.limiter.lock();
            if let Some(h) = s.hosts.get_mut(self.host) {
                h.waiting = h.waiting.saturating_sub(1);
            }
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

/// The limits a [`HostLimiter`] applies.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HostLimits {
    /// Requests a second towards one host, with room for one second of
    /// it.
    pub host: Rate,
    /// Requests in flight towards one host.
    pub host_concurrency: u32,
    /// Requests a second towards all hosts of one registrable domain.
    pub domain: Rate,
    /// Requests in flight towards all hosts of one registrable domain.
    pub domain_concurrency: u32,
}

impl HostLimits {
    /// The limits of `backfill.per_host_*` and `backfill.per_domain_*`.
    /// A domain is never held below what one of its hosts is allowed.
    pub fn from_config(cfg: &Config) -> HostLimits {
        let b = &cfg.backfill;
        HostLimits::new(
            b.per_host_rps,
            b.per_host_concurrency,
            b.per_domain_rps,
            b.per_domain_concurrency,
        )
    }

    /// Limits of `rps` and `concurrency` per host and `domain_rps` and
    /// `domain_concurrency` per domain.
    pub fn new(rps: u32, concurrency: u32, domain_rps: u32, domain_concurrency: u32) -> HostLimits {
        let rps = rps.max(1);
        let concurrency = concurrency.max(1);
        HostLimits {
            host: Rate::one_second(f64::from(rps)),
            host_concurrency: concurrency,
            domain: Rate::one_second(f64::from(domain_rps.max(rps))),
            domain_concurrency: domain_concurrency.max(concurrency),
        }
    }
}

#[derive(Debug)]
struct Limited {
    limits: HostLimits,
    hosts: HashMap<String, HostState>,
    domains: HashMap<String, DomainState>,
}

impl Limited {
    fn host(&mut self, host: &str, now: Instant) -> &mut HostState {
        if !self.hosts.contains_key(host) && self.hosts.len() >= REMEMBERED {
            self.hosts.retain(|_, s| {
                s.inflight > 0
                    || s.waiting > 0
                    || s.cooldown_until.is_some_and(|u| u > now)
                    || s.last_trip
                        .is_some_and(|t| now.saturating_duration_since(t) < BREAKER_REPEAT)
                    || now.saturating_duration_since(s.last_used) < IDLE_AFTER
            });
        }
        let rate = self.limits.host;
        let s = self
            .hosts
            .entry(host.to_owned())
            .or_insert_with(|| HostState {
                bucket: Bucket::full(rate, now),
                inflight: 0,
                waiting: 0,
                cooldown_until: None,
                consecutive_failures: 0,
                trips: 0,
                last_trip: None,
                last_used: now,
            });
        s.last_used = now;
        s
    }

    fn domain(&mut self, domain: &str, now: Instant) -> &mut DomainState {
        if !self.domains.contains_key(domain) && self.domains.len() >= REMEMBERED {
            self.domains.retain(|_, s| {
                s.inflight > 0 || now.saturating_duration_since(s.last_used) < IDLE_AFTER
            });
        }
        let rate = self.limits.domain;
        let s = self
            .domains
            .entry(domain.to_owned())
            .or_insert_with(|| DomainState {
                bucket: Bucket::full(rate, now),
                inflight: 0,
                last_used: now,
            });
        s.last_used = now;
        s
    }
}

/// Per-host limits (token bucket, concurrency, cooldown, breaker) and,
/// over all hosts of one registrable domain, a second token bucket and
/// concurrency limit: an operator who puts every account on a host name
/// of its own under one domain gets one domain's worth of requests, not
/// one host's worth each.
#[derive(Debug)]
pub struct HostLimiter {
    state: Mutex<Limited>,
    freed: Notify,
}

impl HostLimiter {
    /// A limiter with `rps` and `concurrency` per host, and the same per
    /// domain.
    pub fn new(rps: u32, concurrency: u32) -> HostLimiter {
        HostLimiter::with(HostLimits::new(rps, concurrency, rps, concurrency))
    }

    /// A limiter with `limits`.
    pub fn with(limits: HostLimits) -> HostLimiter {
        HostLimiter {
            state: Mutex::new(Limited {
                limits,
                hosts: HashMap::new(),
                domains: HashMap::new(),
            }),
            freed: Notify::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Limited> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Replaces the limits. What is known of each host stays.
    pub fn set_limits(&self, limits: HostLimits) {
        self.lock().limits = limits;
        self.freed.notify_waiters();
    }

    /// How many hosts and domains are remembered.
    pub fn remembered(&self) -> (usize, usize) {
        let s = self.lock();
        (s.hosts.len(), s.domains.len())
    }

    /// Whether a request to `host` could start now (blocked-head skip of
    /// the list-job lanes): the host is not cooling down, and it and
    /// `domain` have a request slot free.
    pub fn has_capacity(&self, host: &str, domain: Option<&str>) -> bool {
        let mut s = self.lock();
        let now = Instant::now();
        let limits = s.limits;
        let h = s.host(host, now);
        if h.cooldown_until.is_some_and(|u| u > now)
            || h.inflight >= limits.host_concurrency
            || h.waiting >= limits.host_concurrency
        {
            return false;
        }
        domain.is_none_or(|d| s.domain(d, now).inflight < limits.domain_concurrency)
    }

    /// Whether `host` has a queue: as many requests in flight, or
    /// waiting for a slot, as it takes at once. More work for it would
    /// only wait. A host that is cooling down has no queue by that: its
    /// requests are refused at once, and a job for it fails and is
    /// retried on its schedule, which is how a host that stays down
    /// comes to be given up.
    pub fn has_queue(&self, host: &str) -> bool {
        let s = self.lock();
        let limits = s.limits;
        s.hosts.get(host).is_some_and(|h| {
            h.inflight >= limits.host_concurrency || h.waiting >= limits.host_concurrency
        })
    }

    /// The hosts known not to take a request now: cooling down, at
    /// their request limit, with as many requests waiting for a slot as
    /// the host takes at once, or under a domain (`domain_of`) at its
    /// limit. Work for them is left where it waits, so it neither takes
    /// a worker nor a place among the candidates loaded. Read-only: it
    /// does not count as a use of the host.
    ///
    /// The requests in flight alone do not say that a host is full. One
    /// that answers in a few hundredths of a second and is asked at its
    /// rate has a request or two in flight and every job that is on it
    /// waiting for the next token. Without the count of those, jobs for
    /// it are started until they hold every worker, and the other hosts
    /// are left with none.
    pub fn blocked(&self, domain_of: impl Fn(&str) -> Option<String>) -> Vec<String> {
        let s = self.lock();
        let now = Instant::now();
        let limits = s.limits;
        s.hosts
            .iter()
            .filter(|(host, h)| {
                h.cooldown_until.is_some_and(|u| u > now)
                    || h.inflight >= limits.host_concurrency
                    || h.waiting >= limits.host_concurrency
                    || domain_of(host)
                        .and_then(|d| s.domains.get(&d))
                        .is_some_and(|d| d.inflight >= limits.domain_concurrency)
            })
            .map(|(host, _)| host.clone())
            .collect()
    }

    /// Seconds `host` is still cooling down, if it is.
    pub fn cooling(&self, host: &str) -> Option<u64> {
        let s = self.lock();
        let now = Instant::now();
        s.hosts
            .get(host)
            .and_then(|s| s.cooldown_until)
            .filter(|u| *u > now)
            .map(|u| u.duration_since(now).as_secs().max(1))
    }

    /// Waits for a slot on `host`: a token and a free request slot of the
    /// host and, with a `domain`, of the domain too.
    pub async fn acquire(&self, host: &str, domain: Option<&str>) -> Result<Slot<'_>, NetError> {
        let start = Instant::now();
        // Counted on the host from its first wait until this call ends,
        // with a slot, with an error or dropped.
        let mut waiting = Waiting {
            limiter: self,
            host,
            counted: false,
        };
        loop {
            let wait = {
                let mut s = self.lock();
                let limits = s.limits;
                let now = Instant::now();
                // The domain first: what it allows is read before the
                // host is looked at, and taken only with the host's.
                let (domain_free, domain_wait) = match domain {
                    Some(d) => {
                        let ds = s.domain(d, now);
                        ds.bucket.refill(now, limits.domain);
                        (
                            ds.inflight < limits.domain_concurrency && ds.bucket.tokens() >= 1.0,
                            slot_wait(&ds.bucket, limits.domain),
                        )
                    }
                    None => (true, Duration::ZERO),
                };
                let h = s.host(host, now);
                if let Some(u) = h.cooldown_until.filter(|u| *u > now) {
                    return Err(NetError::Cooling {
                        host: host.to_owned(),
                        secs: u.duration_since(now).as_secs().max(1),
                    });
                }
                h.bucket.refill(now, limits.host);
                let took =
                    domain_free && h.inflight < limits.host_concurrency && h.bucket.try_take();
                if took {
                    h.inflight += 1;
                }
                let host_wait = slot_wait(&h.bucket, limits.host);
                if !took && !waiting.counted {
                    h.waiting += 1;
                    waiting.counted = true;
                }
                if took {
                    if let Some(d) = domain {
                        let ds = s.domain(d, now);
                        ds.bucket.try_take();
                        ds.inflight += 1;
                    }
                    return Ok(Slot {
                        limiter: self,
                        host: host.to_owned(),
                        domain: domain.map(str::to_owned),
                        released: false,
                    });
                }
                if domain_free {
                    host_wait
                } else {
                    host_wait.max(domain_wait)
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

    /// Frees a slot; with an outcome, records it for the breaker.
    fn release(&self, host: &str, domain: Option<&str>, outcome: Option<SlotOutcome>) {
        let mut state = self.lock();
        let now = Instant::now();
        if let Some(d) = domain {
            let ds = state.domain(d, now);
            ds.inflight = ds.inflight.saturating_sub(1);
        }
        let s = state.host(host, now);
        s.inflight = s.inflight.saturating_sub(1);
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
        drop(state);
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
    half: Mutex<Rate>,
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
    fn half_of(rps: u32) -> Rate {
        Rate::one_second((f64::from(rps.max(2))) / 2.0)
    }

    /// A limiter for `rps` requests per second.
    pub fn new(rps: u32) -> PlcLimiter {
        let half = Self::half_of(rps);
        PlcLimiter {
            reserved: Mutex::new(Bucket::full(half, Instant::now())),
            shared: Mutex::new(Bucket::full(half, Instant::now())),
            half: Mutex::new(half),
        }
    }

    /// Changes the rate to `rps` requests per second.
    pub fn set_rps(&self, rps: u32) {
        *self.half.lock().unwrap_or_else(|e| e.into_inner()) = Self::half_of(rps);
    }

    fn half(&self) -> Rate {
        *self.half.lock().unwrap_or_else(|e| e.into_inner())
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
        let asked = Instant::now();
        self.take_turn(use_).await;
        waited_turn(TURN_PLC, asked);
    }

    async fn take_turn(&self, use_: PlcUse) {
        loop {
            let half = self.half();
            if use_ == PlcUse::Resolver {
                match Self::take(&self.reserved, half) {
                    None => return,
                    Some(w) => match Self::take(&self.shared, half) {
                        None => return,
                        Some(w2) => tokio::time::sleep(w.min(w2)).await,
                    },
                }
            } else {
                match Self::take(&self.shared, half) {
                    None => return,
                    Some(w) => tokio::time::sleep(w).await,
                }
            }
        }
    }
}

tokio::task_local! {
    /// The outbound requests of the job running in this task, counted as
    /// they are made: the scheduler charges a job's requester from it
    /// while the job runs.
    pub static METER: Arc<AtomicU64>;
    /// The time, in milliseconds, the job running in this task has spent
    /// waiting for hosts to answer (not for a request slot). A job's
    /// requester is charged for it as for its requests, and a listing
    /// judges a host's pace by it.
    pub static WAITED_MS: Arc<AtomicU64>;
    /// The time, in milliseconds, the job running in this task has spent
    /// waiting for its turn and not for an answer: [`TURN_SLOT`] for a
    /// request slot of a host, [`TURN_PLC`] for a token of the PLC
    /// directory's limiter. The line a finished job logs carries both.
    pub static TURN_MS: Arc<[AtomicU64; 2]>;
}

/// Index in [`TURN_MS`] of the wait for a host's request slot.
pub const TURN_SLOT: usize = 0;
/// Index in [`TURN_MS`] of the wait for the PLC directory's limiter.
pub const TURN_PLC: usize = 1;

/// Adds the time since `asked` to the running job's wait of `kind`.
fn waited_turn(kind: usize, asked: Instant) {
    let ms = u64::try_from(asked.elapsed().as_millis()).unwrap_or(u64::MAX);
    let _ = TURN_MS.try_with(|t| t[kind].fetch_add(ms, Ordering::Relaxed));
}

/// The milliseconds the job running in this task has waited for its turn
/// so far, at hosts and at the PLC directory; `None` outside a job.
pub fn turn_ms() -> Option<(u64, u64)> {
    TURN_MS
        .try_with(|t| {
            (
                t[TURN_SLOT].load(Ordering::Relaxed),
                t[TURN_PLC].load(Ordering::Relaxed),
            )
        })
        .ok()
}

/// The milliseconds the job running in this task has waited for answers
/// so far; `None` outside a metered job.
pub fn waited_ms() -> Option<u64> {
    WAITED_MS.try_with(|m| m.load(Ordering::Relaxed)).ok()
}

/// What a [`Net`] takes from the config, replaced together at a rebuild.
struct Settings {
    client: Client,
    /// `host[:port]` of the PLC directory.
    plc_host: String,
    /// `limits.large_hosts`: hosts exempt from the per-domain limits.
    large_hosts: Vec<String>,
}

/// The network layer shared by every job.
pub struct Net {
    settings: RwLock<Settings>,
    /// Limits of every host but the PLC directory: per `host[:port]`
    /// (`backfill.per_host_rps`, `backfill.per_host_concurrency`,
    /// cooldowns and the breaker) and per registrable domain
    /// (`backfill.per_domain_rps`, `backfill.per_domain_concurrency`).
    pub hosts: HostLimiter,
    /// The PLC directory's own limiter. A caller takes a token from it
    /// before a request to the directory; requests to that host skip
    /// `hosts`.
    pub plc: PlcLimiter,
}

/// The host part of a URL used as the limiter key (`host[:port]`).
pub fn host_key(url: &Url) -> String {
    let h = url.host_str().unwrap_or("").to_ascii_lowercase();
    match url.port() {
        Some(p) => format!("{h}:{p}"),
        None => h,
    }
}

/// The name in a limiter key, without its port: `pds.example` of
/// `pds.example:2583`, `[2001:db8::1]` of `[2001:db8::1]:443`.
pub fn bare_host(host: &str) -> &str {
    if host.starts_with('[') {
        return host.find(']').map_or(host, |i| &host[..=i]);
    }
    host.split(':').next().unwrap_or(host)
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
    /// A network layer over `client` with the limits of `cfg`:
    /// `backfill.plc_url` names the host that is exempt from the per-host
    /// limits and timed as the PLC directory, and `limits.large_hosts`
    /// the hosts exempt from the per-domain limits.
    pub fn new(client: Client, cfg: &Config) -> Net {
        Net {
            settings: RwLock::new(Self::settings(client, cfg)),
            hosts: HostLimiter::with(HostLimits::from_config(cfg)),
            plc: PlcLimiter::new(cfg.backfill.plc_rps),
        }
    }

    fn settings(client: Client, cfg: &Config) -> Settings {
        Settings {
            client,
            plc_host: Url::parse(&cfg.backfill.plc_url)
                .map(|u| host_key(&u))
                .unwrap_or_default(),
            large_hosts: cfg.limits.large_hosts.clone(),
        }
    }

    /// Takes a new client and the limits of `cfg` (a rebuild after a
    /// config change). Cooldowns, breaker state and requests in flight
    /// stay as they are.
    pub fn reconfigure(&self, client: Client, cfg: &Config) {
        *self.settings.write().unwrap_or_else(|e| e.into_inner()) = Self::settings(client, cfg);
        self.set_limits(cfg);
    }

    /// Applies the request limits of `cfg` (`backfill.per_host_*`,
    /// `backfill.per_domain_*`, `backfill.plc_rps`) to the requests that
    /// follow.
    pub fn set_limits(&self, cfg: &Config) {
        self.hosts.set_limits(HostLimits::from_config(cfg));
        self.plc.set_rps(cfg.backfill.plc_rps);
    }

    /// The registrable domain `host` is limited under with the other
    /// hosts of that domain; `None` for a large host, which is limited
    /// by itself.
    pub fn domain_of(&self, host: &str) -> Option<String> {
        let bare = bare_host(host);
        let s = self.settings.read().unwrap_or_else(|e| e.into_inner());
        if farsight_core::config::host_matches(&s.large_hosts, bare) {
            return None;
        }
        Some(farsight_core::registrable_domain(bare))
    }

    /// Whether `host` has a queue of requests ([`HostLimiter::has_queue`]).
    pub fn has_queue(&self, host: &str) -> bool {
        self.hosts.has_queue(host)
    }

    /// Whether a request to `host` could start now.
    pub fn has_capacity(&self, host: &str) -> bool {
        self.hosts
            .has_capacity(host, self.domain_of(host).as_deref())
    }

    /// The hosts work is not started for now ([`HostLimiter::blocked`]).
    pub fn blocked_hosts(&self) -> Vec<String> {
        self.hosts.blocked(|h| self.domain_of(h))
    }

    /// `GET url` as JSON through the per-host limiter. `method` labels the
    /// metrics (`describeRepo`, `listRecords`, …).
    pub async fn get_json(&self, url: &Url, method: &'static str) -> Result<Value, NetError> {
        let body = self.get_body(url, method).await?;
        serde_json::from_slice(&body).map_err(|e| NetError::Decode(e.to_string()))
    }

    /// `GET url` as JSON from the PLC directory: a DID document. The
    /// caller has taken its turn at the directory's own limiter.
    pub async fn get_plc_json(&self, url: &Url) -> Result<Value, NetError> {
        let body = self.request(url, "plc", true).await?;
        serde_json::from_slice(&body).map_err(|e| NetError::Decode(e.to_string()))
    }

    /// `GET url` as text from the PLC directory (the export's JSON
    /// lines). The caller has taken its turn at the directory's limiter.
    pub async fn get_text(&self, url: &Url) -> Result<String, NetError> {
        let body = self.request(url, "export", true).await?;
        String::from_utf8(body).map_err(|e| NetError::Decode(e.to_string()))
    }

    /// `GET url` through the per-host limiter: the body of a `200`.
    pub async fn get_body(&self, url: &Url, method: &'static str) -> Result<Vec<u8>, NetError> {
        self.request(url, method, false).await
    }

    /// One request. `directory` is true for the two calls made to the
    /// PLC directory, which its own limiter paces; every other request
    /// takes a slot of its host, whatever the host is. An account whose
    /// DID document names the directory's host as its PDS is therefore
    /// limited like any other host and not let through unpaced.
    async fn request(
        &self,
        url: &Url,
        method: &'static str,
        directory: bool,
    ) -> Result<Vec<u8>, NetError> {
        let host = host_key(url);
        let (client, is_plc) = {
            let s = self.settings.read().unwrap_or_else(|e| e.into_inner());
            (s.client.clone(), directory && host == s.plc_host)
        };
        // Held across the request: if this future is dropped at the await
        // below, the slot is freed with it.
        let slot = if is_plc {
            None
        } else {
            let domain = self.domain_of(&host);
            let asked = Instant::now();
            let slot = self.hosts.acquire(&host, domain.as_deref()).await;
            waited_turn(TURN_SLOT, asked);
            Some(slot?)
        };
        let _ = METER.try_with(|m| m.fetch_add(1, Ordering::Relaxed));
        let started = Instant::now();
        let r = client.get(url).await;
        let waited = started.elapsed();
        let _ = WAITED_MS.try_with(|m| {
            m.fetch_add(
                u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            )
        });
        let elapsed = waited.as_secs_f64();
        let label = m::host_label(&host);
        if is_plc {
            metrics::histogram!(m::PLC_REQUEST_SECONDS).record(elapsed);
        } else {
            metrics::histogram!(m::PDS_REQUEST_SECONDS, "host" => label.clone(), "method" => method)
                .record(elapsed);
        }
        let (result, outcome) = match r {
            Err(farsight_core::net::OutboundError::TooLarge(n)) => {
                (Err(NetError::TooLarge(n)), SlotOutcome::Failed)
            }
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
        let a = l.acquire("h", None).await.unwrap();
        let b = l.acquire("h", None).await.unwrap();
        assert!(!l.has_capacity("h", None));
        a.release(SlotOutcome::Healthy);
        assert!(l.has_capacity("h", None));
        b.release(SlotOutcome::Healthy);
        for _ in 0..BREAKER_FAILURES {
            l.acquire("h", None)
                .await
                .unwrap()
                .release(SlotOutcome::Failed);
        }
        assert!(l.cooling("h").is_some_and(|s| s <= 60));
        assert!(matches!(
            l.acquire("h", None).await,
            Err(NetError::Cooling { .. })
        ));
    }

    /// Polls `f` once and says whether it is still waiting.
    fn pending<F: std::future::Future>(f: &mut std::pin::Pin<Box<F>>) -> bool {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        f.as_mut().poll(&mut cx).is_pending()
    }

    #[tokio::test]
    async fn a_host_with_a_queue_of_waiting_requests_takes_no_new_work() {
        // One request a second and room for two at once: the first takes
        // the token there is, and the host is free again once it is back.
        let l = HostLimiter::new(1, 2);
        l.acquire("h", None)
            .await
            .unwrap()
            .release(SlotOutcome::Healthy);
        assert!(l.blocked(|_| None).is_empty());
        assert!(l.has_capacity("h", None));
        // Two more ask before there is another token: nothing is in
        // flight, and both wait.
        let mut a = Box::pin(l.acquire("h", None));
        let mut b = Box::pin(l.acquire("h", None));
        assert!(pending(&mut a));
        // One waiting is under what the host takes at once.
        assert!(l.blocked(|_| None).is_empty());
        assert!(pending(&mut b));
        // As many waiting as it takes at once: no more work for it.
        assert_eq!(l.blocked(|_| None), ["h".to_owned()]);
        assert!(!l.has_capacity("h", None));
        assert!(l.has_queue("h"));
        // Polled again, a waiter is counted once.
        assert!(pending(&mut a));
        assert_eq!(l.blocked(|_| None), ["h".to_owned()]);
        // A waiter that gives up is no longer counted, and the host is
        // kept while one waits.
        drop(a);
        assert!(l.blocked(|_| None).is_empty());
        assert_eq!(l.remembered().0, 1);
        drop(b);
        assert!(l.has_capacity("h", None));
        assert!(!l.has_queue("h"));
        // Another host was never affected.
        assert!(l.has_capacity("other", None));
        // A host that is cooling down takes no request and has no queue:
        // a job for it is to fail, not to wait.
        l.acquire("down", None)
            .await
            .unwrap()
            .release(SlotOutcome::RateLimited(Duration::from_secs(60)));
        assert!(!l.has_capacity("down", None));
        assert!(!l.has_queue("down"));
    }

    #[tokio::test]
    async fn retry_after_cools_host() {
        let l = HostLimiter::new(100, 2);
        l.acquire("x", None)
            .await
            .unwrap()
            .release(SlotOutcome::RateLimited(Duration::from_secs(30)));
        assert!(l.cooling("x").is_some_and(|s| s > 20));
    }

    #[tokio::test]
    async fn hosts_of_one_domain_share_its_slots_and_its_rate() {
        // Two requests at a time per host, three per domain.
        let l = HostLimiter::with(HostLimits::new(100, 2, 100, 3));
        let d = Some("evil.example");
        let a1 = l.acquire("a.evil.example", d).await.unwrap();
        let a2 = l.acquire("a.evil.example", d).await.unwrap();
        // The host is full; a second host of the domain still gets one.
        assert!(!l.has_capacity("a.evil.example", d));
        assert!(l.has_capacity("b.evil.example", d));
        let b1 = l.acquire("b.evil.example", d).await.unwrap();
        // The domain is full now: no host of it has room, a fresh one
        // included; a host of another domain does.
        assert!(!l.has_capacity("b.evil.example", d));
        assert!(!l.has_capacity("c.evil.example", d));
        assert!(l.has_capacity("pds.other.example", Some("other.example")));
        // The hosts work is not started for: every known host of the
        // full domain, and no other. Asking does not count as a use.
        let domain_of = |h: &str| h.split_once('.').map(|(_, d)| d.to_owned());
        let (hosts, _) = l.remembered();
        let mut blocked = l.blocked(domain_of);
        blocked.sort();
        assert_eq!(
            blocked,
            ["a.evil.example", "b.evil.example", "c.evil.example"]
        );
        assert_eq!(l.remembered().0, hosts);
        let waiting =
            tokio::time::timeout(Duration::from_millis(60), l.acquire("c.evil.example", d)).await;
        assert!(
            waiting.is_err(),
            "a fourth request of the domain got a slot"
        );
        // A freed slot of any host of the domain lets it in.
        a1.release(SlotOutcome::Healthy);
        let c1 = tokio::time::timeout(Duration::from_secs(2), l.acquire("c.evil.example", d))
            .await
            .expect("a slot once one is free")
            .unwrap();
        drop((a2, b1, c1));
        assert!(l.has_capacity("c.evil.example", d));

        // The rate: one a second for the domain, however many hosts.
        let l = HostLimiter::with(HostLimits::new(1, 1, 1, 50));
        l.acquire("h0.evil.example", d)
            .await
            .unwrap()
            .release(SlotOutcome::Healthy);
        // The second host has its own token left; the domain has none.
        let second =
            tokio::time::timeout(Duration::from_millis(100), l.acquire("h1.evil.example", d)).await;
        assert!(
            second.is_err(),
            "a second request within the second got a token"
        );
        // Without a domain (a large host) only the host's limits apply.
        l.acquire("h1.evil.example", None)
            .await
            .unwrap()
            .release(SlotOutcome::Healthy);
    }

    #[test]
    fn a_domain_is_never_held_below_one_host() {
        let l = HostLimits::new(10, 4, 2, 1);
        assert_eq!((l.domain.per_sec, l.domain_concurrency), (10.0, 4));
        let l = HostLimits::new(10, 4, 20, 8);
        assert_eq!((l.host.per_sec, l.host_concurrency), (10.0, 4));
        assert_eq!((l.domain.per_sec, l.domain_concurrency), (20.0, 8));
    }

    #[tokio::test]
    async fn new_limits_apply_and_cooldowns_stay() {
        let l = HostLimiter::new(100, 1);
        l.acquire("h", None)
            .await
            .unwrap()
            .release(SlotOutcome::RateLimited(Duration::from_secs(30)));
        let held = l.acquire("g", None).await.unwrap();
        assert!(!l.has_capacity("g", None));
        l.set_limits(HostLimits::new(100, 2, 100, 2));
        // The cooldown is the host's, not the limiter's settings'.
        assert!(l.cooling("h").is_some());
        assert!(l.has_capacity("g", None));
        drop(held);
    }

    #[tokio::test]
    async fn idle_hosts_are_forgotten_and_busy_ones_kept() {
        let l = HostLimiter::new(1000, 4);
        let held = l.acquire("busy", None).await.unwrap();
        l.acquire("cooling", None)
            .await
            .unwrap()
            .release(SlotOutcome::RateLimited(MAX_RETRY_AFTER));
        let now = Instant::now();
        {
            let mut s = l.lock();
            for i in 0..REMEMBERED {
                s.host(&format!("h{i}"), now);
            }
        }
        assert!(l.remembered().0 >= REMEMBERED);
        // Twice the idle time later, the next new host makes room: the
        // host with a request in flight and the one cooling down stay.
        let later = now + IDLE_AFTER * 2;
        l.lock().host("new", later);
        assert_eq!(l.remembered().0, 3, "busy, cooling and the new one");
        assert!(!l.has_capacity("cooling", None));
        drop(held);
    }

    #[test]
    fn bare_hosts() {
        assert_eq!(bare_host("pds.example"), "pds.example");
        assert_eq!(bare_host("pds.example:2583"), "pds.example");
        assert_eq!(bare_host("[2001:db8::1]:443"), "[2001:db8::1]");
        assert_eq!(bare_host("[2001:db8::1]"), "[2001:db8::1]");
        assert_eq!(bare_host("127.0.0.1:8080"), "127.0.0.1");
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
        l.acquire("x", None)
            .await
            .unwrap()
            .release(SlotOutcome::RateLimited(Duration::from_secs(u64::MAX)));
        assert!(
            l.cooling("x")
                .is_some_and(|s| s <= MAX_RETRY_AFTER.as_secs())
        );
        assert!(l.has_capacity("y", None));
    }

    #[tokio::test]
    async fn a_dropped_slot_is_freed_and_is_not_a_failure() {
        let l = HostLimiter::new(100, 1);
        for _ in 0..(BREAKER_FAILURES * 2) {
            // A request whose future is dropped while it holds the slot.
            let slot = l.acquire("h", None).await.unwrap();
            assert!(!l.has_capacity("h", None));
            drop(slot);
            assert!(l.has_capacity("h", None));
        }
        assert_eq!(l.cooling("h"), None);
        // The same when the holder panics.
        let r = farsight_core::task::catch(async {
            let _slot = l.acquire("h", None).await.unwrap();
            if std::hint::black_box(true) {
                panic!("job failed");
            }
        })
        .await;
        assert!(r.is_err());
        assert!(l.has_capacity("h", None));
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
                l.release("h", None, Some(SlotOutcome::RateLimited(d)));
                let left = l.cooling("h");
                prop_assert!(left.is_none_or(|s| (1..=MAX_RETRY_AFTER.as_secs()).contains(&s)));
                prop_assert!(left.is_none_or(|s| s <= d.as_secs().max(1)));
                if d >= Duration::from_secs(2) {
                    prop_assert!(left.is_some() && !l.has_capacity("h", None));
                }
                prop_assert!(l.has_capacity("other", None));
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
