//! Token-bucket rate limits (see `docs/design/api.md`), keyed by
//! resolved client IP for anonymous callers and by token for
//! authenticated ones. An IPv6 caller draws on three buckets at once: its
//! /64 at the class's limit, its /48 at [`V6_48_FACTOR`] times that and
//! its /32 at [`V6_32_FACTOR`] times, so a routed prefix is not as many
//! callers as it has /64s. Buckets live in memory, at most
//! [`MAX_BUCKETS`]; settled ones are swept. The bucket arithmetic is
//! [`farsight_core::bucket`]; this module adds the keys, the classes,
//! what the headers report, and the count of requests each caller has in
//! flight ([`InFlight`]).

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use farsight_core::bucket::{Bucket, Rate};
use farsight_core::config::Config;

/// Rate-limit classes (also the `class` label of
/// `farsight_rate_limited_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    /// Anonymous read queries, per client IP.
    AnonRead,
    /// API-key read queries, per key.
    KeyRead,
    /// `requestBackfill` with the admin token.
    AdminBackfill,
    /// `requestBackfill` with an API key, per key.
    KeyBackfill,
    /// UI handle/DID lookups by anonymous visitors, per IP.
    UiLookup,
    /// UI sign-in attempts (starts, OAuth callbacks), per IP.
    UiLogin,
    /// UI sign-in starts: one bucket for the whole process. An address
    /// with a recent successful sign-in is not charged.
    UiLoginStart,
    /// Public UI page views, per IP (`public_ui.rate_limit_*`).
    PublicUi,
    /// Public UI handle resolutions: one bucket for the whole process.
    PublicHandle,
    /// Public UI profile-card requests, per IP.
    PublicCard,
    /// Public UI profile-card fetches: one bucket for the whole process
    /// (`public_ui.card_rps`, `public_ui.card_burst`).
    PublicCardBudget,
}

/// Admin sign-in flows the whole process may start per second.
pub const UI_LOGIN_START_RPS: f64 = 1.0;
/// Burst of the same budget.
pub const UI_LOGIN_START_BURST: f64 = 10.0;

/// Profile-card requests per second per client address. Cards have their
/// own class so that moving the pointer down a table does not spend the
/// visitor's page budget.
pub const PUBLIC_CARD_RPS: f64 = 2.0;
/// Burst of the same class.
pub const PUBLIC_CARD_BURST: f64 = 20.0;

impl Class {
    /// Metric label and policy name.
    pub fn label(self) -> &'static str {
        match self {
            Class::AnonRead => "anon_read",
            Class::KeyRead => "key_read",
            Class::AdminBackfill => "admin_backfill",
            Class::KeyBackfill => "key_backfill",
            Class::UiLookup => "ui_lookup",
            Class::UiLogin => "ui_login",
            Class::UiLoginStart => "ui_login_start",
            Class::PublicUi => "public_ui",
            Class::PublicHandle => "public_ui_handle",
            Class::PublicCard => "public_ui_card",
            Class::PublicCardBudget => "public_ui_card_budget",
        }
    }

    /// The class's limit from `[rate_limit]` and `[public_ui]`
    /// (`key_rps_override` for a key with its own `read_rps`).
    pub fn limit(self, config: &Config, key_rps_override: Option<f32>) -> Limit {
        let cfg = &config.rate_limit;
        match self {
            Class::AnonRead => Limit::new(f64::from(cfg.anon_rps), f64::from(cfg.anon_burst)),
            Class::KeyRead => {
                let rps = key_rps_override
                    .map(f64::from)
                    .filter(|r| *r > 0.0)
                    .unwrap_or(f64::from(cfg.key_rps));
                // A per-key override keeps the default rate-to-burst ratio.
                let burst = if key_rps_override.is_some() && cfg.key_rps > 0 {
                    (rps * f64::from(cfg.key_burst) / f64::from(cfg.key_rps)).max(1.0)
                } else {
                    f64::from(cfg.key_burst)
                };
                Limit::new(rps, burst)
            }
            Class::AdminBackfill => Limit::new(f64::from(cfg.admin_backfill_rps), 100.0),
            Class::KeyBackfill => Limit::new(f64::from(cfg.key_backfill_rps), 20.0),
            Class::UiLookup => Limit::new(f64::from(cfg.ui_lookup_rps), 5.0),
            Class::UiLogin => Limit::new(5.0 / 60.0, 5.0),
            Class::UiLoginStart => Limit::new(UI_LOGIN_START_RPS, UI_LOGIN_START_BURST),
            Class::PublicUi => Limit::new(
                f64::from(config.public_ui.rate_limit_rps),
                f64::from(config.public_ui.rate_limit_burst),
            ),
            // Process-wide: `public_ui.handle_rps`.
            Class::PublicHandle => Limit::new(
                f64::from(config.public_ui.handle_rps),
                f64::from(config.public_ui.handle_burst()),
            ),
            Class::PublicCard => Limit::new(PUBLIC_CARD_RPS, PUBLIC_CARD_BURST),
            Class::PublicCardBudget => Limit::new(
                f64::from(config.public_ui.card_rps),
                f64::from(config.public_ui.effective_card_burst()),
            ),
        }
    }
}

/// A sustained rate and a burst.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limit {
    /// Sustained requests per second. At 0 the bucket never refills, and
    /// a refused caller is told to retry in an hour.
    pub rate: f64,
    /// Bucket capacity: the requests a full bucket allows at once. At
    /// least 1.
    pub burst: f64,
}

impl Limit {
    /// A limit; burst is at least 1.
    pub fn new(rate: f64, burst: f64) -> Limit {
        Limit {
            rate: rate.max(0.0),
            burst: burst.max(1.0),
        }
    }

    fn bucket_rate(self) -> Rate {
        Rate {
            per_sec: self.rate,
            burst: self.burst,
        }
    }
}

/// What the `RateLimit-Policy` / `RateLimit` headers report.
#[derive(Debug, Clone, PartialEq)]
pub struct RateHeaders {
    /// The policy name both headers quote: [`Class::label`].
    pub class: &'static str,
    /// Quota (burst).
    pub quota: u64,
    /// Window in seconds (time to refill the whole burst).
    pub window: u64,
    /// Whole tokens left in the bucket after this request.
    pub remaining: u64,
    /// Seconds until the bucket is full again.
    pub reset: u64,
}

impl RateHeaders {
    /// `RateLimit-Policy: "<class>";q=<quota>;w=<window>`.
    pub fn policy(&self) -> String {
        format!("\"{}\";q={};w={}", self.class, self.quota, self.window)
    }

    /// `RateLimit: "<class>";r=<remaining>;t=<reset>`.
    pub fn state(&self) -> String {
        format!("\"{}\";r={};t={}", self.class, self.remaining, self.reset)
    }
}

/// Most buckets held at once. A new key beyond it first drops every
/// settled bucket, then the least recently used quarter.
pub const MAX_BUCKETS: usize = 100_000;
/// What an IPv6 /48 may do, as a multiple of one address's limit.
pub const V6_48_FACTOR: f64 = 4.0;
/// What an IPv6 /32 may do, as a multiple of one address's limit.
pub const V6_32_FACTOR: f64 = 16.0;

/// A bucket and the rate it was last used with, which decides when it
/// has nothing left to remember.
#[derive(Debug, Clone, Copy)]
struct Entry {
    bucket: Bucket,
    rate: Rate,
}

impl Entry {
    /// Whether the bucket would be full at `now`: dropping it changes
    /// nothing. A bucket that never refills (rate 0) is settled only
    /// while it is full.
    fn settled(&self, now: Instant) -> bool {
        self.bucket.tokens() + self.bucket.idle(now).as_secs_f64() * self.rate.per_sec
            >= self.rate.burst
    }
}

/// The process-wide limiter.
#[derive(Debug)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<(Class, String), Entry>>,
    cap: usize,
}

impl Default for RateLimiter {
    fn default() -> Self {
        RateLimiter::with_capacity(MAX_BUCKETS)
    }
}

/// The bucket key of an anonymous caller: the address, IPv6 by /64.
pub fn ip_key(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(a) => a.to_string(),
        IpAddr::V6(a) => {
            let s = a.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
        }
    }
}

/// The key under which an anonymous caller's requests in flight are
/// counted: the address, IPv6 by /48 (one routed site).
pub fn site_key(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(a) => a.to_string(),
        IpAddr::V6(a) => {
            let s = a.segments();
            format!("{:x}:{:x}:{:x}::/48", s[0], s[1], s[2])
        }
    }
}

/// The buckets an anonymous caller draws on, narrowest first, each with
/// the multiple of the class's limit it allows.
fn ip_buckets(ip: IpAddr) -> Vec<(String, f64)> {
    match ip {
        IpAddr::V4(_) => vec![(ip_key(ip), 1.0)],
        IpAddr::V6(a) => {
            let s = a.segments();
            vec![
                (ip_key(ip), 1.0),
                (site_key(ip), V6_48_FACTOR),
                (format!("{:x}:{:x}::/32", s[0], s[1]), V6_32_FACTOR),
            ]
        }
    }
}

fn ceil_secs(x: f64) -> u64 {
    if x.is_finite() && x > 0.0 {
        x.ceil() as u64
    } else {
        0
    }
}

fn window_of(limit: Limit) -> u64 {
    if limit.rate > 0.0 {
        ceil_secs(limit.burst / limit.rate)
    } else {
        0
    }
}

fn headers_of(class: Class, limit: Limit, b: &Bucket) -> RateHeaders {
    RateHeaders {
        class: class.label(),
        quota: limit.burst as u64,
        window: window_of(limit),
        remaining: b.tokens().max(0.0).floor() as u64,
        reset: if limit.rate > 0.0 {
            ceil_secs(b.secs_until_full(limit.bucket_rate()))
        } else {
            0
        },
    }
}

fn retry_of(limit: Limit, b: &Bucket) -> u64 {
    if limit.rate > 0.0 {
        ceil_secs(b.secs_until_token(limit.bucket_rate())).max(1)
    } else {
        3600
    }
}

type Buckets = HashMap<(Class, String), Entry>;

impl RateLimiter {
    /// A limiter that holds at most `cap` buckets.
    pub fn with_capacity(cap: usize) -> RateLimiter {
        RateLimiter {
            buckets: Mutex::new(HashMap::new()),
            cap: cap.max(1),
        }
    }

    /// Buckets held now.
    pub fn len(&self) -> usize {
        self.buckets.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether no bucket is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Makes room for one more bucket when the map is at its bound:
    /// settled buckets go first (they hold nothing), then the quarter
    /// used longest ago. One pass over the map frees many places, so the
    /// cost per new key stays small.
    fn make_room(&self, map: &mut Buckets, now: Instant) {
        if map.len() < self.cap {
            return;
        }
        map.retain(|_, e| !e.settled(now));
        if map.len() < self.cap {
            return;
        }
        let mut idle: Vec<Duration> = map.values().map(|e| e.bucket.idle(now)).collect();
        let keep = idle.len() - idle.len().div_ceil(4);
        let at = keep.min(idle.len() - 1);
        let (_, cut, _) = idle.select_nth_unstable(at);
        let cut = *cut;
        let mut over = map.len().saturating_sub(keep);
        // Ties at the cut are dropped only as far as needed.
        map.retain(|_, e| {
            let i = e.bucket.idle(now);
            if i > cut || (i == cut && over > 0) {
                over = over.saturating_sub(1);
                false
            } else {
                true
            }
        });
    }

    /// The bucket of `(class, key)`, refilled at `now` under `limit`;
    /// made full if it does not exist.
    fn refilled<'a>(
        &self,
        map: &'a mut Buckets,
        class: Class,
        key: &str,
        limit: Limit,
        now: Instant,
    ) -> &'a mut Bucket {
        let rate = limit.bucket_rate();
        let k = (class, key.to_owned());
        if !map.contains_key(&k) {
            self.make_room(map, now);
        }
        let e = map.entry(k).or_insert_with(|| Entry {
            bucket: Bucket::full(rate, now),
            rate,
        });
        e.rate = rate;
        e.bucket.refill(now, rate);
        &mut e.bucket
    }

    /// Takes one token from `(class, key)`. `Ok` carries the headers to
    /// report; `Err` carries them plus `Retry-After` seconds.
    pub fn check(
        &self,
        class: Class,
        key: &str,
        limit: Limit,
    ) -> Result<RateHeaders, (RateHeaders, u64)> {
        self.check_at(class, key, limit, Instant::now())
    }

    fn check_at(
        &self,
        class: Class,
        key: &str,
        limit: Limit,
        now: Instant,
    ) -> Result<RateHeaders, (RateHeaders, u64)> {
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let b = self.refilled(&mut map, class, key, limit, now);
        if b.try_take() {
            Ok(headers_of(class, limit, b))
        } else {
            Err((headers_of(class, limit, b), retry_of(limit, b)))
        }
    }

    /// Takes one token for a caller known by address. An IPv4 address
    /// has one bucket. An IPv6 address draws on its /64, its /48 and its
    /// /32 together: the request is admitted only if each has a token,
    /// and then takes one from each. `Ok` carries the /64's headers;
    /// `Err` those of the narrowest bucket that refused, with its wait.
    pub fn check_ip(
        &self,
        class: Class,
        ip: IpAddr,
        limit: Limit,
    ) -> Result<RateHeaders, (RateHeaders, u64)> {
        self.check_ip_at(class, ip, limit, Instant::now())
    }

    fn check_ip_at(
        &self,
        class: Class,
        ip: IpAddr,
        limit: Limit,
        now: Instant,
    ) -> Result<RateHeaders, (RateHeaders, u64)> {
        let buckets = ip_buckets(ip);
        let scaled = |factor: f64| Limit::new(limit.rate * factor, limit.burst * factor);
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        for (key, factor) in &buckets {
            let l = scaled(*factor);
            let b = self.refilled(&mut map, class, key, l, now);
            if b.tokens() < 1.0 {
                return Err((headers_of(class, l, b), retry_of(l, b)));
            }
        }
        let mut own = None;
        for (key, factor) in &buckets {
            let l = scaled(*factor);
            let b = self.refilled(&mut map, class, key, l, now);
            b.try_take();
            if own.is_none() {
                own = Some(headers_of(class, l, b));
            }
        }
        Ok(
            own.unwrap_or_else(|| {
                headers_of(class, limit, &Bucket::full(limit.bucket_rate(), now))
            }),
        )
    }

    /// Takes one token from `(class, key)` only if the bucket holds at
    /// least `reserve + 1`: a background user of a budget leaves `reserve`
    /// tokens for requests (handle warming).
    pub fn take_above(&self, class: Class, key: &str, limit: Limit, reserve: f64) -> bool {
        self.take_above_at(class, key, limit, reserve, Instant::now())
    }

    fn take_above_at(
        &self,
        class: Class,
        key: &str,
        limit: Limit,
        reserve: f64,
        now: Instant,
    ) -> bool {
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        self.refilled(&mut map, class, key, limit, now)
            .try_take_above(reserve)
    }

    /// Drops buckets that have been idle for longer than `idle` and
    /// would be full by now: such a bucket carries no state. One that is
    /// not full is kept however long it was idle: dropping it would hand
    /// its caller a full one, which at a zero rate is a refill the limit
    /// does not allow.
    pub fn sweep(&self, idle: Duration) {
        self.sweep_at(idle, Instant::now());
    }

    fn sweep_at(&self, idle: Duration, now: Instant) {
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, e| !(e.bucket.idle(now) >= idle && e.settled(now)));
    }
}

type Places = std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<tokio::sync::Semaphore>>>>;

/// Requests in flight per caller. A caller holds a place from the moment
/// it is admitted until its [`Flight`] is dropped, and has a bounded
/// number of places, so it cannot occupy every query slot: its further
/// requests wait for one of its own places ([`InFlight::enter`]) or are
/// turned away ([`InFlight::try_enter`]). The map has an entry only for
/// a caller with a request in flight or waiting. A caller's bound is
/// the one given when its entry is made.
#[derive(Debug, Default)]
pub struct InFlight {
    places: Places,
}

/// One request's place in an [`InFlight`] count; dropping it gives the
/// place back.
#[derive(Debug)]
pub struct Flight {
    places: Places,
    key: String,
    place: Option<(
        std::sync::Arc<tokio::sync::Semaphore>,
        tokio::sync::OwnedSemaphorePermit,
    )>,
}

impl InFlight {
    fn places_of(&self, key: &str, max: usize) -> std::sync::Arc<tokio::sync::Semaphore> {
        self.places
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key.to_owned())
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Semaphore::new(max.max(1))))
            .clone()
    }

    fn flight(
        &self,
        key: &str,
        sem: std::sync::Arc<tokio::sync::Semaphore>,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Option<Flight> {
        // Made whether or not a place was got: dropping it is what
        // removes an entry nobody holds.
        let mut flight = Flight {
            places: self.places.clone(),
            key: key.to_owned(),
            place: None,
        };
        match permit {
            Some(p) => {
                flight.place = Some((sem, p));
                Some(flight)
            }
            None => {
                drop(sem);
                None
            }
        }
    }

    /// Takes a place for `key`, waiting up to `wait` while it holds
    /// `max`. `None` when none came free in that time.
    pub async fn enter(&self, key: &str, max: usize, wait: Duration) -> Option<Flight> {
        let sem = self.places_of(key, max);
        let permit = tokio::time::timeout(wait, sem.clone().acquire_owned())
            .await
            .ok()
            .and_then(Result::ok);
        self.flight(key, sem, permit)
    }

    /// Takes a place for `key` unless it already holds `max`.
    pub fn try_enter(&self, key: &str, max: usize) -> Option<Flight> {
        let sem = self.places_of(key, max);
        let permit = sem.clone().try_acquire_owned().ok();
        self.flight(key, sem, permit)
    }

    /// Callers with an entry now.
    pub fn callers(&self) -> usize {
        self.places.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        // Give the place back first, then forget the caller if nothing
        // of it is left: the map's own reference is the only one.
        let held = self.place.take();
        let mut m = self.places.lock().unwrap_or_else(|e| e.into_inner());
        drop(held);
        if m.get(&self.key)
            .is_some_and(|s| std::sync::Arc::strong_count(s) == 1)
        {
            m.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_then_refuse_then_refill() {
        let l = RateLimiter::default();
        let lim = Limit::new(10.0, 50.0);
        let t0 = Instant::now();
        for i in 0..50 {
            let h = l.check_at(Class::AnonRead, "1.2.3.4", lim, t0).unwrap();
            assert_eq!(h.remaining, 49 - i);
        }
        let (h, retry) = l.check_at(Class::AnonRead, "1.2.3.4", lim, t0).unwrap_err();
        assert_eq!((h.remaining, retry), (0, 1));
        // Another key is independent.
        assert!(l.check_at(Class::AnonRead, "5.6.7.8", lim, t0).is_ok());
        // 0.1 s later one token is back.
        let t1 = t0 + Duration::from_millis(100);
        assert!(l.check_at(Class::AnonRead, "1.2.3.4", lim, t1).is_ok());
        assert!(l.check_at(Class::AnonRead, "1.2.3.4", lim, t1).is_err());
    }

    #[test]
    fn a_background_taker_leaves_the_reserve() {
        let l = RateLimiter::default();
        let lim = Limit::new(2.0, 10.0);
        let t0 = Instant::now();
        // From a full bucket of 10 with a reserve of 5: five, then none.
        for _ in 0..5 {
            assert!(l.take_above_at(Class::PublicHandle, "p", lim, 5.0, t0));
        }
        assert!(!l.take_above_at(Class::PublicHandle, "p", lim, 5.0, t0));
        // The five left are there for requests.
        for _ in 0..5 {
            assert!(l.check_at(Class::PublicHandle, "p", lim, t0).is_ok());
        }
        assert!(l.check_at(Class::PublicHandle, "p", lim, t0).is_err());
        // Refilled to 6 after three seconds: one more for the taker.
        let t1 = t0 + Duration::from_secs(3);
        assert!(l.take_above_at(Class::PublicHandle, "p", lim, 5.0, t1));
        assert!(!l.take_above_at(Class::PublicHandle, "p", lim, 5.0, t1));
    }

    #[test]
    fn headers_follow_the_bucket() {
        let l = RateLimiter::default();
        let lim = Limit::new(2.0, 5.0);
        let t0 = Instant::now();
        let h = l.check_at(Class::KeyRead, "k", lim, t0).unwrap();
        // Quota 5 refilled in ceil(5 / 2) s; one spent is back in 1 s.
        assert_eq!((h.quota, h.window, h.remaining, h.reset), (5, 3, 4, 1));
        for _ in 0..4 {
            l.check_at(Class::KeyRead, "k", lim, t0).unwrap();
        }
        let (h, retry) = l.check_at(Class::KeyRead, "k", lim, t0).unwrap_err();
        assert_eq!((h.remaining, h.reset, retry), (0, 3, 1));
        // 0.25 s later half a token is there: still refused, and the
        // wait is never reported as zero.
        let t1 = t0 + Duration::from_millis(250);
        let (h, retry) = l.check_at(Class::KeyRead, "k", lim, t1).unwrap_err();
        assert_eq!((h.remaining, h.reset, retry), (0, 3, 1));
    }

    #[test]
    fn a_zero_rate_spends_the_burst_and_then_refuses_for_an_hour() {
        let l = RateLimiter::default();
        let lim = Limit::new(0.0, 2.0);
        let t0 = Instant::now();
        let h = l.check_at(Class::KeyBackfill, "k", lim, t0).unwrap();
        assert_eq!((h.quota, h.window, h.remaining, h.reset), (2, 0, 1, 0));
        l.check_at(Class::KeyBackfill, "k", lim, t0).unwrap();
        let later = t0 + Duration::from_secs(86_400);
        let (h, retry) = l.check_at(Class::KeyBackfill, "k", lim, later).unwrap_err();
        assert_eq!((h.remaining, h.reset, retry), (0, 0, 3600));
    }

    #[test]
    fn a_changed_limit_applies_to_a_bucket_that_exists() {
        let l = RateLimiter::default();
        let t0 = Instant::now();
        let h = l
            .check_at(Class::PublicUi, "a", Limit::new(1.0, 20.0), t0)
            .unwrap();
        assert_eq!(h.remaining, 19);
        // A smaller burst cuts the tokens down at the next call.
        let h = l
            .check_at(Class::PublicUi, "a", Limit::new(1.0, 3.0), t0)
            .unwrap();
        assert_eq!((h.quota, h.remaining), (3, 2));
        // A larger one is filled only at the rate.
        let h = l
            .check_at(Class::PublicUi, "a", Limit::new(1.0, 20.0), t0)
            .unwrap();
        assert_eq!((h.quota, h.remaining, h.reset), (20, 1, 19));
    }

    #[test]
    fn a_caller_with_an_earlier_clock_gains_nothing() {
        let l = RateLimiter::default();
        let lim = Limit::new(1.0, 1.0);
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(10);
        assert!(l.check_at(Class::UiLogin, "a", lim, t1).is_ok());
        assert!(l.check_at(Class::UiLogin, "a", lim, t0).is_err());
    }

    #[test]
    fn public_ui_limits_come_from_config() {
        let mut c = Config::default();
        assert_eq!(Class::PublicUi.limit(&c, None), Limit::new(5.0, 20.0));
        c.public_ui.rate_limit_rps = 2;
        c.public_ui.rate_limit_burst = 3;
        assert_eq!(Class::PublicUi.limit(&c, None), Limit::new(2.0, 3.0));
        assert_eq!(Class::PublicHandle.limit(&c, None), Limit::new(20.0, 20.0));
        c.public_ui.handle_rps = 2;
        assert_eq!(Class::PublicHandle.limit(&c, None), Limit::new(2.0, 10.0));
        assert_eq!(Class::UiLookup.limit(&c, None), Limit::new(1.0, 5.0));
        assert_eq!(Class::PublicUi.label(), "public_ui");
        assert_eq!(Class::PublicCard.limit(&c, None), Limit::new(2.0, 20.0));
        assert_eq!(
            Class::PublicCardBudget.limit(&c, None),
            Limit::new(4.0, 8.0)
        );
        c.public_ui.card_rps = 6;
        c.public_ui.card_burst = 2;
        // A burst below the rate is raised to it.
        assert_eq!(
            Class::PublicCardBudget.limit(&c, None),
            Limit::new(6.0, 6.0)
        );
    }

    #[test]
    fn headers_format() {
        let h = RateHeaders {
            class: "anon_read",
            quota: 50,
            window: 5,
            remaining: 49,
            reset: 1,
        };
        assert_eq!(h.policy(), "\"anon_read\";q=50;w=5");
        assert_eq!(h.state(), "\"anon_read\";r=49;t=1");
    }

    #[test]
    fn an_ipv6_prefix_is_not_as_many_callers_as_it_has_64s() {
        let l = RateLimiter::default();
        let lim = Limit::new(0.0, 5.0);
        let t0 = Instant::now();
        let addr = |site: u16, net: u16| -> IpAddr {
            format!("2001:db8:{site:x}:{net:x}::1").parse().unwrap()
        };
        // One /64 has the class's own limit.
        for _ in 0..5 {
            assert!(l.check_ip_at(Class::AnonRead, addr(1, 1), lim, t0).is_ok());
        }
        let (h, retry) = l
            .check_ip_at(Class::AnonRead, addr(1, 1), lim, t0)
            .unwrap_err();
        assert_eq!((h.quota, h.remaining, retry), (5, 0, 3600));
        // The /48 allows four times that, however many /64s ask: 15 more.
        let mut admitted = 0;
        for net in 2..200 {
            if l.check_ip_at(Class::AnonRead, addr(1, net), lim, t0)
                .is_ok()
            {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 15);
        // A refusal by the /48 takes nothing from the /64 that asked.
        let (h, _) = l
            .check_ip_at(Class::AnonRead, addr(1, 500), lim, t0)
            .unwrap_err();
        assert_eq!(h.quota, 20);
        // The /32 allows sixteen times: 80 in all, 20 of them spent.
        let mut admitted = 0;
        for site in 2..400 {
            if l.check_ip_at(Class::AnonRead, addr(site, 1), lim, t0)
                .is_ok()
            {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 60);
        // Another /32, and IPv4, are untouched.
        assert!(
            l.check_ip_at(Class::AnonRead, "2001:db9::1".parse().unwrap(), lim, t0)
                .is_ok()
        );
        let v4: IpAddr = "192.0.2.7".parse().unwrap();
        for _ in 0..5 {
            assert!(l.check_ip_at(Class::AnonRead, v4, lim, t0).is_ok());
        }
        assert!(l.check_ip_at(Class::AnonRead, v4, lim, t0).is_err());
    }

    #[test]
    fn the_bucket_map_is_bounded() {
        let l = RateLimiter::with_capacity(100);
        let lim = Limit::new(0.0, 2.0);
        let t0 = Instant::now();
        // 100 buckets that are not full, used one millisecond apart.
        for i in 0..100u64 {
            let t = t0 + Duration::from_millis(i);
            assert!(
                l.check_at(Class::AnonRead, &format!("k{i}"), lim, t)
                    .is_ok()
            );
        }
        assert_eq!(l.len(), 100);
        // One more key: the quarter used longest ago goes.
        let t1 = t0 + Duration::from_secs(1);
        assert!(l.check_at(Class::AnonRead, "new", lim, t1).is_ok());
        assert_eq!(l.len(), 76);
        let map = l.buckets.lock().unwrap();
        assert!(!map.contains_key(&(Class::AnonRead, "k0".to_owned())));
        assert!(!map.contains_key(&(Class::AnonRead, "k24".to_owned())));
        assert!(map.contains_key(&(Class::AnonRead, "k25".to_owned())));
        assert!(map.contains_key(&(Class::AnonRead, "k99".to_owned())));
        drop(map);
        // Settled buckets go before any that holds state.
        let l = RateLimiter::with_capacity(10);
        let fast = Limit::new(100.0, 2.0);
        for i in 0..9 {
            assert!(
                l.check_at(Class::PublicUi, &format!("f{i}"), fast, t0)
                    .is_ok()
            );
        }
        assert!(l.check_at(Class::AnonRead, "slow", lim, t0).is_ok());
        assert!(l.check_at(Class::AnonRead, "next", lim, t1).is_ok());
        assert_eq!(l.len(), 2);
    }

    #[test]
    fn the_sweep_keeps_a_bucket_that_is_not_full() {
        let l = RateLimiter::default();
        let t0 = Instant::now();
        let never = Limit::new(0.0, 1.0);
        assert!(l.check_at(Class::KeyBackfill, "spent", never, t0).is_ok());
        assert!(
            l.check_at(Class::AnonRead, "refills", Limit::new(10.0, 50.0), t0)
                .is_ok()
        );
        let later = t0 + Duration::from_secs(3600);
        l.sweep_at(Duration::from_secs(600), later);
        // The zero-rate bucket is still spent; the other one is gone.
        assert_eq!(l.len(), 1);
        assert!(
            l.check_at(Class::KeyBackfill, "spent", never, later)
                .is_err()
        );
        // Not idle long enough: kept even though it is full again.
        assert!(
            l.check_at(Class::AnonRead, "recent", Limit::new(10.0, 50.0), later)
                .is_ok()
        );
        l.sweep_at(Duration::from_secs(600), later + Duration::from_secs(60));
        assert_eq!(l.len(), 2);
    }

    #[tokio::test]
    async fn one_caller_holds_a_bounded_number_of_places() {
        let f = InFlight::default();
        let short = Duration::from_millis(20);
        let a = f.try_enter("a", 2).unwrap();
        let _b = f.try_enter("a", 2).unwrap();
        assert!(f.try_enter("a", 2).is_none());
        // A request that waits gets a place when one comes free, and
        // none if none does.
        assert!(f.enter("a", 2, short).await.is_none());
        let (waited, ()) = tokio::join!(f.enter("a", 2, Duration::from_secs(5)), async {
            tokio::time::sleep(short).await;
            drop(a);
        });
        assert!(waited.is_some());
        // Another caller has its own places.
        let c = f.try_enter("b", 2).unwrap();
        assert_eq!(f.callers(), 2);
        drop(c);
        assert_eq!(f.callers(), 1);
        drop(waited);
        drop(_b);
        // Nothing in flight, nothing remembered.
        assert_eq!(f.callers(), 0);
        // A bound of zero still admits one.
        assert!(f.try_enter("z", 0).is_some());
        assert_eq!(f.callers(), 0);
    }

    #[test]
    fn in_flight_keys() {
        assert_eq!(site_key("9.8.7.6".parse().unwrap()), "9.8.7.6");
        assert_eq!(
            site_key("2001:db8:1:2:3:4:5:6".parse().unwrap()),
            "2001:db8:1::/48"
        );
    }

    #[test]
    fn ipv6_by_64() {
        let a: IpAddr = "2001:db8:1:2:3:4:5:6".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:ffff::1".parse().unwrap();
        assert_eq!(ip_key(a), ip_key(b));
        assert_eq!(ip_key("9.8.7.6".parse().unwrap()), "9.8.7.6");
    }
}
