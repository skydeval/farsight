//! Token-bucket rate limits (see `docs/design/api.md`), keyed by
//! resolved client IP (IPv6 by /64) for anonymous callers and by token
//! for authenticated ones. Buckets live in memory; idle ones are swept.
//! The bucket arithmetic is [`farsight_core::bucket`]; this module adds
//! the keys, the classes and what the headers report.

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

/// The process-wide limiter.
#[derive(Debug, Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<(Class, String), Bucket>>,
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

fn ceil_secs(x: f64) -> u64 {
    if x.is_finite() && x > 0.0 {
        x.ceil() as u64
    } else {
        0
    }
}

impl RateLimiter {
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
        let rate = limit.bucket_rate();
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let b = map
            .entry((class, key.to_owned()))
            .or_insert_with(|| Bucket::full(rate, now));
        b.refill(now, rate);
        let window = if limit.rate > 0.0 {
            ceil_secs(limit.burst / limit.rate)
        } else {
            0
        };
        let headers = |b: &Bucket| RateHeaders {
            class: class.label(),
            quota: limit.burst as u64,
            window,
            remaining: b.tokens().max(0.0).floor() as u64,
            reset: if limit.rate > 0.0 {
                ceil_secs(b.secs_until_full(rate))
            } else {
                0
            },
        };
        if b.try_take() {
            Ok(headers(b))
        } else {
            let retry = if limit.rate > 0.0 {
                ceil_secs(b.secs_until_token(rate)).max(1)
            } else {
                3600
            };
            Err((headers(b), retry))
        }
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
        let rate = limit.bucket_rate();
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        map.entry((class, key.to_owned()))
            .or_insert_with(|| Bucket::full(rate, now))
            .take_above(now, rate, reserve)
    }

    /// Drops buckets idle for longer than `idle` (a full bucket carries no
    /// state worth keeping).
    pub fn sweep(&self, idle: Duration) {
        let now = Instant::now();
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, b| b.idle(now) < idle);
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
    fn ipv6_by_64() {
        let a: IpAddr = "2001:db8:1:2:3:4:5:6".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:ffff::1".parse().unwrap();
        assert_eq!(ip_key(a), ip_key(b));
        assert_eq!(ip_key("9.8.7.6".parse().unwrap()), "9.8.7.6");
    }
}
