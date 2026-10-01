//! Token-bucket rate limits (design §3.6), keyed by resolved client IP
//! (IPv6 by /64) for anonymous callers and by token for authenticated
//! ones. Buckets live in memory; idle ones are swept.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use farsight_core::config::RateLimitConfig;

/// Rate-limit classes of §3.6 (also the `class` label of
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
    /// UI login attempts, per IP.
    UiLogin,
}

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
        }
    }

    /// The class's limit from `[rate_limit]` (`key_rps_override` for a key
    /// with its own `read_rps`).
    pub fn limit(self, cfg: &RateLimitConfig, key_rps_override: Option<f32>) -> Limit {
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
        }
    }
}

/// A sustained rate and a burst.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limit {
    /// Tokens per second.
    pub rate: f64,
    /// Bucket size.
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
}

/// What the `RateLimit-Policy` / `RateLimit` headers report.
#[derive(Debug, Clone, PartialEq)]
pub struct RateHeaders {
    /// Policy name.
    pub class: &'static str,
    /// Quota (burst).
    pub quota: u64,
    /// Window in seconds (time to refill the whole burst).
    pub window: u64,
    /// Remaining requests.
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

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    last: Instant,
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
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let b = map.entry((class, key.to_owned())).or_insert(Bucket {
            tokens: limit.burst,
            last: now,
        });
        let elapsed = now.saturating_duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * limit.rate).min(limit.burst);
        b.last = now;
        let window = if limit.rate > 0.0 {
            ceil_secs(limit.burst / limit.rate)
        } else {
            0
        };
        let headers = |tokens: f64| RateHeaders {
            class: class.label(),
            quota: limit.burst as u64,
            window,
            remaining: tokens.max(0.0).floor() as u64,
            reset: if limit.rate > 0.0 {
                ceil_secs((limit.burst - tokens) / limit.rate)
            } else {
                0
            },
        };
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Ok(headers(b.tokens))
        } else {
            let retry = if limit.rate > 0.0 {
                ceil_secs((1.0 - b.tokens) / limit.rate).max(1)
            } else {
                3600
            };
            Err((headers(b.tokens), retry))
        }
    }

    /// Drops buckets idle for longer than `idle` (a full bucket carries no
    /// state worth keeping).
    pub fn sweep(&self, idle: Duration) {
        let now = Instant::now();
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, b| now.saturating_duration_since(b.last) < idle);
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
