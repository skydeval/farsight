//! What the API has been asked since the server started, per endpoint,
//! for the dashboard. In memory: a restart begins again from nothing.
//! The same requests are counted for Prometheus in [`crate::metrics`];
//! these counters exist so that a page can read them back.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use chrono::{DateTime, Utc};

use crate::Endpoint;

#[derive(Debug, Default)]
struct Counters {
    requests: AtomicU64,
    errors: AtomicU64,
    limited: AtomicU64,
    /// Unix seconds of the last request; 0 = never.
    last: AtomicI64,
}

/// One endpoint's usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointUsage {
    /// The endpoint.
    pub endpoint: Endpoint,
    /// Requests answered, whatever the status.
    pub requests: u64,
    /// Of them, answered with an error other than a rate limit.
    pub errors: u64,
    /// Of them, refused by a rate limit (429).
    pub limited: u64,
    /// When the last one was answered.
    pub last: Option<DateTime<Utc>>,
}

/// The counters, one set per endpoint.
#[derive(Debug)]
pub struct Usage {
    /// When counting began.
    pub since: DateTime<Utc>,
    per: Vec<Counters>,
}

impl Default for Usage {
    fn default() -> Self {
        Usage {
            since: Utc::now(),
            per: Endpoint::ALL.iter().map(|_| Counters::default()).collect(),
        }
    }
}

impl Usage {
    /// Counts one answered request.
    pub fn record(&self, endpoint: Endpoint, status: u16) {
        let Some(c) = Endpoint::ALL
            .iter()
            .position(|e| *e == endpoint)
            .and_then(|i| self.per.get(i))
        else {
            return;
        };
        c.requests.fetch_add(1, Ordering::Relaxed);
        if status == 429 {
            c.limited.fetch_add(1, Ordering::Relaxed);
        } else if status >= 400 {
            c.errors.fetch_add(1, Ordering::Relaxed);
        }
        c.last.store(Utc::now().timestamp(), Ordering::Relaxed);
    }

    /// Every endpoint's usage, in the order of [`Endpoint::ALL`].
    pub fn snapshot(&self) -> Vec<EndpointUsage> {
        Endpoint::ALL
            .iter()
            .zip(&self.per)
            .map(|(e, c)| {
                let last = c.last.load(Ordering::Relaxed);
                EndpointUsage {
                    endpoint: *e,
                    requests: c.requests.load(Ordering::Relaxed),
                    errors: c.errors.load(Ordering::Relaxed),
                    limited: c.limited.load(Ordering::Relaxed),
                    last: (last > 0)
                        .then(|| DateTime::from_timestamp(last, 0))
                        .flatten(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_errors_and_rate_limits_are_counted_apart() {
        let u = Usage::default();
        u.record(Endpoint::CheckBlocks, 200);
        u.record(Endpoint::CheckBlocks, 400);
        u.record(Endpoint::CheckBlocks, 429);
        let all = u.snapshot();
        assert_eq!(all.len(), Endpoint::ALL.len());
        let used = all
            .iter()
            .find(|e| e.endpoint == Endpoint::CheckBlocks)
            .unwrap();
        assert_eq!((used.requests, used.errors, used.limited), (3, 1, 1));
        assert!(used.last.is_some());
        let unused = all
            .iter()
            .find(|e| e.endpoint == Endpoint::GetStats)
            .unwrap();
        assert_eq!((unused.requests, unused.last), (0, None));
    }
}
