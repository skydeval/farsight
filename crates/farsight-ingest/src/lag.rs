//! Instance lag and source lag over the last 1000 commit events (see
//! `docs/design/firehose.md` and `docs/design/coverage.md`).
//!
//! - **Instance lag** (`lag_A`, used for failover rewinds) = median of
//!   `witness − rev commit time`.
//! - **Source lag** (`sourceLagSeconds`) = now − median rev commit time.
//!
//! The rev commit time is the timestamp inside the commit's TID rev.

use std::collections::VecDeque;
use std::time::Duration;

/// Events kept: the lags are taken over an instance's last 1000 events.
pub const WINDOW: usize = 1000;

/// Trailing window of (witness µs, rev µs) pairs.
#[derive(Debug, Clone, Default)]
pub struct LagTracker {
    samples: VecDeque<(i64, i64)>,
}

fn median(mut v: Vec<i64>) -> Option<i64> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    Some(v[v.len() / 2])
}

impl LagTracker {
    /// An empty tracker.
    pub fn new() -> LagTracker {
        LagTracker::default()
    }

    /// Records one commit event.
    pub fn record(&mut self, witness_us: i64, rev_us: i64) {
        if self.samples.len() == WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back((witness_us, rev_us));
    }

    /// Samples held.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// True if no sample was recorded.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Median `witness − rev time`, clamped at zero; `None` if empty.
    pub fn instance_lag(&self) -> Option<Duration> {
        median(
            self.samples
                .iter()
                .map(|(w, r)| w.saturating_sub(*r))
                .collect(),
        )
        .map(|d| Duration::from_micros(u64::try_from(d).unwrap_or(0)))
    }

    /// `now − median rev time`, in seconds; `None` if empty.
    pub fn source_lag_seconds(&self, now_us: i64) -> Option<f64> {
        median(self.samples.iter().map(|(_, r)| *r).collect())
            .map(|r| ((now_us - r) as f64 / 1e6).max(0.0))
    }

    /// Forgets all samples (a new instance starts its own window).
    pub fn reset(&mut self) {
        self.samples.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn medians_over_a_bounded_window() {
        let mut t = LagTracker::new();
        assert_eq!(t.instance_lag(), None);
        for i in 0..1500i64 {
            // witness 2 s after the rev, except a few outliers.
            let lag = if i % 100 == 0 { 600_000_000 } else { 2_000_000 };
            t.record(1_000_000_000 + i, 1_000_000_000 + i - lag);
        }
        assert_eq!(t.len(), WINDOW);
        assert_eq!(t.instance_lag(), Some(Duration::from_secs(2)));
        let s = t
            .source_lag_seconds(1_000_000_000 + 1500 + 10_000_000)
            .unwrap();
        assert!((s - 12.0).abs() < 0.01, "{s}");
        // A clock-skewed rev in the future clamps at zero.
        let mut t = LagTracker::new();
        t.record(0, 5_000_000);
        assert_eq!(t.instance_lag(), Some(Duration::ZERO));
    }
}
