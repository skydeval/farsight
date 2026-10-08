//! Backfill metrics (see `docs/design/operations.md`), exported by
//! `farsight-backfill` on `metrics.backfill_bind`. Host labels are
//! bounded: the top 50 hosts by request volume keep their name, every
//! other host is `other` (≤ 51 values); series of hosts that fall out of
//! the top 50 expire after an idle period (exporter idle timeout).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

/// `farsight_backfill_queue_depth{tier}`.
pub const QUEUE_DEPTH: &str = "farsight_backfill_queue_depth";
/// `farsight_backfill_repos_total{tier,outcome}`.
pub const REPOS_TOTAL: &str = "farsight_backfill_repos_total";
/// `farsight_backfill_repos_per_hour`.
pub const REPOS_PER_HOUR: &str = "farsight_backfill_repos_per_hour";
/// `farsight_backfill_sweep_progress_ratio{cycle_kind}`.
pub const SWEEP_PROGRESS: &str = "farsight_backfill_sweep_progress_ratio";
/// `farsight_backfill_sweep_eta_seconds`.
pub const SWEEP_ETA: &str = "farsight_backfill_sweep_eta_seconds";
/// `farsight_backfill_pds_request_seconds{host,method}`.
pub const PDS_REQUEST_SECONDS: &str = "farsight_backfill_pds_request_seconds";
/// `farsight_backfill_pds_errors_total{host,kind}`.
pub const PDS_ERRORS: &str = "farsight_backfill_pds_errors_total";
/// `farsight_backfill_plc_request_seconds`.
pub const PLC_REQUEST_SECONDS: &str = "farsight_backfill_plc_request_seconds";
/// `farsight_abuse_capped_total{kind}`.
pub const ABUSE_CAPPED: &str = "farsight_abuse_capped_total";

/// Every backfill metric.
pub const ALL: [&str; 9] = [
    QUEUE_DEPTH,
    REPOS_TOTAL,
    REPOS_PER_HOUR,
    SWEEP_PROGRESS,
    SWEEP_ETA,
    PDS_REQUEST_SECONDS,
    PDS_ERRORS,
    PLC_REQUEST_SECONDS,
    ABUSE_CAPPED,
];

/// Named hosts at most (the rest are `other`).
pub const TOP_HOSTS: usize = 50;
const RECOMPUTE_EVERY: u64 = 1000;

struct HostVolumes {
    counts: HashMap<String, u64>,
    top: HashSet<String>,
    since: u64,
}

static VOLUMES: Mutex<Option<HostVolumes>> = Mutex::new(None);

/// Most hosts whose request counts are kept. The counts exist to pick
/// the [`TOP_HOSTS`] busiest; a host far down the list has no chance of
/// being one of them. When the table is full, the less busy half goes.
pub const MAX_TRACKED_HOSTS: usize = 4096;

impl HostVolumes {
    fn new() -> HostVolumes {
        HostVolumes {
            counts: HashMap::new(),
            top: HashSet::new(),
            since: 0,
        }
    }

    /// The hosts by request count, busiest first.
    fn ranked(&self) -> Vec<(String, u64)> {
        let mut all: Vec<(String, u64)> =
            self.counts.iter().map(|(h, n)| (h.clone(), *n)).collect();
        all.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        all
    }

    /// Counts one request to `host` and returns its label.
    fn label(&mut self, host: &str) -> String {
        // A host not seen before, with the table full: keep the busier
        // half. A host dropped here starts again from zero if it comes
        // back, which costs it nothing unless it was about to be one of
        // the busiest, and those are in the half that stays.
        if self.counts.len() >= MAX_TRACKED_HOSTS && !self.counts.contains_key(host) {
            let keep: HashSet<String> = self
                .ranked()
                .into_iter()
                .take(MAX_TRACKED_HOSTS / 2)
                .map(|(h, _)| h)
                .collect();
            self.counts.retain(|h, _| keep.contains(h));
        }
        *self.counts.entry(host.to_owned()).or_insert(0) += 1;
        self.since += 1;
        if self.top.len() < TOP_HOSTS && !self.top.contains(host) {
            self.top.insert(host.to_owned());
        } else if self.since >= RECOMPUTE_EVERY {
            self.since = 0;
            self.top = self
                .ranked()
                .into_iter()
                .take(TOP_HOSTS)
                .map(|(h, _)| h)
                .collect();
        }
        if self.top.contains(host) {
            host.to_owned()
        } else {
            "other".to_owned()
        }
    }
}

/// The `host` label for a request to `host`, counting its volume.
pub fn host_label(host: &str) -> String {
    VOLUMES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(HostVolumes::new)
        .label(host)
}

/// Registers series at zero so they are visible before first use.
pub fn register() {
    for t in ["1", "2", "3"] {
        metrics::gauge!(QUEUE_DEPTH, "tier" => t).set(0.0);
        for o in ["clean", "complete_with_debts", "inactive", "failed"] {
            metrics::counter!(REPOS_TOTAL, "tier" => t, "outcome" => o).increment(0);
        }
    }
    metrics::gauge!(REPOS_PER_HOUR).set(0.0);
    for k in ["full", "repair"] {
        metrics::gauge!(SWEEP_PROGRESS, "cycle_kind" => k).set(0.0);
    }
    metrics::gauge!(SWEEP_ETA).set(0.0);
    for c in farsight_storage::codes::CapType::ALL {
        metrics::counter!(ABUSE_CAPPED, "kind" => c.label()).increment(0);
    }
}

/// Counts refusals from an apply report by cap type (`abuse_capped_total`).
pub fn count_refusals(report: &farsight_storage::txn::ApplyReport) {
    for r in &report.refusals {
        metrics::counter!(ABUSE_CAPPED, "kind" => r.refusal.cap_type().label()).increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_cardinality_is_bounded() {
        let mut labels = HashSet::new();
        for i in 0..200 {
            labels.insert(host_label(&format!("h{i}.example")));
        }
        assert!(labels.len() <= TOP_HOSTS + 1);
        assert!(labels.contains("other"));
    }

    #[test]
    fn the_volume_table_is_bounded_and_keeps_the_busy_hosts() {
        let mut v = HostVolumes::new();
        // One busy host among a flood of hosts seen once each.
        for _ in 0..500 {
            v.label("busy.example");
        }
        for i in 0..(MAX_TRACKED_HOSTS * 5) {
            v.label(&format!("h{i}.example"));
            assert!(v.counts.len() <= MAX_TRACKED_HOSTS, "{}", v.counts.len());
        }
        assert_eq!(v.counts.get("busy.example"), Some(&500));
        // It is still told apart once the top is worked out again.
        for _ in 0..RECOMPUTE_EVERY {
            v.label("busy.example");
        }
        assert_eq!(v.label("busy.example"), "busy.example");
        assert!(v.top.len() <= TOP_HOSTS);
    }
}
