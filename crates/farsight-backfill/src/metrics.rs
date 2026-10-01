//! Backfill metrics (design §13), exported by `farsight-backfill` on
//! `metrics.backfill_bind`. Host labels are bounded: the top 50 hosts by
//! request volume keep their name, every other host is `other` (≤ 51
//! values); series of hosts that fall out of the top 50 expire after an
//! idle period (exporter idle timeout).

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

/// The `host` label for a request to `host`, counting its volume.
pub fn host_label(host: &str) -> String {
    let mut g = VOLUMES.lock().unwrap_or_else(|e| e.into_inner());
    let v = g.get_or_insert_with(|| HostVolumes {
        counts: HashMap::new(),
        top: HashSet::new(),
        since: 0,
    });
    *v.counts.entry(host.to_owned()).or_insert(0) += 1;
    v.since += 1;
    if v.top.len() < TOP_HOSTS && !v.top.contains(host) {
        v.top.insert(host.to_owned());
    } else if v.since >= RECOMPUTE_EVERY {
        v.since = 0;
        let mut all: Vec<(&String, &u64)> = v.counts.iter().collect();
        all.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        v.top = all
            .iter()
            .take(TOP_HOSTS)
            .map(|(h, _)| (*h).clone())
            .collect();
        // Bound the volume table too.
        if v.counts.len() > 100_000 {
            let keep: HashSet<String> =
                all.iter().take(10_000).map(|(h, _)| (*h).clone()).collect();
            v.counts.retain(|h, _| keep.contains(h));
        }
    }
    if v.top.contains(host) {
        host.to_owned()
    } else {
        "other".to_owned()
    }
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
}
