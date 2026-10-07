//! The `freshness` object (see `docs/design/coverage.md`): its scopes,
//! composed on the global snapshot plus the per-response live reads,
//! rendered as JSON.

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use farsight_core::Collection;
use farsight_storage::codes::{DebtReason, TrackState};
use farsight_storage::coverage::{GlobalSnapshot, Level, network_scope};
use farsight_storage::queries::ActorCoverage;
use serde_json::{Map, Value, json};

/// A scope's coverage while it is being composed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cov {
    /// Level.
    pub level: Level,
    /// `completeSince` (only while the level is not `partial`).
    pub complete_since: Option<DateTime<Utc>>,
    /// Reason codes.
    pub reasons: BTreeSet<&'static str>,
    /// `indexedAt`.
    pub indexed_at: Option<DateTime<Utc>>,
}

impl Cov {
    /// Lowers the level to at most `level` and records `reason`.
    pub fn lower(&mut self, level: Level, reason: &'static str) {
        if level < self.level {
            self.level = level;
        }
        if self.level == Level::Partial {
            self.complete_since = None;
        }
        self.reasons.insert(reason);
    }

    /// Lowers to `partial` with `reason`.
    pub fn partial(&mut self, reason: &'static str) {
        self.lower(Level::Partial, reason);
    }

    /// Records a reason without changing the level (counted exclusions of
    /// a `complete` scope, and `indexedAt` effects).
    pub fn note(&mut self, reason: &'static str) {
        self.reasons.insert(reason);
    }

    /// Caps `indexedAt`.
    pub fn cap_indexed(&mut self, t: DateTime<Utc>) {
        self.indexed_at = Some(match self.indexed_at {
            Some(i) if i < t => i,
            _ => t,
        });
    }

    /// The minimum of two scopes.
    pub fn combine(mut self, other: Cov) -> Cov {
        let level = self.level.min(other.level);
        self.complete_since = if level == Level::Partial {
            None
        } else {
            match (self.complete_since, other.complete_since) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            }
        };
        self.level = level;
        self.reasons.extend(other.reasons);
        if let Some(t) = other.indexed_at {
            self.cap_indexed(t);
        }
        self
    }
}

/// Storage code of `block`.
pub const BLOCK: i16 = 1;
/// Storage code of `listblock`.
pub const LISTBLOCK: i16 = 2;

/// Which discovery scope a subject-scope check uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubjectKind {
    /// Direct blocks (`subject_coverage(X, block)`).
    Block,
    /// listitem → list → listblock chain (`subject_coverage(X, list_chain)`).
    ListChain,
}

/// The snapshot as seen by one response.
#[derive(Debug, Clone, Copy)]
pub struct View<'a> {
    /// The global snapshot.
    pub snap: &'a GlobalSnapshot,
    /// `firehose.tuning.synthetic_gap_lag`.
    pub lag: Duration,
}

/// The reason a `covered(t)` check failed, from the snapshot.
fn uncovered_reason(s: &GlobalSnapshot, lag: Duration) -> &'static str {
    if s.firehose.applied_through.is_none() || !s.firehose.connected {
        "firehose_disconnected"
    } else if s.synthetic_gap(lag) {
        "firehose_lagging"
    } else {
        "firehose_gap"
    }
}

/// Reason for a list in a degraded state.
pub fn list_state_reason(state: TrackState, readmits: bool) -> Option<&'static str> {
    match state {
        TrackState::Pending => Some("list_pending"),
        TrackState::Purging if readmits => Some("list_pending"),
        TrackState::Unavailable => Some("list_unavailable"),
        TrackState::Deferred => Some("list_deferred"),
        TrackState::Missing => Some("list_missing"),
        _ => None,
    }
}

impl View<'_> {
    /// `firehoseAppliedThrough` of the snapshot.
    pub fn applied_through(&self) -> Option<DateTime<Utc>> {
        self.snap.firehose.applied_through
    }

    /// `covered(t)`.
    pub fn covered(&self, t: Option<DateTime<Utc>>) -> bool {
        self.snap.covered(t, self.lag)
    }

    /// The reason a failed `covered` check reports.
    pub fn uncovered_reason(&self) -> &'static str {
        uncovered_reason(self.snap, self.lag)
    }

    /// Network scope for collection `k`.
    pub fn network(&self, k: i16) -> Cov {
        let s = network_scope(self.snap, k, self.lag);
        Cov {
            level: s.level,
            complete_since: s.complete_since,
            reasons: s.reasons.into_iter().collect(),
            indexed_at: self.applied_through(),
        }
    }

    /// Network scope for `k`, or — when it is not complete — subject scope
    /// for `(X, kind)`. Returns the coverage and whether it is at subject
    /// scope (`assisted`).
    pub fn network_or_subject(
        &self,
        k: i16,
        x: Option<&ActorCoverage>,
        kind: SubjectKind,
    ) -> (Cov, bool) {
        let n = self.network(k);
        if n.level == Level::Complete {
            return (n, false);
        }
        let Some(x) = x else { return (n, false) };
        let has = match kind {
            SubjectKind::Block => x.subject_block,
            SubjectKind::ListChain => x.subject_list_chain,
        };
        let d = x.discovered_witness;
        if has && d.is_some() && self.covered(d) {
            return (
                Cov {
                    level: Level::Assisted,
                    complete_since: d,
                    reasons: n.reasons,
                    indexed_at: n.indexed_at,
                },
                true,
            );
        }
        let mut n = n;
        if x.truncated {
            n.partial("discovery_truncated");
        }
        (n, false)
    }

    /// The pending-list table at network scope: historical effective pending
    /// lists lower the level, live ones cap `indexedAt`.
    pub fn apply_pending_table(&self, c: &mut Cov) {
        let p = &self.snap.pending;
        if p.historical {
            c.partial("list_pending_historical");
        }
        if let Some(cap) = p.indexed_at_cap {
            c.cap_indexed(cap);
            c.note("list_pending");
        }
    }

    /// Global coverage (`getStats`): network scope over block and
    /// listblock, plus the pending-list table.
    pub fn global(&self) -> Cov {
        let mut c = self
            .network(Collection::Block.code())
            .combine(self.network(Collection::ListBlock.code()));
        self.apply_pending_table(&mut c);
        c
    }

    /// `coverage.exceptions` (always present).
    pub fn exceptions(&self) -> Value {
        let s = self.snap;
        let debt = |r| s.debt_counts.get(&r).copied().unwrap_or(0);
        json!({
            "unreachableRepos": debt(DebtReason::Unreachable),
            "pendingResyncs": debt(DebtReason::Resync),
            "cappedAuthors": debt(DebtReason::Capped),
            "refusedAuthors": debt(DebtReason::Refused),
            "unavailableLists": s.lists.unavailable,
            "missingLists": s.lists.missing,
            "deferredLists": s.lists.deferred,
            "cappedLists": s.lists.capped,
            "excludedPendingLists": s.pending.excluded,
        })
    }

    /// Renders the `freshness` object.
    pub fn render(&self, c: &Cov, now: DateTime<Utc>, source_lag: Option<f64>) -> Value {
        let mut f = Map::new();
        f.insert("asOf".into(), json!(ts(now)));
        if let Some(i) = c.indexed_at {
            f.insert("indexedAt".into(), json!(ts(i)));
        }
        if let Some(a) = self.applied_through() {
            f.insert("firehoseAppliedThrough".into(), json!(ts(a)));
            f.insert("firehoseLagSeconds".into(), json!(secs(now - a)));
        }
        if let Some(l) = source_lag {
            f.insert("sourceLagSeconds".into(), json!(round3(l.max(0.0))));
        }
        f.insert(
            "firehoseConnected".into(),
            json!(self.snap.firehose.connected),
        );
        let mut cov = Map::new();
        cov.insert("level".into(), json!(level_name(c.level)));
        if let (Some(s), true) = (c.complete_since, c.level != Level::Partial) {
            cov.insert("completeSince".into(), json!(ts(s)));
        }
        cov.insert(
            "reasons".into(),
            json!(c.reasons.iter().copied().collect::<Vec<_>>()),
        );
        cov.insert("exceptions".into(), self.exceptions());
        cov.insert("pendingLists".into(), json!(self.snap.lists.pending));
        f.insert("coverage".into(), Value::Object(cov));
        Value::Object(f)
    }
}

/// Wire name of a level.
pub fn level_name(l: Level) -> &'static str {
    match l {
        Level::Complete => "complete",
        Level::Assisted => "assisted",
        Level::Partial => "partial",
    }
}

/// RFC 3339 with microseconds (truncated, so a watermark is never
/// rounded up).
pub fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

/// Seconds of a duration, three decimals, never negative.
pub fn secs(d: chrono::Duration) -> f64 {
    round3((d.num_microseconds().unwrap_or(i64::MAX) as f64 / 1e6).max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use farsight_storage::codes::Protocol;
    use farsight_storage::coverage::{Baseline, ListCounts, PendingEffects};
    use farsight_storage::firehose::FirehoseState;
    use std::collections::HashMap;

    fn t(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + s, 0).unwrap()
    }

    fn snap(protocol: Protocol, baseline: bool) -> GlobalSnapshot {
        GlobalSnapshot {
            read_at: t(1000),
            firehose: FirehoseState {
                applied_through: Some(t(999)),
                connected: true,
                protocol: Some(protocol),
                ..FirehoseState::default()
            },
            baseline: baseline.then(|| Baseline {
                cycle_id: 1,
                collections: vec![1, 2, 3, 4],
                s_c: Some(t(100)),
                completed_witness: Some(t(500)),
            }),
            gaps: vec![],
            storage_refusal_active: false,
            debt_counts: HashMap::new(),
            lists: ListCounts::default(),
            pending: PendingEffects::default(),
        }
    }

    const LAG: Duration = Duration::from_secs(300);

    #[test]
    fn subject_scope_assisted() {
        let s = snap(Protocol::V2, false);
        let v = View { snap: &s, lag: LAG };
        let x = ActorCoverage {
            discovered_witness: Some(t(900)),
            subject_block: true,
            ..ActorCoverage::default()
        };
        let (c, assisted) = v.network_or_subject(BLOCK, Some(&x), SubjectKind::Block);
        assert!(assisted);
        assert_eq!(c.level, Level::Assisted);
        assert_eq!(c.complete_since, Some(t(900)));
        // List-chain scope was not confirmed.
        let (c, _) = v.network_or_subject(LISTBLOCK, Some(&x), SubjectKind::ListChain);
        assert_eq!(c.level, Level::Partial);
        // Truncated.
        let x = ActorCoverage {
            truncated: true,
            ..x
        };
        let (c, _) = v.network_or_subject(LISTBLOCK, Some(&x), SubjectKind::ListChain);
        assert!(c.reasons.contains("discovery_truncated"));
    }

    #[test]
    fn combine_takes_minimum() {
        let s = snap(Protocol::V2, true);
        let v = View { snap: &s, lag: LAG };
        let mut a = v.network(BLOCK);
        assert_eq!(a.level, Level::Complete);
        let b = a.clone();
        a.partial("list_pending");
        let c = b.combine(a);
        assert_eq!(c.level, Level::Partial);
        assert_eq!(c.complete_since, None);
        assert!(c.reasons.contains("list_pending"));
    }

    #[test]
    fn render_shape() {
        let s = snap(Protocol::V1, false);
        let v = View { snap: &s, lag: LAG };
        let c = v.global();
        let f = v.render(&c, t(1000), Some(1.23456));
        assert_eq!(f["coverage"]["level"], "partial");
        assert_eq!(f["firehoseLagSeconds"], 1.0);
        assert_eq!(f["sourceLagSeconds"], 1.235);
        let reasons = f["coverage"]["reasons"].as_array().unwrap();
        assert!(reasons.contains(&json!("sync_events_unavailable")));
        assert!(reasons.contains(&json!("sweep_incomplete")));
        assert_eq!(f["coverage"]["exceptions"]["missingLists"], 0);
    }
}
