//! Coverage building blocks (design §3.7): the global snapshot, the gap
//! predicate `covered(t)`, the pending-list effects of §3.7.4 and network
//! scope. The API crate composes these into `freshness` objects.
//!
//! The snapshot is read in **one `REPEATABLE READ` transaction**
//! ([`read_snapshot`]) so its inputs are mutually consistent, including
//! `firehoseAppliedThrough` (§3.7.1–§3.7.2). The API refreshes it on
//! `NOTIFY farsight_coverage`, every 10 s and on every LISTEN reconnect.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::codes::{DebtReason, GapCause, Protocol};
use crate::debts;
use crate::error::Result;
use crate::firehose::{self, FirehoseState, Gap};
use crate::keys::Limits;

/// Channel name for coverage notifications.
pub const COVERAGE_CHANNEL: &str = "farsight_coverage";

/// Snapshot refresh period (§3.7.1).
pub const SNAPSHOT_REFRESH: Duration = Duration::from_secs(10);

/// The latest completed full sweep cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Baseline {
    /// `sweep_cycles.id`.
    pub cycle_id: i64,
    /// Collections the cycle covered (storage codes).
    pub collections: Vec<i16>,
    /// `S_C` on the witness clock.
    pub s_c: Option<DateTime<Utc>>,
    /// Witness time of completion.
    pub completed_witness: Option<DateTime<Utc>>,
}

/// List-state counts used by `exceptions` and `pendingLists`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListCounts {
    /// `pending` lists (`coverage.pendingLists`).
    pub pending: i64,
    /// `unavailable` lists.
    pub unavailable: i64,
    /// `missing` lists.
    pub missing: i64,
    /// `deferred` lists.
    pub deferred: i64,
    /// Tracked lists with `capped`.
    pub capped: i64,
}

/// The §3.7.4 effect of pending lists at network scope.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingEffects {
    /// Pending lists considered (pending, plus purging lists that will be
    /// re-admitted).
    pub considered: i64,
    /// Their ids, sorted (the live rule of §3.7.1 compares against it).
    pub considered_ids: Vec<i64>,
    /// `excludedPendingLists`: no relevant listblocks, or beyond the
    /// per-owner-key bound.
    pub excluded: i64,
    /// Lists taking effect.
    pub effective: Vec<i64>,
    /// `indexedAt` cap: min over effective live lists of
    /// `min(witnessed_at) − 1 µs`.
    pub indexed_at_cap: Option<DateTime<Utc>>,
    /// Some effective list has a relevant listblock with NULL
    /// `witnessed_at` ⇒ level `partial` + `list_pending_historical`.
    pub historical: bool,
}

/// One consistent read of every global coverage input (§3.7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalSnapshot {
    /// Database time of the read.
    pub read_at: DateTime<Utc>,
    /// Firehose state, including `applied_through`.
    pub firehose: FirehoseState,
    /// Latest completed full baseline.
    pub baseline: Option<Baseline>,
    /// Every gap (healed ones matter for `completeSince`).
    pub gaps: Vec<Gap>,
    /// A global storage refusal interval is open.
    pub storage_refusal_active: bool,
    /// Distinct actors per debt reason.
    pub debt_counts: HashMap<DebtReason, i64>,
    /// List-state counts.
    pub lists: ListCounts,
    /// Pending-list effects.
    pub pending: PendingEffects,
}

/// `sweep_cycles` columns of the latest completed baseline.
type BaselineRow = (i64, Vec<i16>, Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// Reads the global snapshot in one `REPEATABLE READ, READ ONLY`
/// transaction.
pub async fn read_snapshot(pool: &PgPool, limits: &Limits) -> Result<GlobalSnapshot> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let read_at: DateTime<Utc> = sqlx::query_scalar("SELECT now()")
        .fetch_one(&mut *tx)
        .await?;
    let fh = firehose::read_state(&mut *tx).await?;
    let baseline: Option<BaselineRow> = sqlx::query_as(
        "SELECT id, collections, effective_start_witness, completed_witness
             FROM sweep_cycles WHERE kind = 1 AND completed_at IS NOT NULL
             ORDER BY completed_at DESC LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await?;
    let gaps = firehose::all_gaps(&mut *tx).await?;
    let storage_refusal_active: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM storage_refusals WHERE to_witness IS NULL)",
    )
    .fetch_one(&mut *tx)
    .await?;
    let debt_counts = debts::counts_by_reason(&mut *tx).await?;
    let (pending, unavailable, missing, deferred, capped): (i64, i64, i64, i64, i64) =
        sqlx::query_as(
            "SELECT count(*) FILTER (WHERE track_state = 1),
                    count(*) FILTER (WHERE track_state = 4),
                    count(*) FILTER (WHERE track_state = 6),
                    count(*) FILTER (WHERE track_state = 8),
                    count(*) FILTER (WHERE track_state IN (1, 2, 3, 4) AND capped)
             FROM lists WHERE track_state <> 0",
        )
        .fetch_one(&mut *tx)
        .await?;
    let pending_effects = read_pending_effects(&mut tx, limits).await?;
    tx.commit().await?;
    Ok(GlobalSnapshot {
        read_at,
        firehose: fh,
        baseline: baseline.map(|(cycle_id, collections, s_c, completed_witness)| Baseline {
            cycle_id,
            collections,
            s_c,
            completed_witness,
        }),
        gaps,
        storage_refusal_active,
        debt_counts,
        lists: ListCounts {
            pending,
            unavailable,
            missing,
            deferred,
            capped,
        },
        pending: pending_effects,
    })
}

/// A pending list with its relevant-listblock aggregates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingList {
    /// `lists.id`.
    pub id: i64,
    /// Admission time (slot order: oldest first).
    pub admitted_at: Option<DateTime<Utc>>,
    /// The owner key (§3.7.4).
    pub owner_key: String,
    /// Counted listblocks by covered authors.
    pub relevant: i64,
    /// Of those, rows with NULL `witnessed_at`.
    pub historical: i64,
    /// Minimum `witnessed_at` among relevant rows.
    pub min_witnessed: Option<DateTime<Utc>>,
}

type PendingRow = (
    i64,
    Option<DateTime<Utc>>,
    String,
    Option<String>,
    bool,
    bool,
    i64,
    i64,
    Option<DateTime<Utc>>,
);

/// Reads the pending lists (and purging lists that will be re-admitted)
/// with their relevant listblocks computed on read from current rows
/// (§3.7.4: a covered author has no `resync` or `unreachable` debt).
pub async fn read_pending_lists(conn: &mut sqlx::PgConnection) -> Result<Vec<PendingList>> {
    let rows: Vec<PendingRow> = sqlx::query_as(
        "SELECT l.id, l.admitted_at, a.did, h.cap_key, COALESCE(h.large, false),
                a.pds_host_id IS NOT NULL,
                count(b.author_id), count(b.author_id) FILTER (WHERE b.witnessed_at IS NULL),
                min(b.witnessed_at)
         FROM lists l
         JOIN actors a ON a.id = l.owner_id
         LEFT JOIN pds_hosts h ON h.id = a.pds_host_id
         LEFT JOIN list_blocks b ON b.list_id = l.id AND b.counted
              AND NOT EXISTS (SELECT 1 FROM relist_debt d
                              WHERE d.actor_id = b.author_id AND d.reason IN (1, 2))
         WHERE l.track_state = 1
            OR (l.track_state = 5 AND l.purge_then = 0 AND l.listblock_count > 0)
         GROUP BY l.id, l.admitted_at, a.did, h.cap_key, h.large, a.pds_host_id",
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, admitted_at, did, cap_key, large, resolved, relevant, historical, min_w)| {
                let owner_key = match (resolved, large, cap_key) {
                    (true, false, Some(cap)) => format!("bucket:{cap}"),
                    _ => format!("did:{did}"),
                };
                PendingList {
                    id,
                    admitted_at,
                    owner_key,
                    relevant,
                    historical,
                    min_witnessed: min_w,
                }
            },
        )
        .collect())
}

async fn read_pending_effects(
    conn: &mut sqlx::PgConnection,
    limits: &Limits,
) -> Result<PendingEffects> {
    let lists = read_pending_lists(conn).await?;
    Ok(pending_effects(
        &lists,
        limits.cfg.pending_effects_per_owner_key as usize,
    ))
}

/// The §3.7.4 table and per-owner-key bound, as a pure function.
pub fn pending_effects(lists: &[PendingList], per_owner_key: usize) -> PendingEffects {
    let mut out = PendingEffects {
        considered: lists.len() as i64,
        considered_ids: lists.iter().map(|l| l.id).collect(),
        ..PendingEffects::default()
    };
    out.considered_ids.sort_unstable();
    let mut by_key: BTreeMap<&str, Vec<&PendingList>> = BTreeMap::new();
    for l in lists {
        if l.relevant == 0 {
            out.excluded += 1;
        } else {
            by_key.entry(l.owner_key.as_str()).or_default().push(l);
        }
    }
    for (_, mut ls) in by_key {
        // Oldest admission first; ties by id for determinism.
        ls.sort_by(|a, b| a.admitted_at.cmp(&b.admitted_at).then(a.id.cmp(&b.id)));
        for (i, l) in ls.into_iter().enumerate() {
            if i >= per_owner_key {
                out.excluded += 1;
                continue;
            }
            out.effective.push(l.id);
            if l.historical > 0 {
                out.historical = true;
            } else if let Some(w) = l.min_witnessed {
                let cap = w - chrono::Duration::microseconds(1);
                out.indexed_at_cap = Some(match out.indexed_at_cap {
                    Some(c) if c < cap => c,
                    _ => cap,
                });
            }
        }
    }
    out.effective.sort_unstable();
    out
}

impl GlobalSnapshot {
    /// Whether the synthetic gap `[applied_through, ∞)` exists: the stream
    /// is disconnected or lags more than `synthetic_gap_lag` (§3.7.1).
    pub fn synthetic_gap(&self, synthetic_gap_lag: Duration) -> bool {
        match self.firehose.applied_through {
            None => true,
            Some(a) => {
                !self.firehose.connected
                    || (self.read_at - a).to_std().unwrap_or(Duration::ZERO) > synthetic_gap_lag
            }
        }
    }

    /// `covered(t)` (§3.7.1): `t` defined and no unhealed gap — recorded or
    /// synthetic — overlaps `[t, firehoseAppliedThrough]`.
    pub fn covered(&self, t: Option<DateTime<Utc>>, synthetic_gap_lag: Duration) -> bool {
        let (Some(t), Some(a)) = (t, self.firehose.applied_through) else {
            return false;
        };
        if self.synthetic_gap(synthetic_gap_lag) {
            return false;
        }
        !self
            .gaps
            .iter()
            .filter(|g| g.healed_witness.is_none())
            .any(|g| gap_overlaps(g, t, a))
    }
}

/// Whether gap `g` overlaps `[t, a]`.
pub fn gap_overlaps(g: &Gap, t: DateTime<Utc>, a: DateTime<Utc>) -> bool {
    g.from_at <= a && g.to_at.is_none_or(|to| to >= t)
}

/// Coverage level (open enum on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// Worst.
    Partial,
    /// Via discovery.
    Assisted,
    /// Best.
    Complete,
}

/// A scope's coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// Level.
    pub level: Level,
    /// `completeSince` (witness clock), when the level holds since a point.
    pub complete_since: Option<DateTime<Utc>>,
    /// Reason codes (§3.7.2).
    pub reasons: Vec<&'static str>,
}

/// Network scope for collection `k` (storage code, §7.1) (§3.7.5 item 1): `complete` iff a
/// completed baseline covers `k`, `covered(S_C)`, protocol v2, and no
/// global storage refusal is active.
pub fn network_scope(s: &GlobalSnapshot, k: i16, synthetic_gap_lag: Duration) -> Scope {
    let mut reasons = Vec::new();
    let baseline = s.baseline.as_ref().filter(|b| b.collections.contains(&k));
    if baseline.is_none() {
        reasons.push("sweep_incomplete");
    }
    if s.firehose.applied_through.is_none() || !s.firehose.connected {
        reasons.push("firehose_disconnected");
    } else if s.synthetic_gap(synthetic_gap_lag) {
        reasons.push("firehose_lagging");
    }
    // `covered(S_C)` requires S_C defined (§3.7.1).
    if baseline.is_some_and(|b| b.s_c.is_none()) {
        reasons.push("sweep_incomplete");
    }
    if let (Some(b), Some(a)) = (baseline, s.firehose.applied_through) {
        let sc = b.s_c.unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        if s.gaps
            .iter()
            .filter(|g| g.healed_witness.is_none())
            .any(|g| gap_overlaps(g, sc, a))
        {
            reasons.push("firehose_gap");
        }
    } else if s.gaps.iter().any(|g| g.healed_witness.is_none()) {
        reasons.push("firehose_gap");
    }
    let v1_open = s
        .gaps
        .iter()
        .any(|g| g.cause == GapCause::SyncUnavailable && g.to_at.is_none());
    if s.firehose.protocol != Some(Protocol::V2) || v1_open {
        reasons.push("sync_events_unavailable");
    }
    if s.storage_refusal_active {
        reasons.push("storage_refusal");
    }
    reasons.sort_unstable();
    reasons.dedup();
    if !reasons.is_empty() {
        return Scope {
            level: Level::Partial,
            complete_since: None,
            reasons,
        };
    }
    let b = baseline.expect("checked above");
    let sc = b.s_c.unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let a = s.firehose.applied_through.expect("checked above");
    let last_healed = s
        .gaps
        .iter()
        .filter(|g| gap_overlaps(g, sc, a))
        .filter_map(|g| g.healed_witness)
        .max();
    let complete_since = match (b.completed_witness, last_healed) {
        (Some(c), Some(h)) => Some(c.max(h)),
        (c, h) => c.or(h),
    };
    Scope {
        level: Level::Complete,
        complete_since,
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + s, 0).unwrap()
    }

    fn pl(
        id: i64,
        key: &str,
        admitted: i64,
        relevant: i64,
        historical: i64,
        min_w: Option<i64>,
    ) -> PendingList {
        PendingList {
            id,
            admitted_at: Some(t(admitted)),
            owner_key: key.to_owned(),
            relevant,
            historical,
            min_witnessed: min_w.map(t),
        }
    }

    #[test]
    fn pending_table() {
        // none relevant ⇒ excluded; all witnessed ⇒ indexedAt cap;
        // any NULL ⇒ historical.
        let e = pending_effects(
            &[
                pl(1, "did:a", 0, 0, 0, None),
                pl(2, "did:b", 0, 3, 0, Some(100)),
                pl(3, "did:c", 0, 2, 0, Some(50)),
            ],
            5,
        );
        assert_eq!(e.excluded, 1);
        assert_eq!(e.effective, [2, 3]);
        assert!(!e.historical);
        assert_eq!(
            e.indexed_at_cap,
            Some(t(50) - chrono::Duration::microseconds(1))
        );
        let e = pending_effects(&[pl(4, "did:d", 0, 2, 1, Some(10))], 5);
        assert!(e.historical);
        assert_eq!(e.indexed_at_cap, None);
    }

    #[test]
    fn per_owner_key_bound() {
        let lists: Vec<PendingList> = (0..7)
            .map(|i| pl(10 + i, "bucket:evil.example", 100 - i, 1, 1, None))
            .chain([pl(99, "did:other", 0, 1, 0, Some(5))])
            .collect();
        let e = pending_effects(&lists, 5);
        // Oldest 5 of the flooding key take effect, 2 excluded; the other
        // key is unaffected.
        assert_eq!(e.excluded, 2);
        assert_eq!(e.effective.len(), 6);
        assert!(e.effective.contains(&99));
        assert!(e.effective.contains(&16) && !e.effective.contains(&10));
    }

    fn snap(applied: Option<i64>, connected: bool, gaps: Vec<Gap>) -> GlobalSnapshot {
        GlobalSnapshot {
            read_at: t(1000),
            firehose: FirehoseState {
                applied_through: applied.map(t),
                connected,
                protocol: Some(Protocol::V2),
                ..FirehoseState::default()
            },
            baseline: Some(Baseline {
                cycle_id: 1,
                collections: vec![1, 2, 3, 4],
                s_c: Some(t(100)),
                completed_witness: Some(t(500)),
            }),
            gaps,
            storage_refusal_active: false,
            debt_counts: HashMap::new(),
            lists: ListCounts::default(),
            pending: PendingEffects::default(),
        }
    }

    fn gap(from: i64, to: Option<i64>, healed: Option<i64>) -> Gap {
        Gap {
            id: 1,
            from_at: t(from),
            to_at: to.map(t),
            cause: GapCause::CursorTooOld,
            healed_witness: healed.map(t),
        }
    }

    const LAG: Duration = Duration::from_secs(300);

    #[test]
    fn covered_predicate() {
        let s = snap(Some(999), true, vec![]);
        assert!(s.covered(Some(t(100)), LAG));
        assert!(!s.covered(None, LAG));
        // Disconnected or lagging: synthetic gap.
        assert!(!snap(Some(999), false, vec![]).covered(Some(t(100)), LAG));
        assert!(!snap(Some(600), true, vec![]).covered(Some(t(100)), LAG));
        // Recorded unhealed gap overlapping [t, A].
        let s = snap(Some(999), true, vec![gap(200, Some(300), None)]);
        assert!(!s.covered(Some(t(100)), LAG));
        assert!(!s.covered(Some(t(250)), LAG));
        assert!(s.covered(Some(t(301)), LAG));
        // Healed gaps do not count.
        let s = snap(Some(999), true, vec![gap(200, Some(300), Some(400))]);
        assert!(s.covered(Some(t(100)), LAG));
        // Open gap (v1 interval) covers everything after its start.
        let s = snap(Some(999), true, vec![gap(200, None, None)]);
        assert!(!s.covered(Some(t(900)), LAG));
    }

    #[test]
    fn network_scope_levels() {
        let s = snap(Some(999), true, vec![]);
        let n = network_scope(&s, 1, LAG);
        assert_eq!(n.level, Level::Complete);
        assert_eq!(n.complete_since, Some(t(500)));
        let s = snap(Some(999), true, vec![gap(700, Some(800), Some(900))]);
        assert_eq!(network_scope(&s, 1, LAG).complete_since, Some(t(900)));
        let s = snap(Some(999), true, vec![gap(700, Some(800), None)]);
        let n = network_scope(&s, 1, LAG);
        assert_eq!((n.level, n.reasons), (Level::Partial, vec!["firehose_gap"]));
        let mut s = snap(Some(999), true, vec![]);
        s.baseline = None;
        assert_eq!(network_scope(&s, 1, LAG).reasons, ["sweep_incomplete"]);
        let mut s = snap(Some(999), true, vec![]);
        s.firehose.protocol = Some(Protocol::V1);
        assert_eq!(
            network_scope(&s, 1, LAG).reasons,
            ["sync_events_unavailable"]
        );
        let mut s = snap(Some(999), true, vec![]);
        s.storage_refusal_active = true;
        assert_eq!(network_scope(&s, 1, LAG).reasons, ["storage_refusal"]);
    }
}
