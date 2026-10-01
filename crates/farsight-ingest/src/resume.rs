//! Where to resume, and which gap to record (design §6.3), as pure
//! functions over the persisted state.
//!
//! | Situation | Cursor | Gap rule |
//! |---|---|---|
//! | first start (nothing applied) | live tail | none (§6.3 "first start has no gap") |
//! | same instance, v2 → v2 | `seq + 1` (exact) | none; a `CursorTooOld` rejection ⇒ live tail + gap `[applied_through, first live event]` |
//! | same instance, v1 (or v1 → v2) | `cursor_us − 120 s` | first event − cursor > `gap_threshold` ⇒ gap `[applied_through, first event]` (v1 clamps silently) |
//! | other instance (failover), lag known and ≤ `failover_max_lag` | `applied − max(failover_rewind_min, lag + 5 min)` | B rejects the position (clamp / `OutdatedCursor`) ⇒ gap `[applied − 30 min, first event]` |
//! | other instance, lag unknown or too large | `applied − 30 min` | always gap `[applied − 30 min, first event]` |

use std::time::Duration;

use farsight_storage::codes::GapCause;

use crate::frame::Protocol;

/// v1 same-instance replay margin (§6.3).
pub const V1_REPLAY: Duration = Duration::from_secs(120);
/// Fixed width of a failover gap's start (§6.3).
pub const FAILOVER_GAP: Duration = Duration::from_secs(30 * 60);
/// Added to the measured instance lag for the failover rewind (§6.3).
pub const FAILOVER_LAG_MARGIN: Duration = Duration::from_secs(5 * 60);

/// What was persisted by the last committed batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Persisted {
    /// `firehose_state.source_url`.
    pub source_url: Option<String>,
    /// `firehose_state.protocol`.
    pub protocol: Option<Protocol>,
    /// `cursor_seq` (v2).
    pub cursor_seq: Option<i64>,
    /// `cursor_us` (v1 / witness µs).
    pub cursor_us: Option<i64>,
    /// `applied_through`, µs.
    pub applied_through_us: Option<i64>,
}

/// The `cursor` query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cursor {
    /// Omit: live tail.
    Live,
    /// v2 sequence number.
    Seq(i64),
    /// Unix microseconds (v1 always; v2 interprets values ≥ 1e15 as one).
    TimeUs(i64),
}

/// How the first event of the session decides whether a gap is recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapRule {
    /// Exact resume or first start: no gap.
    None,
    /// Gap `[from_us, first event]` if the cursor was clamped — the first
    /// event's witness is more than `threshold` after `requested_us`, or
    /// the server announced it (`#info OutdatedCursor`) — and the first
    /// event lies after `from_us` (otherwise the replay covers us).
    IfClamped {
        /// The requested timestamp.
        requested_us: i64,
        /// Gap start.
        from_us: i64,
        /// Cause to record.
        cause: GapCause,
    },
    /// Always record `[from_us, first event]`.
    Always {
        /// Gap start.
        from_us: i64,
        /// Cause to record.
        cause: GapCause,
    },
}

/// A resume decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Plan {
    /// Cursor to send.
    pub cursor: Cursor,
    /// Gap rule for the first event.
    pub gap: GapRule,
}

/// Tuning inputs (`firehose.tuning`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tuning {
    /// `gap_threshold`.
    pub gap_threshold: Duration,
    /// `failover_rewind_min`.
    pub failover_rewind_min: Duration,
    /// `failover_max_lag`.
    pub failover_max_lag: Duration,
}

fn us(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

/// Plans the resume against instance `url` speaking `protocol`. `lag` is
/// the trailing median `witness − rev time` of the previous instance
/// (`None` if unmeasured), used only on failover.
pub fn plan(
    p: &Persisted,
    url: &str,
    protocol: Protocol,
    lag: Option<Duration>,
    t: &Tuning,
) -> Plan {
    let Some(applied) = p.applied_through_us else {
        return Plan {
            cursor: Cursor::Live,
            gap: GapRule::None,
        };
    };
    let same_instance = p.source_url.as_deref() == Some(url);
    if same_instance {
        if protocol == Protocol::V2 && p.protocol == Some(Protocol::V2) {
            if let Some(seq) = p.cursor_seq {
                return Plan {
                    cursor: Cursor::Seq(seq + 1),
                    gap: GapRule::None,
                };
            }
        }
        let base = p.cursor_us.unwrap_or(applied);
        let requested = base - us(V1_REPLAY);
        return Plan {
            cursor: Cursor::TimeUs(requested),
            gap: GapRule::IfClamped {
                requested_us: requested,
                from_us: applied,
                cause: GapCause::Heuristic,
            },
        };
    }
    let gap_from = applied - us(FAILOVER_GAP);
    match lag {
        Some(l) if l <= t.failover_max_lag => {
            let rewind = t.failover_rewind_min.max(l + FAILOVER_LAG_MARGIN);
            let requested = applied - us(rewind);
            Plan {
                cursor: Cursor::TimeUs(requested),
                gap: GapRule::IfClamped {
                    requested_us: requested,
                    from_us: gap_from,
                    cause: GapCause::Failover,
                },
            }
        }
        _ => Plan {
            cursor: Cursor::TimeUs(gap_from),
            gap: GapRule::Always {
                from_us: gap_from,
                cause: GapCause::Failover,
            },
        },
    }
}

/// The gap to record once the first event (witness `first_us`) arrives,
/// if any. `clamped_notice` is true when the server sent `#info
/// OutdatedCursor` before it.
pub fn gap_for_first_event(
    rule: GapRule,
    first_us: i64,
    clamped_notice: bool,
    t: &Tuning,
) -> Option<(i64, i64, GapCause)> {
    match rule {
        GapRule::None => None,
        GapRule::Always { from_us, cause } => Some((from_us.min(first_us), first_us, cause)),
        GapRule::IfClamped {
            requested_us,
            from_us,
            cause,
        } => {
            // A clamp loses data only if the server's first event is past
            // our position; if it replayed from an older floor, nothing
            // was skipped.
            let clamped = clamped_notice || first_us - requested_us > us(t.gap_threshold);
            if clamped && first_us > from_us {
                Some((from_us, first_us, cause))
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: Tuning = Tuning {
        gap_threshold: Duration::from_secs(300),
        failover_rewind_min: Duration::from_secs(600),
        failover_max_lag: Duration::from_secs(1800),
    };
    const S: i64 = 1_000_000;
    const A: &str = "wss://a.example";
    const B: &str = "wss://b.example";

    fn persisted(proto: Protocol) -> Persisted {
        Persisted {
            source_url: Some(A.into()),
            protocol: Some(proto),
            cursor_seq: Some(500),
            cursor_us: Some(10_000 * S),
            applied_through_us: Some(10_000 * S),
        }
    }

    #[test]
    fn first_start_is_live_without_gap() {
        let p = plan(&Persisted::default(), A, Protocol::V1, None, &T);
        assert_eq!(
            p,
            Plan {
                cursor: Cursor::Live,
                gap: GapRule::None
            }
        );
    }

    #[test]
    fn same_instance_v2_resumes_exactly() {
        let p = plan(&persisted(Protocol::V2), A, Protocol::V2, None, &T);
        assert_eq!(
            p,
            Plan {
                cursor: Cursor::Seq(501),
                gap: GapRule::None
            }
        );
    }

    #[test]
    fn same_instance_v1_rewinds_120s_and_detects_clamps() {
        let p = plan(&persisted(Protocol::V1), A, Protocol::V1, None, &T);
        assert_eq!(p.cursor, Cursor::TimeUs(10_000 * S - 120 * S));
        // First event soon after the requested position: no gap.
        assert_eq!(gap_for_first_event(p.gap, 9_890 * S, false, &T), None);
        // Silent clamp: first event > 300 s after the request.
        assert_eq!(
            gap_for_first_event(p.gap, 20_000 * S, false, &T),
            Some((10_000 * S, 20_000 * S, GapCause::Heuristic))
        );
    }

    #[test]
    fn protocol_upgrade_on_same_instance_uses_timestamp() {
        let p = plan(&persisted(Protocol::V1), A, Protocol::V2, None, &T);
        assert!(matches!(p.cursor, Cursor::TimeUs(_)));
        assert!(matches!(p.gap, GapRule::IfClamped { .. }));
        // An OutdatedCursor notice records the gap even if the first event
        // is close to the request, as long as it is past our position.
        assert!(gap_for_first_event(p.gap, 10_001 * S, true, &T).is_some());
        // A clamp to a floor older than our position loses nothing.
        assert!(gap_for_first_event(p.gap, 9_000 * S, true, &T).is_none());
    }

    #[test]
    fn failover_with_measured_lag() {
        let p = plan(
            &persisted(Protocol::V2),
            B,
            Protocol::V2,
            Some(Duration::from_secs(60)),
            &T,
        );
        // max(10 min, 1 min + 5 min) = 10 min.
        assert_eq!(p.cursor, Cursor::TimeUs(10_000 * S - 600 * S));
        let p = plan(
            &persisted(Protocol::V2),
            B,
            Protocol::V2,
            Some(Duration::from_secs(900)),
            &T,
        );
        // max(10 min, 15 min + 5 min) = 20 min.
        assert_eq!(p.cursor, Cursor::TimeUs(10_000 * S - 1200 * S));
        assert_eq!(
            gap_for_first_event(p.gap, 20_000 * S, false, &T),
            Some((10_000 * S - 1800 * S, 20_000 * S, GapCause::Failover))
        );
    }

    #[test]
    fn failover_without_usable_lag_always_records_a_gap() {
        for lag in [None, Some(Duration::from_secs(3600))] {
            let p = plan(&persisted(Protocol::V1), B, Protocol::V1, lag, &T);
            assert_eq!(p.cursor, Cursor::TimeUs(10_000 * S - 1800 * S));
            assert_eq!(
                gap_for_first_event(p.gap, 9_000 * S, false, &T),
                Some((8_200 * S, 9_000 * S, GapCause::Failover))
            );
        }
    }
}
