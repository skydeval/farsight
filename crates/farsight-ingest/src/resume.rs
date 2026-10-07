//! Where to resume, and which gap to record (see
//! `docs/design/firehose.md`), as pure functions over the persisted
//! state.
//!
//! | Situation | Cursor | Gap rule |
//! |---|---|---|
//! | first start (nothing applied) | live tail | none |
//! | same instance, v2 → v2 | `seq + 1` (exact) | none; a `CursorTooOld` rejection ⇒ live tail + gap `[applied_through, first live event]` |
//! | same instance, v1 (or v1 → v2) | `cursor_us − 120 s` | first event − cursor > `gap_threshold` ⇒ gap `[applied_through, first event]` (v1 clamps silently) |
//! | other instance (failover), lag known and ≤ `failover_max_lag` | `applied − max(failover_rewind_min, lag + 5 min)` | B rejects the position (clamp / `OutdatedCursor`) ⇒ gap `[applied − 30 min, first event]` |
//! | other instance, lag unknown or too large | `applied − 30 min` | always gap `[applied − 30 min, first event]` |

use std::time::Duration;

use farsight_storage::codes::GapCause;

use crate::frame::Protocol;

/// How far before the persisted `cursor_us` a time-cursor resume on the
/// same instance asks to start.
pub const V1_REPLAY: Duration = Duration::from_secs(120);
/// Fixed width of a failover gap's start.
pub const FAILOVER_GAP: Duration = Duration::from_secs(30 * 60);
/// Added to the measured instance lag for the failover rewind.
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
        /// The timestamp sent as the cursor, µs.
        requested_us: i64,
        /// Where the gap starts if one is recorded, witness µs.
        from_us: i64,
        /// Cause to record.
        cause: GapCause,
    },
    /// Always record `[from_us, first event]`.
    Always {
        /// Where the gap starts, witness µs.
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

/// A duration in microseconds, `i64::MAX` for one beyond it.
pub(crate) fn us(d: Duration) -> i64 {
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
        if protocol == Protocol::V2
            && p.protocol == Some(Protocol::V2)
            && let Some(seq) = p.cursor_seq
        {
            return Plan {
                cursor: Cursor::Seq(seq.saturating_add(1)),
                gap: GapRule::None,
            };
        }
        let base = p.cursor_us.unwrap_or(applied);
        let requested = base.saturating_sub(us(V1_REPLAY));
        return Plan {
            cursor: Cursor::TimeUs(requested),
            gap: GapRule::IfClamped {
                requested_us: requested,
                from_us: applied,
                cause: GapCause::Heuristic,
            },
        };
    }
    let gap_from = applied.saturating_sub(us(FAILOVER_GAP));
    match lag {
        Some(l) if l <= t.failover_max_lag => {
            let rewind = t
                .failover_rewind_min
                .max(l.saturating_add(FAILOVER_LAG_MARGIN));
            let requested = applied.saturating_sub(us(rewind));
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
            let clamped =
                clamped_notice || first_us.saturating_sub(requested_us) > us(t.gap_threshold);
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

    mod properties {
        use super::*;
        use proptest::prelude::*;

        fn duration() -> impl Strategy<Value = Duration> {
            prop_oneof![
                (0u64..7200).prop_map(Duration::from_secs),
                (0u64..10_000_000_000).prop_map(Duration::from_micros),
                (any::<u64>(), 0u32..1_000_000_000).prop_map(|(s, n)| Duration::new(s, n)),
                Just(Duration::MAX),
            ]
        }

        fn micros() -> impl Strategy<Value = i64> {
            prop_oneof![
                0i64..4_000_000_000_000_000,
                any::<i64>(),
                Just(i64::MIN),
                Just(i64::MAX),
            ]
        }

        fn protocol() -> impl Strategy<Value = Protocol> {
            prop_oneof![Just(Protocol::V1), Just(Protocol::V2)]
        }

        fn tuning() -> impl Strategy<Value = Tuning> {
            (duration(), duration(), duration()).prop_map(|(a, b, c)| Tuning {
                gap_threshold: a,
                failover_rewind_min: b,
                failover_max_lag: c,
            })
        }

        fn persisted() -> impl Strategy<Value = Persisted> {
            (
                prop::option::of(prop_oneof![Just(A), Just(B)]),
                prop::option::of(protocol()),
                prop::option::of(micros()),
                prop::option::of(micros()),
                prop::option::of(micros()),
            )
                .prop_map(|(url, protocol, seq, us, applied)| Persisted {
                    source_url: url.map(str::to_owned),
                    protocol,
                    cursor_seq: seq,
                    cursor_us: us,
                    applied_through_us: applied,
                })
        }

        fn rule() -> impl Strategy<Value = GapRule> {
            let cause = || prop_oneof![Just(GapCause::Heuristic), Just(GapCause::Failover)];
            prop_oneof![
                Just(GapRule::None),
                (micros(), cause()).prop_map(|(from_us, cause)| GapRule::Always { from_us, cause }),
                (micros(), micros(), cause()).prop_map(|(requested_us, from_us, cause)| {
                    GapRule::IfClamped {
                        requested_us,
                        from_us,
                        cause,
                    }
                }),
            ]
        }

        /// The table at the top of this module, written out with wide
        /// integers.
        fn expected(
            p: &Persisted,
            url: &str,
            proto: Protocol,
            lag: Option<Duration>,
            t: &Tuning,
        ) -> Plan {
            let sub = |a: i64, d: Duration| -> i64 {
                let d = i128::try_from(d.as_micros())
                    .unwrap_or(i128::MAX)
                    .min(i128::from(i64::MAX));
                i64::try_from((i128::from(a) - d).max(i128::from(i64::MIN))).unwrap()
            };
            let Some(applied) = p.applied_through_us else {
                return Plan {
                    cursor: Cursor::Live,
                    gap: GapRule::None,
                };
            };
            if p.source_url.as_deref() == Some(url) {
                return match (proto, p.protocol, p.cursor_seq) {
                    (Protocol::V2, Some(Protocol::V2), Some(seq)) => Plan {
                        cursor: Cursor::Seq(seq.saturating_add(1)),
                        gap: GapRule::None,
                    },
                    _ => {
                        let requested = sub(p.cursor_us.unwrap_or(applied), V1_REPLAY);
                        Plan {
                            cursor: Cursor::TimeUs(requested),
                            gap: GapRule::IfClamped {
                                requested_us: requested,
                                from_us: applied,
                                cause: GapCause::Heuristic,
                            },
                        }
                    }
                };
            }
            let floor = sub(applied, FAILOVER_GAP);
            match lag.filter(|l| *l <= t.failover_max_lag) {
                Some(l) => {
                    let rewind = t
                        .failover_rewind_min
                        .max(l.saturating_add(FAILOVER_LAG_MARGIN));
                    let requested = sub(applied, rewind);
                    Plan {
                        cursor: Cursor::TimeUs(requested),
                        gap: GapRule::IfClamped {
                            requested_us: requested,
                            from_us: floor,
                            cause: GapCause::Failover,
                        },
                    }
                }
                None => Plan {
                    cursor: Cursor::TimeUs(floor),
                    gap: GapRule::Always {
                        from_us: floor,
                        cause: GapCause::Failover,
                    },
                },
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(512))]

            /// For any persisted state, instance, protocol, lag and
            /// tuning the plan is the table's, and so: nothing applied
            /// means the live tail without a gap; a timestamp cursor is
            /// never after the position it rewinds from; a failover gap
            /// starts 30 minutes before `applied_through`; and a gap is
            /// unconditional exactly on a failover without a usable lag.
            #[test]
            fn the_plan_is_total_and_is_the_documented_one(
                p in persisted(),
                url in prop_oneof![Just(A), Just(B)],
                proto in protocol(),
                lag in prop::option::of(duration()),
                t in tuning(),
            ) {
                let plan = plan(&p, url, proto, lag, &t);
                prop_assert_eq!(plan, expected(&p, url, proto, lag, &t));
                let same = p.source_url.as_deref() == Some(url);
                let Some(applied) = p.applied_through_us else {
                    prop_assert_eq!(plan, Plan { cursor: Cursor::Live, gap: GapRule::None });
                    return Ok(());
                };
                match plan.cursor {
                    Cursor::Live => prop_assert!(false, "live tail with a position"),
                    Cursor::Seq(s) => {
                        prop_assert!(same && proto == Protocol::V2 && p.protocol == Some(Protocol::V2));
                        prop_assert!(p.cursor_seq.is_some_and(|c| s >= c));
                        prop_assert_eq!(plan.gap, GapRule::None);
                    }
                    Cursor::TimeUs(c) => {
                        let from = if same { p.cursor_us.unwrap_or(applied) } else { applied };
                        prop_assert!(c <= from);
                        if same {
                            // Never further back than the replay margin.
                            prop_assert!(i128::from(from) - i128::from(c) <= i128::from(us(V1_REPLAY)));
                        }
                    }
                }
                match plan.gap {
                    GapRule::None => prop_assert!(matches!(plan.cursor, Cursor::Seq(_))),
                    GapRule::IfClamped { requested_us, from_us, cause } => {
                        prop_assert_eq!(plan.cursor, Cursor::TimeUs(requested_us));
                        if same {
                            prop_assert_eq!((from_us, cause), (applied, GapCause::Heuristic));
                        } else {
                            prop_assert_eq!(cause, GapCause::Failover);
                            prop_assert_eq!(from_us, applied.saturating_sub(us(FAILOVER_GAP)));
                            prop_assert!(lag.is_some_and(|l| l <= t.failover_max_lag));
                        }
                    }
                    GapRule::Always { from_us, cause } => {
                        prop_assert!(!same && lag.is_none_or(|l| l > t.failover_max_lag));
                        prop_assert_eq!(cause, GapCause::Failover);
                        prop_assert_eq!(plan.cursor, Cursor::TimeUs(from_us));
                        prop_assert_eq!(from_us, applied.saturating_sub(us(FAILOVER_GAP)));
                    }
                }
            }

            /// For any rule, first event and tuning: a gap is recorded
            /// exactly when the rule says, it ends at the first event and
            /// never starts after it.
            #[test]
            fn a_gap_is_recorded_exactly_when_the_rule_says(
                rule in rule(),
                first in micros(),
                notice in any::<bool>(),
                t in tuning(),
            ) {
                let gap = gap_for_first_event(rule, first, notice, &t);
                let expected = match rule {
                    GapRule::None => None,
                    GapRule::Always { from_us, cause } => Some((from_us.min(first), first, cause)),
                    GapRule::IfClamped { requested_us, from_us, cause } => {
                        let late = i128::from(first) - i128::from(requested_us)
                            > i128::try_from(t.gap_threshold.as_micros()).unwrap_or(i128::MAX);
                        ((notice || late) && first > from_us).then_some((from_us, first, cause))
                    }
                };
                // The code saturates where the model is exact: the two can
                // differ only when `first − requested` leaves the `i64` range.
                if first.checked_sub(rule_requested(rule).unwrap_or(0)).is_some() {
                    prop_assert_eq!(gap, expected);
                }
                if let Some((from, to, _)) = gap {
                    prop_assert!(from <= to && to == first);
                }
            }

            /// The plan and its gap together: whatever the first event of
            /// the session is, a recorded gap never starts after
            /// `applied_through`.
            #[test]
            fn a_recorded_gap_never_starts_after_the_applied_position(
                p in persisted(),
                url in prop_oneof![Just(A), Just(B)],
                proto in protocol(),
                lag in prop::option::of(duration()),
                t in tuning(),
                first in micros(),
                notice in any::<bool>(),
            ) {
                let plan = plan(&p, url, proto, lag, &t);
                let gap = gap_for_first_event(plan.gap, first, notice, &t);
                match (gap, p.applied_through_us) {
                    (Some((from, to, _)), Some(applied)) => {
                        prop_assert!(from <= applied && from <= to && to == first);
                    }
                    (Some(_), None) => prop_assert!(false, "gap on a first start"),
                    (None, _) => {}
                }
            }
        }

        fn rule_requested(rule: GapRule) -> Option<i64> {
            match rule {
                GapRule::IfClamped { requested_us, .. } => Some(requested_us),
                _ => None,
            }
        }
    }
}
