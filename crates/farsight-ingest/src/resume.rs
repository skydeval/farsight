//! Where to resume, and which gap to record (see
//! `docs/design/firehose.md`), as pure functions over the persisted
//! state.
//!
//! | Situation | Cursor | Gap rule |
//! |---|---|---|
//! | first start (nothing applied) | live tail | none |
//! | instance with its own cursor, v2 → v2 | `seq + 1` (exact) | gap `[from, first event]` if the instance announces a clamp (`#info OutdatedCursor`), answers with a `seq` below the one asked for (its sequence started again; the stored cursor is dropped), or its first event is more than `gap_threshold` after the stored cursor |
//! | instance with its own cursor, v1 (or v1 → v2) | `cursor_us − 120 s` | gap `[from, first event]` if a clamp is announced or the first event is later than the stored cursor: the instance no longer holds what it sent before |
//! | instance without a cursor (failover), lag known and ≤ `failover_max_lag` | `applied − max(failover_rewind_min, lag + 5 min) − gap_threshold` | gap `[applied − 30 min, first event]` if a clamp is announced or the first event is more than `gap_threshold` after the cursor, that is, later than `applied − max(failover_rewind_min, lag + 5 min)` |
//! | instance without a cursor, lag unknown or too large | `applied − 30 min` | always gap `[applied − 30 min, first event]` |
//!
//! `from` is `applied_through` when the last applied batch came from the
//! instance being resumed, and `applied_through − 30 min` otherwise: two
//! instances are not equally far behind the network. An instance that
//! refuses the cursor (`CursorTooOld`) is read from the live tail and the
//! gap `[from, first live event]` is always recorded.
//!
//! A conditional gap is recorded only when the first event lies after
//! `from`; one that lies before it replays what was already applied. An
//! unconditional gap is never empty: when the first event is not after
//! `from`, the two clocks disagree, and the gap is the 30 minutes before
//! that event.
//!
//! The failover cursor asks for `gap_threshold` more than the rewind the
//! change of instance needs. A clamp shorter than `gap_threshold`
//! cannot be told from a quiet stream and records no gap; it then eats
//! only that extra, never the rewind.
//!
//! `applied_through` is taken as at most the current time wherever a
//! position is computed for another instance, so a witness clock that
//! ran ahead cannot place a cursor in the future.

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
    /// The instance the cursor below belongs to (`firehose_cursors`), if
    /// the instance being resumed has one.
    pub source_url: Option<String>,
    /// `firehose_cursors.protocol`.
    pub protocol: Option<Protocol>,
    /// `cursor_seq` (v2).
    pub cursor_seq: Option<i64>,
    /// `cursor_us` (v1 / witness µs).
    pub cursor_us: Option<i64>,
    /// `applied_through`, µs.
    pub applied_through_us: Option<i64>,
    /// The instance the last applied batch was read from
    /// (`firehose_state.source_url`).
    pub applied_from: Option<String>,
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
    /// First start: no gap.
    None,
    /// A resume by `seq`. Gap `[from_us, first event]` if the instance
    /// announced a clamp, its first `seq` is below `requested_seq`, or
    /// its first event's witness is after `clamp_after_us`, and the
    /// first event lies after `from_us`.
    Exact {
        /// The `seq` sent as the cursor.
        requested_seq: i64,
        /// A first event witnessed after this was not the next one.
        clamp_after_us: i64,
        /// Where the gap starts if one is recorded, witness µs.
        from_us: i64,
    },
    /// A resume by timestamp. Gap `[from_us, first event]` if the cursor
    /// was clamped (the server announced it, or the first event's
    /// witness is after `clamp_after_us`) and the first event lies after
    /// `from_us`.
    IfClamped {
        /// A first event witnessed after this means the instance started
        /// later than asked.
        clamp_after_us: i64,
        /// Where the gap starts if one is recorded, witness µs.
        from_us: i64,
        /// Cause to record.
        cause: GapCause,
    },
    /// Always record a gap from `from_us` to the first event.
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
    /// Where the gap starts if the instance refuses the cursor as too
    /// old, witness µs. `None` on a first start, which sends no cursor.
    pub refused_from_us: Option<i64>,
}

impl Plan {
    /// The gap rule of the live-tail session that follows a refused
    /// cursor.
    pub fn refused(&self) -> GapRule {
        match self.refused_from_us {
            Some(from_us) => GapRule::Always {
                from_us,
                cause: GapCause::CursorTooOld,
            },
            None => GapRule::None,
        }
    }
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

/// Plans the resume against instance `url` speaking `protocol`, at
/// `now_us` by this machine's clock. `lag` is the trailing median
/// `witness − rev time` of the previous instance (`None` if unmeasured),
/// used only on failover.
pub fn plan(
    p: &Persisted,
    url: &str,
    protocol: Protocol,
    lag: Option<Duration>,
    t: &Tuning,
    now_us: i64,
) -> Plan {
    let Some(applied) = p.applied_through_us else {
        return Plan {
            cursor: Cursor::Live,
            gap: GapRule::None,
            refused_from_us: None,
        };
    };
    // The position as another instance can be asked for it.
    let base = applied.min(now_us);
    let failover_from = base.saturating_sub(us(FAILOVER_GAP));
    let same_instance = p.source_url.as_deref() == Some(url);
    if same_instance {
        let applied_here = p.applied_from.as_deref().is_none_or(|u| u == url);
        let from_us = if applied_here { applied } else { failover_from };
        let stored_us = p.cursor_us.unwrap_or(applied);
        if protocol == Protocol::V2
            && p.protocol == Some(Protocol::V2)
            && let Some(seq) = p.cursor_seq
        {
            let requested_seq = seq.saturating_add(1);
            return Plan {
                cursor: Cursor::Seq(requested_seq),
                gap: GapRule::Exact {
                    requested_seq,
                    clamp_after_us: stored_us.saturating_add(us(t.gap_threshold)),
                    from_us,
                },
                refused_from_us: Some(from_us),
            };
        }
        let requested = stored_us.min(now_us).saturating_sub(us(V1_REPLAY));
        return Plan {
            cursor: Cursor::TimeUs(requested),
            gap: GapRule::IfClamped {
                clamp_after_us: stored_us,
                from_us,
                cause: GapCause::Heuristic,
            },
            refused_from_us: Some(from_us),
        };
    }
    match lag {
        Some(l) if l <= t.failover_max_lag => {
            let rewind = t
                .failover_rewind_min
                .max(l.saturating_add(FAILOVER_LAG_MARGIN));
            // The rewind is what the change of instance needs. A clamp
            // can only be told from a quiet stream when it is longer than
            // `gap_threshold`, so that much more is asked for: a clamp
            // too short to see then costs only the extra, and a first
            // event later than the rewind itself is always a gap.
            let needed_from = base.saturating_sub(us(rewind));
            let requested = needed_from.saturating_sub(us(t.gap_threshold));
            Plan {
                cursor: Cursor::TimeUs(requested),
                gap: GapRule::IfClamped {
                    clamp_after_us: needed_from,
                    from_us: failover_from,
                    cause: GapCause::Failover,
                },
                refused_from_us: Some(failover_from),
            }
        }
        _ => Plan {
            cursor: Cursor::TimeUs(failover_from),
            gap: GapRule::Always {
                from_us: failover_from,
                cause: GapCause::Failover,
            },
            refused_from_us: Some(failover_from),
        },
    }
}

/// What the first event of a resumed session says about the resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Resumed {
    /// The gap to record `(from, to, cause)`, witness µs.
    pub gap: Option<(i64, i64, GapCause)>,
    /// The instance answered a `seq` resume with a lower `seq`: its
    /// sequence started again, and the cursor stored for it describes a
    /// stream that no longer exists.
    pub sequence_restarted: bool,
}

/// Reads the resume off the first event (`first_seq` on v2, witness
/// `first_us`). `clamped_notice` is true when the server sent `#info
/// OutdatedCursor` before it.
pub fn first_event(
    rule: GapRule,
    first_seq: Option<i64>,
    first_us: i64,
    clamped_notice: bool,
) -> Resumed {
    match rule {
        GapRule::None => Resumed::default(),
        GapRule::Always { from_us, cause } => {
            let from = if from_us < first_us {
                from_us
            } else {
                first_us.saturating_sub(us(FAILOVER_GAP))
            };
            Resumed {
                gap: Some((from, first_us, cause)),
                sequence_restarted: false,
            }
        }
        GapRule::IfClamped {
            clamp_after_us,
            from_us,
            cause,
        } => {
            // A clamp loses data only if the server's first event is past
            // our position; if it replayed from an older floor, nothing
            // was skipped.
            let clamped = clamped_notice || first_us > clamp_after_us;
            Resumed {
                gap: (clamped && first_us > from_us).then_some((from_us, first_us, cause)),
                sequence_restarted: false,
            }
        }
        GapRule::Exact {
            requested_seq,
            clamp_after_us,
            from_us,
        } => {
            let restarted = first_seq.is_some_and(|s| s < requested_seq);
            let cause = if clamped_notice {
                Some(GapCause::CursorTooOld)
            } else if restarted || first_us > clamp_after_us {
                Some(GapCause::Heuristic)
            } else {
                None
            };
            Resumed {
                gap: cause
                    .filter(|_| first_us > from_us)
                    .map(|c| (from_us, first_us, c)),
                sequence_restarted: restarted,
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
    /// The clock, well after the persisted position.
    const NOW: i64 = 50_000 * S;

    fn persisted(proto: Protocol) -> Persisted {
        Persisted {
            source_url: Some(A.into()),
            protocol: Some(proto),
            cursor_seq: Some(500),
            cursor_us: Some(10_000 * S),
            applied_through_us: Some(10_000 * S),
            applied_from: Some(A.into()),
        }
    }

    /// The state a failover to an instance never read from starts with.
    fn elsewhere(proto: Protocol) -> Persisted {
        Persisted {
            source_url: None,
            cursor_seq: None,
            cursor_us: None,
            ..persisted(proto)
        }
    }

    fn gap(
        rule: GapRule,
        seq: Option<i64>,
        first_us: i64,
        notice: bool,
    ) -> Option<(i64, i64, GapCause)> {
        first_event(rule, seq, first_us, notice).gap
    }

    #[test]
    fn first_start_is_live_without_gap() {
        let p = plan(&Persisted::default(), A, Protocol::V1, None, &T, NOW);
        assert_eq!(
            p,
            Plan {
                cursor: Cursor::Live,
                gap: GapRule::None,
                refused_from_us: None,
            }
        );
        assert_eq!(p.refused(), GapRule::None);
        assert_eq!(first_event(p.gap, Some(1), NOW, true), Resumed::default());
    }

    #[test]
    fn same_instance_v2_resumes_exactly() {
        let p = plan(&persisted(Protocol::V2), A, Protocol::V2, None, &T, NOW);
        assert_eq!(p.cursor, Cursor::Seq(501));
        // The next event, soon after the stored one: nothing to record.
        assert_eq!(
            first_event(p.gap, Some(501), 10_001 * S, false),
            Resumed::default()
        );
        assert_eq!(
            first_event(p.gap, Some(9_000), 10_200 * S, false),
            Resumed::default()
        );
    }

    #[test]
    fn an_announced_clamp_on_a_seq_resume_is_a_gap() {
        let p = plan(&persisted(Protocol::V2), A, Protocol::V2, None, &T, NOW);
        // The instance kept less than the outage: it says so and goes on
        // from its floor.
        assert_eq!(
            gap(p.gap, Some(90_000), 30_000 * S, true),
            Some((10_000 * S, 30_000 * S, GapCause::CursorTooOld))
        );
        // Announced even when the floor is close to the stored cursor.
        assert_eq!(
            gap(p.gap, Some(600), 10_002 * S, true),
            Some((10_000 * S, 10_002 * S, GapCause::CursorTooOld))
        );
        // A floor at or before the position replays it: nothing was lost.
        assert_eq!(gap(p.gap, Some(600), 10_000 * S, true), None);
        assert_eq!(gap(p.gap, Some(600), 9_000 * S, true), None);
    }

    #[test]
    fn a_sequence_that_started_again_is_detected() {
        let p = plan(&persisted(Protocol::V2), A, Protocol::V2, None, &T, NOW);
        // Asked for 501, answered from the live tail of a new sequence.
        let r = first_event(p.gap, Some(7), 20_000 * S, false);
        assert!(r.sequence_restarted);
        assert_eq!(r.gap, Some((10_000 * S, 20_000 * S, GapCause::Heuristic)));
        // A new sequence replayed from before the position loses nothing,
        // and the stored cursor is still wrong.
        let r = first_event(p.gap, Some(7), 9_000 * S, false);
        assert!(r.sequence_restarted);
        assert_eq!(r.gap, None);
        // A new sequence already past the stored one: only the witness
        // time shows that the stream did not continue.
        let r = first_event(p.gap, Some(501), 20_000 * S, false);
        assert!(!r.sequence_restarted);
        assert_eq!(r.gap, Some((10_000 * S, 20_000 * S, GapCause::Heuristic)));
        // Within the threshold the next event is taken as the next event.
        assert_eq!(gap(p.gap, Some(501), 10_300 * S, false), None);
        assert!(gap(p.gap, Some(501), 10_300 * S + 1, false).is_some());
    }

    #[test]
    fn same_instance_v1_rewinds_120s_and_detects_clamps() {
        let p = plan(&persisted(Protocol::V1), A, Protocol::V1, None, &T, NOW);
        assert_eq!(p.cursor, Cursor::TimeUs(10_000 * S - 120 * S));
        // The replay starts inside the window already applied: no gap.
        assert_eq!(gap(p.gap, None, 9_890 * S, false), None);
        assert_eq!(gap(p.gap, None, 10_000 * S, false), None);
        // The instance no longer holds the event the cursor was taken
        // from: everything between it and the first event is missing,
        // however little that is.
        assert_eq!(
            gap(p.gap, None, 10_000 * S + 1, false),
            Some((10_000 * S, 10_000 * S + 1, GapCause::Heuristic))
        );
        assert_eq!(
            gap(p.gap, None, 10_170 * S, false),
            Some((10_000 * S, 10_170 * S, GapCause::Heuristic))
        );
        assert_eq!(
            gap(p.gap, None, 20_000 * S, false),
            Some((10_000 * S, 20_000 * S, GapCause::Heuristic))
        );
    }

    #[test]
    fn protocol_upgrade_on_same_instance_uses_timestamp() {
        let p = plan(&persisted(Protocol::V1), A, Protocol::V2, None, &T, NOW);
        assert!(matches!(p.cursor, Cursor::TimeUs(_)));
        assert!(matches!(p.gap, GapRule::IfClamped { .. }));
        // An OutdatedCursor notice records the gap as long as the first
        // event is past our position.
        assert!(gap(p.gap, Some(3), 10_001 * S, true).is_some());
        // A clamp to a floor older than our position loses nothing.
        assert!(gap(p.gap, Some(3), 9_000 * S, true).is_none());
    }

    #[test]
    fn failover_with_measured_lag() {
        let p = plan(
            &elsewhere(Protocol::V2),
            B,
            Protocol::V2,
            Some(Duration::from_secs(60)),
            &T,
            NOW,
        );
        // max(10 min, 1 min + 5 min) = 10 min, and the 5 min of the
        // threshold on top.
        assert_eq!(p.cursor, Cursor::TimeUs(10_000 * S - 600 * S - 300 * S));
        let p = plan(
            &elsewhere(Protocol::V2),
            B,
            Protocol::V2,
            Some(Duration::from_secs(900)),
            &T,
            NOW,
        );
        // max(10 min, 15 min + 5 min) = 20 min, and the threshold on top.
        assert_eq!(p.cursor, Cursor::TimeUs(10_000 * S - 1200 * S - 300 * S));
        assert_eq!(gap(p.gap, Some(1), 8_600 * S, false), None);
        assert_eq!(
            gap(p.gap, Some(1), 20_000 * S, false),
            Some((10_000 * S - 1800 * S, 20_000 * S, GapCause::Failover))
        );
    }

    #[test]
    fn a_failover_clamp_never_eats_the_rewind_without_a_gap() {
        let p = plan(
            &elsewhere(Protocol::V2),
            B,
            Protocol::V2,
            Some(Duration::from_secs(900)),
            &T,
            NOW,
        );
        // The change of instance needs the 20 minutes before the position.
        let needed_from = 10_000 * S - 1200 * S;
        let asked = needed_from - 300 * S;
        assert_eq!(p.cursor, Cursor::TimeUs(asked));
        // A first event anywhere in the extra five minutes: a clamp that
        // short, or a quiet stream. The whole rewind was replayed.
        for first in [asked, asked + 150 * S, needed_from] {
            assert_eq!(gap(p.gap, Some(1), first, false), None, "{first}");
        }
        // One microsecond into the rewind: part of it was not replayed,
        // and that is a gap, however little.
        assert_eq!(
            gap(p.gap, Some(1), needed_from + 1, false),
            Some((10_000 * S - 1800 * S, needed_from + 1, GapCause::Failover))
        );
        assert_eq!(
            gap(p.gap, Some(1), needed_from + 200 * S, false),
            Some((
                10_000 * S - 1800 * S,
                needed_from + 200 * S,
                GapCause::Failover
            ))
        );
    }

    #[test]
    fn failover_without_usable_lag_always_records_a_gap() {
        for lag in [None, Some(Duration::from_secs(3600))] {
            let p = plan(&elsewhere(Protocol::V1), B, Protocol::V1, lag, &T, NOW);
            assert_eq!(p.cursor, Cursor::TimeUs(10_000 * S - 1800 * S));
            assert_eq!(
                gap(p.gap, None, 9_000 * S, false),
                Some((8_200 * S, 9_000 * S, GapCause::Failover))
            );
        }
    }

    #[test]
    fn a_refused_cursor_starts_its_gap_where_the_plan_does() {
        // The instance the position was read from: the gap starts at it.
        let p = plan(&persisted(Protocol::V2), A, Protocol::V2, None, &T, NOW);
        assert_eq!(
            p.refused(),
            GapRule::Always {
                from_us: 10_000 * S,
                cause: GapCause::CursorTooOld
            }
        );
        // An instance never read from, and one returned to while the
        // position came from another: 30 minutes earlier.
        let fresh = plan(&elsewhere(Protocol::V2), B, Protocol::V2, None, &T, NOW);
        let back = Persisted {
            cursor_us: Some(4_000 * S),
            applied_from: Some(B.into()),
            ..persisted(Protocol::V2)
        };
        let back = plan(&back, A, Protocol::V2, None, &T, NOW);
        assert_eq!(back.cursor, Cursor::Seq(501));
        for p in [fresh, back] {
            assert_eq!(
                p.refused(),
                GapRule::Always {
                    from_us: 8_200 * S,
                    cause: GapCause::CursorTooOld
                }
            );
        }
        // And so does a clamp announced on the way back.
        assert_eq!(
            gap(back.gap, Some(900), 9_000 * S, true),
            Some((8_200 * S, 9_000 * S, GapCause::CursorTooOld))
        );
    }

    #[test]
    fn a_position_ahead_of_the_clock_does_not_hide_a_failover() {
        // The instance left had a clock 4 minutes fast.
        let now = 10_000 * S - 240 * S;
        let lagging = plan(
            &elsewhere(Protocol::V2),
            B,
            Protocol::V2,
            Some(Duration::from_secs(60)),
            &T,
            now,
        );
        // The cursor is rewound from the clock, not from the position.
        assert_eq!(lagging.cursor, Cursor::TimeUs(now - 600 * S - 300 * S));
        let blind = plan(&elsewhere(Protocol::V2), B, Protocol::V2, None, &T, now);
        assert_eq!(blind.cursor, Cursor::TimeUs(now - 1800 * S));
        assert_eq!(
            gap(blind.gap, Some(1), now - 1700 * S, false),
            Some((now - 1800 * S, now - 1700 * S, GapCause::Failover))
        );
        // A gap that must be recorded is never empty.
        let always = GapRule::Always {
            from_us: 10_000 * S,
            cause: GapCause::CursorTooOld,
        };
        assert_eq!(
            gap(always, Some(1), 9_000 * S, false),
            Some((9_000 * S - 1800 * S, 9_000 * S, GapCause::CursorTooOld))
        );
        assert_eq!(
            gap(always, Some(1), 10_000 * S, false),
            Some((10_000 * S - 1800 * S, 10_000 * S, GapCause::CursorTooOld))
        );
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

        fn instance() -> impl Strategy<Value = &'static str> {
            prop_oneof![Just(A), Just(B)]
        }

        fn persisted() -> impl Strategy<Value = Persisted> {
            (
                prop::option::of(instance()),
                prop::option::of(protocol()),
                prop::option::of(micros()),
                prop::option::of(micros()),
                prop::option::of(micros()),
                prop::option::of(instance()),
            )
                .prop_map(|(url, protocol, seq, us, applied, from)| Persisted {
                    source_url: url.map(str::to_owned),
                    protocol,
                    cursor_seq: seq,
                    cursor_us: us,
                    applied_through_us: applied,
                    applied_from: from.map(str::to_owned),
                })
        }

        fn rule() -> impl Strategy<Value = GapRule> {
            let cause = || prop_oneof![Just(GapCause::Heuristic), Just(GapCause::Failover)];
            prop_oneof![
                Just(GapRule::None),
                (micros(), cause()).prop_map(|(from_us, cause)| GapRule::Always { from_us, cause }),
                (micros(), micros(), cause()).prop_map(|(clamp_after_us, from_us, cause)| {
                    GapRule::IfClamped {
                        clamp_after_us,
                        from_us,
                        cause,
                    }
                }),
                (micros(), micros(), micros()).prop_map(
                    |(requested_seq, clamp_after_us, from_us)| GapRule::Exact {
                        requested_seq,
                        clamp_after_us,
                        from_us,
                    }
                ),
            ]
        }

        /// `a − d`, exact, held to the `i64` range.
        fn sub(a: i64, d: Duration) -> i64 {
            let d = i128::try_from(d.as_micros())
                .unwrap_or(i128::MAX)
                .min(i128::from(i64::MAX));
            i64::try_from((i128::from(a) - d).max(i128::from(i64::MIN))).unwrap()
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(512))]

            /// For any persisted state, instance, protocol, lag, tuning
            /// and clock: nothing applied means the live tail without a
            /// gap; a `seq` cursor is sent only to the instance it came
            /// from, on v2; a timestamp cursor is never after the
            /// position nor after the clock; a gap starts at
            /// `applied_through` only on the instance the position came
            /// from, and 30 minutes before the position or the clock
            /// otherwise; and a gap is unconditional exactly on an
            /// instance without a cursor and without a usable lag.
            #[test]
            fn the_plan_is_total_and_is_the_documented_one(
                p in persisted(),
                url in instance(),
                proto in protocol(),
                lag in prop::option::of(duration()),
                t in tuning(),
                now in micros(),
            ) {
                let plan = plan(&p, url, proto, lag, &t, now);
                let same = p.source_url.as_deref() == Some(url);
                let Some(applied) = p.applied_through_us else {
                    prop_assert_eq!(
                        plan,
                        Plan { cursor: Cursor::Live, gap: GapRule::None, refused_from_us: None }
                    );
                    return Ok(());
                };
                let here = same && p.applied_from.as_deref().is_none_or(|u| u == url);
                let from = if here { applied } else { sub(applied.min(now), FAILOVER_GAP) };
                prop_assert_eq!(plan.refused_from_us, Some(from));
                prop_assert_eq!(
                    plan.refused(),
                    GapRule::Always { from_us: from, cause: GapCause::CursorTooOld }
                );
                match plan.cursor {
                    Cursor::Live => prop_assert!(false, "live tail with a position"),
                    Cursor::Seq(s) => {
                        prop_assert!(same && proto == Protocol::V2 && p.protocol == Some(Protocol::V2));
                        prop_assert!(p.cursor_seq.is_some_and(|c| s >= c));
                        let exact = matches!(
                            plan.gap,
                            GapRule::Exact { requested_seq, .. } if requested_seq == s
                        );
                        prop_assert!(exact);
                    }
                    Cursor::TimeUs(c) => {
                        let stored = if same { p.cursor_us.unwrap_or(applied) } else { applied };
                        prop_assert!(c <= stored && c <= now);
                        if same {
                            // Never further back than the replay margin.
                            prop_assert_eq!(c, sub(stored.min(now), V1_REPLAY));
                        }
                    }
                }
                match plan.gap {
                    GapRule::None => prop_assert!(false, "no rule with a position"),
                    GapRule::Exact { from_us, .. } => {
                        prop_assert!(same);
                        prop_assert_eq!(from_us, from);
                    }
                    GapRule::IfClamped { clamp_after_us, from_us, cause } => {
                        prop_assert_eq!(from_us, from);
                        if same {
                            prop_assert_eq!(cause, GapCause::Heuristic);
                            prop_assert_eq!(clamp_after_us, p.cursor_us.unwrap_or(applied));
                        } else {
                            prop_assert_eq!(cause, GapCause::Failover);
                            prop_assert!(lag.is_some_and(|l| l <= t.failover_max_lag));
                            // The cursor asks for the threshold more than
                            // the rewind, so a clamp too short to see
                            // costs none of the rewind.
                            let rewind = t
                                .failover_rewind_min
                                .max(lag.unwrap_or_default().saturating_add(FAILOVER_LAG_MARGIN));
                            prop_assert_eq!(clamp_after_us, sub(applied.min(now), rewind));
                            prop_assert_eq!(
                                plan.cursor,
                                Cursor::TimeUs(sub(clamp_after_us, t.gap_threshold))
                            );
                        }
                    }
                    GapRule::Always { from_us, cause } => {
                        prop_assert!(!same && lag.is_none_or(|l| l > t.failover_max_lag));
                        prop_assert_eq!(cause, GapCause::Failover);
                        prop_assert_eq!(plan.cursor, Cursor::TimeUs(from_us));
                        prop_assert_eq!(from_us, from);
                    }
                }
            }

            /// For any rule and first event: an unconditional gap is
            /// always recorded and is never empty; an announced clamp
            /// is a gap whenever the first event lies after the gap's
            /// start, on every rule that sent a cursor; a recorded gap
            /// ends at the first event and starts before it; and a
            /// sequence counts as started again exactly when a `seq`
            /// resume is answered with a lower `seq`.
            #[test]
            fn the_first_event_decides_as_the_rule_says(
                rule in rule(),
                seq in prop::option::of(micros()),
                first in micros(),
                notice in any::<bool>(),
            ) {
                let r = first_event(rule, seq, first, notice);
                if let Some((from, to, _)) = r.gap {
                    prop_assert!(to == first);
                    prop_assert!(from < to || first == i64::MIN);
                }
                match rule {
                    GapRule::None => prop_assert_eq!(r, Resumed::default()),
                    GapRule::Always { from_us, cause } => {
                        let (from, _, c) = r.gap.expect("always a gap");
                        prop_assert_eq!(c, cause);
                        prop_assert!(from <= from_us);
                        prop_assert!(!r.sequence_restarted);
                    }
                    GapRule::IfClamped { clamp_after_us, from_us, cause } => {
                        let clamped = notice || first > clamp_after_us;
                        prop_assert_eq!(
                            r.gap,
                            (clamped && first > from_us).then_some((from_us, first, cause))
                        );
                        prop_assert!(!r.sequence_restarted);
                    }
                    GapRule::Exact { requested_seq, clamp_after_us, from_us } => {
                        let restarted = seq.is_some_and(|s| s < requested_seq);
                        prop_assert_eq!(r.sequence_restarted, restarted);
                        let lost = notice || restarted || first > clamp_after_us;
                        prop_assert_eq!(r.gap.is_some(), lost && first > from_us);
                        if let Some((from, _, cause)) = r.gap {
                            prop_assert_eq!(from, from_us);
                            prop_assert_eq!(
                                cause == GapCause::CursorTooOld,
                                notice
                            );
                        }
                    }
                }
            }

            /// The plan and its first event together: a recorded gap
            /// never starts after `applied_through`, whether the cursor
            /// was accepted or refused, and a refused cursor always
            /// records one.
            #[test]
            fn a_recorded_gap_never_starts_after_the_applied_position(
                p in persisted(),
                url in instance(),
                proto in protocol(),
                lag in prop::option::of(duration()),
                t in tuning(),
                now in micros(),
                seq in prop::option::of(micros()),
                first in micros(),
                notice in any::<bool>(),
            ) {
                let plan = plan(&p, url, proto, lag, &t, now);
                let accepted = first_event(plan.gap, seq, first, notice).gap;
                let refused = first_event(plan.refused(), seq, first, notice).gap;
                match p.applied_through_us {
                    Some(applied) => {
                        prop_assert!(refused.is_some());
                        for (from, to, _) in [accepted, refused].into_iter().flatten() {
                            prop_assert!(from <= applied && from <= to && to == first);
                        }
                    }
                    None => prop_assert_eq!((accepted, refused), (None, None)),
                }
            }
        }
    }
}
