//! The list transition function (see `docs/design/list-indexing.md`), as a
//! pure function.
//!
//! `transition(facts, event, ctx)` returns the new state, the new
//! `purge_then`, and the side effects the caller applies under the list
//! lock (see `crate::tracking`). Every cell of the transition table is
//! one match arm below and one unit test.
//!
//! Where the table leaves something implicit, the interpretation is
//! documented at the arm:
//! - "stay" cells return the unchanged state with `changed = false` and a
//!   [`Effect::StayRetry`] marker where the table says "(retry)".
//! - The owner re-admission budget for **DV** on `ready`/`retained` is
//!   charged when DV fires (the purge target becomes `untracked`, and PD
//!   re-admits uncharged); over budget, the purge target is `deferred`
//!   with cause `OwnerReadmissions`. This keeps the PD cell free of any
//!   memory of why the purge started.

use crate::codes::{DeferCause, RecordState, TrackState};

/// The events of the transition table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Event {
    /// **+**: counted listblocks 0 → ≥ 1.
    Plus,
    /// **−**: counted listblocks ≥ 1 → 0.
    Minus,
    /// **RP**: list record present (apply or record check).
    RecordPresent,
    /// **RD**: list record deleted (firehose delete, owner purge).
    RecordDeleted,
    /// **NF**: record check authoritatively not found.
    NotFound,
    /// **NFx**: not-found retries exhausted.
    NotFoundExhausted,
    /// **GF**: a gate failed (phase-1 host gate, budget, ceiling, lists cap).
    GateFail(DeferCause),
    /// **GO**: gate reopened.
    GateOpen,
    /// **OK**: a run promotes the list.
    Ok,
    /// **FT**: run fails terminally, or `pending_max_age` exceeded.
    FailTerminal,
    /// **OI**: owner inactive at phase 1 or during a run.
    OwnerInactive,
    /// **OA**: owner reactivated.
    OwnerActive,
    /// **DV**: owner repo diverged.
    Diverged,
    /// **GE**: grace expired.
    GraceExpired,
    /// **PD**: purge done.
    PurgeDone,
}

impl Event {
    /// Every event, with `GateFail` represented once.
    pub const ALL: [Event; 15] = [
        Event::Plus,
        Event::Minus,
        Event::RecordPresent,
        Event::RecordDeleted,
        Event::NotFound,
        Event::NotFoundExhausted,
        Event::GateFail(DeferCause::Budget),
        Event::GateOpen,
        Event::Ok,
        Event::FailTerminal,
        Event::OwnerInactive,
        Event::OwnerActive,
        Event::Diverged,
        Event::GraceExpired,
        Event::PurgeDone,
    ];
}

/// The list columns the transition function reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListFacts {
    /// `track_state`.
    pub state: TrackState,
    /// `record_state`.
    pub record_state: RecordState,
    /// `listblock_count` **after** the change that fired the event.
    pub listblock_count: i32,
    /// `purge_then` (meaningful while `purging`).
    pub purge_then: Option<TrackState>,
}

/// Inputs from outside the list row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ctx {
    /// Whether the owner has owner-caused re-admissions left today
    /// (`limits.owner_readmissions_per_day`).
    pub owner_readmit_available: bool,
}

/// Side effects the caller applies, in order, under list(L) exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// **admit**: `admit_epoch += 1`, `admitted_at = now()`, clear
    /// `fetch_run_id`/`fetched_at`, state `pending`, enqueue phase 1,
    /// rebuild scheduling lanes.
    Admit,
    /// Charge one owner-caused re-admission to the owner's daily budget.
    ChargeOwnerReadmission,
    /// Enter `deferred` with the given cause (`next_retry_at` next UTC day
    /// for the owner budget).
    Defer(DeferCause),
    /// **purge→X**: state `purging`, `purge_then` set, claim cleared; the
    /// janitor deletes items and later fires PD.
    BeginPurge,
    /// PD completed: `capped` cleared (counters were adjusted per batch).
    PurgeFinished,
    /// OK on pending/unavailable: `fetched_at`, `fetched_witness`,
    /// `fetch_attempts = 0`.
    Promote,
    /// OK on ready/retained: `refresh_requested = false`, `fetched_witness`
    /// updated, `capped` cleared if nothing was refused.
    RefreshDone,
    /// ready → retained: `retain_until = now + list_grace`.
    StartGrace,
    /// retained → ready: `retain_until` cleared.
    ClearGrace,
    /// A "stay (retry)" cell: the caller reschedules its retry.
    StayRetry,
}

/// Result of one transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The new `track_state`.
    pub state: TrackState,
    /// The new `purge_then`.
    pub purge_then: Option<TrackState>,
    /// Side effects, in order.
    pub effects: Vec<Effect>,
    /// Whether any stored tracking column changes (state, purge target or
    /// an effect other than [`Effect::StayRetry`]).
    pub changed: bool,
}

fn stay(f: &ListFacts) -> Outcome {
    Outcome {
        state: f.state,
        purge_then: f.purge_then,
        effects: Vec::new(),
        changed: false,
    }
}

fn stay_retry(f: &ListFacts) -> Outcome {
    Outcome {
        state: f.state,
        purge_then: f.purge_then,
        effects: vec![Effect::StayRetry],
        changed: false,
    }
}

fn to(state: TrackState, effects: Vec<Effect>) -> Outcome {
    Outcome {
        state,
        purge_then: None,
        effects,
        changed: true,
    }
}

fn purge(then: TrackState) -> Outcome {
    Outcome {
        state: TrackState::Purging,
        purge_then: Some(then),
        effects: vec![Effect::BeginPurge],
        changed: true,
    }
}

fn set_purge_then(f: &ListFacts, then: TrackState) -> Outcome {
    Outcome {
        state: TrackState::Purging,
        purge_then: Some(then),
        effects: Vec::new(),
        changed: f.purge_then != Some(then),
    }
}

/// **admit**, with the owner re-admission budget applied when `charged`.
fn admit(charged: bool, ctx: &Ctx) -> Outcome {
    if charged && !ctx.owner_readmit_available {
        return to(
            TrackState::Deferred,
            vec![Effect::Defer(DeferCause::OwnerReadmissions)],
        );
    }
    let mut effects = Vec::with_capacity(2);
    if charged {
        effects.push(Effect::ChargeOwnerReadmission);
    }
    effects.push(Effect::Admit);
    to(TrackState::Pending, effects)
}

/// The transition function.
pub fn transition(f: &ListFacts, event: Event, ctx: &Ctx) -> Outcome {
    use Event as E;
    use TrackState as S;
    let count_pos = f.listblock_count > 0;
    match (f.state, event) {
        // untracked
        (S::Untracked, E::Plus) => {
            if f.record_state == RecordState::Deleted {
                to(S::Dead, Vec::new())
            } else {
                admit(false, ctx)
            }
        }
        (S::Untracked, _) => stay(f),

        // pending
        (S::Pending, E::Minus) => purge(S::Untracked),
        (S::Pending, E::RecordDeleted) => purge(S::Dead),
        (S::Pending, E::NotFound) => purge(S::Missing),
        (S::Pending, E::GateFail(cause)) => {
            let mut o = purge(S::Deferred);
            o.effects.insert(0, Effect::Defer(cause));
            o
        }
        (S::Pending, E::Ok) => to(S::Ready, vec![Effect::Promote]),
        (S::Pending, E::FailTerminal) => to(S::Unavailable, Vec::new()),
        (S::Pending, E::OwnerInactive) => to(S::Unavailable, Vec::new()),
        // Owner-caused (divergence): charged. See the module docs: GO, OA
        // and first admissions are never charged; a DV-caused re-admission
        // is.
        (S::Pending, E::Diverged) => admit(true, ctx),
        (S::Pending, _) => stay(f),

        // ready
        (S::Ready, E::Minus) => to(S::Retained, vec![Effect::StartGrace]),
        (S::Ready, E::RecordDeleted) => purge(S::Dead),
        (S::Ready, E::Ok) => Outcome {
            state: S::Ready,
            purge_then: None,
            effects: vec![Effect::RefreshDone],
            changed: true,
        },
        (S::Ready, E::FailTerminal) | (S::Ready, E::OwnerInactive) => stay(f),
        (S::Ready, E::Diverged) => diverged_fetched(ctx),
        (S::Ready, _) => stay(f),

        // retained
        (S::Retained, E::Plus) => to(S::Ready, vec![Effect::ClearGrace]),
        (S::Retained, E::RecordDeleted) => purge(S::Dead),
        (S::Retained, E::Ok) => Outcome {
            state: S::Retained,
            purge_then: None,
            effects: vec![Effect::RefreshDone],
            changed: true,
        },
        (S::Retained, E::FailTerminal) | (S::Retained, E::OwnerInactive) => stay(f),
        (S::Retained, E::Diverged) => diverged_fetched(ctx),
        (S::Retained, E::GraceExpired) => purge(S::Untracked),
        (S::Retained, _) => stay(f),

        // unavailable
        (S::Unavailable, E::Minus) => purge(S::Untracked),
        (S::Unavailable, E::RecordDeleted) => purge(S::Dead),
        (S::Unavailable, E::NotFound) => purge(S::Missing),
        (S::Unavailable, E::GateFail(cause)) => {
            let mut o = purge(S::Deferred);
            o.effects.insert(0, Effect::Defer(cause));
            o
        }
        (S::Unavailable, E::Ok) => to(S::Ready, vec![Effect::Promote]),
        (S::Unavailable, E::FailTerminal) => stay_retry(f),
        (S::Unavailable, E::OwnerInactive) => stay(f),
        (S::Unavailable, E::OwnerActive) => admit(false, ctx),
        (S::Unavailable, E::Diverged) => admit(true, ctx),
        (S::Unavailable, _) => stay(f),

        // purging
        (S::Purging, E::Plus) => {
            if f.purge_then == Some(S::Dead) {
                stay(f)
            } else {
                set_purge_then(f, S::Untracked)
            }
        }
        (S::Purging, E::Minus) => set_purge_then(f, S::Untracked),
        (S::Purging, E::RecordPresent) => {
            if f.purge_then == Some(S::Dead) {
                set_purge_then(f, S::Untracked)
            } else {
                stay(f)
            }
        }
        (S::Purging, E::RecordDeleted) => set_purge_then(f, S::Dead),
        (S::Purging, E::PurgeDone) => {
            let target = f.purge_then.unwrap_or(S::Untracked);
            if target == S::Untracked && count_pos {
                // ³ PD re-admits; never charged (a DV charge was taken at DV).
                let mut o = admit(false, ctx);
                o.effects.insert(0, Effect::PurgeFinished);
                o
            } else {
                to(target, vec![Effect::PurgeFinished])
            }
        }
        (S::Purging, _) => stay(f),

        // missing
        (S::Missing, E::Minus) => to(S::Untracked, Vec::new()),
        (S::Missing, E::RecordPresent) => {
            // `missing` × RP is not owner-caused (the record was there).
            if count_pos {
                admit(false, ctx)
            } else {
                stay(f)
            }
        }
        (S::Missing, E::RecordDeleted) => to(S::Dead, Vec::new()),
        (S::Missing, E::NotFound) => stay_retry(f),
        (S::Missing, E::NotFoundExhausted) => to(S::Dead, Vec::new()),
        // Retry; not counted toward NFx (the caller does not advance it).
        (S::Missing, E::OwnerInactive) => stay_retry(f),
        (S::Missing, _) => stay(f),

        // dead
        (S::Dead, E::Minus) => to(S::Untracked, Vec::new()),
        // The owner re-created a deleted list: owner-caused, charged.
        (S::Dead, E::RecordPresent) => {
            if count_pos {
                admit(true, ctx)
            } else {
                stay(f)
            }
        }
        (S::Dead, _) => stay(f),

        // deferred
        (S::Deferred, E::Minus) => to(S::Untracked, Vec::new()),
        (S::Deferred, E::RecordDeleted) => to(S::Dead, Vec::new()),
        (S::Deferred, E::GateOpen) => {
            if count_pos {
                admit(false, ctx)
            } else {
                stay(f)
            }
        }
        (S::Deferred, _) => stay(f),
    }
}

/// DV on `ready`/`retained`: purge→`untracked` (PD re-admits, count > 0),
/// with the owner-caused re-admission charged now; over budget the purge
/// ends in `deferred` instead.
fn diverged_fetched(ctx: &Ctx) -> Outcome {
    if ctx.owner_readmit_available {
        let mut o = purge(TrackState::Untracked);
        o.effects.insert(0, Effect::ChargeOwnerReadmission);
        o
    } else {
        let mut o = purge(TrackState::Deferred);
        o.effects
            .insert(0, Effect::Defer(DeferCause::OwnerReadmissions));
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Event as E;
    use TrackState as S;
    use proptest::prelude::*;

    const OK_CTX: Ctx = Ctx {
        owner_readmit_available: true,
    };
    const NO_BUDGET: Ctx = Ctx {
        owner_readmit_available: false,
    };

    fn facts(state: S, count: i32) -> ListFacts {
        ListFacts {
            state,
            record_state: RecordState::Present,
            listblock_count: count,
            purge_then: if state == S::Purging {
                Some(S::Untracked)
            } else {
                None
            },
        }
    }

    fn purging(then: S, count: i32) -> ListFacts {
        ListFacts {
            purge_then: Some(then),
            ..facts(S::Purging, count)
        }
    }

    fn is_admit(o: &Outcome) -> bool {
        o.state == S::Pending && o.effects.contains(&Effect::Admit)
    }

    fn is_purge(o: &Outcome, then: S) -> bool {
        o.state == S::Purging
            && o.purge_then == Some(then)
            && o.effects.contains(&Effect::BeginPurge)
    }

    fn unchanged(o: &Outcome, f: &ListFacts) -> bool {
        o.state == f.state && o.purge_then == f.purge_then && !o.changed
    }

    /// The cells of the table that are not "—", as (state, event).
    fn defined_cells() -> Vec<(S, E)> {
        let gf = E::GateFail(DeferCause::Budget);
        vec![
            (S::Untracked, E::Plus),
            (S::Pending, E::Minus),
            (S::Pending, E::RecordDeleted),
            (S::Pending, E::NotFound),
            (S::Pending, gf),
            (S::Pending, E::Ok),
            (S::Pending, E::FailTerminal),
            (S::Pending, E::OwnerInactive),
            (S::Pending, E::Diverged),
            (S::Ready, E::Minus),
            (S::Ready, E::RecordDeleted),
            (S::Ready, E::Ok),
            (S::Ready, E::FailTerminal),
            (S::Ready, E::OwnerInactive),
            (S::Ready, E::Diverged),
            (S::Retained, E::Plus),
            (S::Retained, E::RecordDeleted),
            (S::Retained, E::Ok),
            (S::Retained, E::FailTerminal),
            (S::Retained, E::OwnerInactive),
            (S::Retained, E::Diverged),
            (S::Retained, E::GraceExpired),
            (S::Unavailable, E::Minus),
            (S::Unavailable, E::RecordDeleted),
            (S::Unavailable, E::NotFound),
            (S::Unavailable, gf),
            (S::Unavailable, E::Ok),
            (S::Unavailable, E::FailTerminal),
            (S::Unavailable, E::OwnerInactive),
            (S::Unavailable, E::OwnerActive),
            (S::Unavailable, E::Diverged),
            (S::Purging, E::Plus),
            (S::Purging, E::Minus),
            (S::Purging, E::RecordPresent),
            (S::Purging, E::RecordDeleted),
            (S::Purging, E::PurgeDone),
            (S::Missing, E::Minus),
            (S::Missing, E::RecordPresent),
            (S::Missing, E::RecordDeleted),
            (S::Missing, E::NotFound),
            (S::Missing, E::NotFoundExhausted),
            (S::Missing, E::OwnerInactive),
            (S::Dead, E::Minus),
            (S::Dead, E::RecordPresent),
            (S::Deferred, E::Minus),
            (S::Deferred, E::RecordDeleted),
            (S::Deferred, E::GateOpen),
        ]
    }

    // ---- one test per cell group, following the table row by row ----

    #[test]
    fn untracked_row() {
        assert!(is_admit(&transition(
            &facts(S::Untracked, 1),
            E::Plus,
            &OK_CTX
        )));
        // First admissions are never charged, even with no owner budget.
        let o = transition(&facts(S::Untracked, 1), E::Plus, &NO_BUDGET);
        assert!(is_admit(&o));
        assert!(!o.effects.contains(&Effect::ChargeOwnerReadmission));
        let deleted = ListFacts {
            record_state: RecordState::Deleted,
            ..facts(S::Untracked, 1)
        };
        assert_eq!(transition(&deleted, E::Plus, &OK_CTX).state, S::Dead);
    }

    #[test]
    fn pending_row() {
        let f = facts(S::Pending, 1);
        assert!(is_purge(
            &transition(&facts(S::Pending, 0), E::Minus, &OK_CTX),
            S::Untracked
        ));
        assert!(is_purge(
            &transition(&f, E::RecordDeleted, &OK_CTX),
            S::Dead
        ));
        assert!(is_purge(&transition(&f, E::NotFound, &OK_CTX), S::Missing));
        let gf = transition(&f, E::GateFail(DeferCause::HostCap), &OK_CTX);
        assert!(is_purge(&gf, S::Deferred));
        assert!(gf.effects.contains(&Effect::Defer(DeferCause::HostCap)));
        let ok = transition(&f, E::Ok, &OK_CTX);
        assert_eq!(
            (ok.state, ok.effects.clone()),
            (S::Ready, vec![Effect::Promote])
        );
        assert_eq!(
            transition(&f, E::FailTerminal, &OK_CTX).state,
            S::Unavailable
        );
        assert_eq!(
            transition(&f, E::OwnerInactive, &OK_CTX).state,
            S::Unavailable
        );
        let dv = transition(&f, E::Diverged, &OK_CTX);
        assert!(is_admit(&dv) && dv.effects.contains(&Effect::ChargeOwnerReadmission));
        let dv = transition(&f, E::Diverged, &NO_BUDGET);
        assert_eq!(dv.state, S::Deferred);
        assert!(
            dv.effects
                .contains(&Effect::Defer(DeferCause::OwnerReadmissions))
        );
    }

    #[test]
    fn ready_row() {
        let f = facts(S::Ready, 1);
        let m = transition(&facts(S::Ready, 0), E::Minus, &OK_CTX);
        assert_eq!(
            (m.state, m.effects),
            (S::Retained, vec![Effect::StartGrace])
        );
        assert!(is_purge(
            &transition(&f, E::RecordDeleted, &OK_CTX),
            S::Dead
        ));
        let ok = transition(&f, E::Ok, &OK_CTX);
        assert_eq!(
            (ok.state, ok.effects),
            (S::Ready, vec![Effect::RefreshDone])
        );
        assert!(unchanged(&transition(&f, E::FailTerminal, &OK_CTX), &f));
        assert!(unchanged(&transition(&f, E::OwnerInactive, &OK_CTX), &f));
        let dv = transition(&f, E::Diverged, &OK_CTX);
        assert!(is_purge(&dv, S::Untracked));
        assert!(dv.effects.contains(&Effect::ChargeOwnerReadmission));
        let dv = transition(&f, E::Diverged, &NO_BUDGET);
        assert!(is_purge(&dv, S::Deferred));
    }

    #[test]
    fn retained_row() {
        let f = facts(S::Retained, 0);
        let p = transition(&facts(S::Retained, 1), E::Plus, &OK_CTX);
        assert_eq!((p.state, p.effects), (S::Ready, vec![Effect::ClearGrace]));
        assert!(is_purge(
            &transition(&f, E::RecordDeleted, &OK_CTX),
            S::Dead
        ));
        let ok = transition(&f, E::Ok, &OK_CTX);
        assert_eq!(
            (ok.state, ok.effects),
            (S::Retained, vec![Effect::RefreshDone])
        );
        assert!(unchanged(&transition(&f, E::FailTerminal, &OK_CTX), &f));
        assert!(unchanged(&transition(&f, E::OwnerInactive, &OK_CTX), &f));
        assert!(is_purge(
            &transition(&f, E::Diverged, &OK_CTX),
            S::Untracked
        ));
        assert!(is_purge(
            &transition(&f, E::GraceExpired, &OK_CTX),
            S::Untracked
        ));
    }

    #[test]
    fn unavailable_row() {
        let f = facts(S::Unavailable, 1);
        assert!(is_purge(
            &transition(&facts(S::Unavailable, 0), E::Minus, &OK_CTX),
            S::Untracked
        ));
        assert!(is_purge(
            &transition(&f, E::RecordDeleted, &OK_CTX),
            S::Dead
        ));
        assert!(is_purge(&transition(&f, E::NotFound, &OK_CTX), S::Missing));
        assert!(is_purge(
            &transition(&f, E::GateFail(DeferCause::Budget), &OK_CTX),
            S::Deferred
        ));
        assert_eq!(transition(&f, E::Ok, &OK_CTX).state, S::Ready);
        let ft = transition(&f, E::FailTerminal, &OK_CTX);
        assert!(unchanged(&ft, &f) && ft.effects == [Effect::StayRetry]);
        assert!(unchanged(&transition(&f, E::OwnerInactive, &OK_CTX), &f));
        // OA is never charged.
        let oa = transition(&f, E::OwnerActive, &NO_BUDGET);
        assert!(is_admit(&oa) && !oa.effects.contains(&Effect::ChargeOwnerReadmission));
        assert!(is_admit(&transition(&f, E::Diverged, &OK_CTX)));
    }

    #[test]
    fn purging_row() {
        // + : purge_then ≠ dead ⇒ untracked (PD re-admits); dead stays.
        let o = transition(&purging(S::Missing, 1), E::Plus, &OK_CTX);
        assert_eq!((o.state, o.purge_then), (S::Purging, Some(S::Untracked)));
        let f = purging(S::Dead, 1);
        assert!(unchanged(&transition(&f, E::Plus, &OK_CTX), &f));
        // − : ⇒ untracked.
        let o = transition(&purging(S::Deferred, 0), E::Minus, &OK_CTX);
        assert_eq!(o.purge_then, Some(S::Untracked));
        // RP : dead ⇒ untracked; otherwise unchanged.
        let o = transition(&purging(S::Dead, 1), E::RecordPresent, &OK_CTX);
        assert_eq!(o.purge_then, Some(S::Untracked));
        let f = purging(S::Missing, 1);
        assert!(unchanged(&transition(&f, E::RecordPresent, &OK_CTX), &f));
        // RD : ⇒ dead.
        let o = transition(&purging(S::Untracked, 1), E::RecordDeleted, &OK_CTX);
        assert_eq!(o.purge_then, Some(S::Dead));
        // PD : → purge_then; untracked with count > 0 re-admits.
        for target in [S::Untracked, S::Dead, S::Missing, S::Deferred] {
            let o = transition(&purging(target, 0), E::PurgeDone, &OK_CTX);
            assert_eq!(o.state, target);
            assert!(o.effects.contains(&Effect::PurgeFinished));
        }
        let o = transition(&purging(S::Untracked, 2), E::PurgeDone, &NO_BUDGET);
        assert!(is_admit(&o));
        assert!(!o.effects.contains(&Effect::ChargeOwnerReadmission));
        let o = transition(&purging(S::Dead, 2), E::PurgeDone, &OK_CTX);
        assert_eq!(o.state, S::Dead);
    }

    #[test]
    fn missing_row() {
        let f = facts(S::Missing, 1);
        assert_eq!(
            transition(&facts(S::Missing, 0), E::Minus, &OK_CTX).state,
            S::Untracked
        );
        let rp = transition(&f, E::RecordPresent, &NO_BUDGET);
        assert!(is_admit(&rp) && !rp.effects.contains(&Effect::ChargeOwnerReadmission));
        let f0 = facts(S::Missing, 0);
        assert!(unchanged(&transition(&f0, E::RecordPresent, &OK_CTX), &f0));
        assert_eq!(transition(&f, E::RecordDeleted, &OK_CTX).state, S::Dead);
        assert_eq!(
            transition(&f, E::NotFound, &OK_CTX).effects,
            [Effect::StayRetry]
        );
        assert_eq!(transition(&f, E::NotFoundExhausted, &OK_CTX).state, S::Dead);
        let oi = transition(&f, E::OwnerInactive, &OK_CTX);
        assert!(unchanged(&oi, &f) && oi.effects == [Effect::StayRetry]);
    }

    #[test]
    fn dead_row() {
        assert_eq!(
            transition(&facts(S::Dead, 0), E::Minus, &OK_CTX).state,
            S::Untracked
        );
        let rp = transition(&facts(S::Dead, 1), E::RecordPresent, &OK_CTX);
        assert!(is_admit(&rp) && rp.effects.contains(&Effect::ChargeOwnerReadmission));
        let rp = transition(&facts(S::Dead, 1), E::RecordPresent, &NO_BUDGET);
        assert_eq!(rp.state, S::Deferred);
        let f0 = facts(S::Dead, 0);
        assert!(unchanged(&transition(&f0, E::RecordPresent, &OK_CTX), &f0));
    }

    #[test]
    fn deferred_row() {
        assert_eq!(
            transition(&facts(S::Deferred, 0), E::Minus, &OK_CTX).state,
            S::Untracked
        );
        assert_eq!(
            transition(&facts(S::Deferred, 1), E::RecordDeleted, &OK_CTX).state,
            S::Dead
        );
        let go = transition(&facts(S::Deferred, 1), E::GateOpen, &NO_BUDGET);
        assert!(is_admit(&go) && !go.effects.contains(&Effect::ChargeOwnerReadmission));
        let f0 = facts(S::Deferred, 0);
        assert!(unchanged(&transition(&f0, E::GateOpen, &OK_CTX), &f0));
    }

    #[test]
    fn undefined_cells_are_no_ops() {
        let defined = defined_cells();
        for &state in S::ALL {
            for event in E::ALL {
                let is_defined = defined.iter().any(|&(s, e)| {
                    s == state
                        && (e == event || matches!((e, event), (E::GateFail(_), E::GateFail(_))))
                });
                if is_defined {
                    continue;
                }
                for count in [0, 1] {
                    let f = facts(state, count);
                    let o = transition(&f, event, &OK_CTX);
                    assert!(unchanged(&o, &f), "{state:?} x {event:?} changed: {o:?}");
                    assert!(o.effects.is_empty(), "{state:?} x {event:?} had effects");
                }
            }
        }
    }

    // ---- invariant: pending/ready/unavailable only while count > 0 ----

    fn event_strategy() -> impl Strategy<Value = (Event, i32, bool)> {
        let events: Vec<Event> = E::ALL
            .iter()
            .copied()
            .chain([
                E::GateFail(DeferCause::HostCap),
                E::GateFail(DeferCause::Ceiling),
            ])
            .collect();
        (prop::sample::select(events), 1..5i32, any::<bool>())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]
        #[test]
        fn invariant_holds(seq in prop::collection::vec(event_strategy(), 1..80),
                           record_deleted in any::<bool>()) {
            let mut f = ListFacts {
                state: S::Untracked,
                record_state: if record_deleted { RecordState::Deleted } else { RecordState::Unknown },
                listblock_count: 0,
                purge_then: None,
            };
            for (event, n, budget) in seq {
                // Keep the model faithful: + only fires on 0 → n, − only on
                // n → 0; RP/RD also move record_state as the apply would.
                match event {
                    E::Plus if f.listblock_count != 0 => continue,
                    E::Minus if f.listblock_count == 0 => continue,
                    E::Plus => f.listblock_count = n,
                    E::Minus => f.listblock_count = 0,
                    E::RecordPresent => f.record_state = RecordState::Present,
                    E::RecordDeleted => f.record_state = RecordState::Deleted,
                    // PD only fires while purging.
                    E::PurgeDone if f.state != S::Purging => continue,
                    _ => {}
                }
                let ctx = Ctx { owner_readmit_available: budget };
                let o = transition(&f, event, &ctx);
                f.state = o.state;
                f.purge_then = if o.state == S::Purging { o.purge_then } else { None };
                if matches!(f.state, S::Pending | S::Ready | S::Unavailable) {
                    prop_assert!(f.listblock_count > 0,
                        "{:?} with count 0 after {:?}", f.state, event);
                }
                if f.state == S::Purging {
                    prop_assert!(f.purge_then.is_some());
                }
            }
        }
    }
}
