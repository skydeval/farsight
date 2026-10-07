//! Stream 4: every defined cell of the transition table, driven through
//! the real write paths (listblock inserts/deletes for + and −, list
//! record writes for RP and RD, the janitor for PD and GE, `fire_event`
//! for the events backfill fires).
//!
//! The expected results below are transcribed from the table in
//! `docs/design/list-indexing.md` independently of `transition.rs`, so
//! the stream does not test the implementation against itself.

use chrono::{DateTime, Utc};
use farsight_core::{Collection, Did};
use farsight_storage::Result;
use farsight_storage::codes::{DeferCause, TrackState};
use farsight_storage::janitor;
use farsight_storage::tracking::FireArgs;
use farsight_storage::transition::Event;

use crate::fixtures::*;

use TrackState as S;

/// How to reach a starting state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    Untracked,
    UntrackedDeleted,
    Pending,
    Ready,
    Retained,
    Unavailable,
    PurgingUntracked0,
    PurgingUntracked1,
    PurgingDead0,
    PurgingDead1,
    PurgingMissing,
    PurgingDeferred,
    Missing,
    Dead,
    Deferred,
}

/// The action that fires the event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Act {
    /// Insert a counted listblock (+ when count was 0).
    Plus,
    /// Delete every listblock (− when count was ≥ 1).
    Minus,
    /// Write the list record (RP).
    RecordPresent,
    /// Delete the list record (RD).
    RecordDeleted,
    /// Run the purge janitor (PD).
    PurgeDone,
    /// Run the grace janitor past the grace (GE).
    GraceExpired,
    /// Fire directly (backfill-stage events).
    Fire(Event),
}

struct Case {
    start: Start,
    act: Act,
    want: TrackState,
    want_then: Option<TrackState>,
    /// Expected `admit_epoch` increase.
    epoch_delta: i32,
}

const fn case(
    start: Start,
    act: Act,
    want: TrackState,
    want_then: Option<TrackState>,
    epoch_delta: i32,
) -> Case {
    Case {
        start,
        act,
        want,
        want_then,
        epoch_delta,
    }
}

fn cases() -> Vec<Case> {
    use Act::*;
    use Start as St;
    let f = Fire;
    let gf = Event::GateFail(DeferCause::Budget);
    vec![
        // untracked
        case(St::Untracked, Plus, S::Pending, None, 1),
        case(St::UntrackedDeleted, Plus, S::Dead, None, 0),
        // pending
        case(St::Pending, Minus, S::Purging, Some(S::Untracked), 0),
        case(St::Pending, RecordDeleted, S::Purging, Some(S::Dead), 0),
        case(
            St::Pending,
            f(Event::NotFound),
            S::Purging,
            Some(S::Missing),
            0,
        ),
        case(St::Pending, f(gf), S::Purging, Some(S::Deferred), 0),
        case(St::Pending, f(Event::Ok), S::Ready, None, 0),
        case(St::Pending, f(Event::FailTerminal), S::Unavailable, None, 0),
        case(
            St::Pending,
            f(Event::OwnerInactive),
            S::Unavailable,
            None,
            0,
        ),
        case(St::Pending, f(Event::Diverged), S::Pending, None, 1),
        // ready
        case(St::Ready, Minus, S::Retained, None, 0),
        case(St::Ready, RecordDeleted, S::Purging, Some(S::Dead), 0),
        case(St::Ready, f(Event::Ok), S::Ready, None, 0),
        case(St::Ready, f(Event::FailTerminal), S::Ready, None, 0),
        case(St::Ready, f(Event::OwnerInactive), S::Ready, None, 0),
        case(
            St::Ready,
            f(Event::Diverged),
            S::Purging,
            Some(S::Untracked),
            0,
        ),
        // retained
        case(St::Retained, Plus, S::Ready, None, 0),
        case(St::Retained, RecordDeleted, S::Purging, Some(S::Dead), 0),
        case(St::Retained, f(Event::Ok), S::Retained, None, 0),
        case(St::Retained, f(Event::FailTerminal), S::Retained, None, 0),
        case(St::Retained, f(Event::OwnerInactive), S::Retained, None, 0),
        case(
            St::Retained,
            f(Event::Diverged),
            S::Purging,
            Some(S::Untracked),
            0,
        ),
        case(
            St::Retained,
            GraceExpired,
            S::Purging,
            Some(S::Untracked),
            0,
        ),
        // unavailable
        case(St::Unavailable, Minus, S::Purging, Some(S::Untracked), 0),
        case(St::Unavailable, RecordDeleted, S::Purging, Some(S::Dead), 0),
        case(
            St::Unavailable,
            f(Event::NotFound),
            S::Purging,
            Some(S::Missing),
            0,
        ),
        case(St::Unavailable, f(gf), S::Purging, Some(S::Deferred), 0),
        case(St::Unavailable, f(Event::Ok), S::Ready, None, 0),
        case(
            St::Unavailable,
            f(Event::FailTerminal),
            S::Unavailable,
            None,
            0,
        ),
        case(
            St::Unavailable,
            f(Event::OwnerInactive),
            S::Unavailable,
            None,
            0,
        ),
        case(St::Unavailable, f(Event::OwnerActive), S::Pending, None, 1),
        case(St::Unavailable, f(Event::Diverged), S::Pending, None, 1),
        // purging
        case(
            St::PurgingUntracked0,
            Plus,
            S::Purging,
            Some(S::Untracked),
            0,
        ),
        case(St::PurgingDead0, Plus, S::Purging, Some(S::Dead), 0),
        case(St::PurgingMissing, Minus, S::Purging, Some(S::Untracked), 0),
        case(
            St::PurgingDead1,
            RecordPresent,
            S::Purging,
            Some(S::Untracked),
            0,
        ),
        case(
            St::PurgingMissing,
            RecordPresent,
            S::Purging,
            Some(S::Missing),
            0,
        ),
        case(
            St::PurgingUntracked1,
            RecordDeleted,
            S::Purging,
            Some(S::Dead),
            0,
        ),
        case(St::PurgingUntracked1, PurgeDone, S::Pending, None, 1),
        case(St::PurgingUntracked0, PurgeDone, S::Untracked, None, 0),
        case(St::PurgingDead1, PurgeDone, S::Dead, None, 0),
        case(St::PurgingMissing, PurgeDone, S::Missing, None, 0),
        case(St::PurgingDeferred, PurgeDone, S::Deferred, None, 0),
        // missing
        case(St::Missing, Minus, S::Untracked, None, 0),
        case(St::Missing, RecordPresent, S::Pending, None, 1),
        case(St::Missing, RecordDeleted, S::Dead, None, 0),
        case(St::Missing, f(Event::NotFound), S::Missing, None, 0),
        case(St::Missing, f(Event::NotFoundExhausted), S::Dead, None, 0),
        case(St::Missing, f(Event::OwnerInactive), S::Missing, None, 0),
        // dead
        case(St::Dead, Minus, S::Untracked, None, 0),
        case(St::Dead, RecordPresent, S::Pending, None, 1),
        // deferred
        case(St::Deferred, Minus, S::Untracked, None, 0),
        case(St::Deferred, RecordDeleted, S::Dead, None, 0),
        case(St::Deferred, f(Event::GateOpen), S::Pending, None, 1),
        // a sample of "—" cells: no change
        case(St::Untracked, f(Event::Ok), S::Untracked, None, 0),
        case(St::Dead, f(Event::NotFound), S::Dead, None, 0),
        case(St::Deferred, f(Event::Ok), S::Deferred, None, 0),
        case(St::Missing, f(Event::GateOpen), S::Missing, None, 0),
        case(St::Ready, f(Event::GateOpen), S::Ready, None, 0),
        case(St::Pending, f(Event::GraceExpired), S::Pending, None, 0),
    ]
}

/// One list under test.
struct L {
    owner: Did,
    rkey: String,
    id: i64,
    blockers: Vec<(Did, String)>,
    next_rev: u64,
    tag: u64,
}

impl L {
    fn rev(&mut self) -> i64 {
        self.next_rev += 1;
        rev(self.next_rev)
    }
}

async fn new_list(env: &Env, tag: u64) -> Result<L> {
    let owner = plc("tableowner", tag);
    let mut l = L {
        owner,
        rkey: format!("t{tag}"),
        id: 0,
        blockers: Vec::new(),
        next_rev: 1000,
        tag,
    };
    let r = l.rev();
    env.firehose(vec![list(&l.owner, &l.rkey, r)]).await?;
    l.id = env
        .list_id(&l.owner, &l.rkey)
        .await?
        .ok_or_else(|| farsight_storage::StorageError::Invariant("list not created".into()))?;
    Ok(l)
}

async fn plus(env: &Env, l: &mut L) -> Result<()> {
    let b = plc("tableblocker", l.tag * 100 + l.blockers.len() as u64);
    let rkey = format!("lb{}", l.blockers.len());
    let r = l.rev();
    env.firehose(vec![listblock(&b, &rkey, &l.owner, &l.rkey, r)])
        .await?;
    l.blockers.push((b, rkey));
    Ok(())
}

async fn minus(env: &Env, l: &mut L) -> Result<()> {
    let mut writes = Vec::new();
    for (b, rkey) in std::mem::take(&mut l.blockers) {
        let r = l.rev();
        writes.push(delete(&b, Collection::ListBlock, &rkey, r));
    }
    env.firehose(writes).await?;
    Ok(())
}

async fn record_present(env: &Env, l: &mut L) -> Result<()> {
    let r = l.rev();
    env.firehose(vec![list(&l.owner, &l.rkey, r)]).await?;
    Ok(())
}

async fn record_deleted(env: &Env, l: &mut L) -> Result<()> {
    let r = l.rev();
    env.firehose(vec![delete(&l.owner, Collection::List, &l.rkey, r)])
        .await?;
    Ok(())
}

async fn fire(env: &Env, l: &L, event: Event, run_point: Option<DateTime<Utc>>) -> Result<()> {
    janitor::fire_event(
        &env.pool,
        &env.limits,
        &env.counters,
        l.id,
        event,
        FireArgs {
            run_point,
            items_refused: false,
        },
    )
    .await?;
    Ok(())
}

async fn purge(env: &Env) -> Result<()> {
    // Loop until no purging list has items left to delete.
    for _ in 0..20 {
        let r = janitor::process_purges(&env.pool, &env.limits, &env.counters, 1000).await?;
        if r.finished.is_empty() && r.items_deleted == 0 {
            break;
        }
    }
    Ok(())
}

async fn add_item(env: &Env, l: &mut L) -> Result<()> {
    let r = l.rev();
    let subject = plc("tablemember", l.tag);
    let rk = format!("it{}", l.next_rev);
    env.firehose(vec![item(&l.owner, &rk, &l.rkey, &subject, r)])
        .await?;
    Ok(())
}

async fn drive(env: &Env, l: &mut L, start: Start) -> Result<()> {
    use Start as St;
    match start {
        St::Untracked => {}
        St::UntrackedDeleted => record_deleted(env, l).await?,
        St::Pending => {
            plus(env, l).await?;
            add_item(env, l).await?;
        }
        St::Ready => {
            Box::pin(drive(env, l, St::Pending)).await?;
            fire(env, l, Event::Ok, Some(Utc::now())).await?;
        }
        St::Retained => {
            Box::pin(drive(env, l, St::Ready)).await?;
            minus(env, l).await?;
        }
        St::Unavailable => {
            Box::pin(drive(env, l, St::Pending)).await?;
            fire(env, l, Event::FailTerminal, None).await?;
        }
        St::PurgingUntracked0 => {
            Box::pin(drive(env, l, St::Pending)).await?;
            minus(env, l).await?;
        }
        St::PurgingUntracked1 => {
            Box::pin(drive(env, l, St::PurgingUntracked0)).await?;
            plus(env, l).await?;
        }
        St::PurgingDead0 => {
            Box::pin(drive(env, l, St::PurgingUntracked0)).await?;
            record_deleted(env, l).await?;
        }
        St::PurgingDead1 => {
            Box::pin(drive(env, l, St::Pending)).await?;
            record_deleted(env, l).await?;
        }
        St::PurgingMissing => {
            Box::pin(drive(env, l, St::Pending)).await?;
            fire(env, l, Event::NotFound, None).await?;
        }
        St::PurgingDeferred => {
            Box::pin(drive(env, l, St::Pending)).await?;
            fire(env, l, Event::GateFail(DeferCause::Budget), None).await?;
        }
        St::Missing => {
            Box::pin(drive(env, l, St::PurgingMissing)).await?;
            purge(env).await?;
        }
        St::Dead => {
            Box::pin(drive(env, l, St::PurgingDead1)).await?;
            purge(env).await?;
        }
        St::Deferred => {
            Box::pin(drive(env, l, St::PurgingDeferred)).await?;
            purge(env).await?;
        }
    }
    Ok(())
}

fn expected_start(start: Start) -> (TrackState, Option<TrackState>) {
    use Start as St;
    match start {
        St::Untracked | St::UntrackedDeleted => (S::Untracked, None),
        St::Pending => (S::Pending, None),
        St::Ready => (S::Ready, None),
        St::Retained => (S::Retained, None),
        St::Unavailable => (S::Unavailable, None),
        St::PurgingUntracked0 | St::PurgingUntracked1 => (S::Purging, Some(S::Untracked)),
        St::PurgingDead0 | St::PurgingDead1 => (S::Purging, Some(S::Dead)),
        St::PurgingMissing => (S::Purging, Some(S::Missing)),
        St::PurgingDeferred => (S::Purging, Some(S::Deferred)),
        St::Missing => (S::Missing, None),
        St::Dead => (S::Dead, None),
        St::Deferred => (S::Deferred, None),
    }
}

/// Stream 4.
pub async fn s4_transition_table(env: &mut Env, c: &mut Checks) -> Result<()> {
    for (i, cs) in cases().into_iter().enumerate() {
        let label = format!("{:?} × {:?}", cs.start, cs.act);
        let mut l = new_list(env, i as u64 + 1).await?;
        drive(env, &mut l, cs.start).await?;
        let before = env.list_view(l.id).await?;
        if !c.eq(
            format!("{label}: start state reached"),
            (before.state, before.purge_then),
            expected_start(cs.start),
        ) {
            continue;
        }
        // Postgres keeps microseconds; compare at that precision.
        if matches!(cs.start, Start::UntrackedDeleted | Start::Dead) {
            c.eq(
                format!("{label}: start has record_state = deleted"),
                before.record_state,
                2,
            );
        }
        let run_point = now_micros();
        match cs.act {
            Act::Plus => plus(env, &mut l).await?,
            Act::Minus => minus(env, &mut l).await?,
            Act::RecordPresent => record_present(env, &mut l).await?,
            Act::RecordDeleted => record_deleted(env, &mut l).await?,
            Act::PurgeDone => {
                // Mark it capped so "PD clears capped" is a real check.
                sqlx::query("UPDATE lists SET capped = true WHERE id = $1")
                    .bind(l.id)
                    .execute(&env.pool)
                    .await?;
                purge(env).await?;
            }
            Act::GraceExpired => {
                let r = janitor::expire_grace(
                    &env.pool,
                    &env.limits,
                    &env.counters,
                    Utc::now() + chrono::Duration::days(8),
                )
                .await?;
                c.check(
                    format!("{label}: GE fired by the grace janitor"),
                    r.transitions.iter().any(|t| t.list_id == l.id),
                    format!("{} transitions", r.transitions.len()),
                );
            }
            Act::Fire(e) => fire(env, &l, e, Some(run_point)).await?,
        }
        let after = env.list_view(l.id).await?;
        c.eq(
            format!("{label}: → state / purge_then"),
            (after.state, after.purge_then),
            (cs.want, cs.want_then),
        );
        c.eq(
            format!("{label}: admit_epoch delta"),
            after.admit_epoch - before.admit_epoch,
            cs.epoch_delta,
        );
        // Side effects implied by the resulting cell.
        if cs.epoch_delta > 0 {
            c.check(
                format!("{label}: admit enqueued phase 1 for the new epoch and built lanes"),
                after.job_current && after.admitted && after.sched_keys > 0,
                format!("{after:?}"),
            );
        }
        if after.state == S::Purging || !after.state.is_waiting() {
            c.check(
                format!("{label}: not waiting ⇒ no phase-1 job, no lanes"),
                !after.job_current && after.sched_keys == 0,
                format!("{after:?}"),
            );
        }
        if after.state.is_waiting() && after.state != before.state {
            c.check(
                format!("{label}: waiting ⇒ lanes present"),
                after.sched_keys > 0,
                format!("{after:?}"),
            );
        }
        if after.state == S::Ready && before.state != S::Ready && cs.act == Act::Fire(Event::Ok) {
            c.eq(
                format!("{label}: OK stamped fetched_witness with the run point"),
                after.fetched_witness,
                Some(run_point),
            );
        }
        if cs.act == Act::Fire(Event::Ok)
            && before.state == after.state
            && matches!(before.state, S::Ready | S::Retained)
        {
            c.eq(
                format!("{label}: refresh updated fetched_witness"),
                after.fetched_witness,
                Some(run_point),
            );
        }
        if after.state == S::Retained && before.state != S::Retained {
            c.check(
                format!("{label}: grace started"),
                after.retaining,
                format!("{after:?}"),
            );
        }
        if before.state == S::Retained && after.state == S::Ready {
            c.check(
                format!("{label}: grace cleared"),
                !after.retaining,
                format!("{after:?}"),
            );
        }
        if cs.act == Act::PurgeDone {
            c.check(
                format!("{label}: PD emptied the list and cleared capped"),
                after.item_count == 0 && !after.capped,
                format!("{after:?}"),
            );
        }
        if cs.want_then == Some(S::Deferred) {
            c.eq(
                format!("{label}: deferred_by recorded"),
                after.deferred_by,
                Some(DeferCause::Budget.code()),
            );
        }
        if matches!(after.state, S::Pending | S::Ready | S::Unavailable) {
            c.check(
                format!("{label}: invariant count > 0"),
                after.listblock_count > 0,
                format!("{after:?}"),
            );
        }
    }

    // Owner re-admission budget: dead × RP is owner-caused and charged;
    // with no budget left it defers to the next UTC day.
    let saved = env.limits.cfg.owner_readmissions_per_day;
    env.limits.cfg.owner_readmissions_per_day = 0;
    let mut l = new_list(env, 900).await?;
    drive(env, &mut l, Start::Dead).await?;
    record_present(env, &mut l).await?;
    let v = env.list_view(l.id).await?;
    c.eq(
        "dead × RP over the owner budget ⇒ deferred (owner re-admissions), retry tomorrow",
        (v.state, v.deferred_by, v.retry_scheduled),
        (
            S::Deferred,
            Some(DeferCause::OwnerReadmissions.code()),
            true,
        ),
    );
    // Outsider toggles are never charged to the owner: + on untracked
    // admits even with no owner budget.
    let mut l = new_list(env, 901).await?;
    plus(env, &mut l).await?;
    c.eq(
        "first admission is never charged (no owner budget needed)",
        env.list_view(l.id).await?.state,
        S::Pending,
    );
    env.limits.cfg.owner_readmissions_per_day = saved;
    Ok(())
}
