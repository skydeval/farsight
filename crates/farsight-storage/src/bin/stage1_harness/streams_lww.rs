//! Streams 1–3: LWW ordering, refusal tombstones at `E − 1`, and the
//! §4.5 "a refused listitem is never lost" race.

use chrono::Utc;
use farsight_core::Collection;
use farsight_storage::Result;
use farsight_storage::codes::{CapType, DebtReason, TrackState};
use farsight_storage::janitor;
use farsight_storage::tracking::FireArgs;
use farsight_storage::transition::Event;

use crate::fixtures::*;

/// Stream 1: same key, revs A < B < C, all six delivery orders.
pub async fn s1_lww(env: &mut Env, c: &mut Checks) -> Result<()> {
    let a = plc("lwwauthor", 1);
    let owner = plc("lwwowner", 1);
    let subj = [plc("lwwsubj", 1), plc("lwwsubj", 2), plc("lwwsubj", 3)];
    let revs = [rev(100), rev(200), rev(300)];
    let perms = permutations(&[0usize, 1, 2]);
    c.eq("six orderings generated", perms.len(), 6);
    for (pi, perm) in perms.iter().enumerate() {
        // Blocks: create A, update B, update C (subject changes).
        let key = format!("b{pi}");
        for &i in perm {
            env.firehose(vec![block(&a, &key, &subj[i], revs[i])])
                .await?;
        }
        c.eq(
            format!("block order {perm:?}: final row is C"),
            env.block_row(&a, &key).await?,
            Some((subj[2].to_string(), revs[2])),
        );
        c.eq(
            format!("block order {perm:?}: no tombstone"),
            env.tombstone(Collection::Block, &a, &key).await?,
            None,
        );

        // Listblocks: same, subject = three different lists; the counter
        // path must leave exactly C's list counted.
        let lists = [format!("l{pi}a"), format!("l{pi}b"), format!("l{pi}c")];
        let key = format!("lb{pi}");
        for &i in perm {
            env.firehose(vec![listblock(&a, &key, &owner, &lists[i], revs[i])])
                .await?;
        }
        c.eq(
            format!("listblock order {perm:?}: final row is C, counted"),
            env.listblock_row(&a, &key).await?,
            Some((owner.to_string(), lists[2].clone(), true, revs[2])),
        );
        c.eq(
            format!("listblock order {perm:?}: no tombstone"),
            env.tombstone(Collection::ListBlock, &a, &key).await?,
            None,
        );
        for (i, l) in lists.iter().enumerate() {
            let count = match env.list_id(&owner, l).await? {
                Some(id) => env.list_view(id).await?.listblock_count,
                None => 0,
            };
            c.eq(
                format!("listblock order {perm:?}: listblock_count of list {i}"),
                count,
                i32::from(i == 2),
            );
        }

        // Create A, delete B, create C: C wins in every order; the delete's
        // tombstone (rev B) is legitimately left behind.
        let key = format!("d{pi}");
        for &i in perm {
            let w = if i == 1 {
                delete(&a, Collection::Block, &key, revs[1])
            } else {
                block(&a, &key, &subj[i], revs[i])
            };
            env.firehose(vec![w]).await?;
        }
        c.eq(
            format!("create/delete/create order {perm:?}: final row is C"),
            env.block_row(&a, &key).await?,
            Some((subj[2].to_string(), revs[2])),
        );
        c.eq(
            format!("create/delete/create order {perm:?}: tombstone at B"),
            env.tombstone(Collection::Block, &a, &key).await?,
            Some(revs[1]),
        );
    }
    // Equal stamps are skipped, not applied twice.
    let r = env
        .firehose(vec![block(&a, "b0", &subj[0], revs[2])])
        .await?;
    c.eq("equal stamp is stale", (r.applied, r.stale), (0, 1));
    Ok(())
}

/// Stream 2: pass-14 LB — refusal tombstone at `E − 1`.
pub async fn s2_refusal_tombstone(env: &mut Env, c: &mut Checks) -> Result<()> {
    // (0) The kickoff's suggested trigger: saturate blocks_per_author. Under
    // §4.2 a subject-changing update is a delete plus an insert, so it is
    // count-neutral and the per-author cap cannot refuse it. Recorded here
    // so the report can say why the refusal below is provoked by the
    // intern rate instead.
    let b = plc("capauthor", 1);
    env.limits.cfg.blocks_per_author = 1;
    env.firehose(vec![block(&b, "k1", &plc("capsubj", 1), rev(10))])
        .await?;
    let r = env
        .firehose(vec![block(&b, "k2", &plc("capsubj", 2), rev(11))])
        .await?;
    c.eq(
        "blocks_per_author saturated: second create refused",
        r.refused,
        1,
    );
    let r = env
        .firehose(vec![block(&b, "k1", &plc("capsubj", 3), rev(12))])
        .await?;
    c.eq(
        "blocks_per_author saturated: subject-changing update is applied (count-neutral, §4.2)",
        (r.applied, r.refused, r.refusal_tombstones),
        (1, 0, 0),
    );
    env.limits.cfg.blocks_per_author = 1_000_000;

    // (a) Block at rev A; the author's intern rate is then exhausted:
    // author row (1, charged, never refused) + S1 (2) with a limit of 2.
    let a = plc("tombauthor", 1);
    let s1 = plc("tombsubj", 1);
    let s2 = plc("tombsubj", 2);
    env.limits.cfg.intern_per_did_per_day = 2;
    let (rev_a, e) = (rev(100), rev(200));
    let r = env.firehose(vec![block(&a, "k", &s1, rev_a)]).await?;
    c.eq("block created at A", r.applied, 1);

    // (b) Subject-changing update at E to a new subject: interning S2 is
    // refused ⇒ old row deleted, refusal tombstone at E − 1, capped debt.
    let r = env.firehose(vec![block(&a, "k", &s2, e)]).await?;
    c.eq(
        "update at E refused with a refusal tombstone",
        (r.refused, r.refusal_tombstones),
        (1, 1),
    );
    c.eq("stored row is gone", env.block_row(&a, "k").await?, None);
    c.eq(
        "tombstone rev is E − 1",
        env.tombstone(Collection::Block, &a, "k").await?,
        Some(e - 1),
    );
    c.eq(
        "author holds a capped debt for the intern rate",
        env.debt(&a, DebtReason::Capped.code()).await?,
        Some(Some(CapType::InternRate.code())),
    );

    // Listings stamped R < E (showing the old version) cannot re-insert.
    for (label, stamp) in [("R = A", rev_a), ("R = E − 1", e - 1)] {
        let r = env.listing(vec![block(&a, "k", &s1, 0)], stamp).await?;
        c.eq(
            format!("listing {label} with old version is stale"),
            r.stale,
            1,
        );
        c.eq(
            format!("listing {label}: row still absent"),
            env.block_row(&a, "k").await?,
            None,
        );
    }
    // R = E while the refusal's cause persists: refused again, not stale.
    let r = env.listing(vec![block(&a, "k", &s2, 0)], e).await?;
    c.eq(
        "listing R = E before relaxing: refused (not skipped as stale)",
        (r.stale, r.refused),
        (0, 1),
    );

    // Relax the cause; R ≥ E must apply (with a tombstone at E the
    // equal-rev rule would skip it forever).
    env.limits.cfg.intern_per_did_per_day = 1_000_000;
    let r = env.listing(vec![block(&a, "k", &s2, 0)], e).await?;
    c.eq("listing R = E after relaxing applies", r.applied, 1);
    c.eq(
        "row now holds the refused-but-current version",
        env.block_row(&a, "k").await?,
        Some((s2.to_string(), e)),
    );

    // Same rule for listblocks, including the counter path on the old list.
    let a2 = plc("tomblbauthor", 1);
    let o1 = plc("tomblbowner", 1);
    let o2 = plc("tomblbowner", 2);
    // author row (1) + owner o1 (2) + placeholder list (3).
    env.limits.cfg.intern_per_did_per_day = 3;
    env.firehose(vec![listblock(&a2, "k", &o1, "l1", rev_a)])
        .await?;
    let l1 = env.list_id(&o1, "l1").await?.unwrap_or(-1);
    let v = env.list_view(l1).await?;
    c.eq(
        "listblock at A admitted its list",
        (v.state, v.listblock_count),
        (TrackState::Pending, 1),
    );
    let r = env
        .firehose(vec![listblock(&a2, "k", &o2, "l2", e)])
        .await?;
    c.eq(
        "listblock update at E refused with a refusal tombstone",
        (r.refused, r.refusal_tombstones),
        (1, 1),
    );
    c.eq(
        "listblock row gone",
        env.listblock_row(&a2, "k").await?,
        None,
    );
    c.eq(
        "listblock tombstone at E − 1",
        env.tombstone(Collection::ListBlock, &a2, "k").await?,
        Some(e - 1),
    );
    let v = env.list_view(l1).await?;
    c.eq(
        "old list lost its counted listblock (− fired: pending → purging)",
        (v.listblock_count, v.state, v.purge_then),
        (0, TrackState::Purging, Some(TrackState::Untracked)),
    );
    let r = env
        .listing(vec![listblock(&a2, "k", &o1, "l1", 0)], e - 1)
        .await?;
    c.eq("listblock listing R = E − 1 is stale", r.stale, 1);
    env.limits.cfg.intern_per_did_per_day = 1_000_000;
    let r = env
        .listing(vec![listblock(&a2, "k", &o2, "l2", 0)], e)
        .await?;
    c.eq(
        "listblock listing R = E after relaxing applies",
        r.applied,
        1,
    );
    c.eq(
        "listblock row holds the new version, counted",
        env.listblock_row(&a2, "k").await?,
        Some((o2.to_string(), "l2".to_owned(), true, e)),
    );
    Ok(())
}

async fn promote(env: &Env, list_id: i64) -> Result<()> {
    janitor::fire_event(
        &env.pool,
        &env.limits,
        &env.counters,
        list_id,
        Event::Ok,
        FireArgs {
            run_point: Some(Utc::now()),
            items_refused: false,
        },
    )
    .await?;
    Ok(())
}

async fn item_count_consistent(env: &Env, c: &mut Checks, what: &str, list_id: i64) -> Result<()> {
    let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM list_items WHERE list_id = $1")
        .bind(list_id)
        .fetch_one(&env.pool)
        .await?;
    let v = env.list_view(list_id).await?;
    c.eq(
        format!("{what}: item_count matches rows"),
        i64::from(v.item_count),
        actual,
    );
    Ok(())
}

/// Stream 3: §4.5 — a listitem refused before its list is admitted is
/// recovered by the admission's run; one processed after the flip applies.
pub async fn s3_refused_item(env: &mut Env, c: &mut Checks) -> Result<()> {
    let o = plc("raceowner", 1);
    let b = plc("raceblocker", 1);
    let x = plc("racesubj", 1);
    let y = plc("racesubj", 2);
    env.firehose(vec![
        list(&o, "L", rev(1)),
        list(&o, "M", rev(2)),
        list(&o, "N0", rev(3)),
        list(&o, "N", rev(4)),
        list(&o, "P", rev(5)),
    ])
    .await?;

    // Order 1: I's event is processed before the flip commits.
    let r = env.firehose(vec![item(&o, "i1", "L", &x, rev(50))]).await?;
    c.eq(
        "order 1: item on untracked list refused (no debt)",
        (r.untracked_items, r.debts),
        (1, 0),
    );
    c.eq(
        "order 1: item not stored",
        env.item_row(&o, "i1").await?,
        None,
    );
    env.firehose(vec![listblock(&b, "lb1", &o, "L", rev(60))])
        .await?;
    let l = env.list_id(&o, "L").await?.unwrap_or(-1);
    let v = env.list_view(l).await?;
    c.eq(
        "order 1: flip admitted L with a phase-1 job",
        (v.state, v.job_current),
        (TrackState::Pending, true),
    );
    // The list job's run reads its stamp after the flip: R ≥ I's rev.
    let r = env
        .listing(vec![item(&o, "i1", "L", &x, 0)], rev(70))
        .await?;
    c.eq("order 1: the run's listing applies I", r.applied, 1);
    promote(env, l).await?;
    c.eq(
        "order 1: L ready",
        env.list_view(l).await?.state,
        TrackState::Ready,
    );
    c.eq(
        "order 1: I present in L",
        env.item_row(&o, "i1").await?.map(|r| (r.0, r.1)),
        Some(("L".to_owned(), x.to_string())),
    );
    item_count_consistent(env, c, "order 1", l).await?;

    // Order 2: I's event is processed after the flip commits.
    env.firehose(vec![listblock(&b, "lb2", &o, "M", rev(80))])
        .await?;
    let r = env.firehose(vec![item(&o, "i2", "M", &x, rev(81))]).await?;
    c.eq(
        "order 2: item after the flip is applied while pending",
        r.applied,
        1,
    );
    let m = env.list_id(&o, "M").await?.unwrap_or(-1);
    item_count_consistent(env, c, "order 2", m).await?;

    // Order 3 (§4.5 amended by r15): I moved into untracked N by an update
    // at E is refused with a tombstone at E − 1; the later admission's
    // run (stamp R ≥ E) applies it, an older stamp does not.
    env.firehose(vec![listblock(&b, "lb3", &o, "N0", rev(90))])
        .await?;
    env.firehose(vec![item(&o, "i3", "N0", &x, rev(91))])
        .await?;
    let e = rev(100);
    let r = env.firehose(vec![item(&o, "i3", "N", &x, e)]).await?;
    c.eq(
        "order 3: move into untracked N refused with a refusal tombstone",
        r.refusal_tombstones,
        1,
    );
    c.eq(
        "order 3: old version removed from N0",
        env.item_row(&o, "i3").await?,
        None,
    );
    c.eq(
        "order 3: tombstone at E − 1",
        env.tombstone(Collection::ListItem, &o, "i3").await?,
        Some(e - 1),
    );
    let n0 = env.list_id(&o, "N0").await?.unwrap_or(-1);
    item_count_consistent(env, c, "order 3 (N0)", n0).await?;
    env.firehose(vec![listblock(&b, "lb4", &o, "N", rev(110))])
        .await?;
    let r = env.listing(vec![item(&o, "i3", "N", &x, 0)], e - 1).await?;
    c.eq("order 3: a listing stamped E − 1 cannot apply", r.stale, 1);
    let r = env
        .listing(vec![item(&o, "i3", "N", &x, 0)], rev(120))
        .await?;
    c.eq(
        "order 3: the run's listing (R ≥ E) applies I into N",
        r.applied,
        1,
    );
    c.eq(
        "order 3: I present in N",
        env.item_row(&o, "i3").await?.map(|r| r.0),
        Some("N".to_owned()),
    );

    // Reverse: listitem after the last listblock went away.
    promote(env, m).await?;
    env.firehose(vec![delete(&b, Collection::ListBlock, "lb2", rev(130))])
        .await?;
    c.eq(
        "reverse: ready M → retained",
        env.list_view(m).await?.state,
        TrackState::Retained,
    );
    let r = env
        .firehose(vec![item(&o, "i4", "M", &y, rev(131))])
        .await?;
    c.eq("reverse: item on retained list applied", r.applied, 1);
    env.firehose(vec![listblock(&b, "lb5", &o, "P", rev(140))])
        .await?;
    env.firehose(vec![delete(&b, Collection::ListBlock, "lb5", rev(141))])
        .await?;
    let p = env.list_id(&o, "P").await?.unwrap_or(-1);
    c.eq(
        "reverse: pending P → purging",
        env.list_view(p).await?.state,
        TrackState::Purging,
    );
    let r = env
        .firehose(vec![item(&o, "i5", "P", &y, rev(142))])
        .await?;
    c.eq(
        "reverse: item on purging list refused",
        r.untracked_items,
        1,
    );
    Ok(())
}
