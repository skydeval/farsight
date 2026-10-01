//! Streams 5–9: counter drift, counted stickiness, tombstone TTL, lock
//! order under contention, and coverage/clock/notify plumbing.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use farsight_core::Collection;
use farsight_storage::apply::{self, Batch, Origin};
use farsight_storage::codes::{CapType, DebtReason, GapCause, Protocol, TrackState};
use farsight_storage::coverage::{self, COVERAGE_CHANNEL};
use farsight_storage::firehose::{self, FirehoseProgress};
use farsight_storage::{Result, StorageError, janitor, recount};
use sqlx::postgres::PgListener;

use crate::fixtures::*;

async fn db_now(env: &Env) -> Result<DateTime<Utc>> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&env.pool)
        .await?)
}

async fn scalar(env: &Env, sql: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(sql).fetch_one(&env.pool).await?)
}

/// Independent (non-recount) consistency queries; each must return 0.
const CONSISTENCY: [(&str, &str); 8] = [
    (
        "listblock_count = counted rows",
        "SELECT count(*) FROM lists l WHERE l.listblock_count <>
           (SELECT count(*) FROM list_blocks b WHERE b.list_id = l.id AND b.counted)",
    ),
    (
        "item_count = item rows",
        "SELECT count(*) FROM lists l WHERE l.item_count <>
           (SELECT count(*) FROM list_items i WHERE i.list_id = l.id)",
    ),
    (
        "owned_items = owner's item rows",
        "SELECT count(*) FROM actors a WHERE a.owned_items <>
           (SELECT count(*) FROM list_items i WHERE i.owner_id = a.id)",
    ),
    (
        "authored_blocks = block rows",
        "SELECT count(*) FROM actors a WHERE a.authored_blocks <>
           (SELECT count(*) FROM blocks b WHERE b.author_id = a.id)",
    ),
    (
        "authored_listblocks = listblock rows",
        "SELECT count(*) FROM actors a WHERE a.authored_listblocks <>
           (SELECT count(*) FROM list_blocks b WHERE b.author_id = a.id)",
    ),
    (
        "fetch_triggers = counted listblock rows",
        "SELECT count(*) FROM actors a WHERE a.fetch_triggers <>
           (SELECT count(*) FROM list_blocks b WHERE b.author_id = a.id AND b.counted)",
    ),
    (
        "invariant: pending/ready/unavailable only while count > 0",
        "SELECT count(*) FROM lists WHERE track_state IN (1, 2, 4) AND listblock_count = 0",
    ),
    (
        "lanes exist only for waiting lists and match counted rows",
        "SELECT count(*) FROM list_sched_keys k JOIN lists l ON l.id = k.list_id
         WHERE l.track_state NOT IN (1, 4, 6)
            OR k.n <> (SELECT count(*) FROM list_blocks b
                       WHERE b.list_id = k.list_id AND b.counted AND b.sched_key = k.key)",
    ),
];

async fn consistency(env: &Env, c: &mut Checks, label: &str) -> Result<()> {
    for (what, sql) in CONSISTENCY {
        let n = scalar(env, sql).await?;
        c.eq(format!("{label}: {what} (violations)"), n, 0);
    }
    Ok(())
}

async fn recount_all(env: &Env) -> Result<recount::RecountReport> {
    let mut total = recount::RecountReport::default();
    let mut after = 0;
    loop {
        let (r, last) =
            recount::recount_lists(&env.pool, &env.limits, &env.counters, after, 500).await?;
        total.checked += r.checked;
        total.drift.extend(r.drift);
        match last {
            Some(l) => after = l,
            None => break,
        }
    }
    let mut after = 0;
    loop {
        let (r, last) = recount::recount_actors(&env.pool, after, 500).await?;
        total.checked += r.checked;
        total.drift.extend(r.drift);
        match last {
            Some(l) => after = l,
            None => break,
        }
    }
    Ok(total)
}

/// Stream 5: 10k listblocks with churn; recounts match the maintained
/// counters exactly, and the recount is shown to detect injected drift.
pub async fn s5_counter_drift(env: &mut Env, c: &mut Checks) -> Result<()> {
    let mut rng = Rng::new(0x5eed_0005);
    const AUTHORS: u64 = 100;
    const OWNERS: u64 = 30;
    const LISTS_PER_OWNER: u64 = 10;
    let mut revn: u64 = 10_000;
    let mut next_rev = || {
        revn += 1;
        rev(revn)
    };
    let owners: Vec<_> = (0..OWNERS).map(|i| plc("churnowner", i)).collect();
    let mut writes = Vec::new();
    for o in &owners {
        for j in 0..LISTS_PER_OWNER {
            writes.push(list(o, &format!("l{j}"), next_rev()));
        }
    }
    env.firehose(writes).await?;

    let mut pending: Vec<farsight_storage::apply::Write> = Vec::new();
    let mut lb_live: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut lb_next: HashMap<u64, u64> = HashMap::new();
    let mut creates = 0u64;
    let mut ops = 0u64;
    while creates < 10_000 {
        let a = rng.below(AUTHORS);
        let author = plc("churnauthor", a);
        let lo = rng.below(OWNERS);
        let lj = rng.below(LISTS_PER_OWNER);
        let live = lb_live.entry(a).or_default();
        let op = rng.below(10);
        if op < 5 || live.is_empty() {
            let n = lb_next.entry(a).or_insert(0);
            *n += 1;
            live.push(*n);
            pending.push(listblock(
                &author,
                &format!("k{n}"),
                &owners[lo as usize],
                &format!("l{lj}"),
                next_rev(),
            ));
            creates += 1;
        } else if op < 8 {
            let k = live[rng.below(live.len() as u64) as usize];
            pending.push(listblock(
                &author,
                &format!("k{k}"),
                &owners[lo as usize],
                &format!("l{lj}"),
                next_rev(),
            ));
        } else {
            let idx = rng.below(live.len() as u64) as usize;
            let k = live.swap_remove(idx);
            pending.push(delete(
                &author,
                Collection::ListBlock,
                &format!("k{k}"),
                next_rev(),
            ));
        }
        ops += 1;
        if pending.len() >= 100 {
            env.firehose(std::mem::take(&mut pending)).await?;
        }
    }
    // Blocks and listitems churn (authored_blocks, owned_items, item_count).
    let mut b_live: Vec<(u64, u64)> = Vec::new();
    let mut i_live: Vec<(u64, u64)> = Vec::new();
    for n in 0..6_000u64 {
        let op = rng.below(10);
        if n % 2 == 0 {
            let a = rng.below(AUTHORS);
            let subject = plc("churnsubj", rng.below(500));
            if op < 6 || b_live.is_empty() {
                b_live.push((a, n));
                pending.push(block(
                    &plc("churnauthor", a),
                    &format!("b{n}"),
                    &subject,
                    next_rev(),
                ));
            } else if op < 8 {
                let (a, k) = b_live[rng.below(b_live.len() as u64) as usize];
                pending.push(block(
                    &plc("churnauthor", a),
                    &format!("b{k}"),
                    &subject,
                    next_rev(),
                ));
            } else {
                let (a, k) = b_live.swap_remove(rng.below(b_live.len() as u64) as usize);
                pending.push(delete(
                    &plc("churnauthor", a),
                    Collection::Block,
                    &format!("b{k}"),
                    next_rev(),
                ));
            }
        } else {
            let o = rng.below(OWNERS);
            let lj = rng.below(LISTS_PER_OWNER);
            let subject = plc("churnmember", rng.below(500));
            if op < 6 || i_live.is_empty() {
                i_live.push((o, n));
                pending.push(item(
                    &owners[o as usize],
                    &format!("i{n}"),
                    &format!("l{lj}"),
                    &subject,
                    next_rev(),
                ));
            } else if op < 8 {
                // Move between the owner's own lists (maybe untracked).
                let (o, k) = i_live[rng.below(i_live.len() as u64) as usize];
                pending.push(item(
                    &owners[o as usize],
                    &format!("i{k}"),
                    &format!("l{lj}"),
                    &subject,
                    next_rev(),
                ));
            } else {
                let (o, k) = i_live.swap_remove(rng.below(i_live.len() as u64) as usize);
                pending.push(delete(
                    &owners[o as usize],
                    Collection::ListItem,
                    &format!("i{k}"),
                    next_rev(),
                ));
            }
        }
        if pending.len() >= 100 {
            env.firehose(std::mem::take(&mut pending)).await?;
        }
        // Interleave purges so purge batches race the churn.
        if n % 1000 == 999 {
            janitor::process_purges(&env.pool, &env.limits, &env.counters, 1000).await?;
        }
    }
    if !pending.is_empty() {
        env.firehose(std::mem::take(&mut pending)).await?;
    }
    for _ in 0..5 {
        janitor::process_purges(&env.pool, &env.limits, &env.counters, 1000).await?;
    }
    let stored = scalar(env, "SELECT count(*) FROM list_blocks").await?;
    let expected_live: usize = lb_live.values().map(Vec::len).sum();
    c.check(
        "10k listblocks inserted with churn; stored rows = live keys (nothing lost or duplicated)",
        stored == expected_live as i64,
        format!("{creates} creates in {ops} ops; {stored} rows stored, {expected_live} expected"),
    );
    let tracked = scalar(
        env,
        "SELECT count(*) FROM lists WHERE track_state IN (1,2,3,4)",
    )
    .await?;
    let uncounted = scalar(env, "SELECT count(*) FROM list_blocks WHERE NOT counted").await?;
    c.check(
        "churn exercised tracking",
        tracked > 0,
        format!("{tracked} tracked lists, {uncounted} uncounted listblocks"),
    );

    consistency(env, c, "after churn").await?;
    let r = recount_all(env).await?;
    c.check(
        "nightly recount finds no drift",
        r.drift.is_empty(),
        format!(
            "checked {}, drift {:?}",
            r.checked,
            r.drift.iter().take(5).collect::<Vec<_>>()
        ),
    );

    // The recount is not vacuous: injected drift is found and repaired.
    sqlx::query(
        "UPDATE lists SET listblock_count = listblock_count + 1
         WHERE id = (SELECT id FROM lists WHERE listblock_count > 0 ORDER BY id LIMIT 1)",
    )
    .execute(&env.pool)
    .await?;
    sqlx::query(
        "UPDATE actors SET owned_items = owned_items + 5
         WHERE id = (SELECT owner_id FROM list_items ORDER BY owner_id LIMIT 1)",
    )
    .execute(&env.pool)
    .await?;
    let r = recount_all(env).await?;
    let mut cols: Vec<&str> = r.drift.iter().map(|d| d.column).collect();
    cols.sort_unstable();
    c.eq(
        "injected drift detected",
        cols,
        vec!["listblock_count", "owned_items"],
    );
    let r = recount_all(env).await?;
    c.eq("drift repaired", r.drift.len(), 0);
    consistency(env, c, "after repair").await?;
    Ok(())
}

/// Stream 6: counted stickiness and the capped-debt re-evaluation.
pub async fn s6_counted_stickiness(env: &mut Env, c: &mut Checks) -> Result<()> {
    // 5,000 counted listblocks on 5,000 new lists are 5,000 admissions;
    // the default per-DID daily admission rate (200) would uncount most of
    // them, so it is raised for this stream to isolate the trigger cap.
    env.limits.cfg.did_admissions_per_day = 10_000;
    let cap = env.limits.cfg.listblock_fetch_triggers_per_author;
    c.eq("trigger cap is the design default", cap, 5_000);
    let a = plc("stickyauthor", 1);
    let owners: Vec<_> = (0..50).map(|i| plc("stickyowner", i)).collect();
    let mut n_rev = 100u64;
    let mut batch = Vec::new();
    for i in 0..5_000u64 {
        n_rev += 1;
        batch.push(listblock(
            &a,
            &format!("c{i:04}"),
            &owners[(i % 50) as usize],
            &format!("l{i}"),
            rev(n_rev),
        ));
        if batch.len() == 500 {
            env.firehose(std::mem::take(&mut batch)).await?;
        }
    }
    let a_id = env.actor_id(&a).await?.unwrap_or(-1);
    let counts = |sql: &'static str| {
        let pool = env.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sql)
                .bind(a_id)
                .fetch_one(&pool)
                .await
                .map_err(StorageError::from)
        }
    };
    let counted_sql = "SELECT count(*) FROM list_blocks WHERE author_id = $1 AND counted";
    let triggers_sql = "SELECT fetch_triggers::BIGINT FROM actors WHERE id = $1";
    c.eq(
        "5,000 counted listblocks",
        counts(counted_sql).await?,
        5_000,
    );
    c.eq(
        "fetch_triggers at the cap",
        counts(triggers_sql).await?,
        5_000,
    );

    // The next listblock is stored uncounted with a capped debt.
    n_rev += 1;
    let r = env
        .firehose(vec![listblock(&a, "u1", &owners[0], "x1", rev(n_rev))])
        .await?;
    c.eq(
        "5,001st listblock stored uncounted",
        (r.applied, r.uncounted),
        (1, 1),
    );
    c.eq(
        "capped debt with cap_type trigger cap",
        env.debt(&a, DebtReason::Capped.code()).await?,
        Some(Some(CapType::TriggerCap.code())),
    );
    let x1 = env.list_id(&owners[0], "x1").await?.unwrap_or(-1);
    c.eq(
        "uncounted listblock cannot admit its list",
        (
            env.list_view(x1).await?.state,
            env.list_view(x1).await?.listblock_count,
        ),
        (TrackState::Untracked, 0),
    );

    // Re-listing keeps both flags (sticky per (author, rkey)).
    n_rev += 10;
    let r = env
        .listing(
            vec![
                listblock(&a, "c0000", &owners[0], "l0", 0),
                listblock(&a, "u1", &owners[0], "x1", 0),
            ],
            rev(n_rev),
        )
        .await?;
    c.eq("re-listing applied both rows", r.applied, 2);
    c.eq(
        "re-listing kept c0000 counted",
        env.listblock_row(&a, "c0000").await?.map(|r| r.2),
        Some(true),
    );
    c.eq(
        "re-listing kept u1 uncounted",
        env.listblock_row(&a, "u1").await?.map(|r| r.2),
        Some(false),
    );

    // Four more uncounted rows, then three deletes of counted rows.
    let mut writes = Vec::new();
    for k in 2..=5 {
        n_rev += 1;
        writes.push(listblock(
            &a,
            &format!("u{k}"),
            &owners[0],
            &format!("x{k}"),
            rev(n_rev),
        ));
    }
    let r = env.firehose(writes).await?;
    c.eq("u2..u5 uncounted", r.uncounted, 4);
    let mut writes = Vec::new();
    for k in ["c0000", "c0001", "c0002"] {
        n_rev += 1;
        writes.push(delete(&a, Collection::ListBlock, k, rev(n_rev)));
    }
    env.firehose(writes).await?;
    c.eq(
        "fetch_triggers after 3 deletes",
        counts(triggers_sql).await?,
        4_997,
    );

    // Clean run with a capped debt: uncounted → counted in rkey order,
    // while under the cap.
    // Clear u1's stored key so the check below proves the re-evaluation
    // assigns the author's current key (the insert had already stored it).
    sqlx::query("UPDATE list_blocks SET sched_key = NULL WHERE author_id = $1 AND rkey = 'u1'")
        .bind(a_id)
        .execute(&env.pool)
        .await?;
    let rep = recount::reevaluate_uncounted(&env.pool, &env.limits, &env.counters, &a).await?;
    c.eq(
        "re-evaluation flipped 3, left 2, stopped by the trigger cap",
        (rep.flipped, rep.remaining, rep.stopped_by),
        (3, 2, Some(CapType::TriggerCap)),
    );
    for (k, want) in [
        ("u1", true),
        ("u2", true),
        ("u3", true),
        ("u4", false),
        ("u5", false),
    ] {
        c.eq(
            format!("{k} counted = {want} (rkey order)"),
            env.listblock_row(&a, k).await?.map(|r| r.2),
            Some(want),
        );
    }
    for k in 1..=3 {
        let id = env
            .list_id(&owners[0], &format!("x{k}"))
            .await?
            .unwrap_or(-1);
        let v = env.list_view(id).await?;
        c.eq(
            format!("x{k} admitted by the flip"),
            (v.state, v.listblock_count),
            (TrackState::Pending, 1),
        );
    }
    let key: Option<String> = sqlx::query_scalar(
        "SELECT sched_key FROM list_blocks WHERE author_id = $1 AND rkey = 'u1'",
    )
    .bind(a_id)
    .fetch_one(&env.pool)
    .await?;
    c.eq(
        "flipped row carries the author's current key",
        key,
        Some(format!("unresolved:{a}")),
    );
    c.eq(
        "never un-counts: counted total back at the cap",
        counts(counted_sql).await?,
        5_000,
    );
    consistency(env, c, "after re-evaluation").await?;
    Ok(())
}

/// Stream 7: tombstone TTL and the 72 h stamp invariant.
pub async fn s7_tombstone_ttl(env: &mut Env, c: &mut Checks) -> Result<()> {
    let a = plc("ttlauthor", 1);
    let s = plc("ttlsubj", 1);
    let ttl = env.limits.tombstone_ttl;
    c.eq(
        "tombstone TTL is 7 days",
        ttl,
        Duration::from_secs(7 * 86_400),
    );
    env.firehose(vec![block(&a, "k", &s, rev(100))]).await?;
    let e = rev(200);
    env.firehose(vec![delete(&a, Collection::Block, "k", e)])
        .await?;
    let t: DateTime<Utc> = sqlx::query_scalar(
        "SELECT t.deleted_at FROM tombstones t JOIN actors a ON a.id = t.author_id
         WHERE a.did = $1 AND t.rkey = 'k'",
    )
    .bind(a.as_str())
    .fetch_one(&env.pool)
    .await?;

    let n =
        janitor::purge_tombstones(&env.pool, t + chrono::Duration::hours(6 * 24 + 23), ttl).await?;
    c.eq("janitor at T + 6d23h deletes nothing", n, 0);
    c.eq(
        "tombstone still present",
        env.tombstone(Collection::Block, &a, "k").await?,
        Some(e),
    );
    let r = env.listing(vec![block(&a, "k", &s, 0)], rev(150)).await?;
    c.eq(
        "stale listing (R < E) cannot re-insert while the tombstone lives",
        r.stale,
        1,
    );
    c.eq("row absent", env.block_row(&a, "k").await?, None);

    let n =
        janitor::purge_tombstones(&env.pool, t + chrono::Duration::hours(7 * 24 + 1), ttl).await?;
    c.eq("janitor at T + 7d1h deletes the tombstone", n, 1);
    c.eq(
        "tombstone gone",
        env.tombstone(Collection::Block, &a, "k").await?,
        None,
    );

    // With the tombstone gone, a listing stamped R < E could re-insert the
    // deleted record — which is why such a stamp must never be applied
    // after t_R + 72 h. A stamp R < E was read before the delete (t_R < T),
    // so at T + 7d1h it is > 72 h old; apply refuses it.
    let now = db_now(env).await?;
    let stale_read_at = now - chrono::Duration::hours(7 * 24 + 1);
    let err = env
        .listing_at(vec![block(&a, "k", &s, 0)], rev(150), stale_read_at)
        .await;
    c.check(
        "listing with a stamp read > 72 h ago is refused (StaleStamp)",
        matches!(err, Err(StorageError::StaleStamp(_))),
        format!("{err:?}"),
    );
    c.eq(
        "deleted record not resurrected",
        env.block_row(&a, "k").await?,
        None,
    );
    // Boundary of the 72 h window.
    let ok = env
        .listing_at(
            vec![block(&a, "k71", &s, 0)],
            rev(300),
            now - chrono::Duration::hours(71),
        )
        .await;
    c.check(
        "stamp read 71 h ago is accepted",
        ok.is_ok(),
        format!("{ok:?}"),
    );
    let late = env
        .listing_at(
            vec![block(&a, "k73", &s, 0)],
            rev(300),
            now - chrono::Duration::hours(73),
        )
        .await;
    c.check(
        "stamp read 73 h ago is refused",
        matches!(late, Err(StorageError::StaleStamp(_))),
        format!("{late:?}"),
    );
    Ok(())
}

/// Stream 8: lock order under contention and the deadlock retry path.
pub async fn s8_lock_contention(env: &mut Env, c: &mut Checks) -> Result<()> {
    // Part A: two writers with disjoint authors (so author locks do not
    // serialize them) that touch the same two lists in opposite batch
    // orders, each round. Sorted list-lock acquisition must make this
    // deadlock-free: every other row they share (the lists rows, the
    // owner's actors row) is written under those list locks.
    let a = plc("contendera", 1);
    let b = plc("contenderb", 1);
    let o = plc("contendowner", 1);
    let rounds = 100u64;
    let mut max_retries = 0u32;
    let mut failures = Vec::new();
    for r in 0..rounds {
        let w1 = vec![
            listblock(&a, &format!("p{r}"), &o, "L1", rev(1000 + r * 4)),
            listblock(&a, &format!("q{r}"), &o, "L2", rev(1001 + r * 4)),
        ];
        let w2 = vec![
            listblock(&b, &format!("p{r}"), &o, "L2", rev(1000 + r * 4)),
            listblock(&b, &format!("q{r}"), &o, "L1", rev(1001 + r * 4)),
        ];
        let (x, y) = tokio::join!(env.firehose(w1), env.firehose(w2));
        for res in [x, y] {
            match res {
                Ok(rep) => max_retries = max_retries.max(rep.deadlock_retries),
                Err(e) => failures.push(e.to_string()),
            }
        }
    }
    c.check(
        "opposite-order writers: every batch committed",
        failures.is_empty(),
        format!("{} failures; first: {:?}", failures.len(), failures.first()),
    );
    c.eq(
        "opposite-order writers: sorted lock order never deadlocks (0 retries)",
        max_retries,
        0,
    );
    consistency(env, c, "after contention").await?;

    // Part B: two writers with disjoint authors (no shared advisory lock)
    // intern the same 200 new subjects in opposite orders. Before the
    // intern locks, the unique index on actors.did made them deadlock
    // repeatedly (a batch exhausted its retries). With intern locks taken in
    // ascending order this must commit with no deadlock at all.
    let mut failures = Vec::new();
    let mut max_retries = 0;
    for r in 0..10u64 {
        let subjects: Vec<_> = (0..200).map(|i| plc("forcedsubj", r * 1000 + i)).collect();
        let x = plc("forcedx", r);
        let y = plc("forcedy", r);
        let w1: Vec<_> = subjects
            .iter()
            .enumerate()
            .map(|(i, s)| block(&x, &format!("k{i}"), s, rev(10 + i as u64)))
            .collect();
        let w2: Vec<_> = subjects
            .iter()
            .rev()
            .enumerate()
            .map(|(i, s)| block(&y, &format!("k{i}"), s, rev(10 + i as u64)))
            .collect();
        let (p, q) = tokio::join!(env.firehose(w1), env.firehose(w2));
        for res in [p, q] {
            match res {
                Ok(rep) => max_retries = max_retries.max(rep.deadlock_retries),
                Err(e) => failures.push(e.to_string()),
            }
        }
    }
    c.check(
        "shared new subjects, opposite orders: every batch committed",
        failures.is_empty(),
        format!("{} failures; first: {:?}", failures.len(), failures.first()),
    );
    c.eq(
        "shared new subjects, opposite orders: intern locks prevent deadlock (0 retries)",
        max_retries,
        0,
    );
    let n = scalar(env, "SELECT count(*) FROM blocks WHERE rkey LIKE 'k%'").await?;
    c.eq(
        "shared new subjects: all 4,000 blocks stored exactly once",
        n,
        4_000,
    );

    // Part C: a deadlock that ordering does not prevent, to exercise the
    // retry path. did:web authors are charged to their domain's bucket key;
    // writer 1 charges bucket d1 then d2, writer 2 charges d2 then d1, so
    // their intern_rate row locks cross. Postgres aborts one with 40P01 and
    // `apply` must retry it to completion.
    let mut rounds_with_deadlock = 0;
    let mut failures = Vec::new();
    let mut max_retries = 0;
    let web = |host: String| farsight_core::Did::parse(&format!("did:web:{host}")).expect("valid");
    for r in 0..10u64 {
        let d1 = format!("forcedalpha{r}.com");
        let d2 = format!("forcedbeta{r}.com");
        let (a1, b2) = (web(format!("a.{d1}")), web(format!("b.{d2}")));
        let (c2, e1) = (web(format!("c.{d2}")), web(format!("e.{d1}")));
        let seg = |author: &farsight_core::Did, tag: &str| -> Vec<farsight_storage::apply::Write> {
            (0..50u64)
                .map(|i| {
                    block(
                        author,
                        &format!("m{i}"),
                        &plc(tag, r * 1000 + i),
                        rev(10 + i),
                    )
                })
                .collect()
        };
        let mut w1 = seg(&a1, "ratesubja");
        w1.extend(seg(&b2, "ratesubjb"));
        let mut w2 = seg(&c2, "ratesubjc");
        w2.extend(seg(&e1, "ratesubje"));
        let (p, q) = tokio::join!(env.firehose(w1), env.firehose(w2));
        let mut any = false;
        for res in [p, q] {
            match res {
                Ok(rep) => {
                    any |= rep.deadlock_retries > 0;
                    max_retries = max_retries.max(rep.deadlock_retries);
                }
                Err(e) => failures.push(e.to_string()),
            }
        }
        if any {
            rounds_with_deadlock += 1;
        }
    }
    c.check(
        "crossed rate rows: every batch eventually committed (no livelock)",
        failures.is_empty(),
        format!("{} failures; first: {:?}", failures.len(), failures.first()),
    );
    if rounds_with_deadlock == 0 {
        c.unverified(
            "crossed rate rows: deadlock retry path exercised",
            "no 40P01 occurred in 10 rounds; the retry path was not exercised by this run",
        );
    } else {
        c.check(
            "crossed rate rows: deadlock retry path exercised",
            true,
            format!("{rounds_with_deadlock}/10 rounds hit a deadlock; max retries {max_retries}"),
        );
    }
    let n = scalar(env, "SELECT count(*) FROM blocks WHERE rkey LIKE 'm%'").await?;
    // 10 rounds × 2 writers × 2 authors × 50 blocks.
    c.eq(
        "crossed rate rows: all 2,000 blocks stored exactly once",
        n,
        2_000,
    );
    Ok(())
}

fn progress(applied_through: DateTime<Utc>, seq: i64) -> FirehoseProgress {
    FirehoseProgress {
        source_url: "wss://jetstream.test".to_owned(),
        protocol: Protocol::V2,
        cursor_seq: Some(seq),
        cursor_us: None,
        applied_through,
    }
}

async fn firehose_batch(
    env: &Env,
    writes: Vec<farsight_storage::apply::Write>,
    p: FirehoseProgress,
) -> Result<farsight_storage::txn::ApplyReport> {
    let mut b = Batch::new(Origin::Firehose);
    let w = p.applied_through;
    b.writes = writes
        .into_iter()
        .map(|mut x| {
            x.witness = Some(w);
            x
        })
        .collect();
    b.firehose = Some(p);
    apply::apply(&env.pool, &env.ctx(), &b).await
}

async fn expect_notify(listener: &mut PgListener) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(5), listener.recv()).await,
        Ok(Ok(n)) if n.channel() == COVERAGE_CHANNEL
    )
}

async fn drain(listener: &mut PgListener) {
    while let Ok(Ok(Some(_))) =
        tokio::time::timeout(Duration::from_millis(200), listener.try_recv()).await
    {}
}

/// Stream 9: NOTIFY sender, firehose_clock/`clock(t)`, gaps, snapshot.
pub async fn s9_coverage_plumbing(env: &mut Env, c: &mut Checks) -> Result<()> {
    let lag = env.limits.synthetic_gap_lag;
    c.eq(
        "clock undefined before the first batch",
        firehose::clock_now(&env.pool).await?,
        None,
    );
    let snap = coverage::read_snapshot(&env.pool, &env.limits).await?;
    c.check(
        "snapshot before first batch: nothing covered",
        !snap.covered(Some(snap.read_at), lag) && snap.firehose.applied_through.is_none(),
        format!("{:?}", snap.firehose),
    );

    let mut listener = PgListener::connect_with(&env.pool).await?;
    listener.listen(COVERAGE_CHANNEL).await?;

    let w1 = now_micros();
    firehose_batch(env, vec![], progress(w1, 1)).await?;
    c.check(
        "NOTIFY on ingest batch commit",
        expect_notify(&mut listener).await,
        "",
    );
    let between = db_now(env).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let w2 = w1 + chrono::Duration::seconds(1);
    firehose_batch(env, vec![], progress(w2, 2)).await?;
    // A failover rewind re-applies older events without lowering the max.
    firehose_batch(env, vec![], progress(w1 - chrono::Duration::seconds(30), 3)).await?;
    drain(&mut listener).await;
    let st = firehose::read_state(&env.pool).await?;
    c.eq(
        "applied_through is a running maximum",
        st.applied_through,
        Some(w2),
    );
    c.eq("cursor persisted with the batch", st.cursor_seq, Some(3));
    c.eq(
        "one clock row per batch",
        scalar(env, "SELECT count(*) FROM firehose_clock").await?,
        3,
    );
    c.eq(
        "clock(t) rounds down to the batch before t",
        firehose::clock(&env.pool, between).await?,
        Some(w1),
    );
    c.eq(
        "clock(now) = latest",
        firehose::clock_now(&env.pool).await?,
        Some(w2),
    );

    // Tracking change ⇒ NOTIFY; a live pending list lowers indexedAt.
    let blocker = plc("covblocker", 1);
    let owner = plc("covowner", 1);
    let w3 = w2 + chrono::Duration::seconds(1);
    // No firehose progress in this batch: only the admission may set the
    // notify flag.
    let mut w = listblock(&blocker, "k", &owner, "L", rev(10));
    w.witness = Some(w3);
    env.firehose(vec![w]).await?;
    c.check(
        "NOTIFY on admission",
        expect_notify(&mut listener).await,
        "",
    );
    drain(&mut listener).await;
    let snap = coverage::read_snapshot(&env.pool, &env.limits).await?;
    let l = env.list_id(&owner, "L").await?.unwrap_or(-1);
    c.eq("snapshot: one pending list", snap.lists.pending, 1);
    c.eq(
        "snapshot: pending list takes effect",
        snap.pending.effective.clone(),
        vec![l],
    );
    c.eq(
        "snapshot: indexedAt capped just before the admitting listblock's witness",
        snap.pending.indexed_at_cap,
        Some(w3 - chrono::Duration::microseconds(1)),
    );
    c.check(
        "snapshot: live admission is not historical",
        !snap.pending.historical,
        "",
    );
    // A listing-stored listblock (witnessed_at NULL) makes it historical.
    env.listing(
        vec![listblock(&plc("covblocker", 2), "k", &owner, "L", 0)],
        rev(20),
    )
    .await?;
    let snap = coverage::read_snapshot(&env.pool, &env.limits).await?;
    c.check(
        "snapshot: historical listblock ⇒ list_pending_historical",
        snap.pending.historical,
        "",
    );
    // A resync debt makes the author uncovered: its listblock stops counting.
    let b2 = env.actor_id(&plc("covblocker", 2)).await?.unwrap_or(-1);
    farsight_storage::debts::add_debt(&env.pool, b2, DebtReason::Resync, None, w3).await?;
    c.check(
        "NOTIFY on debt insert",
        expect_notify(&mut listener).await,
        "",
    );
    drain(&mut listener).await;
    let snap = coverage::read_snapshot(&env.pool, &env.limits).await?;
    c.check(
        "snapshot: an author with a resync debt is not a covered author",
        !snap.pending.historical && snap.debt_counts.get(&DebtReason::Resync) == Some(&1),
        format!("{:?}", snap.pending),
    );
    let d = farsight_storage::debts::debts_for(&env.pool, &[b2]).await?;
    c.eq(
        "debts_for reads per actor and reason",
        d.get(&b2).map(|v| v[0].reason),
        Some(DebtReason::Resync),
    );

    // Gaps: the v1 interval is one open gap, idempotently.
    // applied_through is w2 here (the admission batch carried no progress),
    // so the gap opens at w2 for [t, applied_through] to be non-empty.
    let g1 = firehose::open_sync_unavailable(&env.pool, w2).await?;
    let g2 = firehose::open_sync_unavailable(&env.pool, w2).await?;
    c.eq("v1-interval gap open is idempotent", g1, g2);
    let snap = coverage::read_snapshot(&env.pool, &env.limits).await?;
    c.check(
        "open v1 gap ⇒ nothing after it covered, sync_events_unavailable",
        !snap.covered(Some(w2), lag)
            && !snap.covered(Some(w1), lag)
            && coverage::network_scope(&snap, 1, lag)
                .reasons
                .contains(&"sync_events_unavailable"),
        format!("{:?}", coverage::network_scope(&snap, 1, lag)),
    );
    let closed =
        firehose::close_sync_unavailable(&env.pool, w3 + chrono::Duration::seconds(5)).await?;
    c.eq("close returns the v1 gap", closed, Some(g1));
    let gaps = firehose::unhealed_gaps(&env.pool).await?;
    c.check(
        "closed v1 gap stays unhealed until repair",
        gaps.len() == 1 && gaps[0].cause == GapCause::SyncUnavailable && gaps[0].to_at.is_some(),
        format!("{gaps:?}"),
    );
    let healed =
        firehose::heal_gaps(&env.pool, &[g1], w3 + chrono::Duration::seconds(9), 1).await?;
    c.eq("repair heals the closed gap", healed, 1);

    // firehose_clock maintenance: 1-minute granularity after 24 h, 30 d
    // retention.
    let now = db_now(env).await?;
    let old = now - chrono::Duration::days(2);
    for s in [0, 10, 20, 70, 80] {
        sqlx::query("INSERT INTO firehose_clock (server_at, witness_at) VALUES ($1, $1)")
            .bind(old + chrono::Duration::seconds(s))
            .execute(&env.pool)
            .await?;
    }
    sqlx::query("INSERT INTO firehose_clock (server_at, witness_at) VALUES ($1, $1)")
        .bind(now - chrono::Duration::days(31))
        .execute(&env.pool)
        .await?;
    let before = scalar(env, "SELECT count(*) FROM firehose_clock").await?;
    let removed = firehose::maintain_clock(&env.pool, now).await?;
    let after = scalar(env, "SELECT count(*) FROM firehose_clock").await?;
    let per_minute: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM firehose_clock WHERE server_at < $1 - interval '24 hours'",
    )
    .bind(now)
    .fetch_one(&env.pool)
    .await?;
    let minutes = scalar(
        env,
        "SELECT count(DISTINCT date_trunc('minute', server_at)) FROM firehose_clock
         WHERE server_at < now() - interval '24 hours'",
    )
    .await?;
    let expired: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM firehose_clock WHERE server_at < $1 - interval '30 days'",
    )
    .bind(now)
    .fetch_one(&env.pool)
    .await?;
    c.check(
        "clock maintenance: one row per minute after 24 h, none after 30 d",
        per_minute == minutes && removed as i64 == before - after && expired == 0 && before - after >= 1,
        format!("before {before}, after {after}, removed {removed}, old rows {per_minute}, minutes {minutes}"),
    );
    Ok(())
}
