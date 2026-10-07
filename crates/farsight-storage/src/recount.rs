//! Nightly recounts (the listblock counters, in batches, each under the
//! list lock: repair drift, re-run the transition function for every
//! repaired list, alert; and the rebuild of the approximate counters; see
//! `docs/design/list-indexing.md` and `docs/design/storage.md`) and the
//! `capped`-debt re-evaluation of uncounted listblocks.

use crate::codes::sql::RECORD_PRESENT;
use crate::ids::{ActorId, ListId};
use std::collections::{BTreeMap, BTreeSet};

use farsight_core::Did;
use sqlx::PgPool;

use crate::apply::decide_counted;
use crate::codes::CapType;
use crate::counters::{CounterSink, stat};
use crate::error::{Result, StorageError};
use crate::keys::{self, HostFacts, Limits};
use crate::tracking::FireArgs;
use crate::transition::Event;
use crate::txn::{Gates, Txn};

/// The row a repaired counter is on: the list counters are columns of
/// `lists`, the per-author counters columns of `actors`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftRow {
    /// A `lists` row.
    List(ListId),
    /// An `actors` row.
    Actor(ActorId),
}

impl std::fmt::Display for DriftRow {
    /// The row's id, as a number.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriftRow::List(id) => id.fmt(f),
            DriftRow::Actor(id) => id.fmt(f),
        }
    }
}

/// One repaired counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drift {
    /// The row the counter is on.
    pub id: DriftRow,
    /// Name of the counter column, e.g. `listblock_count`.
    pub column: &'static str,
    /// Stored value before repair.
    pub stored: i64,
    /// What counting the rows gave; the column holds it after the repair.
    pub actual: i64,
}

/// Result of a recount pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecountReport {
    /// Rows checked.
    pub checked: u64,
    /// Every repaired counter (empty = no drift).
    pub drift: Vec<Drift>,
}

/// Recounts `listblock_count` and `item_count` for lists with
/// `id > after`, up to `batch` lists, each under list(L) exclusive, and
/// fires **+** / **−** where a repair crosses zero. Returns the report and
/// the last id checked (`None` when done).
pub async fn recount_lists(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    after: ListId,
    batch: i64,
) -> Result<(RecountReport, Option<ListId>)> {
    let lists: Vec<(ListId, String, String)> = sqlx::query_as(
        "SELECT l.id, a.did, l.rkey FROM lists l JOIN actors a ON a.id = l.owner_id
         WHERE l.id > $1 ORDER BY l.id LIMIT $2",
    )
    .bind(after)
    .bind(batch)
    .fetch_all(pool)
    .await?;
    let mut report = RecountReport::default();
    let mut last = None;
    for (id, owner, rkey) in lists {
        last = Some(id);
        let mut tx = pool.begin().await?;
        let deltas = {
            let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
            t.lock_lists(
                &[(keys::list_lock_key(&owner, &rkey), true)]
                    .into_iter()
                    .collect(),
            )
            .await?;
            let (stored_lb, stored_items, actual_lb, actual_items): (i32, i32, i64, i64) =
                sqlx::query_as(
                    "SELECT l.listblock_count, l.item_count,
                            (SELECT count(*) FROM list_blocks b WHERE b.list_id = l.id AND b.counted),
                            (SELECT count(*) FROM list_items i WHERE i.list_id = l.id)
                     FROM lists l WHERE l.id = $1",
                )
                .bind(id)
                .fetch_one(&mut *t.conn)
                .await?;
            report.checked += 1;
            if i64::from(stored_items) != actual_items {
                report.drift.push(Drift {
                    id: DriftRow::List(id),
                    column: "item_count",
                    stored: i64::from(stored_items),
                    actual: actual_items,
                });
                sqlx::query("UPDATE lists SET item_count = $2 WHERE id = $1")
                    .bind(id)
                    .bind(actual_items as i32)
                    .execute(&mut *t.conn)
                    .await?;
            }
            if i64::from(stored_lb) != actual_lb {
                report.drift.push(Drift {
                    id: DriftRow::List(id),
                    column: "listblock_count",
                    stored: i64::from(stored_lb),
                    actual: actual_lb,
                });
                sqlx::query("UPDATE lists SET listblock_count = $2 WHERE id = $1")
                    .bind(id)
                    .bind(actual_lb as i32)
                    .execute(&mut *t.conn)
                    .await?;
                let state = t.list_tracking(id).await?.state;
                if stored_lb == 0 && actual_lb > 0 {
                    t.fire(id, Event::Plus, FireArgs::default()).await?;
                } else if stored_lb > 0 && actual_lb == 0 {
                    t.fire(id, Event::Minus, FireArgs::default()).await?;
                } else if state.is_waiting() {
                    t.rebuild_sched_keys(id).await?;
                }
            }
            if t.notify {
                t.send_notify().await?;
            }
            t.finish().1
        };
        tx.commit().await?;
        counters.add(deltas);
    }
    Ok((report, last))
}

/// Recounts the exact per-actor counters (`authored_blocks`,
/// `authored_listblocks`, `authored_lists`, `owned_items`,
/// `fetch_triggers`) for actors with `id > after`, `batch` at a time, under
/// their author locks. Returns the report and the last id checked.
pub async fn recount_actors(
    pool: &PgPool,
    after: ActorId,
    batch: i64,
) -> Result<(RecountReport, Option<ActorId>)> {
    let actors: Vec<(ActorId, String)> =
        sqlx::query_as("SELECT id, did FROM actors WHERE id > $1 ORDER BY id LIMIT $2")
            .bind(after)
            .bind(batch)
            .fetch_all(pool)
            .await?;
    let mut report = RecountReport::default();
    let Some(&(last, _)) = actors.last() else {
        return Ok((report, None));
    };
    let first = actors[0].0;
    let mut tx = pool.begin().await?;
    let locks: BTreeSet<i64> = actors
        .iter()
        .map(|(_, d)| keys::author_lock_key(d))
        .collect();
    for k in &locks {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(*k)
            .execute(&mut *tx)
            .await?;
    }
    type Row = (ActorId, i32, i32, i32, i32, i32, i64, i64, i64, i64, i64);
    let rows: Vec<Row> = sqlx::query_as(
        &format!("SELECT a.id, a.authored_blocks, a.authored_listblocks, a.authored_lists,
                a.owned_items, a.fetch_triggers,
                (SELECT count(*) FROM blocks x WHERE x.author_id = a.id),
                (SELECT count(*) FROM list_blocks x WHERE x.author_id = a.id),
                (SELECT count(*) FROM lists x WHERE x.owner_id = a.id AND x.record_state = {RECORD_PRESENT}),
                (SELECT count(*) FROM list_items x WHERE x.owner_id = a.id),
                (SELECT count(*) FROM list_blocks x WHERE x.author_id = a.id AND x.counted)
         FROM actors a WHERE a.id BETWEEN $1 AND $2 ORDER BY a.id"),
    )
    .bind(first)
    .bind(last)
    .fetch_all(&mut *tx)
    .await?;
    for r in rows {
        report.checked += 1;
        let checks: [(&'static str, i32, i64); 5] = [
            ("authored_blocks", r.1, r.6),
            ("authored_listblocks", r.2, r.7),
            ("authored_lists", r.3, r.8),
            ("owned_items", r.4, r.9),
            ("fetch_triggers", r.5, r.10),
        ];
        for (column, stored, actual) in checks {
            if i64::from(stored) != actual {
                report.drift.push(Drift {
                    id: DriftRow::Actor(r.0),
                    column,
                    stored: i64::from(stored),
                    actual,
                });
                // Column names come from the fixed list above.
                sqlx::query(&format!("UPDATE actors SET {column} = $2 WHERE id = $1"))
                    .bind(r.0)
                    .bind(actual as i32)
                    .execute(&mut *tx)
                    .await?;
            }
        }
    }
    tx.commit().await?;
    Ok((report, Some(last)))
}

/// Rebuilds `stats_counters` exactly (shard 0 holds the total; other
/// shards are reset) and the `stored_blocks/items/listblocks/lists`
/// columns of `host_usage` from the exact per-author counters, grouped by
/// each author's current buckets. `stored_interned` is a lifetime charge
/// with no per-row record and is left as is; placeholder-list charges in
/// `stored_listblocks` are not reconstructed.
pub async fn rebuild_approximate_counters(pool: &PgPool, batch: i64) -> Result<()> {
    let lists = format!("SELECT count(*) FROM lists WHERE record_state = {RECORD_PRESENT}");
    let exact: [(&str, &str); 5] = [
        (stat::BLOCKS, "SELECT count(*) FROM blocks"),
        (stat::LIST_BLOCKS, "SELECT count(*) FROM list_blocks"),
        (stat::LISTS, &lists),
        (stat::LIST_ITEMS, "SELECT count(*) FROM list_items"),
        (stat::ACTORS, "SELECT count(*) FROM actors"),
    ];
    // Counted first, outside the transaction that writes them: counting
    // the large tables takes a minute, and the lock below is not held
    // that long.
    let mut counts = Vec::with_capacity(exact.len());
    for (name, sql) in exact {
        let n: i64 = sqlx::query_scalar(sql).fetch_one(pool).await?;
        counts.push((name, n));
    }
    let mut tx = pool.begin().await?;
    // The writers' flushes update these rows one at a time, in their own
    // order, and replacing them all at once under row locks deadlocks
    // with a flush in progress. The table lock waits for flushes under
    // way and holds back new ones for the moment the rows are replaced.
    sqlx::query("LOCK TABLE stats_counters IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    for (name, n) in counts {
        sqlx::query("DELETE FROM stats_counters WHERE name = $1")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO stats_counters (name, shard, value) VALUES ($1, 0, $2)")
            .bind(name)
            .bind(n)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;

    let mut usage: BTreeMap<String, [i64; 4]> = BTreeMap::new();
    let mut after = ActorId::new(0);
    type Row = (
        ActorId,
        String,
        Option<String>,
        i32,
        bool,
        Option<String>,
        Option<String>,
        bool,
        i32,
        i32,
        i32,
        i32,
    );
    loop {
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT a.id, a.did, a.admission_key, a.resolve_failures, a.pds_host_id IS NOT NULL,
                    h.cap_key, h.ip_bucket, COALESCE(h.large, false),
                    a.authored_blocks, a.owned_items, a.authored_listblocks, a.authored_lists
             FROM actors a LEFT JOIN pds_hosts h ON h.id = a.pds_host_id
             WHERE a.id > $1 ORDER BY a.id LIMIT $2",
        )
        .bind(after)
        .bind(batch)
        .fetch_all(pool)
        .await?;
        let Some(last) = rows.last().map(|r| r.0) else {
            break;
        };
        after = last;
        for r in rows {
            if r.8 == 0 && r.9 == 0 && r.10 == 0 && r.11 == 0 {
                continue;
            }
            let did = Did::parse(&r.1).map_err(|e| StorageError::Invariant(e.to_string()))?;
            let facts = HostFacts {
                admission_key: r.2,
                resolve_failures: r.3,
                resolved: r.4,
                cap_key: r.5,
                ip_bucket: r.6,
                large: r.7,
            };
            for b in keys::buckets(&did, &facts) {
                let e = usage.entry(b).or_insert([0; 4]);
                e[0] += i64::from(r.8);
                e[1] += i64::from(r.9);
                e[2] += i64::from(r.10);
                e[3] += i64::from(r.11);
            }
        }
    }
    let mut tx = pool.begin().await?;
    // As above: every row is rewritten, so flushes wait for the moment.
    sqlx::query("LOCK TABLE host_usage IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE host_usage SET stored_blocks = 0, stored_items = 0, stored_listblocks = 0,
                               stored_lists = 0",
    )
    .execute(&mut *tx)
    .await?;
    for (bucket, v) in &usage {
        sqlx::query(
            "INSERT INTO host_usage (bucket, stored_blocks, stored_items, stored_listblocks,
                                     stored_lists)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (bucket) DO UPDATE SET stored_blocks = EXCLUDED.stored_blocks,
               stored_items = EXCLUDED.stored_items,
               stored_listblocks = EXCLUDED.stored_listblocks,
               stored_lists = EXCLUDED.stored_lists",
        )
        .bind(bucket.as_str())
        .bind(v[0])
        .bind(v[1])
        .bind(v[2])
        .bind(v[3])
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Result of a `counted` re-evaluation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReevalReport {
    /// Rows flipped uncounted → counted.
    pub flipped: u64,
    /// Uncounted rows remaining.
    pub remaining: u64,
    /// Why the pass stopped early, if it did.
    pub stopped_by: Option<CapType>,
}

/// The one exception to `counted` stickiness: during a clean run of an
/// author holding a `capped` debt, re-evaluates the author's uncounted
/// listblocks in rkey order, flipping them to counted while
/// `fetch_triggers` is under the cap and within the admission key's
/// remaining daily rate, assigning the author's current key as
/// `sched_key`. Never un-counts. The caller (the backfill job) clears the
/// debt only if `remaining == 0` and the run is clean.
pub async fn reevaluate_uncounted(
    pool: &PgPool,
    limits: &Limits,
    counters: &CounterSink,
    did: &Did,
) -> Result<ReevalReport> {
    let mut tx = pool.begin().await?;
    let (report, deltas) = {
        let mut t = Txn::start(&mut tx, limits, Gates::default()).await?;
        t.lock_authors(&[keys::author_lock_key(did.as_str())].into_iter().collect())
            .await?;
        let author = t.author(did).await?;
        let rows: Vec<(String, ListId, String, String)> = sqlx::query_as(
            "SELECT r.rkey, r.list_id, a.did, l.rkey FROM list_blocks r
             JOIN lists l ON l.id = r.list_id JOIN actors a ON a.id = l.owner_id
             WHERE r.author_id = $1 AND NOT r.counted ORDER BY r.rkey",
        )
        .bind(author.id)
        .fetch_all(&mut *t.conn)
        .await?;
        let locks: BTreeMap<i64, bool> = rows
            .iter()
            .map(|(_, _, owner, lrkey)| (keys::list_lock_key(owner, lrkey), true))
            .collect();
        t.lock_lists(&locks).await?;
        let mut out = ReevalReport::default();
        for (rkey, list_id, _, _) in &rows {
            match decide_counted(&mut t, &author, *list_id).await? {
                Ok(()) => {
                    sqlx::query(
                        "UPDATE list_blocks SET counted = true, sched_key = $3
                         WHERE author_id = $1 AND rkey = $2",
                    )
                    .bind(author.id)
                    .bind(rkey.as_str())
                    .bind(author.key.as_str())
                    .execute(&mut *t.conn)
                    .await?;
                    t.change_listblock_count(*list_id, 1, Some(author.key.as_str()))
                        .await?;
                    out.flipped += 1;
                }
                Err(CapType::TriggerCap) => {
                    out.stopped_by = Some(CapType::TriggerCap);
                    break;
                }
                Err(other) => {
                    // Admission rate exhausted: rows on already-tracked
                    // lists may still flip (they consume no rate).
                    out.stopped_by = Some(other);
                }
            }
        }
        out.remaining = u64::try_from(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM list_blocks WHERE author_id = $1 AND NOT counted",
            )
            .bind(author.id)
            .fetch_one(&mut *t.conn)
            .await?,
        )
        .unwrap_or(0);
        if t.notify {
            t.send_notify().await?;
        }
        let (_, deltas) = t.finish();
        (out, deltas)
    };
    tx.commit().await?;
    counters.add(deltas);
    Ok(report)
}
