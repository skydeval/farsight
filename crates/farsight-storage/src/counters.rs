//! Approximate counters (see `docs/design/storage.md`): `stats_counters`
//! and `host_usage`.
//!
//! Apply transactions never touch these hot rows. Each transaction collects
//! its deltas in a [`Deltas`]; only after the transaction **commits** are
//! they merged into the process's [`CounterSink`], which a background task
//! flushes every 5 s ([`FLUSH_INTERVAL`]) in its own transaction. A crash
//! loses at most one interval of deltas; the nightly rebuild
//! ([`crate::recount::rebuild_approximate_counters`]) restores exact values.
//!
//! `admission_rate` and `intern_rate` are *not* here: they are exact and
//! updated inside the apply transaction (see `crate::apply`).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use sqlx::PgPool;

use crate::error::Result;
use crate::keys::{CapKind, Limits};

/// How often the deltas accumulated in a [`CounterSink`] are written to
/// `stats_counters` and `host_usage`. A crash loses at most this much.
pub const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// `stats_counters.name` values.
pub mod stat {
    /// Stored `blocks` rows.
    pub const BLOCKS: &str = "blocks";
    /// Stored `list_blocks` rows.
    pub const LIST_BLOCKS: &str = "list_blocks";
    /// `lists` rows with a present record.
    pub const LISTS: &str = "lists";
    /// Stored `list_items` rows.
    pub const LIST_ITEMS: &str = "list_items";
    /// `actors` rows.
    pub const ACTORS: &str = "actors";
    /// All names.
    pub const ALL: [&str; 5] = [BLOCKS, LIST_BLOCKS, LISTS, LIST_ITEMS, ACTORS];
}

/// Per-bucket usage delta.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HostDelta {
    /// Change to `host_usage.stored_blocks`.
    pub blocks: i64,
    /// Change to `host_usage.stored_items`.
    pub items: i64,
    /// `stored_listblocks` (placeholder lists included).
    pub listblocks: i64,
    /// Change to `host_usage.stored_lists`.
    pub lists: i64,
    /// `stored_interned` (never decremented).
    pub interned: i64,
}

impl HostDelta {
    fn add(&mut self, o: &HostDelta) {
        self.blocks += o.blocks;
        self.items += o.items;
        self.listblocks += o.listblocks;
        self.lists += o.lists;
        self.interned += o.interned;
    }

    fn get_mut(&mut self, kind: CapKind) -> &mut i64 {
        match kind {
            CapKind::Blocks => &mut self.blocks,
            CapKind::Items => &mut self.items,
            CapKind::Listblocks => &mut self.listblocks,
            CapKind::Lists => &mut self.lists,
            CapKind::Interned => &mut self.interned,
        }
    }
}

/// Deltas collected by one transaction (or merged across many).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Deltas {
    /// `stats_counters` name → delta.
    pub stats: HashMap<&'static str, i64>,
    /// `host_usage` bucket → delta.
    pub hosts: HashMap<String, HostDelta>,
    /// History rows written and skipped; published as metrics when the
    /// transaction's deltas reach the sink, never flushed to a table.
    pub history: crate::history::Counts,
}

impl Deltas {
    /// Adds `n` (which may be negative) to the counter called `name`, one
    /// of the names in the `stat` module.
    pub fn stat(&mut self, name: &'static str, n: i64) {
        *self.stats.entry(name).or_insert(0) += n;
    }

    /// Adds `n` of `kind` to every bucket in `buckets`.
    pub fn host(&mut self, buckets: &[String], kind: CapKind, n: i64) {
        for b in buckets {
            *self.hosts.entry(b.clone()).or_default().get_mut(kind) += n;
        }
    }

    /// Merges another set of deltas into this one.
    pub fn merge(&mut self, other: Deltas) {
        for (k, v) in other.stats {
            *self.stats.entry(k).or_insert(0) += v;
        }
        for (k, v) in other.hosts {
            self.hosts.entry(k).or_default().add(&v);
        }
    }

    /// True if nothing is pending.
    pub fn is_empty(&self) -> bool {
        self.stats.values().all(|v| *v == 0)
            && self.hosts.values().all(|h| *h == HostDelta::default())
    }
}

/// Buckets whose `capped_mask` changed during a flush.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushReport {
    /// `host_usage` rows the flush wrote.
    pub buckets: usize,
    /// (bucket, bits newly set).
    pub capped: Vec<(String, i16)>,
    /// (bucket, bits newly cleared) — the caller fires GO / sets
    /// `refresh_requested` for lists waiting on them.
    pub reopened: Vec<(String, i16)>,
}

/// Process-wide accumulator for committed deltas.
#[derive(Debug, Default)]
pub struct CounterSink {
    shard: i16,
    pending: Mutex<Deltas>,
}

impl CounterSink {
    /// A sink writing to `stats_counters` shard `shard` (one per process).
    pub fn new(shard: i16) -> CounterSink {
        CounterSink {
            shard,
            pending: Mutex::new(Deltas::default()),
        }
    }

    /// Merges the deltas of a committed transaction.
    pub fn add(&self, mut d: Deltas) {
        std::mem::take(&mut d.history).publish();
        if d.is_empty() {
            return;
        }
        let mut p = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        p.merge(d);
    }

    /// A copy of what is waiting to be flushed.
    pub fn pending(&self) -> Deltas {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Writes pending deltas in one transaction and recomputes each touched
    /// bucket's `capped_mask` (set at ≥ 100% of the cap, cleared below 95%).
    /// On failure the deltas are put back.
    pub async fn flush(&self, pool: &PgPool, limits: &Limits) -> Result<FlushReport> {
        let taken = {
            let mut p = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *p)
        };
        if taken.is_empty() {
            return Ok(FlushReport::default());
        }
        match flush_deltas(pool, self.shard, &taken, limits).await {
            Ok(r) => Ok(r),
            Err(e) => {
                self.add(taken);
                Err(e)
            }
        }
    }
}

/// New `capped_mask` from usage with hysteresis: a bit is set when usage
/// reaches the cap and cleared only when usage falls below 95% of it.
pub fn next_mask(bucket: &str, usage: &HostDelta, old_mask: i16, limits: &Limits) -> i16 {
    let mut mask = old_mask;
    let mut usage = *usage;
    for kind in CapKind::ALL {
        let cap = limits.bucket_cap(bucket, kind);
        let used = *usage.get_mut(kind);
        let bit = kind.bit();
        if used >= cap {
            mask |= bit;
        } else if (used as i128) * 100 < (cap as i128) * 95 {
            mask &= !bit;
        }
    }
    mask
}

async fn flush_deltas(
    pool: &PgPool,
    shard: i16,
    d: &Deltas,
    limits: &Limits,
) -> Result<FlushReport> {
    let mut tx = pool.begin().await?;
    let mut names: Vec<&&'static str> = d.stats.keys().collect();
    names.sort();
    for name in names {
        let v = d.stats[*name];
        if v == 0 {
            continue;
        }
        sqlx::query(
            "INSERT INTO stats_counters (name, shard, value) VALUES ($1, $2, $3)
             ON CONFLICT (name, shard) DO UPDATE
               SET value = stats_counters.value + EXCLUDED.value, updated_at = now()",
        )
        .bind(*name)
        .bind(shard)
        .bind(v)
        .execute(&mut *tx)
        .await?;
    }
    let mut report = FlushReport::default();
    // Sorted so concurrent flushers (two processes) lock rows in one order.
    let mut buckets: Vec<&String> = d.hosts.keys().collect();
    buckets.sort();
    for bucket in buckets {
        let h = d.hosts[bucket];
        let row: (i16, i64, i64, i64, i64, i64) = sqlx::query_as(
            "INSERT INTO host_usage (bucket, stored_blocks, stored_items, stored_listblocks,
                                     stored_lists, stored_interned)
             VALUES ($1, GREATEST($2, 0), GREATEST($3, 0), GREATEST($4, 0), GREATEST($5, 0),
                     GREATEST($6, 0))
             ON CONFLICT (bucket) DO UPDATE SET
               stored_blocks = GREATEST(host_usage.stored_blocks + $2, 0),
               stored_items = GREATEST(host_usage.stored_items + $3, 0),
               stored_listblocks = GREATEST(host_usage.stored_listblocks + $4, 0),
               stored_lists = GREATEST(host_usage.stored_lists + $5, 0),
               stored_interned = GREATEST(host_usage.stored_interned + $6, 0)
             RETURNING capped_mask, stored_blocks, stored_items, stored_listblocks,
                       stored_lists, stored_interned",
        )
        .bind(bucket.as_str())
        .bind(h.blocks)
        .bind(h.items)
        .bind(h.listblocks)
        .bind(h.lists)
        .bind(h.interned)
        .fetch_one(&mut *tx)
        .await?;
        let usage = HostDelta {
            blocks: row.1,
            items: row.2,
            listblocks: row.3,
            lists: row.4,
            interned: row.5,
        };
        let old = row.0;
        let new = next_mask(bucket, &usage, old, limits);
        if new != old {
            sqlx::query("UPDATE host_usage SET capped_mask = $2 WHERE bucket = $1")
                .bind(bucket.as_str())
                .bind(new)
                .execute(&mut *tx)
                .await?;
            if new & !old != 0 {
                report.capped.push((bucket.clone(), new & !old));
            }
            if old & !new != 0 {
                report.reopened.push((bucket.clone(), old & !new));
            }
        }
        report.buckets += 1;
    }
    tx.commit().await?;
    Ok(report)
}

/// A change to where an account is hosted, as the resolver learns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostChange {
    /// The account resolved to this `pds_hosts` row; its admissions are
    /// charged to this key from now on.
    Resolved {
        /// `pds_hosts.id`.
        host: crate::ids::HostId,
        /// The new `actors.admission_key`.
        admission_key: String,
    },
    /// Resolution failed (not authoritatively): one more
    /// `resolve_failures`.
    Failed,
}

/// The deltas that move an account's stored rows from the buckets it
/// was counted in to those it is counted in now: `counts` are its exact
/// `authored_blocks`, `owned_items`, `authored_listblocks` and
/// `authored_lists`. Nothing when the buckets are the same. The lifetime
/// intern charge stays where it was made.
pub fn moved(old: &[String], new: &[String], counts: [i64; 4]) -> Deltas {
    let mut d = Deltas::default();
    if old == new {
        return d;
    }
    let kinds = [
        CapKind::Blocks,
        CapKind::Items,
        CapKind::Listblocks,
        CapKind::Lists,
    ];
    for (kind, n) in kinds.into_iter().zip(counts) {
        if n != 0 {
            d.host(old, kind, -n);
            d.host(new, kind, n);
        }
    }
    d
}

/// Records `change` on the `actors` row of `did` and moves the account's
/// usage between cap buckets when the change gives it other buckets (the
/// row's own counters say how much). Without it the rows an unresolved
/// author stored would stay counted under `unresolved` after its host is
/// known, while their deletes were taken from the host's bucket. Runs
/// under author(D), so the counters it reads are those of committed
/// batches. An account without a row is left alone.
pub async fn record_host_change(
    pool: &PgPool,
    counters: &CounterSink,
    did: &farsight_core::Did,
    change: &HostChange,
) -> Result<()> {
    use crate::keys::{self, HostFacts};
    type Row = (
        crate::ids::ActorId,
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
    const READ: &str =
        "SELECT a.id, a.admission_key, a.resolve_failures, a.pds_host_id IS NOT NULL,
                h.cap_key, h.ip_bucket, COALESCE(h.large, false),
                a.authored_blocks, a.owned_items, a.authored_listblocks, a.authored_lists
         FROM actors a LEFT JOIN pds_hosts h ON h.id = a.pds_host_id WHERE a.did = $1";
    let facts = |r: &Row| HostFacts {
        admission_key: r.1.clone(),
        resolve_failures: r.2,
        resolved: r.3,
        cap_key: r.4.clone(),
        ip_bucket: r.5.clone(),
        large: r.6,
    };
    let mut tx = pool.begin().await?;
    crate::txn::lock_ascending(
        &mut tx,
        &[keys::author_lock_key(did.as_str())].into_iter().collect(),
    )
    .await?;
    let Some(before) = sqlx::query_as::<_, Row>(READ)
        .bind(did.as_str())
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(());
    };
    match change {
        HostChange::Resolved {
            host,
            admission_key,
        } => {
            sqlx::query(
                "UPDATE actors SET pds_host_id = $2, pds_resolved_at = now(),
                   resolve_failures = 0, admission_key = $3
                 WHERE id = $1",
            )
            .bind(before.0)
            .bind(*host)
            .bind(admission_key)
            .execute(&mut *tx)
            .await?;
        }
        HostChange::Failed => {
            sqlx::query("UPDATE actors SET resolve_failures = resolve_failures + 1 WHERE id = $1")
                .bind(before.0)
                .execute(&mut *tx)
                .await?;
        }
    }
    let after: Row = sqlx::query_as(READ)
        .bind(did.as_str())
        .fetch_one(&mut *tx)
        .await?;
    let deltas = moved(
        &keys::buckets(did, &facts(&before)),
        &keys::buckets(did, &facts(&after)),
        [before.7, before.8, before.9, before.10].map(i64::from),
    );
    tx.commit().await?;
    counters.add(deltas);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_and_empty() {
        let mut a = Deltas::default();
        assert!(a.is_empty());
        a.stat(stat::BLOCKS, 2);
        a.host(&["d:x".to_owned(), "ip:y".to_owned()], CapKind::Blocks, 1);
        let mut b = Deltas::default();
        b.stat(stat::BLOCKS, -2);
        b.host(&["d:x".to_owned()], CapKind::Blocks, -1);
        a.merge(b);
        assert_eq!(a.stats[stat::BLOCKS], 0);
        assert_eq!(a.hosts["d:x"].blocks, 0);
        assert_eq!(a.hosts["ip:y"].blocks, 1);
        assert!(!a.is_empty());
    }

    #[test]
    fn a_host_change_moves_usage_between_buckets() {
        let b = |names: &[&str]| names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
        // Unresolved, then resolved on a small host: the stored rows
        // leave `unresolved` and are counted under the host's buckets.
        let d = moved(
            &b(&["unresolved"]),
            &b(&["d:example.com", "ip:203.0.113.0/24"]),
            [7, 0, 3, 1],
        );
        assert_eq!(
            d.hosts["unresolved"],
            HostDelta {
                blocks: -7,
                listblocks: -3,
                lists: -1,
                ..HostDelta::default()
            }
        );
        for bucket in ["d:example.com", "ip:203.0.113.0/24"] {
            assert_eq!(
                d.hosts[bucket],
                HostDelta {
                    blocks: 7,
                    listblocks: 3,
                    lists: 1,
                    ..HostDelta::default()
                }
            );
        }
        // The sum over all buckets of a kind changes only by the number
        // of buckets the rows are counted in.
        let total: i64 = d.hosts.values().map(|h| h.blocks).sum();
        assert_eq!(total, 7);
        // A large host has no buckets: the rows are only taken out.
        let d = moved(&b(&["unresolved"]), &[], [2, 5, 0, 0]);
        assert_eq!(d.hosts.len(), 1);
        assert_eq!(
            (d.hosts["unresolved"].blocks, d.hosts["unresolved"].items),
            (-2, -5)
        );
        // The same buckets, or nothing stored: nothing moves.
        assert!(moved(&b(&["d:x"]), &b(&["d:x"]), [9, 9, 9, 9]).is_empty());
        assert!(moved(&b(&["unresolved"]), &b(&["d:x"]), [0; 4]).is_empty());
    }

    #[test]
    fn mask_hysteresis() {
        let limits = Limits::defaults();
        let cap = limits.bucket_cap("d:x", CapKind::Blocks);
        let at = |n: i64| HostDelta {
            blocks: n,
            ..HostDelta::default()
        };
        assert_eq!(next_mask("d:x", &at(cap - 1), 0, &limits), 0);
        assert_eq!(next_mask("d:x", &at(cap), 0, &limits), 1);
        // Between 95% and 100%: keeps whatever it was.
        assert_eq!(next_mask("d:x", &at(cap * 96 / 100), 1, &limits), 1);
        assert_eq!(next_mask("d:x", &at(cap * 96 / 100), 0, &limits), 0);
        assert_eq!(next_mask("d:x", &at(cap * 94 / 100), 1, &limits), 0);
    }
}
