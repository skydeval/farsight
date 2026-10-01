//! Subject discovery (design §5.6): with `backfill.backlinks.url` set, find
//! who blocks X through a backlink index — (a) blocks with `.subject = X`,
//! (b) listitems with `.subject = X` in their list owner's repo, (c)
//! listblocks on every list from (b) — verify every reference with
//! `getRecord` at its author's PDS and apply it with `W = 0`, charged to
//! the requester. Only an untruncated completion confirms subject coverage.

use chrono::{DateTime, Utc};
use farsight_core::record::parse_record;
use farsight_core::{AtUri, Collection, Did, Record, RecordKey};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Write, WriteAction};
use farsight_storage::txn::{Cause, Txn};

use crate::ctx::Ctx;
use crate::jobs::{self, JobResult, Outcome};
use crate::xrpc::{self, Backlink};

/// Verified references applied per transaction.
const APPLY_BATCH: usize = 100;

struct Run<'a> {
    ctx: &'a Ctx,
    requester: String,
    base: String,
    max_refs: u64,
    refs: u64,
    truncated: bool,
    cost: u64,
    pending: Vec<Write>,
}

impl Run<'_> {
    /// Pages through one backlink query, up to the reference cap.
    async fn links(&mut self, target: &str, collection: &str) -> Result<Vec<Backlink>, String> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            if self.refs >= self.max_refs {
                self.truncated = true;
                return Ok(out);
            }
            let (page, next) = xrpc::backlinks(
                &self.ctx.net,
                &self.base,
                target,
                collection,
                ".subject",
                cursor.as_deref(),
            )
            .await
            .map_err(|e| e.to_string())?;
            self.cost += 1;
            for l in page {
                if self.refs >= self.max_refs {
                    self.truncated = true;
                    return Ok(out);
                }
                self.refs += 1;
                out.push(l);
            }
            match next {
                Some(n) if Some(&n) != cursor.as_ref() => cursor = Some(n),
                _ => return Ok(out),
            }
        }
    }

    /// Fetches and parses one referenced record at its author's PDS.
    async fn verify(&mut self, l: &Backlink, k: Collection) -> Option<(Did, RecordKey, Record)> {
        let did = Did::parse(&l.did).ok()?;
        let rkey = RecordKey::parse(&l.rkey).ok()?;
        let pds = self.ctx.resolver.resolve(&did, false).await.ok()?;
        self.cost += 1;
        let value = xrpc::get_record(
            &self.ctx.net,
            &pds.endpoint,
            did.as_str(),
            k.nsid(),
            rkey.as_str(),
        )
        .await
        .ok()??;
        let rec = parse_record(&did, k, &value).ok()?;
        Some((did, rkey, rec))
    }

    async fn push(
        &mut self,
        did: Did,
        k: Collection,
        rkey: RecordKey,
        rec: Record,
    ) -> Result<(), String> {
        self.pending.push(Write {
            author: did,
            collection: k,
            rkey,
            stamp: 0,
            witness: None,
            action: WriteAction::Upsert(rec),
        });
        if self.pending.len() >= APPLY_BATCH {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), String> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let limits = self.ctx.limits();
        // Admin-requested work continues under the budget (§11.2).
        let mut gates = self.ctx.gates.load();
        if self.requester == "admin" {
            gates.budget_refusing = false;
        }
        let actx = ApplyCtx {
            limits: &limits,
            gates,
            counters: &self.ctx.counters,
        };
        let mut b = Batch::new(Origin::Discovery {
            requester: self.requester.clone(),
        });
        b.writes = std::mem::take(&mut self.pending);
        let report = apply::apply(&self.ctx.pool, &actx, &b)
            .await
            .map_err(|e| e.to_string())?;
        crate::metrics::count_refusals(&report);
        Ok(())
    }
}

/// Runs discovery for subject `x` on behalf of `requester`.
pub async fn run(ctx: &Ctx, x: &Did, requester: &str) -> JobResult {
    let mut cost = 0;
    let outcome = match run_inner(ctx, x, requester, &mut cost).await {
        Ok(o) => o,
        Err(e) => {
            let _ = sqlx::query(
                "UPDATE discovery_state SET state = 4, last_error = $2
                 WHERE actor_id = (SELECT id FROM actors WHERE did = $1)",
            )
            .bind(x.as_str())
            .bind(&e)
            .execute(&ctx.pool)
            .await;
            Outcome::Failed {
                error: e,
                terminal: false,
            }
        }
    };
    JobResult { outcome, cost }
}

async fn run_inner(ctx: &Ctx, x: &Did, requester: &str, cost: &mut u64) -> Result<Outcome, String> {
    let cfg = ctx.cfg();
    let base = cfg.backfill.backlinks.url.trim_end_matches('/').to_owned();
    if base.is_empty() {
        return Ok(Outcome::Clean);
    }
    let pool = &ctx.pool;
    let x_id = jobs::intern(ctx, x).await.map_err(|e| e.to_string())?;
    // The coverage point: clock(started_at) − lag_allowance (§3.7.1).
    let started: DateTime<Utc> = jobs::db_now(pool).await.map_err(|e| e.to_string())?;
    let point = farsight_storage::firehose::clock(pool, started)
        .await
        .map_err(|e| e.to_string())?
        .map(|p| {
            p - chrono::Duration::from_std(cfg.backfill.backlinks.lag_allowance.get())
                .unwrap_or_default()
        });
    sqlx::query(
        "INSERT INTO discovery_state (actor_id, state, source, started_at, discovered_witness, truncated, refs_found)
         VALUES ($1, 2, $2, $3, $4, false, 0)
         ON CONFLICT (actor_id) DO UPDATE SET state = 2, source = $2, started_at = $3,
           discovered_witness = $4, completed_at = NULL, truncated = false, refs_found = 0,
           last_error = NULL",
    )
    .bind(x_id)
    .bind(&base)
    .bind(started)
    .bind(point)
    .execute(pool)
    .await
    .map_err(|e| e.to_string())?;
    let mut run = Run {
        ctx,
        requester: requester.to_owned(),
        base,
        max_refs: cfg.backfill.backlinks.max_refs,
        refs: 0,
        truncated: false,
        cost: 0,
        pending: Vec::new(),
    };
    // (a) Direct blocks of X.
    for l in run.links(x.as_str(), Collection::Block.nsid()).await? {
        if let Some((did, rkey, rec)) = run.verify(&l, Collection::Block).await {
            if matches!(&rec, Record::Block(b) if b.subject == *x) {
                run.push(did, Collection::Block, rkey, rec).await?;
            }
        }
    }
    // (b) Listitems naming X, authored in the list's own repo.
    let mut lists: Vec<AtUri> = Vec::new();
    for l in run.links(x.as_str(), Collection::ListItem.nsid()).await? {
        if let Some((did, rkey, rec)) = run.verify(&l, Collection::ListItem).await {
            if let Record::ListItem(i) = &rec {
                if i.subject == *x && i.list.authority == did {
                    if !lists.contains(&i.list) {
                        lists.push(i.list.clone());
                    }
                    run.push(did, Collection::ListItem, rkey, rec).await?;
                }
            }
        }
    }
    run.flush().await?;
    // Every list found naming X, whatever its state (§5.6); a list without
    // a row is interned as a placeholder charged to the requester.
    for list in &lists {
        record_subject_list(ctx, x_id, list, requester).await?;
    }
    // (c) Listblocks on those lists (verified listblocks admit lists the
    // normal way).
    for list in &lists {
        for l in run
            .links(&list.to_string(), Collection::ListBlock.nsid())
            .await?
        {
            if let Some((did, rkey, rec)) = run.verify(&l, Collection::ListBlock).await {
                if matches!(&rec, Record::ListBlock(b) if b.subject == *list) {
                    run.push(did, Collection::ListBlock, rkey, rec).await?;
                }
            }
        }
    }
    run.flush().await?;
    *cost = run.cost;
    sqlx::query(
        "UPDATE discovery_state SET state = 3, completed_at = now(), truncated = $2, refs_found = $3
         WHERE actor_id = $1",
    )
    .bind(x_id)
    .bind(run.truncated)
    .bind(i32::try_from(run.refs).unwrap_or(i32::MAX))
    .execute(pool)
    .await
    .map_err(|e| e.to_string())?;
    if !run.truncated {
        sqlx::query(
            "INSERT INTO subject_coverage (actor_id, scope, confirmed_at, refs_found)
             VALUES ($1, 1, now(), $2), ($1, 2, now(), $2)
             ON CONFLICT (actor_id, scope) DO UPDATE SET confirmed_at = now(),
               refs_found = EXCLUDED.refs_found",
        )
        .bind(x_id)
        .bind(i32::try_from(run.refs).unwrap_or(i32::MAX))
        .execute(pool)
        .await
        .map_err(|e| e.to_string())?;
    }
    let _ = sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(pool)
        .await;
    Ok(if run.truncated {
        Outcome::CompleteWithDebts
    } else {
        Outcome::Clean
    })
}

async fn record_subject_list(
    ctx: &Ctx,
    x_id: i64,
    list: &AtUri,
    requester: &str,
) -> Result<(), String> {
    let limits = ctx.limits();
    let mut tx = ctx.pool.begin().await.map_err(|e| e.to_string())?;
    let deltas = {
        let mut t = Txn::start(&mut tx, &limits, ctx.gates.load())
            .await
            .map_err(|e| e.to_string())?;
        let key =
            farsight_storage::keys::list_lock_key(list.authority.as_str(), list.rkey.as_str());
        t.lock_lists(&[(key, true)].into_iter().collect())
            .await
            .map_err(|e| e.to_string())?;
        t.lock_new_dids(&[list.authority.as_str()])
            .await
            .map_err(|e| e.to_string())?;
        let cause = Cause {
            key: requester.to_owned(),
            buckets: Vec::new(),
            large: true,
            mask: 0,
        };
        if let Ok(list_id) = t
            .intern_list(&list.authority, list.rkey.as_str(), &cause)
            .await
            .map_err(|e| e.to_string())?
        {
            sqlx::query("INSERT INTO subject_lists (actor_id, list_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind(x_id)
                .bind(list_id)
                .execute(&mut *t.conn)
                .await
                .map_err(|e| e.to_string())?;
        }
        let (_, d) = t.finish();
        d
    };
    tx.commit().await.map_err(|e| e.to_string())?;
    ctx.counters.add(deltas);
    Ok(())
}
