//! Subject discovery (see `docs/design/backfill.md`): with
//! `backfill.backlinks.url` set, find who blocks X through a backlink index
//! — (a) blocks with `.subject = X`, (b) listitems with `.subject = X` in
//! their list owner's repo, (c) listblocks on every list from (b) — verify
//! every reference with `getRecord` at its author's PDS and apply it with
//! `W = 0`, charged to the requester. Only an untruncated completion
//! confirms subject coverage.

use chrono::{DateTime, Utc};
use farsight_core::record::parse_record;
use farsight_core::{AtUri, Collection, Did, Record, RecordKey};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Write, WriteAction};
use farsight_storage::codes::RequesterKey;
use farsight_storage::codes::sql::{
    DISCOVERY_DONE, DISCOVERY_FAILED, DISCOVERY_RUNNING, SCOPE_BLOCK, SCOPE_LIST_CHAIN,
};
use farsight_storage::ids::{ActorId, Stamp};
use farsight_storage::txn::{Cause, Txn};

use crate::ctx::Ctx;
use crate::jobs::{self, JobError, JobResult, Outcome};
use crate::xrpc::{self, Backlink};

/// Verified references applied per transaction.
const APPLY_BATCH: usize = 100;
/// Pages without a reference, in a row, before a backlink query is ended
/// as truncated.
const MAX_EMPTY_PAGES: u32 = 3;

struct Run<'a> {
    ctx: &'a Ctx,
    requester: RequesterKey,
    base: String,
    max_refs: u64,
    refs: u64,
    truncated: bool,
    cost: u64,
    pending: Vec<Write>,
}

impl Run<'_> {
    /// Pages through one backlink query, up to the reference cap.
    async fn links(&mut self, target: &str, collection: &str) -> Result<Vec<Backlink>, JobError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        let mut empty_pages = 0u32;
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
            .await?;
            self.cost += 1;
            // The reference cap counts references, so an index that
            // answers page after page with a new cursor and no reference
            // would be followed without end.
            if page.is_empty() {
                empty_pages += 1;
                if empty_pages >= MAX_EMPTY_PAGES {
                    self.truncated = true;
                    return Ok(out);
                }
            } else {
                empty_pages = 0;
            }
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
        let value = xrpc::get_record(&self.ctx.net, &pds.endpoint, &did, k.nsid(), rkey.as_str())
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
    ) -> Result<(), JobError> {
        self.pending.push(Write {
            author: did,
            collection: k,
            rkey,
            stamp: Stamp::ZERO,
            witness: None,
            action: WriteAction::Upsert(rec),
        });
        if self.pending.len() >= APPLY_BATCH {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), JobError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let limits = self.ctx.limits();
        // Admin-requested work continues under the budget.
        let mut gates = self.ctx.gates.load();
        if self.requester == RequesterKey::Admin {
            gates.budget_refusing = false;
        }
        let actx = ApplyCtx {
            limits: &limits,
            gates,
            counters: &self.ctx.counters,
        };
        let mut b = Batch::new(Origin::Discovery {
            requester: self.requester,
        });
        b.writes = std::mem::take(&mut self.pending);
        let report = apply::apply(&self.ctx.pool, &actx, &b).await?;
        crate::metrics::count_refusals(&report);
        Ok(())
    }
}

/// Runs discovery for subject `x` on behalf of `requester`.
pub async fn run(ctx: &Ctx, x: &Did, requester: RequesterKey) -> JobResult {
    let mut cost = 0;
    let outcome = match run_inner(ctx, x, requester, &mut cost).await {
        Ok(o) => o,
        Err(e) => {
            let e = e.to_string();
            let _ = sqlx::query(&format!(
                "UPDATE discovery_state SET state = {DISCOVERY_FAILED}, last_error = $2
                 WHERE actor_id = (SELECT id FROM actors WHERE did = $1)"
            ))
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

async fn run_inner(
    ctx: &Ctx,
    x: &Did,
    requester: RequesterKey,
    cost: &mut u64,
) -> Result<Outcome, JobError> {
    let cfg = ctx.cfg();
    let base = cfg.backfill.backlinks.url.trim_end_matches('/').to_owned();
    if base.is_empty() {
        return Ok(Outcome::Clean);
    }
    let pool = &ctx.pool;
    let x_id = jobs::intern(ctx, x).await?;
    // The coverage point: clock(started_at) − lag_allowance.
    let started: DateTime<Utc> = jobs::db_now(pool).await?;
    let point = farsight_storage::firehose::clock(pool, started)
        .await?
        .map(|p| {
            p - chrono::Duration::from_std(cfg.backfill.backlinks.lag_allowance.get())
                .unwrap_or_default()
        });
    sqlx::query(
        &format!("INSERT INTO discovery_state (actor_id, state, source, started_at, discovered_witness, truncated, refs_found)
         VALUES ($1, {DISCOVERY_RUNNING}, $2, $3, $4, false, 0)
         ON CONFLICT (actor_id) DO UPDATE SET state = {DISCOVERY_RUNNING}, source = $2, started_at = $3,
           discovered_witness = $4, completed_at = NULL, truncated = false, refs_found = 0,
           last_error = NULL"),
    )
    .bind(x_id)
    .bind(&base)
    .bind(started)
    .bind(point)
    .execute(pool)
    .await?;
    let mut run = Run {
        ctx,
        requester,
        base,
        max_refs: cfg.backfill.backlinks.max_refs,
        refs: 0,
        truncated: false,
        cost: 0,
        pending: Vec::new(),
    };
    // (a) Direct blocks of X.
    for l in run.links(x.as_str(), Collection::Block.nsid()).await? {
        if let Some((did, rkey, rec)) = run.verify(&l, Collection::Block).await
            && matches!(&rec, Record::Block(b) if b.subject == *x)
        {
            run.push(did, Collection::Block, rkey, rec).await?;
        }
    }
    // (b) Listitems naming X, authored in the list's own repo.
    let mut lists: Vec<AtUri> = Vec::new();
    for l in run.links(x.as_str(), Collection::ListItem.nsid()).await? {
        if let Some((did, rkey, rec)) = run.verify(&l, Collection::ListItem).await
            && let Record::ListItem(i) = &rec
            && i.subject == *x
            && i.list.authority == did
        {
            if !lists.contains(&i.list) {
                lists.push(i.list.clone());
            }
            run.push(did, Collection::ListItem, rkey, rec).await?;
        }
    }
    run.flush().await?;
    // Every list found naming X, whatever its state; a list without a
    // row is interned as a placeholder charged to the requester.
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
            if let Some((did, rkey, rec)) = run.verify(&l, Collection::ListBlock).await
                && matches!(&rec, Record::ListBlock(b) if b.subject == *list)
            {
                run.push(did, Collection::ListBlock, rkey, rec).await?;
            }
        }
    }
    run.flush().await?;
    *cost = run.cost;
    sqlx::query(
        &format!("UPDATE discovery_state SET state = {DISCOVERY_DONE}, completed_at = now(), truncated = $2, refs_found = $3
         WHERE actor_id = $1"),
    )
    .bind(x_id)
    .bind(run.truncated)
    .bind(i32::try_from(run.refs).unwrap_or(i32::MAX))
    .execute(pool)
    .await?;
    if !run.truncated {
        sqlx::query(&format!(
            "INSERT INTO subject_coverage (actor_id, scope, confirmed_at, refs_found)
             VALUES ($1, {SCOPE_BLOCK}, now(), $2), ($1, {SCOPE_LIST_CHAIN}, now(), $2)
             ON CONFLICT (actor_id, scope) DO UPDATE SET confirmed_at = now(),
               refs_found = EXCLUDED.refs_found"
        ))
        .bind(x_id)
        .bind(i32::try_from(run.refs).unwrap_or(i32::MAX))
        .execute(pool)
        .await?;
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
    x_id: ActorId,
    list: &AtUri,
    requester: RequesterKey,
) -> Result<(), JobError> {
    let limits = ctx.limits();
    let mut tx = ctx.pool.begin().await?;
    let deltas = {
        let mut t = Txn::start(&mut tx, &limits, ctx.gates.load()).await?;
        let key =
            farsight_storage::keys::list_lock_key(list.authority.as_str(), list.rkey.as_str());
        t.lock_lists(&[(key, true)].into_iter().collect()).await?;
        t.lock_new_dids(&[&list.authority]).await?;
        let cause = Cause {
            key: requester.to_string(),
            buckets: Vec::new(),
            large: true,
            mask: 0,
        };
        if let Ok(list_id) = t.intern_list(&list.authority, &list.rkey, &cause).await? {
            sqlx::query("INSERT INTO subject_lists (actor_id, list_id) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind(x_id)
                .bind(list_id)
                .execute(&mut *t.conn)
                .await?;
        }
        let (_, d) = t.finish();
        d
    };
    tx.commit().await?;
    ctx.counters.add(deltas);
    Ok(())
}
