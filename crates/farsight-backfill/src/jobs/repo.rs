//! The per-repo job (see `docs/design/backfill.md`): resolve → stamp →
//! describeRepo → late stamp → whole-range reconcile of absent collections
//! (early stamp only) → list each present collection with range reconcile
//! → done.

use farsight_storage::codes::sql::{DEBT_CAPPED, JOB_REPO, RECORD_UNKNOWN, REPO_RUNNING, TRACKED};
use std::collections::{BTreeSet, HashSet};

use chrono::{DateTime, Utc};
use farsight_core::record::parse_record;
use farsight_core::{AtUri, Collection, Did, RecordKey};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Reconcile, Write, WriteAction};
use farsight_storage::codes::{ActorStatus, DebtReason, JobKind, Priority, RequesterKey, Tier};
use farsight_storage::ids::{ListId, RepoRunId, RunId, Stamp};
use farsight_storage::repo_events::RepoEvent;
use farsight_storage::transition::Event;
use farsight_storage::txn::Gates;
use sqlx::PgPool;

use crate::ctx::Ctx;
use crate::jobs::{self, Finish, JobReq, JobResult, Outcome};
use crate::net::NetError;
use crate::resolve::{Pds, ResolveError};
use crate::xrpc;

/// Pages per collection per attempt (reaching it yields).
pub const MAX_PAGES: u32 = 50_000;
/// Rows purged per divergence-purge transaction.
pub const PURGE_BATCH: i64 = 10_000;

/// The collections a `repo` job lists.
pub const REPO_COLLECTIONS: [Collection; 4] = [
    Collection::Block,
    Collection::ListBlock,
    Collection::List,
    Collection::ListItem,
];

/// Why a job step stopped.
#[derive(Debug)]
pub enum Stop {
    /// A repo-level error (re-resolve / relay status).
    RepoLevel(NetError),
    /// Any other failure.
    Failed(String),
    /// Page bound reached.
    Yield,
}

impl From<farsight_storage::StorageError> for Stop {
    fn from(e: farsight_storage::StorageError) -> Stop {
        Stop::Failed(e.to_string())
    }
}

impl From<sqlx::Error> for Stop {
    fn from(e: sqlx::Error) -> Stop {
        Stop::Failed(e.to_string())
    }
}

fn net_stop(e: NetError) -> Stop {
    if xrpc::is_repo_level(&e) {
        Stop::RepoLevel(e)
    } else {
        Stop::Failed(e.to_string())
    }
}

/// The stamp of a run: the rev its listing writes carry, and how it
/// was read.
#[derive(Debug, Clone, Copy)]
pub struct ListingStamp {
    /// `R` (decoded rev).
    pub rev: Stamp,
    /// When it was read (database clock).
    pub read_at: DateTime<Utc>,
    /// Taken late (after describeRepo).
    pub late: bool,
}

/// The run a listing's `backfill_cursors` rows belong to. The job kind
/// decides what the row's `run_id` holds: a repo job's own number, or the
/// `list_fetch_runs` row of a fetch run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorRun {
    /// A repo job's run.
    Repo(RepoRunId),
    /// A list fetch run.
    ListFetch(RunId),
}

impl CursorRun {
    /// `backfill_cursors.job_kind`.
    pub fn job_kind(self) -> JobKind {
        match self {
            CursorRun::Repo(_) => JobKind::Repo,
            CursorRun::ListFetch(_) => JobKind::ListFetch,
        }
    }

    /// `backfill_cursors.run_id`.
    pub fn id(self) -> i64 {
        match self {
            CursorRun::Repo(id) => id.get(),
            CursorRun::ListFetch(id) => id.get(),
        }
    }
}

/// What a listing produced.
#[derive(Debug, Default, Clone)]
pub struct Listing {
    /// Writes refused (caps, rates, gates, deletes-only).
    pub refused: u64,
    /// Reconcile skipped for a collection (seen-set overflow).
    pub reconcile_skipped: bool,
    /// Outbound requests the listing made (added to the job's cost).
    pub cost: u64,
    /// At least one page ran deletes-only.
    pub deletes_only: bool,
}

/// How a job is subject to the storage gates, re-evaluated before every
/// page so a job crossing the threshold mid-run goes deletes-only for
/// its remaining pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatePolicy {
    /// Admin-requested: never deletes-only, and exempt from the budget
    /// (not the ceiling).
    pub admin: bool,
    /// A job class the budget gate applies to: tier 2, tier 3, API-key
    /// tier 1, resync, `list_fetch`.
    pub gated: bool,
    /// The repo is on a large host.
    pub large: bool,
}

impl GatePolicy {
    /// The policy of `req` on a host that is `large` or not.
    pub fn new(req: &JobReq, large: bool) -> GatePolicy {
        GatePolicy {
            admin: req.admin(),
            gated: req.tier != Tier::OnDemand
                || matches!(
                    req.requester,
                    RequesterKey::Token(_) | RequesterKey::Resync | RequesterKey::Lists
                ),
            large,
        }
    }

    /// Whether a page under `g` runs deletes-only: at ≥ 100% of budget the
    /// gated classes on non-large or unresolved hosts; at the ceiling every
    /// non-admin job.
    pub fn deletes_only(&self, g: Gates) -> bool {
        if self.admin {
            return false;
        }
        if g.ceiling_refusing {
            return true;
        }
        g.budget_refusing && !self.large && self.gated
    }

    /// The gates handed to `apply` (admin jobs' writes continue under the
    /// budget).
    pub fn apply_gates(&self, mut g: Gates) -> Gates {
        if self.admin {
            g.budget_refusing = false;
        }
        g
    }
}

/// Whether a resolved PDS host is a large host (exempt from the budget
/// gate and bucket caps).
pub fn host_is_large(ctx: &Ctx, pds: &Pds) -> bool {
    let bare = pds.host.split(':').next().unwrap_or(&pds.host);
    ctx.cfg().limits.is_large_host(bare)
}

/// Lists one collection of `did` with stamp `R` through its own cursor rows
/// and applies each page with range reconcile.
#[allow(clippy::too_many_arguments)]
pub async fn list_collection(
    ctx: &Ctx,
    pds: &Pds,
    did: &Did,
    k: Collection,
    stamp: ListingStamp,
    run: CursorRun,
    policy: GatePolicy,
) -> Result<Listing, Stop> {
    let pool = &ctx.pool;
    let limits = ctx.limits();
    let seen_cap = ctx.cfg().backfill.seen_set_cap as usize;
    let mut out = Listing::default();
    // Resume from this run's own cursor row, if any (never another job's).
    let mut cursor: Option<String> = None;
    let mut prev_last: Option<String> = None;
    if let Some(id) = jobs::actor_id(pool, did.as_str()).await? {
        let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT cursor, prev_last FROM backfill_cursors
             WHERE actor_id = $1 AND collection = $2 AND job_kind = $3 AND run_id = $4",
        )
        .bind(id)
        .bind(k.code())
        .bind(run.job_kind())
        .bind(run.id())
        .fetch_optional(pool)
        .await?;
        if let Some((c, p)) = row {
            if c.is_none() && p.is_some() {
                // Finished in an earlier attempt of this run.
                return Ok(out);
            }
            cursor = c;
            prev_last = p;
        }
    }
    let mut seen: Option<HashSet<String>> = None;
    let mut pages = 0u32;
    loop {
        pages += 1;
        if pages > MAX_PAGES {
            return Err(Stop::Yield);
        }
        let (records, next) =
            xrpc::list_records(&ctx.net, &pds.endpoint, did, k.nsid(), cursor.as_deref())
                .await
                .map_err(net_stop)?;
        out.cost += 1;
        if next.is_some() && next == cursor {
            return Err(Stop::Failed("listRecords cursor did not change".into()));
        }
        let mut writes = Vec::new();
        let mut keys: Vec<RecordKey> = Vec::new();
        let mut last: Option<String> = prev_last.clone();
        let mut order_broken = false;
        let mut invalid = 0u64;
        for r in &records {
            let Ok(uri) = AtUri::parse(&r.uri) else {
                invalid += 1;
                continue;
            };
            if uri.authority != *did || uri.indexed_collection() != Some(k) {
                invalid += 1;
                continue;
            }
            let rk = uri.rkey.as_str().to_owned();
            if last.as_deref().is_some_and(|l| rk.as_str() <= l) && seen.is_none() {
                order_broken = true;
                break;
            }
            last = Some(rk.clone());
            if let Some(s) = seen.as_mut() {
                s.insert(rk.clone());
            }
            keys.push(uri.rkey.clone());
            match parse_record(did, k, &r.value) {
                Ok(rec) => writes.push(Write {
                    author: did.clone(),
                    collection: k,
                    rkey: uri.rkey.clone(),
                    stamp: stamp.rev,
                    witness: None,
                    action: WriteAction::Upsert(rec),
                }),
                Err(_) => invalid += 1,
            }
        }
        if invalid > 0 {
            let _ = sqlx::query(
                "UPDATE pds_hosts SET errors_total = errors_total + $2 WHERE host = $1",
            )
            .bind(&pds.host)
            .bind(invalid as i64)
            .execute(pool)
            .await;
        }
        if order_broken {
            // Restart this collection with an in-memory seen-set.
            tracing::warn!(did = %did, collection = %k, "rkeys not increasing; restarting with a seen-set");
            seen = Some(HashSet::new());
            cursor = None;
            prev_last = None;
            continue;
        }
        let through = if next.is_some() {
            last.clone().and_then(|l| RecordKey::parse(&l).ok())
        } else {
            None
        };
        let gates = ctx.gates.load();
        let deletes_only = policy.deletes_only(gates);
        out.deletes_only |= deletes_only;
        let actx = ApplyCtx {
            limits: &limits,
            gates: policy.apply_gates(gates),
            counters: &ctx.counters,
        };
        let mut batch = Batch::new(Origin::Listing {
            stamp_read_at: stamp.read_at,
            deletes_only,
        });
        batch.writes = writes;
        if seen.is_none() {
            batch.reconciles.push(Reconcile {
                author: did.clone(),
                collection: k,
                stamp: stamp.rev,
                after: prev_last.as_deref().and_then(|p| RecordKey::parse(p).ok()),
                through,
                keep: keys,
            });
        }
        let report = apply::apply(pool, &actx, &batch).await?;
        crate::metrics::count_refusals(&report);
        out.refused += report.refused;
        if next.is_none() {
            if let Some(s) = seen.take() {
                // The seen-set listing reconciles the whole range at the end.
                if s.len() > seen_cap {
                    out.reconcile_skipped = true;
                } else {
                    let mut b = Batch::new(Origin::Listing {
                        stamp_read_at: stamp.read_at,
                        deletes_only,
                    });
                    b.reconciles.push(Reconcile {
                        author: did.clone(),
                        collection: k,
                        stamp: stamp.rev,
                        after: None,
                        through: None,
                        keep: s.iter().filter_map(|x| RecordKey::parse(x).ok()).collect(),
                    });
                    apply::apply(pool, &actx, &b).await?;
                }
            }
        } else if seen.as_ref().is_some_and(|s| s.len() > seen_cap) {
            out.reconcile_skipped = true;
            seen = None;
        }
        // Persist (cursor, prev_last, R, stamp_read_at) for this run.
        if let Some(id) = jobs::actor_id(pool, did.as_str()).await? {
            sqlx::query(
                "INSERT INTO backfill_cursors (actor_id, collection, job_kind, run_id, stamp_rev,
                    stamp_read_at, late_stamp, cursor, prev_last)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (actor_id, collection, job_kind, run_id) DO UPDATE SET
                   cursor = EXCLUDED.cursor, prev_last = EXCLUDED.prev_last",
            )
            .bind(id)
            .bind(k.code())
            .bind(run.job_kind())
            .bind(run.id())
            .bind(stamp.rev)
            .bind(stamp.read_at)
            .bind(stamp.late)
            .bind(&next)
            .bind(
                last.clone()
                    .or_else(|| prev_last.clone())
                    .or(Some(String::new())),
            )
            .execute(pool)
            .await?;
            jobs::renew_lease(pool, did.as_str(), &ctx.lease_owner).await?;
        }
        match next {
            Some(n) => {
                cursor = Some(n);
                prev_last = last;
            }
            None => return Ok(out),
        }
    }
}

/// Fires **DV** on the DID's tracked lists, purges its authored rows and
/// adds a `resync` debt (the divergence check).
pub async fn diverged(ctx: &Ctx, did: &Did, witness: DateTime<Utc>) -> Result<(), Stop> {
    let pool = &ctx.pool;
    let limits = ctx.limits();
    let Some(id) = jobs::actor_id(pool, did.as_str()).await? else {
        return Ok(());
    };
    let lists: Vec<ListId> = sqlx::query_scalar(&format!(
        "SELECT id FROM lists WHERE owner_id = $1 AND track_state IN {TRACKED} ORDER BY id"
    ))
    .bind(id)
    .fetch_all(pool)
    .await?;
    for l in lists {
        farsight_storage::janitor::fire_event(
            pool,
            &limits,
            &ctx.counters,
            l,
            Event::Diverged,
            Default::default(),
        )
        .await?;
    }
    while !farsight_storage::janitor::purge_for_divergence_batch(
        pool,
        &limits,
        &ctx.counters,
        did,
        PURGE_BATCH,
    )
    .await?
    {}
    farsight_storage::debts::add_debt(pool, id, DebtReason::Resync, None, witness).await?;
    Ok(())
}

async fn holds_rows(pool: &PgPool, did: &Did) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        &format!("SELECT EXISTS (SELECT 1 FROM actors a WHERE a.did = $1 AND (
            a.authored_blocks > 0 OR a.authored_listblocks > 0 OR a.authored_lists > 0
            OR a.owned_items > 0
            OR EXISTS (SELECT 1 FROM lists l WHERE l.owner_id = a.id AND l.record_state <> {RECORD_UNKNOWN})
            OR EXISTS (SELECT 1 FROM list_blocks b WHERE b.author_id = a.id)))"),
    )
    .bind(did.as_str())
    .fetch_one(pool)
    .await
}

async fn owns_tracked_list(pool: &PgPool, did: &Did) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM lists l JOIN actors a ON a.id = l.owner_id
                        WHERE a.did = $1 AND l.track_state IN {TRACKED})"
    ))
    .bind(did.as_str())
    .fetch_one(pool)
    .await
}

async fn read_stamp(ctx: &Ctx, pds: &Pds, did: &Did, late: bool) -> Result<ListingStamp, Stop> {
    let rev = xrpc::latest_rev(&ctx.net, &pds.endpoint, did)
        .await
        .map_err(net_stop)?;
    let read_at = jobs::db_now(&ctx.pool).await?;
    Ok(ListingStamp { rev, read_at, late })
}

/// Applies an account status learned from the relay (status only from
/// relay / Jetstream / PLC) through the normal event path.
pub async fn apply_status(
    ctx: &Ctx,
    did: &Did,
    active: bool,
    status: Option<String>,
) -> Result<(), Stop> {
    jobs::intern(ctx, did).await?;
    let limits = ctx.limits();
    let actx = ApplyCtx {
        limits: &limits,
        gates: ctx.gates.load(),
        counters: &ctx.counters,
    };
    let mut b = Batch::new(Origin::Discovery {
        requester: RequesterKey::Sweep,
    });
    b.events.push(RepoEvent::Account {
        did: did.clone(),
        witness: Utc::now(),
        active,
        status,
    });
    let report = apply::apply(&ctx.pool, &actx, &b).await?;
    for d in &report.deleted_accounts {
        farsight_storage::janitor::purge_account(&ctx.pool, &limits, &ctx.counters, d).await?;
    }
    Ok(())
}

/// The repo-level error rule after re-resolution failed to help: the
/// relay's `getRepoStatus` decides between **inactive** and **failed**.
async fn relay_verdict(ctx: &Ctx, did: &Did, cost: &mut u64) -> Option<Outcome> {
    let relay = ctx.cfg().backfill.relay_url.clone();
    *cost += 1;
    match xrpc::repo_status(&ctx.net, &relay, did).await {
        Ok(s) if !s.active => {
            let hidden = s
                .status
                .as_deref()
                .is_some_and(|st| ActorStatus::from_upstream(Some(st)).is_hidden());
            if hidden {
                if let Err(e) = apply_status(ctx, did, false, s.status.clone()).await {
                    tracing::warn!(did = %did, error = ?e, "recording relay status failed");
                }
                Some(Outcome::Inactive)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Runs a `repo` job for `req.did`. The caller holds the lease.
pub async fn run(ctx: &Ctx, req: &JobReq) -> JobResult {
    let mut cost = 0u64;
    let job_start = match jobs::db_now(&ctx.pool).await {
        Ok(t) => t,
        Err(e) => {
            return JobResult {
                outcome: Outcome::Failed {
                    error: e.to_string(),
                    terminal: false,
                },
                cost,
            };
        }
    };
    let point = farsight_storage::firehose::clock(&ctx.pool, job_start)
        .await
        .ok()
        .flatten();
    let (outcome, stamp) = match attempt(ctx, req, point, &mut cost).await {
        Ok((o, s)) => (o, s),
        Err((Stop::Yield, _)) => {
            requeue_yielded(ctx, req).await;
            return JobResult {
                outcome: Outcome::Yielded,
                cost,
            };
        }
        Err((Stop::RepoLevel(e), s)) => {
            let o = relay_verdict(ctx, &req.did, &mut cost)
                .await
                .unwrap_or(Outcome::Failed {
                    error: e.to_string(),
                    terminal: false,
                });
            (o, s)
        }
        Err((Stop::Failed(e), s)) => (
            Outcome::Failed {
                error: e,
                terminal: false,
            },
            s,
        ),
    };
    let f = Finish {
        req,
        point,
        job_start,
        stamp,
        outcome: &outcome,
    };
    let outcome = match jobs::finish_repo(ctx, &f).await {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(did = %req.did, error = %e, "finishing a repo job failed");
            outcome
        }
    };
    JobResult { outcome, cost }
}

async fn requeue_yielded(ctx: &Ctx, req: &JobReq) {
    if let Ok(id) = jobs::intern(ctx, &req.did).await {
        if let Ok(mut conn) = ctx.pool.acquire().await {
            let _ = farsight_storage::queue::enqueue(
                &mut conn,
                id,
                JobKind::Repo,
                req.tier,
                Priority::Normal,
                req.requester,
                None,
            )
            .await;
        }
    }
}

type AttemptErr = (Stop, Option<Stamp>);

async fn attempt(
    ctx: &Ctx,
    req: &JobReq,
    point: Option<DateTime<Utc>>,
    cost: &mut u64,
) -> Result<(Outcome, Option<Stamp>), AttemptErr> {
    let pool = &ctx.pool;
    let did = &req.did;
    let e = |s: Stop| (s, None);
    // An actor already known as hidden: the relay confirms (inactive) or
    // contradicts it (the status is updated and the repo listed).
    let status: Option<ActorStatus> =
        sqlx::query_scalar("SELECT status FROM actors WHERE did = $1")
            .bind(did.as_str())
            .fetch_optional(pool)
            .await
            .map_err(|x| e(x.into()))?;
    if status.is_some_and(ActorStatus::is_hidden) {
        if let Some(o) = relay_verdict(ctx, did, cost).await {
            return Ok((o, None));
        }
        apply_status(ctx, did, true, None).await.map_err(e)?;
    }
    // Resume this DID's own run if it is fresh (< 72 h).
    let resume: Option<(RepoRunId, Stamp, DateTime<Utc>, bool)> = sqlx::query_as(&format!(
        "SELECT s.current_run_id, c.stamp_rev, c.stamp_read_at, c.late_stamp
         FROM backfill_state s JOIN actors a ON a.id = s.actor_id
         JOIN backfill_cursors c ON c.actor_id = s.actor_id AND c.job_kind = {JOB_REPO}
              AND c.run_id = s.current_run_id
         WHERE a.did = $1 AND c.stamp_read_at > now() - interval '72 hours'
         LIMIT 1"
    ))
    .bind(did.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|x| e(x.into()))?;
    let (run_id, mut stamp) = match resume {
        Some((run, rev, at, late)) => (
            run,
            Some(ListingStamp {
                rev,
                read_at: at,
                late,
            }),
        ),
        None => {
            let mut b = [0u8; 8];
            let _ = getrandom::getrandom(&mut b);
            (RepoRunId::new(i64::from_le_bytes(b) & i64::MAX), None)
        }
    };
    let resumed = stamp.is_some();
    if let Some(id) = jobs::actor_id(pool, did.as_str())
        .await
        .map_err(|x| e(x.into()))?
    {
        sqlx::query(&format!(
            "INSERT INTO backfill_state (actor_id, state, current_run_id, current_run_point)
             VALUES ($1, {REPO_RUNNING}, $2, $3)
             ON CONFLICT (actor_id) DO UPDATE SET state = {REPO_RUNNING}, current_run_id = $2,
               current_run_point = $3"
        ))
        .bind(id)
        .bind(run_id)
        .bind(point)
        .execute(pool)
        .await
        .map_err(|x| e(x.into()))?;
    }
    // 1. Resolve (re-resolve bypassing the cache on a repo-level error).
    let mut bypass = false;
    loop {
        let pds = match ctx.resolver.resolve(did, bypass).await {
            Ok(p) => p,
            Err(ResolveError::NotFound) => {
                return Err(e(Stop::Failed("DID does not resolve".into())));
            }
            Err(ResolveError::Tombstoned) => {
                apply_status(ctx, did, false, Some("deleted".into()))
                    .await
                    .map_err(e)?;
                return Ok((Outcome::Inactive, None));
            }
            Err(ResolveError::Transient(m)) => return Err(e(Stop::Failed(m))),
        };
        *cost += 1;
        match list_repo(ctx, req, &pds, &mut stamp, resumed, point, run_id, cost).await {
            Ok(o) => return Ok((o, stamp.map(|s| s.rev))),
            Err(Stop::RepoLevel(err)) if !bypass => {
                // Re-resolve bypassing the cache; if the PDS changed,
                // retry there.
                bypass = true;
                match ctx.resolver.resolve(did, true).await {
                    Ok(p2) if p2 != pds => continue,
                    _ => return Err((Stop::RepoLevel(err), stamp.map(|s| s.rev))),
                }
            }
            Err(s) => return Err((s, stamp.map(|s| s.rev))),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn list_repo(
    ctx: &Ctx,
    req: &JobReq,
    pds: &Pds,
    stamp: &mut Option<ListingStamp>,
    resumed: bool,
    point: Option<DateTime<Utc>>,
    run_id: RepoRunId,
    cost: &mut u64,
) -> Result<Outcome, Stop> {
    let pool = &ctx.pool;
    let did = &req.did;
    let policy = GatePolicy::new(req, host_is_large(ctx, pds));
    let mut deletes_only = policy.deletes_only(ctx.gates.load());
    // 2. Stamp first if D holds rows.
    if stamp.is_none() && holds_rows(pool, did).await? {
        *stamp = Some(read_stamp(ctx, pds, did, false).await?);
        *cost += 1;
    }
    // 3. describeRepo.
    let collections = xrpc::describe_repo(&ctx.net, &pds.endpoint, did)
        .await
        .map_err(net_stop)?;
    *cost += 1;
    let tracked = owns_tracked_list(pool, did).await?;
    let present: BTreeSet<Collection> = REPO_COLLECTIONS
        .iter()
        .copied()
        .filter(|k| collections.iter().any(|c| c == k.nsid()))
        .filter(|k| *k != Collection::ListItem || tracked)
        .collect();
    let absent: Vec<Collection> = REPO_COLLECTIONS
        .iter()
        .copied()
        .filter(|k| !present.contains(k))
        .collect();
    // 4. Late stamp.
    if !present.is_empty() && stamp.is_none() {
        *stamp = Some(read_stamp(ctx, pds, did, true).await?);
        *cost += 1;
    }
    // Divergence check against the previous listing stamp (fresh runs).
    if let (Some(s), false) = (*stamp, resumed) {
        let prev: Option<Stamp> = sqlx::query_scalar(
            "SELECT s.backfill_rev FROM backfill_state s JOIN actors a ON a.id = s.actor_id
             WHERE a.did = $1",
        )
        .bind(did.as_str())
        .fetch_optional(pool)
        .await?
        .flatten();
        if prev.is_some_and(|p| s.rev < p) {
            tracing::warn!(did = %did, "repo went backwards (divergence); purging and re-listing");
            let witness = point.unwrap_or(s.read_at);
            diverged(ctx, did, witness).await?;
        }
    }
    let Some(stamp) = *stamp else {
        // Nothing present and nothing held: an empty repo (one describeRepo).
        return Ok(Outcome::Clean);
    };
    // 5. Whole-range reconcile of absent collections (early stamp only).
    if !stamp.late && !absent.is_empty() {
        let limits = ctx.limits();
        let gates = ctx.gates.load();
        deletes_only |= policy.deletes_only(gates);
        let actx = ApplyCtx {
            limits: &limits,
            gates: policy.apply_gates(gates),
            counters: &ctx.counters,
        };
        let mut b = Batch::new(Origin::Listing {
            stamp_read_at: stamp.read_at,
            deletes_only: policy.deletes_only(gates),
        });
        for k in &absent {
            b.reconciles.push(Reconcile {
                author: did.clone(),
                collection: *k,
                stamp: stamp.rev,
                after: None,
                through: None,
                keep: Vec::new(),
            });
        }
        apply::apply(pool, &actx, &b).await?;
    }
    // 6. List each present collection.
    let mut refused = 0u64;
    let mut skipped = false;
    for k in &present {
        let l = list_collection(ctx, pds, did, *k, stamp, CursorRun::Repo(run_id), policy).await?;
        *cost += l.cost;
        deletes_only |= l.deletes_only;
        refused += l.refused;
        skipped |= l.reconcile_skipped;
    }
    // 7. Outcome.
    let Some(id) = jobs::actor_id(pool, did.as_str()).await? else {
        return Ok(Outcome::Clean);
    };
    if skipped {
        let witness = point.unwrap_or(stamp.read_at);
        farsight_storage::debts::add_debt(pool, id, DebtReason::Unreachable, None, witness).await?;
    }
    // A clean run of an author holding a `capped` debt re-evaluates its
    // uncounted listblocks.
    let capped: bool = sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM relist_debt WHERE actor_id = $1 AND reason = {DEBT_CAPPED})"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    if capped && refused == 0 && !skipped && !deletes_only {
        farsight_storage::recount::reevaluate_uncounted(pool, &ctx.limits(), &ctx.counters, did)
            .await?;
    }
    let uncounted: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM list_blocks WHERE author_id = $1 AND NOT counted)",
    )
    .bind(id)
    .fetch_one(pool)
    .await?;
    if refused > 0 || skipped || uncounted || deletes_only {
        return Ok(Outcome::CompleteWithDebts);
    }
    Ok(Outcome::Clean)
}
