//! List job phase 2 — the fetch run for one owner (see
//! `docs/design/backfill.md`): claim the owner's claimable lists, list the
//! owner's `listitem` collection through the run's own cursor, then
//! promote every list still claimed for its epoch.
//!
//! A run row is open from its start to its end, and lists are claimed by
//! it in between. Whatever ends the job, the run is closed and its claim
//! released: by the job itself on every path, an error included, and for
//! a job that was killed by the feeder, which queues the owner again so
//! that the run is taken up ([`crate::feeder`]).

use farsight_storage::codes::sql::{JOB_LIST_FETCH, TRACK_SERVED, TRACK_UNFETCHED};
use std::time::Duration;

use chrono::{DateTime, Utc};
use farsight_core::{Collection, Did};
use farsight_storage::codes::{FetchOutcome, JobKind, Priority, RequesterKey, Tier, TrackState};
use farsight_storage::ids::{ActorId, ListId, RunId, Stamp};
use farsight_storage::tracking::FireArgs;
use farsight_storage::transition::Event;
use farsight_storage::txn::MAX_LOCKS;

use crate::ctx::Ctx;
use crate::jobs::list_phase1::WEEKLY;
use crate::jobs::repo::{self, CursorRun, ListingStamp, PageBudget, Stop};
use crate::jobs::{self, JobError, JobReq, JobResult, Outcome};
use crate::resolve::ResolveError;
use crate::xrpc;

/// Retry delay of a failed run whose lists keep trying (before weekly).
pub const RUN_RETRY: Duration = Duration::from_secs(300);

async fn requeue(ctx: &Ctx, owner_id: ActorId, delay: Duration) {
    let cap = ctx.limits().system_queue_cap;
    if let Ok(mut conn) = ctx.pool.acquire().await {
        let _ = farsight_storage::queue::enqueue(
            &mut conn,
            owner_id,
            JobKind::ListFetch,
            Tier::OnDemand,
            Priority::Normal,
            RequesterKey::Lists,
            Some(cap),
            (!delay.is_zero()).then_some(delay),
        )
        .await;
    }
}

/// Runs a fetch run for `owner` (the scheduler picked its `list_fetch`
/// queue entry).
pub async fn run(ctx: &Ctx, owner_id: ActorId, owner: &Did) -> JobResult {
    let mut cost = 0u64;
    let outcome = match run_inner(ctx, owner_id, owner, &mut cost).await {
        Ok(o) => o,
        Err(e) => Outcome::Failed {
            error: e.to_string(),
            terminal: false,
        },
    };
    JobResult { outcome, cost }
}

async fn finish_run(ctx: &Ctx, run_id: RunId, outcome: FetchOutcome) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE list_fetch_runs SET finished_at = now(), outcome = $2 WHERE id = $1")
        .bind(run_id)
        .bind(outcome)
        .execute(&ctx.pool)
        .await?;
    sqlx::query(&format!(
        "DELETE FROM backfill_cursors WHERE job_kind = {JOB_LIST_FETCH} AND run_id = $1"
    ))
    .bind(run_id)
    .execute(&ctx.pool)
    .await?;
    Ok(())
}

async fn release_claim(ctx: &Ctx, run_id: RunId) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE lists SET fetch_run_id = NULL, fetch_run_epoch = NULL WHERE fetch_run_id = $1",
    )
    .bind(run_id)
    .execute(&ctx.pool)
    .await?;
    Ok(())
}

async fn claimed(ctx: &Ctx, run_id: RunId) -> Result<Vec<(ListId, TrackState)>, sqlx::Error> {
    sqlx::query_as("SELECT id, track_state FROM lists WHERE fetch_run_id = $1 ORDER BY id")
        .bind(run_id)
        .fetch_all(&ctx.pool)
        .await
}

/// The claim: under the list locks of the lists it takes, every list of
/// O that passed phase 1 in its epoch (pending/unavailable) or asked for
/// a refresh (ready/retained), up to [`MAX_LOCKS`] of them, lowest id
/// first. What is left over is claimed by the owner's next run.
async fn claim(
    ctx: &Ctx,
    owner_id: ActorId,
    owner: &Did,
    run_id: RunId,
) -> Result<Vec<ListId>, farsight_storage::StorageError> {
    let claimable = format!(
        "((track_state IN {TRACK_UNFETCHED} AND phase1_epoch = admit_epoch)
          OR (track_state IN {TRACK_SERVED} AND refresh_requested))"
    );
    let mut tx = ctx.pool.begin().await?;
    let wanted: Vec<(ListId, String)> = sqlx::query_as(&format!(
        "SELECT id, rkey FROM lists WHERE owner_id = $1 AND {claimable} ORDER BY id LIMIT $2"
    ))
    .bind(owner_id)
    .bind(MAX_LOCKS as i64)
    .fetch_all(&mut *tx)
    .await?;
    let lock_keys: std::collections::BTreeSet<i64> = wanted
        .iter()
        .map(|(_, rk)| farsight_storage::keys::list_lock_key(owner.as_str(), rk))
        .collect();
    farsight_storage::txn::lock_ascending(&mut tx, &lock_keys).await?;
    // Checked again under the locks: a list may have left the claimable
    // states since it was read.
    let ids: Vec<ListId> = wanted.iter().map(|(id, _)| *id).collect();
    let ids: Vec<ListId> = sqlx::query_scalar(&format!(
        "UPDATE lists SET fetch_run_id = $2, fetch_run_epoch = admit_epoch
         WHERE id = ANY($1) AND {claimable}
         RETURNING id"
    ))
    .bind(&ids)
    .bind(run_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(ids)
}

async fn run_inner(
    ctx: &Ctx,
    owner_id: ActorId,
    owner: &Did,
    cost: &mut u64,
) -> Result<Outcome, JobError> {
    let cfg = ctx.cfg();
    let pool = &ctx.pool;
    // Resume a crashed run (its run_id, cursor, claimed set and point).
    let resume: Option<(RunId, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT id, coverage_point FROM list_fetch_runs
         WHERE owner_id = $1 AND finished_at IS NULL ORDER BY id DESC LIMIT 1",
    )
    .bind(owner_id)
    .fetch_optional(pool)
    .await?;
    if resume.is_none() {
        // Coalescing: at most one run per owner per cooldown.
        let recent: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT max(started_at) FROM list_fetch_runs WHERE owner_id = $1")
                .bind(owner_id)
                .fetch_one(pool)
                .await?;
        let now = jobs::db_now(pool).await?;
        let cooldown = cfg.backfill.owner_fetch_cooldown.get();
        if let Some(r) = recent {
            let age = (now - r).to_std().unwrap_or_default();
            if age < cooldown {
                requeue(ctx, owner_id, cooldown - age).await;
                return Ok(Outcome::Busy);
            }
        }
    }
    let lease = ctx.lease_owner();
    if !jobs::acquire_lease(pool, owner.as_str(), &lease).await? {
        requeue(ctx, owner_id, Duration::from_secs(60)).await;
        return Ok(Outcome::Busy);
    }
    let mut run_id = resume.map(|(id, _)| id);
    let r = match fetch(ctx, owner_id, owner, resume, &mut run_id, cost).await {
        Ok(o) => Ok(o),
        Err(e) => {
            // Whatever stopped the job, its run does not stay open with
            // lists claimed: nothing else would take them up.
            if let Some(id) = run_id
                && let Err(x) = abort_run(ctx, id).await
            {
                tracing::warn!(owner = %owner, error = %x, "closing a failed list fetch run failed");
            }
            requeue(ctx, owner_id, RUN_RETRY).await;
            Err(e)
        }
    };
    jobs::release_lease(pool, owner.as_str(), &lease).await;
    r
}

/// Closes a run that an error ended: its claim is released and the run
/// finished as failed. No attempt is counted on its lists: the failure
/// was not the fetch's.
async fn abort_run(ctx: &Ctx, run_id: RunId) -> Result<(), sqlx::Error> {
    release_claim(ctx, run_id).await?;
    finish_run(ctx, run_id, FetchOutcome::Failed).await
}

/// A real failure of the fetch: counts an attempt on every claimed list
/// (claimed pending lists out of attempts fire FT → unavailable),
/// releases the claim, finishes the run as failed and queues the owner
/// again: in [`RUN_RETRY`], or in a week once every claimed list is out
/// of attempts (unavailable lists retry weekly, indefinitely, and stay
/// claimable).
async fn fail_run(ctx: &Ctx, owner_id: ActorId, run_id: RunId) -> Result<(), JobError> {
    let pool = &ctx.pool;
    let max_attempts = i32::try_from(ctx.cfg().backfill.list_fetch_max_attempts).unwrap_or(2);
    let limits = ctx.limits();
    let rows: Vec<(ListId, TrackState, i32)> = sqlx::query_as(
        "UPDATE lists SET fetch_attempts = fetch_attempts + 1 WHERE fetch_run_id = $1
         RETURNING id, track_state, fetch_attempts",
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;
    let mut all_exhausted = true;
    for (id, state, attempts) in &rows {
        if *attempts >= max_attempts {
            if *state == TrackState::Pending {
                farsight_storage::janitor::fire_event(
                    pool,
                    &limits,
                    &ctx.counters,
                    *id,
                    Event::FailTerminal,
                    FireArgs::default(),
                )
                .await?;
            }
        } else {
            all_exhausted = false;
        }
    }
    release_claim(ctx, run_id).await?;
    finish_run(ctx, run_id, FetchOutcome::Failed).await?;
    requeue(
        ctx,
        owner_id,
        if all_exhausted { WEEKLY } else { RUN_RETRY },
    )
    .await;
    Ok(())
}

/// The owner's job ended without returning (it panicked): its open run,
/// if it left one, is failed like a fetch that failed, so the lists it
/// claimed are not left claimed and the owner is tried again within the
/// attempts its lists have.
pub async fn abandon(ctx: &Ctx, owner_id: ActorId) -> Result<(), JobError> {
    let open: Vec<RunId> = sqlx::query_scalar(
        "SELECT id FROM list_fetch_runs WHERE owner_id = $1 AND finished_at IS NULL ORDER BY id",
    )
    .bind(owner_id)
    .fetch_all(&ctx.pool)
    .await?;
    for run_id in open {
        fail_run(ctx, owner_id, run_id).await?;
    }
    Ok(())
}

async fn fetch(
    ctx: &Ctx,
    owner_id: ActorId,
    owner: &Did,
    resume: Option<(RunId, Option<DateTime<Utc>>)>,
    open: &mut Option<RunId>,
    cost: &mut u64,
) -> Result<Outcome, JobError> {
    let cfg = ctx.cfg();
    let pool = &ctx.pool;
    let started = std::time::Instant::now();
    let pds = match ctx.resolver.resolve(owner, false).await {
        Ok(p) => p,
        Err(ResolveError::Tombstoned) => {
            repo::apply_status(ctx, owner, false, Some("deleted".into()))
                .await
                .map_err(JobError::Status)?;
            if let Some((run_id, _)) = resume {
                release_claim(ctx, run_id).await?;
                finish_run(ctx, run_id, FetchOutcome::OwnerInactive).await?;
            }
            return Ok(Outcome::Inactive);
        }
        Err(x) => return Err(x.into()),
    };
    *cost += 1;
    let (run_id, point, stamp) = match resume {
        Some((run_id, point)) => {
            let s: Option<(Stamp, DateTime<Utc>, bool)> = sqlx::query_as(&format!(
                "SELECT stamp_rev, stamp_read_at, late_stamp FROM backfill_cursors
                 WHERE actor_id = $1 AND job_kind = {JOB_LIST_FETCH} AND run_id = $2 LIMIT 1"
            ))
            .bind(owner_id)
            .bind(run_id)
            .fetch_optional(pool)
            .await?;
            let stamp = match s {
                Some((rev, at, late)) if (Utc::now() - at).num_hours() < 72 => ListingStamp {
                    rev,
                    read_at: at,
                    late,
                },
                _ => {
                    // Stamp expired (a real failure): fresh stamp.
                    let rev = xrpc::latest_rev(&ctx.net, &pds.endpoint, owner).await?;
                    ListingStamp {
                        rev,
                        read_at: jobs::db_now(pool).await?,
                        late: false,
                    }
                }
            };
            (run_id, point, stamp)
        }
        None => {
            // Run start: the run row and its coverage point, the stamp, then
            // the claim; every page is read after the claim commits.
            let now = jobs::db_now(pool).await?;
            let point = farsight_storage::firehose::clock(pool, now).await?;
            let run_id: RunId = sqlx::query_scalar(
                "INSERT INTO list_fetch_runs (owner_id, coverage_point) VALUES ($1, $2) RETURNING id",
            )
            .bind(owner_id)
            .bind(point)
            .fetch_one(pool)
            .await?;
            *open = Some(run_id);
            let rev = xrpc::latest_rev(&ctx.net, &pds.endpoint, owner).await?;
            *cost += 1;
            let read_at = jobs::db_now(pool).await?;
            let ids = claim(ctx, owner_id, owner, run_id).await?;
            if ids.is_empty() {
                finish_run(ctx, run_id, FetchOutcome::Ok).await?;
                return Ok(Outcome::Clean);
            }
            (
                run_id,
                point,
                ListingStamp {
                    rev,
                    read_at,
                    late: false,
                },
            )
        }
    };
    // Budget gate: runs for non-large owners go deletes-only, promote
    // nothing and release their claim.
    let req = JobReq {
        did: owner.clone(),
        tier: Tier::OnDemand,
        requester: RequesterKey::Lists,
    };
    let policy = repo::GatePolicy::new(&req, repo::host_is_large(ctx, &pds));
    let max = cfg.backfill.list_fetch_max_duration.get();
    let mut budget = PageBudget::new();
    let listing = tokio::time::timeout(
        max.saturating_sub(started.elapsed()),
        repo::list_collection(
            ctx,
            &pds,
            owner,
            Collection::ListItem,
            stamp,
            CursorRun::ListFetch(run_id),
            policy,
            &mut budget,
        ),
    )
    .await;
    let failure = match listing {
        Ok(Ok(l)) => {
            *cost += l.cost;
            if l.deletes_only {
                release_claim(ctx, run_id).await?;
                finish_run(ctx, run_id, FetchOutcome::Ok).await?;
                return Ok(Outcome::CompleteWithDebts);
            }
            // Run end: OK on every list still claimed for its epoch.
            let args = FireArgs {
                run_point: point,
                items_refused: l.refused > 0,
            };
            let limits = ctx.limits();
            for (id, _) in claimed(ctx, run_id).await? {
                farsight_storage::janitor::promote_claimed(
                    pool,
                    &limits,
                    &ctx.counters,
                    id,
                    run_id,
                    args,
                )
                .await?;
            }
            release_claim(ctx, run_id).await?;
            finish_run(ctx, run_id, FetchOutcome::Ok).await?;
            return Ok(if l.refused > 0 {
                Outcome::CompleteWithDebts
            } else {
                Outcome::Clean
            });
        }
        // More pages than one run lists: a failure like the time bound,
        // so an owner whose listing never ends runs out of attempts.
        Ok(Err(Stop::Yield)) => format!("the listing did not end within {} pages", repo::MAX_PAGES),
        Ok(Err(Stop::RepoLevel(x))) => {
            // Owner inactive (relay-confirmed) ⇒ OI on claimed pending lists.
            let relay = cfg.backfill.relay_url.clone();
            *cost += 1;
            if let Ok(s) = xrpc::repo_status(&ctx.net, &relay, owner).await
                && !s.active
            {
                repo::apply_status(ctx, owner, false, s.status)
                    .await
                    .map_err(JobError::Status)?;
                let limits = ctx.limits();
                for (id, state) in claimed(ctx, run_id).await? {
                    if state == TrackState::Pending {
                        farsight_storage::janitor::fire_event(
                            pool,
                            &limits,
                            &ctx.counters,
                            id,
                            Event::OwnerInactive,
                            FireArgs::default(),
                        )
                        .await?;
                    }
                }
                release_claim(ctx, run_id).await?;
                finish_run(ctx, run_id, FetchOutcome::OwnerInactive).await?;
                return Ok(Outcome::Inactive);
            }
            x.to_string()
        }
        Ok(Err(Stop::Failed(x))) => x,
        Ok(Err(Stop::Unconfirmed)) => "the owner's host could not be confirmed".into(),
        Err(_) => "list fetch run exceeded backfill.list_fetch_max_duration".into(),
    };
    fail_run(ctx, owner_id, run_id).await?;
    Ok(Outcome::Failed {
        error: failure,
        terminal: false,
    })
}
