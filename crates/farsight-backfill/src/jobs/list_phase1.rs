//! List job phase 1 — record check and gate (see
//! `docs/design/backfill.md`), once per admission epoch, plus the
//! `missing` re-checks.

use chrono::{DateTime, Utc};
use farsight_core::record::parse_record;
use farsight_core::{Collection, Did, RecordKey};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Write, WriteAction};
use farsight_storage::codes::{DeferCause, TrackState};
use farsight_storage::keys::CapKind;
use farsight_storage::transition::Event;

use crate::ctx::Ctx;
use crate::jobs::{self, JobResult, Outcome};
use crate::resolve::ResolveError;
use crate::xrpc;

/// Weekly retry after the short schedules are exhausted.
pub const WEEKLY: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 3600);
/// Requester of list jobs.
pub const SYSTEM_LISTS: &str = "system:lists";

/// A waiting list as phase 1 sees it.
#[derive(Debug, Clone)]
pub struct ListRow {
    /// `lists.id`.
    pub id: i64,
    /// Owner `actors.id`.
    pub owner_id: i64,
    /// Owner DID.
    pub owner: Did,
    /// List rkey.
    pub rkey: String,
    /// `record_state` code.
    pub record_state: i16,
    /// Tracking state.
    pub state: TrackState,
    /// `admit_epoch`.
    pub admit_epoch: i32,
    /// `phase1_attempts`.
    pub attempts: i32,
}

/// Loads a list row.
pub async fn load(ctx: &Ctx, list_id: i64) -> Result<Option<ListRow>, sqlx::Error> {
    let r: Option<(i64, String, String, i16, i16, i32, i32)> = sqlx::query_as(
        "SELECT l.owner_id, o.did, l.rkey, l.record_state, l.track_state, l.admit_epoch,
                l.phase1_attempts
         FROM lists l JOIN actors o ON o.id = l.owner_id WHERE l.id = $1",
    )
    .bind(list_id)
    .fetch_optional(&ctx.pool)
    .await?;
    Ok(
        r.and_then(|(owner_id, did, rkey, rs, ts, epoch, attempts)| {
            Some(ListRow {
                id: list_id,
                owner_id,
                owner: Did::parse(&did).ok()?,
                rkey,
                record_state: rs,
                state: TrackState::from_code(ts)?,
                admit_epoch: epoch,
                attempts,
            })
        }),
    )
}

async fn fire(ctx: &Ctx, list_id: i64, event: Event) -> Result<(), farsight_storage::StorageError> {
    farsight_storage::janitor::fire_event(
        &ctx.pool,
        &ctx.limits(),
        &ctx.counters,
        list_id,
        event,
        Default::default(),
    )
    .await
    .map(|_| ())
}

async fn set_job_not_before(
    ctx: &Ctx,
    list_id: i64,
    delay: std::time::Duration,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE list_jobs SET not_before = now() + make_interval(secs => $2) WHERE list_id = $1",
    )
    .bind(list_id)
    .bind(delay.as_secs_f64())
    .execute(&ctx.pool)
    .await?;
    Ok(())
}

async fn set_retry_at(
    ctx: &Ctx,
    list_id: i64,
    delay: std::time::Duration,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE lists SET next_retry_at = now() + make_interval(secs => $2) WHERE id = $1")
        .bind(list_id)
        .bind(delay.as_secs_f64())
        .execute(&ctx.pool)
        .await?;
    Ok(())
}

/// What the record check found.
enum Check {
    Present,
    NotFound,
    OwnerInactive,
    Refused,
    Error(String),
}

/// Runs phase 1 for one list (the scheduler picked it from its lanes).
pub async fn run(ctx: &Ctx, list_id: i64) -> JobResult {
    let mut cost = 0u64;
    let outcome = match run_inner(ctx, list_id, &mut cost).await {
        Ok(o) => o,
        Err(e) => Outcome::Failed {
            error: e,
            terminal: false,
        },
    };
    JobResult { outcome, cost }
}

async fn run_inner(ctx: &Ctx, list_id: i64, cost: &mut u64) -> Result<Outcome, String> {
    let err = |e: &dyn std::fmt::Display| e.to_string();
    let Some(l) = load(ctx, list_id).await.map_err(|e| err(&e))? else {
        return Ok(Outcome::Clean);
    };
    if !l.state.is_waiting() {
        sqlx::query("DELETE FROM list_jobs WHERE list_id = $1")
            .bind(list_id)
            .execute(&ctx.pool)
            .await
            .map_err(|e| err(&e))?;
        return Ok(Outcome::Clean);
    }
    let epoch: i32 = sqlx::query_scalar("SELECT admit_epoch FROM list_jobs WHERE list_id = $1")
        .bind(list_id)
        .fetch_optional(&ctx.pool)
        .await
        .map_err(|e| err(&e))?
        .unwrap_or(l.admit_epoch);
    if epoch != l.admit_epoch {
        // A newer admission replaced this job (results apply only if
        // the epoch is still current).
        sqlx::query("UPDATE list_jobs SET admit_epoch = $2 WHERE list_id = $1")
            .bind(list_id)
            .bind(l.admit_epoch)
            .execute(&ctx.pool)
            .await
            .map_err(|e| err(&e))?;
    }
    // The owner's lease, only for the getRecord call.
    if !jobs::acquire_lease(&ctx.pool, l.owner.as_str(), &ctx.lease_owner)
        .await
        .map_err(|e| err(&e))?
    {
        return Ok(Outcome::Busy);
    }
    let check = record_check(ctx, &l, cost).await;
    jobs::release_lease(&ctx.pool, l.owner.as_str(), &ctx.lease_owner).await;
    let cfg = ctx.cfg();
    match check {
        Check::Present => {
            // Re-read: the apply may have fired RP (e.g. missing → admit, a
            // new epoch with its own phase-1 job).
            let Some(now) = load(ctx, list_id).await.map_err(|e| err(&e))? else {
                return Ok(Outcome::Clean);
            };
            if now.admit_epoch != l.admit_epoch
                || !matches!(now.state, TrackState::Pending | TrackState::Unavailable)
            {
                return Ok(Outcome::Clean);
            }
            // Host gate: the owner's bucket over host_list_items.
            let owner_mask: i16 = sqlx::query_scalar(
                "SELECT COALESCE(bit_or(u.capped_mask), 0)::SMALLINT FROM host_usage u
                 WHERE u.bucket = ANY(
                   SELECT 'd:' || h.cap_key FROM actors a JOIN pds_hosts h ON h.id = a.pds_host_id
                   WHERE a.id = $1 AND NOT h.large
                   UNION SELECT 'ip:' || h.ip_bucket FROM actors a JOIN pds_hosts h ON h.id = a.pds_host_id
                   WHERE a.id = $1 AND NOT h.large AND h.ip_bucket IS NOT NULL)",
            )
            .bind(l.owner_id)
            .fetch_one(&ctx.pool)
            .await
            .map_err(|e| err(&e))?;
            if owner_mask & CapKind::Items.bit() != 0 {
                fire(ctx, list_id, Event::GateFail(DeferCause::HostCap))
                    .await
                    .map_err(|e| err(&e))?;
                return Ok(Outcome::CompleteWithDebts);
            }
            pass(ctx, &now).await.map_err(|e| err(&e))?;
            Ok(Outcome::Clean)
        }
        Check::Refused => {
            fire(ctx, list_id, Event::GateFail(DeferCause::ListsCap))
                .await
                .map_err(|e| err(&e))?;
            Ok(Outcome::CompleteWithDebts)
        }
        Check::OwnerInactive => {
            fire(ctx, list_id, Event::OwnerInactive)
                .await
                .map_err(|e| err(&e))?;
            if l.state != TrackState::Missing {
                // Only OA (a new epoch) revives a list that went
                // unavailable via OI at phase 1.
                sqlx::query("DELETE FROM list_jobs WHERE list_id = $1")
                    .bind(list_id)
                    .execute(&ctx.pool)
                    .await
                    .map_err(|e| err(&e))?;
            } else {
                // A missing list's retry continues; OI does not count.
                let i = (l.attempts.max(0) as usize).min(cfg.backfill.missing_retry.len() - 1);
                set_retry_at(ctx, list_id, cfg.backfill.missing_retry[i].get())
                    .await
                    .map_err(|e| err(&e))?;
            }
            Ok(Outcome::Inactive)
        }
        Check::NotFound => {
            let retries = &cfg.backfill.missing_retry;
            if l.state == TrackState::Missing {
                let n = l.attempts + 1;
                if n as usize >= retries.len() {
                    fire(ctx, list_id, Event::NotFoundExhausted)
                        .await
                        .map_err(|e| err(&e))?;
                } else {
                    sqlx::query("UPDATE lists SET phase1_attempts = $2 WHERE id = $1")
                        .bind(list_id)
                        .bind(n)
                        .execute(&ctx.pool)
                        .await
                        .map_err(|e| err(&e))?;
                    set_retry_at(ctx, list_id, retries[n as usize].get())
                        .await
                        .map_err(|e| err(&e))?;
                }
            } else {
                fire(ctx, list_id, Event::NotFound)
                    .await
                    .map_err(|e| err(&e))?;
                // Entering missing: the retry ladder starts.
                sqlx::query("UPDATE lists SET phase1_attempts = 0 WHERE id = $1")
                    .bind(list_id)
                    .execute(&ctx.pool)
                    .await
                    .map_err(|e| err(&e))?;
                set_retry_at(ctx, list_id, retries[0].get())
                    .await
                    .map_err(|e| err(&e))?;
            }
            Ok(Outcome::CompleteWithDebts)
        }
        Check::Error(e) => {
            if l.state == TrackState::Missing {
                // Errors are not not-found: no progress toward NFx; after
                // the last scheduled retry, weekly.
                let i = l.attempts.max(0) as usize;
                let d = cfg
                    .backfill
                    .missing_retry
                    .get(i)
                    .map(|d| d.get())
                    .unwrap_or(WEEKLY);
                set_retry_at(ctx, list_id, d).await.map_err(|x| err(&x))?;
            } else {
                let n = l.attempts + 1;
                sqlx::query("UPDATE lists SET phase1_attempts = $2 WHERE id = $1")
                    .bind(list_id)
                    .bind(n)
                    .execute(&ctx.pool)
                    .await
                    .map_err(|x| err(&x))?;
                let sched = &cfg.backfill.phase1_retry;
                if (n as usize) <= sched.len() {
                    set_job_not_before(ctx, list_id, sched[n as usize - 1].get())
                        .await
                        .map_err(|x| err(&x))?;
                } else {
                    if l.state == TrackState::Pending {
                        fire(ctx, list_id, Event::FailTerminal)
                            .await
                            .map_err(|x| err(&x))?;
                    }
                    set_job_not_before(ctx, list_id, WEEKLY)
                        .await
                        .map_err(|x| err(&x))?;
                }
            }
            Ok(Outcome::Failed {
                error: e,
                terminal: false,
            })
        }
    }
}

/// Phase 1 passed: `phase1_epoch = admit_epoch` (if still current), the
/// phase-1 row goes, and the owner's `list_fetch` is enqueued.
pub async fn pass(ctx: &Ctx, l: &ListRow) -> Result<(), farsight_storage::StorageError> {
    let mut tx = ctx.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(farsight_storage::keys::list_lock_key(
            l.owner.as_str(),
            &l.rkey,
        ))
        .execute(&mut *tx)
        .await?;
    let ok = sqlx::query(
        "UPDATE lists SET phase1_epoch = admit_epoch, phase1_attempts = 0
         WHERE id = $1 AND admit_epoch = $2",
    )
    .bind(l.id)
    .bind(l.admit_epoch)
    .execute(&mut *tx)
    .await?
    .rows_affected()
        > 0;
    if ok {
        sqlx::query("DELETE FROM list_jobs WHERE list_id = $1 AND admit_epoch <= $2")
            .bind(l.id)
            .bind(l.admit_epoch)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    if ok {
        let cap = ctx.limits().system_queue_cap;
        let mut conn = ctx.pool.acquire().await?;
        farsight_storage::queue::enqueue(
            &mut conn,
            l.owner_id,
            farsight_storage::queue::JobKind::ListFetch,
            1,
            farsight_storage::repo_events::priority::NORMAL,
            SYSTEM_LISTS,
            Some(cap),
        )
        .await?;
    }
    Ok(())
}

async fn record_check(ctx: &Ctx, l: &ListRow, cost: &mut u64) -> Check {
    if l.record_state == 1 {
        return Check::Present;
    }
    let mut bypass = false;
    loop {
        let pds = match ctx.resolver.resolve(&l.owner, bypass).await {
            Ok(p) => p,
            Err(ResolveError::NotFound) => return Check::NotFound,
            Err(ResolveError::Tombstoned) => return Check::OwnerInactive,
            Err(ResolveError::Transient(e)) => return Check::Error(e),
        };
        *cost += 1;
        match xrpc::get_record(
            &ctx.net,
            &pds.endpoint,
            l.owner.as_str(),
            Collection::List.nsid(),
            &l.rkey,
        )
        .await
        {
            Ok(Some(value)) => {
                return match apply_record(ctx, l, &value).await {
                    Ok(true) => Check::Present,
                    Ok(false) => Check::Refused,
                    Err(e) => Check::Error(e),
                };
            }
            Ok(None) | Err(_) if !bypass => {
                // Not-found is authoritative only from the owner's current
                // PDS after a re-resolve.
                bypass = true;
                continue;
            }
            Ok(None) => return Check::NotFound,
            Err(e) if xrpc::is_repo_level(&e) => {
                if e.xrpc_name() == Some("RepoNotFound") {
                    // The repo may have moved or gone: the relay decides.
                    let relay = ctx.cfg().backfill.relay_url.clone();
                    *cost += 1;
                    return match xrpc::repo_status(&ctx.net, &relay, l.owner.as_str()).await {
                        Ok(s) if !s.active => {
                            let _ = crate::jobs::repo::apply_status(ctx, &l.owner, false, s.status)
                                .await;
                            Check::OwnerInactive
                        }
                        Ok(_) => Check::NotFound,
                        Err(_) => Check::Error(e.to_string()),
                    };
                }
                let relay = ctx.cfg().backfill.relay_url.clone();
                *cost += 1;
                return match xrpc::repo_status(&ctx.net, &relay, l.owner.as_str()).await {
                    Ok(s) if !s.active => {
                        let _ =
                            crate::jobs::repo::apply_status(ctx, &l.owner, false, s.status).await;
                        Check::OwnerInactive
                    }
                    _ => Check::Error(e.to_string()),
                };
            }
            Err(e) => return Check::Error(e.to_string()),
        }
    }
}

/// Applies a fetched list record with `W = 0` under author(O) + list(L)
/// exclusive (the apply path); `false` if a cap refused it.
async fn apply_record(ctx: &Ctx, l: &ListRow, value: &serde_json::Value) -> Result<bool, String> {
    let rec = parse_record(&l.owner, Collection::List, value).map_err(|e| e.to_string())?;
    let limits = ctx.limits();
    let actx = ApplyCtx {
        limits: &limits,
        gates: ctx.gates.load(),
        counters: &ctx.counters,
    };
    let mut b = Batch::new(Origin::Discovery {
        requester: SYSTEM_LISTS.into(),
    });
    b.writes.push(Write {
        author: l.owner.clone(),
        collection: Collection::List,
        rkey: RecordKey::parse(&l.rkey).map_err(|e| e.to_string())?,
        stamp: 0,
        witness: None,
        action: WriteAction::Upsert(rec),
    });
    let report = apply::apply(&ctx.pool, &actx, &b)
        .await
        .map_err(|e| e.to_string())?;
    crate::metrics::count_refusals(&report);
    Ok(report.refused == 0)
}

/// The wall-clock bound: lists `pending` longer than
/// `limits.pending_max_age` since admission (queue wait included) fire
/// **FT** (→ `unavailable`, counted). Their `list_jobs` row and lanes are
/// untouched, so they keep their place and are served in turn.
pub async fn pending_timeouts(
    ctx: &Ctx,
    now: DateTime<Utc>,
) -> Result<usize, farsight_storage::StorageError> {
    let max_age = ctx.cfg().limits.pending_max_age.get();
    let lists: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM lists WHERE track_state = 1
           AND admitted_at < $1 - make_interval(secs => $2) ORDER BY admitted_at, id",
    )
    .bind(now)
    .bind(max_age.as_secs_f64())
    .fetch_all(&ctx.pool)
    .await?;
    for id in &lists {
        fire(ctx, *id, Event::FailTerminal).await?;
    }
    Ok(lists.len())
}
