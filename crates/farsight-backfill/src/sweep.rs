//! Sweep and repair cycles (see `docs/design/backfill.md` and
//! `docs/design/storage.md`): enumerate a source into `cycle_outstanding`
//! (bounded by `backfill.sweep.max_outstanding`), let the scheduler's
//! tier 3 run the members, complete the cycle when enumeration finished
//! and every remaining row is terminal.
//!
//! - Full cycles enumerate `backfill.sweep.source`; `relay_collections`
//!   falls back to `relay_repos` when the relay lacks it: the relay is
//!   probed before every full cycle, and a cycle under way switches when
//!   a page is answered with "no such method".
//! - Repair cycles cover every closed, unhealed gap at their start with
//!   `listRepos` candidates (rev time ≥ `from − repair_slack − lag`, plus
//!   reactivations); with the relay down they re-list every known DID and
//!   heal nothing (the next full cycle heals).
//! - No cycle starts before the firehose committed its first batch;
//!   `S_C = clock(max(started_at, first_applied_at))`.

use farsight_storage::codes::sql::{
    ACTOR_ACTIVE, CYCLE_FULL, CYCLE_REPAIR, MEMBER_OUTSTANDING, RUN_LISTED, TRACKED,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use farsight_core::config::SweepSource;
use farsight_core::{Collection, Tid};
use farsight_storage::codes::{CycleKind, CycleSource, DebtReason, TrackState};
use farsight_storage::ids::{CycleId, GapId, ListId};
use farsight_storage::tracking::FireArgs;
use farsight_storage::transition::Event;
use tokio::sync::watch;

use crate::ctx::Ctx;
use crate::metrics as m;
use crate::xrpc;

const TICK: Duration = Duration::from_secs(5);
/// Largest enumeration page asked for (`listReposByCollection` limit).
const PAGE: i64 = 2000;
/// Collections a sweep enumerates with `relay_collections`.
const SWEPT: [Collection; 3] = [Collection::Block, Collection::ListBlock, Collection::List];

/// An open cycle.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Cycle {
    /// `sweep_cycles.id`.
    pub id: CycleId,
    /// Full sweep or repair.
    pub kind: CycleKind,
    /// What the cycle enumerates. A repair's is `relay_repos` or
    /// `known_dids`, whatever the configured sweep source.
    pub source: CycleSource,
    /// `sweep_cycles.started_at`: server time at which the cycle's row
    /// was inserted. A job that started after it settles the DID's
    /// membership in the cycle.
    pub started_at: DateTime<Utc>,
    /// `S_C` on the witness clock. `None` only on a row another process
    /// inserted (`admin.startRepair`) that this one has not adopted yet.
    pub effective_start_witness: Option<DateTime<Utc>>,
    /// `sweep_cycles.enumerated_at`: server time at which the last page
    /// of the source was stored. `None` while enumeration goes on.
    pub enumerated_at: Option<DateTime<Utc>>,
    /// `sweep_cycles.checkpoint`: where the next page starts, in the
    /// source's own terms: `<collection index>|<cursor>` for
    /// `relay_collections`, the relay's cursor for `relay_repos`, the
    /// last operation's `createdAt` for the PLC export, the last DID for
    /// known DIDs. `None` before the first page.
    pub checkpoint: Option<String>,
    /// Repair lower bound (`min(from_at)` of covered gaps).
    pub repair_from: Option<DateTime<Utc>>,
}

/// One enumerated member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// The member's DID as the source gave it: the key of its
    /// `cycle_outstanding` row, which needs no `actors` row.
    pub did: String,
    /// Relay says active while Farsight holds it inactive (repair only).
    pub reactivated: bool,
}

/// Sweep state kept between ticks.
#[derive(Default)]
pub struct Sweep {
    /// The relay was last found without `listReposByCollection`: full
    /// cycles use `relay_repos`.
    pub fell_back: AtomicBool,
}

impl Sweep {
    /// The source label a new full cycle uses. With `relay_collections`
    /// configured the relay is asked for one repo first, every time: a
    /// relay without the method gets a `relay_repos` cycle, and one that
    /// gained it since the last cycle is used with it again. Any other
    /// failure (the relay is down, or busy) says nothing about the method
    /// and leaves the configured source; the cycle's pages retry.
    async fn full_source(&self, ctx: &Ctx) -> CycleSource {
        match ctx.cfg().backfill.sweep.source {
            source @ (SweepSource::RelayRepos | SweepSource::Plc) => source.into(),
            SweepSource::RelayCollections => {
                let relay = ctx.cfg().backfill.relay_url.clone();
                let probe = xrpc::list_repos_by_collection(
                    &ctx.net,
                    &relay,
                    Collection::List.nsid(),
                    None,
                    1,
                )
                .await;
                let missing = matches!(&probe, Err(e) if e.method_missing());
                if missing {
                    tracing::warn!(
                        "relay lacks listReposByCollection; sweep falls back to relay_repos"
                    );
                }
                self.fell_back.store(missing, Ordering::Relaxed);
                if missing {
                    CycleSource::RelayRepos
                } else {
                    CycleSource::RelayCollections
                }
            }
        }
    }
}

/// Runs cycles until `stop` flips.
pub async fn run(ctx: Arc<Ctx>, sweep: Arc<Sweep>, mut stop: watch::Receiver<bool>) {
    loop {
        if let Err(e) = tick(&ctx, &sweep).await {
            tracing::warn!(error = %e, "sweep tick failed");
        }
        tokio::select! {
            _ = tokio::time::sleep(TICK) => {}
            _ = stop.changed() => {}
        }
        if *stop.borrow() {
            return;
        }
    }
}

/// Why a step of the sweep failed. The step is tried again at the next
/// tick.
#[derive(Debug, thiserror::Error)]
pub enum SweepError {
    /// A statement failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// The storage layer refused or failed.
    #[error(transparent)]
    Storage(#[from] farsight_storage::StorageError),
    /// The relay or the directory could not be read.
    #[error(transparent)]
    Net(#[from] crate::net::NetError),
}

type Res<T> = Result<T, SweepError>;

/// One step: start cycles that are due, enumerate a page per open cycle
/// while under the outstanding bound, complete finished cycles.
pub async fn tick(ctx: &Ctx, sweep: &Sweep) -> Res<()> {
    let pool = &ctx.pool;
    let first: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT first_applied_at FROM firehose_state WHERE id = 1")
            .fetch_optional(pool)
            .await?
            .flatten();
    let Some(first_applied) = first else {
        return Ok(()); // no cycle before the first committed batch
    };
    maybe_start(ctx, sweep, first_applied).await?;
    let open: Vec<Cycle> = sqlx::query_as(
        "SELECT id, kind, source, started_at, effective_start_witness, enumerated_at, checkpoint,
                repair_from
         FROM sweep_cycles WHERE completed_at IS NULL ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let max_out = i64::try_from(ctx.cfg().backfill.sweep.max_outstanding).unwrap_or(i64::MAX);
    for c in &open {
        if c.effective_start_witness.is_none() {
            // Created outside this process (`admin.startRepair`).
            adopt(ctx, c, first_applied).await?;
            continue;
        }
        // A full cycle pauses with the sweep (operator toggle) or at 90% of
        // the budget; its members also stop being dispatched (tier 3).
        // A repair pauses with its own switch.
        let paused = if c.kind == CycleKind::Full {
            !ctx.cfg().backfill.sweep.enabled || ctx.sweep_paused_by_storage()
        } else {
            ctx.cfg().backfill.repair.paused
        };
        if c.enumerated_at.is_none() && !paused {
            let outstanding: i64 = sqlx::query_scalar(
                &format!("SELECT count(*) FROM cycle_outstanding WHERE cycle_id = $1 AND state = {MEMBER_OUTSTANDING}"),
            )
            .bind(c.id)
            .fetch_one(pool)
            .await?;
            // Each page asks for at most the room left, so outstanding
            // rows never exceed the bound.
            if outstanding < max_out {
                let room = u32::try_from((max_out - outstanding).min(PAGE)).unwrap_or(1);
                enumerate_page(ctx, sweep, c, room).await?;
            }
        }
        maybe_complete(ctx, c).await?;
    }
    publish_progress(ctx).await;
    Ok(())
}

async fn maybe_start(ctx: &Ctx, sweep: &Sweep, first_applied: DateTime<Utc>) -> Res<()> {
    let pool = &ctx.pool;
    let cfg = ctx.cfg();
    let (open_full, open_repair, last_full): (bool, bool, Option<DateTime<Utc>>) = sqlx::query_as(
        &format!("SELECT EXISTS (SELECT 1 FROM sweep_cycles WHERE kind = {CYCLE_FULL} AND completed_at IS NULL),
                EXISTS (SELECT 1 FROM sweep_cycles WHERE kind = {CYCLE_REPAIR} AND completed_at IS NULL),
                (SELECT max(started_at) FROM sweep_cycles WHERE kind = {CYCLE_FULL})"),
    )
    .fetch_one(pool)
    .await?;
    if cfg.backfill.sweep.enabled && !open_full {
        let due = match last_full {
            None => true,
            Some(t) => {
                let days = cfg.backfill.sweep.full_every_days;
                days > 0 && Utc::now() - t >= chrono::Duration::days(i64::from(days))
            }
        };
        if due && !ctx.sweep_paused_by_storage() {
            let source = sweep.full_source(ctx).await;
            start_cycle(ctx, CycleKind::Full, source, first_applied, None).await?;
        }
    }
    if !open_repair && cfg.backfill.repair.auto_start {
        // One repair covers every closed, unhealed gap not yet claimed
        // (coalesced); an open gap waits for its stream to come back.
        let from: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT min(from_at) FROM firehose_gaps
             WHERE to_at IS NOT NULL AND healed_at IS NULL AND repair_cycle_id IS NULL",
        )
        .fetch_one(pool)
        .await?;
        if let Some(from) = from {
            let relay = cfg.backfill.relay_url.clone();
            let source = match xrpc::list_repos(&ctx.net, &relay, None, 1).await {
                Ok(_) => CycleSource::RelayRepos,
                Err(err) => {
                    tracing::warn!(error = %err, "relay unavailable; repair re-lists known DIDs");
                    CycleSource::KnownDids
                }
            };
            start_cycle(ctx, CycleKind::Repair, source, first_applied, Some(from)).await?;
        }
    }
    Ok(())
}

async fn start_cycle(
    ctx: &Ctx,
    kind: CycleKind,
    source: CycleSource,
    first_applied: DateTime<Utc>,
    repair_from: Option<DateTime<Utc>>,
) -> Res<CycleId> {
    let mut tx = ctx.pool.begin().await?;
    let collections: Vec<i16> = Collection::ALL.iter().map(|c| c.code()).collect();
    let id: CycleId = sqlx::query_scalar(
        "INSERT INTO sweep_cycles (kind, source, collections, started_at, effective_start, repair_from)
         VALUES ($1, $2, $3, clock_timestamp(), GREATEST(clock_timestamp(), $4), $5) RETURNING id",
    )
    .bind(kind)
    .bind(source)
    .bind(&collections)
    .bind(first_applied)
    .bind(repair_from)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE sweep_cycles SET effective_start_witness =
           (SELECT witness_at FROM firehose_clock WHERE server_at <= sweep_cycles.effective_start
            ORDER BY server_at DESC LIMIT 1)
         WHERE id = $1",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;
    if kind == CycleKind::Repair && source != CycleSource::KnownDids {
        sqlx::query(
            "UPDATE firehose_gaps SET repair_cycle_id = $1
             WHERE to_at IS NOT NULL AND healed_at IS NULL AND repair_cycle_id IS NULL",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    tracing::info!(
        cycle = id.get(),
        kind = kind.code(),
        source = source.as_str(),
        "cycle started"
    );
    Ok(id)
}

/// Completes a cycle row another process inserted: `S_C`, the repair
/// source (listRepos, or known DIDs while the relay is down) and the gaps
/// it covers.
async fn adopt(ctx: &Ctx, c: &Cycle, first_applied: DateTime<Utc>) -> Res<()> {
    let source = if c.kind == CycleKind::Repair {
        let relay = ctx.cfg().backfill.relay_url.clone();
        match xrpc::list_repos(&ctx.net, &relay, None, 1).await {
            Ok(_) => CycleSource::RelayRepos,
            Err(err) => {
                tracing::warn!(error = %err, "relay unavailable; repair re-lists known DIDs");
                CycleSource::KnownDids
            }
        }
    } else {
        c.source
    };
    let mut tx = ctx.pool.begin().await?;
    sqlx::query(
        "UPDATE sweep_cycles SET source = $2,
           effective_start = GREATEST(started_at, $3),
           effective_start_witness = (SELECT witness_at FROM firehose_clock
             WHERE server_at <= GREATEST(sweep_cycles.started_at, $3) ORDER BY server_at DESC LIMIT 1)
         WHERE id = $1",
    )
    .bind(c.id)
.bind(source)
    .bind(first_applied)
    .execute(&mut *tx)
    .await?;
    if c.kind == CycleKind::Repair && source != CycleSource::KnownDids {
        sqlx::query(
            "UPDATE firehose_gaps SET repair_cycle_id = $1
             WHERE to_at IS NOT NULL AND healed_at IS NULL AND repair_cycle_id IS NULL",
        )
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE sweep_cycles SET repair_from = COALESCE(repair_from,
               (SELECT min(from_at) FROM firehose_gaps WHERE repair_cycle_id = $1))
             WHERE id = $1",
        )
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    tracing::info!(
        cycle = c.id.get(),
        kind = c.kind.code(),
        source = source.as_str(),
        "cycle adopted"
    );
    Ok(())
}

/// Why a page of a cycle's source could not be read.
#[derive(Debug)]
enum PageError {
    /// The relay does not have `listReposByCollection`.
    NoCollectionListing,
    /// Anything else; the page is retried.
    Other(SweepError),
}

impl From<SweepError> for PageError {
    fn from(e: SweepError) -> PageError {
        PageError::Other(e)
    }
}

/// One page: the members, the next checkpoint, whether enumeration
/// finished.
type Page = (Vec<Member>, Option<String>, bool);

/// Fetches the next page of the cycle's source.
async fn next_page(ctx: &Ctx, c: &Cycle, room: u32) -> Result<Page, PageError> {
    let cfg = ctx.cfg();
    let relay = cfg.backfill.relay_url.clone();
    let cp = c.checkpoint.clone();
    // Repairs enumerate listRepos whatever the sweep source.
    let source = if c.kind == CycleKind::Repair && c.source != CycleSource::KnownDids {
        CycleSource::RelayRepos
    } else {
        c.source
    };
    match source {
        CycleSource::RelayCollections => {
            // Checkpoint `<collection index>|<cursor>`.
            let (mut idx, cursor) = match cp.as_deref().and_then(|s| s.split_once('|')) {
                Some((i, cur)) => (
                    i.parse::<usize>().unwrap_or(0),
                    Some(cur.to_owned()).filter(|s| !s.is_empty()),
                ),
                None => (0, None),
            };
            let Some(k) = SWEPT.get(idx) else {
                return Ok((Vec::new(), cp, true));
            };
            let (dids, next) =
                xrpc::list_repos_by_collection(&ctx.net, &relay, k.nsid(), cursor.as_deref(), room)
                    .await
                    .map_err(|err| {
                        if err.method_missing() {
                            PageError::NoCollectionListing
                        } else {
                            PageError::Other(err.into())
                        }
                    })?;
            let members = dids
                .into_iter()
                .map(|did| Member {
                    did,
                    reactivated: false,
                })
                .collect();
            let next_cp = match next.filter(|n| Some(n) != cursor.as_ref()) {
                Some(n) => format!("{idx}|{n}"),
                None => {
                    idx += 1;
                    format!("{idx}|")
                }
            };
            let done = idx >= SWEPT.len();
            Ok((members, Some(next_cp), done))
        }
        CycleSource::RelayRepos => {
            let (repos, next) = xrpc::list_repos(&ctx.net, &relay, cp.as_deref(), room)
                .await
                .map_err(SweepError::from)?;
            let members = if c.kind == CycleKind::Repair {
                repair_candidates(ctx, c, repos).await?
            } else {
                repos
                    .into_iter()
                    .map(|r| Member {
                        did: r.did,
                        reactivated: false,
                    })
                    .collect()
            };
            let next = next.filter(|n| Some(n) != cp.as_ref());
            let done = next.is_none();
            Ok((members, next.or(cp), done))
        }
        CycleSource::Plc => {
            let ops = xrpc::plc_export(&ctx.net, &cfg.backfill.plc_url, cp.as_deref(), room)
                .await
                .map_err(SweepError::from)?;
            let done = ops.len() < room.min(1000) as usize;
            let next = ops.last().map(|o| o.created_at.clone()).or(cp);
            // The directory decides how many operations a page holds, so
            // DIDs are told apart with a set, not by searching the page.
            let mut members = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for op in ops.into_iter().filter(|o| !o.nullified) {
                if cfg.backfill.plc_seed_from_export
                    && let Some(p) = &op.pds
                {
                    ctx.resolver.seed(&op.did, p);
                }
                if seen.insert(op.did.clone()) {
                    members.push(Member {
                        did: op.did,
                        reactivated: false,
                    });
                }
            }
            Ok((members, next, done))
        }
        CycleSource::KnownDids => {
            // KNOWN_DIDS: every DID in backfill_state plus owners of
            // tracked lists, in DID order; checkpoint = last DID.
            let after = cp.clone().unwrap_or_default();
            let dids: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT did FROM (
                   SELECT a.did FROM backfill_state b JOIN actors a ON a.id = b.actor_id
                   UNION
                   SELECT a.did FROM lists l JOIN actors a ON a.id = l.owner_id
                   WHERE l.track_state IN {TRACKED}) x
                 WHERE did > $1 ORDER BY did LIMIT $2"
            ))
            .bind(&after)
            .bind(i64::from(room))
            .fetch_all(&ctx.pool)
            .await
            .map_err(SweepError::from)?;
            let done = dids.len() < room as usize;
            let next = dids.last().cloned().or(cp);
            Ok((
                dids.into_iter()
                    .map(|did| Member {
                        did,
                        reactivated: false,
                    })
                    .collect(),
                next,
                done,
            ))
        }
    }
}

/// Repos changed since `from − slack − lag`, and repos the relay
/// reports active that Farsight holds inactive.
async fn repair_candidates(ctx: &Ctx, c: &Cycle, repos: Vec<xrpc::ListedRepo>) -> Res<Vec<Member>> {
    let cfg = ctx.cfg();
    let lag: i64 = sqlx::query_scalar(
        "SELECT COALESCE(GREATEST(0, EXTRACT(EPOCH FROM now() - applied_through))::BIGINT, 0)
         FROM firehose_state WHERE id = 1",
    )
    .fetch_optional(&ctx.pool)
    .await?
    .unwrap_or(0);
    let slack = chrono::Duration::from_std(cfg.backfill.repair_slack.get()).unwrap_or_default()
        + chrono::Duration::seconds(lag);
    let from = c.repair_from.unwrap_or(c.started_at) - slack;
    let from_us = from.timestamp_micros();
    let active: Vec<String> = repos
        .iter()
        .filter(|r| r.active)
        .map(|r| r.did.clone())
        .collect();
    let held_inactive: Vec<String> = sqlx::query_scalar(
        &format!("SELECT a.did FROM actors a LEFT JOIN backfill_state b ON b.actor_id = a.id
         WHERE a.did = ANY($1) AND (a.status <> {ACTOR_ACTIVE} OR COALESCE(b.inactive_at_listing, false))"),
    )
    .bind(&active)
    .fetch_all(&ctx.pool)
    .await?;
    let mut out = Vec::new();
    for r in repos {
        let reactivated = r.active && held_inactive.contains(&r.did);
        let changed = r
            .rev
            .as_deref()
            .and_then(|v| Tid::parse(v).ok())
            .is_none_or(|t| i64::try_from(t.micros()).unwrap_or(i64::MAX) >= from_us);
        if changed || reactivated {
            out.push(Member {
                did: r.did,
                reactivated,
            });
        }
    }
    Ok(out)
}

/// Enumerates one page into `cycle_outstanding` and advances the
/// checkpoint in the same transaction.
async fn enumerate_page(ctx: &Ctx, sweep: &Sweep, c: &Cycle, room: u32) -> Res<()> {
    let (members, next, done) = match next_page(ctx, c, room).await {
        Ok(p) => p,
        Err(PageError::NoCollectionListing) => {
            // The relay lost the method, or was never probed with it (a
            // probe that found it down): the cycle goes on with
            // `listRepos` from its start. Members already enumerated stay.
            sqlx::query(
                "UPDATE sweep_cycles SET source = $2, checkpoint = NULL
                 WHERE id = $1 AND source = $3 AND enumerated_at IS NULL",
            )
            .bind(c.id)
            .bind(CycleSource::RelayRepos)
            .bind(CycleSource::RelayCollections)
            .execute(&ctx.pool)
            .await?;
            sweep.fell_back.store(true, Ordering::Relaxed);
            tracing::warn!(
                cycle = c.id.get(),
                "relay lacks listReposByCollection; the cycle continues with relay_repos"
            );
            return Ok(());
        }
        Err(PageError::Other(err)) => {
            tracing::warn!(cycle = c.id.get(), error = %err, "sweep enumeration page failed; retrying");
            return Ok(());
        }
    };
    for m in members.iter().filter(|m| m.reactivated) {
        reactivation(ctx, &m.did).await?;
    }
    let dids: Vec<String> = members.into_iter().map(|m| m.did).collect();
    let mut tx = ctx.pool.begin().await?;
    // A DID whose job started after S_C already satisfied this cycle (its
    // point is ≥ S_C), e.g. seen in an earlier collection's listing.
    let n = sqlx::query(&format!(
        "INSERT INTO cycle_outstanding (cycle_id, did, state)
         SELECT $1, d, {MEMBER_OUTSTANDING} FROM unnest($2::text[]) d
         WHERE NOT EXISTS (
           SELECT 1 FROM actors a JOIN backfill_state b ON b.actor_id = a.id
           WHERE a.did = d AND b.last_outcome IN {RUN_LISTED}
             AND b.backfilled_witness >= $3)
         ON CONFLICT DO NOTHING"
    ))
    .bind(c.id)
    .bind(&dids)
    .bind(c.effective_start_witness)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    sqlx::query(
        "UPDATE sweep_cycles SET checkpoint = $2,
           total_est = COALESCE(total_est, 0) + $3,
           enumerated_at = CASE WHEN $4 THEN now() END
         WHERE id = $1",
    )
    .bind(c.id)
    .bind(&next)
    .bind(n as i64)
    .bind(done)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// A repo the relay reports active that Farsight holds inactive: a
/// `resync` debt, and OA on its `unavailable` lists.
async fn reactivation(ctx: &Ctx, did: &str) -> Res<()> {
    let Some(id) = crate::jobs::actor_id(&ctx.pool, did).await? else {
        return Ok(());
    };
    let witness = farsight_storage::firehose::clock_now(&ctx.pool)
        .await?
        .unwrap_or_else(Utc::now);
    farsight_storage::debts::add_debt(&ctx.pool, id, DebtReason::Resync, None, witness).await?;
    let lists: Vec<ListId> =
        sqlx::query_scalar("SELECT id FROM lists WHERE owner_id = $1 AND track_state = $2")
            .bind(id)
            .bind(TrackState::Unavailable.code())
            .fetch_all(&ctx.pool)
            .await?;
    let limits = ctx.limits();
    for l in lists {
        farsight_storage::janitor::fire_event(
            &ctx.pool,
            &limits,
            &ctx.counters,
            l,
            Event::OwnerActive,
            FireArgs::default(),
        )
        .await?;
    }
    Ok(())
}

/// Completes a cycle whose enumeration finished and whose remaining rows
/// are all terminal; a repair heals the gaps it claimed, a full cycle
/// heals every gap that closed before its `S_C`.
async fn maybe_complete(ctx: &Ctx, c: &Cycle) -> Res<()> {
    let pool = &ctx.pool;
    let enumerated: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT enumerated_at FROM sweep_cycles WHERE id = $1")
            .bind(c.id)
            .fetch_one(pool)
            .await?;
    if enumerated.is_none() {
        return Ok(());
    }
    let outstanding: bool = sqlx::query_scalar(
        &format!("SELECT EXISTS (SELECT 1 FROM cycle_outstanding WHERE cycle_id = $1 AND state = {MEMBER_OUTSTANDING})"),
    )
    .bind(c.id)
    .fetch_one(pool)
    .await?;
    if outstanding {
        return Ok(());
    }
    let now: DateTime<Utc> = crate::jobs::db_now(pool).await?;
    let witness = farsight_storage::firehose::clock(pool, now)
        .await?
        .unwrap_or(now);
    let gaps: Vec<GapId> = if c.kind == CycleKind::Repair && c.source != CycleSource::KnownDids {
        sqlx::query_scalar(
            "SELECT id FROM firehose_gaps WHERE repair_cycle_id = $1 AND healed_at IS NULL",
        )
        .bind(c.id)
        .fetch_all(pool)
        .await?
    } else if c.kind == CycleKind::Full {
        sqlx::query_scalar(
            "SELECT id FROM firehose_gaps WHERE healed_at IS NULL AND to_at IS NOT NULL
               AND to_at <= $1",
        )
        .bind(c.effective_start_witness)
        .fetch_all(pool)
        .await?
    } else {
        Vec::new()
    };
    let mut tx = pool.begin().await?;
    // Terminal rows are kept counted as `unreachable` debts; the cycle's
    // own rows go. They go before the cycle's row is written, the order a
    // job settling a member takes the two locks in.
    sqlx::query("DELETE FROM cycle_outstanding WHERE cycle_id = $1")
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE sweep_cycles SET completed_at = $2, completed_witness = $3 WHERE id = $1")
        .bind(c.id)
        .bind(now)
        .bind(witness)
        .execute(&mut *tx)
        .await?;
    if c.kind == CycleKind::Repair {
        // Gaps it could not heal (relay down) go back to the next repair
        // or full cycle.
        sqlx::query("UPDATE firehose_gaps SET repair_cycle_id = NULL WHERE repair_cycle_id = $1 AND healed_at IS NULL AND NOT (id = ANY($2))")
            .bind(c.id)
            .bind(&gaps)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    if !gaps.is_empty() {
        farsight_storage::firehose::heal_gaps(pool, &gaps, witness, c.id).await?;
    }
    tracing::info!(
        cycle = c.id.get(),
        kind = c.kind.code(),
        healed = gaps.len(),
        "cycle completed"
    );
    Ok(())
}

/// Progress gauges per cycle kind, and the ETA of the open full cycle
/// (remaining ÷ trailing-1h rate).
async fn publish_progress(ctx: &Ctx) {
    for (kind, label) in [(CycleKind::Full, "full"), (CycleKind::Repair, "repair")] {
        let row: Option<(i64, i64, i64, bool)> = sqlx::query_as(
            &format!("SELECT c.done, c.failed_terminal,
                    (SELECT count(*) FROM cycle_outstanding o WHERE o.cycle_id = c.id AND o.state = {MEMBER_OUTSTANDING}),
                    c.enumerated_at IS NOT NULL
             FROM sweep_cycles c WHERE c.completed_at IS NULL AND c.kind = $1 ORDER BY c.id LIMIT 1"),
        )
        .bind(kind)
        .fetch_optional(&ctx.pool)
        .await
        .ok()
        .flatten();
        let Some((done, failed, outstanding, enumerated)) = row else {
            metrics::gauge!(m::SWEEP_PROGRESS, "cycle_kind" => label).set(1.0);
            continue;
        };
        // Until enumeration ends the total is unknown: progress over what
        // has been enumerated so far.
        let total = (done + failed + outstanding).max(1) as f64;
        metrics::gauge!(m::SWEEP_PROGRESS, "cycle_kind" => label)
            .set((done + failed) as f64 / total);
        if kind == CycleKind::Full {
            let rate: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM backfill_state WHERE backfilled_at > now() - interval '1 hour'",
            )
            .fetch_one(&ctx.pool)
            .await
            .unwrap_or(0);
            if rate > 0 && enumerated {
                metrics::gauge!(m::SWEEP_ETA).set(outstanding as f64 / rate as f64 * 3600.0);
            }
        }
    }
}
