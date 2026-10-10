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
    ACTOR_ACTIVE, CYCLE_FULL, CYCLE_REPAIR, MEMBER_OUTSTANDING, REPO_FAILED, RUN_LISTED, TRACKED,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use farsight_core::config::SweepSource;
use farsight_core::{Collection, Did, Tid};
use farsight_storage::codes::{CycleKind, CycleSource, DebtReason, TrackState};
use farsight_storage::ids::{ActorId, CycleId, GapId, ListId};
use farsight_storage::tracking::FireArgs;
use farsight_storage::transition::Event;
use tokio::sync::watch;

use crate::ctx::Ctx;
use crate::metrics as m;
use crate::xrpc;

const TICK: Duration = Duration::from_secs(5);
/// Pages a cycle enumerates in one tick while it has room for them. A
/// source of some millions of entries is read in minutes, and the relay
/// is asked a few times a second.
const PAGES_PER_TICK: u32 = 25;
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
    /// The cycle whose members this process has counted or is counting
    /// (`sweep_cycles.id`; 0 before the first).
    counted: AtomicI64,
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

/// Largest page of the count (`listReposByCollection` limit).
const COUNT_PAGE: u32 = 2000;
/// Failed pages in a row after which a count is given up.
const COUNT_RETRIES: u32 = 20;
/// Wait before a failed page of the count is asked for again.
const COUNT_RETRY_WAIT: Duration = Duration::from_secs(30);

/// The open full `relay_collections` cycle whose members are not counted
/// yet, if the sweep is on. Each process counts a cycle once: a cycle
/// left open by an earlier process is counted again, since what that
/// process stored may be the pages it had taken and not a count.
async fn cycle_to_count(ctx: &Ctx, sweep: &Sweep) -> Option<CycleId> {
    if !ctx.cfg().backfill.sweep.enabled {
        return None;
    }
    let id: CycleId = sqlx::query_scalar(
        "SELECT id FROM sweep_cycles
         WHERE completed_at IS NULL AND enumerated_at IS NULL AND kind = $1 AND source = $2
         ORDER BY id LIMIT 1",
    )
    .bind(CycleKind::Full)
    .bind(CycleSource::RelayCollections)
    .fetch_optional(&ctx.pool)
    .await
    .ok()
    .flatten()?;
    (sweep.counted.swap(id.get(), Ordering::Relaxed) != id.get()).then_some(id)
}

/// Counts the members of a `relay_collections` cycle and stores the count
/// as the cycle's total.
///
/// Enumeration takes a page only when the cycle has room for its members
/// (`backfill.sweep.max_outstanding`), so for most of a cycle it has seen
/// a small part of the source and cannot say how much is left. The relay
/// gives no total either. This reads every page of the three listings
/// once, keeping nothing but the number of entries: a few thousand
/// requests for some millions of accounts. An account that is in more
/// than one listing is counted in each, and one that is listed before the
/// count reaches it and satisfied before enumeration does is counted and
/// never becomes a member, so the total is somewhat high. The last page
/// of the enumeration replaces it with the members settled and
/// outstanding at that moment.
pub async fn count_members(ctx: Arc<Ctx>, cycle: CycleId) {
    let relay = ctx.cfg().backfill.relay_url.clone();
    let mut total = 0i64;
    for k in SWEPT {
        let mut cursor: Option<String> = None;
        let mut failures = 0u32;
        loop {
            let page = xrpc::list_repos_by_collection(
                &ctx.net,
                &relay,
                k.nsid(),
                cursor.as_deref(),
                COUNT_PAGE,
            )
            .await;
            match page {
                Ok((dids, next)) => {
                    failures = 0;
                    total += dids.len() as i64;
                    match next.filter(|n| Some(n) != cursor.as_ref()) {
                        Some(n) => cursor = Some(n),
                        None => break,
                    }
                }
                Err(e) => {
                    failures += 1;
                    if failures > COUNT_RETRIES {
                        tracing::warn!(cycle = cycle.get(), error = %e, "counting the cycle's members failed; its progress stays unknown");
                        return;
                    }
                    tokio::time::sleep(COUNT_RETRY_WAIT).await;
                }
            }
        }
    }
    let stored = sqlx::query(
        "UPDATE sweep_cycles SET total_est = $2 WHERE id = $1 AND enumerated_at IS NULL",
    )
    .bind(cycle)
    .bind(total)
    .execute(&ctx.pool)
    .await;
    match stored {
        Ok(_) => tracing::info!(
            cycle = cycle.get(),
            members = total,
            "cycle members counted"
        ),
        Err(e) => {
            tracing::warn!(cycle = cycle.get(), error = %e, "storing the cycle's count failed")
        }
    }
}

/// Runs cycles until `stop` flips.
pub async fn run(ctx: Arc<Ctx>, sweep: Arc<Sweep>, mut stop: watch::Receiver<bool>) {
    // The count of a cycle's members runs beside the ticks, and ends
    // with this future.
    let mut counts: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        if let Err(e) = tick(&ctx, &sweep).await {
            tracing::warn!(error = %e, "sweep tick failed");
        }
        while counts.try_join_next().is_some() {}
        if counts.is_empty()
            && let Some(cycle) = cycle_to_count(&ctx, &sweep).await
        {
            counts.spawn(count_members(ctx.clone(), cycle));
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

/// One step: start cycles that are due, enumerate pages of each open cycle
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
            let mut outstanding: i64 = sqlx::query_scalar(
                &format!("SELECT count(*) FROM cycle_outstanding WHERE cycle_id = $1 AND state = {MEMBER_OUTSTANDING}"),
            )
            .bind(c.id)
            .fetch_one(pool)
            .await?;
            // A full bound must not stop enumeration for as long as
            // failing members are retried (a week each).
            if outstanding >= max_out {
                outstanding -= release_waiting(ctx, c.id, max_out).await?;
            }
            // Each page asks for at most the room left, so outstanding
            // rows never exceed the bound. The pages of one tick are
            // counted as full: the rows are counted once a tick.
            let mut cycle = c.clone();
            for _ in 0..PAGES_PER_TICK {
                if outstanding >= max_out {
                    break;
                }
                let room = u32::try_from((max_out - outstanding).min(PAGE)).unwrap_or(1);
                enumerate_page(ctx, sweep, &cycle, room).await?;
                outstanding += i64::from(room);
                let next: Option<Cycle> = sqlx::query_as(
                    "SELECT id, kind, source, started_at, effective_start_witness, enumerated_at,
                            checkpoint, repair_from
                     FROM sweep_cycles WHERE id = $1 AND completed_at IS NULL",
                )
                .bind(c.id)
                .fetch_optional(pool)
                .await?;
                // The next page follows at once only when this one moved
                // the cycle on: a page that failed, the last one, and a
                // change of source all wait for the next tick.
                match next {
                    Some(n)
                        if n.enumerated_at.is_none()
                            && n.source == cycle.source
                            && n.checkpoint != cycle.checkpoint =>
                    {
                        cycle = n;
                    }
                    _ => break,
                }
            }
        }
        maybe_complete(ctx, c).await?;
    }
    publish_progress(ctx).await;
    Ok(())
}

/// Makes room in a full cycle whose members mostly wait for a retry.
///
/// A member whose job failed stays outstanding while it is retried, for
/// up to `backfill.terminal_after`. Enough of them (accounts on hosts
/// that are down, or on a host that fails on purpose) fill the bound,
/// and nothing new is enumerated until they give up. So when at least
/// half of a full bound is members that wait for their next attempt, up
/// to one page of them, the longest-failing first, are settled now as
/// what they would become: `terminal` in the cycle, with an
/// `unreachable` debt. Coverage counts them from then on. Their retries
/// stay queued, and one that succeeds clears the debt. Returns how many
/// were settled.
async fn release_waiting(ctx: &Ctx, cycle: CycleId, max_out: i64) -> Res<i64> {
    let pool = &ctx.pool;
    let waiting: Vec<(String, ActorId, Option<DateTime<Utc>>)> = sqlx::query_as(&format!(
        "SELECT o.did, a.id, b.current_run_point FROM cycle_outstanding o
         JOIN actors a ON a.did = o.did JOIN backfill_state b ON b.actor_id = a.id
         WHERE o.cycle_id = $1 AND o.state = {MEMBER_OUTSTANDING}
           AND b.state = {REPO_FAILED} AND b.next_attempt_at > now()
         ORDER BY b.first_failed_at NULLS LAST, a.id LIMIT $2"
    ))
    .bind(cycle)
    .bind(max_out)
    .fetch_all(pool)
    .await?;
    if (waiting.len() as i64) < (max_out + 1) / 2 {
        return Ok(0);
    }
    let now = crate::jobs::db_now(pool).await?;
    let witness = farsight_storage::firehose::clock(pool, now)
        .await?
        .unwrap_or(now);
    let mut settled = 0;
    for (did, id, point) in waiting.into_iter().take(PAGE as usize) {
        let Ok(did) = Did::parse(&did) else { continue };
        farsight_storage::debts::add_debt(
            pool,
            id,
            DebtReason::Unreachable,
            None,
            point.unwrap_or(witness),
        )
        .await?;
        crate::jobs::settle_membership(pool, &did, now, true).await?;
        settled += 1;
    }
    if settled > 0 {
        tracing::warn!(
            cycle = cycle.get(),
            settled,
            "the sweep's outstanding bound was full of members waiting for a retry; \
             the longest-failing are counted as unreachable so that enumeration goes on"
        );
    }
    Ok(settled)
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
    if kind == CycleKind::Repair {
        // `admin.startRepair` starts repairs too. Both take this lock and
        // look for an open repair under it, so one of them starts it and
        // the other finds it.
        sqlx::query(farsight_storage::firehose::START_REPAIR_LOCK)
            .execute(&mut *tx)
            .await?;
        let open: Option<CycleId> = sqlx::query_scalar(&format!(
            "SELECT id FROM sweep_cycles WHERE kind = {CYCLE_REPAIR} AND completed_at IS NULL
             ORDER BY id LIMIT 1"
        ))
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(id) = open {
            return Ok(id);
        }
    }
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
            // A page too large to read is asked for again at half the
            // size: at the same size it would be too large every time.
            let mut count = room.clamp(1, 1000);
            let mut ops = loop {
                match xrpc::plc_export(&ctx.net, &cfg.backfill.plc_url, cp.as_deref(), count).await
                {
                    Err(crate::net::NetError::TooLarge(_)) if count > 1 => count /= 2,
                    r => break r.map_err(SweepError::from)?,
                }
            };
            let full = ops.len() >= count as usize;
            let done = !full;
            let (used, next) = xrpc::export_step(&ops, full);
            let next = next.map(str::to_owned).or(cp);
            ops.truncate(used);
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

/// Repos changed since `from − slack − lag`, repos the relay reports
/// active that Farsight holds inactive, and repos the relay reports
/// inactive that Farsight holds active with rows stored. An account
/// event carries no rev, so one lost in the gap (a deletion, a takedown,
/// a reactivation) shows only in this difference of status; without the
/// last group the rows of an account deleted during a gap would stay.
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
    let inactive: Vec<String> = repos
        .iter()
        .filter(|r| !r.active)
        .map(|r| r.did.clone())
        .collect();
    let held_active: std::collections::HashSet<String> = if inactive.is_empty() {
        Default::default()
    } else {
        sqlx::query_scalar::<_, String>(&format!(
            "SELECT a.did FROM actors a
             WHERE a.did = ANY($1) AND a.status = {ACTOR_ACTIVE}
               AND (a.authored_blocks > 0 OR a.authored_listblocks > 0
                    OR a.authored_lists > 0 OR a.owned_items > 0)"
        ))
        .bind(&inactive)
        .fetch_all(&ctx.pool)
        .await?
        .into_iter()
        .collect()
    };
    let mut out = Vec::new();
    for r in repos {
        let reactivated = r.active && held_inactive.contains(&r.did);
        let deactivated = !r.active && held_active.contains(&r.did);
        let changed = r
            .rev
            .as_deref()
            .and_then(|v| Tid::parse(v).ok())
            .is_none_or(|t| i64::try_from(t.micros()).unwrap_or(i64::MAX) >= from_us);
        if changed || reactivated || deactivated {
            out.push(Member {
                did: r.did,
                reactivated,
            });
        }
    }
    Ok(out)
}

/// The members whose DID is one, and how many were not. An entry that is
/// not a DID names no repository: as a member it could never be listed,
/// and its cycle would never complete.
pub fn valid_members(members: Vec<Member>) -> (Vec<Member>, usize) {
    let listed = members.len();
    let valid: Vec<Member> = members
        .into_iter()
        .filter(|m| farsight_core::Did::parse(&m.did).is_ok())
        .collect();
    let invalid = listed - valid.len();
    (valid, invalid)
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
    let (members, invalid) = valid_members(members);
    if invalid > 0 {
        tracing::warn!(
            cycle = c.id.get(),
            invalid,
            "the source listed entries that are not DIDs; they are left out"
        );
    }
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
    // The total of a `relay_collections` cycle is counted ahead of its
    // enumeration ([`count_members`]), which only ever knows the pages it
    // has taken: there the count stands until the last page, and is then
    // replaced by the members settled and outstanding. The other sources have no
    // count, and their total grows page by page.
    sqlx::query(&format!(
        "UPDATE sweep_cycles SET checkpoint = $2,
           total_est = CASE
             WHEN source <> $5 THEN COALESCE(total_est, 0) + $3
             WHEN $4 THEN done + failed_terminal
               + (SELECT count(*) FROM cycle_outstanding o
                  WHERE o.cycle_id = sweep_cycles.id AND o.state = {MEMBER_OUTSTANDING})
             ELSE total_est END,
           enumerated_at = CASE WHEN $4 THEN now() END
         WHERE id = $1",
    ))
    .bind(c.id)
    .bind(&next)
    .bind(n as i64)
    .bind(done)
    .bind(CycleSource::RelayCollections)
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
    // The gaps are healed with the completion. Done after it, a failure
    // in between would leave them claimed by a cycle that is over, which
    // no later repair takes up.
    if !gaps.is_empty() {
        farsight_storage::firehose::heal_gaps_in(&mut tx, &gaps, witness, c.id).await?;
    }
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
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
        let row: Option<(i64, i64, i64, bool, CycleSource, Option<i64>)> = sqlx::query_as(
            &format!("SELECT c.done, c.failed_terminal,
                    (SELECT count(*) FROM cycle_outstanding o WHERE o.cycle_id = c.id AND o.state = {MEMBER_OUTSTANDING}),
                    c.enumerated_at IS NOT NULL, c.source, c.total_est
             FROM sweep_cycles c WHERE c.completed_at IS NULL AND c.kind = $1 ORDER BY c.id LIMIT 1"),
        )
        .bind(kind)
        .fetch_optional(&ctx.pool)
        .await
        .ok()
        .flatten();
        let Some((done, failed, outstanding, enumerated, source, counted)) = row else {
            metrics::gauge!(m::SWEEP_PROGRESS, "cycle_kind" => label).set(1.0);
            continue;
        };
        let settled = done + failed;
        // What is left to do. Once enumeration has ended that is the
        // members still outstanding. Before, only a `relay_collections`
        // cycle knows: its members were counted ahead. Any other cycle
        // knows the pages it has taken and no more, and its progress is
        // over those.
        let left = if enumerated {
            Some(outstanding)
        } else if source == CycleSource::RelayCollections {
            counted.map(|t| (t - settled).max(outstanding))
        } else {
            None
        };
        let total = (settled + left.unwrap_or(outstanding)).max(1) as f64;
        // A cycle whose count is not in yet has no progress to give.
        let ratio = if !enumerated && source == CycleSource::RelayCollections && counted.is_none() {
            0.0
        } else {
            settled as f64 / total
        };
        metrics::gauge!(m::SWEEP_PROGRESS, "cycle_kind" => label).set(ratio);
        if kind == CycleKind::Full {
            let rate: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM backfill_state WHERE backfilled_at > now() - interval '1 hour'",
            )
            .fetch_one(&ctx.pool)
            .await
            .unwrap_or(0);
            if let (true, Some(left)) = (rate > 0, left) {
                metrics::gauge!(m::SWEEP_ETA).set(left as f64 / rate as f64 * 3600.0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entries_that_are_not_dids_are_left_out_of_a_cycle() {
        let member = |did: &str| Member {
            did: did.to_owned(),
            reactivated: false,
        };
        let listed = vec![
            member("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"),
            member(""),
            member("not a did"),
            member("did:plc:"),
            member("did:web:example.com"),
            member("DID:PLC:aaaaaaaaaaaaaaaaaaaaaaaa"),
            member("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa\u{0}"),
        ];
        let (valid, invalid) = valid_members(listed);
        let dids: Vec<&str> = valid.iter().map(|m| m.did.as_str()).collect();
        assert_eq!(
            dids,
            ["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:web:example.com"]
        );
        assert_eq!(invalid, 5);
        assert_eq!(valid_members(Vec::new()), (Vec::new(), 0));
    }
}
