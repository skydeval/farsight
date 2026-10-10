//! The per-repo job (see `docs/design/backfill.md`): resolve → stamp →
//! describeRepo → late stamp → whole-range reconcile of absent collections
//! (early stamp only) → list each present collection with range reconcile
//! → done.

use farsight_storage::codes::sql::{
    DEBT_CAPPED, JOB_REPO, RECORD_PRESENT, RECORD_UNKNOWN, REPO_RUNNING, TRACKED,
};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeSet, HashSet};
use std::hash::BuildHasher;
use std::time::Duration;

use chrono::{DateTime, Utc};
use farsight_core::record::parse_record;
use farsight_core::{AtUri, Collection, Did, Record, RecordKey, Tid};
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

/// Pages one attempt of a job lists, over all its collections (reaching
/// it yields). An account at every per-author cap is about 31,000 pages.
pub const MAX_PAGES: u32 = 25_000;
/// Attempts of one run that may end at a bound (pages or time) before
/// the run counts as failed.
pub const MAX_YIELDS: i32 = 10;
/// Rows purged per divergence-purge transaction.
pub const PURGE_BATCH: i64 = 10_000;
/// Stored keys compared with a seen-set per reconcile.
pub const SEEN_CHUNK: i64 = 2_000;

/// How long a job that stopped at a bound for the `n`-th time in a row
/// waits before it goes on: not at all the first two times, then 30 s
/// doubling up to an hour.
pub fn yield_delay(n: i32) -> Duration {
    if n <= 2 {
        return Duration::ZERO;
    }
    let doublings = u32::try_from(n - 3).unwrap_or(0).min(16);
    Duration::from_secs((30u64 << doublings).min(3600))
}

/// The pages an attempt may still list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageBudget(pub u32);

impl PageBudget {
    /// The budget of a fresh attempt: [`MAX_PAGES`].
    pub fn new() -> PageBudget {
        PageBudget(MAX_PAGES)
    }

    /// Takes one page; `false` when none is left.
    pub fn take(&mut self) -> bool {
        if self.0 == 0 {
            return false;
        }
        self.0 -= 1;
        true
    }
}

impl Default for PageBudget {
    fn default() -> PageBudget {
        PageBudget::new()
    }
}

/// The cursors a listing has followed, to tell one that comes back. A
/// PDS that answers every page with a cursor it gave before (the same
/// one, or one of a cycle) would be listed without end.
#[derive(Debug, Default)]
pub struct Cursors {
    hasher: RandomState,
    seen: HashSet<u64>,
}

impl Cursors {
    /// Notes `next` as the cursor of the page after the one read with
    /// `current`; `false` if the listing has been there before.
    pub fn follow(&mut self, current: Option<&str>, next: &str) -> bool {
        if let Some(c) = current {
            self.seen.insert(self.hasher.hash_one(c));
        }
        self.seen.insert(self.hasher.hash_one(next))
    }

    /// Forgets every cursor (the listing starts again from its first
    /// page).
    pub fn clear(&mut self) {
        self.seen.clear();
    }
}

/// How a listing tells which stored rows the repo no longer has.
#[derive(Debug)]
enum Mode {
    /// Record keys ascend: each page reconciles its own range.
    Ordered,
    /// The PDS lists out of order: the listing started again and
    /// remembers every key it sees (as a hash), and the stored rows are
    /// compared with the set at the end.
    Seen(HashSet<u64>),
    /// More keys than `backfill.seen_set_cap`: the listing goes on to the
    /// end and reconciles nothing.
    Unreconciled,
}

/// Whether an attempt that takes the listing up from the cursor stored
/// with this page could not reconcile it: the listing is past the
/// seen-set cap, or it is out of order and has pages to go (the set of
/// keys seen is in memory and ends with the attempt). A seen-set listing
/// that reached its last page was reconciled against the set.
fn resumes_unreconciled(mode: &Mode, more_pages: bool) -> bool {
    match mode {
        Mode::Ordered => false,
        Mode::Seen(_) => more_pages,
        Mode::Unreconciled => true,
    }
}

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
    /// A bound of the attempt (pages) was reached.
    Yield,
    /// The repo looks diverged, or a page would remove stored rows, as
    /// read from a host that came out of a cache: the host is confirmed
    /// with the directory before anything is purged or removed on its
    /// word.
    Unconfirmed,
}

impl From<farsight_storage::StorageError> for Stop {
    fn from(e: farsight_storage::StorageError) -> Stop {
        match e {
            farsight_storage::StorageError::UnconfirmedHost => Stop::Unconfirmed,
            e => Stop::Failed(e.to_string()),
        }
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

/// Pages of one collection after which an attempt judges the host's
/// pace.
pub const SLOW_AFTER_PAGES: u32 = 5;
/// The mean time a host may take to answer a `listRecords` page. A host
/// slower than this over [`SLOW_AFTER_PAGES`] pages or more ends the
/// attempt as failed: the job gives its worker back and is retried on
/// the retry schedule, and goes on from its cursor then.
pub const SLOW_PAGE: Duration = Duration::from_secs(10);

/// Whether a host that took `waited_ms` to answer `pages` pages is too
/// slow to go on with. Only the time spent waiting for the host counts,
/// not the time a request waited for a slot under Farsight's own limits.
pub fn too_slow(pages: u32, waited_ms: u64) -> bool {
    pages >= SLOW_AFTER_PAGES && u128::from(waited_ms) > SLOW_PAGE.as_millis() * u128::from(pages)
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
    /// The host was looked up in the directory for this attempt, not
    /// taken from a cache. Only then may a listing remove stored rows.
    pub host_confirmed: bool,
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
            host_confirmed: true,
        }
    }

    /// The policy for a host that was (`true`) or was not looked up in
    /// the directory for this attempt.
    pub fn on_host(mut self, confirmed: bool) -> GatePolicy {
        self.host_confirmed = confirmed;
        self
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
    ctx.cfg()
        .limits
        .is_large_host(crate::net::bare_host(&pds.host))
}

/// Applies `batch`, then its reconciles alone for as long as they have
/// more rows to remove than one transaction takes. Returns the report of
/// the first application (the one with the writes).
async fn apply_through(
    pool: &PgPool,
    actx: &ApplyCtx<'_>,
    batch: &Batch,
) -> Result<farsight_storage::txn::ApplyReport, Stop> {
    let first = apply::apply(pool, actx, batch).await?;
    if first.reconcile_pending {
        let mut rest = Batch::new(batch.origin.clone());
        rest.reconciles = batch.reconciles.clone();
        loop {
            let r = apply::apply(pool, actx, &rest).await?;
            if !r.reconcile_pending {
                break;
            }
            if r.reconciled == 0 {
                return Err(Stop::Failed(
                    "a reconcile had rows left to remove and removed none".into(),
                ));
            }
        }
    }
    Ok(first)
}

/// The record a listing page holds under a key, if it is one Farsight
/// indexes: `None` for a value whose text could not be parsed and for a
/// record that is not valid for its collection. Such a key is listed in
/// the repo and holds nothing to store.
pub fn listed_record(
    did: &Did,
    k: Collection,
    value: Option<&serde_json::Value>,
) -> Option<Record> {
    value.and_then(|v| parse_record(did, k, v).ok())
}

/// Lists one collection of `did` with stamp `R` through its own cursor rows
/// and applies each page with range reconcile. Every page is taken from
/// `budget`; when none is left the listing stops with [`Stop::Yield`].
#[allow(clippy::too_many_arguments)]
pub async fn list_collection(
    ctx: &Ctx,
    pds: &Pds,
    did: &Did,
    k: Collection,
    stamp: ListingStamp,
    run: CursorRun,
    policy: GatePolicy,
    budget: &mut PageBudget,
) -> Result<Listing, Stop> {
    let pool = &ctx.pool;
    let limits = ctx.limits();
    let seen_cap = ctx.cfg().backfill.seen_set_cap as usize;
    let mut out = Listing::default();
    // Resume from this run's own cursor row, if any (never another job's).
    let mut cursor: Option<String> = None;
    let mut prev_last: Option<String> = None;
    if let Some(id) = jobs::actor_id(pool, did.as_str()).await? {
        let row: Option<(Option<String>, Option<String>, bool)> = sqlx::query_as(
            "SELECT cursor, prev_last, unreconciled FROM backfill_cursors
             WHERE actor_id = $1 AND collection = $2 AND job_kind = $3 AND run_id = $4",
        )
        .bind(id)
        .bind(k.code())
        .bind(run.job_kind())
        .bind(run.id())
        .fetch_optional(pool)
        .await?;
        if let Some((c, p, unreconciled)) = row {
            // What an earlier attempt of this run could not reconcile
            // stays unreconciled: the attempt that goes on, or finds the
            // collection finished, reports it all the same.
            out.reconcile_skipped = unreconciled;
            if c.is_none() && p.is_some() {
                // Finished in an earlier attempt of this run.
                return Ok(out);
            }
            cursor = c;
            prev_last = p;
        }
    }
    // An attempt that takes up a listing which was out of order (its
    // seen-set went with the attempt that held it) or past the seen-set
    // cap cannot tell which stored rows the repo no longer has: it lists
    // on to the end and reconciles nothing.
    let mut mode = if out.reconcile_skipped {
        Mode::Unreconciled
    } else {
        Mode::Ordered
    };
    let hasher = RandomState::new();
    let mut cursors = Cursors::default();
    let mut pages = 0u32;
    let waited_at_start = crate::net::waited_ms();
    loop {
        if !budget.take() {
            return Err(Stop::Yield);
        }
        let (records, next) =
            xrpc::list_records(&ctx.net, &pds.endpoint, did, k.nsid(), cursor.as_deref())
                .await
                .map_err(net_stop)?;
        out.cost += 1;
        pages += 1;
        if let Some(n) = &next
            && !cursors.follow(cursor.as_deref(), n)
        {
            return Err(Stop::Failed(
                "listRecords gave a cursor it had given before".into(),
            ));
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
            if matches!(mode, Mode::Ordered) && last.as_deref().is_some_and(|l| rk.as_str() <= l) {
                order_broken = true;
                break;
            }
            // The key counts for the order of the listing whatever its
            // record is.
            let seen_hash = hasher.hash_one(rk.as_str());
            last = Some(rk);
            match listed_record(did, k, r.value.as_ref()) {
                Some(rec) => {
                    if let Mode::Seen(s) = &mut mode {
                        s.insert(seen_hash);
                    }
                    keys.push(uri.rkey.clone());
                    writes.push(Write {
                        author: did.clone(),
                        collection: k,
                        rkey: uri.rkey.clone(),
                        stamp: stamp.rev,
                        witness: None,
                        action: WriteAction::Upsert(rec),
                    });
                }
                // The key holds nothing Farsight indexes: it is not among
                // the keys the reconcile keeps, so a version stored under
                // it from before this listing is removed, as the firehose
                // removes one when an update turns a record invalid.
                None => invalid += 1,
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
            // Restart this collection with an in-memory seen-set. Only an
            // ordered listing does: one that outgrew its set stays
            // unreconciled, however the pages are ordered.
            tracing::warn!(did = %did, collection = %k, "rkeys not increasing; restarting with a seen-set");
            mode = Mode::Seen(HashSet::new());
            cursor = None;
            prev_last = None;
            cursors.clear();
            continue;
        }
        if matches!(&mode, Mode::Seen(s) if s.len() > seen_cap) {
            tracing::warn!(did = %did, collection = %k, "more keys than the seen-set holds; listing on without reconcile");
            mode = Mode::Unreconciled;
            out.reconcile_skipped = true;
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
            host_confirmed: policy.host_confirmed,
        });
        batch.writes = writes;
        if matches!(mode, Mode::Ordered) {
            batch.reconciles.push(Reconcile {
                author: did.clone(),
                collection: k,
                stamp: stamp.rev,
                after: prev_last.as_deref().and_then(|p| RecordKey::parse(p).ok()),
                through,
                keep: keys,
            });
        }
        let report = apply_through(pool, &actx, &batch).await?;
        crate::metrics::count_refusals(&report);
        out.refused += report.refused;
        if next.is_none()
            && let Mode::Seen(seen) = &mode
        {
            // The seen-set listing reconciles the whole range at the end.
            reconcile_unseen(ctx, policy, did, k, stamp, seen, &hasher).await?;
        }
        // Persist (cursor, prev_last, R, stamp_read_at) for this run.
        if let Some(id) = jobs::actor_id(pool, did.as_str()).await? {
            sqlx::query(
                "INSERT INTO backfill_cursors (actor_id, collection, job_kind, run_id, stamp_rev,
                    stamp_read_at, late_stamp, cursor, prev_last, unreconciled)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 ON CONFLICT (actor_id, collection, job_kind, run_id) DO UPDATE SET
                   cursor = EXCLUDED.cursor, prev_last = EXCLUDED.prev_last,
                   unreconciled = EXCLUDED.unreconciled",
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
            .bind(resumes_unreconciled(&mode, next.is_some()))
            .execute(pool)
            .await?;
            jobs::renew_lease(pool, did.as_str(), &ctx.lease_owner()).await?;
        }
        match next {
            Some(n) => {
                cursor = Some(n);
                prev_last = last;
            }
            None => return Ok(out),
        }
        // The page is stored and its cursor with it: a host too slow to
        // go on with costs this attempt, not what it has listed.
        if let (Some(from), Some(now)) = (waited_at_start, crate::net::waited_ms())
            && too_slow(pages, now.saturating_sub(from))
        {
            return Err(Stop::Failed(format!(
                "the host took {} s to answer {pages} pages",
                now.saturating_sub(from) / 1000
            )));
        }
    }
}

/// The end of a seen-set listing: removes the author's stored rows of
/// `k` (those below the stamp) whose key the listing did not see. The
/// stored keys are read [`SEEN_CHUNK`] at a time and each chunk is
/// reconciled over its own key range, keeping the keys that are in the
/// set. Two keys with one hash would keep a row the repo no longer has;
/// the hash is keyed anew for every listing.
async fn reconcile_unseen(
    ctx: &Ctx,
    policy: GatePolicy,
    did: &Did,
    k: Collection,
    stamp: ListingStamp,
    seen: &HashSet<u64>,
    hasher: &RandomState,
) -> Result<(), Stop> {
    let pool = &ctx.pool;
    let Some(id) = jobs::actor_id(pool, did.as_str()).await? else {
        return Ok(());
    };
    let sql = match k {
        Collection::Block => {
            "SELECT rkey FROM blocks WHERE author_id = $1 AND rkey > $2 ORDER BY rkey LIMIT $3"
                .to_owned()
        }
        Collection::ListBlock => {
            "SELECT rkey FROM list_blocks WHERE author_id = $1 AND rkey > $2 ORDER BY rkey LIMIT $3"
                .to_owned()
        }
        Collection::ListItem => {
            "SELECT rkey FROM list_items WHERE owner_id = $1 AND rkey > $2 ORDER BY rkey LIMIT $3"
                .to_owned()
        }
        Collection::List => format!(
            "SELECT rkey FROM lists WHERE owner_id = $1 AND record_state = {RECORD_PRESENT}
               AND rkey > $2 ORDER BY rkey LIMIT $3"
        ),
    };
    let limits = ctx.limits();
    let mut after = String::new();
    loop {
        let stored: Vec<String> = sqlx::query_scalar(&sql)
            .bind(id)
            .bind(&after)
            .bind(SEEN_CHUNK)
            .fetch_all(pool)
            .await?;
        let Some(last) = stored.last().cloned() else {
            return Ok(());
        };
        let done = (stored.len() as i64) < SEEN_CHUNK;
        let gates = ctx.gates.load();
        let actx = ApplyCtx {
            limits: &limits,
            gates: policy.apply_gates(gates),
            counters: &ctx.counters,
        };
        let mut b = Batch::new(Origin::Listing {
            stamp_read_at: stamp.read_at,
            deletes_only: policy.deletes_only(gates),
            host_confirmed: policy.host_confirmed,
        });
        b.reconciles.push(Reconcile {
            author: did.clone(),
            collection: k,
            stamp: stamp.rev,
            after: RecordKey::parse(&after).ok(),
            through: if done {
                None
            } else {
                RecordKey::parse(&last).ok()
            },
            keep: stored
                .iter()
                .filter(|rk| seen.contains(&hasher.hash_one(rk.as_str())))
                .filter_map(|rk| RecordKey::parse(rk).ok())
                .collect(),
        });
        apply_through(pool, &actx, &b).await?;
        if done {
            return Ok(());
        }
        after = last;
    }
}

/// Records the divergence of `did` (the divergence check): a `resync`
/// debt first, so that coverage says so for as long as rows are going;
/// then **DV** on the DID's tracked lists and the purge of what it
/// authored under the discarded history. The purge takes rows whose rev
/// is below the stamp of this moment: what the firehose writes while it
/// runs carries later revs and stays.
pub async fn diverged(ctx: &Ctx, did: &Did, witness: DateTime<Utc>) -> Result<(), Stop> {
    let pool = &ctx.pool;
    let limits = ctx.limits();
    let Some(id) = jobs::actor_id(pool, did.as_str()).await? else {
        return Ok(());
    };
    farsight_storage::debts::add_debt(pool, id, DebtReason::Resync, None, witness).await?;
    let now = jobs::db_now(pool).await?;
    let below = u64::try_from(now.timestamp_micros())
        .ok()
        .and_then(|us| Tid::from_parts(us, 0))
        .map(Stamp::from_tid)
        .ok_or_else(|| Stop::Failed("the clock is outside the range of a rev".into()))?;
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
        below,
    )
    .await?
    {}
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
        time: None,
        active,
        status,
    });
    // An account that became `deleted` is purged by the server: the
    // transaction that records the status asks for it.
    apply::apply(&ctx.pool, &actx, &b).await?;
    Ok(())
}

/// What the relay's `getRepoStatus` says of an account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayVerdict {
    /// Inactive with a status that hides the account's rows.
    Hidden(Option<String>),
    /// Inactive with a status that hides nothing (`throttled`,
    /// `desynchronized`, anything unknown).
    Shown(Option<String>),
    /// The relay stated that the account is active.
    Active,
    /// The relay gave no answer to act on: an error, a cooling host, a
    /// timeout, or a response that does not say whether the account is
    /// active.
    Unknown(String),
}

/// Reads the relay's answer. Only an answer is a verdict: an error says
/// nothing about the account, so it neither confirms nor lifts a status.
pub fn relay_verdict_of(answer: &Result<xrpc::RepoStatus, crate::net::NetError>) -> RelayVerdict {
    match answer {
        Err(e) => RelayVerdict::Unknown(e.to_string()),
        Ok(s) if !s.stated => RelayVerdict::Unknown("the relay's answer has no `active`".into()),
        Ok(s) if s.active => RelayVerdict::Active,
        Ok(s) => {
            let hidden = s
                .status
                .as_deref()
                .is_some_and(|st| ActorStatus::from_upstream(Some(st)).is_hidden());
            if hidden {
                RelayVerdict::Hidden(s.status.clone())
            } else {
                RelayVerdict::Shown(s.status.clone())
            }
        }
    }
}

/// Asks the relay for the account's status (one request, added to
/// `cost`).
async fn relay_verdict(ctx: &Ctx, did: &Did, cost: &mut u64) -> RelayVerdict {
    let relay = ctx.cfg().backfill.relay_url.clone();
    *cost += 1;
    relay_verdict_of(&xrpc::repo_status(&ctx.net, &relay, did).await)
}

/// Records a hidden status the relay reported. A failure to record it is
/// logged; the job's outcome is **inactive** either way.
async fn record_hidden(ctx: &Ctx, did: &Did, status: Option<String>) {
    if let Err(e) = apply_status(ctx, did, false, status).await {
        tracing::warn!(did = %did, error = ?e, "recording relay status failed");
    }
}

/// The run an attempt belongs to: a new one, or the DID's unfinished run
/// taken up again.
#[derive(Debug, Clone, Copy)]
struct Run {
    /// `backfill_state.current_run_id`: the key of the run's cursor rows.
    id: RepoRunId,
    /// The listing stamp of the run, when it is resumed.
    stamp: Option<ListingStamp>,
    /// The run's coverage point: `clock` of its first attempt's start. A
    /// later attempt keeps it, since the pages the earlier ones read are
    /// no newer than that.
    point: Option<DateTime<Utc>>,
    /// Server time of the first attempt's start (cycle membership).
    started: DateTime<Utc>,
}

/// The DID's own run, if it has one whose stamp is fresh (< 72 h) and
/// whose coverage point is known; a new run otherwise, starting at `now`
/// with the point `point`.
async fn pick_run(
    pool: &PgPool,
    did: &Did,
    now: DateTime<Utc>,
    point: Option<DateTime<Utc>>,
) -> Result<Run, sqlx::Error> {
    type Resumed = (
        RepoRunId,
        Stamp,
        DateTime<Utc>,
        bool,
        DateTime<Utc>,
        DateTime<Utc>,
    );
    let resume: Option<Resumed> = sqlx::query_as(&format!(
        "SELECT s.current_run_id, c.stamp_rev, c.stamp_read_at, c.late_stamp,
                s.current_run_point, s.current_run_started_at
         FROM backfill_state s JOIN actors a ON a.id = s.actor_id
         JOIN backfill_cursors c ON c.actor_id = s.actor_id AND c.job_kind = {JOB_REPO}
              AND c.run_id = s.current_run_id
         WHERE a.did = $1 AND c.stamp_read_at > now() - interval '72 hours'
           AND s.current_run_point IS NOT NULL AND s.current_run_started_at IS NOT NULL
         LIMIT 1"
    ))
    .bind(did.as_str())
    .fetch_optional(pool)
    .await?;
    Ok(match resume {
        Some((id, rev, read_at, late, run_point, started)) => Run {
            id,
            stamp: Some(ListingStamp { rev, read_at, late }),
            point: Some(run_point),
            started,
        },
        None => {
            let mut b = [0u8; 8];
            let _ = getrandom::getrandom(&mut b);
            Run {
                id: RepoRunId::new(i64::from_le_bytes(b) & i64::MAX),
                stamp: None,
                point,
                started: now,
            }
        }
    })
}

/// Notes on the DID's state row which run is under way, so that its next
/// attempt takes it up again.
async fn note_run(
    pool: &PgPool,
    id: farsight_storage::ids::ActorId,
    run: &Run,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "INSERT INTO backfill_state (actor_id, state, current_run_id, current_run_point,
                                     current_run_started_at)
         VALUES ($1, {REPO_RUNNING}, $2, $3, $4)
         ON CONFLICT (actor_id) DO UPDATE SET state = {REPO_RUNNING}, current_run_id = $2,
           current_run_point = $3, current_run_started_at = $4"
    ))
    .bind(id)
    .bind(run.id)
    .bind(run.point)
    .bind(run.started)
    .execute(pool)
    .await?;
    Ok(())
}

/// Runs a `repo` job for `req.did`. The caller holds the lease.
///
/// One attempt runs for at most `backfill.repo_job_max_duration` and
/// lists at most [`MAX_PAGES`] pages; at either bound it stops where it
/// is and is queued to go on ([`Outcome::Yielded`]). A run that stops at
/// a bound more than [`MAX_YIELDS`] times in a row has failed.
pub async fn run(ctx: &Ctx, req: &JobReq) -> JobResult {
    let mut cost = 0u64;
    let failed = |error: String, cost: u64| JobResult {
        outcome: Outcome::Failed {
            error,
            terminal: false,
        },
        cost,
    };
    // An account whose host has a queue of its own is not listed now.
    // The scheduler leaves such an account where it waits, but only when
    // it knows the host, and a sweep member met for the first time is
    // resolved here. Listed all the same, it would stand in the host's
    // queue and hold its worker for as long: enough of them hold every
    // worker, and the other hosts get none. So the job ends as busy,
    // with the host on the account's row, and the account is taken up
    // again when the host takes work.
    //
    // Only a queue does this. A host that is cooling down refuses the
    // job's first request, the job fails and is retried on its schedule:
    // that is how an account on a host that stays down becomes terminal.
    // The resolution is handed to the attempt, which resolves once.
    let first = ctx.resolver.resolve_noting(&req.did, false).await;
    if let Ok((pds, _)) = &first
        && ctx.net.has_queue(&pds.host)
    {
        cost += 1;
        let noted = async {
            jobs::intern(ctx, &req.did).await?;
            ctx.resolver.note_host(&req.did, pds).await
        };
        if let Err(e) = noted.await {
            tracing::warn!(did = %req.did, error = %e, "recording the host of an account left for later failed");
        }
        return JobResult {
            outcome: Outcome::Busy,
            cost,
        };
    }
    let job_start = match jobs::db_now(&ctx.pool).await {
        Ok(t) => t,
        Err(e) => return failed(e.to_string(), cost),
    };
    let now_point = farsight_storage::firehose::clock(&ctx.pool, job_start)
        .await
        .ok()
        .flatten();
    let run = match pick_run(&ctx.pool, &req.did, job_start, now_point).await {
        Ok(r) => r,
        Err(e) => return failed(e.to_string(), cost),
    };
    let max = ctx.cfg().backfill.repo_job_max_duration.get();
    let attempted = match tokio::time::timeout(max, attempt(ctx, req, &run, &mut cost, first)).await
    {
        Ok(r) => r,
        // Out of time: what was listed is stored and its cursor with it.
        Err(_) => Err((Stop::Yield, None)),
    };
    let (outcome, stamp) = match attempted {
        Ok((o, s)) => (o, s),
        Err((Stop::Yield, s)) => match yielded(ctx, req, &run).await {
            Ok(None) => {
                return JobResult {
                    outcome: Outcome::Yielded,
                    cost,
                };
            }
            Ok(Some(n)) => (
                Outcome::Failed {
                    error: format!("stopped at a bound {n} times in a row without finishing"),
                    terminal: false,
                },
                s,
            ),
            Err(e) => (
                Outcome::Failed {
                    error: format!("queueing the rest of the job: {e}"),
                    terminal: false,
                },
                s,
            ),
        },
        // The repo-level error rule after re-resolution failed to help:
        // the relay decides between **inactive** and **failed**.
        Err((Stop::RepoLevel(e), s)) => {
            let o = match relay_verdict(ctx, &req.did, &mut cost).await {
                RelayVerdict::Hidden(status) => {
                    record_hidden(ctx, &req.did, status).await;
                    Outcome::Inactive
                }
                _ => Outcome::Failed {
                    error: e.to_string(),
                    terminal: false,
                },
            };
            (o, s)
        }
        Err((Stop::Failed(e), s)) => (
            Outcome::Failed {
                error: e,
                terminal: false,
            },
            s,
        ),
        Err((Stop::Unconfirmed, s)) => (
            Outcome::Failed {
                error: "the account's host could not be confirmed".into(),
                terminal: false,
            },
            s,
        ),
    };
    let f = Finish {
        req,
        point: run.point,
        job_start: run.started,
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

/// An attempt stopped at a bound: counts it on the run and queues the
/// job to go on, after [`yield_delay`]. `Ok(Some(n))` when this was the
/// `n`-th in a row and `n` is past [`MAX_YIELDS`]: nothing is queued and
/// the caller records a failure.
async fn yielded(
    ctx: &Ctx,
    req: &JobReq,
    run: &Run,
) -> Result<Option<i32>, farsight_storage::StorageError> {
    let id = jobs::intern(ctx, &req.did).await?;
    note_run(&ctx.pool, id, run).await?;
    let n: i32 = sqlx::query_scalar(
        "UPDATE backfill_state SET yields = yields + 1 WHERE actor_id = $1 RETURNING yields",
    )
    .bind(id)
    .fetch_one(&ctx.pool)
    .await?;
    if n > MAX_YIELDS {
        return Ok(Some(n));
    }
    let mut conn = ctx.pool.acquire().await?;
    farsight_storage::queue::enqueue(
        &mut conn,
        id,
        JobKind::Repo,
        req.tier,
        Priority::Normal,
        req.requester,
        None,
        Some(yield_delay(n)),
    )
    .await?;
    Ok(None)
}

type AttemptErr = (Stop, Option<Stamp>);

async fn attempt(
    ctx: &Ctx,
    req: &JobReq,
    run: &Run,
    cost: &mut u64,
    first: Result<(Pds, bool), ResolveError>,
) -> Result<(Outcome, Option<Stamp>), AttemptErr> {
    let pool = &ctx.pool;
    let did = &req.did;
    let e = |s: Stop| (s, None);
    // An actor already known as hidden: the relay confirms (inactive) or
    // contradicts it (the status is updated and the repo listed). Only
    // the relay's own answer lifts the status. Without one the account
    // stays hidden and the job fails, to be tried again.
    let status: Option<ActorStatus> =
        sqlx::query_scalar("SELECT status FROM actors WHERE did = $1")
            .bind(did.as_str())
            .fetch_optional(pool)
            .await
            .map_err(|x| e(x.into()))?;
    if status.is_some_and(ActorStatus::is_hidden) {
        match relay_verdict(ctx, did, cost).await {
            RelayVerdict::Hidden(status) => {
                record_hidden(ctx, did, status).await;
                return Ok((Outcome::Inactive, None));
            }
            RelayVerdict::Active => apply_status(ctx, did, true, None).await.map_err(e)?,
            RelayVerdict::Shown(status) => {
                apply_status(ctx, did, false, status).await.map_err(e)?
            }
            RelayVerdict::Unknown(why) => {
                return Err(e(Stop::Failed(format!(
                    "relay status of a hidden account: {why}"
                ))));
            }
        }
    }
    let mut stamp = run.stamp;
    let resumed = stamp.is_some();
    if let Some(id) = jobs::actor_id(pool, did.as_str())
        .await
        .map_err(|x| e(x.into()))?
    {
        note_run(pool, id, run).await.map_err(|x| e(x.into()))?;
    }
    let mut budget = PageBudget::new();
    // 1. Resolve (re-resolve bypassing the cache on a repo-level error,
    // and before a divergence is acted on).
    let mut bypass = false;
    // The job resolved the account before this attempt: the first turn
    // uses that answer, cached or not as it was.
    let mut first = Some(first);
    loop {
        let resolved = match first.take() {
            Some(r) if !bypass => r,
            _ => ctx.resolver.resolve_noting(did, bypass).await,
        };
        let (pds, cached) = match resolved {
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
        let listed = list_repo(
            ctx,
            req,
            &pds,
            &mut stamp,
            Attempt {
                resumed,
                confirmed: !cached,
                run,
            },
            &mut budget,
            cost,
        )
        .await;
        match listed {
            Ok(o) => return Ok((o, stamp.map(|s| s.rev))),
            Err(Stop::RepoLevel(err)) if !bypass => {
                // Re-resolve bypassing the cache; if the PDS changed,
                // retry there.
                bypass = true;
                match ctx.resolver.resolve(did, true).await {
                    Ok(p2) if p2 != pds => {
                        // A stamp read at the old host says nothing of
                        // the repo at the new one.
                        if !resumed {
                            stamp = None;
                        }
                        continue;
                    }
                    _ => return Err((Stop::RepoLevel(err), stamp.map(|s| s.rev))),
                }
            }
            Err(Stop::Unconfirmed) if !bypass => {
                bypass = true;
                if !resumed {
                    stamp = None;
                }
            }
            Err(s) => return Err((s, stamp.map(|s| s.rev))),
        }
    }
}

/// What `list_repo` needs to know of the attempt it runs in.
#[derive(Clone, Copy)]
struct Attempt<'a> {
    /// The run is taken up again with its stamp.
    resumed: bool,
    /// The host was read from the directory for this attempt, not from a
    /// cache.
    confirmed: bool,
    run: &'a Run,
}

#[allow(clippy::too_many_arguments)]
async fn list_repo(
    ctx: &Ctx,
    req: &JobReq,
    pds: &Pds,
    stamp: &mut Option<ListingStamp>,
    at: Attempt<'_>,
    budget: &mut PageBudget,
    cost: &mut u64,
) -> Result<Outcome, Stop> {
    let (resumed, point, run_id) = (at.resumed, at.run.point, at.run.id);
    let pool = &ctx.pool;
    let did = &req.did;
    let policy = GatePolicy::new(req, host_is_large(ctx, pds)).on_host(at.confirmed);
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
            // A host out of a cache may be one the account has left, and
            // what it still serves is then the past, not a divergence.
            if !at.confirmed {
                return Err(Stop::Unconfirmed);
            }
            tracing::warn!(did = %did, "repo went backwards (divergence); purging and re-listing");
            let witness = point.unwrap_or(s.read_at);
            diverged(ctx, did, witness).await?;
        }
    }
    let Some(stamp) = *stamp else {
        // Nothing present and nothing held: an empty repo (one describeRepo).
        return Ok(Outcome::Clean);
    };
    // A sweep member holds no row, and its resolution is in memory alone.
    // The row its first stored record makes would know no host, and what
    // is listed here would be charged to `unresolved` and stay there. So
    // the row is made before anything is stored, with the host on it.
    if !present.is_empty() {
        jobs::intern(ctx, did).await?;
    }
    ctx.resolver.note_host(did, pds).await?;
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
            host_confirmed: policy.host_confirmed,
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
        apply_through(pool, &actx, &b).await?;
    }
    // 6. List each present collection.
    let mut refused = 0u64;
    let mut skipped = false;
    for k in &present {
        let l = list_collection(
            ctx,
            pds,
            did,
            *k,
            stamp,
            CursorRun::Repo(run_id),
            policy,
            budget,
        )
        .await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_is_judged_slow_only_after_enough_pages() {
        // Four pages at half a minute each: not judged yet.
        assert!(!too_slow(4, 120_000));
        // Five at just over ten seconds each: too slow.
        assert!(too_slow(5, 50_001));
        assert!(!too_slow(5, 50_000));
        // A long listing at one second a page is fine.
        assert!(!too_slow(20_000, 20_000_000));
    }

    #[test]
    fn a_listing_that_cannot_be_reconciled_says_so_to_the_attempt_that_resumes_it() {
        assert!(!resumes_unreconciled(&Mode::Ordered, true));
        assert!(!resumes_unreconciled(&Mode::Ordered, false));
        // Out of order with pages to go: the set ends with the attempt.
        assert!(resumes_unreconciled(&Mode::Seen(HashSet::new()), true));
        // Its last page was reconciled against the set.
        assert!(!resumes_unreconciled(&Mode::Seen(HashSet::new()), false));
        assert!(resumes_unreconciled(&Mode::Unreconciled, true));
        assert!(resumes_unreconciled(&Mode::Unreconciled, false));
    }

    #[test]
    fn a_listed_key_is_kept_only_with_a_record_to_index() {
        let did = Did::parse("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let block = serde_json::json!({
            "subject": "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
            "createdAt": "2026-01-01T00:00:00Z"
        });
        assert!(matches!(
            listed_record(&did, Collection::Block, Some(&block)),
            Some(Record::Block(_))
        ));
        // A value that is not a block, and a value whose text could not
        // be parsed at all: the key holds nothing to store.
        let not_a_block = serde_json::json!({"subject": "not a DID"});
        assert_eq!(
            listed_record(&did, Collection::Block, Some(&not_a_block)),
            None
        );
        assert_eq!(listed_record(&did, Collection::Block, None), None);
    }

    fn answer(active: bool, stated: bool, status: Option<&str>) -> xrpc::RepoStatus {
        xrpc::RepoStatus {
            active,
            stated,
            status: status.map(str::to_owned),
        }
    }

    #[test]
    fn a_job_that_keeps_stopping_at_a_bound_waits_longer_each_time() {
        assert_eq!(yield_delay(1), Duration::ZERO);
        assert_eq!(yield_delay(2), Duration::ZERO);
        assert_eq!(yield_delay(3), Duration::from_secs(30));
        assert_eq!(yield_delay(4), Duration::from_secs(60));
        assert_eq!(yield_delay(9), Duration::from_secs(1920));
        // Never more than an hour, whatever the count.
        for n in [10, MAX_YIELDS, 50, i32::MAX] {
            assert_eq!(yield_delay(n), Duration::from_secs(3600), "{n}");
        }
        for n in 1..=MAX_YIELDS {
            assert!(yield_delay(n) <= yield_delay(n + 1));
        }
        assert_eq!(yield_delay(0), Duration::ZERO);
        assert_eq!(yield_delay(-3), Duration::ZERO);
    }

    #[test]
    fn an_attempt_lists_no_more_pages_than_its_budget() {
        let mut b = PageBudget(3);
        assert!(b.take() && b.take() && b.take());
        assert!(!b.take());
        assert!(!b.take());
        assert_eq!(PageBudget::new().0, MAX_PAGES);
    }

    #[test]
    fn a_cursor_that_comes_back_is_told_whatever_the_cycle() {
        // The same cursor again.
        let mut c = Cursors::default();
        assert!(c.follow(None, "a"));
        assert!(!c.follow(Some("a"), "a"));
        // A → B → A.
        let mut c = Cursors::default();
        assert!(c.follow(None, "a"));
        assert!(c.follow(Some("a"), "b"));
        assert!(!c.follow(Some("b"), "a"));
        // A cycle of any length, entered after a lead-in.
        for len in [2usize, 3, 7, 100] {
            let mut c = Cursors::default();
            let mut current: Option<String> = None;
            let mut pages = 0;
            let next_of = |page: usize| -> String {
                if page < 5 {
                    format!("lead{page}")
                } else {
                    format!("cycle{}", (page - 5) % len)
                }
            };
            loop {
                let next = next_of(pages);
                pages += 1;
                if !c.follow(current.as_deref(), &next) {
                    break;
                }
                current = Some(next);
                assert!(pages < 1_000, "a cycle of {len} was followed without end");
            }
            assert_eq!(pages, 5 + len + 1, "cycle of {len}");
        }
        // A resumed listing knows the cursor it starts from.
        let mut c = Cursors::default();
        assert!(c.follow(Some("resume"), "x"));
        assert!(!c.follow(Some("x"), "resume"));
        // After a restart from the first page the same cursors are new.
        c.clear();
        assert!(c.follow(None, "x"));
    }

    #[test]
    fn only_the_relays_own_answer_is_a_verdict() {
        // Every way of not answering: none of them lifts a hidden status.
        let errors = [
            NetError::Http {
                status: 500,
                name: String::new(),
            },
            NetError::Http {
                status: 429,
                name: "RateLimitExceeded".into(),
            },
            NetError::Http {
                status: 400,
                name: "RepoNotFound".into(),
            },
            NetError::Cooling {
                host: "relay.example".into(),
                secs: 30,
            },
            NetError::Transport("timed out".into()),
            NetError::Decode("not JSON".into()),
        ];
        for e in errors {
            assert!(
                matches!(relay_verdict_of(&Err(e.clone())), RelayVerdict::Unknown(_)),
                "{e}"
            );
        }
        // An answer that does not say whether the account is active.
        assert!(matches!(
            relay_verdict_of(&Ok(answer(true, false, None))),
            RelayVerdict::Unknown(_)
        ));
        assert_eq!(
            relay_verdict_of(&Ok(answer(true, true, None))),
            RelayVerdict::Active
        );
        for status in ["deactivated", "takendown", "suspended", "deleted"] {
            assert_eq!(
                relay_verdict_of(&Ok(answer(false, true, Some(status)))),
                RelayVerdict::Hidden(Some(status.to_owned()))
            );
        }
        for status in [Some("throttled"), Some("desynchronized"), Some("new"), None] {
            assert_eq!(
                relay_verdict_of(&Ok(answer(false, true, status))),
                RelayVerdict::Shown(status.map(str::to_owned))
            );
        }
    }
}
