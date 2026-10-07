//! The scheduler (see `docs/design/backfill.md`): a worker pool of
//! `backfill.concurrency`, three tiers whose shares are minimums (unused
//! share flows to the others), cost-based deficit round-robin across tier-1
//! requesters charged in outbound requests, `high` before `normal` 4:1
//! within a requester, and the list-job lanes inside `system:lists`.
//!
//! A job is charged when it is dispatched, not when it ends: one request
//! ([`DISPATCH_CHARGE`]) at once, the rest of its cost when it finishes.
//! Charged only at the end, a requester would look cheapest for as long as
//! its jobs ran and take every free worker in one go.
//!
//! Each job runs in its own task. Its worker slot and its in-flight
//! markers are held by a guard and given back when the task ends, however
//! it ends: a job that panics is logged, counted and costs its slot
//! nothing.

use farsight_storage::codes::sql::{
    CYCLE_REPAIR, JOB_LIST_FETCH, JOB_REPO, MEMBER_OUTSTANDING, TIER_ACTIVE, TIER_ON_DEMAND,
    TIER_SWEEP, TRACK_MISSING,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use std::hash::Hash;

use farsight_core::Did;
use farsight_core::bucket::{Bucket, Rate};
use farsight_storage::codes::{CycleKind, JobKind, Priority, RequesterKey, Tier};
use farsight_storage::ids::{ActorId, QueueId};
use sqlx::PgPool;
use tokio::sync::{Notify, watch};

use crate::ctx::Ctx;
use crate::jobs::{self, JobReq, JobResult, Outcome};
use crate::lanes::{self, Item, Lanes};
use crate::metrics as m;

/// High-priority picks per normal pick within a requester.
pub const HIGH_PER_NORMAL: u32 = 4;
/// What a job is charged when it is dispatched, in outbound requests: the
/// least a job costs. The remainder follows when it finishes.
pub const DISPATCH_CHARGE: u64 = 1;
/// The task name panics of jobs are counted under.
pub const JOB_TASK: &str = "backfill_job";
/// What the tier-3 pacing counts as elapsed on its first use: the bucket
/// starts empty, so the first sweep job is dispatched at once only at a
/// rate of one a second or more.
pub const TIER3_HEAD_START: Duration = Duration::from_secs(1);

/// The tier-3 rate for `per_hour` repos an hour: the bucket has room for
/// one second of it, and for one job at least.
fn tier3_rate(per_hour: u64) -> Rate {
    let per_sec = per_hour as f64 / 3600.0;
    Rate {
        per_sec,
        burst: per_sec.max(1.0),
    }
}

/// Charges `requester` for a job being dispatched.
pub fn charge_dispatch<K: Eq + Hash>(charged: &mut HashMap<K, f64>, requester: K) {
    *charged.entry(requester).or_insert(0.0) += DISPATCH_CHARGE as f64;
}

/// What a finished job of cost `cost` still owes after its dispatch
/// charge.
pub fn remaining_charge(cost: u64) -> u64 {
    cost.max(DISPATCH_CHARGE) - DISPATCH_CHARGE
}

/// Charges `requester` the rest of a finished job's cost.
pub fn charge_finish<K: Eq + Hash>(charged: &mut HashMap<K, f64>, requester: K, cost: u64) {
    *charged.entry(requester).or_insert(0.0) += remaining_charge(cost) as f64;
}

/// Picks the tier to serve: among tiers with work, the one furthest below
/// its guaranteed share (running / share), so every share is a minimum and
/// unused share flows to the others.
pub fn pick_tier(running: [usize; 3], shares: [u32; 3], has_work: [bool; 3]) -> Option<Tier> {
    Tier::ALL
        .iter()
        .copied()
        .filter(|t| has_work[t.index()])
        .min_by(|a, b| {
            let (a, b) = (a.index(), b.index());
            let ra = running[a] as f64 / f64::from(shares[a].max(1));
            let rb = running[b] as f64 / f64::from(shares[b].max(1));
            ra.partial_cmp(&rb)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(&b))
        })
}

/// Cost-based DRR across requesters: the least-charged requester with work
/// goes next; a requester new to the round starts at the current minimum.
pub fn pick_requester<K: Clone + Eq + Hash + Ord>(
    charged: &mut HashMap<K, f64>,
    with_work: &[K],
) -> Option<K> {
    let floor = with_work
        .iter()
        .filter_map(|r| charged.get(r))
        .copied()
        .fold(f64::INFINITY, f64::min);
    let floor = if floor.is_finite() { floor } else { 0.0 };
    for r in with_work {
        charged.entry(r.clone()).or_insert(floor);
    }
    with_work
        .iter()
        .min_by(|a, b| {
            charged[*a]
                .partial_cmp(&charged[*b])
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(b))
        })
        .cloned()
}

/// Whether the next pick of a requester with both kinds waiting should be
/// `high`: four highs, then one normal.
pub fn want_high(streak: u32) -> bool {
    streak < HIGH_PER_NORMAL
}

/// One unit of work.
#[derive(Debug, Clone)]
enum Work {
    Queue {
        actor_id: ActorId,
        did: String,
        kind: JobKind,
        tier: Tier,
        priority: Priority,
        requester: RequesterKey,
    },
    List {
        cand: Box<lanes::Candidate>,
        lane: String,
    },
    Member {
        did: String,
        requester: RequesterKey,
    },
}

#[derive(Default)]
struct State {
    running: [usize; 3],
    charged: HashMap<RequesterKey, f64>,
    streak: HashMap<RequesterKey, u32>,
    inflight_dids: HashSet<String>,
    inflight_items: HashSet<Item>,
    lanes: Lanes,
    member_cursor: Option<String>,
    completions: VecDeque<Instant>,
    tier3: Option<Bucket>,
}

impl Work {
    /// The tier-1 requester the job is charged to, as far as it is known
    /// before the job runs.
    fn requester(&self) -> RequesterKey {
        match self {
            Work::Queue { requester, .. } | Work::Member { requester, .. } => *requester,
            Work::List { .. } => RequesterKey::Lists,
        }
    }
}

/// A dispatched job's hold on the pool: one worker of its tier and its
/// in-flight markers. Dropping it gives them back and wakes the
/// dispatcher, so they are returned when the job's task ends for any
/// reason, a panic included.
struct Running {
    sched: Arc<Scheduler>,
    tier: Tier,
    work: Work,
}

impl Drop for Running {
    fn drop(&mut self) {
        {
            let mut s = self.sched.st();
            let i = self.tier.index();
            s.running[i] = s.running[i].saturating_sub(1);
            match &self.work {
                Work::Queue { did, .. } | Work::Member { did, .. } => {
                    s.inflight_dids.remove(did);
                }
                Work::List { cand, .. } => {
                    s.inflight_items.remove(&cand.item);
                    if let Item::Fetch { owner, .. } = &cand.item {
                        s.inflight_dids.remove(owner);
                    }
                }
            }
        }
        self.sched.freed.notify_waiters();
    }
}

/// The scheduler.
pub struct Scheduler {
    ctx: Arc<Ctx>,
    st: Mutex<State>,
    freed: Notify,
    /// Harness: run every job as a no-op that records its pick time.
    #[cfg(feature = "harness")]
    dry: bool,
    /// Harness: a job for one of these DIDs panics when it starts.
    #[cfg(feature = "harness")]
    panic_dids: Mutex<HashSet<String>>,
}

type QueueRow = (
    QueueId,
    ActorId,
    String,
    JobKind,
    Tier,
    Priority,
    RequesterKey,
);

impl Scheduler {
    /// A scheduler over `ctx`.
    pub fn new(ctx: Arc<Ctx>) -> Arc<Scheduler> {
        Arc::new(Scheduler {
            ctx,
            st: Mutex::new(State::default()),
            freed: Notify::new(),
            #[cfg(feature = "harness")]
            dry: false,
            #[cfg(feature = "harness")]
            panic_dids: Mutex::default(),
        })
    }

    /// Harness: a scheduler whose jobs only record their pick.
    #[cfg(feature = "harness")]
    pub fn new_dry(ctx: Arc<Ctx>) -> Arc<Scheduler> {
        Arc::new(Scheduler {
            ctx,
            st: Mutex::new(State::default()),
            freed: Notify::new(),
            dry: true,
            panic_dids: Mutex::default(),
        })
    }

    /// Harness: makes jobs for `did` panic (`on`), or stops doing so.
    #[cfg(feature = "harness")]
    pub fn panic_for(&self, did: &str, on: bool) {
        let mut set = self.panic_dids.lock().unwrap_or_else(|e| e.into_inner());
        if on {
            set.insert(did.to_owned());
        } else {
            set.remove(did);
        }
    }

    fn st(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Repos completed in the trailing hour.
    pub fn repos_last_hour(&self) -> usize {
        let mut s = self.st();
        let hour = Duration::from_secs(3600);
        while s.completions.front().is_some_and(|t| t.elapsed() > hour) {
            s.completions.pop_front();
        }
        s.completions.len()
    }

    /// Whether tier-3 dispatch is paused: storage ≥ 90%.
    fn tier3_paused(&self) -> bool {
        self.ctx.sweep_paused_by_storage()
    }

    /// Tier-3 pacing (`backfill.sweep.max_repos_per_hour`, 0 = unbounded).
    fn tier3_token(&self) -> bool {
        let per_hour = self.ctx.cfg().backfill.sweep.max_repos_per_hour;
        if per_hour == 0 {
            return true;
        }
        self.st()
            .tier3
            .get_or_insert(Bucket::unused(0.0, TIER3_HEAD_START))
            .take(Instant::now(), tier3_rate(per_hour))
    }

    async fn has_work(&self, pool: &PgPool) -> Result<[bool; 3], sqlx::Error> {
        let (t1, t2, t3): (bool, bool, bool) = sqlx::query_as(
            &format!("SELECT EXISTS (SELECT 1 FROM backfill_queue WHERE tier = {TIER_ON_DEMAND}
                              AND (not_before IS NULL OR not_before <= now())),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE tier = {TIER_ACTIVE}
                              AND (not_before IS NULL OR not_before <= now())),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE tier = {TIER_SWEEP}
                              AND (not_before IS NULL OR not_before <= now()))
                    OR EXISTS (SELECT 1 FROM cycle_outstanding o JOIN sweep_cycles c ON c.id = o.cycle_id
                               WHERE o.state = {MEMBER_OUTSTANDING} AND c.completed_at IS NULL)"),
        )
        .fetch_one(pool)
        .await?;
        let lists: bool = sqlx::query_scalar(
            &format!("SELECT EXISTS (SELECT 1 FROM list_jobs WHERE not_before IS NULL OR not_before <= now())
                 OR EXISTS (SELECT 1 FROM lists WHERE track_state = {TRACK_MISSING} AND next_retry_at <= now())"),
        )
        .fetch_one(pool)
        .await?;
        Ok([t1 || lists, t2, t3 && !self.tier3_paused()])
    }

    async fn claim_queue(
        &self,
        sql_filter: &str,
        args: (Tier, Option<RequesterKey>, Option<Priority>),
    ) -> Result<Option<Work>, sqlx::Error> {
        let inflight: Vec<String> = self.st().inflight_dids.iter().cloned().collect();
        let sql = format!(
            "DELETE FROM backfill_queue WHERE id = (
               SELECT q.id FROM backfill_queue q JOIN actors a ON a.id = q.actor_id
               WHERE q.tier = $1 AND (q.not_before IS NULL OR q.not_before <= now())
                 AND a.did <> ALL($2) {sql_filter}
               ORDER BY {} q.enqueued_at, q.id LIMIT 1 FOR UPDATE SKIP LOCKED)
             RETURNING id, actor_id,
               (SELECT did FROM actors WHERE id = backfill_queue.actor_id), kind, tier, priority, requester",
            if args.2.is_some() { "(q.priority = $4) DESC," } else { "" }
        );
        let row: Option<QueueRow> = sqlx::query_as(&sql)
            .bind(args.0)
            .bind(&inflight)
            .bind(args.1)
            .bind(args.2)
            .fetch_optional(&self.ctx.pool)
            .await?;
        Ok(row.map(
            |(_, actor_id, did, kind, tier, priority, requester)| Work::Queue {
                actor_id,
                did,
                kind,
                tier,
                priority,
                requester,
            },
        ))
    }

    async fn pick_tier1(&self) -> Result<Option<Work>, sqlx::Error> {
        let pool = &self.ctx.pool;
        // Read as text: an entry whose requester this build does not
        // know is left waiting, and the others are served.
        let stored: Vec<String> = sqlx::query_scalar(
            &format!("SELECT DISTINCT requester FROM backfill_queue
             WHERE tier = {TIER_ON_DEMAND} AND kind <> {JOB_LIST_FETCH} AND (not_before IS NULL OR not_before <= now())"),
        )
        .fetch_all(pool)
        .await?;
        let mut reqs: Vec<RequesterKey> = stored
            .iter()
            .filter_map(|r| RequesterKey::parse(r))
            .collect();
        let candidates = lanes::load(pool).await?;
        if !candidates.is_empty() && !reqs.contains(&RequesterKey::Lists) {
            reqs.push(RequesterKey::Lists);
        }
        reqs.sort();
        loop {
            let Some(r) = pick_requester(&mut self.st().charged, &reqs) else {
                return Ok(None);
            };
            if r == RequesterKey::Lists {
                let picked = {
                    let mut s = self.st();
                    let inflight_items = s.inflight_items.clone();
                    let inflight_dids = s.inflight_dids.clone();
                    let hosts = &self.ctx.net.hosts;
                    s.lanes.pick(&candidates, |c| {
                        !inflight_items.contains(&c.item)
                            && match &c.item {
                                Item::Fetch { owner, .. } => !inflight_dids.contains(owner),
                                Item::Phase1 { .. } => true,
                            }
                            && c.host.as_deref().is_none_or(|h| hosts.has_capacity(h))
                    })
                };
                if let Some((cand, lane)) = picked {
                    if let Item::Fetch { queue_id, .. } = &cand.item {
                        let gone = sqlx::query("DELETE FROM backfill_queue WHERE id = $1")
                            .bind(*queue_id)
                            .execute(pool)
                            .await?
                            .rows_affected();
                        if gone == 0 {
                            reqs.retain(|x| *x != r);
                            continue;
                        }
                    }
                    return Ok(Some(Work::List {
                        cand: Box::new(cand),
                        lane,
                    }));
                }
                reqs.retain(|x| *x != r);
                continue;
            }
            let streak = *self.st().streak.get(&r).unwrap_or(&0);
            let pref = if want_high(streak) {
                Priority::High
            } else {
                Priority::Normal
            };
            if let Some(w) = self
                .claim_queue(
                    &format!("AND q.requester = $3 AND q.kind <> {JOB_LIST_FETCH}"),
                    (Tier::OnDemand, Some(r), Some(pref)),
                )
                .await?
            {
                if let Work::Queue { priority, .. } = &w {
                    self.note_priority(r, *priority == Priority::High);
                }
                return Ok(Some(w));
            }
            reqs.retain(|x| *x != r);
        }
    }

    async fn pick_tier3(&self) -> Result<Option<Work>, sqlx::Error> {
        if !self.tier3_token() {
            return Ok(None);
        }
        if let Some(w) = self.claim_queue("", (Tier::Sweep, None, None)).await? {
            return Ok(Some(w));
        }
        let full_enabled = self.ctx.cfg().backfill.sweep.enabled;
        let (cursor, inflight) = {
            let s = self.st();
            (
                s.member_cursor.clone().unwrap_or_default(),
                s.inflight_dids.iter().cloned().collect::<Vec<_>>(),
            )
        };
        // Members are dispatched straight from cycle_outstanding (no
        // actors row needed); failed members retry via the queue.
        let row: Option<(String, CycleKind)> = sqlx::query_as(
            &format!("SELECT o.did, c.kind FROM cycle_outstanding o JOIN sweep_cycles c ON c.id = o.cycle_id
             WHERE o.state = {MEMBER_OUTSTANDING} AND c.completed_at IS NULL AND o.did > $1 AND o.did <> ALL($2)
               AND ($3 OR c.kind = {CYCLE_REPAIR}) AND ($4 OR c.kind <> {CYCLE_REPAIR})
               AND NOT EXISTS (SELECT 1 FROM job_leases j WHERE j.did = o.did AND j.lease_until > now())
               AND NOT EXISTS (SELECT 1 FROM backfill_queue q JOIN actors a ON a.id = q.actor_id
                               WHERE a.did = o.did AND q.kind = {JOB_REPO})
             ORDER BY o.did LIMIT 1"),
        )
        .bind(&cursor)
        .bind(&inflight)
        .bind(full_enabled)
        .bind(!self.ctx.cfg().backfill.repair.paused)
        .fetch_optional(&self.ctx.pool)
        .await?;
        match row {
            Some((did, kind)) => {
                self.st().member_cursor = Some(did.clone());
                Ok(Some(Work::Member {
                    did,
                    requester: match kind {
                        CycleKind::Repair => RequesterKey::Repair,
                        CycleKind::Full => RequesterKey::Sweep,
                    },
                }))
            }
            None => {
                // Wrap around: members not done yet are retried in turn.
                self.st().member_cursor = None;
                Ok(None)
            }
        }
    }

    async fn pick(&self) -> Result<Option<(Tier, Work)>, sqlx::Error> {
        let mut has = self.has_work(&self.ctx.pool).await?;
        let shares: [u32; 3] = {
            let s = &self.ctx.cfg().backfill.tier_shares;
            [
                s.first().copied().unwrap_or(60),
                s.get(1).copied().unwrap_or(25),
                s.get(2).copied().unwrap_or(15),
            ]
        };
        loop {
            let running = self.st().running;
            let Some(t) = pick_tier(running, shares, has) else {
                break;
            };
            let w = match t {
                Tier::OnDemand => self.pick_tier1().await?,
                Tier::Active => self.claim_queue("", (Tier::Active, None, None)).await?,
                Tier::Sweep => self.pick_tier3().await?,
            };
            match w {
                Some(w) => return Ok(Some((t, w))),
                None => has[t.index()] = false,
            }
        }
        Ok(None)
    }

    /// Runs the pool until `stop` flips.
    pub async fn run(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        loop {
            if *stop.borrow() {
                return;
            }
            let concurrency = self.ctx.cfg().backfill.concurrency.max(1) as usize;
            let running: usize = self.st().running.iter().sum();
            let picked = if running < concurrency {
                match self.pick().await {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(error = %e, "scheduler pick failed");
                        None
                    }
                }
            } else {
                None
            };
            match picked {
                Some((tier, work)) => {
                    self.start(tier, work);
                }
                None => {
                    tokio::select! {
                        _ = self.freed.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                        _ = stop.changed() => {}
                    }
                }
            }
        }
    }

    /// Workers busy per tier, and DIDs with a job in flight (harness and
    /// tests: what a finished or failed job must have given back).
    pub fn in_flight(&self) -> ([usize; 3], usize) {
        let s = self.st();
        (s.running, s.inflight_dids.len() + s.inflight_items.len())
    }

    fn start(self: &Arc<Self>, tier: Tier, work: Work) {
        {
            let mut s = self.st();
            s.running[tier.index()] += 1;
            match &work {
                Work::Queue { did, .. } | Work::Member { did, .. } => {
                    s.inflight_dids.insert(did.clone());
                }
                Work::List { cand, .. } => {
                    s.inflight_items.insert(cand.item.clone());
                    if let Item::Fetch { owner, .. } = &cand.item {
                        s.inflight_dids.insert(owner.clone());
                    }
                }
            }
            // Charged now, so the next pick already sees this job.
            if tier == Tier::OnDemand {
                charge_dispatch(&mut s.charged, work.requester());
            }
            if let Work::List { lane, .. } = &work {
                s.lanes.charge(lane, DISPATCH_CHARGE);
            }
        }
        let running = Running {
            sched: self.clone(),
            tier,
            work,
        };
        tokio::spawn(async move {
            // `running` is owned by this task: it is dropped when the task
            // ends, whether the job returned or panicked.
            if let Err(message) =
                farsight_core::task::catch(running.sched.run_job(running.tier, &running.work)).await
            {
                farsight_core::task::report_panic(JOB_TASK, &message);
            }
        });
    }

    /// Runs one dispatched job, logs it and settles its cost.
    async fn run_job(&self, tier: Tier, work: &Work) {
        let started = Instant::now();
        let (result, requester, lane) = self.execute(work).await;
        // One line per job at the default level.
        let (kind, subject) = match work {
            Work::Queue { kind, did, .. } => (
                match kind {
                    JobKind::ListFetch => "list_fetch",
                    JobKind::Discovery => "discovery",
                    JobKind::Repo => "repo",
                },
                did.clone(),
            ),
            Work::Member { did, .. } => ("repo", did.clone()),
            Work::List { cand, .. } => match &cand.item {
                Item::Phase1 { list_id } => ("list_phase1", list_id.to_string()),
                Item::Fetch { owner, .. } => ("list_fetch", owner.clone()),
            },
        };
        match &result.outcome {
            Outcome::Failed { error, terminal } => tracing::info!(
                kind,
                subject,
                tier = tier.code(),
                requester = requester.to_string(),
                outcome = result.outcome.label(),
                terminal,
                error,
                cost = result.cost,
                ms = started.elapsed().as_millis() as u64,
                "job finished"
            ),
            o => tracing::info!(
                kind,
                subject,
                tier = tier.code(),
                requester = requester.to_string(),
                outcome = o.label(),
                cost = result.cost,
                ms = started.elapsed().as_millis() as u64,
                "job finished"
            ),
        }
        let mut s = self.st();
        // The dispatch charge is already in; the rest of the cost follows.
        if tier == Tier::OnDemand {
            charge_finish(&mut s.charged, requester, result.cost);
        }
        if let Some(l) = lane {
            let rest = remaining_charge(result.cost);
            if rest > 0 {
                s.lanes.charge(&l, rest);
            }
        }
        if !matches!(result.outcome, Outcome::Busy | Outcome::Yielded) {
            s.completions.push_back(Instant::now());
        }
    }

    async fn execute(&self, work: &Work) -> (JobResult, RequesterKey, Option<String>) {
        let ctx = &self.ctx;
        #[cfg(feature = "harness")]
        if self.dry {
            return self.dry_run(work).await;
        }
        #[cfg(feature = "harness")]
        if let Work::Queue { did, .. } = work {
            let hit = self
                .panic_dids
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(did);
            assert!(!hit, "injected job panic (harness) for {did}");
        }
        match work {
            Work::Queue {
                actor_id,
                did,
                kind,
                tier: qt,
                requester,
                ..
            } => {
                let Ok(d) = Did::parse(did) else {
                    return (
                        JobResult {
                            outcome: Outcome::Clean,
                            cost: 0,
                        },
                        *requester,
                        None,
                    );
                };
                let r = match kind {
                    JobKind::Discovery => {
                        if jobs::acquire_lease(&ctx.pool, did, &ctx.lease_owner)
                            .await
                            .unwrap_or(false)
                        {
                            let r = jobs::discovery::run(ctx, &d, *requester).await;
                            jobs::release_lease(&ctx.pool, did, &ctx.lease_owner).await;
                            r
                        } else {
                            self.requeue(*actor_id, JobKind::Discovery, *qt, *requester, 60)
                                .await;
                            JobResult {
                                outcome: Outcome::Busy,
                                cost: 0,
                            }
                        }
                    }
                    JobKind::ListFetch => jobs::list_fetch::run(ctx, *actor_id, &d).await,
                    JobKind::Repo => {
                        if jobs::acquire_lease(&ctx.pool, did, &ctx.lease_owner)
                            .await
                            .unwrap_or(false)
                        {
                            let req = JobReq {
                                did: d,
                                tier: *qt,
                                requester: *requester,
                            };
                            let r = jobs::repo::run(ctx, &req).await;
                            jobs::release_lease(&ctx.pool, did, &ctx.lease_owner).await;
                            r
                        } else {
                            // A job for the DID is running: this waiting
                            // entry (force / system) runs after it.
                            self.requeue(*actor_id, JobKind::Repo, *qt, *requester, 60)
                                .await;
                            JobResult {
                                outcome: Outcome::Busy,
                                cost: 0,
                            }
                        }
                    }
                };
                (r, *requester, None)
            }
            Work::List { cand, lane } => {
                let r = match &cand.item {
                    Item::Phase1 { list_id } => jobs::list_phase1::run(ctx, *list_id).await,
                    Item::Fetch {
                        owner_id, owner, ..
                    } => match Did::parse(owner) {
                        Ok(d) => jobs::list_fetch::run(ctx, *owner_id, &d).await,
                        Err(_) => JobResult {
                            outcome: Outcome::Clean,
                            cost: 0,
                        },
                    },
                };
                (r, RequesterKey::Lists, Some(lane.clone()))
            }
            Work::Member { did, requester } => {
                let Ok(d) = Did::parse(did) else {
                    return (
                        JobResult {
                            outcome: Outcome::Clean,
                            cost: 0,
                        },
                        *requester,
                        None,
                    );
                };
                if !jobs::acquire_lease(&ctx.pool, did, &ctx.lease_owner)
                    .await
                    .unwrap_or(false)
                {
                    return (
                        JobResult {
                            outcome: Outcome::Busy,
                            cost: 0,
                        },
                        *requester,
                        None,
                    );
                }
                let req = JobReq {
                    did: d,
                    tier: Tier::Sweep,
                    requester: *requester,
                };
                let r = jobs::repo::run(ctx, &req).await;
                jobs::release_lease(&ctx.pool, did, &ctx.lease_owner).await;
                (r, *requester, None)
            }
        }
    }

    async fn requeue(
        &self,
        actor_id: ActorId,
        kind: JobKind,
        tier: Tier,
        requester: RequesterKey,
        delay_s: u64,
    ) {
        if let Ok(mut conn) = self.ctx.pool.acquire().await {
            let _ = farsight_storage::queue::enqueue(
                &mut conn,
                actor_id,
                kind,
                tier,
                Priority::Normal,
                requester,
                None,
            )
            .await;
            let _ = sqlx::query(
                "UPDATE backfill_queue SET not_before = now() + make_interval(secs => $3)
                 WHERE actor_id = $1 AND kind = $2",
            )
            .bind(actor_id)
            .bind(kind)
            .bind(delay_s as f64)
            .execute(&mut *conn)
            .await;
        }
    }

    /// Harness: records the pick (`backfill_state.backfilled_at` =
    /// `clock_timestamp()`), costs 1 request, does nothing else.
    #[cfg(feature = "harness")]
    async fn dry_run(&self, work: &Work) -> (JobResult, RequesterKey, Option<String>) {
        let (actor, requester) = match work {
            Work::Queue {
                actor_id,
                requester,
                ..
            } => (Some(*actor_id), *requester),
            Work::List { .. } => (None, RequesterKey::Lists),
            Work::Member { requester, .. } => (None, *requester),
        };
        if let Some(a) = actor {
            use farsight_storage::codes::sql::REPO_DONE;
            let _ = sqlx::query(
                &format!("INSERT INTO backfill_state (actor_id, state, backfilled_at) VALUES ($1, {REPO_DONE}, clock_timestamp())
                 ON CONFLICT (actor_id) DO UPDATE SET state = {REPO_DONE}, backfilled_at = clock_timestamp()"),
            )
            .bind(a)
            .execute(&self.ctx.pool)
            .await;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        (
            JobResult {
                outcome: Outcome::Clean,
                cost: 1,
            },
            requester,
            None,
        )
    }

    /// Bumps the high/normal streak after a tier-1 pick (called by the
    /// claim path with the picked priority).
    pub fn note_priority(&self, requester: RequesterKey, high: bool) {
        let mut s = self.st();
        let e = s.streak.entry(requester).or_insert(0);
        if high {
            *e += 1;
        } else {
            *e = 0;
        }
    }

    /// Publishes queue depth and throughput gauges.
    pub async fn publish_gauges(&self) {
        if let Ok(rows) = sqlx::query_as::<_, (Tier, i64)>(
            "SELECT tier, count(*) FROM backfill_queue GROUP BY tier",
        )
        .fetch_all(&self.ctx.pool)
        .await
        {
            for t in Tier::ALL {
                let n = rows.iter().find(|(x, _)| x == t).map_or(0, |(_, n)| *n);
                metrics::gauge!(m::QUEUE_DEPTH, "tier" => t.label()).set(n as f64);
            }
        }
        metrics::gauge!(m::REPOS_PER_HOUR).set(self.repos_last_hour() as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier3_pacing_starts_empty_and_holds_one_job_at_least() {
        let t0 = Instant::now();
        // 1800 an hour is one every two seconds, with room for one.
        let rate = tier3_rate(1800);
        assert_eq!((rate.per_sec, rate.burst), (0.5, 1.0));
        let mut b = Bucket::unused(0.0, TIER3_HEAD_START);
        // The first look finds half a token.
        assert!(!b.take(t0, rate));
        assert!(b.take(t0 + Duration::from_secs(1), rate));
        assert!(!b.take(t0 + Duration::from_secs(2), rate));
        assert!(b.take(t0 + Duration::from_secs(3), rate));
        // A long pause saves up one job, not more.
        assert!(b.take(t0 + Duration::from_secs(3600), rate));
        assert!(!b.take(t0 + Duration::from_secs(3600), rate));
        // 7200 an hour is two a second: the first look dispatches, and
        // the bucket holds two.
        let rate = tier3_rate(7200);
        assert_eq!((rate.per_sec, rate.burst), (2.0, 2.0));
        let mut b = Bucket::unused(0.0, TIER3_HEAD_START);
        assert!(b.take(t0, rate));
        assert!(b.take(t0, rate));
        assert!(!b.take(t0, rate));
    }

    #[test]
    fn shares_are_minimums() {
        // All tiers busy: the one furthest below its share goes next.
        // 20/60 = 0.33, 8/25 = 0.32, 5/15 = 0.33: tier 2 is furthest below.
        assert_eq!(
            pick_tier([20, 8, 5], [60, 25, 15], [true; 3]),
            Some(Tier::Active)
        );
        assert_eq!(
            pick_tier([10, 8, 5], [60, 25, 15], [true; 3]),
            Some(Tier::OnDemand)
        );
        // Unused share flows: only tier 3 has work.
        assert_eq!(
            pick_tier([0, 0, 30], [60, 25, 15], [false, false, true]),
            Some(Tier::Sweep)
        );
        assert_eq!(pick_tier([0, 0, 0], [60, 25, 15], [false; 3]), None);
    }

    #[test]
    fn drr_is_fair_in_cost() {
        let mut c = HashMap::new();
        let reqs = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let mut served: HashMap<String, u32> = HashMap::new();
        for _ in 0..300 {
            let r = pick_requester(&mut c, &reqs).unwrap();
            // "a" jobs cost 3, the others 1: "a" gets a third of the cost,
            // not a third of the jobs.
            let cost = if r == "a" { 3.0 } else { 1.0 };
            *c.get_mut(&r).unwrap() += cost;
            *served.entry(r).or_default() += 1;
        }
        assert!((served["b"] as i32 - served["c"] as i32).abs() <= 1);
        assert!((served["a"] as f64 / served["b"] as f64 - 1.0 / 3.0).abs() < 0.05);
    }

    #[test]
    fn jobs_dispatched_together_are_shared_between_requesters() {
        // Eight free workers, two requesters with work, no job finished
        // yet: the dispatch charge alone must spread the picks.
        let mut c = HashMap::new();
        let reqs = vec!["a".to_owned(), "b".to_owned()];
        let mut served: HashMap<String, u32> = HashMap::new();
        for _ in 0..8 {
            let r = pick_requester(&mut c, &reqs).unwrap();
            charge_dispatch(&mut c, r.clone());
            *served.entry(r).or_default() += 1;
        }
        assert_eq!(served["a"], 4, "{served:?}");
        assert_eq!(served["b"], 4, "{served:?}");
    }

    #[test]
    fn dispatch_and_finish_charge_the_cost_once() {
        for cost in [0u64, 1, 2, 37] {
            let mut c = HashMap::new();
            charge_dispatch(&mut c, "a");
            charge_finish(&mut c, "a", cost);
            assert_eq!(c["a"], cost.max(1) as f64, "cost {cost}");
        }
    }

    #[test]
    fn four_high_then_one_normal() {
        let mut streak = 0;
        let mut seq = Vec::new();
        for _ in 0..10 {
            let high = want_high(streak);
            seq.push(high);
            streak = if high { streak + 1 } else { 0 };
        }
        assert_eq!(
            seq,
            [true, true, true, true, false, true, true, true, true, false]
        );
    }
}
