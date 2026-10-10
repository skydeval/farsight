//! The scheduler (see `docs/design/backfill.md`): a worker pool of
//! `backfill.concurrency`, three tiers whose shares are minimums (unused
//! share flows to the others), `backfill.on_demand_reserved` workers that
//! tiers 2 and 3 never use, cost-based deficit round-robin across tier-1
//! requesters charged in outbound requests, `high` before `normal` 4:1
//! within a requester, and the list-job lanes inside `system:lists`.
//!
//! A job is charged as it goes: one request ([`DISPATCH_CHARGE`]) when it
//! is dispatched, each further request when the job has made it, and at
//! its end whatever its cost says beyond that. Charged only at the end, a
//! requester would look cheapest for as long as its jobs ran and take
//! every free worker.
//!
//! Nothing the scheduler has taken is lost when the process is killed. A
//! queue entry is claimed, not deleted, when its job starts
//! (`farsight_storage::queue`): the claim is renewed while the job runs,
//! the entry is deleted when the job has ended, and an entry whose claim
//! ran out waits again. A cycle member stays in `cycle_outstanding` until
//! its job settles it.
//!
//! Each job runs in its own task, in a set the scheduler owns: when the
//! scheduler stops (shutdown, or a rebuild after a config change) every
//! job is stopped at its next await, and the entries and leases they held
//! are given back. A job's worker slot and its in-flight markers are held
//! by a guard and given back when its task ends, however it ends. A job
//! that panics is logged, counted and recorded as a failed job, so it is
//! retried on the failure schedule and not at once.

use farsight_storage::codes::sql::{
    CYCLE_REPAIR, DISCOVERY_FAILED, JOB_LIST_FETCH, JOB_REPO, MEMBER_OUTSTANDING, TIER_ACTIVE,
    TIER_ON_DEMAND, TIER_SWEEP, TRACK_MISSING,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use std::hash::Hash;

use farsight_core::Did;
use farsight_core::bucket::{Bucket, Rate};
use farsight_storage::codes::{CycleKind, JobKind, Priority, RequesterKey, Tier};
use farsight_storage::ids::{ActorId, CycleId, QueueId};
use farsight_storage::queue;
use sqlx::PgPool;
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;

use crate::ctx::{Ctx, JOB};
use crate::jobs::{self, Finish, JobReq, JobResult, Outcome};
use crate::lanes::{self, Item, Lanes};
use crate::metrics as m;
use crate::net::{METER, TURN_MS, WAITED_MS};

/// High-priority picks per normal pick within a requester.
pub const HIGH_PER_NORMAL: u32 = 4;
/// What a job is charged when it is dispatched, in outbound requests: the
/// least a job costs.
pub const DISPATCH_CHARGE: u64 = 1;
/// The task name panics of jobs are counted under.
pub const JOB_TASK: &str = "backfill_job";
/// What the tier-3 pacing counts as elapsed on its first use: the bucket
/// starts empty, so the first sweep job is dispatched at once only at a
/// rate of one a second or more.
pub const TIER3_HEAD_START: Duration = Duration::from_secs(1);
/// How often the claims on the queue entries of running jobs are renewed.
pub const RENEW_EVERY: Duration = Duration::from_secs(60);
/// How long an entry waits whose job found the DID's lease held.
pub const BUSY_RETRY: Duration = Duration::from_secs(60);
/// How long a phase-1 check whose job panicked waits.
pub const PANIC_RETRY: Duration = Duration::from_secs(3600);

/// The tier-3 rate for `per_hour` repos an hour: the bucket has room for
/// one second of it, and for one job at least.
fn tier3_rate(per_hour: u64) -> Rate {
    let per_sec = per_hour as f64 / 3600.0;
    Rate {
        per_sec,
        burst: per_sec.max(1.0),
    }
}

/// What a job has cost: one unit per request it made and one per second
/// it waited for hosts to answer. Counted in requests alone, a job on a
/// host that takes half a minute per answer would look the cheapest of
/// all while it holds a worker the longest.
pub fn job_cost(requests: u64, waited_ms: u64) -> u64 {
    requests.saturating_add(waited_ms / 1000)
}

/// Charges `requester` for a job being dispatched.
pub fn charge_dispatch<K: Eq + Hash>(charged: &mut HashMap<K, f64>, requester: K) {
    *charged.entry(requester).or_insert(0.0) += DISPATCH_CHARGE as f64;
}

/// What a job that has cost `total` requests so far still owes when
/// `charged` of them are charged. A job costs [`DISPATCH_CHARGE`] at
/// least, and nothing is ever given back.
pub fn outstanding_charge(total: u64, charged: u64) -> u64 {
    total.max(DISPATCH_CHARGE).saturating_sub(charged)
}

/// The workers kept for tier 1: `backfill.on_demand_reserved`, and never
/// all of them, so tiers 2 and 3 always have one.
pub fn on_demand_reserve(configured: u32, concurrency: usize) -> usize {
    (configured as usize).min(concurrency.saturating_sub(1))
}

/// Whether tiers 2 and 3, with `lower_running` jobs between them, may
/// take another worker of `concurrency` when `reserve` are kept for
/// tier 1.
pub fn lower_tiers_have_room(lower_running: usize, concurrency: usize, reserve: usize) -> bool {
    lower_running < concurrency.saturating_sub(reserve)
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
        /// The claimed `backfill_queue` entry.
        id: QueueId,
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

/// What the scheduler keeps of a job while it runs, to charge it as it
/// goes.
#[derive(Debug)]
struct Metered {
    tier: Tier,
    requester: RequesterKey,
    lane: Option<String>,
    /// The job's outbound requests so far (`crate::net::METER`).
    meter: Arc<AtomicU64>,
    /// The milliseconds it has waited for hosts to answer so far
    /// (`crate::net::WAITED_MS`).
    waited_ms: Arc<AtomicU64>,
    /// How many of them are charged.
    charged: u64,
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
    /// The jobs running, by number.
    jobs: HashMap<u64, Metered>,
    /// The queue entries whose jobs run: their claims are renewed.
    claims: HashSet<QueueId>,
    next_job: u64,
}

impl State {
    /// Charges `job` what it owes at `total` requests: to its tier-1
    /// requester and to its lane.
    fn charge(&mut self, job: u64, total: u64) {
        let Some(j) = self.jobs.get_mut(&job) else {
            return;
        };
        let due = outstanding_charge(total, j.charged);
        if due == 0 {
            return;
        }
        j.charged += due;
        let (tier, requester, lane) = (j.tier, j.requester, j.lane.clone());
        if tier == Tier::OnDemand {
            *self.charged.entry(requester).or_insert(0.0) += due as f64;
        }
        if let Some(l) = lane {
            self.lanes.charge(&l, due);
        }
    }
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

    /// The queue entry the job holds a claim on, if it came from one.
    fn claim(&self) -> Option<QueueId> {
        match self {
            Work::Queue { id, .. } => Some(*id),
            Work::List { cand, .. } => match &cand.item {
                Item::Fetch { queue_id, .. } => Some(*queue_id),
                Item::Phase1 { .. } => None,
            },
            Work::Member { .. } => None,
        }
    }
}

/// The lease owners' names of the running jobs `jobs` of `process`, in
/// order.
fn lease_owners(process: &str, jobs: impl Iterator<Item = u64>) -> Vec<String> {
    let mut owners: Vec<String> = jobs.map(|n| jobs::job_lease_owner(process, n)).collect();
    owners.sort_unstable();
    owners
}

/// A dispatched job's hold on the pool: one worker of its tier, its
/// in-flight markers, its meter and the renewal of its claim. Dropping it
/// gives them back and wakes the dispatcher, so they are returned when the
/// job's task ends for any reason: a panic, or the scheduler stopping it.
struct Running {
    sched: Arc<Scheduler>,
    tier: Tier,
    work: Work,
    job: u64,
}

impl Drop for Running {
    fn drop(&mut self) {
        {
            let mut s = self.sched.st();
            let i = self.tier.index();
            s.running[i] = s.running[i].saturating_sub(1);
            s.jobs.remove(&self.job);
            if let Some(id) = self.work.claim() {
                s.claims.remove(&id);
            }
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
                              AND claimed_by IS NULL
                              AND (not_before IS NULL OR not_before <= now())),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE tier = {TIER_ACTIVE}
                              AND claimed_by IS NULL
                              AND (not_before IS NULL OR not_before <= now())),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE tier = {TIER_SWEEP}
                              AND claimed_by IS NULL
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

    /// Claims the next waiting entry of a tier for this process. An entry
    /// whose DID has a job in flight here, or a live lease anywhere, is
    /// left waiting: its job would find the DID busy.
    async fn claim_queue(
        &self,
        sql_filter: &str,
        args: (Tier, Option<RequesterKey>, Option<Priority>),
    ) -> Result<Option<Work>, sqlx::Error> {
        let inflight: Vec<String> = self.st().inflight_dids.iter().cloned().collect();
        // An entry whose account is on a host that takes no request now
        // (cooling down, or at its limit) is left waiting too: its job
        // would hold a worker while it waits for the host. This is what
        // keeps a few slow hosts from holding every worker.
        let blocked = self.ctx.net.blocked_hosts();
        // With a preferred priority, the entries of that priority are read
        // first and the others after: each read follows
        // `backfill_queue_pick` in its order and ends at the first entry
        // it can take. One read ordered "preferred first" sorts every
        // waiting entry of the requester for each job started.
        let priorities: Vec<Option<Priority>> = match args.2 {
            Some(Priority::High) => vec![Some(Priority::High), Some(Priority::Normal)],
            Some(Priority::Normal) => vec![Some(Priority::Normal), Some(Priority::High)],
            None => vec![None],
        };
        for priority in priorities {
            let sql = format!(
                "UPDATE backfill_queue SET claimed_by = $5,
                   claimed_until = now() + make_interval(secs => $6)
                 WHERE id = (
                   SELECT q.id FROM backfill_queue q JOIN actors a ON a.id = q.actor_id
                   WHERE q.tier = $1 AND q.claimed_by IS NULL
                     AND (q.not_before IS NULL OR q.not_before <= now())
                     AND a.did <> ALL($2) {sql_filter} {}
                     AND NOT EXISTS (SELECT 1 FROM pds_hosts h
                                     WHERE h.id = a.pds_host_id AND h.host = ANY($7))
                     AND NOT EXISTS (SELECT 1 FROM job_leases j
                                     WHERE j.did = a.did AND j.lease_until > now())
                   ORDER BY q.enqueued_at, q.id LIMIT 1 FOR UPDATE OF q SKIP LOCKED)
                 RETURNING id, actor_id,
                   (SELECT did FROM actors WHERE id = backfill_queue.actor_id), kind, tier, priority, requester",
                if priority.is_some() { "AND q.priority = $4" } else { "" }
            );
            let row: Option<QueueRow> = sqlx::query_as(&sql)
                .bind(args.0)
                .bind(&inflight)
                .bind(args.1)
                .bind(priority)
                .bind(&self.ctx.process)
                .bind(queue::CLAIM.as_secs_f64())
                .bind(&blocked)
                .fetch_optional(&self.ctx.pool)
                .await?;
            if let Some((id, actor_id, did, kind, tier, priority, requester)) = row {
                return Ok(Some(Work::Queue {
                    id,
                    actor_id,
                    did,
                    kind,
                    tier,
                    priority,
                    requester,
                }));
            }
        }
        Ok(None)
    }

    async fn pick_tier1(&self) -> Result<Option<Work>, sqlx::Error> {
        let pool = &self.ctx.pool;
        // Read as text: an entry whose requester this build does not
        // know is left waiting, and the others are served.
        let stored: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT DISTINCT requester FROM backfill_queue
             WHERE tier = {TIER_ON_DEMAND} AND kind <> {JOB_LIST_FETCH} AND claimed_by IS NULL
               AND (not_before IS NULL OR not_before <= now())"
        ))
        .fetch_all(pool)
        .await?;
        let mut reqs: Vec<RequesterKey> = stored
            .iter()
            .filter_map(|r| RequesterKey::parse(r))
            .collect();
        let candidates = lanes::load(pool, &self.ctx.net.blocked_hosts()).await?;
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
                    let net = &self.ctx.net;
                    s.lanes.pick(&candidates, |c| {
                        !inflight_items.contains(&c.item)
                            && match &c.item {
                                Item::Fetch { owner, .. } => !inflight_dids.contains(owner),
                                Item::Phase1 { .. } => true,
                            }
                            && c.host.as_deref().is_none_or(|h| net.has_capacity(h))
                    })
                };
                if let Some((cand, lane)) = picked {
                    if let Item::Fetch { queue_id, .. } = &cand.item {
                        let mine = queue::claim(pool, *queue_id, &self.ctx.process)
                            .await
                            .map_err(storage_as_sqlx)?;
                        if !mine {
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
        //
        // Each open cycle is asked for its next member through its own
        // index, in DID order from the cursor, and the earliest of them is
        // taken. The read ends at the first member it can take, so it
        // costs the same with ten thousand members outstanding or with
        // millions. Asked of all cycles at once, the members after the
        // cursor are read and sorted for every pick.
        //
        // What rules a member out is asked about that member alone, as
        // one scalar subquery: whether its host takes no request now, and
        // whether it has an entry in the queue (waiting entries and
        // claimed ones each through their own index). Written as joins,
        // these are the planner's to reorder, and it has chosen to read
        // every row of `actors` for one pick. The leases are read once.
        let cycles: Vec<(CycleId, CycleKind)> = sqlx::query_as(&format!(
            "SELECT id, kind FROM sweep_cycles
             WHERE completed_at IS NULL
               AND ($1 OR kind = {CYCLE_REPAIR}) AND ($2 OR kind <> {CYCLE_REPAIR})
             ORDER BY id"
        ))
        .bind(full_enabled)
        .bind(!self.ctx.cfg().backfill.repair.paused)
        .fetch_all(&self.ctx.pool)
        .await?;
        let blocked = self.ctx.net.blocked_hosts();
        let mut row: Option<(String, CycleKind)> = None;
        for (cycle, kind) in cycles {
            let did: Option<String> = sqlx::query_scalar(&format!(
                "SELECT o.did FROM cycle_outstanding o
                 WHERE o.cycle_id = $1 AND o.state = {MEMBER_OUTSTANDING}
                   AND o.did > $2 AND o.did <> ALL($3)
                   AND o.did NOT IN (SELECT j.did FROM job_leases j WHERE j.lease_until > now())
                   AND NOT COALESCE((
                     SELECT h.host = ANY($4)
                       OR EXISTS (SELECT 1 FROM backfill_queue q
                                  WHERE q.actor_id = a.id AND q.kind = {JOB_REPO}
                                    AND q.claimed_by IS NULL)
                       OR EXISTS (SELECT 1 FROM backfill_queue q
                                  WHERE q.actor_id = a.id AND q.kind = {JOB_REPO}
                                    AND q.claimed_by IS NOT NULL)
                     FROM actors a LEFT JOIN pds_hosts h ON h.id = a.pds_host_id
                     WHERE a.did = o.did), false)
                 ORDER BY o.did LIMIT 1"
            ))
            .bind(cycle)
            .bind(&cursor)
            .bind(&inflight)
            .bind(&blocked)
            .fetch_optional(&self.ctx.pool)
            .await?;
            if let Some(did) = did
                && row.as_ref().is_none_or(|(earliest, _)| did < *earliest)
            {
                row = Some((did, kind));
            }
        }
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

    async fn pick(&self, concurrency: usize) -> Result<Option<(Tier, Work)>, sqlx::Error> {
        let mut has = self.has_work(&self.ctx.pool).await?;
        let cfg = self.ctx.cfg();
        let shares: [u32; 3] = {
            let s = &cfg.backfill.tier_shares;
            [
                s.first().copied().unwrap_or(60),
                s.get(1).copied().unwrap_or(25),
                s.get(2).copied().unwrap_or(15),
            ]
        };
        let reserve = on_demand_reserve(cfg.backfill.on_demand_reserved, concurrency);
        loop {
            let running = self.st().running;
            // The workers kept for tier 1 are not lent: a lent worker
            // comes back only when its job ends.
            if !lower_tiers_have_room(running[1] + running[2], concurrency, reserve) {
                has[Tier::Active.index()] = false;
                has[Tier::Sweep.index()] = false;
            }
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

    /// Charges every running job what it has cost since it was last
    /// charged ([`job_cost`]).
    fn charge_running(&self) {
        let mut s = self.st();
        let progress: Vec<(u64, u64)> = s
            .jobs
            .iter()
            .map(|(n, j)| {
                (
                    *n,
                    job_cost(
                        j.meter.load(Ordering::Relaxed),
                        j.waited_ms.load(Ordering::Relaxed),
                    ),
                )
            })
            .collect();
        for (job, total) in progress {
            s.charge(job, total);
        }
    }

    /// Renews what the jobs that run hold: the claims on their queue
    /// entries, and their DID leases.
    async fn renew_claims(&self) {
        let (ids, owners): (Vec<QueueId>, Vec<String>) = {
            let s = self.st();
            (
                s.claims.iter().copied().collect(),
                lease_owners(&self.ctx.process, s.jobs.keys().copied()),
            )
        };
        if let Err(e) = queue::renew(&self.ctx.pool, &ids, &self.ctx.process).await {
            tracing::warn!(error = %e, "renewing queue claims failed");
        }
        if let Err(e) = jobs::renew_leases(&self.ctx.pool, &owners).await {
            tracing::warn!(error = %e, "renewing job leases failed");
        }
    }

    /// Gives back what this process holds: its claims on queue entries
    /// (they wait again) and its jobs' leases. Called with no job
    /// running.
    async fn release_held(&self) {
        let pool = &self.ctx.pool;
        match queue::release_owned(pool, &self.ctx.process).await {
            Ok(n) if n > 0 => tracing::info!(entries = n, "queue entries given back"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "giving back queue entries failed"),
        }
        if let Err(e) = jobs::release_process_leases(pool, &self.ctx.process).await {
            tracing::warn!(error = %e, "releasing job leases failed");
        }
    }

    /// Runs the pool until `stop` flips, then stops its jobs and gives
    /// back what they held.
    pub async fn run(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        // The jobs live in this set: when this future ends or is dropped,
        // they are stopped with it.
        let mut jobs: JoinSet<()> = JoinSet::new();
        // What an earlier run of this loop left claimed (it panicked) is
        // free again: its jobs went with it.
        self.release_held().await;
        let mut renewed = Instant::now();
        loop {
            if *stop.borrow() {
                break;
            }
            while jobs.try_join_next().is_some() {}
            self.charge_running();
            if renewed.elapsed() >= RENEW_EVERY {
                renewed = Instant::now();
                self.renew_claims().await;
            }
            let concurrency = self.ctx.cfg().backfill.concurrency.max(1) as usize;
            let running: usize = self.st().running.iter().sum();
            let picked = if running < concurrency {
                match self.pick(concurrency).await {
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
                    self.start(&mut jobs, tier, work);
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
        let stopped = jobs.len();
        jobs.shutdown().await;
        self.release_held().await;
        if stopped > 0 {
            tracing::info!(
                jobs = stopped,
                "running jobs stopped; their work waits in the queue"
            );
        }
    }

    /// Workers busy per tier, and DIDs with a job in flight (harness and
    /// tests: what a finished or failed job must have given back).
    pub fn in_flight(&self) -> ([usize; 3], usize) {
        let s = self.st();
        (s.running, s.inflight_dids.len() + s.inflight_items.len())
    }

    fn start(self: &Arc<Self>, jobs: &mut JoinSet<()>, tier: Tier, work: Work) {
        let meter = Arc::new(AtomicU64::new(0));
        let waited = Arc::new(AtomicU64::new(0));
        let job = {
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
            if let Some(id) = work.claim() {
                s.claims.insert(id);
            }
            let lane = match &work {
                Work::List { lane, .. } => Some(lane.clone()),
                _ => None,
            };
            // Charged now, so the next pick already sees this job.
            if tier == Tier::OnDemand {
                charge_dispatch(&mut s.charged, work.requester());
            }
            if let Some(l) = &lane {
                s.lanes.charge(l, DISPATCH_CHARGE);
            }
            s.next_job += 1;
            let job = s.next_job;
            s.jobs.insert(
                job,
                Metered {
                    tier,
                    requester: work.requester(),
                    lane,
                    meter: meter.clone(),
                    waited_ms: waited.clone(),
                    charged: DISPATCH_CHARGE,
                },
            );
            job
        };
        let running = Running {
            sched: self.clone(),
            tier,
            work,
            job,
        };
        jobs.spawn(JOB.scope(
            job,
            METER.scope(
                meter,
                WAITED_MS.scope(
                    waited,
                    TURN_MS.scope(Arc::default(), async move {
                        // `running` is owned by this task: it is dropped when the
                        // task ends, whether the job returned, panicked or was
                        // stopped.
                        let sched = running.sched.clone();
                        let ended = farsight_core::task::catch(sched.run_job(
                            running.tier,
                            &running.work,
                            job,
                        ))
                        .await;
                        if let Err(message) = ended {
                            farsight_core::task::report_panic(JOB_TASK, &message);
                            let handled = farsight_core::task::catch(sched.panicked(
                                &running.work,
                                job,
                                &message,
                            ))
                            .await;
                            if let Err(again) = handled {
                                farsight_core::task::report_panic(JOB_TASK, &again);
                            }
                        }
                    }),
                ),
            ),
        ));
    }

    /// Runs one dispatched job, logs it, settles its cost and its queue
    /// entry.
    async fn run_job(&self, tier: Tier, work: &Work, job: u64) {
        let started = Instant::now();
        let (result, requester, _) = self.execute(work).await;
        // Where the job's time went that was not its own work: waiting
        // for hosts to answer, for a request slot of a host, and for the
        // PLC directory's limiter.
        let answer_ms = crate::net::waited_ms().unwrap_or(0);
        let (slot_ms, plc_ms) = crate::net::turn_ms().unwrap_or((0, 0));
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
                answer_ms,
                slot_ms,
                plc_ms,
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
                answer_ms,
                slot_ms,
                plc_ms,
                "job finished"
            ),
        }
        {
            let mut s = self.st();
            // What the job cost beyond what was charged while it ran.
            let metered = s
                .jobs
                .get(&job)
                .map_or(0, |j| j.meter.load(Ordering::Relaxed));
            let waited = s
                .jobs
                .get(&job)
                .map_or(0, |j| j.waited_ms.load(Ordering::Relaxed));
            s.charge(job, job_cost(result.cost.max(metered), waited));
            if !matches!(result.outcome, Outcome::Busy | Outcome::Yielded) {
                s.completions.push_back(Instant::now());
            }
        }
        // The entry goes when its job has ended. A job that found the
        // DID's lease held did not run: its entry waits again.
        if let Some(id) = work.claim() {
            let pool = &self.ctx.pool;
            let process = &self.ctx.process;
            let settled = match (work, &result.outcome) {
                (Work::Queue { .. }, Outcome::Busy) => {
                    queue::release(pool, id, process, BUSY_RETRY).await
                }
                _ => queue::finish(pool, id, process).await,
            };
            if let Err(e) = settled {
                // The claim is no longer renewed: it runs out and the
                // entry is served again.
                tracing::warn!(error = %e, "settling a queue entry failed");
            }
        }
    }

    /// A job panicked: records it as a failed job of its kind, so that it
    /// is tried again on the failure schedule (and, for a repo, becomes a
    /// counted `unreachable` debt if it keeps failing), then drops the
    /// job's lease and queue entry.
    async fn panicked(&self, work: &Work, job: u64, message: &str) {
        let ctx = &self.ctx;
        let error = format!("the job panicked: {message}");
        let recorded: Result<(), String> = match work {
            Work::Queue {
                did,
                kind: JobKind::Repo,
                tier,
                requester,
                ..
            } => self.fail_repo(did, *tier, *requester, &error).await,
            Work::Member { did, requester } => {
                self.fail_repo(did, Tier::Sweep, *requester, &error).await
            }
            Work::Queue {
                actor_id,
                kind: JobKind::Discovery,
                ..
            } => sqlx::query(&format!(
                "UPDATE discovery_state SET state = {DISCOVERY_FAILED}, last_error = $2
                 WHERE actor_id = $1"
            ))
            .bind(*actor_id)
            .bind(&error)
            .execute(&ctx.pool)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
            Work::Queue {
                actor_id,
                kind: JobKind::ListFetch,
                ..
            } => jobs::list_fetch::abandon(ctx, *actor_id)
                .await
                .map_err(|e| e.to_string()),
            Work::List { cand, .. } => match &cand.item {
                Item::Fetch { owner_id, .. } => jobs::list_fetch::abandon(ctx, *owner_id)
                    .await
                    .map_err(|e| e.to_string()),
                Item::Phase1 { list_id } => jobs::list_phase1::postpone(ctx, *list_id, PANIC_RETRY)
                    .await
                    .map_err(|e| e.to_string()),
            },
        };
        if let Err(e) = recorded {
            tracing::warn!(error = e, "recording a panicked job failed");
        }
        let lease = jobs::job_lease_owner(&ctx.process, job);
        let _ = sqlx::query("DELETE FROM job_leases WHERE lease_owner = $1")
            .bind(&lease)
            .execute(&ctx.pool)
            .await;
        if let Some(id) = work.claim()
            && let Err(e) = queue::finish(&ctx.pool, id, &ctx.process).await
        {
            tracing::warn!(error = %e, "settling a queue entry failed");
        }
    }

    /// Records a repo job that ended without an outcome as failed.
    async fn fail_repo(
        &self,
        did: &str,
        tier: Tier,
        requester: RequesterKey,
        error: &str,
    ) -> Result<(), String> {
        let ctx = &self.ctx;
        let Ok(did) = Did::parse(did) else {
            return Ok(());
        };
        let job_start = jobs::db_now(&ctx.pool).await.map_err(|e| e.to_string())?;
        let req = JobReq {
            did,
            tier,
            requester,
        };
        let outcome = Outcome::Failed {
            error: error.to_owned(),
            terminal: false,
        };
        let f = Finish {
            req: &req,
            point: None,
            job_start,
            stamp: None,
            outcome: &outcome,
        };
        jobs::finish_repo(ctx, &f)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
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
        let nothing = JobResult {
            outcome: Outcome::Clean,
            cost: 0,
        };
        let busy = JobResult {
            outcome: Outcome::Busy,
            cost: 0,
        };
        let lease = ctx.lease_owner();
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
                    return (nothing, *requester, None);
                };
                let r = match kind {
                    JobKind::Discovery => {
                        if jobs::acquire_lease(&ctx.pool, did, &lease)
                            .await
                            .unwrap_or(false)
                        {
                            let r = jobs::discovery::run(ctx, &d, *requester).await;
                            jobs::release_lease(&ctx.pool, did, &lease).await;
                            r
                        } else {
                            busy
                        }
                    }
                    JobKind::ListFetch => jobs::list_fetch::run(ctx, *actor_id, &d).await,
                    JobKind::Repo => {
                        if jobs::acquire_lease(&ctx.pool, did, &lease)
                            .await
                            .unwrap_or(false)
                        {
                            let req = JobReq {
                                did: d,
                                tier: *qt,
                                requester: *requester,
                            };
                            let r = jobs::repo::run(ctx, &req).await;
                            jobs::release_lease(&ctx.pool, did, &lease).await;
                            r
                        } else {
                            // A job for the DID is running: this entry
                            // (force / system) runs after it.
                            busy
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
                        Err(_) => nothing,
                    },
                };
                (r, RequesterKey::Lists, Some(lane.clone()))
            }
            Work::Member { did, requester } => {
                let Ok(d) = Did::parse(did) else {
                    // Not a DID: there is no repo to list, and the member
                    // must not keep its cycle open.
                    if let Err(e) = jobs::settle_invalid_member(&ctx.pool, did).await {
                        tracing::warn!(error = %e, "settling an invalid cycle member failed");
                    }
                    return (nothing, *requester, None);
                };
                if !jobs::acquire_lease(&ctx.pool, did, &lease)
                    .await
                    .unwrap_or(false)
                {
                    return (busy, *requester, None);
                }
                let req = JobReq {
                    did: d,
                    tier: Tier::Sweep,
                    requester: *requester,
                };
                let r = jobs::repo::run(ctx, &req).await;
                jobs::release_lease(&ctx.pool, did, &lease).await;
                (r, *requester, None)
            }
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
            "SELECT tier, count(*) FROM backfill_queue WHERE claimed_by IS NULL GROUP BY tier",
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

/// A storage error as the scheduler's pick reports it.
fn storage_as_sqlx(e: farsight_storage::StorageError) -> sqlx::Error {
    match e {
        farsight_storage::StorageError::Db(e) => e,
        other => sqlx::Error::Protocol(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_leases_renewed_are_those_of_the_jobs_that_run() {
        assert_eq!(
            lease_owners("backfill-1", [12, 3].into_iter()),
            ["backfill-1#12", "backfill-1#3"]
        );
        assert!(lease_owners("p", std::iter::empty()).is_empty());
        // The name a job takes its lease under.
        assert_eq!(jobs::job_lease_owner("p", 7), "p#7");
    }

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
    fn a_job_is_charged_as_it_goes_and_its_cost_once() {
        // Whatever the moments it is looked at, a job of a given cost is
        // charged that cost, and one request at least.
        for cost in [0u64, 1, 2, 37] {
            for looks in [
                vec![],
                vec![0],
                vec![1, 1, 5],
                vec![3, 20, 36, 37],
                vec![90],
            ] {
                let mut charged = DISPATCH_CHARGE;
                for metered in looks.iter().copied().filter(|m| *m <= cost) {
                    let due = outstanding_charge(metered, charged);
                    charged += due;
                    assert!(charged <= cost.max(1));
                }
                charged += outstanding_charge(cost, charged);
                assert_eq!(charged, cost.max(1), "cost {cost}, looks {looks:?}");
            }
        }
        // Nothing is given back when the count read is behind the charge.
        assert_eq!(outstanding_charge(3, 10), 0);
    }

    #[test]
    fn a_long_job_is_charged_before_it_ends() {
        // Requesters a and b have each had a job dispatched. a's has made
        // 400 requests and still runs; b's jobs cost one each. Charged as
        // it goes, a is behind b for the next 399 picks.
        let mut s = State::default();
        let meter = Arc::new(AtomicU64::new(0));
        charge_dispatch(&mut s.charged, RequesterKey::Token(1));
        charge_dispatch(&mut s.charged, RequesterKey::Token(2));
        s.jobs.insert(
            1,
            Metered {
                tier: Tier::OnDemand,
                requester: RequesterKey::Token(1),
                lane: None,
                meter: meter.clone(),
                waited_ms: Arc::default(),
                charged: DISPATCH_CHARGE,
            },
        );
        // A job on a slow host: few requests, much waiting. It costs
        // what it waited.
        assert_eq!(job_cost(3, 0), 3);
        assert_eq!(job_cost(3, 59_999), 62);
        assert_eq!(job_cost(120, 3_600_000), 3_720);
        meter.store(400, Ordering::Relaxed);
        s.charge(1, meter.load(Ordering::Relaxed));
        assert_eq!(s.charged[&RequesterKey::Token(1)], 400.0);
        let reqs = [RequesterKey::Token(1), RequesterKey::Token(2)];
        let mut served_b = 0;
        for _ in 0..300 {
            let r = pick_requester(&mut s.charged, &reqs).unwrap();
            assert_eq!(r, RequesterKey::Token(2));
            charge_dispatch(&mut s.charged, r);
            served_b += 1;
        }
        assert_eq!(served_b, 300);
        // The same look twice charges nothing more; the end charges the
        // rest of the cost.
        s.charge(1, 400);
        assert_eq!(s.charged[&RequesterKey::Token(1)], 400.0);
        s.charge(1, 425);
        assert_eq!(s.charged[&RequesterKey::Token(1)], 425.0);
        // A tier-2 job charges no tier-1 requester.
        s.jobs.insert(
            2,
            Metered {
                tier: Tier::Active,
                requester: RequesterKey::Firehose,
                lane: None,
                meter: Arc::new(AtomicU64::new(0)),
                waited_ms: Arc::default(),
                charged: DISPATCH_CHARGE,
            },
        );
        s.charge(2, 50);
        assert!(!s.charged.contains_key(&RequesterKey::Firehose));
    }

    #[test]
    fn workers_kept_for_on_demand_are_never_lent() {
        // 32 workers, 4 kept: tiers 2 and 3 share 28.
        let reserve = on_demand_reserve(4, 32);
        assert_eq!(reserve, 4);
        assert!(lower_tiers_have_room(27, 32, reserve));
        assert!(!lower_tiers_have_room(28, 32, reserve));
        assert!(!lower_tiers_have_room(40, 32, reserve));
        // Never all of them: one worker is always there for the others.
        assert_eq!(on_demand_reserve(4, 1), 0);
        assert_eq!(on_demand_reserve(4, 3), 2);
        assert_eq!(on_demand_reserve(10_000, 32), 31);
        assert_eq!(on_demand_reserve(0, 32), 0);
        assert!(lower_tiers_have_room(0, 1, on_demand_reserve(4, 1)));
        // A pool filling up with lower-tier work, tier 1 idle: the kept
        // workers stay free, and a tier-1 job that arrives is dispatched.
        let (concurrency, shares) = (8usize, [60, 25, 15]);
        let reserve = on_demand_reserve(2, concurrency);
        let mut running = [0usize; 3];
        loop {
            let mut has = [false, true, true];
            if !lower_tiers_have_room(running[1] + running[2], concurrency, reserve) {
                has[1] = false;
                has[2] = false;
            }
            match pick_tier(running, shares, has) {
                Some(t) => running[t.index()] += 1,
                None => break,
            }
        }
        assert_eq!(running[0], 0);
        assert_eq!(running[1] + running[2], 6, "{running:?}");
        let mut has = [true, true, true];
        if !lower_tiers_have_room(running[1] + running[2], concurrency, reserve) {
            has[1] = false;
            has[2] = false;
        }
        assert_eq!(pick_tier(running, shares, has), Some(Tier::OnDemand));
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
