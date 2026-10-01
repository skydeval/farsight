//! The scheduler (design §5.3): a worker pool of `backfill.concurrency`,
//! three tiers whose shares are minimums (unused share flows to the
//! others), cost-based deficit round-robin across tier-1 requesters charged
//! in outbound requests, `high` before `normal` 4:1 within a requester, and
//! the list-job lanes inside `system:lists`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use farsight_core::Did;
use sqlx::PgPool;
use tokio::sync::{Notify, watch};

use crate::ctx::Ctx;
use crate::jobs::{self, JobReq, JobResult, Outcome};
use crate::lanes::{self, Item, Lanes};
use crate::metrics as m;

/// Requester of sweep members (§11.2: interning charged to `system:sweep`).
pub const SYSTEM_SWEEP: &str = "system:sweep";
/// Requester of repair-cycle members.
pub const SYSTEM_REPAIR: &str = "system:repair";
/// High-priority picks per normal pick within a requester (§5.3).
pub const HIGH_PER_NORMAL: u32 = 4;

/// Picks the tier to serve: among tiers with work, the one furthest below
/// its guaranteed share (running / share), so every share is a minimum and
/// unused share flows to the others.
pub fn pick_tier(running: [usize; 3], shares: [u32; 3], has_work: [bool; 3]) -> Option<usize> {
    (0..3).filter(|t| has_work[*t]).min_by(|a, b| {
        let ra = running[*a] as f64 / f64::from(shares[*a].max(1));
        let rb = running[*b] as f64 / f64::from(shares[*b].max(1));
        ra.partial_cmp(&rb)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.cmp(b))
    })
}

/// Cost-based DRR across requesters: the least-charged requester with work
/// goes next; a requester new to the round starts at the current minimum.
pub fn pick_requester(charged: &mut HashMap<String, f64>, with_work: &[String]) -> Option<String> {
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
/// `high`: four highs, then one normal (§5.3).
pub fn want_high(streak: u32) -> bool {
    streak < HIGH_PER_NORMAL
}

/// One unit of work.
#[derive(Debug, Clone)]
enum Work {
    Queue {
        actor_id: i64,
        did: String,
        kind: i16,
        tier: i16,
        priority: i16,
        requester: String,
    },
    List {
        cand: Box<lanes::Candidate>,
        lane: String,
    },
    Member {
        did: String,
        requester: &'static str,
    },
}

#[derive(Default)]
struct State {
    running: [usize; 3],
    charged: HashMap<String, f64>,
    streak: HashMap<String, u32>,
    inflight_dids: HashSet<String>,
    inflight_items: HashSet<Item>,
    lanes: Lanes,
    member_cursor: Option<String>,
    completions: VecDeque<Instant>,
    tier3_tokens: f64,
    tier3_last: Option<Instant>,
}

/// The scheduler.
pub struct Scheduler {
    ctx: Arc<Ctx>,
    st: Mutex<State>,
    freed: Notify,
    /// Harness: run every job as a no-op that records its pick time.
    #[cfg(feature = "harness")]
    dry: bool,
}

type QueueRow = (i64, i64, String, i16, i16, i16, String);

impl Scheduler {
    /// A scheduler over `ctx`.
    pub fn new(ctx: Arc<Ctx>) -> Arc<Scheduler> {
        Arc::new(Scheduler {
            ctx,
            st: Mutex::new(State::default()),
            freed: Notify::new(),
            #[cfg(feature = "harness")]
            dry: false,
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
        })
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

    /// Whether tier-3 dispatch is paused: storage ≥ 90% (§11.2).
    fn tier3_paused(&self) -> bool {
        self.ctx.sweep_paused_by_storage()
    }

    /// Tier-3 pacing (`backfill.sweep.max_repos_per_hour`, 0 = unbounded).
    fn tier3_token(&self) -> bool {
        let per_hour = self.ctx.cfg().backfill.sweep.max_repos_per_hour;
        if per_hour == 0 {
            return true;
        }
        let rate = per_hour as f64 / 3600.0;
        let mut s = self.st();
        let now = Instant::now();
        let el = s
            .tier3_last
            .map_or(1.0, |t| now.duration_since(t).as_secs_f64());
        s.tier3_tokens = (s.tier3_tokens + el * rate).min(rate.max(1.0));
        s.tier3_last = Some(now);
        if s.tier3_tokens >= 1.0 {
            s.tier3_tokens -= 1.0;
            true
        } else {
            false
        }
    }

    async fn has_work(&self, pool: &PgPool) -> Result<[bool; 3], sqlx::Error> {
        let (t1, t2, t3): (bool, bool, bool) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM backfill_queue WHERE tier = 1
                              AND (not_before IS NULL OR not_before <= now())),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE tier = 2
                              AND (not_before IS NULL OR not_before <= now())),
                    EXISTS (SELECT 1 FROM backfill_queue WHERE tier = 3
                              AND (not_before IS NULL OR not_before <= now()))
                    OR EXISTS (SELECT 1 FROM cycle_outstanding o JOIN sweep_cycles c ON c.id = o.cycle_id
                               WHERE o.state = 1 AND c.completed_at IS NULL)",
        )
        .fetch_one(pool)
        .await?;
        let lists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM list_jobs WHERE not_before IS NULL OR not_before <= now())
                 OR EXISTS (SELECT 1 FROM lists WHERE track_state = 6 AND next_retry_at <= now())",
        )
        .fetch_one(pool)
        .await?;
        Ok([t1 || lists, t2, t3 && !self.tier3_paused()])
    }

    async fn claim_queue(
        &self,
        sql_filter: &str,
        args: (i16, Option<&str>, Option<i16>),
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
        let mut reqs: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT requester FROM backfill_queue
             WHERE tier = 1 AND kind <> 2 AND (not_before IS NULL OR not_before <= now())",
        )
        .fetch_all(pool)
        .await?;
        let candidates = lanes::load(pool).await?;
        if !candidates.is_empty()
            && !reqs
                .iter()
                .any(|r| r == crate::jobs::list_phase1::SYSTEM_LISTS)
        {
            reqs.push(crate::jobs::list_phase1::SYSTEM_LISTS.to_owned());
        }
        reqs.sort();
        loop {
            let Some(r) = pick_requester(&mut self.st().charged, &reqs) else {
                return Ok(None);
            };
            if r == crate::jobs::list_phase1::SYSTEM_LISTS {
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
            let pref = if want_high(streak) { 1 } else { 0 };
            if let Some(w) = self
                .claim_queue(
                    "AND q.requester = $3 AND q.kind <> 2",
                    (1, Some(r.as_str()), Some(pref)),
                )
                .await?
            {
                if let Work::Queue { priority, .. } = &w {
                    self.note_priority(
                        &r,
                        *priority == farsight_storage::repo_events::priority::HIGH,
                    );
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
        if let Some(w) = self.claim_queue("", (3, None, None)).await? {
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
        // actors row needed, D4); failed members retry via the queue.
        let row: Option<(String, i16)> = sqlx::query_as(
            "SELECT o.did, c.kind FROM cycle_outstanding o JOIN sweep_cycles c ON c.id = o.cycle_id
             WHERE o.state = 1 AND c.completed_at IS NULL AND o.did > $1 AND o.did <> ALL($2)
               AND ($3 OR c.kind = 2)
               AND NOT EXISTS (SELECT 1 FROM job_leases j WHERE j.did = o.did AND j.lease_until > now())
               AND NOT EXISTS (SELECT 1 FROM backfill_queue q JOIN actors a ON a.id = q.actor_id
                               WHERE a.did = o.did AND q.kind = 1)
             ORDER BY o.did LIMIT 1",
        )
        .bind(&cursor)
        .bind(&inflight)
        .bind(full_enabled)
        .fetch_optional(&self.ctx.pool)
        .await?;
        match row {
            Some((did, kind)) => {
                self.st().member_cursor = Some(did.clone());
                Ok(Some(Work::Member {
                    did,
                    requester: if kind == 2 {
                        SYSTEM_REPAIR
                    } else {
                        SYSTEM_SWEEP
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

    async fn pick(&self) -> Result<Option<(usize, Work)>, sqlx::Error> {
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
                0 => self.pick_tier1().await?,
                1 => self.claim_queue("", (2, None, None)).await?,
                _ => self.pick_tier3().await?,
            };
            match w {
                Some(w) => return Ok(Some((t, w))),
                None => has[t] = false,
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

    fn start(self: &Arc<Self>, tier: usize, work: Work) {
        {
            let mut s = self.st();
            s.running[tier] += 1;
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
        }
        let me = self.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let (result, requester, lane) = me.execute(&work).await;
            // §13: one line per job at the default level.
            let (kind, subject) = match &work {
                Work::Queue { kind, did, .. } => (
                    match kind {
                        2 => "list_fetch",
                        3 => "discovery",
                        _ => "repo",
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
                    tier = tier + 1,
                    requester,
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
                    tier = tier + 1,
                    requester,
                    outcome = o.label(),
                    cost = result.cost,
                    ms = started.elapsed().as_millis() as u64,
                    "job finished"
                ),
            }
            let mut s = me.st();
            s.running[tier] = s.running[tier].saturating_sub(1);
            match &work {
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
            if tier == 0 {
                *s.charged.entry(requester.clone()).or_insert(0.0) += result.cost.max(1) as f64;
            }
            if let Some(l) = lane {
                s.lanes.charge(&l, result.cost);
            }
            if !matches!(result.outcome, Outcome::Busy | Outcome::Yielded) {
                s.completions.push_back(Instant::now());
            }
            drop(s);
            me.freed.notify_waiters();
        });
    }

    async fn execute(&self, work: &Work) -> (JobResult, String, Option<String>) {
        let ctx = &self.ctx;
        #[cfg(feature = "harness")]
        if self.dry {
            return self.dry_run(work).await;
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
                        requester.clone(),
                        None,
                    );
                };
                let r = match kind {
                    3 => {
                        if jobs::acquire_lease(&ctx.pool, did, &ctx.lease_owner)
                            .await
                            .unwrap_or(false)
                        {
                            let r = jobs::discovery::run(ctx, &d, requester).await;
                            jobs::release_lease(&ctx.pool, did, &ctx.lease_owner).await;
                            r
                        } else {
                            self.requeue(*actor_id, 3, *qt, requester, 60).await;
                            JobResult {
                                outcome: Outcome::Busy,
                                cost: 0,
                            }
                        }
                    }
                    2 => jobs::list_fetch::run(ctx, *actor_id, &d).await,
                    _ => {
                        if jobs::acquire_lease(&ctx.pool, did, &ctx.lease_owner)
                            .await
                            .unwrap_or(false)
                        {
                            let req = JobReq {
                                did: d,
                                tier: *qt,
                                requester: requester.clone(),
                            };
                            let r = jobs::repo::run(ctx, &req).await;
                            jobs::release_lease(&ctx.pool, did, &ctx.lease_owner).await;
                            r
                        } else {
                            // A job for the DID is running: this waiting
                            // entry (force / system) runs after it.
                            self.requeue(*actor_id, 1, *qt, requester, 60).await;
                            JobResult {
                                outcome: Outcome::Busy,
                                cost: 0,
                            }
                        }
                    }
                };
                (r, requester.clone(), None)
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
                (
                    r,
                    crate::jobs::list_phase1::SYSTEM_LISTS.to_owned(),
                    Some(lane.clone()),
                )
            }
            Work::Member { did, requester } => {
                let Ok(d) = Did::parse(did) else {
                    return (
                        JobResult {
                            outcome: Outcome::Clean,
                            cost: 0,
                        },
                        (*requester).to_owned(),
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
                        (*requester).to_owned(),
                        None,
                    );
                }
                let req = JobReq {
                    did: d,
                    tier: 3,
                    requester: (*requester).to_owned(),
                };
                let r = jobs::repo::run(ctx, &req).await;
                jobs::release_lease(&ctx.pool, did, &ctx.lease_owner).await;
                (r, (*requester).to_owned(), None)
            }
        }
    }

    async fn requeue(&self, actor_id: i64, kind: i16, tier: i16, requester: &str, delay_s: u64) {
        if let Ok(mut conn) = self.ctx.pool.acquire().await {
            let job_kind = match kind {
                2 => farsight_storage::queue::JobKind::ListFetch,
                3 => farsight_storage::queue::JobKind::Discovery,
                _ => farsight_storage::queue::JobKind::Repo,
            };
            let _ = farsight_storage::queue::enqueue(
                &mut conn,
                actor_id,
                job_kind,
                tier,
                farsight_storage::repo_events::priority::NORMAL,
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
    async fn dry_run(&self, work: &Work) -> (JobResult, String, Option<String>) {
        let (actor, requester) = match work {
            Work::Queue {
                actor_id,
                requester,
                ..
            } => (Some(*actor_id), requester.clone()),
            Work::List { .. } => (None, crate::jobs::list_phase1::SYSTEM_LISTS.to_owned()),
            Work::Member { requester, .. } => (None, (*requester).to_owned()),
        };
        if let Some(a) = actor {
            let _ = sqlx::query(
                "INSERT INTO backfill_state (actor_id, state, backfilled_at) VALUES ($1, 3, clock_timestamp())
                 ON CONFLICT (actor_id) DO UPDATE SET state = 3, backfilled_at = clock_timestamp()",
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
    pub fn note_priority(&self, requester: &str, high: bool) {
        let mut s = self.st();
        let e = s.streak.entry(requester.to_owned()).or_insert(0);
        if high {
            *e += 1;
        } else {
            *e = 0;
        }
    }

    /// Publishes queue depth and throughput gauges.
    pub async fn publish_gauges(&self) {
        if let Ok(rows) = sqlx::query_as::<_, (i16, i64)>(
            "SELECT tier, count(*) FROM backfill_queue GROUP BY tier",
        )
        .fetch_all(&self.ctx.pool)
        .await
        {
            for t in 1..=3i16 {
                let n = rows.iter().find(|(x, _)| *x == t).map_or(0, |(_, n)| *n);
                metrics::gauge!(m::QUEUE_DEPTH, "tier" => t.to_string()).set(n as f64);
            }
        }
        metrics::gauge!(m::REPOS_PER_HOUR).set(self.repos_last_hour() as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares_are_minimums() {
        // All tiers busy: the one furthest below its share goes next.
        // 20/60 = 0.33, 8/25 = 0.32, 5/15 = 0.33: tier 2 is furthest below.
        assert_eq!(pick_tier([20, 8, 5], [60, 25, 15], [true; 3]), Some(1));
        assert_eq!(pick_tier([10, 8, 5], [60, 25, 15], [true; 3]), Some(0));
        // Unused share flows: only tier 3 has work.
        assert_eq!(
            pick_tier([0, 0, 30], [60, 25, 15], [false, false, true]),
            Some(2)
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
