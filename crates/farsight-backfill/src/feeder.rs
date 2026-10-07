//! The debt feeder (see `docs/design/coverage.md` and
//! `docs/design/backfill.md`): turns `relist_debt` rows into
//! `system:resync` re-list jobs when, and only when, the re-list can clear
//! them, and enqueues `list_fetch` runs for list work whose queue entry was
//! dropped at the system-queue cap. Nothing loops: a debt whose cause still
//! applies waits, a daily-rate debt is fed once per UTC day, and every
//! other debt at most once per [`MIN_REFEED`] (a re-list that ended
//! complete-with-debts does not clear, so without the bound it would be
//! re-fed at once).

use farsight_storage::codes::sql::{
    JOB_LIST_FETCH, JOB_REPO, REPO_FAILED, TRACK_SERVED, TRACK_UNFETCHED,
};
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use farsight_core::Did;
use farsight_storage::codes::{CapType, DebtReason, Priority, RequesterKey, Tier};
use farsight_storage::ids::ActorId;
use farsight_storage::keys::{self, CapKind, HostFacts, Limits};
use farsight_storage::queue::{self, Enqueued, JobKind};
use farsight_storage::txn::Gates;

use crate::ctx::Ctx;

/// Debts examined per pass.
const PASS: i64 = 2000;
/// Minimum interval between two re-lists fed for the same non-daily debt.
pub const MIN_REFEED: Duration = Duration::from_secs(3600);
/// Hysteresis for per-author caps (under 90%).
const UNDER: f64 = 0.9;

/// What the feeder knows about one debt.
#[derive(Debug, Clone, PartialEq)]
pub struct DebtView {
    /// `relist_debt.reason`: which rule of [`eligible`] applies.
    pub reason: DebtReason,
    /// Cap or rate named by the debt.
    pub cap_type: Option<CapType>,
    /// `relist_debt.since_witness`: the witness time the debt dates from.
    /// A run whose coverage point is at or after it has covered the debt.
    pub since: DateTime<Utc>,
    /// Author is on a large host.
    pub large: bool,
    /// OR of the author's buckets' `capped_mask`.
    pub mask: i16,
    /// Author counters: blocks, listblocks, lists, fetch triggers.
    pub authored: [i64; 4],
    /// Admissions used / limit today by the author's admission key.
    pub admissions: (i64, i64),
    /// Interns charged / limit today to the author's cause key.
    pub interns: (i64, i64),
    /// `backfill_state.backfilled_at` (server time): when the author's
    /// last run that listed to the end, or found the account inactive,
    /// finished. A failed run does not move it. `None` before the first.
    pub last_run: Option<DateTime<Utc>>,
    /// `backfill_state.backfilled_witness`: the coverage point of that
    /// run, on the witness clock.
    pub last_point: Option<DateTime<Utc>>,
    /// `backfill_state.next_attempt_at` (failed runs).
    pub next_attempt: Option<DateTime<Utc>>,
}

/// Whether a run now would be deletes-only (the budget gate).
fn deletes_only(gates: Gates, large: bool) -> bool {
    gates.ceiling_refusing || (gates.budget_refusing && !large)
}

fn bucket_bit(c: CapType) -> Option<i16> {
    Some(match c {
        CapType::HostBlocks => CapKind::Blocks.bit(),
        CapType::HostListItems => CapKind::Items.bit(),
        CapType::HostListblocks => CapKind::Listblocks.bit(),
        CapType::HostLists => CapKind::Lists.bit(),
        CapType::InternLifetime => CapKind::Interned.bit(),
        _ => return None,
    })
}

const CONTENT_BITS: i16 = 1 | 2 | 4 | 8;

/// Eligibility of one debt at `now` (UTC) under `gates` and `limits`.
pub fn eligible(d: &DebtView, gates: Gates, limits: &Limits, now: DateTime<Utc>) -> bool {
    let today: NaiveDate = now.date_naive();
    let ran_today = d.last_run.is_some_and(|t| t.date_naive() == today);
    let refed_recently = d
        .last_run
        .is_some_and(|t| (now - t).to_std().unwrap_or_default() < MIN_REFEED);
    let has_rate = |(used, limit): (i64, i64)| used < limit;
    match d.reason {
        // Retry schedule: the failed run's `next_attempt_at`, or (a
        // reconcile skip) the refeed bound.
        DebtReason::Unreachable => match d.next_attempt {
            Some(t) => t <= now,
            None => !refed_recently,
        },
        // Immediately, unless the run would be deletes-only or its bucket
        // is closed; once per cause (a run whose point covers the debt
        // ended without clearing it: other debts hold it, they feed it).
        DebtReason::Resync => {
            let covered = d.last_point.is_some_and(|p| p >= d.since);
            !deletes_only(gates, d.large) && d.mask & CONTENT_BITS == 0 && !covered
        }
        DebtReason::Capped => {
            let c = d.cap_type;
            if c.is_some_and(CapType::is_daily_rate) {
                let rate = match c {
                    Some(CapType::InternRate) => d.interns,
                    _ => d.admissions,
                };
                return !ran_today && has_rate(rate) && !deletes_only(gates, d.large);
            }
            let cfg = &limits.cfg;
            let under = |n: i64, cap: u64| (n as f64) < UNDER * cap as f64;
            let cap_clear = match c {
                Some(CapType::BlocksPerAuthor) => under(d.authored[0], cfg.blocks_per_author),
                Some(CapType::ListblocksPerAuthor) => {
                    under(d.authored[1], cfg.listblocks_per_author)
                }
                Some(CapType::ListsPerAuthor) => under(d.authored[2], cfg.lists_per_author),
                Some(CapType::TriggerCap) => {
                    under(d.authored[3], cfg.listblock_fetch_triggers_per_author)
                }
                _ => {
                    under(d.authored[0], cfg.blocks_per_author)
                        && under(d.authored[1], cfg.listblocks_per_author)
                        && under(d.authored[2], cfg.lists_per_author)
                }
            };
            cap_clear && has_rate(d.admissions) && !refed_recently && !deletes_only(gates, d.large)
        }
        DebtReason::Refused => {
            let open = match d.cap_type {
                Some(CapType::Budget) | Some(CapType::DeletesOnly) => !deletes_only(gates, d.large),
                Some(CapType::Ceiling) => !gates.ceiling_refusing,
                Some(c) => match bucket_bit(c) {
                    Some(bit) => d.mask & bit == 0 && !deletes_only(gates, d.large),
                    None => !deletes_only(gates, d.large),
                },
                None => d.mask & CONTENT_BITS == 0 && !deletes_only(gates, d.large),
            };
            open && !refed_recently
        }
    }
}

#[derive(sqlx::FromRow)]
struct Row {
    actor_id: ActorId,
    did: String,
    reason: i16,
    cap_type: Option<i16>,
    since_witness: DateTime<Utc>,
    admission_key: Option<String>,
    resolve_failures: i32,
    resolved: bool,
    cap_key: Option<String>,
    ip_bucket: Option<String>,
    large: bool,
    authored_blocks: i32,
    authored_listblocks: i32,
    authored_lists: i32,
    fetch_triggers: i32,
    backfilled_at: Option<DateTime<Utc>>,
    backfilled_witness: Option<DateTime<Utc>>,
    next_attempt_at: Option<DateTime<Utc>>,
}

/// One feeder pass. Returns the number of jobs enqueued.
pub async fn pass(ctx: &Ctx) -> Result<u64, farsight_storage::StorageError> {
    let pool = &ctx.pool;
    let limits = ctx.limits();
    let gates = ctx.gates.load();
    let now: DateTime<Utc> = crate::jobs::db_now(pool).await?;
    let cap = Some(limits.system_queue_cap);
    // Actors with a debt, not running and without a waiting repo entry,
    // oldest debt first.
    let rows: Vec<Row> = sqlx::query_as(
            &format!("SELECT d.actor_id, a.did, d.reason, d.cap_type, d.since_witness,
                    a.admission_key, a.resolve_failures, a.pds_host_id IS NOT NULL AS resolved,
                    h.cap_key, h.ip_bucket, COALESCE(h.large, false) AS large,
                    a.authored_blocks, a.authored_listblocks, a.authored_lists, a.fetch_triggers,
                    b.backfilled_at, b.backfilled_witness,
                    CASE WHEN b.state = {REPO_FAILED} THEN b.next_attempt_at END AS next_attempt_at
             FROM relist_debt d JOIN actors a ON a.id = d.actor_id
             LEFT JOIN pds_hosts h ON h.id = a.pds_host_id
             LEFT JOIN backfill_state b ON b.actor_id = d.actor_id
             WHERE NOT EXISTS (SELECT 1 FROM backfill_queue q WHERE q.actor_id = d.actor_id AND q.kind = {JOB_REPO})
               AND NOT EXISTS (SELECT 1 FROM job_leases j WHERE j.did = a.did AND j.lease_until > now())
             ORDER BY d.created_at LIMIT $1"),
        )
        .bind(PASS)
        .fetch_all(pool)
        .await?;
    let mut fed = 0u64;
    let mut done: std::collections::HashSet<ActorId> = std::collections::HashSet::new();
    let today = now.date_naive();
    for r in rows {
        let actor = r.actor_id;
        if done.contains(&actor) {
            continue;
        }
        let Some(reason) = DebtReason::from_code(r.reason) else {
            continue;
        };
        let Ok(d) = Did::parse(&r.did) else { continue };
        let facts = HostFacts {
            admission_key: r.admission_key,
            resolve_failures: r.resolve_failures,
            cap_key: r.cap_key,
            ip_bucket: r.ip_bucket,
            large: r.large,
            resolved: r.resolved,
        };
        let key = keys::admission_key(&d, &facts);
        let buckets = keys::buckets(&d, &facts);
        let mask: i16 = sqlx::query_scalar(
            "SELECT COALESCE(bit_or(capped_mask), 0)::SMALLINT FROM host_usage WHERE bucket = ANY($1)",
        )
        .bind(&buckets)
        .fetch_one(pool)
        .await?;
        let (adm, int): (i64, i64) = sqlx::query_as(
            "SELECT COALESCE((SELECT admissions::BIGINT FROM admission_rate WHERE key = $1 AND utc_day = $2), 0),
                    COALESCE((SELECT n FROM intern_rate WHERE key = $1 AND utc_day = $2), 0)",
        )
        .bind(&key)
        .bind(today)
        .fetch_one(pool)
        .await?;
        let view = DebtView {
            reason,
            cap_type: r.cap_type.and_then(CapType::from_code),
            since: r.since_witness,
            large: keys::is_large(&facts),
            mask,
            authored: [
                r.authored_blocks,
                r.authored_listblocks,
                r.authored_lists,
                r.fetch_triggers,
            ]
            .map(i64::from),
            admissions: (adm, limits.admission_limit(&key)),
            interns: (int, limits.intern_limit(&key)),
            last_run: r.backfilled_at,
            last_point: r.backfilled_witness,
            next_attempt: r.next_attempt_at,
        };
        if !eligible(&view, gates, &limits, now) {
            continue;
        }
        let mut conn = pool.acquire().await?;
        match queue::enqueue(
            &mut conn,
            actor,
            JobKind::Repo,
            Tier::OnDemand,
            Priority::Normal,
            RequesterKey::Resync,
            cap,
        )
        .await?
        {
            Enqueued::Waiting => {
                fed += 1;
                done.insert(actor);
            }
            // The system queue is full: the debts stay (counted) and are
            // fed as capacity frees.
            Enqueued::CapReached => return Ok(fed),
        }
    }
    fed += feed_list_fetches(ctx, &limits).await?;
    Ok(fed)
}

/// `list_fetch` work whose queue entry was dropped at the system cap:
/// owners with phase-1-passed unclaimed lists, or refresh-requested
/// tracked lists past the owner cooldown, and no waiting entry.
async fn feed_list_fetches(
    ctx: &Ctx,
    limits: &Limits,
) -> Result<u64, farsight_storage::StorageError> {
    let pool = &ctx.pool;
    let cooldown = ctx.cfg().backfill.owner_fetch_cooldown.get().as_secs_f64();
    let owners: Vec<ActorId> = sqlx::query_scalar(
        &format!("SELECT DISTINCT l.owner_id FROM lists l
         WHERE ((l.track_state IN {TRACK_UNFETCHED} AND l.phase1_epoch = l.admit_epoch AND l.fetch_run_id IS NULL)
                OR (l.track_state IN {TRACK_SERVED} AND l.refresh_requested))
           AND NOT EXISTS (SELECT 1 FROM backfill_queue q WHERE q.actor_id = l.owner_id AND q.kind = {JOB_LIST_FETCH})
           AND NOT EXISTS (SELECT 1 FROM list_fetch_runs r WHERE r.owner_id = l.owner_id
                             AND (r.finished_at IS NULL
                                  OR (r.started_at > now() - make_interval(secs => $2)
                                      AND l.track_state IN {TRACK_SERVED})))
         LIMIT $1"),
    )
    .bind(PASS)
    .bind(cooldown)
    .fetch_all(pool)
    .await?;
    let mut n = 0;
    let mut conn = pool.acquire().await?;
    for o in owners {
        match queue::enqueue(
            &mut conn,
            o,
            JobKind::ListFetch,
            Tier::OnDemand,
            Priority::Normal,
            RequesterKey::Lists,
            Some(limits.system_queue_cap),
        )
        .await?
        {
            Enqueued::Waiting => n += 1,
            Enqueued::CapReached => break,
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn base(reason: DebtReason, cap: Option<CapType>) -> DebtView {
        DebtView {
            reason,
            cap_type: cap,
            since: t("2026-10-01T10:00:00Z"),
            large: false,
            mask: 0,
            authored: [0; 4],
            admissions: (0, 100),
            interns: (0, 100),
            last_run: None,
            last_point: None,
            next_attempt: None,
        }
    }

    const NOW: &str = "2026-10-01T12:00:00Z";
    fn open() -> Gates {
        Gates::default()
    }
    fn budget() -> Gates {
        Gates {
            budget_refusing: true,
            ..Gates::default()
        }
    }

    #[test]
    fn resync_waits_on_gates_and_feeds_once() {
        let l = Limits::defaults();
        let d = base(DebtReason::Resync, None);
        assert!(eligible(&d, open(), &l, t(NOW)));
        // Deletes-only run (budget gate, non-large host): wait.
        assert!(!eligible(&d, budget(), &l, t(NOW)));
        // Large hosts are not deletes-only under the budget gate.
        assert!(eligible(
            &DebtView {
                large: true,
                ..d.clone()
            },
            budget(),
            &l,
            t(NOW)
        ));
        // Bucket closed: wait.
        assert!(!eligible(
            &DebtView {
                mask: 1,
                ..d.clone()
            },
            open(),
            &l,
            t(NOW)
        ));
        // A run whose point covers the debt ended without clearing it:
        // not re-fed (the other debts feed it).
        let covered = DebtView {
            last_point: Some(t("2026-10-01T10:00:00Z")),
            ..d.clone()
        };
        assert!(!eligible(&covered, open(), &l, t(NOW)));
        // A newer cause (since raised) makes it eligible again.
        let newer = DebtView {
            since: t("2026-10-01T11:00:00Z"),
            ..covered
        };
        assert!(eligible(&newer, open(), &l, t(NOW)));
    }

    #[test]
    fn capped_needs_hysteresis_and_rate() {
        let l = Limits::defaults();
        let cap = l.cfg.blocks_per_author as i64;
        let d = base(DebtReason::Capped, Some(CapType::BlocksPerAuthor));
        // At 95% of the cap: still waiting (needs < 90%).
        let at95 = DebtView {
            authored: [cap * 95 / 100, 0, 0, 0],
            ..d.clone()
        };
        assert!(!eligible(&at95, open(), &l, t(NOW)));
        let at80 = DebtView {
            authored: [cap * 80 / 100, 0, 0, 0],
            ..d.clone()
        };
        assert!(eligible(&at80, open(), &l, t(NOW)));
        // No admission rate left today: wait for the next UTC day.
        assert!(!eligible(
            &DebtView {
                admissions: (100, 100),
                ..at80.clone()
            },
            open(),
            &l,
            t(NOW)
        ));
        // Fed within the hour: no loop.
        let recent = DebtView {
            last_run: Some(t("2026-10-01T11:30:00Z")),
            ..at80
        };
        assert!(!eligible(&recent, open(), &l, t(NOW)));
    }

    #[test]
    fn daily_rate_debts_once_per_utc_day() {
        let l = Limits::defaults();
        let d = base(DebtReason::Capped, Some(CapType::InternRate));
        assert!(eligible(&d, open(), &l, t(NOW)));
        let ran_today = DebtView {
            last_run: Some(t("2026-10-01T00:05:00Z")),
            ..d.clone()
        };
        assert!(!eligible(&ran_today, open(), &l, t(NOW)));
        // Ran yesterday: eligible again just after midnight UTC.
        let ran_yesterday = DebtView {
            last_run: Some(t("2026-09-30T23:59:00Z")),
            ..d.clone()
        };
        assert!(eligible(
            &ran_yesterday,
            open(),
            &l,
            t("2026-10-01T00:01:00Z")
        ));
        // The rate it waits on is the intern rate, not admissions.
        assert!(!eligible(
            &DebtView {
                interns: (100, 100),
                ..d.clone()
            },
            open(),
            &l,
            t(NOW)
        ));
        assert!(eligible(
            &DebtView {
                admissions: (100, 100),
                ..d
            },
            open(),
            &l,
            t(NOW)
        ));
    }

    #[test]
    fn refused_waits_on_its_specific_gate() {
        let l = Limits::defaults();
        let d = base(DebtReason::Refused, Some(CapType::HostListItems));
        // Only the items bit matters.
        assert!(!eligible(
            &DebtView {
                mask: 2,
                ..d.clone()
            },
            open(),
            &l,
            t(NOW)
        ));
        assert!(eligible(
            &DebtView {
                mask: 1,
                ..d.clone()
            },
            open(),
            &l,
            t(NOW)
        ));
        let b = base(DebtReason::Refused, Some(CapType::Budget));
        assert!(!eligible(&b, budget(), &l, t(NOW)));
        assert!(eligible(&b, open(), &l, t(NOW)));
        let c = base(DebtReason::Refused, Some(CapType::Ceiling));
        let ceiling = Gates {
            ceiling_refusing: true,
            ..Gates::default()
        };
        assert!(!eligible(&c, ceiling, &l, t(NOW)));
        // Deletes-only debts wait on the budget gate.
        let dl = base(DebtReason::Refused, Some(CapType::DeletesOnly));
        assert!(!eligible(&dl, budget(), &l, t(NOW)));
    }

    #[test]
    fn unreachable_follows_retry_schedule() {
        let l = Limits::defaults();
        let d = base(DebtReason::Unreachable, None);
        let later = DebtView {
            next_attempt: Some(t("2026-10-01T13:00:00Z")),
            ..d.clone()
        };
        assert!(!eligible(&later, open(), &l, t(NOW)));
        let due = DebtView {
            next_attempt: Some(t("2026-10-01T11:00:00Z")),
            ..d.clone()
        };
        assert!(eligible(&due, open(), &l, t(NOW)));
        // Reconcile-skip debts (no failed run): refeed bound.
        let recent = DebtView {
            last_run: Some(t("2026-10-01T11:45:00Z")),
            ..d
        };
        assert!(!eligible(&recent, open(), &l, t(NOW)));
    }
}
