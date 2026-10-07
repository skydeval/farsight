//! List-job lanes within `system:lists` (see `docs/design/backfill.md`):
//! phase-1 checks and `list_fetch` runs share one deficit round-robin over
//! admission keys (`list_sched_keys`) plus a shared fallback lane every
//! waiting list is in. A list is served by whichever of its lanes reaches
//! it first; within a lane the oldest admission goes first; a lane whose
//! head's host has no capacity serves its next item whose host has, or
//! yields its turn.

use farsight_storage::codes::sql::{
    JOB_LIST_FETCH, TRACK_MISSING, TRACK_SERVED, TRACK_UNFETCHED, TRACK_WAITING,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use farsight_core::Did;
use farsight_storage::ids::{ActorId, ListId, QueueId};
use sqlx::PgPool;

/// The shared fallback lane.
pub const FALLBACK: &str = "*fallback";
const LOAD_LIMIT: i64 = 5000;

/// A waiting list-job item.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Item {
    /// Phase 1 (record check and gate) for a list.
    Phase1 {
        /// `lists.id`.
        list_id: ListId,
    },
    /// A fetch run for an owner (its `backfill_queue` entry).
    Fetch {
        /// `backfill_queue.id`.
        queue_id: QueueId,
        /// Owner `actors.id`.
        owner_id: ActorId,
        /// The owner's DID as stored in `actors.did`, not parsed (see
        /// [`owner_did`]).
        owner: String,
    },
}

/// An item with its lanes, age and host.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// What would run if this candidate is picked.
    pub item: Item,
    /// Admission time (lane order: oldest first).
    pub admitted_at: Option<DateTime<Utc>>,
    /// Owner's PDS host, if resolved.
    pub host: Option<String>,
    /// Lanes it is eligible in (always includes the fallback lane).
    pub lanes: BTreeSet<String>,
}

/// Loads the waiting items: due `list_jobs` rows, `missing` lists whose
/// re-check is due, and due `list_fetch` queue entries.
pub async fn load(pool: &PgPool) -> Result<Vec<Candidate>, sqlx::Error> {
    type P1 = (ListId, Option<DateTime<Utc>>, Option<String>);
    // Ordered as a lane serves (oldest admission first, lists never
    // admitted before those), so when more wait than are loaded, the ones
    // left out are the youngest and the same ones on every load.
    let p1: Vec<P1> = sqlx::query_as(
        &format!("SELECT id, admitted_at, host FROM (
           SELECT l.id, l.admitted_at, h.host FROM list_jobs j
           JOIN lists l ON l.id = j.list_id JOIN actors o ON o.id = l.owner_id
           LEFT JOIN pds_hosts h ON h.id = o.pds_host_id
           WHERE (j.not_before IS NULL OR j.not_before <= now()) AND l.track_state IN {TRACK_WAITING}
           UNION ALL
           SELECT l.id, l.admitted_at, h.host FROM lists l
           JOIN actors o ON o.id = l.owner_id LEFT JOIN pds_hosts h ON h.id = o.pds_host_id
           WHERE l.track_state = {TRACK_MISSING} AND (l.next_retry_at IS NULL OR l.next_retry_at <= now())
             AND NOT EXISTS (SELECT 1 FROM list_jobs j WHERE j.list_id = l.id)) w
         ORDER BY admitted_at NULLS FIRST, id LIMIT $1"),
    )
    .bind(LOAD_LIMIT)
    .fetch_all(pool)
    .await?;
    type F = (
        QueueId,
        ActorId,
        String,
        Option<DateTime<Utc>>,
        Option<String>,
    );
    let fetch: Vec<F> = sqlx::query_as(&format!(
        "SELECT q.id, q.actor_id, a.did,
                (SELECT min(l.admitted_at) FROM lists l WHERE l.owner_id = q.actor_id
                   AND ((l.track_state IN {TRACK_UNFETCHED} AND l.phase1_epoch = l.admit_epoch)
                        OR (l.track_state IN {TRACK_SERVED} AND l.refresh_requested))),
                h.host
         FROM backfill_queue q JOIN actors a ON a.id = q.actor_id
         LEFT JOIN pds_hosts h ON h.id = a.pds_host_id
         WHERE q.kind = {JOB_LIST_FETCH} AND (q.not_before IS NULL OR q.not_before <= now())
         ORDER BY q.enqueued_at, q.id LIMIT $1"
    ))
    .bind(LOAD_LIMIT)
    .fetch_all(pool)
    .await?;
    let list_ids: Vec<ListId> = p1.iter().map(|r| r.0).collect();
    let owners: Vec<ActorId> = fetch.iter().map(|r| r.1).collect();
    let lane_rows: Vec<(ListId, String)> =
        sqlx::query_as("SELECT list_id, key FROM list_sched_keys WHERE list_id = ANY($1)")
            .bind(&list_ids)
            .fetch_all(pool)
            .await?;
    let mut by_list: HashMap<ListId, BTreeSet<String>> = HashMap::new();
    for (l, k) in lane_rows {
        by_list.entry(l).or_default().insert(k);
    }
    // A run is eligible under the union of its claimable lists' lanes;
    // refresh-only runs use the fallback lane.
    let owner_lanes: Vec<(ActorId, String)> = sqlx::query_as(
        &format!("SELECT DISTINCT l.owner_id, k.key FROM lists l JOIN list_sched_keys k ON k.list_id = l.id
         WHERE l.owner_id = ANY($1) AND l.track_state IN {TRACK_UNFETCHED} AND l.phase1_epoch = l.admit_epoch"),
    )
    .bind(&owners)
    .fetch_all(pool)
    .await?;
    let mut by_owner: HashMap<ActorId, BTreeSet<String>> = HashMap::new();
    for (o, k) in owner_lanes {
        by_owner.entry(o).or_default().insert(k);
    }
    let mut out = Vec::with_capacity(p1.len() + fetch.len());
    let mut seen = HashSet::new();
    for (id, at, host) in p1 {
        if !seen.insert(id) {
            continue;
        }
        let mut lanes = by_list.remove(&id).unwrap_or_default();
        lanes.insert(FALLBACK.to_owned());
        out.push(Candidate {
            item: Item::Phase1 { list_id: id },
            admitted_at: at,
            host,
            lanes,
        });
    }
    for (qid, owner_id, did, at, host) in fetch {
        let mut lanes = by_owner.remove(&owner_id).unwrap_or_default();
        lanes.insert(FALLBACK.to_owned());
        out.push(Candidate {
            item: Item::Fetch {
                queue_id: qid,
                owner_id,
                owner: did,
            },
            admitted_at: at,
            host,
            lanes,
        });
    }
    Ok(out)
}

/// Deficit state of the lanes (charged in outbound requests).
#[derive(Debug, Default)]
pub struct Lanes {
    charged: HashMap<String, f64>,
}

impl Lanes {
    /// Picks the next item: the least-charged lane with a servable item
    /// serves its oldest servable item. `servable` says whether an item's
    /// host has capacity and it is not already running.
    pub fn pick(
        &mut self,
        candidates: &[Candidate],
        servable: impl Fn(&Candidate) -> bool,
    ) -> Option<(Candidate, String)> {
        let mut lanes: BTreeMap<&str, Vec<&Candidate>> = BTreeMap::new();
        for c in candidates {
            for l in &c.lanes {
                lanes.entry(l.as_str()).or_default().push(c);
            }
        }
        // New lanes start at the current minimum (no burst advantage).
        let floor = lanes
            .keys()
            .filter_map(|l| self.charged.get(*l))
            .copied()
            .fold(f64::INFINITY, f64::min);
        let floor = if floor.is_finite() { floor } else { 0.0 };
        for l in lanes.keys() {
            self.charged.entry((*l).to_owned()).or_insert(floor);
        }
        let mut order: Vec<(&str, f64)> = lanes.keys().map(|l| (*l, self.charged[*l])).collect();
        order.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(b.0))
        });
        for (lane, _) in order {
            let mut items = lanes.remove(lane).unwrap_or_default();
            items.sort_by(|a, b| a.admitted_at.cmp(&b.admitted_at).then(a.item.cmp(&b.item)));
            // Blocked head: serve the next item whose host has capacity;
            // none ⇒ the lane yields its turn.
            if let Some(c) = items.into_iter().find(|c| servable(c)) {
                return Some((c.clone(), lane.to_owned()));
            }
        }
        None
    }

    /// Charges `cost` requests to `lane`.
    pub fn charge(&mut self, lane: &str, cost: u64) {
        *self.charged.entry(lane.to_owned()).or_insert(0.0) += cost.max(1) as f64;
    }
}

/// A fetch item's owner DID, parsed.
pub fn owner_did(item: &Item) -> Option<Did> {
    match item {
        Item::Fetch { owner, .. } => Did::parse(owner).ok(),
        Item::Phase1 { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(id: i64, lanes: &[&str], age: i64, host: &str) -> Candidate {
        Candidate {
            item: Item::Phase1 {
                list_id: ListId::new(id),
            },
            admitted_at: Some(DateTime::<Utc>::from_timestamp(1_800_000_000 + age, 0).unwrap()),
            host: Some(host.into()),
            lanes: lanes
                .iter()
                .map(|s| (*s).to_owned())
                .chain([FALLBACK.to_owned()])
                .collect(),
        }
    }

    #[test]
    fn flooder_cannot_starve_other_keys() {
        let mut l = Lanes::default();
        // 50 lists of a flooding key, one list of another key, older first.
        let mut c: Vec<Candidate> = (0..50)
            .map(|i| cand(i, &["bucket:evil"], i, "h1"))
            .collect();
        c.push(cand(99, &["bucket:good"], 1000, "h2"));
        let mut served = Vec::new();
        for _ in 0..4 {
            let (pick, lane) = l.pick(&c, |_| true).unwrap();
            l.charge(&lane, 10);
            if let Item::Phase1 { list_id } = pick.item {
                served.push(list_id);
                c.retain(|x| x.item != pick.item);
            }
        }
        assert!(served.contains(&ListId::new(99)), "{served:?}");
    }

    #[test]
    fn blocked_head_is_skipped() {
        let mut l = Lanes::default();
        let c = vec![cand(1, &["k"], 0, "slow"), cand(2, &["k"], 1, "fast")];
        let (p, _) = l.pick(&c, |c| c.host.as_deref() != Some("slow")).unwrap();
        assert_eq!(
            p.item,
            Item::Phase1 {
                list_id: ListId::new(2)
            }
        );
        assert!(
            l.pick(&c[..1], |c| c.host.as_deref() != Some("slow"))
                .is_none()
        );
    }
}
