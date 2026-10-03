//! Handle warming (design §3.6, §8.6): a background worker verifies the
//! handles of accounts that pages had to show as bare DIDs, so that a
//! later view shows the handle.
//!
//! Rendering a row never waits for an outbound request. A row whose
//! account has no live cache entry — or a verified handle about to expire
//! — is put on the **warming queue**; one worker takes DIDs from it and
//! verifies them as a page verifies its own subject.
//!
//! - The queue is memory: at most [`QUEUE_CAP`] DIDs, no duplicates. The
//!   newest request is served first (the page someone is looking at now
//!   matters more than one from ten minutes ago), a DID asked for again
//!   moves to the front, and when the queue is full the oldest entry is
//!   dropped. Within one page the first row is served first.
//! - The worker draws on the process-wide handle budget and takes a token
//!   only while the bucket holds more than [`RESERVE`], so requests always
//!   find some. It cannot raise the server's outbound rate.
//! - No table is scanned and nothing is read from the database.
//! - `public_ui.handle_warming_enabled = false`: pages queue nothing, the
//!   queue is emptied, the worker idles.
//!
//! What it cannot do: the first view of a page nobody has rendered still
//! shows DIDs, and a restart empties the cache and the queue.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use farsight_api::ratelimit::Class;
use farsight_core::Did;
use farsight_core::config::Config;
use farsight_storage::handles::Cached;
use tokio::sync::{Notify, Semaphore, watch};

use super::handles::{self, BUDGET_KEY, NEGATIVE_TTL};
use crate::pages::WebState;

/// Most DIDs the queue holds.
pub const QUEUE_CAP: usize = 2_000;
/// Tokens of the handle budget the worker leaves for requests.
pub const RESERVE: f64 = 5.0;
/// How long the worker waits when the budget has nothing to spare.
pub const BUDGET_WAIT: Duration = Duration::from_millis(500);
/// Deadline of one verification.
pub const VERIFY_DEADLINE: Duration = Duration::from_secs(5);
/// Verifications in flight at once.
pub const IN_FLIGHT: usize = 2;
/// How often an idle or switched-off worker looks at the setting.
pub const IDLE_POLL: Duration = Duration::from_secs(5);

/// `farsight_handle_warming_total{outcome}`.
pub const WARMING: &str = "farsight_handle_warming_total";
/// `farsight_handle_warming_queue`: DIDs waiting.
pub const QUEUE: &str = "farsight_handle_warming_queue";

/// How a queued DID ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Verified in both directions and cached.
    Resolved,
    /// The document names a handle that does not resolve back to the DID.
    Unverified,
    /// The document could not be read, names no handle, or the deadline
    /// passed.
    Failed,
    /// The cache had a live entry by the time the worker reached it.
    Cached,
    /// Pushed out of a full queue.
    Dropped,
}

impl Outcome {
    /// Every outcome.
    pub const ALL: [Outcome; 5] = [
        Outcome::Resolved,
        Outcome::Unverified,
        Outcome::Failed,
        Outcome::Cached,
        Outcome::Dropped,
    ];

    /// Metric label.
    pub fn label(self) -> &'static str {
        match self {
            Outcome::Resolved => "resolved",
            Outcome::Unverified => "unverified",
            Outcome::Failed => "failed",
            Outcome::Cached => "cached",
            Outcome::Dropped => "dropped",
        }
    }
}

fn count(o: Outcome, n: u64) {
    ::metrics::counter!(WARMING, "outcome" => o.label()).increment(n);
}

/// Registers the series at zero.
pub fn register() {
    for o in Outcome::ALL {
        count(o, 0);
    }
    ::metrics::gauge!(QUEUE).set(0.0);
}

#[derive(Debug, Default)]
struct Inner {
    // Front = served next.
    order: VecDeque<String>,
    set: HashSet<String>,
    // Taken by the worker and not finished: asking for one of these again
    // would verify it twice.
    taken: HashSet<String>,
}

/// The warming queue.
#[derive(Debug, Default)]
pub struct WarmQueue {
    inner: Mutex<Inner>,
    woken: Notify,
}

impl WarmQueue {
    /// Queues the DIDs one page asked for, in the page's row order, ahead
    /// of everything already waiting. Returns how many entries a full
    /// queue dropped.
    pub fn push_page(&self, dids: Vec<String>) -> usize {
        if dids.is_empty() {
            return 0;
        }
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let inner = &mut *g;
        // Last row first, so that the page's first row ends at the front.
        for did in dids.into_iter().rev() {
            if inner.taken.contains(&did) {
                continue;
            }
            if !inner.set.insert(did.clone()) {
                // Asked for again: it moves to the front.
                if let Some(i) = inner.order.iter().position(|d| *d == did) {
                    inner.order.remove(i);
                }
            }
            inner.order.push_front(did);
        }
        let mut dropped = 0;
        while inner.order.len() > QUEUE_CAP {
            if let Some(old) = inner.order.pop_back() {
                inner.set.remove(&old);
                dropped += 1;
            }
        }
        let len = inner.order.len();
        drop(g);
        ::metrics::gauge!(QUEUE).set(len as f64);
        if dropped > 0 {
            count(Outcome::Dropped, dropped as u64);
        }
        self.woken.notify_one();
        dropped
    }

    /// Takes the DID to serve next. Until [`WarmQueue::finished`] is
    /// called for it, asking for it again queues nothing.
    pub fn pop(&self) -> Option<String> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let did = g.order.pop_front()?;
        g.set.remove(&did);
        g.taken.insert(did.clone());
        let len = g.order.len();
        drop(g);
        ::metrics::gauge!(QUEUE).set(len as f64);
        Some(did)
    }

    /// The worker is done with a DID it took: its answer is in the cache,
    /// or it was not worth asking.
    pub fn finished(&self, did: &str) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.taken.remove(did);
    }

    /// Puts a DID the worker could not serve yet back at the front.
    fn put_back(&self, did: String) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.taken.remove(&did);
        if g.set.insert(did.clone()) {
            g.order.push_front(did);
        }
        let len = g.order.len();
        drop(g);
        ::metrics::gauge!(QUEUE).set(len as f64);
    }

    /// Empties the queue.
    pub fn clear(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.order.clear();
        g.set.clear();
        drop(g);
        ::metrics::gauge!(QUEUE).set(0.0);
    }

    /// DIDs waiting.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .order
            .len()
    }

    /// Nothing waiting.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Whether a row showing `did` asks the worker for it: the cache has no
/// live entry, or a verified handle that expires within a quarter of
/// `ttl` (refreshed before expiry, so a handle on a page that keeps being
/// viewed does not blink back to a DID). A live negative entry is left
/// alone.
pub fn due(entry: Option<&(Cached, Duration)>, ttl: Duration) -> bool {
    match entry {
        None => true,
        Some((Cached::Handle(_), left)) => *left < ttl / 4,
        Some((Cached::None, _)) => false,
    }
}

/// The DIDs one page render asks the worker for, in row order.
#[derive(Debug)]
pub struct Asked {
    enabled: bool,
    ttl: Duration,
    dids: Vec<String>,
}

impl Asked {
    /// A collector for one render under `cfg`.
    pub fn new(cfg: &Config) -> Asked {
        Asked {
            enabled: cfg.public_ui.handle_warming_enabled,
            ttl: cfg.public_ui.handle_cache_ttl.get(),
            dids: Vec::new(),
        }
    }

    /// The cached handle of an account a cell shows, if there is one.
    /// Asks for the account when the cell has to show its DID, or its
    /// handle is about to expire. Never fetches anything.
    pub fn handle(&mut self, st: &WebState, did: &str) -> Option<String> {
        let entry = st.public.handles.lookup_remaining(did);
        if self.enabled && due(entry.as_ref(), self.ttl) {
            self.dids.push(did.to_owned());
        }
        match entry {
            Some((Cached::Handle(h), _)) => Some(h),
            _ => None,
        }
    }

    /// Hands what the render asked for to the queue.
    pub fn submit(self, st: &WebState) {
        st.public.warm.push_page(self.dids);
    }
}

async fn verify_one(st: Arc<WebState>, did: Did) {
    let cfg = st.api.config.current();
    let cfg = &cfg.config;
    let found = tokio::time::timeout(VERIFY_DEADLINE, handles::verify(&st.safe, cfg, &did)).await;
    let (outcome, handle) = match found {
        Ok((handles::Outcome::Resolved, Some(h))) => (Outcome::Resolved, Some(h)),
        Ok((handles::Outcome::Unverified, _)) => (Outcome::Unverified, None),
        _ => (Outcome::Failed, None),
    };
    // A failure is remembered too, so that the next render does not queue
    // the account again at once.
    let ttl = if handle.is_some() {
        cfg.public_ui.handle_cache_ttl.get()
    } else {
        NEGATIVE_TTL
    };
    st.public.handles.insert(did.as_str(), handle, ttl);
    count(outcome, 1);
}

async fn pause(stop: &mut watch::Receiver<bool>, d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => false,
        r = stop.changed() => r.is_err() || *stop.borrow(),
    }
}

/// The worker: one long-running task, started with the server's periodic
/// tasks. Ends when `stop` flips.
pub async fn run(st: Arc<WebState>, mut stop: watch::Receiver<bool>) {
    let slots = Arc::new(Semaphore::new(IN_FLIGHT));
    let queue = &st.public.warm;
    loop {
        if *stop.borrow() {
            return;
        }
        let cfg = st.api.config.current();
        if !cfg.config.public_ui.handle_warming_enabled {
            queue.clear();
            if pause(&mut stop, IDLE_POLL).await {
                return;
            }
            continue;
        }
        let Some(raw) = queue.pop() else {
            tokio::select! {
                _ = queue.woken.notified() => {}
                _ = tokio::time::sleep(IDLE_POLL) => {}
                r = stop.changed() => {
                    if r.is_err() || *stop.borrow() { return; }
                }
            }
            continue;
        };
        let Ok(did) = Did::parse(&raw) else {
            queue.finished(&raw);
            continue;
        };
        let entry = st.public.handles.lookup_remaining(did.as_str());
        if !due(entry.as_ref(), cfg.config.public_ui.handle_cache_ttl.get()) {
            queue.finished(&raw);
            count(Outcome::Cached, 1);
            continue;
        }
        // Pages come first: the worker gets what they leave.
        let limit = Class::PublicHandle.limit(&cfg.config, None);
        if !st
            .api
            .limiter
            .take_above(Class::PublicHandle, BUDGET_KEY, limit, RESERVE)
        {
            queue.put_back(raw);
            if pause(&mut stop, BUDGET_WAIT).await {
                return;
            }
            continue;
        }
        let Ok(slot) = slots.clone().acquire_owned().await else {
            return;
        };
        let st2 = st.clone();
        tokio::spawn(async move {
            verify_one(st2.clone(), did).await;
            st2.public.warm.finished(&raw);
            drop(slot);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(prefix: &str, n: usize) -> Vec<String> {
        (0..n).map(|i| format!("did:plc:{prefix}{i}")).collect()
    }

    #[test]
    fn newest_page_first_and_its_first_row_first() {
        let q = WarmQueue::default();
        assert_eq!(q.push_page(page("a", 3)), 0);
        assert_eq!(q.push_page(page("b", 2)), 0);
        let order: Vec<String> = std::iter::from_fn(|| q.pop()).collect();
        assert_eq!(
            order,
            [
                "did:plc:b0",
                "did:plc:b1",
                "did:plc:a0",
                "did:plc:a1",
                "did:plc:a2"
            ]
        );
        assert!(q.is_empty());
    }

    #[test]
    fn no_duplicates_and_asking_again_moves_to_the_front() {
        let q = WarmQueue::default();
        q.push_page(page("a", 3));
        q.push_page(vec!["did:plc:a2".into(), "did:plc:a2".into()]);
        assert_eq!(q.len(), 3);
        assert_eq!(q.pop().as_deref(), Some("did:plc:a2"));
        assert_eq!(q.pop().as_deref(), Some("did:plc:a0"));
        // A DID the worker is done with can be queued again.
        q.finished("did:plc:a2");
        q.push_page(vec!["did:plc:a2".into()]);
        assert_eq!(q.pop().as_deref(), Some("did:plc:a2"));
    }

    #[test]
    fn an_account_being_verified_is_not_queued_again() {
        let q = WarmQueue::default();
        q.push_page(page("a", 2));
        let taken = q.pop().unwrap();
        // A second render of the page while the worker has its first row.
        q.push_page(page("a", 2));
        assert_eq!(q.len(), 1);
        assert_eq!(q.pop().as_deref(), Some("did:plc:a1"));
        assert_eq!(q.pop(), None);
        q.finished(&taken);
        q.push_page(vec![taken.clone()]);
        assert_eq!(q.pop(), Some(taken));
    }

    #[test]
    fn a_full_queue_drops_the_oldest() {
        let q = WarmQueue::default();
        assert_eq!(q.push_page(page("old", QUEUE_CAP)), 0);
        assert_eq!(q.push_page(page("new", 50)), 50);
        assert_eq!(q.len(), QUEUE_CAP);
        assert_eq!(q.pop().as_deref(), Some("did:plc:new0"));
        // The oldest page's last rows went.
        let left: HashSet<String> = std::iter::from_fn(|| q.pop()).collect();
        assert!(left.contains("did:plc:old0"));
        assert!(!left.contains(&format!("did:plc:old{}", QUEUE_CAP - 1)));
        q.push_page(page("x", 3));
        q.clear();
        assert!(q.is_empty() && q.pop().is_none());
    }

    #[test]
    fn what_is_due() {
        let ttl = Duration::from_secs(3600);
        assert!(due(None, ttl));
        let fresh = (Cached::Handle("a.example".into()), ttl);
        assert!(!due(Some(&fresh), ttl));
        let stale = (Cached::Handle("a.example".into()), Duration::from_secs(899));
        assert!(due(Some(&stale), ttl));
        // "Nothing to show" is left alone until it expires.
        let none = (Cached::None, Duration::from_secs(1));
        assert!(!due(Some(&none), ttl));
    }

    #[test]
    fn outcome_labels_are_the_documented_set() {
        let labels: Vec<&str> = Outcome::ALL.iter().map(|o| o.label()).collect();
        assert_eq!(
            labels,
            ["resolved", "unverified", "failed", "cached", "dropped"]
        );
    }
}
