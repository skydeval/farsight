//! Handle warming (see `docs/design/api.md` and
//! `docs/design/web-ui.md`): a background worker verifies the handles
//! of accounts that pages had to show as bare DIDs, so that a later
//! view shows the handle.
//!
//! Rendering a row never waits for an outbound request. A row whose
//! account has no cache entry, in memory or stored — or a stored handle
//! verified more than a week ago ([`handles::STALE_AFTER`]), which the row
//! shows meanwhile — is put on the **warming queue**; one worker takes
//! DIDs from it and verifies them as a page verifies its own subject.
//!
//! - The queue is memory: at most [`QUEUE_CAP`] DIDs, no duplicates. The
//!   newest request is served first (the page someone is looking at now
//!   matters more than one from ten minutes ago), a DID asked for again
//!   moves to the front, and when the queue is full the oldest entry is
//!   dropped. Within one page the first row is served first.
//! - The worker draws on the process-wide handle budget and takes a token
//!   only while the bucket holds more than [`RESERVE`], so requests always
//!   find some. It cannot raise the server's outbound rate.
//! - No table is scanned. The worker reads nothing from the database and
//!   writes one `handle_cache` row for each handle it verifies.
//! - `public_ui.handle_warming_enabled = false`: pages queue nothing, the
//!   queue is emptied, the worker idles.
//!
//! What it cannot do: the first view of a page nobody has rendered still
//! shows DIDs. A restart empties the queue and the memory cache; the
//! stored handles stay.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use farsight_api::ratelimit::Class;
use farsight_core::Did;
use farsight_core::config::Config;
use farsight_storage::handles::Cached;
use tokio::sync::{Notify, Semaphore, watch};

use super::handles::{self, BUDGET_KEY};
use crate::pages::WebState;

/// Most DIDs the queue holds.
pub const QUEUE_CAP: usize = 2_000;
/// Tokens of the handle budget the worker leaves for requests.
pub const RESERVE: f64 = 5.0;
/// How long the worker waits when the budget has nothing to spare.
pub const BUDGET_WAIT: Duration = Duration::from_millis(500);
/// Deadline of one verification.
pub const VERIFY_DEADLINE: Duration = Duration::from_secs(5);
/// Most verifications in flight at once. The handle budget sets the pace;
/// this bounds what slow hosts can pile up.
pub const IN_FLIGHT: usize = 64;
/// How often an idle or switched-off worker looks at the setting.
pub const IDLE_POLL: Duration = Duration::from_secs(5);

/// Task name panics of single checks are counted under.
pub const CHECK_TASK: &str = "handle_warming_check";

/// One check in flight: its DID is marked in the queue and its slot is
/// held until this is dropped.
struct Checking {
    st: Arc<WebState>,
    raw: String,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for Checking {
    fn drop(&mut self) {
        self.st.public.warm.finished(&self.raw);
    }
}

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

    /// The `outcome` label of `farsight_handle_warming_total`.
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

/// The warming queue: the DIDs whose handles pages have asked for, most
/// recently asked first, at most [`QUEUE_CAP`]. A DID is in it once; one
/// the worker has taken is not queued again until it is finished.
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

    /// Drops every waiting DID. Those the worker has already taken are
    /// not affected.
    pub fn clear(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.order.clear();
        g.set.clear();
        drop(g);
        ::metrics::gauge!(QUEUE).set(0.0);
    }

    /// DIDs waiting, not counting those the worker has taken.
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
/// live entry, or a stale one. A live answer is left alone, also when it
/// is "nothing to show".
pub fn due(entry: Option<&(Cached, bool)>) -> bool {
    match entry {
        None => true,
        Some((_, stale)) => *stale,
    }
}

/// What a page knows about an account it is about to show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Known {
    /// Its verified handle.
    Handle(String),
    /// It was checked and has no handle to show: its DID is the answer.
    NoHandle,
    /// Not checked yet. The worker has been asked.
    Pending,
}

/// The DIDs one page render asks the worker for, in row order.
#[derive(Debug)]
pub struct Asked {
    enabled: bool,
    dids: Vec<String>,
}

impl Asked {
    /// A collector for one render under `cfg`.
    pub fn new(cfg: &Config) -> Asked {
        Asked {
            enabled: cfg.public_ui.handle_warming_enabled,
            dids: Vec::new(),
        }
    }

    /// The cached handle of an account a cell shows, if there is one.
    /// Asks for the account when the cell has to show its DID, or its
    /// handle is stale. Never fetches anything and reads memory alone:
    /// the page calls [`handles::recall`] for its accounts first.
    pub fn handle(&mut self, st: &WebState, did: &str) -> Option<String> {
        let entry = st.public.handles.lookup_stale(did);
        if self.enabled && due(entry.as_ref()) {
            self.dids.push(did.to_owned());
        }
        match entry {
            Some((Cached::Handle(h), _)) => Some(h),
            _ => None,
        }
    }

    /// What is known about an account a public row would show. Asks the
    /// worker as [`Asked::handle`] does. With warming off nothing would
    /// ever check the account, so it is never [`Known::Pending`].
    pub fn known(&mut self, st: &WebState, did: &str) -> Known {
        let entry = st.public.handles.lookup_stale(did);
        if self.enabled && due(entry.as_ref()) {
            self.dids.push(did.to_owned());
        }
        match entry {
            Some((Cached::Handle(h), _)) => Known::Handle(h),
            Some((Cached::None, _)) => Known::NoHandle,
            None if self.enabled => Known::Pending,
            None => Known::NoHandle,
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
    handles::settle(&st, cfg, &did, handle).await;
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
        let entry = st.public.handles.lookup_stale(did.as_str());
        if !due(entry.as_ref()) {
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
        // The DID is marked as being checked until the guard is dropped,
        // and the slot is held as long: both are given back when the task
        // ends, also when the check panics.
        let checking = Checking {
            st: st.clone(),
            raw,
            _slot: slot,
        };
        tokio::spawn(async move {
            if let Err(message) =
                farsight_core::task::catch(verify_one(checking.st.clone(), did)).await
            {
                farsight_core::task::report_panic(CHECK_TASK, &message);
            }
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
        assert!(due(None));
        let fresh = (Cached::Handle("a.example".into()), false);
        assert!(!due(Some(&fresh)));
        let stale = (Cached::Handle("a.example".into()), true);
        assert!(due(Some(&stale)));
        // "Nothing to show" is left alone until it is stale.
        let none = (Cached::None, false);
        assert!(!due(Some(&none)));
        assert!(due(Some(&(Cached::None, true))));
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
