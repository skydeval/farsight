//! The public UI's handle cache (design §8.6): in memory, bounded, least
//! recently used evicted. Farsight stores no handles; this holds what the
//! public pages verified, for as long as the caller says.
//!
//! An entry is either a verified handle or the fact that the DID has none
//! worth showing (no handle, or a resolution that failed), so that a page
//! view does not repeat a lookup that just failed.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Default capacity.
pub const DEFAULT_CAPACITY: usize = 50_000;

#[derive(Debug)]
struct Entry {
    handle: Option<String>,
    expires: Instant,
    tick: u64,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<String, Entry>,
    // Use order: tick → DID. The lowest tick is the least recently used.
    order: BTreeMap<u64, String>,
    tick: u64,
}

/// A cached answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cached {
    /// A handle verified in both directions.
    Handle(String),
    /// Nothing to show for this DID for now.
    None,
}

/// DID → verified handle.
#[derive(Debug)]
pub struct HandleCache {
    inner: Mutex<Inner>,
    capacity: usize,
}

impl Default for HandleCache {
    fn default() -> Self {
        HandleCache::new(DEFAULT_CAPACITY)
    }
}

impl HandleCache {
    /// A cache holding at most `capacity` DIDs.
    pub fn new(capacity: usize) -> HandleCache {
        HandleCache {
            inner: Mutex::new(Inner::default()),
            capacity: capacity.max(1),
        }
    }

    /// The live entry for `did`, if any; marks it used.
    pub fn lookup(&self, did: &str) -> Option<Cached> {
        self.lookup_at(did, Instant::now()).map(|(c, _)| c)
    }

    /// The live entry for `did` and how long it still lives; marks it
    /// used. A handle about to expire can be refreshed before a page that
    /// keeps being viewed loses it.
    pub fn lookup_remaining(&self, did: &str) -> Option<(Cached, Duration)> {
        self.lookup_at(did, Instant::now())
    }

    fn lookup_at(&self, did: &str, now: Instant) -> Option<(Cached, Duration)> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let inner = &mut *g;
        let e = inner.entries.get_mut(did)?;
        if e.expires <= now {
            let tick = e.tick;
            inner.entries.remove(did);
            inner.order.remove(&tick);
            return None;
        }
        inner.order.remove(&e.tick);
        inner.tick += 1;
        e.tick = inner.tick;
        inner.order.insert(e.tick, did.to_owned());
        let cached = match &e.handle {
            Some(h) => Cached::Handle(h.clone()),
            None => Cached::None,
        };
        Some((cached, e.expires.saturating_duration_since(now)))
    }

    /// The verified handle of `did`, if one is cached.
    pub fn get(&self, did: &str) -> Option<String> {
        match self.lookup(did)? {
            Cached::Handle(h) => Some(h),
            Cached::None => None,
        }
    }

    /// Caches a verified `handle` for `did`, or `None` for "nothing to
    /// show", for `ttl`.
    pub fn insert(&self, did: &str, handle: Option<String>, ttl: Duration) {
        self.insert_at(did, handle, ttl, Instant::now());
    }

    fn insert_at(&self, did: &str, handle: Option<String>, ttl: Duration, now: Instant) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let inner = &mut *g;
        if let Some(old) = inner.entries.remove(did) {
            inner.order.remove(&old.tick);
        }
        while inner.entries.len() >= self.capacity {
            let Some((_, lru)) = inner.order.pop_first() else {
                break;
            };
            inner.entries.remove(&lru);
        }
        inner.tick += 1;
        let tick = inner.tick;
        inner.order.insert(tick, did.to_owned());
        inner.entries.insert(
            did.to_owned(),
            Entry {
                handle,
                expires: now + ttl,
                tick,
            },
        );
    }

    /// Entries held (expired ones included until they are next looked up
    /// or evicted).
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .len()
    }

    /// No entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(60);

    #[test]
    fn hit_miss_and_expiry() {
        let c = HandleCache::new(10);
        let t0 = Instant::now();
        assert_eq!(c.lookup_at("did:plc:a", t0), None);
        c.insert_at("did:plc:a", Some("a.example".into()), TTL, t0);
        c.insert_at("did:plc:b", None, TTL, t0);
        assert_eq!(
            c.lookup_at("did:plc:a", t0),
            Some((Cached::Handle("a.example".into()), TTL))
        );
        assert_eq!(c.lookup_at("did:plc:b", t0), Some((Cached::None, TTL)));
        // The remaining lifetime shrinks with the clock.
        assert_eq!(
            c.lookup_at("did:plc:b", t0 + TTL / 4).map(|(_, left)| left),
            Some(TTL - TTL / 4)
        );
        assert_eq!(c.get("did:plc:b"), None);
        // Expired entries are gone, not returned.
        assert_eq!(c.lookup_at("did:plc:a", t0 + TTL), None);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn least_recently_used_goes_first() {
        let c = HandleCache::new(2);
        let t0 = Instant::now();
        c.insert_at("a", Some("a.example".into()), TTL, t0);
        c.insert_at("b", Some("b.example".into()), TTL, t0);
        // Using `a` makes `b` the eviction candidate.
        assert!(c.lookup_at("a", t0).is_some());
        c.insert_at("c", Some("c.example".into()), TTL, t0);
        assert_eq!(c.len(), 2);
        assert!(c.lookup_at("b", t0).is_none());
        assert!(c.lookup_at("a", t0).is_some());
        assert!(c.lookup_at("c", t0).is_some());
        // Re-inserting a key replaces it without growing.
        c.insert_at("a", None, TTL, t0);
        assert_eq!(c.len(), 2);
        assert_eq!(c.lookup_at("a", t0).map(|(c, _)| c), Some(Cached::None));
    }
}
