//! The UI's handle cache (design §8.6), in two layers.
//!
//! In memory: bounded, least recently used evicted, each entry living as
//! long as the caller says. An entry is either a verified handle or the
//! fact that the DID has none worth showing (no handle, or a resolution
//! that failed), so that a page view does not repeat a lookup that just
//! failed.
//!
//! In the database: the `handle_cache` table holds, for every DID that
//! was checked, the verified handle and the time of the check, so that a
//! restart loses nothing. A check that found no handle to show is stored
//! as an empty handle, and never replaces a verified one. The memory
//! layer stays in front of it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use crate::error::Result;

/// Default capacity.
pub const DEFAULT_CAPACITY: usize = 50_000;

#[derive(Debug)]
struct Entry {
    handle: Option<String>,
    // The answer is older than the caller trusts: used as it is, and due
    // a fresh verification.
    stale: bool,
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

    /// The live entry for `did` and whether it is a stale handle (see
    /// [`HandleCache::insert_stale`]); marks it used.
    pub fn lookup_stale(&self, did: &str) -> Option<(Cached, bool)> {
        self.lookup_at(did, Instant::now())
    }

    fn lookup_at(&self, did: &str, now: Instant) -> Option<(Cached, bool)> {
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
        Some((cached, e.stale))
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
        self.insert_at(did, handle, false, ttl, Instant::now());
    }

    /// Caches an answer that was checked longer ago than the caller
    /// trusts: it is used as it is, and [`HandleCache::lookup_stale`]
    /// reports it as due a fresh verification.
    pub fn insert_stale(&self, did: &str, handle: Option<String>, ttl: Duration) {
        self.insert_at(did, handle, true, ttl, Instant::now());
    }

    fn insert_at(
        &self,
        did: &str,
        handle: Option<String>,
        stale: bool,
        ttl: Duration,
        now: Instant,
    ) {
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
                stale,
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

/// A row of `handle_cache`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// The DID.
    pub did: String,
    /// Its handle, as last verified; empty: the last check found none to
    /// show.
    pub handle: String,
    /// When that was.
    pub resolved_at: DateTime<Utc>,
}

/// The stored handles of those of `dids` that have one.
pub async fn stored(conn: &mut PgConnection, dids: &[String]) -> Result<Vec<Stored>> {
    if dids.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(String, String, DateTime<Utc>)> =
        sqlx::query_as("SELECT did, handle, resolved_at FROM handle_cache WHERE did = ANY($1)")
            .bind(dids)
            .fetch_all(conn)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(did, handle, resolved_at)| Stored {
            did,
            handle,
            resolved_at,
        })
        .collect())
}

/// Stores `handle` as the handle of `did`, verified now. Replaces what
/// was stored for the DID.
pub async fn store(conn: &mut PgConnection, did: &str, handle: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO handle_cache (did, handle, resolved_at) VALUES ($1, $2, now())
         ON CONFLICT (did) DO UPDATE
           SET handle = EXCLUDED.handle, resolved_at = EXCLUDED.resolved_at",
    )
    .bind(did)
    .bind(handle)
    .execute(conn)
    .await?;
    Ok(())
}

/// Records that a check of `did`, made now, found no handle to show.
/// A verified handle already stored for the DID is left as it is.
pub async fn store_none(conn: &mut PgConnection, did: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO handle_cache (did, handle, resolved_at) VALUES ($1, '', now())
         ON CONFLICT (did) DO UPDATE SET resolved_at = EXCLUDED.resolved_at
           WHERE handle_cache.handle = ''",
    )
    .bind(did)
    .execute(conn)
    .await?;
    Ok(())
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
        c.insert_at("did:plc:a", Some("a.example".into()), false, TTL, t0);
        c.insert_at("did:plc:b", None, false, TTL, t0);
        assert_eq!(
            c.lookup_at("did:plc:a", t0),
            Some((Cached::Handle("a.example".into()), false))
        );
        assert_eq!(c.lookup_at("did:plc:b", t0), Some((Cached::None, false)));
        assert_eq!(c.get("did:plc:b"), None);
        // Expired entries are gone, not returned.
        assert_eq!(c.lookup_at("did:plc:a", t0 + TTL), None);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn a_stale_handle_is_shown_and_reported_until_replaced() {
        let c = HandleCache::new(10);
        c.insert_stale("did:plc:a", Some("old.example".into()), TTL);
        assert_eq!(
            c.lookup_stale("did:plc:a"),
            Some((Cached::Handle("old.example".into()), true))
        );
        assert_eq!(c.get("did:plc:a").as_deref(), Some("old.example"));
        c.insert("did:plc:a", Some("new.example".into()), TTL);
        assert_eq!(
            c.lookup_stale("did:plc:a"),
            Some((Cached::Handle("new.example".into()), false))
        );
    }

    #[test]
    fn least_recently_used_goes_first() {
        let c = HandleCache::new(2);
        let t0 = Instant::now();
        c.insert_at("a", Some("a.example".into()), false, TTL, t0);
        c.insert_at("b", Some("b.example".into()), false, TTL, t0);
        // Using `a` makes `b` the eviction candidate.
        assert!(c.lookup_at("a", t0).is_some());
        c.insert_at("c", Some("c.example".into()), false, TTL, t0);
        assert_eq!(c.len(), 2);
        assert!(c.lookup_at("b", t0).is_none());
        assert!(c.lookup_at("a", t0).is_some());
        assert!(c.lookup_at("c", t0).is_some());
        // Re-inserting a key replaces it without growing.
        c.insert_at("a", None, false, TTL, t0);
        assert_eq!(c.len(), 2);
        assert_eq!(c.lookup_at("a", t0).map(|(c, _)| c), Some(Cached::None));
    }
}
