//! DID → PDS resolution (see `docs/design/backfill.md` and
//! `docs/design/security.md`): `did:plc` through the configured PLC
//! directory (its own limiter, resolver half reserved), `did:web` through
//! `https://<host>/.well-known/did.json`, both via the safe client.
//! Results are cached on `actors.pds_host_id` (TTL 7 days; `identity`
//! events invalidate) for interned DIDs and in memory for the rest (sweep
//! members hold no row); nonexistent DIDs are negatively cached for 24 h.
//! Both in-memory caches are bounded: at their cap the entries past their
//! TTL go, and all of them if none is.
//! Resolving records the host in `pds_hosts` with its cap buckets
//! (registrable domain, /24 or /48 address block unless a shared CDN
//! range) and the author's admission key.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use farsight_core::{Did, DidMethod};
use farsight_storage::counters::{self, CounterSink, HostChange};
use ipnet::IpNet;
use serde_json::Value;
use sqlx::PgPool;
use url::Url;

use crate::net::{Net, NetError, PlcUse, host_key};

/// Cache TTL on `actors`.
pub const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 3600);
/// Negative cache for nonexistent DIDs.
pub const NEGATIVE_TTL: Duration = Duration::from_secs(24 * 3600);
/// In-memory TTL for DIDs without an `actors` row.
pub const MEMORY_TTL: Duration = Duration::from_secs(3600);
const MEMORY_CAP: usize = 200_000;
/// Nonexistent DIDs remembered at most.
pub const NEGATIVE_CAP: usize = 200_000;

/// Makes room in a cache that has reached `cap`: the entries for which
/// `expired` says so go, and every entry if that leaves it full.
fn make_room<V>(cache: &mut HashMap<String, V>, cap: usize, expired: impl Fn(&V) -> bool) {
    if cache.len() < cap {
        return;
    }
    cache.retain(|_, v| !expired(v));
    if cache.len() >= cap {
        cache.clear();
    }
}

/// A resolved PDS: where a DID's repo is read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pds {
    /// Base URL, e.g. `https://pds.example`.
    pub endpoint: String,
    /// `host[:port]` (limiter and `pds_hosts` key).
    pub host: String,
}

/// Why resolution failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// Authoritative: the DID does not exist (PLC 404; did:web host
    /// reachable and 404 for its document).
    #[error("DID not found")]
    NotFound,
    /// The DID is tombstoned in PLC (deactivated: purge).
    #[error("DID tombstoned in PLC")]
    Tombstoned,
    /// Anything else; retried with backoff.
    #[error("resolution failed: {0}")]
    Transient(String),
}

/// The resolver: DID → [`Pds`] through `actors.pds_host_id`, the
/// in-memory caches, then the PLC directory or the `did:web` host.
pub struct Resolver {
    net: Arc<Net>,
    pool: PgPool,
    plc_url: String,
    allow_http: Vec<String>,
    large_hosts: Vec<String>,
    cdn_ranges: Vec<IpNet>,
    dns: Option<farsight_core::net::SafeClient>,
    counters: Arc<CounterSink>,
    memory: Mutex<HashMap<String, (Pds, Instant)>>,
    negative: Mutex<HashMap<String, Instant>>,
}

/// The `#atproto_pds` service endpoint of a DID document.
pub fn pds_endpoint(doc: &Value) -> Option<String> {
    doc.get("service")?
        .as_array()?
        .iter()
        .find(|s| {
            s.get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.ends_with("#atproto_pds"))
        })?
        .get("serviceEndpoint")?
        .as_str()
        .map(|e| e.trim_end_matches('/').to_owned())
}

/// The address block of `ip` for cap buckets: /24 (IPv4) or /48 (IPv6).
pub fn ip_block(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(a) => {
            let o = a.octets();
            format!("{}.{}.{}.0/24", o[0], o[1], o[2])
        }
        IpAddr::V6(a) => {
            let s = a.segments();
            format!("{:x}:{:x}:{:x}::/48", s[0], s[1], s[2])
        }
    }
}

impl Resolver {
    /// A resolver with empty caches. From `config` it takes the PLC
    /// directory's URL, the hosts allowed over plain HTTP, the large
    /// hosts and the extra CDN ranges, and keeps them for its lifetime: a
    /// change to any of them restarts the run. `dns` looks up a host's
    /// address for its cap bucket. `counters` takes the usage that moves
    /// between cap buckets when an account's host becomes known.
    pub fn new(
        net: Arc<Net>,
        pool: PgPool,
        config: &farsight_core::Config,
        dns: Option<farsight_core::net::SafeClient>,
        counters: Arc<CounterSink>,
    ) -> Resolver {
        let mut cdn = farsight_core::cloudflare::bundled();
        cdn.extend(config.limits.cdn_ranges_extra.iter().copied());
        Resolver {
            net,
            pool,
            plc_url: config.backfill.plc_url.trim_end_matches('/').to_owned(),
            allow_http: config
                .net
                .allow_http_hosts
                .iter()
                .map(|h| h.to_ascii_lowercase())
                .collect(),
            large_hosts: config.limits.large_hosts.clone(),
            cdn_ranges: cdn,
            dns,
            counters,
            memory: Mutex::new(HashMap::new()),
            negative: Mutex::new(HashMap::new()),
        }
    }

    fn endpoint_for(&self, host: &str) -> String {
        let bare = host.split(':').next().unwrap_or(host);
        if self.allow_http.iter().any(|h| h == bare) {
            format!("http://{host}")
        } else {
            format!("https://{host}")
        }
    }

    fn is_large(&self, host: &str) -> bool {
        let bare = host.split(':').next().unwrap_or(host).to_ascii_lowercase();
        self.large_hosts.iter().any(|p| match p.strip_prefix("*.") {
            Some(suffix) => bare.ends_with(&format!(".{suffix}")),
            None => bare == p.to_ascii_lowercase(),
        })
    }

    /// Cached resolution only (no I/O beyond the database).
    pub async fn cached(&self, did: &Did) -> Option<Pds> {
        if let Some((p, at)) = self
            .memory
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(did.as_str())
            && at.elapsed() < MEMORY_TTL
        {
            return Some(p.clone());
        }
        let row: Option<(Option<String>, Option<chrono::DateTime<chrono::Utc>>)> = sqlx::query_as(
            "SELECT h.host, a.pds_resolved_at FROM actors a JOIN pds_hosts h ON h.id = a.pds_host_id
             WHERE a.did = $1",
        )
        .bind(did.as_str())
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();
        let (Some(host), Some(at)) = row? else {
            return None;
        };
        let fresh = (chrono::Utc::now() - at)
            .to_std()
            .is_ok_and(|age| age < CACHE_TTL);
        fresh.then(|| Pds {
            endpoint: self.endpoint_for(&host),
            host,
        })
    }

    /// Seeds the in-memory cache (PLC export with `plc_seed_from_export`).
    /// The endpoint is the one the operation set, which a later operation
    /// of the export may have replaced: like every cached resolution it
    /// is confirmed with the directory before a repo is purged on its
    /// word (see `jobs::repo`).
    pub fn seed(&self, did: &str, endpoint: &str) {
        if let Ok(u) = Url::parse(endpoint) {
            let mut m = self.memory.lock().unwrap_or_else(|e| e.into_inner());
            make_room(&mut m, MEMORY_CAP, |(_, at)| at.elapsed() >= MEMORY_TTL);
            m.insert(
                did.to_owned(),
                (
                    Pds {
                        endpoint: endpoint.trim_end_matches('/').to_owned(),
                        host: host_key(&u),
                    },
                    Instant::now(),
                ),
            );
        }
    }

    /// Resolves `did`; `bypass` skips every cache (repo-level errors).
    pub async fn resolve(&self, did: &Did, bypass: bool) -> Result<Pds, ResolveError> {
        self.resolve_noting(did, bypass).await.map(|(pds, _)| pds)
    }

    /// [`Resolver::resolve`], also saying whether the answer came out of
    /// a cache (`true`) or was read from the directory or the `did:web`
    /// host just now.
    pub async fn resolve_noting(
        &self,
        did: &Did,
        bypass: bool,
    ) -> Result<(Pds, bool), ResolveError> {
        if !bypass {
            if let Some(p) = self.cached(did).await {
                return Ok((p, true));
            }
            let neg = self.negative.lock().unwrap_or_else(|e| e.into_inner());
            if neg
                .get(did.as_str())
                .is_some_and(|t| t.elapsed() < NEGATIVE_TTL)
            {
                return Err(ResolveError::NotFound);
            }
        }
        let r = self.resolve_uncached(did).await;
        match &r {
            Ok(pds) => {
                if let Err(e) = self.record(did, pds).await {
                    tracing::warn!(did = %did, error = %e, "recording resolution failed");
                }
            }
            Err(ResolveError::NotFound) => {
                let mut neg = self.negative.lock().unwrap_or_else(|e| e.into_inner());
                make_room(&mut neg, NEGATIVE_CAP, |at| at.elapsed() >= NEGATIVE_TTL);
                neg.insert(did.as_str().to_owned(), Instant::now());
            }
            Err(ResolveError::Transient(_)) => {
                // The third failure moves an unresolved did:plc author to
                // a bucket of its own: its usage moves with it.
                let _ = counters::record_host_change(
                    &self.pool,
                    &self.counters,
                    did,
                    &HostChange::Failed,
                )
                .await;
            }
            Err(ResolveError::Tombstoned) => {}
        }
        r.map(|pds| (pds, false))
    }

    async fn resolve_uncached(&self, did: &Did) -> Result<Pds, ResolveError> {
        let doc = match did.method() {
            DidMethod::Plc => {
                self.net.plc.acquire(PlcUse::Resolver).await;
                let u = Url::parse(&format!("{}/{}", self.plc_url, did.as_str()))
                    .map_err(|e| ResolveError::Transient(e.to_string()))?;
                match self.net.get_json(&u, "plc").await {
                    Ok(v) => v,
                    Err(NetError::Http { status: 404, .. }) => return Err(ResolveError::NotFound),
                    Err(NetError::Http { status: 410, .. }) => {
                        return Err(ResolveError::Tombstoned);
                    }
                    Err(e) => return Err(ResolveError::Transient(e.to_string())),
                }
            }
            DidMethod::Web => {
                let host = did
                    .web_host()
                    .ok_or_else(|| ResolveError::Transient("bad did:web".into()))?;
                let u = Url::parse(&format!(
                    "{}/.well-known/did.json",
                    self.endpoint_for(&host)
                ))
                .map_err(|e| ResolveError::Transient(e.to_string()))?;
                match self.net.get_json(&u, "didWeb").await {
                    Ok(v) => v,
                    Err(NetError::Http { status: 404, .. }) => return Err(ResolveError::NotFound),
                    Err(e) => return Err(ResolveError::Transient(e.to_string())),
                }
            }
        };
        if doc.get("id").and_then(Value::as_str) != Some(did.as_str()) {
            return Err(ResolveError::Transient("DID document id mismatch".into()));
        }
        let endpoint = pds_endpoint(&doc).ok_or(ResolveError::NotFound)?;
        let u = Url::parse(&endpoint).map_err(|e| ResolveError::Transient(e.to_string()))?;
        Ok(Pds {
            endpoint,
            host: host_key(&u),
        })
    }

    async fn ip_bucket(&self, host: &str) -> Option<String> {
        let bare = host.split(':').next().unwrap_or(host);
        let ips: Vec<IpAddr> = match bare.parse::<IpAddr>() {
            Ok(ip) => vec![ip],
            Err(_) => match &self.dns {
                Some(d) => d.lookup_ip(bare).await.ok()?,
                None => return None,
            },
        };
        let ip = *ips.first()?;
        if self.cdn_ranges.iter().any(|n| n.contains(&ip)) {
            return None;
        }
        Some(ip_block(ip))
    }

    async fn record(&self, did: &Did, pds: &Pds) -> Result<(), farsight_storage::StorageError> {
        // In memory for DIDs without a row.
        {
            let mut m = self.memory.lock().unwrap_or_else(|e| e.into_inner());
            make_room(&mut m, MEMORY_CAP, |(_, at)| at.elapsed() >= MEMORY_TTL);
            m.insert(did.as_str().to_owned(), (pds.clone(), Instant::now()));
        }
        let bare = crate::net::bare_host(&pds.host);
        let cap_key = farsight_core::registrable_domain(bare);
        let large = self.is_large(&pds.host);
        let ip_bucket = if large {
            None
        } else {
            self.ip_bucket(&pds.host).await
        };
        let host_id: farsight_storage::ids::HostId = sqlx::query_scalar(
            "INSERT INTO pds_hosts (host, cap_key, ip_bucket, large) VALUES ($1, $2, $3, $4)
             ON CONFLICT (host) DO UPDATE SET cap_key = EXCLUDED.cap_key,
               ip_bucket = COALESCE(EXCLUDED.ip_bucket, pds_hosts.ip_bucket), large = EXCLUDED.large
             RETURNING id",
        )
        .bind(&pds.host)
        .bind(&cap_key)
        .bind(&ip_bucket)
        .bind(large)
        .fetch_one(&self.pool)
        .await?;
        let key = if large {
            format!("did:{}", did.as_str())
        } else {
            format!("bucket:{cap_key}")
        };
        // The account's stored rows are counted under its buckets, and
        // those follow its host: the usage moves with the change.
        counters::record_host_change(
            &self.pool,
            &self.counters,
            did,
            &HostChange::Resolved {
                host: host_id,
                admission_key: key,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_full_cache_drops_what_expired_and_everything_if_nothing_did() {
        let entries = |n: usize| -> HashMap<String, u32> {
            (0..n).map(|i| (format!("did:plc:{i}"), i as u32)).collect()
        };
        // Below the cap: untouched.
        let mut c = entries(9);
        make_room(&mut c, 10, |_| true);
        assert_eq!(c.len(), 9);
        // At the cap: the expired half goes.
        let mut c = entries(10);
        make_room(&mut c, 10, |v| v % 2 == 0);
        assert_eq!(c.len(), 5);
        assert!(c.values().all(|v| v % 2 == 1));
        // At the cap with nothing expired: emptied, so it never grows
        // past the cap.
        let mut c = entries(10);
        make_room(&mut c, 10, |_| false);
        assert!(c.is_empty());
        for i in 0..1_000 {
            make_room(&mut c, 10, |_| false);
            c.insert(format!("did:plc:n{i}"), 0);
            assert!(c.len() <= 10);
        }
    }

    #[test]
    fn endpoint_from_doc() {
        let doc = json!({"id": "did:plc:x", "service": [
            {"id": "#atproto_labeler", "type": "x", "serviceEndpoint": "https://l"},
            {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example/"}
        ]});
        assert_eq!(pds_endpoint(&doc).as_deref(), Some("https://pds.example"));
        assert_eq!(ip_block("203.0.113.9".parse().unwrap()), "203.0.113.0/24");
        assert_eq!(
            ip_block("2001:db8:1:2::1".parse().unwrap()),
            "2001:db8:1::/48"
        );
    }
}
