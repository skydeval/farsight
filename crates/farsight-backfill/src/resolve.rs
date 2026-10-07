//! DID → PDS resolution (see `docs/design/backfill.md` and
//! `docs/design/security.md`): `did:plc` through the configured PLC
//! directory (its own limiter, resolver half reserved), `did:web` through
//! `https://<host>/.well-known/did.json`, both via the safe client.
//! Results are cached on `actors.pds_host_id` (TTL 7 days; `identity`
//! events invalidate) for interned DIDs and in memory for the rest (sweep
//! members hold no row); nonexistent DIDs are negatively cached for 24 h.
//! Resolving records the host in `pds_hosts` with its cap buckets
//! (registrable domain, /24 or /48 address block unless a shared CDN
//! range) and the author's admission key.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use farsight_core::{Did, DidMethod};
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

/// A resolved PDS.
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

/// The resolver.
pub struct Resolver {
    net: Arc<Net>,
    pool: PgPool,
    plc_url: String,
    allow_http: Vec<String>,
    large_hosts: Vec<String>,
    cdn_ranges: Vec<IpNet>,
    dns: Option<farsight_core::net::SafeClient>,
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
    /// A resolver.
    pub fn new(
        net: Arc<Net>,
        pool: PgPool,
        config: &farsight_core::Config,
        dns: Option<farsight_core::net::SafeClient>,
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
        {
            if at.elapsed() < MEMORY_TTL {
                return Some(p.clone());
            }
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
    pub fn seed(&self, did: &str, endpoint: &str) {
        if let Ok(u) = Url::parse(endpoint) {
            let mut m = self.memory.lock().unwrap_or_else(|e| e.into_inner());
            if m.len() >= MEMORY_CAP {
                m.clear();
            }
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
        if !bypass {
            if let Some(p) = self.cached(did).await {
                return Ok(p);
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
                self.negative
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(did.as_str().to_owned(), Instant::now());
            }
            Err(ResolveError::Transient(_)) => {
                let _ = sqlx::query(
                    "UPDATE actors SET resolve_failures = resolve_failures + 1 WHERE did = $1",
                )
                .bind(did.as_str())
                .execute(&self.pool)
                .await;
            }
            Err(ResolveError::Tombstoned) => {}
        }
        r
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

    async fn record(&self, did: &Did, pds: &Pds) -> Result<(), sqlx::Error> {
        // In memory for DIDs without a row.
        {
            let mut m = self.memory.lock().unwrap_or_else(|e| e.into_inner());
            if m.len() >= MEMORY_CAP {
                m.clear();
            }
            m.insert(did.as_str().to_owned(), (pds.clone(), Instant::now()));
        }
        let bare = pds.host.split(':').next().unwrap_or(&pds.host);
        let cap_key = farsight_core::registrable_domain(bare);
        let large = self.is_large(&pds.host);
        let ip_bucket = if large {
            None
        } else {
            self.ip_bucket(&pds.host).await
        };
        let host_id: i32 = sqlx::query_scalar(
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
        sqlx::query(
            "UPDATE actors SET pds_host_id = $2, pds_resolved_at = now(), resolve_failures = 0,
               admission_key = $3
             WHERE did = $1",
        )
        .bind(did.as_str())
        .bind(host_id)
        .bind(key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
