//! Advisory-lock keys, admission keys and cap buckets.

use std::time::Duration;

use farsight_core::config::{Config, LimitsConfig};
use farsight_core::{Did, DidMethod, registrable_domain};
use sha2::{Digest, Sha256};

/// A stable 64-bit hash: the first 8 bytes of SHA-256, big-endian, as
/// `i64` (Postgres advisory-lock keys are `bigint`). Stable across
/// processes, builds and versions, which the lock protocol requires (both
/// binaries must derive the same key).
pub fn hash64(s: &str) -> i64 {
    let digest = Sha256::digest(s.as_bytes());
    let mut b = [0u8; 8];
    b.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(b)
}

/// `hash64("a:" || did)` (design §4.3).
pub fn author_lock_key(did: &str) -> i64 {
    hash64(&format!("a:{did}"))
}

/// `hash64("l:" || owner_did || "/" || rkey)` (design §4.3).
pub fn list_lock_key(owner_did: &str, rkey: &str) -> i64 {
    hash64(&format!("l:{owner_did}/{rkey}"))
}

/// The limits the storage layer enforces, taken from `[limits]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Limits {
    /// The `[limits]` section.
    pub cfg: LimitsConfig,
    /// `firehose.tuning.synthetic_gap_lag` (coverage).
    pub synthetic_gap_lag: Duration,
    /// `storage.tombstone_ttl`.
    pub tombstone_ttl: Duration,
}

impl Limits {
    /// From a loaded config.
    pub fn from_config(config: &Config) -> Limits {
        Limits {
            cfg: config.limits.clone(),
            synthetic_gap_lag: config.firehose.tuning.synthetic_gap_lag.get(),
            tombstone_ttl: config.storage.tombstone_ttl.get(),
        }
    }

    /// Limits with every default of design §16.
    pub fn defaults() -> Limits {
        Limits::from_config(&Config::default())
    }

    /// Daily admission limit for an admission key (§11.1).
    pub fn admission_limit(&self, key: &str) -> i64 {
        if key.starts_with(BUCKET_KEY_PREFIX) {
            clamp(self.cfg.bucket_admissions_per_day)
        } else {
            clamp(self.cfg.did_admissions_per_day)
        }
    }

    /// Daily intern limit for a cause key (§11.2).
    pub fn intern_limit(&self, key: &str) -> i64 {
        if key.starts_with(BUCKET_KEY_PREFIX) {
            clamp(self.cfg.intern_per_bucket_per_day)
        } else {
            clamp(self.cfg.intern_per_did_per_day)
        }
    }

    /// Cap of `kind` for a host-usage bucket.
    pub fn bucket_cap(&self, bucket: &str, kind: CapKind) -> i64 {
        let c = &self.cfg;
        let v = if bucket == UNRESOLVED_BUCKET {
            match kind {
                CapKind::Blocks => c.unresolved_blocks,
                CapKind::Items => c.unresolved_list_items,
                CapKind::Listblocks => c.unresolved_listblocks,
                CapKind::Lists => c.unresolved_lists,
                CapKind::Interned => c.host_interned_lifetime,
            }
        } else if bucket.starts_with(DID_BUCKET_PREFIX) {
            // A per-DID bucket uses the per-author caps (§11.2).
            match kind {
                CapKind::Blocks => c.blocks_per_author,
                CapKind::Items => c.list_items_per_owner,
                CapKind::Listblocks => c.listblocks_per_author,
                CapKind::Lists => c.lists_per_author,
                CapKind::Interned => c.host_interned_lifetime,
            }
        } else {
            match kind {
                CapKind::Blocks => c.host_blocks,
                CapKind::Items => c.host_list_items,
                CapKind::Listblocks => c.host_listblocks,
                CapKind::Lists => c.host_lists,
                CapKind::Interned => c.host_interned_lifetime,
            }
        };
        clamp(v)
    }
}

fn clamp(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Record kinds with per-bucket caps; the bit is the `host_usage.capped_mask`
/// bit (design §7.1: one bit per cap type, plus the lifetime intern bound).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CapKind {
    /// `host_blocks`.
    Blocks,
    /// `host_list_items`.
    Items,
    /// `host_listblocks` (placeholder lists included).
    Listblocks,
    /// `host_lists`.
    Lists,
    /// `host_interned_lifetime` (non-large buckets).
    Interned,
}

impl CapKind {
    /// All kinds.
    pub const ALL: [CapKind; 5] = [
        CapKind::Blocks,
        CapKind::Items,
        CapKind::Listblocks,
        CapKind::Lists,
        CapKind::Interned,
    ];

    /// `capped_mask` bit.
    pub fn bit(self) -> i16 {
        match self {
            CapKind::Blocks => 1,
            CapKind::Items => 2,
            CapKind::Listblocks => 4,
            CapKind::Lists => 8,
            CapKind::Interned => 16,
        }
    }
}

/// Prefix of bucket admission / cause keys.
pub const BUCKET_KEY_PREFIX: &str = "bucket:";
/// Prefix of DID admission / cause keys (large hosts, per-DID buckets).
pub const DID_KEY_PREFIX: &str = "did:";
/// Prefix of not-yet-resolved did:plc keys (per-DID rate, §4.2).
pub const UNRESOLVED_KEY_PREFIX: &str = "unresolved:";

/// Host-usage bucket for not-yet-resolved did:plc authors.
pub const UNRESOLVED_BUCKET: &str = "unresolved";
/// Prefix of domain buckets in `host_usage.bucket`.
pub const DOMAIN_BUCKET_PREFIX: &str = "d:";
/// Prefix of address-block buckets.
pub const IP_BUCKET_PREFIX: &str = "ip:";
/// Prefix of per-DID buckets (after 3 resolution failures).
pub const DID_BUCKET_PREFIX: &str = "did:";

/// Resolution facts about an author, as stored on `actors`/`pds_hosts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostFacts {
    /// `actors.admission_key`, if the resolver set one.
    pub admission_key: Option<String>,
    /// `actors.resolve_failures`.
    pub resolve_failures: i32,
    /// Resolved: `pds_hosts.cap_key`.
    pub cap_key: Option<String>,
    /// Resolved: `pds_hosts.ip_bucket` (NULL for shared CDN ranges).
    pub ip_bucket: Option<String>,
    /// Resolved: `pds_hosts.large`.
    pub large: bool,
    /// Whether `pds_host_id` is set.
    pub resolved: bool,
}

/// The author's admission key (§4.2, §5.5, §11.1): the stored key if the
/// resolver set one; otherwise derived: resolved large host ⇒ `did:<did>`,
/// resolved other host ⇒ `bucket:<cap_key>`, unresolved did:web ⇒
/// `bucket:<registrable domain>`, unresolved did:plc ⇒ `unresolved:<did>`.
pub fn admission_key(did: &Did, facts: &HostFacts) -> String {
    if let Some(k) = &facts.admission_key {
        return k.clone();
    }
    if facts.resolved {
        if facts.large {
            return format!("{DID_KEY_PREFIX}{did}");
        }
        if let Some(cap) = &facts.cap_key {
            return format!("{BUCKET_KEY_PREFIX}{cap}");
        }
    }
    match (did.method(), did.web_host()) {
        (DidMethod::Web, Some(host)) => format!("{BUCKET_KEY_PREFIX}{}", registrable_domain(&host)),
        _ => format!("{UNRESOLVED_KEY_PREFIX}{did}"),
    }
}

/// The author's cap buckets (§11.2). Empty for large hosts (exempt).
pub fn buckets(did: &Did, facts: &HostFacts) -> Vec<String> {
    if facts.resolved {
        if facts.large {
            return Vec::new();
        }
        let mut v = Vec::with_capacity(2);
        if let Some(cap) = &facts.cap_key {
            v.push(format!("{DOMAIN_BUCKET_PREFIX}{cap}"));
        }
        if let Some(ip) = &facts.ip_bucket {
            v.push(format!("{IP_BUCKET_PREFIX}{ip}"));
        }
        if !v.is_empty() {
            return v;
        }
    }
    match (did.method(), did.web_host()) {
        (DidMethod::Web, Some(host)) => {
            vec![format!(
                "{DOMAIN_BUCKET_PREFIX}{}",
                registrable_domain(&host)
            )]
        }
        _ if facts.resolve_failures >= 3 => vec![format!("{DID_BUCKET_PREFIX}{did}")],
        _ => vec![UNRESOLVED_BUCKET.to_owned()],
    }
}

/// Whether an author is exempt from the budget gate and bucket caps (a
/// resolved large host). Unresolved authors are never large.
pub fn is_large(facts: &HostFacts) -> bool {
    facts.resolved && facts.large
}

#[cfg(test)]
mod tests {
    use super::*;

    fn did(s: &str) -> Did {
        Did::parse(s).unwrap()
    }

    #[test]
    fn hash_is_stable() {
        // Pinned: both binaries (and future versions) must agree.
        assert_eq!(
            hash64(""),
            i64::from_be_bytes([0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14])
        );
        assert_ne!(author_lock_key("did:plc:x"), list_lock_key("did:plc:x", ""));
        assert_eq!(
            list_lock_key("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "3k"),
            hash64("l:did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/3k")
        );
    }

    #[test]
    fn admission_keys() {
        let plc = did("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(
            admission_key(&plc, &HostFacts::default()),
            "unresolved:did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let web = did("did:web:alice.pds.example.co.uk");
        assert_eq!(
            admission_key(&web, &HostFacts::default()),
            "bucket:example.co.uk"
        );
        let large = HostFacts {
            resolved: true,
            large: true,
            cap_key: Some("bsky.network".to_owned()),
            ..HostFacts::default()
        };
        assert_eq!(
            admission_key(&plc, &large),
            "did:did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert!(buckets(&plc, &large).is_empty());
        let small = HostFacts {
            resolved: true,
            cap_key: Some("example.com".to_owned()),
            ip_bucket: Some("203.0.113.0/24".to_owned()),
            ..HostFacts::default()
        };
        assert_eq!(admission_key(&plc, &small), "bucket:example.com");
        assert_eq!(
            buckets(&plc, &small),
            ["d:example.com", "ip:203.0.113.0/24"]
        );
        assert_eq!(buckets(&plc, &HostFacts::default()), ["unresolved"]);
        let failing = HostFacts {
            resolve_failures: 3,
            ..HostFacts::default()
        };
        assert_eq!(
            buckets(&plc, &failing),
            ["did:did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"]
        );
    }

    #[test]
    fn limit_selection() {
        let l = Limits::defaults();
        assert_eq!(l.admission_limit("bucket:example.com"), 20_000);
        assert_eq!(l.admission_limit("unresolved:did:plc:x"), 200);
        assert_eq!(l.intern_limit("token:3"), 1_000_000);
        assert_eq!(l.bucket_cap("unresolved", CapKind::Blocks), 1_000_000);
        assert_eq!(l.bucket_cap("d:example.com", CapKind::Blocks), 20_000_000);
        assert_eq!(l.bucket_cap("did:did:plc:x", CapKind::Listblocks), 100_000);
    }
}
