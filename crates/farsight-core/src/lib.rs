//! Shared types for Farsight: DID, AT-URI, TID and NSID handling, record
//! validation, configuration loading (TOML plus environment overrides), the
//! safe outbound HTTP client and the supervision of background tasks.

#![warn(missing_docs)]

pub mod aturi;
pub mod bucket;
pub mod cloudflare;
pub mod config;
pub mod did;
pub mod duration;
pub mod listen;
pub mod net;
pub mod nsid;
pub mod record;
pub mod task;
pub mod tid;

pub use aturi::{AtUri, AtUriError, RecordKey, RecordKeyError};
pub use config::{Config, ConfigError, LoadedConfig, StartMode};
pub use did::{Did, DidError, DidMethod};
pub use duration::ConfigDuration;
pub use nsid::{Collection, Nsid, NsidError};
pub use record::{
    BlockRecord, CommitAction, CommitOp, ListBlockRecord, ListItemRecord, ListPurpose, ListRecord,
    Operation, Record, RecordError,
};
pub use tid::{Tid, TidError};

/// The address block an address is counted in for cap buckets and
/// limits: its /24 (IPv4) or its /48 (IPv6).
pub fn ip_block(ip: std::net::IpAddr) -> String {
    match ip {
        std::net::IpAddr::V4(a) => {
            let o = a.octets();
            format!("{}.{}.{}.0/24", o[0], o[1], o[2])
        }
        std::net::IpAddr::V6(a) => {
            let s = a.segments();
            format!("{:x}:{:x}:{:x}::/48", s[0], s[1], s[2])
        }
    }
}

/// The address a host name is, if it is one written out: `203.0.113.7`,
/// `2001:db8::1`, or the bracketed form a URL gives an IPv6 address,
/// `[2001:db8::1]`.
pub fn literal_ip(host: &str) -> Option<std::net::IpAddr> {
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    bare.parse().ok()
}

/// The registrable domain (eTLD+1, Public Suffix List) of a hostname, used
/// as the domain cap bucket and admission key. Falls back to the
/// lowercased host itself when the PSL has no answer (e.g. a bare public
/// suffix).
///
/// A host that is an address has no domain. It gets its address block
/// ([`ip_block`]) instead: the addresses of one block are one party
/// here, as they are for the address bucket, so a holder of many
/// addresses does not get a bucket for each.
pub fn registrable_domain(host: &str) -> String {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if let Some(ip) = literal_ip(&host) {
        return ip_block(ip);
    }
    match psl::domain_str(&host) {
        Some(d) => d.to_owned(),
        None => host,
    }
}

#[cfg(test)]
mod tests {
    use super::{ip_block, literal_ip, registrable_domain};

    #[test]
    fn registrable_domains() {
        assert_eq!(registrable_domain("pds.example.com"), "example.com");
        assert_eq!(registrable_domain("a.b.example.co.uk"), "example.co.uk");
        assert_eq!(registrable_domain("Alice.Example.COM."), "example.com");
    }

    #[test]
    fn a_host_that_is_an_address_is_counted_by_its_block() {
        // Every address of a /48 is one party, bracketed or not.
        let block = "2001:db8:7::/48";
        for host in [
            "[2001:db8:7::1]",
            "[2001:db8:7:ffff::2]",
            "[2001:DB8:7:1:2:3:4:5]",
            "2001:db8:7::99",
        ] {
            assert_eq!(registrable_domain(host), block, "{host}");
        }
        assert_ne!(registrable_domain("[2001:db8:8::1]"), block);
        // IPv4: the /24, and never the last two octets read as a domain.
        assert_eq!(registrable_domain("203.0.113.7"), "203.0.113.0/24");
        assert_eq!(registrable_domain("203.0.113.200"), "203.0.113.0/24");
        assert_ne!(registrable_domain("198.51.113.7"), "203.0.113.0/24");
        assert_eq!(
            ip_block("203.0.113.7".parse().expect("address")),
            "203.0.113.0/24"
        );
        // A name is a name.
        assert_eq!(literal_ip("pds.example.com"), None);
        assert_eq!(literal_ip("[pds.example.com]"), None);
        assert_eq!(literal_ip("2001.example"), None);
    }
}
