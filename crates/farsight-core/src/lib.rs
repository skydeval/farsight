//! Shared types for Farsight: DID, AT-URI, TID and NSID handling, record
//! validation, configuration loading (TOML plus environment overrides) and the
//! safe outbound HTTP client.

#![warn(missing_docs)]

pub mod aturi;
pub mod cloudflare;
pub mod config;
pub mod did;
pub mod duration;
pub mod net;
pub mod nsid;
pub mod record;
pub mod tid;

pub use aturi::{AtUri, AtUriError, RecordKey, RecordKeyError};
pub use config::{Config, ConfigError, LoadedConfig, StartMode};
pub use did::{Did, DidError, DidMethod};
pub use duration::ConfigDuration;
pub use nsid::{Collection, Nsid, NsidError};
pub use record::{
    BlockRecord, CommitAction, CommitOp, JetstreamEvent, ListBlockRecord, ListItemRecord,
    ListPurpose, ListRecord, Operation, Record, RecordError,
};
pub use tid::{Tid, TidError};

/// The registrable domain (eTLD+1, Public Suffix List) of a hostname, used
/// as the domain cap bucket and admission key. Falls back to the
/// lowercased host itself when the PSL has no answer (e.g. a bare public
/// suffix).
pub fn registrable_domain(host: &str) -> String {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    match psl::domain_str(&host) {
        Some(d) => d.to_owned(),
        None => host,
    }
}

#[cfg(test)]
mod tests {
    use super::registrable_domain;

    #[test]
    fn registrable_domains() {
        assert_eq!(registrable_domain("pds.example.com"), "example.com");
        assert_eq!(registrable_domain("a.b.example.co.uk"), "example.co.uk");
        assert_eq!(registrable_domain("Alice.Example.COM."), "example.com");
    }
}
