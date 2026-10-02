//! Handles on public pages (design §3.6, §8.6). Farsight stores no
//! handles; a page shows one only when it has been verified in both
//! directions:
//!
//! 1. read the DID document — did:plc from `backfill.plc_url`, did:web
//!    from its host — through the safe client (§11.3);
//! 2. take the first `at://` entry of `alsoKnownAs`;
//! 3. resolve that handle forward and require the result to equal the DID.
//!
//! At most one resolution per page view (the page's subject or list
//! owner), under a process-wide budget. Rows never trigger a resolution;
//! they show a handle only if one is already cached. A profile card
//! ([`super::card`]) verifies the handle of its account under the card
//! budget and writes the same cache, so rows fill in as cards are opened
//! and as pages are visited.

use std::time::Duration;

use farsight_api::ratelimit::Class;
use farsight_core::config::Config;
use farsight_core::net::{OutboundClient, SafeClient};
use farsight_core::{Did, DidMethod};
use farsight_storage::handles::Cached;

use super::metrics as m;
use crate::pages::{WebState, resolve_handle};

/// How long a page waits for its handle before rendering the DID alone.
pub const RESOLVE_WAIT: Duration = Duration::from_secs(2);
/// How long a failure or "no handle" is remembered.
pub const NEGATIVE_TTL: Duration = Duration::from_secs(600);
/// Bucket key of the process-wide resolution budget.
pub const BUDGET_KEY: &str = "process";

/// How a resolution ended (`farsight_public_ui_handle_resolutions_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Answered from the cache.
    Cached,
    /// Verified in both directions.
    Resolved,
    /// The document names a handle that does not resolve back to the DID.
    Unverified,
    /// The document could not be read, or names no handle.
    Failed,
    /// No budget: nothing was fetched.
    Skipped,
}

impl Outcome {
    /// Every outcome.
    pub const ALL: [Outcome; 5] = [
        Outcome::Cached,
        Outcome::Resolved,
        Outcome::Unverified,
        Outcome::Failed,
        Outcome::Skipped,
    ];

    /// Metric label.
    pub fn label(self) -> &'static str {
        match self {
            Outcome::Cached => "cached",
            Outcome::Resolved => "resolved",
            Outcome::Unverified => "unverified",
            Outcome::Failed => "failed",
            Outcome::Skipped => "skipped",
        }
    }
}

/// Takes one resolution from the process-wide budget (§3.6: 2 per second,
/// burst 10, not per client). Search resolves handles from the same
/// budget.
pub fn take_budget(st: &WebState, cfg: &Config) -> bool {
    let limit = Class::PublicHandle.limit(cfg, None);
    st.api
        .limiter
        .check(Class::PublicHandle, BUDGET_KEY, limit)
        .is_ok()
}

/// The URL of a DID's document.
pub fn document_url(cfg: &Config, did: &Did) -> Option<url::Url> {
    let s = match did.method() {
        DidMethod::Plc => format!(
            "{}/{}",
            cfg.backfill.plc_url.trim_end_matches('/'),
            did.as_str()
        ),
        DidMethod::Web => format!("https://{}/.well-known/did.json", did.web_host()?),
    };
    url::Url::parse(&s).ok()
}

/// The handle a DID document claims: the first `at://` entry of
/// `alsoKnownAs`, lower-cased, if it is a valid hostname.
pub fn claimed_handle(doc: &serde_json::Value) -> Option<String> {
    let h = doc["alsoKnownAs"]
        .as_array()?
        .iter()
        .filter_map(|v| v.as_str())
        .find_map(|s| s.strip_prefix("at://"))?
        .to_ascii_lowercase();
    farsight_core::did::is_valid_hostname(&h).then_some(h)
}

async fn verify(safe: &SafeClient, cfg: &Config, did: &Did) -> (Outcome, Option<String>) {
    let Some(url) = document_url(cfg, did) else {
        return (Outcome::Failed, None);
    };
    let doc = match safe.get(&url).await {
        Ok(r) if r.status == 200 => serde_json::from_slice::<serde_json::Value>(&r.body).ok(),
        _ => None,
    };
    let Some(handle) = doc.as_ref().and_then(claimed_handle) else {
        return (Outcome::Failed, None);
    };
    match resolve_handle(safe, &handle).await {
        Ok(back) if back == *did => (Outcome::Resolved, Some(handle)),
        _ => (Outcome::Unverified, None),
    }
}

/// The verified handle of a page's subject or list owner, resolving it if
/// it is not cached and the budget allows. The caller has checked that
/// the DID has an `actors` row and is not withheld, and calls this before
/// taking a render slot, so a slow host holds no slot.
pub async fn page_handle(st: &WebState, cfg: &Config, did: &Did) -> Option<String> {
    let cache = &st.public.handles;
    if let Some(c) = cache.lookup(did.as_str()) {
        m::handle_resolution(Outcome::Cached);
        return match c {
            Cached::Handle(h) => Some(h),
            Cached::None => None,
        };
    }
    if !take_budget(st, cfg) {
        m::handle_resolution(Outcome::Skipped);
        return None;
    }
    match tokio::time::timeout(RESOLVE_WAIT, verify(&st.safe, cfg, did)).await {
        Ok((outcome, handle)) => {
            m::handle_resolution(outcome);
            let ttl = if handle.is_some() {
                cfg.public_ui.handle_cache_ttl.get()
            } else {
                NEGATIVE_TTL
            };
            cache.insert(did.as_str(), handle.clone(), ttl);
            handle
        }
        // Too slow: the page renders with the DID alone and nothing is
        // cached.
        Err(_) => {
            m::handle_resolution(Outcome::Failed);
            None
        }
    }
}

/// The cached handle of a DID named in a row, if there is one.
pub fn row_handle(st: &WebState, did: &str) -> Option<String> {
    st.public.handles.get(did)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn first_at_entry_is_the_claim() {
        let doc =
            json!({"alsoKnownAs": ["https://x.example", "at://Alice.Example", "at://b.example"]});
        assert_eq!(claimed_handle(&doc).as_deref(), Some("alice.example"));
        assert_eq!(claimed_handle(&json!({"alsoKnownAs": []})), None);
        assert_eq!(claimed_handle(&json!({})), None);
        // Not a hostname: nothing to verify, nothing to show.
        assert_eq!(
            claimed_handle(&json!({"alsoKnownAs": ["at://<script>"]})),
            None
        );
    }

    #[test]
    fn document_urls() {
        let mut c = Config::default();
        c.backfill.plc_url = "https://plc.example/".into();
        let plc = Did::parse("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        assert_eq!(
            document_url(&c, &plc).unwrap().as_str(),
            "https://plc.example/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let web = Did::parse("did:web:Alice.Example").unwrap();
        assert_eq!(
            document_url(&c, &web).unwrap().as_str(),
            "https://alice.example/.well-known/did.json"
        );
    }
}
