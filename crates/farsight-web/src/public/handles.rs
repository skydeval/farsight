//! Handles on public pages (see `docs/design/api.md` and
//! `docs/design/web-ui.md`). A page shows a handle only when it has
//! been verified in both directions:
//!
//! 1. read the DID document — did:plc from `backfill.plc_url`, did:web
//!    from its host — through the safe client (see
//!    `docs/design/security.md`);
//! 2. take the first `at://` entry of `alsoKnownAs`;
//! 3. resolve that handle forward and require the result to equal the DID.
//!
//! At most one resolution per page view (the page's subject or list
//! owner), under a process-wide budget. Rendering a row never waits for a
//! resolution: it shows a handle only if one is already cached, and asks
//! the warming worker ([`super::warming`]) for the accounts it had to show
//! as DIDs. A profile card ([`super::card`]) verifies the handle of its
//! account under the card budget and writes the same cache.
//!
//! The answer of a check is kept in two places: the memory cache, and
//! the `handle_cache` table, which a restart does not empty. A read
//! looks in memory, then in the table ([`recall`]); a check writes both
//! ([`settle`]). A stored handle verified more than [`STALE_AFTER`] ago
//! is still shown, and is verified again in the background. If that
//! check establishes nothing (a host that does not answer), the handle
//! stays. If it establishes that the handle is no longer the account's
//! (the document names none or another one, or the handle now resolves
//! to another account), the handle is dropped at once, from memory and
//! from the table: a name that someone else may hold is never shown on
//! this account's rows. A stored "no handle to show" is checked again
//! after [`NONE_STALE_AFTER`].
//!
//! With warming on, a public table leaves out an account that has no
//! answer yet and says how many it left out; the page's script reads
//! the page again until they are there. The account then shows with its
//! verified handle, or as its DID if the check found none.

use std::time::Duration;

use chrono::{DateTime, Utc};

use farsight_api::ratelimit::Class;
use farsight_core::config::Config;
use farsight_core::net::{OutboundClient, SafeClient};
use farsight_core::{Did, DidMethod};
use farsight_storage::handles::Cached;

use super::metrics as m;
use crate::pages::WebState;

/// How long a page waits for its handle before rendering the DID alone.
pub const RESOLVE_WAIT: Duration = Duration::from_secs(2);
/// How long a failure or "no handle" is remembered.
pub const NEGATIVE_TTL: Duration = Duration::from_secs(600);
/// A stored handle verified longer ago than this is verified again in the
/// background the next time it is read.
pub const STALE_AFTER: Duration = Duration::from_secs(7 * 24 * 3600);
/// A stored "no handle to show" older than this is checked again in the
/// background the next time it is read.
pub const NONE_STALE_AFTER: Duration = Duration::from_secs(3600);
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

    /// The `outcome` label of
    /// `farsight_public_ui_handle_resolutions_total`.
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

/// Takes one resolution from the process-wide budget
/// (`public_ui.handle_rps`, not per client). Search resolves handles from
/// the same budget.
pub fn take_budget(st: &WebState, cfg: &Config) -> bool {
    let limit = Class::PublicHandle.limit(cfg, None);
    st.api
        .limiter
        .check(Class::PublicHandle, BUDGET_KEY, limit)
        .is_ok()
}

/// The URL of a DID's document, with the PLC directory the config names.
pub fn document_url(cfg: &Config, did: &Did) -> Option<url::Url> {
    document_url_at(&cfg.backfill.plc_url, did)
}

/// The URL of a DID's document: did:plc in the directory at `plc_url`,
/// did:web on its own host.
pub fn document_url_at(plc_url: &str, did: &Did) -> Option<url::Url> {
    let s = match did.method() {
        DidMethod::Plc => format!("{}/{}", plc_url.trim_end_matches('/'), did.as_str()),
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

/// What a verification established about an account's handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verified {
    /// The document names this handle and it resolves back to the DID.
    Handle(String),
    /// The account has no handle to show, for certain: its directory
    /// says the DID is gone, its document names no handle, or the handle
    /// it names resolves to another account.
    Gone,
    /// The document names this handle, which could not be resolved back.
    /// Whatever else was shown for the account is no longer its handle.
    Unresolved(String),
    /// Nothing was established: the document could not be read.
    Unknown,
}

/// Classifies what was read: the document (or why there is none) and,
/// if it names a handle, what that handle resolved to. `answered` says
/// that a failed resolution is the handle's own domain saying it names
/// no account (see `handle_to_did_answered`).
pub(crate) fn classify(
    did: &Did,
    claim: Option<String>,
    back: Option<Result<Did, crate::pages::HandleError>>,
    answered: bool,
) -> (Outcome, Verified) {
    let Some(claim) = claim else {
        return (Outcome::Failed, Verified::Gone);
    };
    match back {
        Some(Ok(b)) if b == *did => (Outcome::Resolved, Verified::Handle(claim)),
        // The handle is someone else's now.
        Some(Ok(_)) => (Outcome::Unverified, Verified::Gone),
        // The domain withdrew it: it names no account any more.
        Some(Err(_)) if answered => (Outcome::Unverified, Verified::Gone),
        _ => (Outcome::Unverified, Verified::Unresolved(claim)),
    }
}

/// Verifies the handle of `did` in both directions (steps 1–3 above).
pub(crate) async fn verify(safe: &SafeClient, cfg: &Config, did: &Did) -> (Outcome, Verified) {
    let Some(url) = document_url(cfg, did) else {
        return (Outcome::Failed, Verified::Unknown);
    };
    let doc = match safe.get(&url).await {
        Ok(r) if r.status == 200 => match serde_json::from_slice::<serde_json::Value>(&r.body) {
            Ok(doc) => doc,
            Err(_) => return (Outcome::Failed, Verified::Unknown),
        },
        // The directory knows no such DID, or its tombstone.
        Ok(r) if matches!(r.status, 404 | 410) => return (Outcome::Failed, Verified::Gone),
        _ => return (Outcome::Failed, Verified::Unknown),
    };
    let claim = claimed_handle(&doc);
    let (back, answered) = match &claim {
        Some(handle) => {
            let (back, answered) = crate::pages::handle_to_did_answered(safe, handle).await;
            (Some(back), answered)
        }
        None => (None, false),
    };
    classify(did, claim, back, answered)
}

/// Whether an answer checked at `resolved_at` is older than `after`.
fn older_than(resolved_at: DateTime<Utc>, now: DateTime<Utc>, after: Duration) -> bool {
    now.signed_duration_since(resolved_at)
        .to_std()
        .is_ok_and(|age| age > after)
}

/// Whether a handle verified at `resolved_at` is due a fresh verification.
pub fn is_stale(resolved_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    older_than(resolved_at, now, STALE_AFTER)
}

/// Fills the memory cache from `handle_cache` for those of `dids` it has
/// no live entry for. One query for a page's accounts; nothing is fetched
/// from outside. If the table cannot be read the accounts show as DIDs.
pub async fn recall(st: &WebState, cfg: &Config, dids: &[String]) {
    let cache = &st.public.handles;
    let mut missing: Vec<String> = dids
        .iter()
        .filter(|d| cache.lookup(d).is_none())
        .cloned()
        .collect();
    missing.sort_unstable();
    missing.dedup();
    if missing.is_empty() {
        return;
    }
    let found = match st.api.pool.acquire().await {
        Ok(mut conn) => farsight_storage::handles::stored(&mut conn, &missing).await,
        Err(e) => Err(e.into()),
    };
    let rows = match found {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, "stored handles could not be read");
            return;
        }
    };
    let ttl = cfg.public_ui.handle_cache_ttl.get();
    let now = Utc::now();
    for r in rows {
        // An empty handle: the last check found none to show.
        let (handle, stale) = if r.handle.is_empty() {
            (None, older_than(r.resolved_at, now, NONE_STALE_AFTER))
        } else {
            let stale = is_stale(r.resolved_at, now);
            (Some(r.handle), stale)
        };
        if stale {
            cache.insert_stale(&r.did, handle, ttl);
        } else {
            cache.insert(&r.did, handle, ttl);
        }
    }
}

/// What the cache holds for one account: memory, then the stored handle.
/// A stale handle is returned as it is and handed to the warming worker.
pub(crate) async fn cached(st: &WebState, cfg: &Config, did: &Did) -> Option<Cached> {
    let cache = &st.public.handles;
    if cache.lookup(did.as_str()).is_none() {
        recall(st, cfg, &[did.to_string()]).await;
    }
    let (found, stale) = cache.lookup_stale(did.as_str())?;
    if stale && cfg.public_ui.handle_warming_enabled {
        st.public.warm.push_page(vec![did.to_string()]);
    }
    Some(found)
}

/// Records how a verification of `did` ended and returns the handle to
/// show.
///
/// - A verified handle goes to the memory cache and to the table.
/// - [`Verified::Gone`] removes whatever was shown: memory and table say
///   "nothing to show" from now on.
/// - [`Verified::Unresolved`] keeps a shown handle only if it is the one
///   the document still names (its host may only be unreachable) and it
///   was verified within the last two weeks; any other shown handle is
///   removed like a gone one.
/// - [`Verified::Unknown`] is remembered in memory for [`NEGATIVE_TTL`]:
///   as the handle shown until now if there is one, as "nothing to show"
///   otherwise — and then in the table too, so that the account's rows
///   show its DID from now on instead of waiting for a check on every
///   view.
pub(crate) async fn settle(
    st: &WebState,
    cfg: &Config,
    did: &Did,
    found: Verified,
) -> Option<String> {
    use farsight_storage::handles as stored;
    let cache = &st.public.handles;
    let kept = cache.get(did.as_str());
    let (shown, ttl) = match &found {
        Verified::Handle(h) => (Some(h.clone()), cfg.public_ui.handle_cache_ttl.get()),
        Verified::Gone => (None, cfg.public_ui.handle_cache_ttl.get()),
        Verified::Unresolved(claim) => (kept.clone().filter(|k| k == claim), NEGATIVE_TTL),
        Verified::Unknown => (kept.clone(), NEGATIVE_TTL),
    };
    cache.insert(did.as_str(), shown.clone(), ttl);
    let written = match st.api.pool.acquire().await {
        Ok(mut conn) => match &found {
            Verified::Handle(h) => stored::store(&mut conn, did, h).await,
            Verified::Gone => stored::store_gone(&mut conn, did).await,
            // A stored handle other than the claim is replaced by
            // "nothing to show", whether or not memory still held it;
            // the claim itself is left as it is.
            Verified::Unresolved(claim) => {
                match stored::store_unresolved(&mut conn, did, claim).await {
                    Ok(()) if shown.is_none() => stored::store_none(&mut conn, did).await,
                    other => other,
                }
            }
            Verified::Unknown if kept.is_none() => stored::store_none(&mut conn, did).await,
            Verified::Unknown => Ok(()),
        },
        Err(e) => Err(e.into()),
    };
    if let Err(e) = written {
        tracing::warn!(error = %e, "a handle check could not be stored");
    }
    shown
}

/// What a page knows about the handle of its subject or list owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageHandle {
    /// The verified handle, if there is one to show.
    pub handle: Option<String>,
    /// Whether that is an answer: `false` when nothing could be checked
    /// (the budget was empty, or the check ran out of time). The page
    /// then shows the DID alone for now, and must not be kept by a cache
    /// as if that were the account's state.
    pub settled: bool,
}

/// The verified handle of a page's subject or list owner, resolving it if
/// it is not cached and the budget allows. The caller has checked that
/// the DID has an `actors` row and is not withheld, and calls this before
/// taking a render slot, so a slow host holds no slot.
pub async fn page_handle(st: &WebState, cfg: &Config, did: &Did) -> PageHandle {
    if let Some(c) = cached(st, cfg, did).await {
        m::handle_resolution(Outcome::Cached);
        return PageHandle {
            handle: match c {
                Cached::Handle(h) => Some(h),
                Cached::None => None,
            },
            settled: true,
        };
    }
    if !take_budget(st, cfg) {
        m::handle_resolution(Outcome::Skipped);
        return PageHandle {
            handle: None,
            settled: false,
        };
    }
    match tokio::time::timeout(RESOLVE_WAIT, verify(&st.safe, cfg, did)).await {
        Ok((outcome, found)) => {
            m::handle_resolution(outcome);
            PageHandle {
                handle: settle(st, cfg, did, found).await,
                settled: true,
            }
        }
        // Too slow: the page renders with the DID alone and nothing is
        // cached.
        Err(_) => {
            m::handle_resolution(Outcome::Failed);
            PageHandle {
                handle: None,
                settled: false,
            }
        }
    }
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
    fn a_handle_that_resolves_to_another_account_is_gone() {
        use crate::pages::HandleError;
        let me = Did::parse("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let other = Did::parse("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let claim = || Some("alice.example".to_owned());
        assert_eq!(
            classify(&me, claim(), Some(Ok(me.clone())), true),
            (Outcome::Resolved, Verified::Handle("alice.example".into()))
        );
        // Someone else holds the name now: certain, not a failure to
        // reach a host.
        assert_eq!(
            classify(&me, claim(), Some(Ok(other)), true),
            (Outcome::Unverified, Verified::Gone)
        );
        // The document names no handle at all.
        assert_eq!(
            classify(&me, None, None, false),
            (Outcome::Failed, Verified::Gone)
        );
        // The handle's host did not say: the claim stands unproven.
        assert_eq!(
            classify(
                &me,
                claim(),
                Some(Err(HandleError::NotADid("alice.example".into()))),
                false
            ),
            (
                Outcome::Unverified,
                Verified::Unresolved("alice.example".into())
            )
        );
        // The domain itself answered that it names no account: the
        // handle was withdrawn, and is not kept on the account's rows.
        assert_eq!(
            classify(
                &me,
                claim(),
                Some(Err(HandleError::Http {
                    handle: "alice.example".into(),
                    status: 404
                })),
                true
            ),
            (Outcome::Unverified, Verified::Gone)
        );
    }

    #[test]
    fn a_handle_is_stale_after_seven_days() {
        let now = Utc::now();
        let day = chrono::Duration::days(1);
        assert!(!is_stale(now, now));
        assert!(!is_stale(now - day * 7 + chrono::Duration::seconds(1), now));
        assert!(is_stale(now - day * 7 - chrono::Duration::seconds(1), now));
        // A clock that went backwards makes nothing stale.
        assert!(!is_stale(now + day, now));
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
