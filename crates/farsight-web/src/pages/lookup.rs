//! What the lookups share: handle resolution, the query permit and the
//! forms a list reference is accepted in.

use farsight_core::net::{OutboundClient, SafeClient};
use farsight_core::{AtUri, Collection, Did};

use super::WebState;

/// Why a handle (or a DID typed in its place) did not give a DID.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HandleError {
    /// The text is not a hostname, so not a handle.
    #[error("{0:?} is neither a DID nor a valid handle")]
    Invalid(String),
    /// The text starts like a DID and is not one.
    #[error(transparent)]
    Did(#[from] farsight_core::did::DidError),
    /// The well-known URL could not be formed.
    #[error(transparent)]
    Url(#[from] url::ParseError),
    /// The handle's host gave no answer: refused by the safe client, no
    /// address, a transport failure or a timeout.
    #[error("could not resolve {handle}: {source}")]
    Unreachable {
        /// The handle.
        handle: String,
        /// Why there was no answer.
        #[source]
        source: farsight_core::net::OutboundError,
    },
    /// The handle's host answered with a status other than 200.
    #[error("could not resolve {handle}: HTTP {status}")]
    Http {
        /// The handle.
        handle: String,
        /// The status.
        status: u16,
    },
    /// The handle's host answered with a body that is not a DID.
    #[error("{0} did not resolve to a DID")]
    NotADid(String),
}

/// No query permit came free in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("The server is busy; try again shortly.")]
pub struct Busy;

/// Resolves a handle to a DID: DNS TXT `_atproto.<handle>`, then
/// `https://<handle>/.well-known/atproto-did`, through the safe client.
pub async fn handle_to_did(safe: &SafeClient, handle: &str) -> Result<Did, HandleError> {
    handle_to_did_answered(safe, handle).await.0
}

/// [`handle_to_did`], and whether a failure is the domain's own answer:
/// DNS answered that the name carries no DID, and the host answered the
/// well-known request with a refusal or with something that is not a
/// DID. Then the domain does not name an account, as opposed to not
/// having been heard from.
pub async fn handle_to_did_answered(
    safe: &SafeClient,
    handle: &str,
) -> (Result<Did, HandleError>, bool) {
    let handle = handle.trim().trim_start_matches('@').to_ascii_lowercase();
    if !farsight_core::did::is_valid_hostname(&handle) {
        return (Err(HandleError::Invalid(handle)), false);
    }
    let dns = safe.txt(&format!("_atproto.{handle}")).await;
    let dns_answered = dns.is_ok();
    if let Ok(txts) = dns
        && let Some(did) = txt_did(&txts)
    {
        return (Ok(did), true);
    }
    let url = match url::Url::parse(&format!("https://{handle}/.well-known/atproto-did")) {
        Ok(u) => u,
        Err(e) => return (Err(e.into()), false),
    };
    let r = match safe.get(&url).await {
        Ok(r) => r,
        Err(source) => return (Err(HandleError::Unreachable { handle, source }), false),
    };
    if r.status != 200 {
        let refused = denies(r.status);
        return (
            Err(HandleError::Http {
                handle,
                status: r.status,
            }),
            dns_answered && refused,
        );
    }
    let body = String::from_utf8_lossy(&r.body);
    match Did::parse(body.trim()) {
        Ok(did) => (Ok(did), true),
        Err(_) => (Err(HandleError::NotADid(handle)), dns_answered),
    }
}

/// The DID the `_atproto` TXT records of a handle name: the one every
/// `did=` record that holds a DID agrees on. Records that name different
/// DIDs name none, as the handle specification has it, and the handle is
/// then asked over HTTPS like one without a record.
pub fn txt_did(records: &[String]) -> Option<Did> {
    let mut named = records
        .iter()
        .filter_map(|t| t.strip_prefix("did="))
        .filter_map(|d| Did::parse(d.trim()).ok());
    let first = named.next()?;
    named.all(|d| d == first).then_some(first)
}

/// Whether a status of the well-known request says the host does not
/// name an account: a client error other than "try later".
pub fn denies(status: u16) -> bool {
    (400..500).contains(&status) && !matches!(status, 408 | 425 | 429)
}

pub(crate) async fn permit(st: &WebState) -> Result<tokio::sync::OwnedSemaphorePermit, Busy> {
    match tokio::time::timeout(
        farsight_api::PERMIT_WAIT,
        st.api.query_permits.clone().acquire_owned(),
    )
    .await
    {
        Ok(Ok(p)) => Ok(p),
        _ => Err(Busy),
    }
}

/// Parses an AT-URI or a `https://bsky.app/profile/<actor>/lists/<rkey>`
/// URL into `(actor, rkey)`; `actor` may be a handle.
pub fn parse_list_ref(q: &str) -> Option<(String, String)> {
    let q = q.trim();
    if let Ok(u) = AtUri::parse(q) {
        return (u.indexed_collection() == Some(Collection::List))
            .then(|| (u.authority.as_str().to_owned(), u.rkey.as_str().to_owned()));
    }
    let u = url::Url::parse(q).ok()?;
    let segs: Vec<&str> = u.path_segments()?.filter(|s| !s.is_empty()).collect();
    match segs.as_slice() {
        ["profile", actor, "lists", rkey] => Some(((*actor).to_owned(), (*rkey).to_owned())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txt_records_that_disagree_name_no_account() {
        let txt = |v: &[&str]| -> Vec<String> { v.iter().map(|s| (*s).to_owned()).collect() };
        let a = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let b = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        let did = |s: &str| Did::parse(s).ok();
        assert_eq!(txt_did(&txt(&[&format!("did={a}")])), did(a));
        // Other records, and `did=` records that hold no DID, do not count.
        assert_eq!(
            txt_did(&txt(&["v=spf1 -all", "did=nonsense", &format!("did={a} ")])),
            did(a)
        );
        // The same DID twice is one answer.
        assert_eq!(
            txt_did(&txt(&[&format!("did={a}"), &format!("did={a}")])),
            did(a)
        );
        // Two DIDs are none, in either order.
        assert_eq!(
            txt_did(&txt(&[&format!("did={a}"), &format!("did={b}")])),
            None
        );
        assert_eq!(
            txt_did(&txt(&[&format!("did={b}"), &format!("did={a}")])),
            None
        );
        assert_eq!(txt_did(&txt(&["v=spf1 -all"])), None);
        assert_eq!(txt_did(&[]), None);
    }

    #[test]
    fn only_a_plain_refusal_says_the_host_names_no_account() {
        for s in [400, 401, 403, 404, 410, 451] {
            assert!(denies(s), "{s}");
        }
        for s in [200, 301, 408, 425, 429, 500, 502, 503] {
            assert!(!denies(s), "{s}");
        }
    }

    #[test]
    fn list_refs() {
        assert_eq!(
            parse_list_ref("https://bsky.app/profile/alice.example/lists/3kabc"),
            Some(("alice.example".into(), "3kabc".into()))
        );
        assert_eq!(
            parse_list_ref("at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/3k"),
            Some(("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(), "3k".into()))
        );
        assert_eq!(parse_list_ref("https://bsky.app/profile/x"), None);
    }
}
