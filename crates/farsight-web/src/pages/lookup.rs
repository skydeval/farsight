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
    let handle = handle.trim().trim_start_matches('@').to_ascii_lowercase();
    if !farsight_core::did::is_valid_hostname(&handle) {
        return Err(HandleError::Invalid(handle));
    }
    if let Ok(txts) = safe.txt(&format!("_atproto.{handle}")).await {
        for t in txts {
            if let Some(d) = t.strip_prefix("did=")
                && let Ok(did) = Did::parse(d.trim())
            {
                return Ok(did);
            }
        }
    }
    let url = url::Url::parse(&format!("https://{handle}/.well-known/atproto-did"))?;
    let r = match safe.get(&url).await {
        Ok(r) => r,
        Err(source) => return Err(HandleError::Unreachable { handle, source }),
    };
    if r.status != 200 {
        return Err(HandleError::Http {
            handle,
            status: r.status,
        });
    }
    let body = String::from_utf8_lossy(&r.body);
    Did::parse(body.trim()).map_err(|_| HandleError::NotADid(handle))
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
