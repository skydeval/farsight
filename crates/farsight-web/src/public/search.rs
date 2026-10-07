//! `/search?q=…`: what a visitor may type, and where it leads.
//!
//! Input classes, tried in this order: a DID; an `at://` URI of an account
//! or a list; a `https://bsky.app/profile/…` link; otherwise a handle.
//! Nothing typed here is ever fetched as a URL: a link is only parsed, and
//! the one outbound step is handle resolution through the safe client.

use farsight_core::{Did, RecordKey};

/// Longest accepted query, in bytes.
pub const MAX_QUERY_BYTES: usize = 512;
/// The one collection a list reference may name.
pub const LIST_COLLECTION: &str = "app.bsky.graph.list";

/// Who a reference names: a DID, or a handle still to be resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    /// A DID.
    Did(Did),
    /// A syntactically valid handle, lower-cased.
    Handle(String),
}

/// What a query asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// An account page.
    Account(Authority),
    /// A list page.
    List(Authority, RecordKey),
}

/// Why a query names nothing to look up. The text is the message of the
/// 400 page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QueryError {
    /// Nothing was typed.
    #[error("Enter a handle, a DID, an at:// URI or a bsky.app profile or list link.")]
    Empty,
    /// Longer than any handle, DID or link.
    #[error("That is too long to be a handle, a DID or a link.")]
    TooLong,
    /// Starts like a DID and is not one.
    #[error("That is not a valid DID.")]
    Did,
    /// The account part is neither a DID nor a handle.
    #[error("That is not a valid DID or handle.")]
    Authority,
    /// The list's record key is not valid.
    #[error("That is not a valid list key.")]
    ListKey,
    /// An at:// URI of something other than an account or a list.
    #[error("Only accounts and lists can be looked up.")]
    NotLookup,
    /// A link other than a bsky.app profile or list link.
    #[error("Only https://bsky.app/profile/… links to an account or a list are understood.")]
    Link,
    /// None of the forms a query may take.
    #[error("That is not a handle, a DID, an at:// URI or a bsky.app profile or list link.")]
    Unrecognized,
}

fn handle(s: &str) -> Option<String> {
    let h = s.trim_start_matches('@').to_ascii_lowercase();
    farsight_core::did::is_valid_hostname(&h).then_some(h)
}

fn authority(s: &str) -> Result<Authority, QueryError> {
    if s.starts_with("did:") {
        return Did::parse(s)
            .map(Authority::Did)
            .map_err(|_| QueryError::Did);
    }
    handle(s)
        .map(Authority::Handle)
        .ok_or(QueryError::Authority)
}

fn rkey(s: &str) -> Result<RecordKey, QueryError> {
    RecordKey::parse(s).map_err(|_| QueryError::ListKey)
}

/// Parses a trimmed query. The error's text is the message of the 400
/// page.
pub fn parse(q: &str) -> Result<Target, QueryError> {
    if q.is_empty() {
        return Err(QueryError::Empty);
    }
    if q.len() > MAX_QUERY_BYTES {
        return Err(QueryError::TooLong);
    }
    if q.starts_with("did:") {
        return authority(q).map(Target::Account);
    }
    if let Some(rest) = q.strip_prefix("at://") {
        if rest.contains(['?', '#']) {
            return Err(QueryError::NotLookup);
        }
        let parts: Vec<&str> = rest.split('/').collect();
        return match parts.as_slice() {
            [a] => authority(a).map(Target::Account),
            // The collection is compared exactly: NSIDs are case-sensitive.
            [a, c, r] if *c == LIST_COLLECTION => Ok(Target::List(authority(a)?, rkey(r)?)),
            _ => Err(QueryError::NotLookup),
        };
    }
    if q.starts_with("http://") || q.starts_with("https://") {
        let bad = || QueryError::Link;
        let u = url::Url::parse(q).map_err(|_| bad())?;
        if u.scheme() != "https"
            || u.host_str() != Some("bsky.app")
            || !u.username().is_empty()
            || u.password().is_some()
            || u.port().is_some()
        {
            return Err(bad());
        }
        let segs: Vec<&str> = u.path_segments().map(|s| s.collect()).unwrap_or_default();
        // Segments are taken as written: an escaped one names nothing a
        // profile link carries.
        if segs.iter().any(|s| s.contains('%')) {
            return Err(bad());
        }
        return match segs.as_slice() {
            ["profile", a] => authority(a).map(Target::Account),
            ["profile", a, "lists", r] => Ok(Target::List(authority(a)?, rkey(r)?)),
            _ => Err(bad()),
        };
    }
    handle(q)
        .map(|h| Target::Account(Authority::Handle(h)))
        .ok_or(QueryError::Unrecognized)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    fn did() -> Authority {
        Authority::Did(Did::parse(DID).unwrap())
    }

    fn h(s: &str) -> Authority {
        Authority::Handle(s.to_owned())
    }

    fn rk(s: &str) -> RecordKey {
        RecordKey::parse(s).unwrap()
    }

    #[test]
    fn dids() {
        assert_eq!(parse(DID), Ok(Target::Account(did())));
        assert!(parse("did:plc:short").is_err());
        assert!(parse("did:key:z6Mk").is_err());
    }

    #[test]
    fn at_uris() {
        assert_eq!(parse(&format!("at://{DID}")), Ok(Target::Account(did())));
        assert_eq!(
            parse(&format!("at://{DID}/app.bsky.graph.list/3kabc")),
            Ok(Target::List(did(), rk("3kabc")))
        );
        assert_eq!(
            parse("at://alice.example/app.bsky.graph.list/3kabc"),
            Ok(Target::List(h("alice.example"), rk("3kabc")))
        );
        for bad in [
            format!("at://{DID}/app.bsky.graph.block/3kabc"),
            format!("at://{DID}/App.Bsky.Graph.List/3kabc"),
            format!("at://{DID}/app.bsky.graph.list"),
            format!("at://{DID}/app.bsky.graph.list/3kabc/extra"),
            format!("at://{DID}/app.bsky.graph.list/3kabc?x=1"),
            format!("at://{DID}/app.bsky.graph.list/3kabc#frag"),
            format!("at://{DID}/"),
        ] {
            assert!(parse(&bad).is_err(), "{bad}");
        }
        assert_eq!(
            parse(&format!("at://{DID}/app.bsky.feed.post/3k")),
            Err(QueryError::NotLookup)
        );
    }

    #[test]
    fn bsky_links() {
        assert_eq!(
            parse("https://bsky.app/profile/alice.example"),
            Ok(Target::Account(h("alice.example")))
        );
        assert_eq!(
            parse(&format!("https://bsky.app/profile/{DID}/lists/3kabc")),
            Ok(Target::List(did(), rk("3kabc")))
        );
        for bad in [
            "http://bsky.app/profile/alice.example",
            "https://bsky.app.evil.example/profile/alice.example",
            "https://evil.example/profile/alice.example",
            "https://user@bsky.app/profile/alice.example",
            "https://bsky.app:8443/profile/alice.example",
            "https://bsky.app/profile/alice.example/post/3k",
            "https://bsky.app/profile/alice.example/lists",
            "https://bsky.app/",
            "https://169.254.169.254/latest/meta-data",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn handles() {
        assert_eq!(
            parse("@Alice.Example"),
            Ok(Target::Account(h("alice.example")))
        );
        assert_eq!(
            parse("alice.example"),
            Ok(Target::Account(h("alice.example")))
        );
        assert!(parse("alice").is_err());
        assert!(parse("alice example").is_err());
        assert!(parse("").is_err());
        assert!(parse(&"a".repeat(MAX_QUERY_BYTES + 1)).is_err());
    }
}
