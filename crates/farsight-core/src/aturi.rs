//! Record keys and AT-URIs.
//!
//! Farsight only accepts AT-URIs of the full record form
//! `at://<did>/<collection>/<rkey>` whose authority is a DID: handle
//! authorities would need resolution and can change owner, so they are
//! rejected at parse time.

use std::fmt;
use std::str::FromStr;

use crate::did::{Did, DidError};
use crate::nsid::{Collection, Nsid, NsidError};

/// Maximum record key length (ATProto record key spec).
pub const MAX_RKEY_LEN: usize = 512;

/// Maximum AT-URI length Farsight accepts (ATProto spec: 8 KiB).
pub const MAX_AT_URI_LEN: usize = 8 * 1024;

/// Why a string is not a valid record key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid record key: {0}")]
pub struct RecordKeyError(pub String);

/// A validated record key: 1-512 chars of `[A-Za-z0-9._:~-]`, not `.` or
/// `..`. Ordering is bytewise, matching `COLLATE "C"` and the PDS's
/// `listRecords` order used by range reconcile.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecordKey(String);

impl RecordKey {
    /// Parses and validates a record key.
    pub fn parse(s: &str) -> Result<RecordKey, RecordKeyError> {
        if s.is_empty()
            || s.len() > MAX_RKEY_LEN
            || s == "."
            || s == ".."
            || !s
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'~' | b'-'))
        {
            return Err(RecordKeyError(s.to_owned()));
        }
        Ok(RecordKey(s.to_owned()))
    }

    /// The key string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RecordKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for RecordKey {
    type Err = RecordKeyError;
    fn from_str(s: &str) -> Result<RecordKey, RecordKeyError> {
        RecordKey::parse(s)
    }
}

/// Why a string is not an acceptable AT-URI.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AtUriError {
    /// Not `at://authority/collection/rkey`, too long, or carries a query
    /// or fragment.
    #[error("not a record AT-URI: {0}")]
    Syntax(String),
    /// The authority is a handle (or anything else that is not a DID).
    #[error("AT-URI authority must be a DID: {0}")]
    HandleAuthority(String),
    /// The authority looks like a DID but is not one Farsight accepts.
    #[error("AT-URI authority: {0}")]
    Did(#[from] DidError),
    /// The collection segment is not an NSID.
    #[error("AT-URI collection: {0}")]
    Collection(#[from] NsidError),
    /// The record key segment is invalid.
    #[error("AT-URI record key: {0}")]
    RecordKey(#[from] RecordKeyError),
}

/// A record AT-URI with a DID authority.
///
/// Ordering is by authority, then collection, then record key, all
/// bytewise.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AtUri {
    /// The repo the record is in. Always a DID: a handle authority is
    /// refused at parse.
    pub authority: Did,
    /// The record's collection.
    pub collection: Nsid,
    /// The record key.
    pub rkey: RecordKey,
}

impl AtUri {
    /// Parses a record AT-URI. Handle authorities are rejected.
    pub fn parse(s: &str) -> Result<AtUri, AtUriError> {
        let syntax = || AtUriError::Syntax(s.to_owned());
        if s.len() > MAX_AT_URI_LEN || !s.is_ascii() || s.contains(['?', '#']) {
            return Err(syntax());
        }
        let rest = s.strip_prefix("at://").ok_or_else(syntax)?;
        let mut parts = rest.split('/');
        let authority = parts.next().ok_or_else(syntax)?;
        let collection = parts.next().ok_or_else(syntax)?;
        let rkey = parts.next().ok_or_else(syntax)?;
        if parts.next().is_some() {
            return Err(syntax());
        }
        if !authority.starts_with("did:") {
            return Err(AtUriError::HandleAuthority(s.to_owned()));
        }
        Ok(AtUri {
            authority: Did::parse(authority)?,
            collection: Nsid::parse(collection)?,
            rkey: RecordKey::parse(rkey)?,
        })
    }

    /// Builds a URI from validated parts.
    pub fn new(authority: Did, collection: Collection, rkey: RecordKey) -> AtUri {
        AtUri {
            authority,
            // Indexed collection NSIDs are valid by construction.
            collection: Nsid::parse(collection.nsid()).expect("indexed NSIDs are valid"),
            rkey,
        }
    }

    /// The indexed collection this URI points into, if any.
    pub fn indexed_collection(&self) -> Option<Collection> {
        self.collection.collection()
    }
}

impl fmt::Display for AtUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "at://{}/{}/{}",
            self.authority, self.collection, self.rkey
        )
    }
}

impl FromStr for AtUri {
    type Err = AtUriError;
    fn from_str(s: &str) -> Result<AtUri, AtUriError> {
        AtUri::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DID: &str = "did:plc:z72i7hdynmk6r22z27h6tvur";

    #[test]
    fn rkeys() {
        let max = "x".repeat(512);
        let over = "x".repeat(513);
        for ok in ["3l3qo2vutsw2b", "self", "a.b_c:d~e-f", "A", max.as_str()] {
            assert!(RecordKey::parse(ok).is_ok(), "rejected {ok:?}");
        }
        for bad in ["", ".", "..", "a/b", "a b", "a#b", "é", over.as_str()] {
            assert!(RecordKey::parse(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn parses_full_uri() {
        let s = format!("at://{DID}/app.bsky.graph.list/3k2abc");
        let u = AtUri::parse(&s).unwrap();
        assert_eq!(u.authority.as_str(), DID);
        assert_eq!(u.indexed_collection(), Some(Collection::List));
        assert_eq!(u.rkey.as_str(), "3k2abc");
        assert_eq!(u.to_string(), s);
    }

    #[test]
    fn rejects_handle_authority() {
        let r = AtUri::parse("at://alice.bsky.social/app.bsky.graph.list/3k2abc");
        assert!(matches!(r, Err(AtUriError::HandleAuthority(_))));
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "".to_owned(),
            format!("https://{DID}/app.bsky.graph.list/3k2abc"),
            format!("at://{DID}"),
            format!("at://{DID}/app.bsky.graph.list"),
            format!("at://{DID}/app.bsky.graph.list/"),
            format!("at://{DID}/app.bsky.graph.list/3k2abc/extra"),
            format!("at://{DID}/app.bsky.graph.list/3k2abc?x=1"),
            format!("at://{DID}/app.bsky.graph.list/3k2abc#frag"),
            format!("at://{DID}/not-an-nsid/3k2abc"),
            "at://did:key:zabc/app.bsky.graph.list/3k2abc".to_owned(),
            "at://did:plc:short/app.bsky.graph.list/3k2abc".to_owned(),
        ] {
            assert!(AtUri::parse(&bad).is_err(), "accepted {bad:?}");
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(256))]

        /// Over the record-key alphabet a key is accepted exactly when it
        /// has 1 to 512 characters and is not `.` or `..`; anything else
        /// is accepted only if it is in that alphabet. An accepted key is
        /// the input.
        #[test]
        fn record_keys_follow_the_rule(
            s in proptest::prop_oneof![
                proptest::prelude::any::<String>(),
                "[A-Za-z0-9._:~-]{0,8}",
                "[A-Za-z0-9._:~-]{500,520}",
                "\\.{0,3}",
            ],
        ) {
            let in_alphabet = s
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:~-".contains(&b));
            let expected =
                in_alphabet && (1..=MAX_RKEY_LEN).contains(&s.len()) && s != "." && s != "..";
            let parsed = RecordKey::parse(&s);
            proptest::prop_assert_eq!(parsed.is_ok(), expected);
            if let Ok(k) = parsed {
                proptest::prop_assert_eq!(k.as_str(), s.as_str());
                proptest::prop_assert_eq!(k.to_string(), s.clone());
                proptest::prop_assert_eq!(s.parse::<RecordKey>(), Ok(k));
            }
        }

        /// Any string at all: parsing returns, and what it accepts prints
        /// back as the input, within the length bound, with parts that
        /// are valid alone.
        #[test]
        fn parsing_is_total_and_accepted_uris_round_trip(
            s in proptest::prop_oneof![
                proptest::prelude::any::<String>(),
                "(at://)?[a-z2-7:./#?%-]{0,60}",
                "at://did:plc:[a-z2-7]{24}(/[a-zA-Z.]{0,24}){0,4}",
                "at://(did:web:)?[a-z]{1,8}\\.[a-z]{2,5}/app\\.bsky\\.graph\\.(block|list|x)/[a-z0-9./]{0,14}",
            ],
        ) {
            let Ok(uri) = AtUri::parse(&s) else { return Ok(()) };
            proptest::prop_assert_eq!(uri.to_string(), s.clone());
            proptest::prop_assert_eq!(s.parse::<AtUri>(), Ok(uri.clone()));
            proptest::prop_assert!(s.len() <= MAX_AT_URI_LEN);
            proptest::prop_assert_eq!(Did::parse(uri.authority.as_str()), Ok(uri.authority.clone()));
            proptest::prop_assert_eq!(Nsid::parse(uri.collection.as_str()), Ok(uri.collection.clone()));
            proptest::prop_assert_eq!(
                uri.indexed_collection(),
                Collection::from_nsid(uri.collection.as_str())
            );
        }

        /// Valid parts make a URI that parses back to the same parts.
        #[test]
        fn a_uri_built_from_valid_parts_parses_back(
            plc in "[a-z2-7]{24}",
            c in 0usize..4,
            rkey in "[A-Za-z0-9_:~-][A-Za-z0-9._:~-]{0,40}",
        ) {
            let did = Did::parse(&format!("did:plc:{plc}")).unwrap();
            let rkey = RecordKey::parse(&rkey).unwrap();
            let uri = AtUri::new(did.clone(), Collection::ALL[c], rkey.clone());
            let parsed = AtUri::parse(&uri.to_string()).unwrap();
            proptest::prop_assert_eq!(&parsed, &uri);
            proptest::prop_assert_eq!(parsed.indexed_collection(), Some(Collection::ALL[c]));
            proptest::prop_assert_eq!((parsed.authority, parsed.rkey), (did, rkey));
        }
    }
}
