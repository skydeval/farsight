//! NSIDs (Namespaced Identifiers) and the four collections Farsight indexes.

use std::fmt;
use std::str::FromStr;

/// Maximum NSID length (ATProto NSID spec).
pub const MAX_NSID_LEN: usize = 317;

/// `app.bsky.graph.block`.
pub const BLOCK: &str = "app.bsky.graph.block";
/// `app.bsky.graph.listblock`.
pub const LISTBLOCK: &str = "app.bsky.graph.listblock";
/// `app.bsky.graph.list`.
pub const LIST: &str = "app.bsky.graph.list";
/// `app.bsky.graph.listitem`.
pub const LISTITEM: &str = "app.bsky.graph.listitem";

/// All four indexed collections, in the order of their storage codes.
pub const INDEXED_COLLECTIONS: [&str; 4] = [BLOCK, LISTBLOCK, LIST, LISTITEM];

/// Why a string is not a valid NSID.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid NSID: {0}")]
pub struct NsidError(pub String);

/// A validated NSID, e.g. `app.bsky.graph.block`.
///
/// Syntax per the ATProto spec: at least three dot-separated segments; the
/// domain authority segments are `[a-zA-Z0-9-]`, 1-63 chars, not starting or
/// ending with `-`, the first not starting with a digit, authority at most
/// 253 chars; the final name segment is `[a-zA-Z][a-zA-Z0-9]*`, 1-63 chars.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Nsid(String);

impl Nsid {
    /// Parses and validates an NSID.
    pub fn parse(s: &str) -> Result<Nsid, NsidError> {
        let err = || NsidError(s.to_owned());
        if s.is_empty() || s.len() > MAX_NSID_LEN || !s.is_ascii() {
            return Err(err());
        }
        let segments: Vec<&str> = s.split('.').collect();
        if segments.len() < 3 {
            return Err(err());
        }
        let (name, authority) = segments.split_last().ok_or_else(err)?;
        let authority_len = s.len() - name.len() - 1;
        if authority_len > 253 {
            return Err(err());
        }
        for (i, seg) in authority.iter().enumerate() {
            if seg.is_empty()
                || seg.len() > 63
                || seg.starts_with('-')
                || seg.ends_with('-')
                || !seg.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
                || (i == 0 && seg.as_bytes()[0].is_ascii_digit())
            {
                return Err(err());
            }
        }
        if name.is_empty()
            || name.len() > 63
            || !name.as_bytes()[0].is_ascii_alphabetic()
            || !name.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(err());
        }
        Ok(Nsid(s.to_owned()))
    }

    /// The NSID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The indexed collection this NSID names, if any.
    pub fn collection(&self) -> Option<Collection> {
        Collection::from_nsid(&self.0)
    }
}

impl fmt::Display for Nsid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Nsid {
    type Err = NsidError;
    fn from_str(s: &str) -> Result<Nsid, NsidError> {
        Nsid::parse(s)
    }
}

/// One of the four indexed collections.
///
/// The numeric codes are the `tombstones.collection` / `backfill_cursors`
/// storage codes (1 block, 2 listblock, 3 list, 4 listitem).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Collection {
    /// `app.bsky.graph.block`.
    Block,
    /// `app.bsky.graph.listblock`.
    ListBlock,
    /// `app.bsky.graph.list`.
    List,
    /// `app.bsky.graph.listitem`.
    ListItem,
}

impl Collection {
    /// All four, in storage-code order.
    pub const ALL: [Collection; 4] = [
        Collection::Block,
        Collection::ListBlock,
        Collection::List,
        Collection::ListItem,
    ];

    /// Maps an NSID string to an indexed collection.
    pub fn from_nsid(s: &str) -> Option<Collection> {
        match s {
            BLOCK => Some(Collection::Block),
            LISTBLOCK => Some(Collection::ListBlock),
            LIST => Some(Collection::List),
            LISTITEM => Some(Collection::ListItem),
            _ => None,
        }
    }

    /// The collection's NSID.
    pub fn nsid(self) -> &'static str {
        match self {
            Collection::Block => BLOCK,
            Collection::ListBlock => LISTBLOCK,
            Collection::List => LIST,
            Collection::ListItem => LISTITEM,
        }
    }

    /// The storage code: 1 block, 2 listblock, 3 list, 4 listitem.
    pub fn code(self) -> i16 {
        match self {
            Collection::Block => 1,
            Collection::ListBlock => 2,
            Collection::List => 3,
            Collection::ListItem => 4,
        }
    }

    /// Inverse of [`Collection::code`].
    pub fn from_code(code: i16) -> Option<Collection> {
        match code {
            1 => Some(Collection::Block),
            2 => Some(Collection::ListBlock),
            3 => Some(Collection::List),
            4 => Some(Collection::ListItem),
            _ => None,
        }
    }
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.nsid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid() {
        for ok in [
            BLOCK,
            LISTBLOCK,
            LIST,
            LISTITEM,
            "com.example.fooBar",
            "com.example.foo2",
            "net.users.bob.ping",
            "a-0.b-1.c",
            "cn.8.lex.stuff",
        ] {
            assert!(Nsid::parse(ok).is_ok(), "rejected {ok:?}");
        }
    }

    #[test]
    fn rejects_invalid() {
        for bad in [
            "",
            "com.example",
            "com",
            "com.example.",
            ".com.example.foo",
            "com..example.foo",
            "com.example.3foo",
            "com.example.foo-bar",
            "com.example.foo_bar",
            "1com.example.foo",
            "-com.example.foo",
            "com-.example.foo",
            "com.exa mple.foo",
            "com.example.fooé",
        ] {
            assert!(Nsid::parse(bad).is_err(), "accepted {bad:?}");
        }
        let long_seg = format!("com.{}.foo", "a".repeat(64));
        assert!(Nsid::parse(&long_seg).is_err());
    }

    #[test]
    fn collection_codes_round_trip() {
        for c in Collection::ALL {
            assert_eq!(Collection::from_code(c.code()), Some(c));
            assert_eq!(Collection::from_nsid(c.nsid()), Some(c));
            assert_eq!(Nsid::parse(c.nsid()).unwrap().collection(), Some(c));
        }
        assert_eq!(Collection::from_code(0), None);
        assert_eq!(Collection::from_nsid("app.bsky.feed.post"), None);
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(256))]

        /// Any string at all: parsing returns, and what it accepts is the
        /// input itself, within the length bound, with three segments or
        /// more and a name that starts with a letter.
        #[test]
        fn parsing_is_total_and_accepted_nsids_round_trip(
            s in proptest::prop_oneof![
                proptest::prelude::any::<String>(),
                "[a-zA-Z0-9.-]{0,80}",
                "[a-z0-9-]{1,70}(\\.[a-zA-Z0-9-]{0,70}){0,6}",
            ],
        ) {
            let Ok(nsid) = Nsid::parse(&s) else { return Ok(()) };
            proptest::prop_assert_eq!(nsid.as_str(), s.as_str());
            proptest::prop_assert_eq!(nsid.to_string(), s.clone());
            proptest::prop_assert_eq!(s.parse::<Nsid>(), Ok(nsid.clone()));
            proptest::prop_assert!(s.len() <= MAX_NSID_LEN);
            let segments: Vec<&str> = s.split('.').collect();
            proptest::prop_assert!(segments.len() >= 3);
            proptest::prop_assert!(segments.iter().all(|x| (1..=63).contains(&x.len())));
            let name = segments[segments.len() - 1];
            proptest::prop_assert!(name.as_bytes()[0].is_ascii_alphabetic());
            proptest::prop_assert_eq!(nsid.collection(), Collection::from_nsid(&s));
        }

        /// A well-formed NSID is accepted.
        #[test]
        fn well_formed_nsids_are_accepted(
            s in "[a-z][a-z0-9]{0,9}(\\.[a-z0-9]([a-z0-9-]{0,8}[a-z0-9])?){1,3}\\.[a-zA-Z][a-zA-Z0-9]{0,20}",
        ) {
            proptest::prop_assert!(Nsid::parse(&s).is_ok(), "{}", s);
        }
    }
}
