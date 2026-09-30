//! DIDs: `did:plc` and `did:web`, the two methods ATProto blesses.
//!
//! Validation follows the ATProto DID spec (general DID syntax, max 2048
//! chars) plus the method rules: `did:plc` identifiers are 24 characters of
//! lowercase base32 (`a-z`, `2-7`); `did:web` identifiers are a hostname with
//! no path (ATProto forbids paths) and, only for `localhost`, a
//! percent-encoded port. Any other method is rejected: Farsight stores only
//! DIDs it can resolve.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum DID length allowed by the ATProto DID spec.
pub const MAX_DID_LEN: usize = 2048;

/// The two DID methods Farsight accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DidMethod {
    /// `did:plc:<24 base32 chars>`.
    Plc,
    /// `did:web:<hostname>`.
    Web,
}

/// Why a string is not an acceptable DID.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DidError {
    /// Empty, too long, or not of the form `did:<method>:<id>`.
    #[error("not a DID: {0}")]
    Syntax(String),
    /// A syntactically valid DID whose method Farsight does not accept.
    #[error("unsupported DID method: {0}")]
    UnsupportedMethod(String),
    /// `did:plc` with a malformed identifier.
    #[error("invalid did:plc identifier: {0}")]
    InvalidPlc(String),
    /// `did:web` with a malformed hostname, a path, or a port.
    #[error("invalid did:web identifier: {0}")]
    InvalidWeb(String),
}

/// A validated `did:plc` or `did:web` DID.
///
/// Ordering and equality are bytewise on the canonical string, which is
/// what Postgres `COLLATE "C"` keys use, so a sorted `Vec<Did>` matches the
/// database's index order.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Did(String);

impl Did {
    /// Parses and validates a DID.
    pub fn parse(s: &str) -> Result<Did, DidError> {
        if s.is_empty() || s.len() > MAX_DID_LEN || !s.is_ascii() {
            return Err(DidError::Syntax(s.to_owned()));
        }
        let rest = s
            .strip_prefix("did:")
            .ok_or_else(|| DidError::Syntax(s.to_owned()))?;
        let (method, id) = rest
            .split_once(':')
            .ok_or_else(|| DidError::Syntax(s.to_owned()))?;
        if method.is_empty() || !method.bytes().all(|b| b.is_ascii_lowercase()) {
            return Err(DidError::Syntax(s.to_owned()));
        }
        if id.is_empty()
            || id.ends_with(':')
            || id.ends_with('%')
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'%' | b'-'))
        {
            return Err(DidError::Syntax(s.to_owned()));
        }
        match method {
            "plc" => {
                if id.len() == 24
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
                {
                    Ok(Did(s.to_owned()))
                } else {
                    Err(DidError::InvalidPlc(s.to_owned()))
                }
            }
            "web" => {
                validate_web_id(id).map_err(|_| DidError::InvalidWeb(s.to_owned()))?;
                Ok(Did(s.to_owned()))
            }
            other => Err(DidError::UnsupportedMethod(other.to_owned())),
        }
    }

    /// The canonical string form.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the DID, returning its string.
    pub fn into_string(self) -> String {
        self.0
    }

    /// Which method this DID uses.
    pub fn method(&self) -> DidMethod {
        if self.0.starts_with("did:plc:") {
            DidMethod::Plc
        } else {
            DidMethod::Web
        }
    }

    /// For `did:web`, the hostname (lowercased, port stripped); `None` for
    /// `did:plc`.
    pub fn web_host(&self) -> Option<String> {
        let id = self.0.strip_prefix("did:web:")?;
        let host = id.split("%3A").next().unwrap_or(id);
        let host = host.split("%3a").next().unwrap_or(host);
        Some(host.to_ascii_lowercase())
    }
}

fn validate_web_id(id: &str) -> Result<(), ()> {
    // No path segments: ATProto did:web is hostname-level only.
    if id.contains(':') {
        return Err(());
    }
    let (host, port) = match id.find('%') {
        Some(i) => {
            let (h, p) = id.split_at(i);
            let p = p
                .strip_prefix("%3A")
                .or_else(|| p.strip_prefix("%3a"))
                .ok_or(())?;
            (h, Some(p))
        }
        None => (id, None),
    };
    if let Some(port) = port {
        // Ports are only permitted for localhost (development).
        if !host.eq_ignore_ascii_case("localhost")
            || port.is_empty()
            || port.len() > 5
            || !port.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(());
        }
        return Ok(());
    }
    if is_valid_hostname(host) {
        Ok(())
    } else {
        Err(())
    }
}

/// Hostname syntax as the ATProto handle rules define it: 2+ labels, each
/// 1-63 chars of `[a-zA-Z0-9-]` not starting or ending with `-`, total at
/// most 253 chars, and a TLD that does not start with a digit.
pub fn is_valid_hostname(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    let labels: Vec<&str> = host.split('.').collect();
    if labels.len() < 2 {
        return false;
    }
    for label in &labels {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return false;
        }
    }
    let tld = labels[labels.len() - 1];
    !tld.as_bytes()[0].is_ascii_digit()
}

impl fmt::Display for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Did {
    type Err = DidError;
    fn from_str(s: &str) -> Result<Did, DidError> {
        Did::parse(s)
    }
}

impl AsRef<str> for Did {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Serialize for Did {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Did {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Did, D::Error> {
        let s = String::deserialize(d)?;
        Did::parse(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plc() {
        let d = Did::parse("did:plc:z72i7hdynmk6r22z27h6tvur").unwrap();
        assert_eq!(d.method(), DidMethod::Plc);
        assert_eq!(d.web_host(), None);
    }

    #[test]
    fn accepts_web() {
        let d = Did::parse("did:web:example.com").unwrap();
        assert_eq!(d.method(), DidMethod::Web);
        assert_eq!(d.web_host().as_deref(), Some("example.com"));
        let d = Did::parse("did:web:localhost%3A2583").unwrap();
        assert_eq!(d.web_host().as_deref(), Some("localhost"));
    }

    #[test]
    fn rejects_invalid() {
        for bad in [
            "",
            "did",
            "did:",
            "did:plc",
            "did:plc:",
            "DID:plc:z72i7hdynmk6r22z27h6tvur",
            "did:PLC:z72i7hdynmk6r22z27h6tvur",
            // wrong length
            "did:plc:z72i7hdynmk6r22z27h6tvu",
            "did:plc:z72i7hdynmk6r22z27h6tvurx",
            // uppercase / digits outside base32
            "did:plc:Z72i7hdynmk6r22z27h6tvur",
            "did:plc:z72i7hdynmk6r22z27h6tvu1",
            "did:plc:z72i7hdynmk6r22z27h6tvu8",
            // did:web with path, port, bad hostname
            "did:web:example.com:path",
            "did:web:example.com%3A443",
            "did:web:localhost",
            "did:web:-bad.com",
            "did:web:example.123",
            "did:web:exa_mple.com",
            // trailing colon or percent
            "did:web:example.com:",
            "did:web:example.com%",
            // unsupported methods
            "did:key:zQ3shunBKsXixLxKtC5qeSG9E4J5RkGN57im31pcTzbNQnm5w",
            "did:example:123",
            // handles are not DIDs
            "alice.bsky.social",
            "at://did:plc:z72i7hdynmk6r22z27h6tvur",
            " did:plc:z72i7hdynmk6r22z27h6tvur",
            "did:plc:z72i7hdynmk6r22z27h6tvur ",
        ] {
            assert!(Did::parse(bad).is_err(), "accepted {bad:?}");
        }
        assert!(matches!(
            Did::parse("did:key:zabc"),
            Err(DidError::UnsupportedMethod(_))
        ));
    }

    #[test]
    fn rejects_overlong() {
        let long = format!("did:web:{}.com", "a".repeat(MAX_DID_LEN));
        assert!(Did::parse(&long).is_err());
    }

    #[test]
    fn orders_bytewise() {
        let mut v = [
            Did::parse("did:web:b.example").unwrap(),
            Did::parse("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            Did::parse("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            Did::parse("did:web:A.example").unwrap(),
        ];
        v.sort();
        let s: Vec<&str> = v.iter().map(Did::as_str).collect();
        assert_eq!(
            s,
            [
                "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
                "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
                "did:web:A.example",
                "did:web:b.example",
            ]
        );
    }

    #[test]
    fn serde_round_trip() {
        let d = Did::parse("did:plc:z72i7hdynmk6r22z27h6tvur").unwrap();
        let j = serde_json::to_string(&d).unwrap();
        assert_eq!(serde_json::from_str::<Did>(&j).unwrap(), d);
        assert!(serde_json::from_str::<Did>("\"did:key:x\"").is_err());
    }
}
