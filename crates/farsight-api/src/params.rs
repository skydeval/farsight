//! Query-string parameters (XRPC params; repeated keys allowed, e.g.
//! `others`).

use farsight_core::Did;

use crate::error::XrpcError;

/// Default page size (§3.1).
pub const DEFAULT_LIMIT: i64 = 100;
/// Maximum page size (§3.1).
pub const MAX_LIMIT: i64 = 1000;

/// Parsed query parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Params(Vec<(String, String)>);

impl Params {
    /// Parses a raw query string.
    pub fn parse(q: &str) -> Params {
        Params(
            url::form_urlencoded::parse(q.as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect(),
        )
    }

    /// From explicit pairs.
    pub fn from_pairs(pairs: Vec<(String, String)>) -> Params {
        Params(pairs)
    }

    /// The first value of `name`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Every value of `name` (also accepts `name[]`).
    pub fn all(&self, name: &str) -> Vec<&str> {
        let alt = format!("{name}[]");
        self.0
            .iter()
            .filter(|(k, _)| k == name || *k == alt)
            .map(|(_, v)| v.as_str())
            .collect()
    }

    /// A required DID parameter. Handles are `InvalidRequest` (§3.1).
    pub fn did(&self, name: &str) -> Result<Did, XrpcError> {
        let v = self
            .get(name)
            .ok_or_else(|| XrpcError::invalid(format!("missing required parameter `{name}`")))?;
        parse_did(name, v)
    }

    /// A boolean (`true`/`false`, default false).
    pub fn bool(&self, name: &str) -> Result<bool, XrpcError> {
        match self.get(name) {
            None => Ok(false),
            Some("true") | Some("1") => Ok(true),
            Some("false") | Some("0") => Ok(false),
            Some(v) => Err(XrpcError::invalid(format!(
                "`{name}` must be true or false, got {v:?}"
            ))),
        }
    }

    /// `limit` in `1..=1000`, default 100.
    pub fn limit(&self) -> Result<i64, XrpcError> {
        match self.get("limit") {
            None => Ok(DEFAULT_LIMIT),
            Some(v) => match v.parse::<i64>() {
                Ok(n) if (1..=MAX_LIMIT).contains(&n) => Ok(n),
                _ => Err(XrpcError::invalid(format!(
                    "`limit` must be an integer between 1 and {MAX_LIMIT}"
                ))),
            },
        }
    }
}

/// Parses a DID, refusing handles with a clear message.
pub fn parse_did(name: &str, v: &str) -> Result<Did, XrpcError> {
    Did::parse(v).map_err(|e| {
        if v.starts_with("did:") {
            XrpcError::invalid(format!("`{name}` is not a valid DID: {e}"))
        } else {
            XrpcError::invalid(format!(
                "`{name}` must be a DID; handles are not accepted (got {v:?})"
            ))
        }
    })
}

/// A list purpose filter (`modlist`, `curatelist`, `referencelist`,
/// `other`) as its storage code.
pub fn purpose_code(v: Option<&str>) -> Result<Option<i16>, XrpcError> {
    match v {
        None => Ok(None),
        Some("modlist") => Ok(Some(1)),
        Some("curatelist") => Ok(Some(2)),
        Some("referencelist") => Ok(Some(3)),
        Some("other") => Ok(Some(0)),
        Some(o) => Err(XrpcError::invalid(format!("unknown purpose {o:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_and_limits() {
        let p = Params::parse("actor=did%3Aplc%3Aabc&others=a&others=b&others[]=c&limit=5");
        assert_eq!(p.all("others"), ["a", "b", "c"]);
        assert_eq!(p.limit().unwrap(), 5);
        assert!(Params::parse("limit=0").limit().is_err());
        assert!(Params::parse("limit=1001").limit().is_err());
        assert_eq!(Params::parse("").limit().unwrap(), 100);
    }

    #[test]
    fn handles_rejected() {
        let e = Params::parse("actor=alice.bsky.social")
            .did("actor")
            .unwrap_err();
        assert_eq!(e.name, "InvalidRequest");
        assert!(e.message.contains("handles"));
    }
}
