//! Bearer tokens (see `docs/design/api.md`): admin `fsa_<43 base64url>`
//! and API keys `fsk_<43>`, 256-bit, stored as SHA-256 and compared in
//! constant time.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, RwLock};

use axum::http::{HeaderMap, header};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use farsight_core::Config;
use farsight_storage::codes::RequesterKey;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use subtle::ConstantTimeEq;

use crate::error::XrpcError;

/// Admin token prefix.
pub const ADMIN_PREFIX: &str = "fsa_";
/// API key prefix.
pub const KEY_PREFIX: &str = "fsk_";
/// Base64url characters after the prefix (256 bits).
pub const TOKEN_CHARS: usize = 43;

/// Scope: read queries.
pub const SCOPE_READ: &str = "read";
/// Scope: `requestBackfill` / `getBackfillStatus`.
pub const SCOPE_BACKFILL: &str = "backfill";
/// Scope: `requestBackfill` with `priority = high`.
pub const SCOPE_BACKFILL_HIGH: &str = "backfill:high";
/// Every scope.
pub const SCOPES: [&str; 3] = [SCOPE_READ, SCOPE_BACKFILL, SCOPE_BACKFILL_HIGH];

/// `n` bytes from the OS CSPRNG.
pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut b = [0u8; N];
    getrandom::getrandom(&mut b).expect("OS random source");
    b
}

/// A new token with `prefix` (`fsa_` or `fsk_`).
pub fn generate(prefix: &str) -> String {
    format!("{prefix}{}", URL_SAFE_NO_PAD.encode(random_bytes::<32>()))
}

/// SHA-256 of a token.
pub fn sha256(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Lowercase hex of `bytes`: the form `auth.admin_token_sha256` holds a
/// token hash in.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Whether `token` has the shape `<prefix><43 base64url>`.
pub fn well_formed(token: &str, prefix: &str) -> bool {
    token.strip_prefix(prefix).is_some_and(|rest| {
        rest.len() == TOKEN_CHARS
            && rest
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    })
}

/// Constant-time comparison of a token's hash with the stored admin hash
/// (`auth.admin_token_sha256`, hex).
pub fn is_admin_token(token: &str, cfg: &Config) -> bool {
    let Some(stored) = unhex(&cfg.auth.admin_token_sha256) else {
        return false;
    };
    let h = sha256(token);
    stored.len() == 32 && bool::from(h.ct_eq(stored.as_slice()))
}

/// A live API key: a row of `api_tokens` whose `revoked_at` is null, as
/// [`KeyTable`] holds it in memory.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyInfo {
    /// `api_tokens.id`.
    pub id: i32,
    /// `api_tokens.name`: the label given when the key was created.
    pub name: String,
    /// `api_tokens.scopes`: which of [`SCOPES`] the key carries.
    pub scopes: Vec<String>,
    /// `api_tokens.read_rps`: read requests per second for this key in
    /// place of `rate_limit.key_rps`, with the burst scaled by the same
    /// ratio. `None` (or a value not above zero) uses the configured rate.
    pub read_rps: Option<f32>,
}

impl KeyInfo {
    /// Whether the key has `scope`.
    pub fn has(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope)
    }
}

/// Who is calling.
#[derive(Debug, Clone, PartialEq)]
pub enum Caller {
    /// No `Authorization` header.
    Anonymous,
    /// A well-formed `fsk_` token whose hash is in the [`KeyTable`].
    Key(KeyInfo),
    /// The `fsa_` token whose hash is `auth.admin_token_sha256`.
    Admin,
}

impl Caller {
    /// The requester key (`backfill_queue.requester`, intern cause).
    pub fn requester(&self) -> Option<RequesterKey> {
        match self {
            Caller::Anonymous => None,
            Caller::Key(k) => Some(RequesterKey::Token(k.id)),
            Caller::Admin => Some(RequesterKey::Admin),
        }
    }
}

/// The in-memory table of live API keys, keyed by hash. Refreshed from
/// `api_tokens` periodically and after every create/revoke.
#[derive(Debug, Default)]
pub struct KeyTable {
    keys: RwLock<HashMap<[u8; 32], KeyInfo>>,
    used: Mutex<HashSet<i32>>,
}

impl KeyTable {
    /// Replaces the table with the rows of `api_tokens` that are not
    /// revoked. A row whose stored hash is not 32 bytes is skipped.
    pub async fn refresh(&self, pool: &PgPool) -> Result<(), farsight_storage::StorageError> {
        let rows = farsight_storage::auth::active_tokens(pool).await?;
        let mut map = HashMap::new();
        for t in rows {
            let Ok(h) = <[u8; 32]>::try_from(t.sha256.as_slice()) else {
                continue;
            };
            map.insert(
                h,
                KeyInfo {
                    id: t.id,
                    name: t.name,
                    scopes: t.scopes,
                    read_rps: t.read_rps,
                },
            );
        }
        *self.keys.write().unwrap_or_else(|e| e.into_inner()) = map;
        Ok(())
    }

    /// The key whose hash is `h` (the hash is of a 256-bit token, so the
    /// map lookup leaks nothing useful about other keys).
    pub fn lookup(&self, h: &[u8; 32]) -> Option<KeyInfo> {
        let map = self.keys.read().unwrap_or_else(|e| e.into_inner());
        let k = map.get(h)?;
        self.used
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(k.id);
        Some(k.clone())
    }

    /// Writes `last_used_at` for keys used since the last flush.
    pub async fn flush_usage(&self, pool: &PgPool) -> Result<(), farsight_storage::StorageError> {
        let ids: Vec<i32> = self
            .used
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect();
        farsight_storage::auth::touch_tokens(pool, &ids).await
    }
}

/// The bearer token in `Authorization`, if any. A present header that is
/// not `Bearer <token>` is an error.
pub fn bearer(headers: &HeaderMap) -> Result<Option<&str>, XrpcError> {
    let Some(v) = headers.get(header::AUTHORIZATION) else {
        return Ok(None);
    };
    let v = v
        .to_str()
        .map_err(|_| XrpcError::auth_required("malformed Authorization header"))?;
    let (scheme, token) = v
        .split_once(' ')
        .ok_or_else(|| XrpcError::auth_required("expected `Authorization: Bearer <token>`"))?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(XrpcError::auth_required(
            "expected `Authorization: Bearer <token>`",
        ));
    }
    Ok(Some(token.trim()))
}

/// Identifies the caller. A presented but invalid token is `401
/// AuthRequired`.
pub fn authenticate(
    headers: &HeaderMap,
    cfg: &Config,
    keys: &KeyTable,
) -> Result<Caller, XrpcError> {
    let Some(token) = bearer(headers)? else {
        return Ok(Caller::Anonymous);
    };
    if well_formed(token, ADMIN_PREFIX) && is_admin_token(token, cfg) {
        return Ok(Caller::Admin);
    }
    if well_formed(token, KEY_PREFIX) {
        if let Some(k) = keys.lookup(&sha256(token)) {
            return Ok(Caller::Key(k));
        }
    }
    Err(XrpcError::auth_required("invalid or revoked token"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_shapes() {
        let a = generate(ADMIN_PREFIX);
        assert!(well_formed(&a, ADMIN_PREFIX));
        assert!(!well_formed(&a, KEY_PREFIX));
        assert_eq!(a.len(), 4 + TOKEN_CHARS);
        assert!(!well_formed("fsa_short", ADMIN_PREFIX));
        assert!(!well_formed(
            &format!("fsa_{}", "!".repeat(43)),
            ADMIN_PREFIX
        ));
    }

    #[test]
    fn admin_token_check() {
        let t = generate(ADMIN_PREFIX);
        let mut cfg = Config::default();
        cfg.auth.admin_token_sha256 = hex(&sha256(&t));
        assert!(is_admin_token(&t, &cfg));
        assert!(!is_admin_token(&generate(ADMIN_PREFIX), &cfg));
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            format!("Bearer {t}").parse().unwrap(),
        );
        assert_eq!(
            authenticate(&h, &cfg, &KeyTable::default()).unwrap(),
            Caller::Admin
        );
        h.insert(header::AUTHORIZATION, "Bearer fsk_nope".parse().unwrap());
        assert_eq!(
            authenticate(&h, &cfg, &KeyTable::default())
                .unwrap_err()
                .name,
            "AuthRequired"
        );
        assert_eq!(
            authenticate(&HeaderMap::new(), &cfg, &KeyTable::default()).unwrap(),
            Caller::Anonymous
        );
    }
}
