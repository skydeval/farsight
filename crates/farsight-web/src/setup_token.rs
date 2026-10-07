//! The setup token (see `docs/design/web-ui.md`): 128 bits from the OS
//! CSPRNG, shown as `fst-XXXXX-XXXXX-XXXXX-XXXXX-XXXXXX` (Crockford base32,
//! case-insensitive, hyphens ignored), stored with its creation time in
//! `/etc/farsight/.setup-token` (0600), preserved across restarts while
//! unexpired, expiring after 24 h unless a verified setup session was active
//! in the last hour (then postponed, at most 72 h after creation).

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};

/// Crockford base32 alphabet.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// How long after its creation a token expires, unless a verified setup
/// session postpones it.
pub const LIFETIME: Duration = Duration::from_secs(24 * 3600);
/// The latest a token can expire after its creation, however active the
/// session.
pub const MAX_LIFETIME: Duration = Duration::from_secs(72 * 3600);
/// A verified session within this window postpones rotation.
pub const ACTIVE_WINDOW: Duration = Duration::from_secs(3600);
/// The wizard warns this long before expiry.
pub const WARN_BEFORE: Duration = Duration::from_secs(3600);
/// How often setup mode prints the token to the log again.
pub const REPRINT: Duration = Duration::from_secs(600);

/// A setup token and its creation time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupToken {
    /// Display form, `fst-XXXXX-XXXXX-XXXXX-XXXXX-XXXXXX`.
    pub token: String,
    /// When it was generated (UTC): the second line of the token file,
    /// and what both lifetimes count from.
    pub created: DateTime<Utc>,
}

/// The token file next to `config.toml`.
pub fn token_path(config_path: &Path) -> PathBuf {
    config_path.with_file_name(".setup-token")
}

fn encode(bytes: &[u8; 16]) -> String {
    // 128 bits → 26 base32 digits (the top digit carries 3 bits).
    let n = u128::from_be_bytes(*bytes);
    (0..26)
        .rev()
        .map(|i| ALPHABET[((n >> (i * 5)) & 31) as usize] as char)
        .collect()
}

/// A new token from the OS CSPRNG, created now. Not written anywhere.
pub fn generate() -> SetupToken {
    let digits = encode(&farsight_api::auth::random_bytes::<16>());
    let token = format!(
        "fst-{}-{}-{}-{}-{}",
        &digits[0..5],
        &digits[5..10],
        &digits[10..15],
        &digits[15..20],
        &digits[20..26]
    );
    SetupToken {
        token,
        created: Utc::now(),
    }
}

/// Canonical form for comparison: uppercase, hyphens and spaces removed,
/// the `FST` prefix dropped, Crockford look-alikes mapped (`O`→`0`,
/// `I`/`L`→`1`).
pub fn normalize(s: &str) -> String {
    let up: String = s
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .map(|c| c.to_ascii_uppercase())
        .collect();
    let body = up.strip_prefix("FST").unwrap_or(&up);
    body.chars()
        .map(|c| match c {
            'O' => '0',
            'I' | 'L' => '1',
            c => c,
        })
        .collect()
}

/// Constant-time check of a submitted token.
pub fn matches(submitted: &str, token: &SetupToken) -> bool {
    crate::common::ct_eq(&normalize(submitted), &normalize(&token.token))
}

/// Reads the token file: the token on its first line, its RFC 3339
/// creation time on the second. `None` when the file is missing or does
/// not have that shape.
pub fn read(path: &Path) -> Option<SetupToken> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut lines = text.lines();
    let token = lines.next()?.trim().to_owned();
    let created = DateTime::parse_from_rfc3339(lines.next()?.trim())
        .ok()?
        .with_timezone(&Utc);
    Some(SetupToken { token, created })
}

/// Writes the token file (0600).
pub fn write(path: &Path, t: &SetupToken) -> std::io::Result<()> {
    let text = format!("{}\n{}\n", t.token, t.created.to_rfc3339());
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    farsight_core::config::write_replace(path, &text)
}

/// Deletes the token file (setup completed).
pub fn delete(path: &Path) {
    let _ = std::fs::remove_file(path);
}

/// Prints the token (the only secret Farsight logs).
pub fn print(t: &SetupToken) {
    tracing::warn!(
        setup_token = %t.token,
        "Farsight is in setup mode. Open the web UI and enter this setup token. \
         Re-print it with `docker logs farsight` or `docker exec farsight farsight setup-token`."
    );
}

/// When `t` expires given whether a verified session was active in the
/// last hour: 24 h after creation, postponed while active, never past
/// 72 h.
pub fn expires_at(t: &SetupToken, now: DateTime<Utc>, active: bool) -> DateTime<Utc> {
    let base = t.created + chrono::Duration::from_std(LIFETIME).expect("fits");
    let max = t.created + chrono::Duration::from_std(MAX_LIFETIME).expect("fits");
    if active && now >= base {
        // Postponed: lives on while the session stays active, bounded.
        (now + chrono::Duration::from_std(ACTIVE_WINDOW).expect("fits")).min(max)
    } else {
        base
    }
}

/// The token in force: the stored one if unexpired, else a new one
/// (written and printed). `active` = a verified session in the last hour.
pub fn current_or_rotate(path: &Path, active: bool) -> std::io::Result<(SetupToken, bool)> {
    let now = Utc::now();
    if let Some(t) = read(path) {
        if now < expires_at(&t, now, active) {
            return Ok((t, false));
        }
    }
    let t = generate();
    write(path, &t)?;
    print(&t);
    Ok((t, true))
}

/// Unconditionally replaces the token (`farsight setup-token --rotate`,
/// config reset).
pub fn rotate(path: &Path) -> std::io::Result<SetupToken> {
    let t = generate();
    write(path, &t)?;
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shape_and_normalization() {
        let t = generate();
        assert_eq!(t.token.len(), "fst-XXXXX-XXXXX-XXXXX-XXXXX-XXXXXX".len());
        assert!(t.token.starts_with("fst-"));
        assert!(matches(&t.token.to_lowercase(), &t));
        assert!(matches(&t.token.replace('-', ""), &t));
        assert!(!matches("fst-00000-00000-00000-00000-000000", &t));
        assert_eq!(normalize("fst-o1l-I"), "0111");
    }

    #[test]
    fn expiry_rules() {
        let t = SetupToken {
            token: "x".into(),
            created: Utc::now() - chrono::Duration::hours(25),
        };
        let now = Utc::now();
        assert!(expires_at(&t, now, false) < now);
        assert!(expires_at(&t, now, true) > now);
        let old = SetupToken {
            created: Utc::now() - chrono::Duration::hours(73),
            ..t
        };
        assert!(expires_at(&old, now, true) < now);
    }
}
