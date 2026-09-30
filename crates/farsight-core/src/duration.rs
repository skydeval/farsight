//! Human-written durations used in the config (`"7d"`, `"10m"`, `"300s"`).

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A duration written as `<integer><unit>` with unit `ms`, `s`, `m`, `h` or
/// `d`. Serializes to the largest unit that represents it exactly, so
/// `"24h"` round-trips as `"1d"` with the same value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConfigDuration(pub Duration);

/// Why a duration string is invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid duration {0:?} (expected e.g. \"300s\", \"10m\", \"1h\", \"7d\")")]
pub struct DurationError(pub String);

impl ConfigDuration {
    /// From whole seconds.
    pub const fn secs(s: u64) -> ConfigDuration {
        ConfigDuration(Duration::from_secs(s))
    }

    /// From whole minutes.
    pub const fn mins(m: u64) -> ConfigDuration {
        ConfigDuration(Duration::from_secs(m * 60))
    }

    /// From whole hours.
    pub const fn hours(h: u64) -> ConfigDuration {
        ConfigDuration(Duration::from_secs(h * 3600))
    }

    /// From whole days.
    pub const fn days(d: u64) -> ConfigDuration {
        ConfigDuration(Duration::from_secs(d * 86_400))
    }

    /// The wrapped duration.
    pub fn get(self) -> Duration {
        self.0
    }
}

impl FromStr for ConfigDuration {
    type Err = DurationError;
    fn from_str(s: &str) -> Result<ConfigDuration, DurationError> {
        let err = || DurationError(s.to_owned());
        let t = s.trim();
        let split = t
            .find(|c: char| !c.is_ascii_digit() && c != '_')
            .ok_or_else(err)?;
        let (num, unit) = t.split_at(split);
        let num = num.replace('_', "");
        if num.is_empty() {
            return Err(err());
        }
        let n: u64 = num.parse().map_err(|_| err())?;
        let ms_per: u64 = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => return Err(err()),
        };
        let ms = n.checked_mul(ms_per).ok_or_else(err)?;
        Ok(ConfigDuration(Duration::from_millis(ms)))
    }
}

impl fmt::Display for ConfigDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = self.0.as_millis();
        for (unit, per) in [
            ("d", 86_400_000u128),
            ("h", 3_600_000),
            ("m", 60_000),
            ("s", 1_000),
        ] {
            if ms != 0 && ms % per == 0 {
                return write!(f, "{}{}", ms / per, unit);
            }
        }
        if ms == 0 {
            return f.write_str("0s");
        }
        write!(f, "{ms}ms")
    }
}

impl Serialize for ConfigDuration {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ConfigDuration {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<ConfigDuration, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units() {
        assert_eq!(
            "300s".parse::<ConfigDuration>().unwrap(),
            ConfigDuration::secs(300)
        );
        assert_eq!(
            "10m".parse::<ConfigDuration>().unwrap(),
            ConfigDuration::mins(10)
        );
        assert_eq!(
            "24h".parse::<ConfigDuration>().unwrap(),
            ConfigDuration::days(1)
        );
        assert_eq!(
            "7d".parse::<ConfigDuration>().unwrap(),
            ConfigDuration::days(7)
        );
        assert_eq!(
            "250ms".parse::<ConfigDuration>().unwrap(),
            ConfigDuration(Duration::from_millis(250))
        );
    }

    #[test]
    fn displays_canonically() {
        assert_eq!(ConfigDuration::hours(24).to_string(), "1d");
        assert_eq!(ConfigDuration::secs(300).to_string(), "5m");
        assert_eq!(ConfigDuration::secs(61).to_string(), "61s");
        assert_eq!(
            ConfigDuration(Duration::from_millis(1500)).to_string(),
            "1500ms"
        );
        assert_eq!(ConfigDuration::secs(0).to_string(), "0s");
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["", "7", "d", "7w", "-1s", "1.5h", "h1"] {
            assert!(bad.parse::<ConfigDuration>().is_err(), "accepted {bad:?}");
        }
    }
}
