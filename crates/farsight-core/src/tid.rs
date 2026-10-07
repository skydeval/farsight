//! TIDs (timestamp identifiers), used as repo revisions.
//!
//! A stored `rev` is a decoded TID; events with a non-TID rev are
//! rejected. A TID is a 64-bit integer with the top bit zero (53 bits of
//! microseconds since the Unix epoch, 10 bits of clock id), written as 13
//! characters of base32-sortable (`234567abcdefghijklmnopqrstuvwxyz`). The
//! first character carries the top four bits; since the top bit must be
//! zero, it is one of `234567ab`. Decoded TIDs fit in a non-negative `i64`
//! and sort the same way as their strings, so they are stored as Postgres
//! `BIGINT`.

use std::fmt;
use std::str::FromStr;

const ALPHABET: &[u8; 32] = b"234567abcdefghijklmnopqrstuvwxyz";

/// Length of a TID string.
pub const TID_LEN: usize = 13;

/// How far past the clock the time inside a commit rev may lie, in
/// microseconds. Writes are ordered by rev and the highest one wins, so a
/// rev from far in the future would outrank every later change to its
/// record, its deletion included.
pub const MAX_REV_AHEAD_US: u64 = 5 * 60 * 1_000_000;

/// Why a string is not a TID.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not a TID: {0}")]
pub struct TidError(pub String);

/// A decoded TID. Ordering is numeric, which equals string ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tid(i64);

fn digit(b: u8) -> Option<u64> {
    match b {
        b'2'..=b'7' => Some(u64::from(b - b'2')),
        b'a'..=b'z' => Some(u64::from(b - b'a') + 6),
        _ => None,
    }
}

impl Tid {
    /// Parses a 13-character TID string.
    pub fn parse(s: &str) -> Result<Tid, TidError> {
        let bytes = s.as_bytes();
        if bytes.len() != TID_LEN {
            return Err(TidError(s.to_owned()));
        }
        let mut v: u64 = 0;
        for (i, &b) in bytes.iter().enumerate() {
            let d = digit(b).ok_or_else(|| TidError(s.to_owned()))?;
            // First char: top four bits of 64, and the top bit must be 0.
            if i == 0 && d >= 8 {
                return Err(TidError(s.to_owned()));
            }
            v = (v << 5) | d;
        }
        Ok(Tid(v as i64))
    }

    /// Builds a TID from its decoded value. `None` if negative.
    #[cfg(test)]
    fn from_i64(v: i64) -> Option<Tid> {
        if v >= 0 { Some(Tid(v)) } else { None }
    }

    /// Builds a TID from microseconds since the epoch and a clock id
    /// (0..1024). `None` if out of range.
    pub fn from_parts(micros: u64, clock_id: u16) -> Option<Tid> {
        if micros >= (1 << 53) || clock_id >= 1024 {
            return None;
        }
        Some(Tid(((micros << 10) | u64::from(clock_id)) as i64))
    }

    /// The decoded value, as stored in Postgres.
    pub fn as_i64(self) -> i64 {
        self.0
    }

    /// Microseconds since the Unix epoch encoded in the TID.
    pub fn micros(self) -> u64 {
        (self.0 as u64) >> 10
    }

    /// Whether the time inside the TID is more than [`MAX_REV_AHEAD_US`]
    /// past `now_us` (microseconds since the epoch; a negative clock reads
    /// as the epoch).
    pub fn is_ahead_of(self, now_us: i64) -> bool {
        let now = u64::try_from(now_us).unwrap_or(0);
        self.micros() > now.saturating_add(MAX_REV_AHEAD_US)
    }

    /// Encodes back to the 13-character string.
    pub fn encode(self) -> String {
        let mut out = [0u8; TID_LEN];
        let mut v = self.0 as u64;
        for slot in out.iter_mut().rev() {
            *slot = ALPHABET[(v & 31) as usize];
            v >>= 5;
        }
        // All bytes come from ALPHABET, which is ASCII.
        out.iter().map(|&b| b as char).collect()
    }
}

impl fmt::Display for Tid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.encode())
    }
}

impl FromStr for Tid {
    type Err = TidError;
    fn from_str(s: &str) -> Result<Tid, TidError> {
        Tid::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for s in [
            "3l3qo2vutsw2b",
            "2222222222222",
            "bzzzzzzzzzzzz",
            "3jzfcijpj2z2a",
        ] {
            let t = Tid::parse(s).unwrap();
            assert_eq!(t.encode(), s);
            assert!(t.as_i64() >= 0);
        }
        assert_eq!(Tid::parse("2222222222222").unwrap().as_i64(), 0);
        assert_eq!(Tid::parse("bzzzzzzzzzzzz").unwrap().as_i64(), i64::MAX);
    }

    #[test]
    fn ordering_matches_strings() {
        let a = Tid::parse("3l3qo2vutsw2b").unwrap();
        let b = Tid::parse("3l3qo2vutsw2c").unwrap();
        let c = Tid::parse("3l3qo2vuu2222").unwrap();
        assert!(a < b && b < c);
        assert!("3l3qo2vutsw2b" < "3l3qo2vutsw2c" && "3l3qo2vutsw2c" < "3l3qo2vuu2222");
    }

    #[test]
    fn parts() {
        let t = Tid::from_parts(1_700_000_000_000_000, 7).unwrap();
        assert_eq!(t.micros(), 1_700_000_000_000_000);
        assert_eq!(Tid::parse(&t.encode()).unwrap(), t);
        assert!(Tid::from_parts(1 << 53, 0).is_none());
        assert!(Tid::from_parts(0, 1024).is_none());
    }

    #[test]
    fn a_rev_is_ahead_only_past_the_allowance() {
        let now = 1_700_000_000_000_000u64;
        let at = |us: u64| Tid::from_parts(us, 0).unwrap();
        assert!(!at(0).is_ahead_of(now as i64));
        assert!(!at(now).is_ahead_of(now as i64));
        assert!(!at(now + MAX_REV_AHEAD_US).is_ahead_of(now as i64));
        assert!(at(now + MAX_REV_AHEAD_US + 1).is_ahead_of(now as i64));
        assert!(at((1 << 53) - 1).is_ahead_of(now as i64));
        // A clock before the epoch reads as the epoch.
        assert!(!at(MAX_REV_AHEAD_US).is_ahead_of(-5));
        assert!(at(MAX_REV_AHEAD_US + 1).is_ahead_of(i64::MIN));
        assert!(!at((1 << 53) - 1).is_ahead_of(i64::MAX));
    }

    #[test]
    fn rejects_non_tids() {
        for bad in [
            "",
            "3l3qo2vutsw2",
            "3l3qo2vutsw2bb",
            // top bit set
            "czzzzzzzzzzzz",
            "jzzzzzzzzzzzz",
            "zzzzzzzzzzzzz",
            // outside the alphabet
            "3l3qo2vutsw21",
            "3l3qo2vutsw28",
            "3L3QO2VUTSW2B",
            "3l3qo2vutsw2-",
            // not a TID at all
            "self",
            "bafyreib2rxk3rh6kzwq",
        ] {
            assert!(Tid::parse(bad).is_err(), "accepted {bad:?}");
        }
        assert!(Tid::from_i64(-1).is_none());
    }

    proptest::proptest! {
        #[test]
        fn encode_parse_round_trip(v in 0i64..=i64::MAX) {
            let t = Tid::from_i64(v).unwrap();
            let s = t.encode();
            proptest::prop_assert_eq!(s.len(), TID_LEN);
            proptest::prop_assert_eq!(Tid::parse(&s).unwrap(), t);
        }

        #[test]
        fn string_order_is_numeric_order(a in 0i64..=i64::MAX, b in 0i64..=i64::MAX) {
            let (ta, tb) = (Tid::from_i64(a).unwrap(), Tid::from_i64(b).unwrap());
            proptest::prop_assert_eq!(ta.encode().cmp(&tb.encode()), a.cmp(&b));
        }

        /// Any string at all: parsing returns, and what it accepts is 13
        /// characters that encode back to the input and decode to a value
        /// that is not negative.
        #[test]
        fn parsing_is_total_and_accepted_tids_round_trip(
            s in proptest::prop_oneof![
                proptest::prelude::any::<String>(),
                "[2-7a-z]{12,14}",
                "[0-9a-zA-Z]{13}",
            ],
        ) {
            let parsed = Tid::parse(&s);
            let well_formed = s.len() == TID_LEN
                && s.bytes().all(|b| ALPHABET.contains(&b))
                && b"234567ab".contains(&s.as_bytes()[0]);
            proptest::prop_assert_eq!(parsed.is_ok(), well_formed);
            if let Ok(t) = parsed {
                proptest::prop_assert!(t.as_i64() >= 0);
                proptest::prop_assert_eq!(t.encode(), s.clone());
                proptest::prop_assert_eq!(t.to_string(), s.clone());
                proptest::prop_assert_eq!(s.parse::<Tid>(), Ok(t));
            }
        }

        /// A time and a clock id make a TID exactly when both are in
        /// range, and the time reads back.
        #[test]
        fn parts_in_range_read_back(
            micros in proptest::prop_oneof![0u64..(1 << 53), proptest::prelude::any::<u64>()],
            clock in proptest::prop_oneof![0u16..1024, proptest::prelude::any::<u16>()],
        ) {
            let t = Tid::from_parts(micros, clock);
            proptest::prop_assert_eq!(t.is_some(), micros < (1 << 53) && clock < 1024);
            if let Some(t) = t {
                proptest::prop_assert_eq!(t.micros(), micros);
                proptest::prop_assert_eq!(t.as_i64() & 1023, i64::from(clock));
                proptest::prop_assert_eq!(Tid::parse(&t.encode()), Ok(t));
            }
        }
    }
}
