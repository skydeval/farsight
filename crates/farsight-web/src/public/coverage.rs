//! What a public page says about coverage (see `docs/design/coverage.md`
//! and `docs/design/web-ui.md`).
//!
//! A public page prints no coverage level, so it claims none. What it
//! keeps are the three places where silence would turn into a claim: an
//! empty section says what this instance holds and nothing about the
//! network ([`EMPTY`]), a list that is not indexed says so instead of
//! showing no members, and a withheld subject gets its notice. Each data
//! page ends with one "Last updated" line: the earliest `indexedAt` among
//! the `freshness` objects of the sections it rendered. The dashboard and
//! the lookup pages state the rest.

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::text::Stamp;

/// The one string of an empty section, at every coverage level.
pub const EMPTY: &str = "None on record at this instance.";

/// `indexedAt` of a `freshness` object.
pub fn indexed_at(freshness: &Value) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(freshness["indexedAt"].as_str()?)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// The time of the "Last updated" line: the earliest `indexedAt` among
/// the sections a page rendered. `None` (the line is left out) when no
/// rendered section has one, as before the first ingest batch.
pub fn last_updated(sections: &[&Value]) -> Option<Stamp> {
    sections
        .iter()
        .filter_map(|f| indexed_at(f))
        .min()
        .map(Stamp::of)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_earliest_section_dates_the_page() {
        let a = json!({"indexedAt": "2026-10-02T05:10:22.123Z", "coverage": {"level": "partial"}});
        let b = json!({"indexedAt": "2026-10-02T05:09:00Z"});
        let none = json!({"coverage": {"level": "complete"}});
        assert_eq!(
            last_updated(&[&a, &b, &none]).unwrap().iso,
            "2026-10-02T05:09:00Z"
        );
        assert_eq!(last_updated(&[&a]).unwrap().text, "2026-10-02 05:10:22 UTC");
        assert_eq!(last_updated(&[&none]), None);
        assert_eq!(last_updated(&[]), None);
    }
}
