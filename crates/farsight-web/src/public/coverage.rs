//! Coverage in plain English (design §3.7). Coverage is a property of a
//! response, and a page is built from several: each section prints the
//! line for the `freshness` it was built from, and the page's panel states
//! the lowest level among the sections shown, never a higher one.

use askama::Template;
use serde_json::Value;

use super::text::{Stamp, thousands};

/// A coverage level as the pages word it. Any value the API may add later
/// is treated as `partial` (§12.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// `partial`, and every unknown value.
    Partial,
    /// `assisted`.
    Assisted,
    /// `complete`.
    Complete,
}

impl Level {
    /// From the API's `coverage.level`.
    pub fn parse(s: &str) -> Level {
        match s {
            "complete" => Level::Complete,
            "assisted" => Level::Assisted,
            _ => Level::Partial,
        }
    }

    /// The sentence a section prints for the level.
    pub fn words(self) -> &'static str {
        match self {
            Level::Complete => "Complete.",
            Level::Assisted => {
                "Best effort. This instance has not finished indexing the whole network; \
                 it used a backlink index to find records naming this account."
            }
            Level::Partial => "Partial.",
        }
    }

    /// CSS class.
    pub fn class(self) -> &'static str {
        match self {
            Level::Complete => "complete",
            Level::Assisted => "assisted",
            Level::Partial => "partial",
        }
    }
}

/// The reason codes the pages know, in the order they are listed, each
/// with its phrase. Codes that share a phrase share an anchor on About.
pub const REASONS: [(&str, &str, &str); 15] = [
    (
        "sync_events_unavailable",
        "sync_events_unavailable",
        "the event stream in use does not report repository resyncs",
    ),
    (
        "storage_refusal",
        "storage_refusal",
        "the storage limit was reached",
    ),
    (
        "firehose_disconnected",
        "firehose_disconnected",
        "disconnected from the event stream",
    ),
    (
        "firehose_lagging",
        "firehose_lagging",
        "behind the event stream",
    ),
    (
        "firehose_gap",
        "firehose_gap",
        "a gap in the event stream is being repaired",
    ),
    (
        "sweep_incomplete",
        "sweep_incomplete",
        "the first full pass over the network is not finished",
    ),
    (
        "list_pending",
        "list_pending",
        "some lists are still being indexed",
    ),
    (
        "list_pending_historical",
        "list_pending",
        "some lists are still being indexed",
    ),
    (
        "list_capped",
        "list_capped",
        "a list is stored only in part",
    ),
    (
        "list_unavailable",
        "list_unavailable",
        "a list could not be indexed",
    ),
    (
        "list_missing",
        "list_unavailable",
        "a list could not be indexed",
    ),
    (
        "list_deferred",
        "list_unavailable",
        "a list could not be indexed",
    ),
    (
        "list_not_tracked",
        "list_not_tracked",
        "the list is not indexed because nothing blocks it",
    ),
    (
        "discovery_truncated",
        "discovery_truncated",
        "the backlink search was cut short",
    ),
    (
        "party_debt",
        "party_debt",
        "this account's own records need re-reading",
    ),
];

/// One reason as a section prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reason {
    /// Fragment on the About page.
    pub anchor: String,
    /// Phrase.
    pub phrase: String,
}

/// Reasons present, in the table's order, one entry per phrase; values
/// the pages do not know come last, each named.
pub fn reasons(codes: &[String]) -> Vec<Reason> {
    let mut out: Vec<Reason> = Vec::new();
    for (code, anchor, phrase) in REASONS {
        if codes.iter().any(|c| c == code) && !out.iter().any(|r| r.phrase == phrase) {
            out.push(Reason {
                anchor: format!("reason-{anchor}"),
                phrase: phrase.to_owned(),
            });
        }
    }
    let mut unknown: Vec<&String> = codes
        .iter()
        .filter(|c| !REASONS.iter().any(|(k, _, _)| k == c))
        .collect();
    unknown.sort();
    unknown.dedup();
    for c in unknown {
        out.push(Reason {
            anchor: "reason-other".to_owned(),
            phrase: format!("another limitation ({})", super::text::clean(c)),
        });
    }
    out
}

/// What one `freshness` object says, before it is worded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    /// Level.
    pub level: Level,
    /// Reason codes.
    pub reasons: Vec<String>,
    /// `indexedAt`.
    pub indexed_at: Option<String>,
    /// `asOf`.
    pub as_of: Option<String>,
    /// `coverage.completeSince`.
    pub complete_since: Option<String>,
}

impl Facts {
    /// Reads a `freshness` object.
    pub fn read(f: &Value) -> Facts {
        let c = &f["coverage"];
        let s = |v: &Value| v.as_str().map(str::to_owned);
        Facts {
            level: Level::parse(c["level"].as_str().unwrap_or("")),
            reasons: c["reasons"]
                .as_array()
                .map(|a| a.iter().filter_map(&s).collect())
                .unwrap_or_default(),
            indexed_at: s(&f["indexedAt"]),
            as_of: s(&f["asOf"]),
            complete_since: s(&c["completeSince"]),
        }
    }
}

/// The coverage line under a section heading.
#[derive(Debug, Clone, Template)]
#[template(path = "_coverage_line.html")]
pub struct CoverageLine {
    /// CSS class of the level.
    pub class: &'static str,
    /// The level in words.
    pub words: &'static str,
    /// Reasons, listed at every level: list scope returns `complete` with
    /// a reason for lists it cannot show.
    pub reasons: Vec<Reason>,
    /// "Reflects what this instance saw up to …"; left out when the
    /// `freshness` has no `indexedAt` (before the first ingest batch).
    pub indexed: Option<Stamp>,
    /// "Page built …".
    pub built: Option<Stamp>,
    /// "This has held since …" (`complete` and `assisted` only).
    pub since: Option<Stamp>,
}

impl CoverageLine {
    /// The line for a section built from `freshness`.
    pub fn of(f: &Facts) -> CoverageLine {
        CoverageLine {
            class: f.level.class(),
            words: f.level.words(),
            reasons: reasons(&f.reasons),
            indexed: f.indexed_at.as_deref().and_then(Stamp::parse),
            built: f.as_of.as_deref().and_then(Stamp::parse),
            since: if f.level == Level::Partial {
                None
            } else {
                f.complete_since.as_deref().and_then(Stamp::parse)
            },
        }
    }
}

/// The summary for a page: the lowest level among the sections shown, the
/// earliest `indexedAt`, and the union of their reasons. A page with no
/// section has none.
pub fn summary(sections: &[&Facts]) -> Option<Facts> {
    let level = sections.iter().map(|f| f.level).min()?;
    let mut reasons: Vec<String> = Vec::new();
    for f in sections {
        for r in &f.reasons {
            if !reasons.contains(r) {
                reasons.push(r.clone());
            }
        }
    }
    // RFC 3339 strings in UTC with a fixed layout compare as times, but
    // the comparison is made on parsed values to be safe.
    let indexed_at = sections
        .iter()
        .filter_map(|f| f.indexed_at.as_deref())
        .filter_map(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|t| (t, s.to_owned()))
        })
        .min_by_key(|(t, _)| *t)
        .map(|(_, s)| s);
    Some(Facts {
        level,
        reasons,
        indexed_at,
        as_of: None,
        complete_since: None,
    })
}

/// The instance-wide exception counters in plain words, in the lexicon's
/// order: (key, singular, plural).
pub const EXCEPTIONS: [(&str, &str, &str); 9] = [
    (
        "unreachableRepos",
        "account whose repository could not be read",
        "accounts whose repositories could not be read",
    ),
    (
        "pendingResyncs",
        "account waiting to be read again",
        "accounts waiting to be read again",
    ),
    (
        "cappedAuthors",
        "account with more records than this instance stores for one account",
        "accounts with more records than this instance stores for one account",
    ),
    (
        "refusedAuthors",
        "account with records refused at a storage limit",
        "accounts with records refused at a storage limit",
    ),
    (
        "unavailableLists",
        "list that could not be read from its owner's server",
        "lists that could not be read from their owners' servers",
    ),
    (
        "missingLists",
        "list whose record has not been found",
        "lists whose record has not been found",
    ),
    (
        "deferredLists",
        "list not indexed because of a storage limit",
        "lists not indexed because of a storage limit",
    ),
    (
        "cappedLists",
        "list stored only in part",
        "lists stored only in part",
    ),
    (
        "excludedPendingLists",
        "list still being indexed and left out for now",
        "lists still being indexed and left out for now",
    ),
];

/// One non-zero exception count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exception {
    /// Fragment on the About page.
    pub anchor: String,
    /// "3 lists stored only in part".
    pub text: String,
}

/// The non-zero `coverage.exceptions` of a `freshness`, plus
/// `pendingLists`. They are instance-wide: the same in every section.
pub fn exceptions(f: &Value) -> Vec<Exception> {
    let c = &f["coverage"];
    let mut out = Vec::new();
    for (key, one, many) in EXCEPTIONS {
        let n = c["exceptions"][key].as_i64().unwrap_or(0);
        if n > 0 {
            out.push(Exception {
                anchor: format!("exception-{key}"),
                text: format!("{} {}", thousands(n), if n == 1 { one } else { many }),
            });
        }
    }
    let p = c["pendingLists"].as_i64().unwrap_or(0);
    if p > 0 {
        out.push(Exception {
            anchor: "exception-pendingLists".to_owned(),
            text: format!(
                "{} {}",
                thousands(p),
                if p == 1 {
                    "list being indexed"
                } else {
                    "lists being indexed"
                }
            ),
        });
    }
    out
}

/// One section's raw `freshness`, as the API returns it for the same
/// query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Raw {
    /// Section title.
    pub title: &'static str,
    /// Pretty-printed JSON (escaped by the template).
    pub json: String,
}

/// The page's coverage panel (`#coverage`).
#[derive(Debug, Clone, Template)]
#[template(path = "_freshness_panel.html")]
pub struct CoveragePanel {
    /// The page summary line.
    pub summary: Option<CoverageLine>,
    /// Non-zero instance-wide exception counts.
    pub exceptions: Vec<Exception>,
    /// The operator withholds some accounts.
    pub operator_excludes: bool,
    /// Raw `freshness` per section.
    pub raw: Vec<Raw>,
}

impl CoveragePanel {
    /// The panel for sections built from these `freshness` objects, in
    /// page order.
    pub fn of(sections: &[(&'static str, &Value)], operator_excludes: bool) -> CoveragePanel {
        let facts: Vec<Facts> = sections.iter().map(|(_, f)| Facts::read(f)).collect();
        let refs: Vec<&Facts> = facts.iter().collect();
        CoveragePanel {
            summary: summary(&refs).map(|f| CoverageLine::of(&f)),
            exceptions: sections
                .first()
                .map(|(_, f)| exceptions(f))
                .unwrap_or_default(),
            operator_excludes,
            raw: sections
                .iter()
                .map(|(title, f)| Raw {
                    title,
                    json: serde_json::to_string_pretty(f).unwrap_or_default(),
                })
                .collect(),
        }
    }
}

/// What an empty live section prints: its wording depends on the level,
/// so that "none" is never read as more than the coverage supports.
pub fn empty_words(level: Level) -> &'static str {
    if level == Level::Complete {
        "None found."
    } else {
        "None found so far — coverage is not complete."
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fresh(level: &str, reasons: &[&str], indexed: Option<&str>) -> Value {
        let mut f = json!({
            "asOf": "2026-10-02T03:00:00.000000Z",
            "firehoseConnected": true,
            "coverage": {
                "level": level, "reasons": reasons,
                "completeSince": "2026-09-01T00:00:00.000000Z",
                "exceptions": {"cappedLists": 1, "missingLists": 0, "unreachableRepos": 1200},
                "pendingLists": 2,
            },
        });
        if let Some(i) = indexed {
            f["indexedAt"] = json!(i);
        }
        f
    }

    #[test]
    fn levels_and_unknown_values() {
        assert_eq!(Level::parse("complete"), Level::Complete);
        assert_eq!(Level::parse("assisted"), Level::Assisted);
        assert_eq!(Level::parse("partial"), Level::Partial);
        // An unknown level is partial (§12.2).
        assert_eq!(Level::parse("mostly"), Level::Partial);
        assert!(Level::Partial < Level::Assisted && Level::Assisted < Level::Complete);
    }

    #[test]
    fn reasons_in_table_order_one_per_phrase() {
        let r = reasons(&[
            "party_debt".into(),
            "list_pending_historical".into(),
            "list_pending".into(),
            "sweep_incomplete".into(),
            "brand_new".into(),
        ]);
        let phrases: Vec<&str> = r.iter().map(|r| r.phrase.as_str()).collect();
        assert_eq!(
            phrases,
            [
                "the first full pass over the network is not finished",
                "some lists are still being indexed",
                "this account's own records need re-reading",
                "another limitation (brand_new)",
            ]
        );
        assert_eq!(r[1].anchor, "reason-list_pending");
        assert_eq!(r[3].anchor, "reason-other");
    }

    #[test]
    fn every_lexicon_reason_has_a_phrase() {
        for code in [
            "sweep_incomplete",
            "firehose_gap",
            "firehose_disconnected",
            "firehose_lagging",
            "sync_events_unavailable",
            "storage_refusal",
            "list_pending",
            "list_pending_historical",
            "list_capped",
            "list_unavailable",
            "list_missing",
            "list_deferred",
            "list_not_tracked",
            "discovery_truncated",
            "party_debt",
        ] {
            let r = reasons(&[code.to_owned()]);
            assert_eq!(r.len(), 1, "{code}");
            assert!(!r[0].phrase.starts_with("another"), "{code}");
        }
    }

    #[test]
    fn line_keeps_reasons_at_complete_and_drops_absent_times() {
        let f = fresh("complete", &["list_capped"], None);
        let l = CoverageLine::of(&Facts::read(&f));
        assert_eq!(l.words, "Complete.");
        assert_eq!(l.reasons.len(), 1);
        assert!(l.indexed.is_none() && l.built.is_some() && l.since.is_some());
        let html = l.render().unwrap();
        assert!(html.contains("a list is stored only in part"));
        assert!(!html.contains("Reflects what this instance saw"));
        assert!(html.contains("This has held since"));
        // Partial never says since when it has held.
        let f = fresh(
            "partial",
            &["sweep_incomplete"],
            Some("2026-10-02T02:59:00Z"),
        );
        let l = CoverageLine::of(&Facts::read(&f));
        assert!(l.since.is_none() && l.indexed.is_some());
        assert!(!l.render().unwrap().contains("ago"));
    }

    #[test]
    fn summary_is_the_lowest_level_and_earliest_time() {
        let a = Facts::read(&fresh("complete", &[], Some("2026-10-02T02:59:00Z")));
        let b = Facts::read(&fresh(
            "assisted",
            &["sweep_incomplete"],
            Some("2026-10-02T02:58:00Z"),
        ));
        let c = Facts::read(&fresh(
            "partial",
            &["party_debt"],
            Some("2026-10-02T02:59:30Z"),
        ));
        let s = summary(&[&a, &b]).unwrap();
        assert_eq!(s.level, Level::Assisted);
        assert_eq!(s.indexed_at.as_deref(), Some("2026-10-02T02:58:00Z"));
        let s = summary(&[&a, &b, &c]).unwrap();
        assert_eq!(s.level, Level::Partial);
        assert_eq!(s.reasons, ["sweep_incomplete", "party_debt"]);
        assert_eq!(summary(&[]), None);
        assert_eq!(summary(&[&a]).unwrap().level, Level::Complete);
    }

    #[test]
    fn exceptions_only_when_non_zero() {
        let e = exceptions(&fresh("complete", &[], None));
        let texts: Vec<&str> = e.iter().map(|e| e.text.as_str()).collect();
        assert_eq!(
            texts,
            [
                "1,200 accounts whose repositories could not be read",
                "1 list stored only in part",
                "2 lists being indexed",
            ]
        );
    }

    #[test]
    fn panel_renders_both_notices() {
        let f = fresh(
            "partial",
            &["sweep_incomplete"],
            Some("2026-10-02T02:59:00Z"),
        );
        let p = CoveragePanel::of(&[("Blocked by", &f)], true);
        let html = p.render().unwrap();
        assert!(html.contains("Accounts that are not active are not shown."));
        assert!(html.contains("operator has chosen not to show some accounts"));
        assert!(html.contains("&quot;level&quot;") || html.contains("&#34;level&#34;"));
        let p = CoveragePanel::of(&[("Blocked by", &f)], false);
        assert!(!p.render().unwrap().contains("operator has chosen"));
    }

    #[test]
    fn empty_wording_follows_the_level() {
        assert_eq!(empty_words(Level::Complete), "None found.");
        assert!(empty_words(Level::Assisted).contains("not complete"));
        assert!(empty_words(Level::Partial).contains("not complete"));
    }
}
