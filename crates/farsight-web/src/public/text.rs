//! Text helpers of the public pages: untrusted text, timestamps and links.

use chrono::{DateTime, SecondsFormat, Utc};

/// Removes control characters and bidirectional-override characters from
/// text that comes from records anyone can write (list names, handles) or
/// from the operator's free text. The result is still escaped by the
/// template; this only keeps a name from reordering or hiding the text
/// around it.
pub fn clean(s: &str) -> String {
    s.chars()
        .filter(|c| {
            !c.is_control()
                && !matches!(
                    c,
                    '\u{200E}' | '\u{200F}' | '\u{061C}' | '\u{202A}'
                        ..='\u{202E}' | '\u{2066}'
                        ..='\u{2069}'
                )
        })
        .collect()
}

/// Paragraphs of operator text: blank lines separate them; each is
/// [`clean`]ed and its line breaks become spaces.
pub fn paragraphs(s: &str) -> Vec<String> {
    s.replace("\r\n", "\n")
        .split("\n\n")
        .map(|p| clean(&p.split('\n').collect::<Vec<_>>().join(" ")))
        .map(|p| p.trim().to_owned())
        .filter(|p| !p.is_empty())
        .collect()
}

/// An absolute UTC time for a `<time>` element. The server never renders
/// a relative time: a cached copy would keep saying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamp {
    /// `datetime` attribute (RFC 3339, UTC).
    pub iso: String,
    /// Visible text.
    pub text: String,
}

impl Stamp {
    /// From a time.
    pub fn of(t: DateTime<Utc>) -> Stamp {
        Stamp {
            iso: t.to_rfc3339_opts(SecondsFormat::Secs, true),
            text: t.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        }
    }

    /// From an RFC 3339 string, as the API renders times.
    pub fn parse(s: &str) -> Option<Stamp> {
        DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| Stamp::of(t.with_timezone(&Utc)))
    }
}

/// Percent-encodes one path segment: everything outside the unreserved
/// set and `:` is escaped, so `%` in a did:web port becomes `%25`.
pub fn seg(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b':' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `/public/did/{did}`.
pub fn did_href(did: &str) -> String {
    format!("/public/did/{}", seg(did))
}

/// `/public/list/{did}/{rkey}`.
pub fn list_href(owner: &str, rkey: &str) -> String {
    format!("/public/list/{}/{}", seg(owner), seg(rkey))
}

/// The at-uri of a list.
pub fn list_uri(owner: &str, rkey: &str) -> String {
    format!("at://{owner}/app.bsky.graph.list/{rkey}")
}

/// `1,234`.
pub fn thousands(n: i64) -> String {
    let digits = n.abs().to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

/// A duration in the words the pages use for the retention: whole days,
/// hours or minutes.
pub fn duration_words(d: std::time::Duration) -> String {
    let s = d.as_secs();
    let plural = |n: u64, unit: &str| {
        if n == 1 {
            format!("1 {unit}")
        } else {
            format!("{} {unit}s", thousands(n as i64))
        }
    };
    if s >= 86_400 && s % 86_400 == 0 {
        plural(s / 86_400, "day")
    } else if s >= 3_600 && s % 3_600 == 0 {
        plural(s / 3_600, "hour")
    } else if s >= 60 && s % 60 == 0 {
        plural(s / 60, "minute")
    } else {
        plural(s, "second")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn strips_controls_and_bidi() {
        assert_eq!(clean("a\u{202E}b\u{0007}c\u{2066}d"), "abcd");
        assert_eq!(clean("<b>x</b>"), "<b>x</b>");
        assert_eq!(clean("naïve — ok"), "naïve — ok");
    }

    #[test]
    fn splits_paragraphs() {
        assert_eq!(
            paragraphs("one\nline\r\n\r\ntwo\n\n\n\n"),
            ["one line", "two"]
        );
        assert!(paragraphs("  \n\n ").is_empty());
    }

    #[test]
    fn stamps_are_absolute_utc() {
        let t = Utc.with_ymd_and_hms(2026, 10, 2, 3, 4, 5).unwrap();
        let s = Stamp::of(t);
        assert_eq!(s.iso, "2026-10-02T03:04:05Z");
        assert_eq!(s.text, "2026-10-02 03:04:05 UTC");
        assert_eq!(
            Stamp::parse("2026-10-02T05:04:05.123456+02:00")
                .unwrap()
                .text,
            "2026-10-02 03:04:05 UTC"
        );
        assert_eq!(Stamp::parse("yesterday"), None);
    }

    #[test]
    fn segments() {
        assert_eq!(
            did_href("did:web:example.com%3A8080"),
            "/public/did/did:web:example.com%253A8080"
        );
        assert_eq!(
            list_href("did:plc:abc", "a/b c"),
            "/public/list/did:plc:abc/a%2Fb%20c"
        );
    }

    #[test]
    fn numbers_and_durations() {
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(12), "12");
        assert_eq!(thousands(1_234_567), "1,234,567");
        let d = std::time::Duration::from_secs;
        assert_eq!(duration_words(d(365 * 86_400)), "365 days");
        assert_eq!(duration_words(d(86_400)), "1 day");
        assert_eq!(duration_words(d(7_200)), "2 hours");
        assert_eq!(duration_words(d(90)), "90 seconds");
    }
}
