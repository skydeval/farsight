//! Cells of the admin tables (design §8.6): accounts, records and times
//! on the lookup and history pages.
//!
//! An account cell renders as on the public pages — `@handle` when the
//! handle cache holds a verified one, the DID otherwise — as a link to the
//! DID lookup. For a **signed-in admin** the link also opens a profile
//! card. The lookup pages are served without a session under
//! `ui = public_read`; such a viewer gets the link, the handle and the
//! `title`, and no card, and never the "First seen" column.

use askama::Template;
use chrono::{DateTime, Utc};

use crate::pages::WebState;
use crate::public::text::{Record, Stamp, clean, seg};
use crate::public::warming::Asked;

/// `/lookup/did?q=…`.
pub fn lookup_did_href(did: &str) -> String {
    let q: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("q", did)
        .finish();
    format!("/lookup/did?{q}")
}

/// `/admin/card/{did}`: the profile-card fragment for a signed-in admin.
pub fn admin_card_href(did: &str) -> String {
    format!("/admin/card/{}", seg(did))
}

/// An account as an admin table names it.
#[derive(Debug, Clone, Template)]
#[template(path = "_admin_who.html")]
pub struct Account {
    /// The DID.
    pub did: String,
    /// Its lookup page.
    pub href: String,
    /// Its profile-card fragment; only for a signed-in admin.
    pub card: Option<String>,
    /// A verified handle, if cached.
    pub handle: Option<String>,
}

/// The cell for `did`. Never fetches anything: an account shown as a DID
/// is handed to the warming worker.
pub fn account(st: &WebState, asked: &mut Asked, signed_in: bool, did: &str) -> Account {
    Account {
        did: did.to_owned(),
        href: lookup_did_href(did),
        card: signed_in.then(|| admin_card_href(did)),
        handle: asked.handle(st, did).map(|h| clean(&h)),
    }
}

#[derive(Template)]
#[template(source = "{{ text }}", ext = "html")]
struct TextCell<'a> {
    text: &'a str,
}

#[derive(Template)]
#[template(
    source = r#"{% if let Some(t) = t %}<time datetime="{{ t.iso }}">{{ t.text }}</time>{% endif %}"#,
    ext = "html"
)]
struct TimeCell {
    t: Option<Stamp>,
}

/// What the "First seen" cell says for a row stored before the date was
/// kept.
pub const NO_FIRST_SEEN: &str = "Stored before Farsight kept this date";

#[derive(Template)]
#[template(
    source = r#"{% if let Some(t) = t %}<time datetime="{{ t.iso }}">{{ t.text }}</time>{% else %}<span class="muted" title="{{ none }}">—</span>{% endif %}"#,
    ext = "html"
)]
struct FirstSeenCell {
    t: Option<Stamp>,
    none: &'static str,
}

/// One table cell, rendered and escaped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Cell {
    /// Its markup.
    pub html: String,
}

impl Cell {
    fn of<T: Template>(t: &T) -> Cell {
        Cell {
            html: t.render().unwrap_or_default(),
        }
    }

    /// Plain text.
    pub fn text(text: &str) -> Cell {
        Cell::of(&TextCell { text })
    }

    /// An account.
    pub fn account(a: &Account) -> Cell {
        Cell::of(a)
    }

    /// A record: its at-uri, a link when a viewer is configured.
    pub fn record(r: &Record) -> Cell {
        Cell::of(r)
    }

    /// A time; empty when the record states none.
    pub fn time(t: Option<DateTime<Utc>>) -> Cell {
        Cell::of(&TimeCell {
            t: t.map(Stamp::of),
        })
    }

    /// When Farsight first stored the row.
    pub fn first_seen(t: Option<DateTime<Utc>>) -> Cell {
        Cell::of(&FirstSeenCell {
            t: t.map(Stamp::of),
            none: NO_FIRST_SEEN,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn cells_escape_and_mark_up() {
        assert_eq!(Cell::text("<b>&").html, "&lt;b&gt;&amp;");
        let t = Utc.with_ymd_and_hms(2026, 10, 3, 4, 5, 6).unwrap();
        assert_eq!(
            Cell::time(Some(t)).html,
            "<time datetime=\"2026-10-03T04:05:06Z\">2026-10-03 04:05:06 UTC</time>"
        );
        assert_eq!(Cell::time(None).html, "");
        assert!(Cell::first_seen(Some(t)).html.starts_with("<time "));
        let none = Cell::first_seen(None).html;
        assert!(none.contains("—") && none.contains(NO_FIRST_SEEN));
    }

    #[test]
    fn a_card_only_for_a_signed_in_admin() {
        let a = |card: bool, handle: Option<&str>| {
            let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
            Cell::account(&Account {
                did: did.into(),
                href: lookup_did_href(did),
                card: card.then(|| admin_card_href(did)),
                handle: handle.map(str::to_owned),
            })
            .html
        };
        let anon = a(false, None);
        assert!(anon.contains("href=\"/lookup/did?q=did%3Aplc%3Aaaaaaaaaaaaaaaaaaaaaaaaa\""));
        assert!(anon.contains("title=\"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa\""));
        assert!(anon.contains("<code>did:plc:aaaaaaaaaaaaaaaaaaaaaaaa</code>"));
        assert!(!anon.contains("data-card"), "{anon}");
        let admin = a(true, Some("alice.example"));
        assert!(admin.contains("data-card=\"/admin/card/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa\""));
        assert!(admin.contains("data-card-session"));
        assert!(admin.contains(">@alice.example</a>"));
        // The script names no admin path: the page carries it.
        assert!(!crate::public::PUBLIC_JS.contains("/admin/"));
    }
}
