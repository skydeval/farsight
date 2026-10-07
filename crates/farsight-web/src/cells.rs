//! The account cell of the admin tables (see `docs/design/web-ui.md`), on
//! the lookup and history pages.
//!
//! An account cell renders as on the public pages — `@handle` when the
//! handle cache holds a verified one, the DID otherwise — as a link to the
//! DID lookup that also opens a profile card. The pages these cells are
//! on need a session, so every cell carries the card.

use askama::Template;

use crate::pages::WebState;
use crate::public::text::{clean, seg};
use crate::public::warming::Asked;

/// `/admin/lookup/did?q=…`.
pub fn lookup_did_href(did: &str) -> String {
    let q: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("q", did)
        .finish();
    format!("/admin/lookup/did?{q}")
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
    /// Its profile-card fragment.
    pub card: String,
    /// A verified handle, if cached.
    pub handle: Option<String>,
}

/// The cell for `did`. Never fetches anything and reads the memory cache
/// alone (the page has read the stored handles of its accounts into it):
/// an account shown as a DID is handed to the warming worker.
pub fn account(st: &WebState, asked: &mut Asked, did: &str) -> Account {
    Account {
        did: did.to_owned(),
        href: lookup_did_href(did),
        card: admin_card_href(did),
        handle: asked.handle(st, did).map(|h| clean(&h)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_account_cell() {
        let a = |handle: Option<&str>| {
            let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
            Account {
                did: did.into(),
                href: lookup_did_href(did),
                card: admin_card_href(did),
                handle: handle.map(str::to_owned),
            }
            .render()
            .unwrap()
        };
        let bare = a(None);
        assert!(bare.contains("href=\"/admin/lookup/did?q=did%3Aplc%3Aaaaaaaaaaaaaaaaaaaaaaaaa\""));
        assert!(bare.contains("title=\"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa\""));
        assert!(bare.contains("<code>did:plc:aaaaaaaaaaaaaaaaaaaaaaaa</code>"));
        let full = a(Some("alice.example"));
        assert!(full.contains("data-card=\"/admin/card/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa\""));
        assert!(full.contains("data-card-session"));
        assert!(full.contains(">alice.example</a>"));
        // The script names no admin path: the page carries it.
        assert!(!crate::public::PUBLIC_JS.contains("/admin/"));
    }
}
