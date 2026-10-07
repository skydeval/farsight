//! Layout shared by the admin pages: the header's navigation and the
//! plain message page.

use askama::Template;
use axum::http::StatusCode;
use axum::response::Response;

use super::Admin;
use crate::common::render_private;

/// Navigation shown in the page header.
#[derive(Debug, Clone, Default)]
pub struct Nav {
    /// Logged in. Without a session (the sign-in pages) the header has
    /// the brand and nothing else.
    pub admin: bool,
    /// The session's form token for the header's logout form; empty
    /// without a session.
    pub csrf: String,
}

pub(crate) fn nav(session: &Option<Admin>) -> Nav {
    Nav {
        admin: session.is_some(),
        csrf: session.as_ref().map(|s| s.csrf.clone()).unwrap_or_default(),
    }
}

/// A plain message page.
#[derive(Template)]
#[template(path = "message.html")]
pub struct MessagePage {
    /// Navigation.
    pub nav: Nav,
    /// The page's heading.
    pub title: String,
    /// The one paragraph under it, as plain text.
    pub message: String,
    /// Optional link (relative).
    pub link: Option<(String, String)>,
}

pub(crate) fn message(s: &Option<Admin>, status: StatusCode, title: &str, msg: &str) -> Response {
    let mut r = render_private(&MessagePage {
        nav: nav(s),
        title: title.into(),
        message: msg.into(),
        link: None,
    });
    *r.status_mut() = status;
    r
}
