//! The Farsight web UI (design §8): server-rendered HTML (askama), one CSS
//! file, vendored htmx, no build step, no CDN. Setup mode serves the
//! first-run wizard; normal mode serves the admin sign-in, the dashboard, lookups, operations,
//! settings, reset and the admin history pages, and — when the operator
//! turns it on — the public UI at the root.

#![warn(missing_docs)]
// Handlers return early with a ready `Response` as the error value; boxing
// it would only add noise at every call site.
#![allow(clippy::result_large_err)]

pub mod cells;
pub mod common;
pub mod enter;
pub mod history;
pub mod oauth;
pub mod pages;
pub mod public;
pub mod public_settings;
pub mod rows;
pub mod setup;
pub mod setup_token;

pub use pages::{ServerStatus, WebState};
pub use setup::SetupState;
