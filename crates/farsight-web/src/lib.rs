//! The Farsight web UI (design §8): server-rendered HTML (askama), one CSS
//! file, vendored htmx, no build step, no CDN. Setup mode serves the
//! first-run wizard; normal mode serves the dashboard, lookups, operations,
//! settings and reset.

#![warn(missing_docs)]
// Handlers return early with a ready `Response` as the error value; boxing
// it would only add noise at every call site.
#![allow(clippy::result_large_err)]

pub mod common;
pub mod pages;
pub mod setup;
pub mod setup_token;

pub use pages::{ServerStatus, WebState};
pub use setup::SetupState;
