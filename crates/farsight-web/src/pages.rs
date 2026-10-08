//! Normal-mode pages (see `docs/design/web-ui.md`): admin sessions,
//! dashboard, lookups, operations, settings and reset, with their
//! access rules. This module holds the shared state and the router; each
//! surface is a submodule.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use farsight_api::ApiState;
use farsight_core::config::AdminAuth;
use farsight_core::net::SafeClient;
use farsight_storage::gates::GateState;
use farsight_storage::ui_rows::SortIndexes;
use tokio::sync::watch;

use crate::common;

mod admin_did;
mod admin_list;
mod dashboard;
mod layout;
mod lookup;
mod ops;
mod reset;
mod session;
mod settings;

pub(crate) use dashboard::count;
pub use dashboard::{
    AlertsFragment, ApiRow, DashboardData, DashboardError, DashboardFragment, DashboardPage, Stat,
    Warning, coverage_words, reason_words,
};
use dashboard::{alerts, dashboard, dashboard_fragment, stat};
pub use layout::{MessagePage, Nav};
pub(crate) use layout::{message, nav};
pub(crate) use lookup::permit;
pub use lookup::{Busy, HandleError, handle_to_did, parse_list_ref};
pub use ops::OpsPage;
use ops::{ops_action, ops_page};
pub use reset::{ResetError, ResetPage, perform_reset};
use reset::{reset_page, reset_submit};
use session::logout;
pub use session::{Admin, Return, oauth_session_key, session_key};
pub(crate) use session::{
    admin, admin_cookie_value, check_form, clear_admin_cookie, gate, metrics_rate_limited,
    set_admin_cookie, step_up,
};
pub use settings::SettingsPage;
pub(crate) use settings::{SIGNED_OUT, after_store, settings_base, token_changed};
use settings::{settings_page, settings_save, settings_token};

/// Name of the admin session cookie on a plain-HTTP origin (loopback
/// sign-in). Its value is looked up only as a hash bound to the admin DID
/// (`session::oauth_session_key`).
pub const ADMIN_COOKIE: &str = "farsight_admin";
/// Its name when the request arrived over HTTPS. A browser accepts a
/// `__Host-` cookie only with `Secure`, `Path=/` and no `Domain`, so no
/// other host of the same site can set or replace it.
pub const ADMIN_COOKIE_HOST: &str = "__Host-farsight_admin";
/// A session not used for this long is no longer accepted; every
/// accepted request starts the wait again.
pub const SESSION_IDLE: Duration = Duration::from_secs(12 * 3600);
/// A session this old is no longer accepted, however recently it was
/// used. Also how long an address stays exempt from the process-wide
/// sign-in bucket after a successful sign-in.
pub const SESSION_ABSOLUTE: Duration = Duration::from_secs(7 * 24 * 3600);
/// A sensitive action is carried out only in a session whose sign-in
/// completed at most this long ago; an older session is asked to sign in
/// again first. Sensitive: a settings change that touches `auth.*`,
/// `net.*`, `backfill.plc_url` or `backfill.relay_url` or that widens
/// `access.*` (`farsight_api::config_store::sensitive_changes`), rotating
/// the admin token, and creating an API key.
pub const STEP_UP_WINDOW: Duration = Duration::from_secs(600);

/// [`STEP_UP_WINDOW`] as the running process applies it.
#[cfg(feature = "harness")]
pub fn step_up_window() -> Duration {
    // Harness only: lets a browser probe watch a sign-in stop being
    // fresh without waiting ten minutes.
    std::env::var("FARSIGHT_HARNESS_STEP_UP_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map_or(STEP_UP_WINDOW, Duration::from_secs)
}

/// [`STEP_UP_WINDOW`] as the running process applies it.
#[cfg(not(feature = "harness"))]
pub fn step_up_window() -> Duration {
    STEP_UP_WINDOW
}

/// Status the server's background tasks publish for the dashboard.
#[derive(Debug, Default)]
pub struct ServerStatus {
    inner: RwLock<StatusInner>,
}

/// The published values. The budget monitor writes the sizes and the
/// gate every minute; the other fields have a writer each.
#[derive(Debug, Clone, Default)]
pub struct StatusInner {
    /// `pg_database_size` in bytes; `None` until the budget monitor's
    /// first pass.
    pub db_bytes: Option<u64>,
    /// `storage.budget_bytes` as it was at that pass; 0 before it.
    pub budget_bytes: u64,
    /// The hard ceiling in bytes that the gates use:
    /// `storage.hard_ceiling_bytes`, or 115% of the budget when that is 0.
    pub ceiling_bytes: u64,
    /// What the monitor decided at that pass: the gates handed to
    /// writers, whether the sweep is paused for storage, and whether the
    /// dashboard shows the budget as critical.
    pub gate: GateState,
    /// When `db_bytes` was measured (server time).
    pub measured_at: Option<DateTime<Utc>>,
    /// The sustained-growth alert as the dashboard shows it; `None` while
    /// growth is ordinary or there are too few samples to tell.
    pub growth_warning: Option<String>,
    /// When the Cloudflare ranges were last refreshed.
    pub cf_refreshed_at: Option<DateTime<Utc>>,
    /// Size of the three history tables with their indexes.
    pub history_bytes: Option<u64>,
    /// The UI sort indexes are not being built because the storage budget
    /// has no room: the bytes the next one is estimated to need.
    pub sort_held_bytes: Option<u64>,
}

impl ServerStatus {
    /// Reads the published values.
    pub fn get(&self) -> StatusInner {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Changes the published values under the write lock. Each writer
    /// sets its own fields and leaves the rest.
    pub fn update(&self, f: impl FnOnce(&mut StatusInner)) {
        f(&mut self.inner.write().unwrap_or_else(|e| e.into_inner()));
    }
}

/// Normal-mode web state.
#[derive(Debug)]
pub struct WebState {
    /// The API state (pool, config, snapshot, limits).
    pub api: Arc<ApiState>,
    /// Safe outbound client (handle resolution).
    pub safe: SafeClient,
    /// IPs with a successful sign-in: exempt from the process-wide
    /// sign-in bucket.
    pub recent_logins: Mutex<HashMap<IpAddr, Instant>>,
    /// The OAuth sign-in's state: flows in progress, discovery cache.
    pub oauth: crate::oauth::OAuthState,
    /// Set to true to switch to setup mode (reset).
    pub reset: watch::Sender<bool>,
    /// The setup token's file, next to `config.toml`. Normal mode only
    /// writes it: a reset stores a new token there for the wizard.
    pub token_path: PathBuf,
    /// What the server's background tasks publish for the dashboard.
    pub status: Arc<ServerStatus>,
    /// The public UI's state (handle cache, warming queue, render bound,
    /// pending confirmations).
    pub public: crate::public::PublicState,
    /// Which UI sections sort by shown time: one flag per sort index.
    pub sort: Arc<SortIndexes>,
}

/// The normal-mode UI router. Every route is always mounted; a handler
/// whose surface is switched off answers [`common::not_found`].
pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/admin", get(dashboard))
        .route("/admin/", get(admin_slash))
        .route("/admin/dashboard/fragment", get(dashboard_fragment))
        .route("/admin/alerts", get(alerts))
        .route("/enter", get(crate::enter::page).post(crate::enter::submit))
        .route("/enter/callback", get(crate::enter::callback))
        .route(
            crate::oauth::METADATA_PATH,
            get(crate::enter::client_metadata),
        )
        .route("/admin/logout", post(logout))
        .route("/admin/lookup/did", get(admin_did::lookup_did))
        .route("/admin/lookup/list", get(admin_list::lookup_list))
        .route("/admin/did/{did}/history", get(crate::history::did_history))
        .route(
            "/admin/list/{did}/{rkey}/history",
            get(crate::history::list_history),
        )
        .route("/admin/ops", get(ops_page))
        .route("/admin/ops/{action}", post(ops_action))
        .route("/admin/settings", get(settings_page).post(settings_save))
        .route(
            "/admin/settings/public-ui",
            post(crate::public_settings::save),
        )
        .route(
            "/admin/settings/public-ui/confirm",
            post(crate::public_settings::confirm),
        )
        .route("/admin/settings/token", post(settings_token))
        .route("/admin/reset", get(reset_page).post(reset_submit))
        .route(
            "/setup",
            get(|| async { (StatusCode::NOT_FOUND, "not found") }),
        )
        .route(
            "/setup/{*rest}",
            get(|| async { (StatusCode::NOT_FOUND, "not found") }),
        )
        .route("/admin/card/{did}", get(crate::public::card::admin_route))
        .route("/static/farsight.css", get(common::css))
        .route("/static/public.css", get(common::public_css))
        .route("/static/public.js", get(common::js))
        .route("/static/admin.js", get(common::admin_js))
        .route("/static/htmx.min.js", get(common::htmx))
        .route(common::OG_IMAGE_PATH, get(common::og_image))
        .route("/static/favicon.svg", get(common::favicon))
        .merge(crate::public::router())
        .with_state(state)
}

/// `GET /admin/`: the dashboard's address with a trailing slash.
async fn admin_slash(State(st): State<Arc<WebState>>) -> Response {
    if st.api.config.current().admin_auth() == AdminAuth::Disabled {
        return common::not_found();
    }
    common::redirect("/admin")
}
