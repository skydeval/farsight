//! Normal-mode pages (design §8.6): admin sessions, dashboard, lookups,
//! operations, settings and reset, with the access rules of §3.5.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use askama::Template;
use axum::Router;
use axum::extract::{Form, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use farsight_api::params::Params;
use farsight_api::ratelimit::Class;
use farsight_api::{ApiState, admin as api_admin, handlers};
use farsight_core::config::{AdminAuth, LoadedConfig};
use farsight_core::net::{OutboundClient, SafeClient};
use farsight_core::{AtUri, Collection, Did};
use farsight_storage::backfill_api::{self, Request, RequestOutcome, Requester};
use farsight_storage::gates::GateState;
use farsight_storage::keys::Limits;
use farsight_storage::ui_rows::SortIndexes;
use serde_json::Value;
use tokio::sync::{Semaphore, watch};

use crate::common::{self, NO_STORE, cookie, ct_eq, read_cookie, render_private};

mod admin_did;
mod admin_list;

/// Admin session cookie (§8.6).
pub const ADMIN_COOKIE: &str = "farsight_admin";
/// Idle expiry.
pub const SESSION_IDLE: Duration = Duration::from_secs(12 * 3600);
/// Absolute expiry.
pub const SESSION_ABSOLUTE: Duration = Duration::from_secs(7 * 24 * 3600);
/// How long a migration submit from an IP with a recent successful
/// sign-in may wait for a bcrypt slot; others wait [`LOGIN_WAIT`].
pub const LOGIN_WAIT_KNOWN: Duration = Duration::from_secs(10);
/// Bcrypt slot wait for other IPs.
pub const LOGIN_WAIT: Duration = Duration::from_secs(2);

/// Status the server's background tasks publish for the dashboard.
#[derive(Debug, Default)]
pub struct ServerStatus {
    inner: RwLock<StatusInner>,
}

/// The published values.
#[derive(Debug, Clone, Default)]
pub struct StatusInner {
    /// `pg_database_size`.
    pub db_bytes: Option<u64>,
    /// `storage.budget_bytes`.
    pub budget_bytes: u64,
    /// Effective hard ceiling.
    pub ceiling_bytes: u64,
    /// Gate state.
    pub gate: GateState,
    /// When it was measured.
    pub measured_at: Option<DateTime<Utc>>,
    /// Sustained-growth alert text.
    pub growth_warning: Option<String>,
    /// When the Cloudflare ranges were last refreshed.
    pub cf_refreshed_at: Option<DateTime<Utc>>,
    /// Size of the three history tables with their indexes (§7.7).
    pub history_bytes: Option<u64>,
    /// The UI sort indexes are not being built because the storage budget
    /// has no room: the bytes the next one is estimated to need (§7.6).
    pub sort_held_bytes: Option<u64>,
}

impl ServerStatus {
    /// Reads the published values.
    pub fn get(&self) -> StatusInner {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Updates them.
    pub fn update(&self, f: impl FnOnce(&mut StatusInner)) {
        f(&mut self.inner.write().unwrap_or_else(|e| e.into_inner()));
    }
}

/// Normal-mode web state.
#[derive(Debug)]
pub struct WebState {
    /// The API state (pool, config, snapshot, limits).
    pub api: Arc<ApiState>,
    /// Safe outbound client (handle resolution, §11.3).
    pub safe: SafeClient,
    /// Bound on concurrent bcrypt verifications (§3.6; the migration
    /// page only).
    pub bcrypt_permits: Arc<Semaphore>,
    /// IPs with a successful sign-in: exempt from the process-wide
    /// sign-in bucket (§3.6).
    pub recent_logins: Mutex<HashMap<IpAddr, Instant>>,
    /// The OAuth sign-in's state: flows in progress, discovery cache.
    pub oauth: crate::oauth::OAuthState,
    /// Set to true to switch to setup mode (reset).
    pub reset: watch::Sender<bool>,
    /// `.setup-token` path.
    pub token_path: PathBuf,
    /// Background status.
    pub status: Arc<ServerStatus>,
    /// The public UI's state (handle cache, warming queue, render bound,
    /// pending confirmations).
    pub public: crate::public::PublicState,
    /// Which UI sections sort by shown time: one flag per sort index
    /// (§7.6).
    pub sort: Arc<SortIndexes>,
}

/// A logged-in admin.
#[derive(Debug, Clone)]
pub struct Admin {
    /// CSRF token for forms.
    pub csrf: String,
}

fn hex(b: &[u8]) -> String {
    farsight_api::auth::hex(b)
}

/// The stored key of an OAuth session (§7.1): SHA-256 of the cookie
/// value, a zero byte and the admin DID it was created for. A session is
/// therefore found only while that DID is the configured one.
pub fn oauth_session_key(cookie: &str, did: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(cookie.as_bytes());
    h.update([0u8]);
    h.update(did.as_bytes());
    h.finalize().into()
}

/// The key a session cookie is looked up under with this config: bound
/// to the admin DID when one is configured; the plain hash of a
/// pre-OAuth password session in the migration state; none otherwise.
pub fn session_key(cfg: &LoadedConfig, cookie: &str) -> Option<[u8; 32]> {
    match cfg.admin_auth() {
        AdminAuth::Configured(did) => Some(oauth_session_key(cookie, &did)),
        AdminAuth::Migration => Some(common::sha256(cookie)),
        AdminAuth::Unconfigured | AdminAuth::Disabled => None,
    }
}

pub(crate) async fn admin(st: &WebState, headers: &HeaderMap) -> Option<Admin> {
    let raw = read_cookie(headers, ADMIN_COOKIE)?;
    let id = session_key(&st.api.config.current(), &raw)?;
    let s =
        farsight_storage::auth::touch_session(&st.api.pool, &id, SESSION_IDLE, SESSION_ABSOLUTE)
            .await
            .ok()??;
    Some(Admin { csrf: hex(&s.csrf) })
}

pub(crate) fn login_redirect() -> Response {
    common::redirect("/enter")
}

/// Whether htmx sent the request: the dashboard's poll and the history
/// tables' "Next" links. htmx follows a redirect and swaps what it gets,
/// so such a request must not be sent to the sign-in page.
fn is_htmx(headers: &HeaderMap) -> bool {
    headers.contains_key("hx-request")
}

/// The admin UI's access rule (§3.5, §8.6): every page under `/admin`
/// needs a session. With the admin UI off the pages do not exist; without
/// a session a navigation is sent to the sign-in page and a request made
/// by htmx gets the bare 404, which leaves the page it came from as it
/// is.
pub(crate) async fn gate(st: &WebState, headers: &HeaderMap) -> Result<Admin, Response> {
    if st.api.config.current().admin_auth() == AdminAuth::Disabled {
        return Err(common::not_found());
    }
    match admin(st, headers).await {
        Some(a) => Ok(a),
        None if is_htmx(headers) => Err(common::not_found()),
        None => Err(login_redirect()),
    }
}

pub(crate) fn check_form(
    admin: &Admin,
    headers: &HeaderMap,
    form: &HashMap<String, String>,
) -> Result<(), Response> {
    if !common::same_origin(headers) {
        return Err(common::forbidden("cross-origin request refused"));
    }
    match form.get("csrf") {
        Some(c) if ct_eq(c, &admin.csrf) => Ok(()),
        _ => Err(common::forbidden("invalid form token; reload the page")),
    }
}

/// The normal-mode UI router (§8.6). Every route is always mounted; a
/// handler whose surface is switched off answers [`common::not_found`].
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
        // The admin pages' addresses before they moved under `/admin`:
        // redirected for one release, then gone.
        .route("/lookup/did", get(old_lookup_did))
        .route("/lookup/list", get(old_lookup_list))
        .route("/ops", get(old_ops))
        .route("/settings", get(old_settings))
        .route("/reset", get(old_reset))
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

/// A pre-`/admin` address of an admin page: a permanent redirect to the
/// new one while the admin UI is on, with the request's query. The target
/// is a constant; nothing of the request's path goes into it.
fn old_admin_path(st: &WebState, to: &'static str, query: Option<String>) -> Response {
    if st.api.config.current().admin_auth() == AdminAuth::Disabled {
        return common::not_found();
    }
    common::moved(to, query.as_deref())
}

async fn old_lookup_did(State(st): State<Arc<WebState>>, RawQuery(q): RawQuery) -> Response {
    old_admin_path(&st, "/admin/lookup/did", q)
}

async fn old_lookup_list(State(st): State<Arc<WebState>>, RawQuery(q): RawQuery) -> Response {
    old_admin_path(&st, "/admin/lookup/list", q)
}

async fn old_ops(State(st): State<Arc<WebState>>, RawQuery(q): RawQuery) -> Response {
    old_admin_path(&st, "/admin/ops", q)
}

async fn old_settings(State(st): State<Arc<WebState>>, RawQuery(q): RawQuery) -> Response {
    old_admin_path(&st, "/admin/settings", q)
}

async fn old_reset(State(st): State<Arc<WebState>>, RawQuery(q): RawQuery) -> Response {
    old_admin_path(&st, "/admin/reset", q)
}

// ---------------------------------------------------------------------------
// Layout

/// Navigation shown in the page header.
#[derive(Debug, Clone, Default)]
pub struct Nav {
    /// Logged in. Without a session (the sign-in pages) the header has
    /// the brand and nothing else.
    pub admin: bool,
    /// CSRF token (logout form).
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
    /// Title.
    pub title: String,
    /// Message.
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

// ---------------------------------------------------------------------------
// Coverage in words

/// Words for a reason code.
pub fn reason_words(r: &str) -> &'static str {
    match r {
        "sweep_incomplete" => "the first full sweep has not completed",
        "firehose_gap" => "a firehose gap is not repaired yet",
        "firehose_disconnected" => "the firehose is disconnected",
        "firehose_lagging" => "the firehose is more than 5 minutes behind",
        "sync_events_unavailable" => "the Jetstream instance is v1 (no #sync events)",
        "storage_refusal" => "the storage budget is refusing writes",
        "list_pending" => "a list is still being fetched",
        "list_pending_historical" => "a newly admitted list has listblocks older than the stream",
        "list_capped" => "a list's stored members hit a cap",
        "list_unavailable" => "a list could not be fetched (retrying)",
        "list_missing" => "a list record was not found",
        "list_deferred" => "a list's admission is deferred by a storage gate",
        "list_not_tracked" => "nobody listblocks this list, so its members are not stored",
        "discovery_truncated" => "backlink discovery stopped at its reference cap",
        "party_debt" => "an account in this answer is waiting for a re-list",
        _ => "an unrecognized condition (treated as partial)",
    }
}

/// A `freshness` object in words (dashboard and lookups).
pub fn coverage_words(f: &Value) -> String {
    let c = &f["coverage"];
    let level = c["level"].as_str().unwrap_or("partial");
    let head = match level {
        "complete" => "Complete",
        "assisted" => "Assisted (via backlink discovery)",
        _ => "Partial",
    };
    let since = c["completeSince"]
        .as_str()
        .map(|s| format!(" since {}", short_ts(s)))
        .unwrap_or_default();
    let reasons: Vec<&str> = c["reasons"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(reason_words)
                .collect()
        })
        .unwrap_or_default();
    let indexed = f["indexedAt"]
        .as_str()
        .map(|s| format!(" Reflects everything witnessed up to {}.", short_ts(s)))
        .unwrap_or_default();
    if reasons.is_empty() {
        format!("{head}{since}.{indexed}")
    } else {
        let joiner = if level == "complete" {
            "; excluding: "
        } else {
            ": "
        };
        format!("{head}{since}{joiner}{}.{indexed}", reasons.join("; "))
    }
}

fn short_ts(s: &str) -> String {
    DateTime::parse_from_rfc3339(s)
        .map(|t| {
            t.with_timezone(&Utc)
                .format("%Y-%m-%d %H:%M:%S UTC")
                .to_string()
        })
        .unwrap_or_else(|_| s.to_owned())
}

// ---------------------------------------------------------------------------
// Dashboard

/// One labelled value.
#[derive(Debug, Clone)]
pub struct Stat {
    /// Label.
    pub label: String,
    /// Value.
    pub value: String,
}

/// A warning banner.
#[derive(Debug, Clone)]
pub struct Warning {
    /// `bad` or empty.
    pub class: &'static str,
    /// Text.
    pub text: String,
}

/// Everything the dashboard shows.
#[derive(Debug, Clone, Default)]
pub struct DashboardData {
    /// Warnings (§9.3, budget, v1, gaps).
    pub warnings: Vec<Warning>,
    /// Work still catching up, in words; empty when there is none.
    pub catching: Vec<Stat>,
    /// Index counts.
    pub counts: Vec<Stat>,
    /// Firehose.
    pub firehose: Vec<Stat>,
    /// Open gaps.
    pub gaps: Vec<String>,
    /// Sweep and queues.
    pub backfill: Vec<Stat>,
    /// Exceptions.
    pub exceptions: Vec<Stat>,
    /// Coverage in words.
    pub coverage: String,
    /// Storage.
    pub storage: Vec<Stat>,
    /// Lists per state.
    pub lists: Vec<Stat>,
    /// Top buckets by lifetime interning.
    pub buckets: Vec<(String, String, String, String)>,
}

/// The warning about firehose gaps, if one is due. A closed gap is healed
/// by a repair cycle; one that is open because the firehose is down
/// closes when it reconnects, and only then can be repaired. The open
/// interval of a v1 firehose is the v1 warning's subject, not this one's.
fn gap_warning(repairable: i64, open: i64) -> Option<String> {
    let s = |n: i64| if n == 1 { "" } else { "s" };
    match (repairable, open) {
        (0, 0) => None,
        (r, 0) => Some(format!(
            "{r} firehose gap{} can be repaired: start a repair cycle (Operations → start repair).",
            s(r)
        )),
        (0, o) => Some(format!(
            "{o} firehose gap{} still open: it closes when the firehose reconnects, and can be \
             repaired after that.",
            if o == 1 { " is" } else { "s are" }
        )),
        (r, o) => Some(format!(
            "{r} firehose gap{} can be repaired: start a repair cycle (Operations → start \
             repair). {o} more {} still open and cannot be repaired yet.",
            s(r),
            if o == 1 { "is" } else { "are" }
        )),
    }
}

/// A duration in words, to two units and no less than a minute: "1 day
/// 4 hours".
fn long_secs(s: i64) -> String {
    let unit = |n: i64, w: &str| format!("{n} {w}{}", if n == 1 { "" } else { "s" });
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3600, (s % 3600) / 60);
    if d > 0 && h > 0 {
        format!("{} {}", unit(d, "day"), unit(h, "hour"))
    } else if d > 0 {
        unit(d, "day")
    } else if h > 0 && m > 0 {
        format!("{} {}", unit(h, "hour"), unit(m, "minute"))
    } else if h > 0 {
        unit(h, "hour")
    } else {
        unit(m.max(1), "minute")
    }
}

/// One line of the dashboard's "Catching up": the share done of `total`
/// and, when a rate is known, the time the rest takes.
fn catching_line(done: i64, total: i64, what: &str, left_secs: Option<i64>) -> String {
    // Never "100.0%" while something is left.
    let share = (done.max(0) as f64 / total.max(1) as f64 * 100.0).min(99.9);
    match left_secs {
        Some(s) => format!("{share:.1}% {what}, about {} left", long_secs(s.max(0))),
        None => format!("{share:.1}% {what}"),
    }
}

/// The handle pass's line: none while it is off, before its first step
/// and once it has reached the last account. The time left is what the
/// accounts ahead take at the configured rate; accounts already answered
/// are passed over, so it is an upper bound.
fn handles_line(rps: u32, cursor: i64, last: i64, laps: u64) -> Option<String> {
    if rps == 0 || laps > 0 || last <= 0 || cursor >= last {
        return None;
    }
    Some(catching_line(
        cursor,
        last,
        "of accounts checked",
        Some((last - cursor) / i64::from(rps)),
    ))
}

/// The list filler's line (one list a second while the pass is on): none
/// once every list is read or a round has ended, after which only lists
/// whose owner's server does not answer are left.
fn lists_line(rps: u32, unread: i64, total: i64, rounds: u64) -> Option<String> {
    if rps == 0 || rounds > 0 || unread <= 0 {
        return None;
    }
    Some(catching_line(total - unread, total, "read", Some(unread)))
}

fn stat(label: &str, value: impl ToString) -> Stat {
    Stat {
        label: label.into(),
        value: value.to_string(),
    }
}

async fn dashboard_data(st: &WebState) -> Result<DashboardData, String> {
    let stats = handlers::get_stats(&st.api, &Params::default())
        .await
        .map_err(|e| e.message)?
        .body;
    let mut d = DashboardData::default();
    let c = &stats["counts"];
    for (k, l) in [
        ("blocks", "Blocks"),
        ("listBlocks", "Listblocks"),
        ("lists", "Lists"),
        ("trackedLists", "Tracked lists"),
        ("listItems", "List items"),
        ("actors", "Accounts known"),
    ] {
        d.counts.push(stat(
            l,
            crate::public::text::thousands(c[k].as_i64().unwrap_or(0)),
        ));
    }
    let f = &stats["firehose"];
    let connected = f["connected"].as_bool().unwrap_or(false);
    d.firehose
        .push(stat("Connected", if connected { "yes" } else { "no" }));
    let protocol = f["protocol"].as_str().unwrap_or("—");
    d.firehose.push(stat("Protocol", protocol));
    d.firehose.push(stat(
        "Lag",
        f["lagSeconds"]
            .as_f64()
            .map_or("—".into(), |s| format!("{s:.1} s")),
    ));
    d.firehose.push(stat(
        "Source lag",
        f["sourceLagSeconds"]
            .as_f64()
            .map_or("—".into(), |s| format!("{s:.1} s")),
    ));
    // Unhealed gaps are of three kinds: closed ones, which a repair
    // cycle heals; the interval of a v1 firehose, open until a v2
    // session takes over; and one open because the firehose is down.
    let (mut repairable, mut v1_open, mut other_open) = (0, 0, 0);
    if let Some(gaps) = stats["detail"]["gaps"].as_array() {
        for g in gaps {
            if g["to"].is_string() {
                repairable += 1;
            } else if g["cause"] == "SyncUnavailable" {
                v1_open += 1;
            } else {
                other_open += 1;
            }
            d.gaps.push(format!(
                "{} → {} ({})",
                g["from"].as_str().map(short_ts).unwrap_or_default(),
                g["to"]
                    .as_str()
                    .map(short_ts)
                    .unwrap_or_else(|| "open".into()),
                g["cause"].as_str().unwrap_or("")
            ));
        }
    }
    d.firehose.push(stat("Gaps to repair", repairable));
    d.firehose
        .push(stat("Gaps still open", v1_open + other_open));
    let b = &stats["backfill"];
    if let Some(s) = b.get("sweep") {
        d.backfill.push(stat(
            "Sweep",
            format!(
                "cycle {} · {} · {}",
                s["cycle"],
                s["source"].as_str().unwrap_or(""),
                s["state"].as_str().unwrap_or("")
            ),
        ));
        if let Some(p) = s["progress"].as_f64() {
            d.backfill
                .push(stat("Progress", format!("{:.1}%", p * 100.0)));
        }
        if let Some(e) = s["etaSeconds"].as_i64() {
            d.backfill.push(stat("ETA", common::human_secs(e)));
        }
        if s["state"] != "completed" {
            if let Some(p) = s["progress"].as_f64() {
                let mut line = format!("{:.1}% of accounts swept", (p * 100.0).min(99.9));
                if s["state"] == "paused" {
                    line.push_str(", paused");
                } else if let Some(e) = s["etaSeconds"].as_i64() {
                    line.push_str(&format!(", about {} left", long_secs(e)));
                }
                d.catching.push(stat("History", line));
            }
        }
    } else {
        d.backfill.push(stat("Sweep", "not started"));
    }
    let q = stats["detail"]["queueByTier"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for (i, l) in ["Queue: on-demand", "Queue: active", "Queue: sweep"]
        .iter()
        .enumerate()
    {
        d.backfill
            .push(stat(l, q.get(i).and_then(Value::as_i64).unwrap_or(0)));
    }
    d.backfill
        .push(stat("Repos/hour", b["reposPerHour"].as_i64().unwrap_or(0)));
    let ex = &stats["freshness"]["coverage"]["exceptions"];
    if let Some(m) = ex.as_object() {
        for (k, v) in m {
            d.exceptions.push(stat(k, v.as_i64().unwrap_or(0)));
        }
    }
    d.exceptions.push(stat(
        "pendingLists",
        stats["freshness"]["coverage"]["pendingLists"]
            .as_i64()
            .unwrap_or(0),
    ));
    d.coverage = coverage_words(&stats["freshness"]);

    let mut conn = st.api.pool.acquire().await.map_err(|e| e.to_string())?;
    let rps = st.api.config.current().config.public_ui.handle_pass_rps;
    let at = &st.public.progress;
    if let Some(line) = handles_line(
        rps,
        at.cursor.load(Ordering::Relaxed),
        at.last.load(Ordering::Relaxed),
        at.laps.load(Ordering::Relaxed),
    ) {
        d.catching.push(stat("Handles", line));
    }
    let rounds = at.list_rounds.load(Ordering::Relaxed);
    if rps > 0 && rounds == 0 {
        let (unread, total) = farsight_storage::queries::lists_unread_count(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(line) = lists_line(rps, unread, total, rounds) {
            d.catching.push(stat("List descriptions", line));
        }
    }
    for (s, n) in farsight_storage::queries::lists_by_state(&mut conn)
        .await
        .map_err(|e| e.to_string())?
    {
        d.lists.push(stat(s.api_name(), n));
    }
    for (bucket, interned, blocks, items, _, _, mask) in
        farsight_storage::queries::top_buckets(&mut conn, 10)
            .await
            .map_err(|e| e.to_string())?
    {
        d.buckets.push((
            bucket,
            interned.to_string(),
            format!("{blocks} blocks · {items} items"),
            if mask != 0 {
                "capped".into()
            } else {
                String::new()
            },
        ));
    }
    drop(conn);

    let s = st.status.get();
    if let Some(db) = s.db_bytes {
        let ratio = if s.budget_bytes > 0 {
            db as f64 / s.budget_bytes as f64
        } else {
            0.0
        };
        d.storage.push(stat("Database", common::human_bytes(db)));
        d.storage
            .push(stat("Budget", common::human_bytes(s.budget_bytes)));
        d.storage
            .push(stat("Of budget", format!("{:.1}%", ratio * 100.0)));
        d.storage
            .push(stat("Hard ceiling", common::human_bytes(s.ceiling_bytes)));
        if let Some(h) = s.history_bytes {
            d.storage
                .push(stat("History (in the budget)", common::human_bytes(h)));
        }
        if s.gate.critical {
            d.warnings.push(Warning {
                class: "bad",
                text: format!(
                    "Storage is at {:.0}% of the budget (critical). Creates and updates are \
                     refused; raise storage.budget_bytes after adding disk. Postgres does not \
                     shrink after deletes without VACUUM FULL or pg_repack.",
                    ratio * 100.0
                ),
            });
        } else if s.gate.gates.budget_refusing || s.gate.gates.ceiling_refusing {
            d.warnings.push(Warning {
                class: "bad",
                text: format!(
                    "Storage budget reached ({:.0}%): new list admissions are deferred and \
                     writes from non-large hosts are refused (and counted). Raise the budget \
                     after adding disk; `VACUUM FULL` or `pg_repack` reclaims space after deletes.",
                    ratio * 100.0
                ),
            });
        } else if s.gate.sweep_paused {
            d.warnings.push(Warning {
                class: "",
                text: format!(
                    "Storage at {:.0}% of budget: the sweep is paused.",
                    ratio * 100.0
                ),
            });
        } else if ratio >= 0.8 {
            d.warnings.push(Warning {
                class: "",
                text: format!("Storage at {:.0}% of budget.", ratio * 100.0),
            });
        }
    }
    if let Some(g) = s.growth_warning {
        d.warnings.push(Warning { class: "", text: g });
    }
    if let Some(share) = st.api.cf.warning() {
        d.warnings.push(Warning {
            class: "bad",
            text: format!(
                "{:.0}% of requests in the last 5 minutes came from Cloudflare edges, but \
                 Cloudflare is not trusted: every client shares a few rate-limit buckets. Set \
                 the reverse proxy to Cloudflare in Settings (proxy.mode, proxy.trusted).",
                share * 100.0
            ),
        });
    }
    if !connected {
        d.warnings.push(Warning {
            class: "bad",
            text: "The firehose is disconnected; coverage is partial until it reconnects.".into(),
        });
    }
    if protocol == "v1" {
        d.warnings.push(Warning {
            class: "",
            text: "The firehose is v1: coverage is capped at partial (sync_events_unavailable). \
                   Use a v2 Jetstream for complete coverage. The time spent on v1 is recorded \
                   as one gap, which stays open, and cannot be repaired, until a v2 Jetstream \
                   takes over."
                .into(),
        });
    }
    if let Some(text) = gap_warning(repairable, other_open) {
        d.warnings.push(Warning { class: "", text });
    }
    Ok(d)
}

/// The dashboard.
#[derive(Template)]
#[template(path = "dashboard.html")]
pub struct DashboardPage {
    /// Navigation.
    pub nav: Nav,
    /// Data.
    pub d: DashboardData,
    /// Load error.
    pub error: Option<String>,
}

/// The htmx fragment.
#[derive(Template)]
#[template(path = "dashboard_fragment.html")]
pub struct DashboardFragment {
    /// Data.
    pub d: DashboardData,
    /// Load error.
    pub error: Option<String>,
}

async fn dashboard(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let (d, error) = match dashboard_data(&st).await {
        Ok(d) => (d, None),
        Err(e) => (DashboardData::default(), Some(e)),
    };
    render_private(&DashboardPage {
        nav: nav(&Some(s)),
        d,
        error,
    })
}

/// Tells the admin that sign-in is changing (the migration state, §8.6).
fn migration_warning(st: &WebState, d: &mut DashboardData) {
    if st.api.config.current().admin_auth() == AdminAuth::Migration {
        d.warnings.insert(
            0,
            Warning {
                class: "bad",
                text: "Admin sign-in is changing: open /enter to set the admin DID. The \
                       password stops working once it is set."
                    .into(),
            },
        );
    }
}

/// Tells the admin that the tables do not all sort by creation
/// time yet: the sort indexes are still being built, or are held because
/// the storage budget has no room for them (§7.6).
fn sort_warning(st: &WebState, d: &mut DashboardData) {
    let ready = st.sort.count();
    if ready == 4 {
        return;
    }
    let text = match st.status.get().sort_held_bytes {
        Some(need) => format!(
            "Sorting by creation time: {ready} of 4 indexes ready; the rest are not built, the \
             storage budget has no room (needs ~{}). Tables without their index keep their \
             previous order. Raise storage.budget_bytes after adding disk.",
            common::human_bytes(need)
        ),
        None => format!(
            "Sorting by creation time: {ready} of 4 indexes ready. They build in the \
             background; a table keeps its previous order until its index is ready."
        ),
    };
    d.warnings.push(Warning { class: "", text });
}

async fn dashboard_fragment(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    if let Err(r) = gate(&st, &headers).await {
        return r;
    }
    let (d, error) = match dashboard_data(&st).await {
        Ok(d) => (d, None),
        Err(e) => (DashboardData::default(), Some(e)),
    };
    render_private(&DashboardFragment { d, error })
}

/// What the bar's "Alerts" holds: the warnings and the coverage
/// sentence. Every admin page's bar loads it, and again every 30 s.
#[derive(Template)]
#[template(path = "alerts_fragment.html")]
pub struct AlertsFragment {
    /// Warnings (§9.3, budget, v1, gaps, sign-in, sort indexes).
    pub warnings: Vec<Warning>,
    /// Coverage in words.
    pub coverage: String,
    /// Load error.
    pub error: Option<String>,
}

async fn alerts(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    if let Err(r) = gate(&st, &headers).await {
        return r;
    }
    let (mut d, error) = match dashboard_data(&st).await {
        Ok(d) => (d, None),
        Err(e) => (DashboardData::default(), Some(e)),
    };
    migration_warning(&st, &mut d);
    sort_warning(&st, &mut d);
    render_private(&AlertsFragment {
        warnings: d.warnings,
        coverage: d.coverage,
        error,
    })
}

// ---------------------------------------------------------------------------
// Logout (sign-in is in `crate::enter`)

pub(crate) fn metrics_rate_limited(c: Class) {
    farsight_api::metrics::rate_limited(c);
}

async fn logout(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    if st.api.config.current().admin_auth() == AdminAuth::Disabled {
        return common::not_found();
    }
    if let Some(raw) = read_cookie(&headers, ADMIN_COOKIE) {
        if common::same_origin(&headers) {
            if let Some(key) = session_key(&st.api.config.current(), &raw) {
                let _ = farsight_storage::auth::delete_session(&st.api.pool, &key).await;
            }
        }
    }
    let mut r = common::redirect("/enter");
    r.headers_mut().append(
        header::SET_COOKIE,
        cookie(ADMIN_COOKIE, "", "/", false, Some(0)),
    );
    r
}

// ---------------------------------------------------------------------------
// Lookups

/// Resolves a handle to a DID: DNS TXT `_atproto.<handle>`, then
/// `https://<handle>/.well-known/atproto-did`, through the safe client
/// (§8.6, §11.3).
pub async fn resolve_handle(safe: &SafeClient, handle: &str) -> Result<Did, String> {
    let handle = handle.trim().trim_start_matches('@').to_ascii_lowercase();
    if !farsight_core::did::is_valid_hostname(&handle) {
        return Err(format!("{handle:?} is neither a DID nor a valid handle"));
    }
    if let Ok(txts) = safe.txt(&format!("_atproto.{handle}")).await {
        for t in txts {
            if let Some(d) = t.strip_prefix("did=") {
                if let Ok(did) = Did::parse(d.trim()) {
                    return Ok(did);
                }
            }
        }
    }
    let url = url::Url::parse(&format!("https://{handle}/.well-known/atproto-did"))
        .map_err(|e| e.to_string())?;
    let r = safe
        .get(&url)
        .await
        .map_err(|e| format!("could not resolve {handle}: {e}"))?;
    if r.status != 200 {
        return Err(format!("could not resolve {handle}: HTTP {}", r.status));
    }
    let body = String::from_utf8_lossy(&r.body);
    Did::parse(body.trim()).map_err(|_| format!("{handle} did not resolve to a DID"))
}

pub(crate) async fn permit(st: &WebState) -> Result<tokio::sync::OwnedSemaphorePermit, String> {
    match tokio::time::timeout(
        farsight_api::PERMIT_WAIT,
        st.api.query_permits.clone().acquire_owned(),
    )
    .await
    {
        Ok(Ok(p)) => Ok(p),
        _ => Err("The server is busy; try again shortly.".into()),
    }
}

/// Parses an AT-URI or a `https://bsky.app/profile/<actor>/lists/<rkey>`
/// URL into `(actor, rkey)`; `actor` may be a handle.
pub fn parse_list_ref(q: &str) -> Option<(String, String)> {
    let q = q.trim();
    if let Ok(u) = AtUri::parse(q) {
        return (u.indexed_collection() == Some(Collection::List))
            .then(|| (u.authority.as_str().to_owned(), u.rkey.as_str().to_owned()));
    }
    let u = url::Url::parse(q).ok()?;
    let segs: Vec<&str> = u.path_segments()?.filter(|s| !s.is_empty()).collect();
    match segs.as_slice() {
        ["profile", actor, "lists", rkey] => Some(((*actor).to_owned(), (*rkey).to_owned())),
        _ => None,
    }
}

/// The operations page.
#[derive(Template)]
#[template(path = "ops.html")]
pub struct OpsPage {
    /// Navigation.
    pub nav: Nav,
    /// CSRF token.
    pub csrf: String,
    /// Result of the last action.
    pub notice: Option<String>,
    /// Error of the last action.
    pub error: Option<String>,
    /// A newly created API key, shown once.
    pub new_key: Option<String>,
    /// Recent errors: (when, component, did, message).
    pub errors: Vec<(String, String, String, String)>,
    /// API keys: (id, name, scopes, created, last used, revoked).
    pub keys: Vec<(i32, String, String, String, String, String)>,
    /// The sweep is enabled.
    pub sweep_enabled: bool,
    /// Config is env-managed (sweep toggle unavailable).
    pub env_managed: bool,
}

async fn ops_render(
    st: &WebState,
    s: &Admin,
    notice: Option<String>,
    error: Option<String>,
    new_key: Option<String>,
) -> Response {
    let cfg = st.api.config.current();
    let mut page = OpsPage {
        nav: nav(&Some(s.clone())),
        csrf: s.csrf.clone(),
        notice,
        error,
        new_key,
        errors: Vec::new(),
        keys: Vec::new(),
        sweep_enabled: cfg.config.backfill.sweep.enabled,
        env_managed: cfg.from_env_only,
    };
    if let Ok(mut conn) = st.api.pool.acquire().await {
        if let Ok(rows) = farsight_storage::queries::op_errors(&mut conn, None, 25).await {
            for (_, at, component, did, _, msg) in rows {
                page.errors.push((
                    at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                    component,
                    did.unwrap_or_default(),
                    msg,
                ));
            }
        }
    }
    if let Ok(keys) = farsight_storage::auth::list_tokens(&st.api.pool).await {
        for k in keys {
            page.keys.push((
                k.id,
                k.name,
                k.scopes.join(", "),
                k.created_at.format("%Y-%m-%d %H:%M UTC").to_string(),
                k.last_used_at
                    .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
                    .unwrap_or_else(|| "never".into()),
                k.revoked_at
                    .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
                    .unwrap_or_default(),
            ));
        }
    }
    render_private(&page)
}

async fn ops_page(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    match gate(&st, &headers).await {
        Ok(s) => ops_render(&st, &s, None, None, None).await,
        Err(r) => r,
    }
}

async fn ops_action(
    State(st): State<Arc<WebState>>,
    axum::extract::Path(action): axum::extract::Path<String>,
    headers: HeaderMap,
    Form(form): Form<Vec<(String, String)>>,
) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let map: HashMap<String, String> = form.iter().cloned().collect();
    if let Err(r) = check_form(&s, &headers, &map) {
        return r;
    }
    let get = |k: &str| map.get(k).map(|v| v.trim().to_owned()).unwrap_or_default();
    let result: Result<(String, Option<String>), String> = match action.as_str() {
        "backfill" => {
            let q = get("actor");
            let did = if q.starts_with("did:") {
                Did::parse(&q).map_err(|e| e.to_string())
            } else {
                resolve_handle(&st.safe, &q).await
            };
            match did {
                Err(e) => Err(e),
                Ok(did) => {
                    let cfg = st.api.config.current();
                    let source = cfg.config.backfill.backlinks.url.clone();
                    let requester = Requester {
                        key: "admin".into(),
                        may_high: true,
                    };
                    let req = Request {
                        actor: &did,
                        high: get("priority") == "high",
                        force: map.contains_key("force"),
                        requester: &requester,
                        fresh_window: cfg.config.backfill.request_fresh_window.get(),
                        discovery_source: (!source.is_empty()).then_some(source.as_str()),
                    };
                    match backfill_api::request(
                        &st.api.pool,
                        &Limits::from_config(&cfg.config),
                        &st.api.counters,
                        st.api.gates.load(),
                        &req,
                    )
                    .await
                    {
                        Ok(RequestOutcome::Ok { enqueued, .. }) => Ok((
                            if enqueued {
                                format!("Backfill of {} queued.", did.as_str())
                            } else {
                                format!(
                                    "No new work for {} (already queued, running or fresh; use force).",
                                    did.as_str()
                                )
                            },
                            None,
                        )),
                        Ok(RequestOutcome::QueueFull) => Err("The admin queue is full.".into()),
                        Ok(RequestOutcome::InternRefused) => {
                            Err("The admin's daily intern rate is exhausted.".into())
                        }
                        Err(e) => Err(e.to_string()),
                    }
                }
            }
        }
        "restart-firehose" => match api_admin::restart_firehose(&st.api).await {
            Ok(_) => Ok(("Firehose session restarted.".into(), None)),
            Err(e) => Err(e.message),
        },
        "sweep" => {
            let paused = get("paused") == "true";
            match api_admin::set_sweep_paused(&st.api, paused).await {
                Ok(()) => Ok((
                    if paused {
                        "Sweep paused."
                    } else {
                        "Sweep resumed."
                    }
                    .into(),
                    None,
                )),
                Err(e) => Err(e.message),
            }
        }
        "repair" => match api_admin::start_repair_cycle(&st.api).await {
            Ok(r) if r.gaps == 0 => Ok((
                "Nothing to repair: no gap is closed and not yet healed. A gap that is \
                     still open (a v1 firehose's, or a disconnection in progress) can be \
                     repaired only once it has closed."
                    .into(),
                None,
            )),
            Ok(r) => Ok((
                format!(
                    "Repair cycle {} requested for {} gap(s); the backfill process runs it.",
                    r.cycle.unwrap_or_default(),
                    r.gaps
                ),
                None,
            )),
            Err(e) => Err(e.message),
        },
        "keys-create" => {
            let scopes: Vec<String> = form
                .iter()
                .filter(|(k, _)| k == "scope")
                .map(|(_, v)| v.clone())
                .collect();
            let rps = get("read_rps");
            let rps = if rps.is_empty() {
                Ok(None)
            } else {
                rps.parse::<u32>()
                    .map(Some)
                    .map_err(|_| "read rps must be a number".to_owned())
            };
            match rps {
                Err(e) => Err(e),
                Ok(rps) => match api_admin::create_key(&st.api, &get("name"), &scopes, rps).await {
                    Ok((id, token)) => Ok((format!("API key {id} created."), Some(token))),
                    Err(e) => Err(e.message),
                },
            }
        }
        "keys-revoke" => match get("id").parse::<i32>() {
            Ok(id) => match farsight_storage::auth::revoke_token(&st.api.pool, id).await {
                Ok(true) => {
                    let _ = st.api.keys.refresh(&st.api.pool).await;
                    Ok((format!("API key {id} revoked."), None))
                }
                Ok(false) => Err(format!("No live key {id}.")),
                Err(e) => Err(e.to_string()),
            },
            Err(_) => Err("bad key id".into()),
        },
        _ => return (StatusCode::NOT_FOUND, "not found").into_response(),
    };
    match result {
        Ok((notice, key)) => ops_render(&st, &s, Some(notice), None, key).await,
        Err(e) => ops_render(&st, &s, None, Some(e), None).await,
    }
}

// ---------------------------------------------------------------------------
// Settings

/// The settings page.
#[derive(Template)]
#[template(path = "settings.html")]
pub struct SettingsPage {
    /// Navigation.
    pub nav: Nav,
    /// CSRF token.
    pub csrf: String,
    /// The config file, secrets redacted.
    pub text: String,
    /// Keys set from the environment (locked).
    pub env_keys: Vec<String>,
    /// Config is env-managed.
    pub env_managed: bool,
    /// Result notice.
    pub notice: Option<String>,
    /// Error.
    pub error: Option<String>,
    /// Keys that changed and need a restart.
    pub restart: Vec<String>,
    /// A new admin token, shown once.
    pub new_token: Option<String>,
    /// `access.admin_did` (empty when not set).
    pub admin_did: String,
    /// Its verified handle, when known.
    pub admin_handle: Option<String>,
    /// The instance is in the migration state.
    pub migration: bool,
    /// The Public UI subsection's values.
    pub p: crate::public_settings::View,
}

const REDACTED: &str = "<redacted>";

fn redact_file(text: &str) -> String {
    let Ok(mut t) = text.parse::<toml::Table>() else {
        return text.to_owned();
    };
    for (sec, key) in [
        ("auth", "admin_token_sha256"),
        ("auth", "admin_password_bcrypt"),
        ("metrics", "bearer_token_sha256"),
    ] {
        if let Some(v) = t
            .get_mut(sec)
            .and_then(|s| s.as_table_mut())
            .and_then(|s| s.get_mut(key))
        {
            if v.as_str().is_some_and(|s| !s.is_empty()) {
                *v = toml::Value::String(REDACTED.into());
            }
        }
    }
    if let Some(v) = t
        .get_mut("storage")
        .and_then(|s| s.as_table_mut())
        .and_then(|s| s.get_mut("database_url"))
    {
        if let Some(s) = v.as_str() {
            *v = toml::Value::String(crate::setup::redact_dsn(s));
        }
    }
    toml::to_string_pretty(&t).unwrap_or_else(|_| text.to_owned())
}

/// Puts the real secrets back where the submitted text still shows the
/// redacted placeholder.
fn unredact(submitted: &str, current: &str) -> Result<String, String> {
    let mut t: toml::Table = submitted
        .parse()
        .map_err(|e: toml::de::Error| e.to_string())?;
    let cur: toml::Table = current
        .parse()
        .map_err(|e: toml::de::Error| e.to_string())?;
    let lookup = |sec: &str, key: &str| cur.get(sec).and_then(|s| s.get(key)).cloned();
    for (sec, key) in [
        ("auth", "admin_token_sha256"),
        ("auth", "admin_password_bcrypt"),
        ("metrics", "bearer_token_sha256"),
        ("storage", "database_url"),
    ] {
        let Some(v) = t
            .get_mut(sec)
            .and_then(|s| s.as_table_mut())
            .and_then(|s| s.get_mut(key))
        else {
            continue;
        };
        let original = lookup(sec, key);
        let shown = original.as_ref().and_then(|o| o.as_str()).map(|s| {
            if key == "database_url" {
                crate::setup::redact_dsn(s)
            } else {
                REDACTED.to_owned()
            }
        });
        if v.as_str() == Some(REDACTED) || (v.as_str().is_some() && v.as_str() == shown.as_deref())
        {
            if let Some(o) = original {
                *v = o;
            }
        }
    }
    toml::to_string_pretty(&t).map_err(|e| e.to_string())
}

pub(crate) fn settings_base(st: &WebState, s: &Admin) -> SettingsPage {
    let cur = st.api.config.current();
    SettingsPage {
        nav: nav(&Some(s.clone())),
        csrf: s.csrf.clone(),
        text: st
            .api
            .config
            .file_text()
            .map(|t| redact_file(&t))
            .unwrap_or_default(),
        env_keys: cur.env_keys.clone(),
        env_managed: cur.from_env_only,
        notice: None,
        error: None,
        restart: Vec::new(),
        new_token: None,
        admin_did: cur.config.access.admin_did.clone(),
        admin_handle: st
            .oauth
            .cached_handle(&cur.config.access.admin_did)
            .flatten(),
        migration: cur.admin_auth() == AdminAuth::Migration,
        p: crate::public_settings::View::of(&cur.config),
    }
}

async fn settings_page(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    match gate(&st, &headers).await {
        Ok(s) => {
            // Resolve the admin handle for display (cached; two-second
            // budget), so that `settings_base` finds it.
            let cur = st.api.config.current();
            if !cur.config.access.admin_did.is_empty() {
                st.oauth
                    .handle(&st.safe, &cur.config, &cur.config.access.admin_did)
                    .await;
            }
            render_private(&settings_base(&st, &s))
        }
        Err(r) => r,
    }
}

async fn settings_save(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if let Err(r) = check_form(&s, &headers, &form) {
        return r;
    }
    let submitted = form.get("config").cloned().unwrap_or_default();
    let current = st.api.config.file_text().unwrap_or_default();
    let result = match unredact(&submitted, &current) {
        // Turning the public UI on needs the operator's confirmation,
        // whichever editor asks for it (§8.6).
        Ok(text) => match crate::public_settings::confirmation(
            &st,
            &s,
            crate::public_settings::Change::File(text.clone()),
        ) {
            Ok(Some(confirm)) => return confirm,
            Ok(None) => st
                .api
                .config
                .replace(&text)
                .await
                .map_err(|e| e.to_string()),
            Err(e) => Err(e),
        },
        Err(e) => Err(format!("Not valid TOML: {e}")),
    };
    let mut page = settings_base(&st, &s);
    match result {
        Ok(report) => {
            let _ = farsight_api::config_store::notify_config(&st.api.pool).await;
            page.notice = Some(if report.changed.is_empty() {
                "No changes.".into()
            } else {
                format!("Saved. Changed: {}.", report.changed.join(", "))
            });
            page.restart = report.restart_required;
        }
        Err(e) => {
            page.error = Some(e);
            page.text = submitted;
        }
    }
    render_private(&page)
}

fn set_auth(t: &mut toml::Table, key: &str, value: String) -> Result<(), String> {
    let auth = t
        .entry("auth")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .ok_or("`auth` is not a table")?;
    auth.insert(key.into(), toml::Value::String(value));
    Ok(())
}

async fn settings_token(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if let Err(r) = check_form(&s, &headers, &form) {
        return r;
    }
    let token = farsight_api::auth::generate(farsight_api::auth::ADMIN_PREFIX);
    let hash = farsight_api::auth::hex(&farsight_api::auth::sha256(&token));
    let mut page = settings_base(&st, &s);
    if let Err(e) = st
        .api
        .config
        .edit(|t| set_auth(t, "admin_token_sha256", hash))
        .await
    {
        page.error = Some(e.to_string());
        return render_private(&page);
    }
    let _ = farsight_api::config_store::notify_config(&st.api.pool).await;
    // Rotation revokes every session (§8.6); this page shows the token
    // once, then the operator logs in again.
    let _ = farsight_storage::auth::delete_all_sessions(&st.api.pool).await;
    page.text = st
        .api
        .config
        .file_text()
        .map(|t| redact_file(&t))
        .unwrap_or_default();
    page.notice = Some("Admin token rotated. All sessions were signed out.".into());
    page.new_token = Some(token);
    let mut r = render_private(&page);
    r.headers_mut().append(
        header::SET_COOKIE,
        cookie(ADMIN_COOKIE, "", "/", false, Some(0)),
    );
    r
}

// ---------------------------------------------------------------------------
// Reset

/// The reset page.
#[derive(Template)]
#[template(path = "reset.html")]
pub struct ResetPage {
    /// Navigation.
    pub nav: Nav,
    /// CSRF token.
    pub csrf: String,
    /// The hostname to type.
    pub hostname: String,
    /// Unavailable (env-managed config).
    pub unavailable: bool,
    /// Error.
    pub error: Option<String>,
}

async fn reset_page(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let cur = st.api.config.current();
    render_private(&ResetPage {
        nav: nav(&Some(s.clone())),
        csrf: s.csrf,
        hostname: cur.config.server.hostname.clone(),
        unavailable: cur.from_env_only,
        error: None,
    })
}

/// Performs the reset (§8.6): revokes all admin sessions and API keys,
/// deletes `config.toml`, writes a new setup token, notifies, and asks the
/// server to switch to setup mode. Database contents are kept.
pub async fn perform_reset(st: &WebState) -> Result<(), String> {
    farsight_storage::auth::delete_all_sessions(&st.api.pool)
        .await
        .map_err(|e| e.to_string())?;
    farsight_storage::auth::revoke_all_tokens(&st.api.pool)
        .await
        .map_err(|e| e.to_string())?;
    std::fs::remove_file(st.api.config.path()).map_err(|e| e.to_string())?;
    let t = crate::setup_token::rotate(&st.token_path).map_err(|e| e.to_string())?;
    crate::setup_token::print(&t);
    let _ = farsight_api::config_store::notify_config(&st.api.pool).await;
    tracing::warn!("configuration reset by an admin; switching to setup mode");
    let _ = st.reset.send(true);
    Ok(())
}

async fn reset_submit(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if let Err(r) = check_form(&s, &headers, &form) {
        return r;
    }
    let cur = st.api.config.current();
    let page = |error: Option<String>| ResetPage {
        nav: nav(&Some(s.clone())),
        csrf: s.csrf.clone(),
        hostname: cur.config.server.hostname.clone(),
        unavailable: cur.from_env_only,
        error,
    };
    if cur.from_env_only {
        return render_private(&page(Some(
            "Reset is unavailable: the configuration comes from the environment \
             (FARSIGHT_SKIP_WIZARD)."
                .into(),
        )));
    }
    if form.get("hostname").map(|h| h.trim()) != Some(cur.config.server.hostname.as_str()) {
        return render_private(&page(Some("The hostname does not match.".into())));
    }
    if let Err(e) = perform_reset(&st).await {
        return render_private(&page(Some(format!("Reset failed: {e}"))));
    }
    let mut r = render_private(&MessagePage {
        nav: Nav::default(),
        title: "Configuration reset".into(),
        message: "The configuration, admin sessions, admin token and API keys are gone; the \
                  database is kept. A new setup token is in the server log (docker logs \
                  farsight). Farsight is switching to setup mode."
            .into(),
        link: Some(("/setup".into(), "Open setup".into())),
    });
    r.headers_mut().append(
        header::SET_COOKIE,
        cookie(ADMIN_COOKIE, "", "/", false, Some(0)),
    );
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    r
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gap_warnings() {
        // The open interval of a v1 firehose alone raises none.
        assert_eq!(gap_warning(0, 0), None);
        assert_eq!(
            gap_warning(1, 0).as_deref(),
            Some(
                "1 firehose gap can be repaired: start a repair cycle (Operations → start repair)."
            )
        );
        assert!(
            gap_warning(0, 1)
                .unwrap()
                .starts_with("1 firehose gap is still open")
        );
        assert!(gap_warning(2, 1).unwrap().contains("1 more is still open"));
    }

    #[test]
    fn catching_up_lines() {
        assert_eq!(long_secs(100_800), "1 day 4 hours");
        assert_eq!(long_secs(86_400 * 12), "12 days");
        assert_eq!(long_secs(3_660), "1 hour 1 minute");
        assert_eq!(long_secs(59), "1 minute");
        assert_eq!(
            handles_line(10, 500_000, 10_000_000, 0).as_deref(),
            Some("5.0% of accounts checked, about 10 days 23 hours left")
        );
        // Off, not started, at the end, or a walk already completed.
        assert_eq!(handles_line(0, 5, 10, 0), None);
        assert_eq!(handles_line(10, 0, 0, 0), None);
        assert_eq!(handles_line(10, 10, 10, 0), None);
        assert_eq!(handles_line(10, 5, 10, 1), None);
        assert_eq!(
            lists_line(10, 7_200, 10_000, 0).as_deref(),
            Some("28.0% read, about 2 hours left")
        );
        assert_eq!(lists_line(10, 0, 10_000, 0), None);
        assert_eq!(lists_line(10, 3, 10_000, 1), None);
        assert_eq!(lists_line(0, 3, 10_000, 0), None);
        // Something left is never shown as all done.
        assert_eq!(
            lists_line(10, 1, 1_000_000, 0).as_deref(),
            Some("99.9% read, about 1 minute left")
        );
    }

    #[test]
    fn coverage_sentence() {
        let f = json!({"indexedAt": "2026-10-01T00:00:00Z", "coverage": {
            "level": "partial", "reasons": ["sweep_incomplete", "sync_events_unavailable"]}});
        let s = coverage_words(&f);
        assert!(s.starts_with("Partial: the first full sweep"));
        assert!(s.contains("v1"));
        let f = json!({"coverage": {"level": "complete", "completeSince": "2026-09-01T00:00:00Z",
            "reasons": []}});
        assert_eq!(
            coverage_words(&f),
            "Complete since 2026-09-01 00:00:00 UTC."
        );
    }

    #[test]
    fn list_refs() {
        assert_eq!(
            parse_list_ref("https://bsky.app/profile/alice.example/lists/3kabc"),
            Some(("alice.example".into(), "3kabc".into()))
        );
        assert_eq!(
            parse_list_ref("at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list/3k"),
            Some(("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(), "3k".into()))
        );
        assert_eq!(parse_list_ref("https://bsky.app/profile/x"), None);
    }

    #[test]
    fn redaction_round_trip() {
        let file = "[auth]\nadmin_token_sha256 = \"abc\"\nadmin_password_bcrypt = \"$2b$x\"\n\
                    [storage]\ndatabase_url = \"postgres://u:secret@db/f\"\n";
        let shown = redact_file(file);
        assert!(!shown.contains("secret") && !shown.contains("abc"));
        let back = unredact(&shown, file).unwrap();
        let t: toml::Table = back.parse().unwrap();
        assert_eq!(t["auth"]["admin_token_sha256"].as_str(), Some("abc"));
        assert_eq!(
            t["storage"]["database_url"].as_str(),
            Some("postgres://u:secret@db/f")
        );
    }
}
