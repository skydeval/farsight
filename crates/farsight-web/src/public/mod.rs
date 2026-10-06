//! The public UI (design §8.6): `/`, `/search`, `/did/…`, `/list/…`,
//! `/card/…`, their old addresses under `/public`, and `/robots.txt`.
//!
//! Anyone may look up the block relationships of a DID or a list. The
//! surface is toggled by `access.public_ui` and shaped by `[public_ui]`,
//! both read per request.
//!
//! Rules every handler here keeps:
//!
//! - **Off means absent.** With the toggle off, a public route answers
//!   like any unknown route. (`/` is shared: it then leads to the admin
//!   UI, or is a few lines of text.)
//! - **No way out.** A public page links only to public pages and, when
//!   the operator configures one, to the record viewer. It carries no
//!   login link and names no admin route. Removed records are an admin
//!   page ([`crate::history`]); the paths that once served them here
//!   are not found.
//! - **Responses do not depend on the caller.** A response is a function
//!   of the path, the query string and the instance's state: no cookie is
//!   read or set, and no request header changes the body. That is what
//!   makes `Cache-Control: public` safe (§9.4).
//! - **One withheld rule.** An account that is hidden (§7.4) or in
//!   `public_ui.excluded_dids` has no page and appears in no row, and the
//!   notice is the same for both.
//! - **No writes.** The public UI never interns a row and never enqueues
//!   backfill work. (A row shown as a bare DID asks the in-memory warming
//!   worker for that account's handle; nothing is stored.)
//! - **No inline script.** Everything the pages run is the static script
//!   and the vendored htmx; the CSP allows nothing else.

pub mod card;
pub mod coverage;
pub mod handles;
pub mod metrics;
pub mod pages;
pub mod paging;
pub mod pass;
pub mod search;
pub mod text;
pub mod top;
pub mod warming;

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use askama::Template;
use axum::Router;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use farsight_api::clientip::ClientIp;
use farsight_api::params::Params;
use farsight_api::ratelimit::Class;
use farsight_core::config::{AdminAuth, Config, LoadedConfig};
use farsight_core::{Did, RecordKey};
use farsight_storage::codes::actor_status;
use farsight_storage::handles::HandleCache;
use tokio::sync::Notify;

use self::metrics::Page;
use crate::pages::WebState;

/// Rows per page of every public table. There is no `limit` parameter.
pub const PAGE_ROWS: i64 = 50;
/// A section's count is exact up to here. The bound is far above any
/// section an instance holds today; it only keeps a runaway count from
/// running to the query timeout. Beyond it: "more than 5,000,000", and
/// page controls without a last page.
pub const COUNT_CAP: i64 = 5_000_000;
/// Longest a page waits for a render slot before `503`.
pub const RENDER_WAIT: Duration = Duration::from_secs(2);
/// How long the ids of `public_ui.excluded_dids` are reused before they
/// are read again (an excluded DID may be interned later).
pub const EXCLUDED_REFRESH: Duration = Duration::from_secs(30);
/// Longest `{did}` path segment, in bytes.
pub const MAX_DID_SEGMENT: usize = 512;

/// `Content-Security-Policy` of every public page: no inline script or
/// style, nothing from another origin, no framing.
pub const CSP: &str = "default-src 'none'; style-src 'self'; script-src 'self'; \
                       img-src 'self'; connect-src 'self'; base-uri 'none'; \
                       form-action 'self'; frame-ancestors 'none'";
/// The same with images from any `https` origin: profile cards show an
/// avatar that the visitor's browser fetches from the account's own
/// server. In force while `public_ui.show_avatars` is on.
pub const CSP_AVATARS: &str = "default-src 'none'; style-src 'self'; script-src 'self'; \
                               img-src 'self' https:; connect-src 'self'; base-uri 'none'; \
                               form-action 'self'; frame-ancestors 'none'";

/// The policy in force for `cfg`.
pub fn csp(cfg: &Config) -> &'static str {
    if cfg.public_ui.show_avatars {
        CSP_AVATARS
    } else {
        CSP
    }
}

/// The public stylesheet.
pub const PUBLIC_CSS: &str = include_str!("../../static/public.css");
/// The script: theme toggle, local times, profile cards.
pub const PUBLIC_JS: &str = include_str!("../../static/public.js");
/// The admin pages' script: its own copy, changed separately.
pub const ADMIN_JS: &str = include_str!("../../static/admin.js");
/// The one preview image, the same for every page (1200×630).
pub const OG_IMAGE: &[u8] = include_bytes!("../../static/og-default.png");
/// The icon of every page's browser tab: Farsight's mark.
pub const FAVICON: &str = include_str!("../../static/favicon.svg");
pub use crate::common::OG_IMAGE_PATH;

// ---------------------------------------------------------------------------
// State

/// Bound on concurrent public page renders (`public_ui.query_concurrency`,
/// read per request, so the bound is hot).
#[derive(Debug, Default)]
pub struct RenderGate {
    active: Mutex<usize>,
    freed: Notify,
}

/// A held render slot.
#[derive(Debug)]
pub struct RenderSlot<'a> {
    gate: &'a RenderGate,
}

impl Drop for RenderSlot<'_> {
    fn drop(&mut self) {
        let mut a = self.gate.active.lock().unwrap_or_else(|e| e.into_inner());
        *a = a.saturating_sub(1);
        drop(a);
        self.gate.freed.notify_one();
    }
}

impl RenderGate {
    fn try_take(&self, limit: usize) -> Option<RenderSlot<'_>> {
        let mut a = self.active.lock().unwrap_or_else(|e| e.into_inner());
        if *a < limit.max(1) {
            *a += 1;
            Some(RenderSlot { gate: self })
        } else {
            None
        }
    }

    /// Takes a slot, waiting at most `wait` while `limit` are held.
    pub async fn acquire(&self, limit: usize, wait: Duration) -> Option<RenderSlot<'_>> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let freed = self.freed.notified();
            tokio::pin!(freed);
            // Register before checking, so a slot freed in between wakes us.
            freed.as_mut().enable();
            if let Some(s) = self.try_take(limit) {
                return Some(s);
            }
            if tokio::time::timeout_at(deadline, freed).await.is_err() {
                return None;
            }
        }
    }

    /// Slots held now.
    pub fn active(&self) -> usize {
        *self.active.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Why an account is withheld (`farsight_public_ui_withheld_total`). The
/// pages never show which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WithheldReason {
    /// A hidden account status.
    HiddenStatus,
    /// In `public_ui.excluded_dids`.
    OperatorExcluded,
}

impl WithheldReason {
    /// Metric label.
    pub fn label(self) -> &'static str {
        match self {
            WithheldReason::HiddenStatus => "hidden_status",
            WithheldReason::OperatorExcluded => "operator_excluded",
        }
    }
}

/// The operator's exclusion list, as the pages use it.
#[derive(Debug, Clone, Default)]
pub struct Withheld {
    /// The excluded DIDs (exact, from config).
    pub dids: Arc<HashSet<String>>,
    /// `actors.id` of the excluded DIDs that are interned (SQL filters and
    /// counts; refreshed every [`EXCLUDED_REFRESH`]).
    pub ids: Arc<Vec<i64>>,
}

impl Withheld {
    /// Whether the operator excludes `did`.
    pub fn excluded(&self, did: &str) -> bool {
        self.dids.contains(did)
    }

    /// Why `did` with stored `status` (`None`: no `actors` row) is
    /// withheld, if it is.
    pub fn reason(&self, did: &str, status: Option<i16>) -> Option<WithheldReason> {
        if self.excluded(did) {
            Some(WithheldReason::OperatorExcluded)
        } else if status.is_some_and(actor_status::is_hidden) {
            Some(WithheldReason::HiddenStatus)
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct ExcludedCache {
    // Held so the allocation cannot be reused for another config while
    // this entry is compared by pointer.
    config: Arc<LoadedConfig>,
    at: Instant,
    withheld: Withheld,
}

/// A change waiting for the operator's confirmation (§8.6).
#[derive(Debug, Clone)]
pub struct PendingEnable {
    /// When the confirmation page was rendered.
    pub at: Instant,
    /// CSRF token of the admin session that asked.
    pub csrf: String,
    /// What to write once confirmed.
    pub change: crate::public_settings::Change,
}

/// State of the public UI.
#[derive(Debug, Default)]
pub struct PublicState {
    /// Verified handles: the memory layer in front of `handle_cache`.
    pub handles: HandleCache,
    /// Accounts whose handles the warming worker is asked to verify.
    pub warm: warming::WarmQueue,
    /// Render concurrency bound.
    pub render: RenderGate,
    excluded: Mutex<Option<ExcludedCache>>,
    /// Enable requests awaiting confirmation, by confirmation token.
    pub pending: Mutex<HashMap<String, PendingEnable>>,
    /// How far the handle pass and the list filler have got.
    pub progress: pass::Progress,
}

impl PublicState {
    /// The exclusion list for the config in force.
    pub async fn withheld(
        &self,
        pool: &sqlx::PgPool,
        cfg: &Arc<LoadedConfig>,
    ) -> Result<Withheld, sqlx::Error> {
        {
            let g = self.excluded.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(c) = g.as_ref() {
                if Arc::ptr_eq(&c.config, cfg) && c.at.elapsed() < EXCLUDED_REFRESH {
                    return Ok(c.withheld.clone());
                }
            }
        }
        let list = &cfg.config.public_ui.excluded_dids;
        let ids = if list.is_empty() {
            Vec::new()
        } else {
            let mut conn = pool.acquire().await?;
            farsight_storage::public::actor_ids(&mut conn, list)
                .await
                .map_err(|e| match e {
                    farsight_storage::StorageError::Db(e) => e,
                    other => sqlx::Error::Protocol(other.to_string()),
                })?
        };
        let withheld = Withheld {
            dids: Arc::new(list.iter().cloned().collect()),
            ids: Arc::new(ids),
        };
        *self.excluded.lock().unwrap_or_else(|e| e.into_inner()) = Some(ExcludedCache {
            config: cfg.clone(),
            at: Instant::now(),
            withheld: withheld.clone(),
        });
        Ok(withheld)
    }
}

// ---------------------------------------------------------------------------
// Layout

/// The OpenGraph and Twitter tags of a page. They carry no data: platforms
/// keep previews for days, and a count in a preview is a stale claim with
/// no date and no coverage.
#[derive(Debug, Clone, Template)]
#[template(path = "_opengraph.html")]
pub struct OpenGraph {
    /// `og:title`.
    pub title: String,
    /// `og:description`.
    pub description: &'static str,
    /// `og:url`: `https://{server.hostname}{path}`, never from a header.
    pub url: String,
    /// `og:site_name`.
    pub site: String,
    /// `og:image`, unless `show_opengraph_image` is off.
    pub image: Option<String>,
}

/// Fixed preview text of an account page.
pub const OG_ACCOUNT: &str = "Block records for this account as indexed by this Farsight \
                              instance. Open the page for current data.";
/// Fixed preview text of a list page.
pub const OG_LIST: &str = "Block records for this list as indexed by this Farsight instance. \
                           Open the page for current data.";
/// Fixed preview text of the other pages.
pub const OG_INSTANCE: &str = "An independent index of public block records on the AT Protocol \
                               network. Open the page for current data.";

/// What the shared layout needs.
#[derive(Debug, Clone)]
pub struct Chrome {
    /// `public_ui.dark_mode_default`: `light`, `dark` or `system`.
    pub theme: &'static str,
    /// Preview tags.
    pub og: OpenGraph,
    /// The home page: its bar has no search form and no guide button,
    /// because the page itself has both.
    pub home: bool,
}

/// Builds the layout data of a page at `path` (no query string).
pub fn chrome(cfg: &Config, og_title: &str, og_text: &'static str, path: &str) -> Chrome {
    let host = &cfg.server.hostname;
    Chrome {
        theme: cfg.public_ui.dark_mode_default.as_str(),
        og: OpenGraph {
            title: og_title.to_owned(),
            description: og_text,
            url: format!("https://{host}{path}"),
            site: format!("Farsight at {host}"),
            image: cfg
                .public_ui
                .show_opengraph_image
                .then(|| format!("https://{host}{OG_IMAGE_PATH}")),
        },
        home: false,
    }
}

// ---------------------------------------------------------------------------
// Responses

/// How a response may be cached (§9.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cache {
    /// `public, max-age=<s>`.
    Public(u32),
    /// `no-store`.
    NoStore,
    /// `no-store, private`: an answer made for a signed-in admin.
    Private,
}

/// A response that is not the page asked for.
#[derive(Debug)]
pub enum Fail {
    /// The route does not exist for this configuration.
    Absent,
    /// Rate limited; `Retry-After` seconds.
    Limited(u64),
    /// Unparseable path, query or cursor.
    Bad {
        /// What was wrong.
        message: String,
        /// A way forward: (href, text).
        link: Option<(String, String)>,
    },
    /// Nothing is known under this address.
    NotFound {
        /// Heading.
        title: String,
        /// Message.
        message: String,
        /// A way forward.
        link: Option<(String, String)>,
    },
    /// Render queue full or a query timed out.
    Busy,
    /// Anything else; details are logged.
    Internal,
    /// A failure page the handler rendered itself.
    Rendered(Box<Response>),
}

impl From<sqlx::Error> for Fail {
    fn from(e: sqlx::Error) -> Fail {
        Fail::from(farsight_api::error::XrpcError::from(e))
    }
}

impl From<farsight_storage::StorageError> for Fail {
    fn from(e: farsight_storage::StorageError) -> Fail {
        Fail::from(farsight_api::error::XrpcError::from(e))
    }
}

impl From<farsight_api::error::XrpcError> for Fail {
    fn from(e: farsight_api::error::XrpcError) -> Fail {
        match e.status {
            StatusCode::BAD_REQUEST => Fail::Bad {
                message: "This link carries a position that can no longer be read.".into(),
                link: None,
            },
            StatusCode::SERVICE_UNAVAILABLE => Fail::Busy,
            _ => Fail::Internal,
        }
    }
}

/// A plain message page.
#[derive(Template)]
#[template(path = "public_message.html")]
struct MessagePage {
    c: Chrome,
    title: String,
    message: String,
    link: Option<(String, String)>,
}

fn robots_tag(h: &mut axum::http::HeaderMap, cfg: &Config, indexable: bool) {
    if !(indexable && cfg.public_ui.crawlable) {
        h.insert(
            "x-robots-tag",
            HeaderValue::from_static("noindex, nofollow"),
        );
    }
}

/// Finishes a public response: cache class, robots tag and the fixed
/// security headers.
pub fn finish(mut r: Response, cfg: &Config, cache: Cache, indexable: bool) -> Response {
    let h = r.headers_mut();
    let cc = match cache {
        Cache::Public(age) => format!("public, max-age={age}"),
        Cache::NoStore => "no-store".to_owned(),
        Cache::Private => crate::common::NO_STORE.to_owned(),
    };
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_str(&cc).expect("ascii"),
    );
    robots_tag(h, cfg, indexable);
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(csp(cfg)),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("same-origin"),
    );
    r
}

/// Renders a page template with `status`.
pub fn page<T: Template>(
    t: &T,
    status: StatusCode,
    cfg: &Config,
    cache: Cache,
    indexable: bool,
) -> Response {
    let mut r = crate::common::render(t);
    if r.status() == StatusCode::OK {
        *r.status_mut() = status;
        finish(r, cfg, cache, indexable)
    } else {
        // The template failed to render: an error, never cached.
        finish(r, cfg, Cache::NoStore, false)
    }
}

fn message_page(
    cfg: &Config,
    status: StatusCode,
    title: &str,
    message: &str,
    link: Option<(String, String)>,
) -> Response {
    let t = MessagePage {
        c: chrome(
            cfg,
            &format!("Farsight at {}", cfg.server.hostname),
            OG_INSTANCE,
            "/",
        ),
        title: title.to_owned(),
        message: message.to_owned(),
        link,
    };
    // Every 4xx and 5xx is `no-store` and never offered to search engines.
    page(&t, status, cfg, Cache::NoStore, false)
}

/// The response for a [`Fail`].
pub fn fail(cfg: &Config, f: Fail) -> Response {
    let home = || Some(("/".to_owned(), "Back to search".to_owned()));
    match f {
        Fail::Absent => crate::common::not_found(),
        Fail::Limited(retry) => {
            let mut r = message_page(
                cfg,
                StatusCode::TOO_MANY_REQUESTS,
                "Too many requests",
                "Too many requests from your address. Wait a few seconds and try again.",
                None,
            );
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(retry.max(1)));
            r
        }
        Fail::Bad { message, link } => message_page(
            cfg,
            StatusCode::BAD_REQUEST,
            "That address cannot be read",
            &message,
            link.or_else(home),
        ),
        Fail::NotFound {
            title,
            message,
            link,
        } => message_page(
            cfg,
            StatusCode::NOT_FOUND,
            &title,
            &message,
            link.or_else(home),
        ),
        Fail::Busy => {
            let mut r = message_page(
                cfg,
                StatusCode::SERVICE_UNAVAILABLE,
                "Busy",
                "This instance is busy. Try again in a moment.",
                None,
            );
            r.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(1u64));
            r
        }
        Fail::Internal => message_page(
            cfg,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Something went wrong",
            "Something went wrong. The details were logged for the operator.",
            None,
        ),
        Fail::Rendered(r) => *r,
    }
}

/// A relative redirect to a public page (§8.5), `no-store`.
pub fn redirect(cfg: &Config, to: &str) -> Response {
    debug_assert!(to.starts_with('/') && !to.starts_with("//"));
    let r = (
        StatusCode::SEE_OTHER,
        [(
            header::LOCATION,
            HeaderValue::from_str(to).expect("ascii path"),
        )],
    )
        .into_response();
    finish(r, cfg, Cache::NoStore, false)
}

// ---------------------------------------------------------------------------
// Gates

/// A request that passed the gates.
pub struct Req<'a> {
    /// Web state.
    pub st: &'a WebState,
    /// The config in force for this request.
    pub cfg: Arc<LoadedConfig>,
}

pub(crate) fn client_key(client: Option<ClientIp>) -> String {
    let ip = client.map_or(IpAddr::from([0, 0, 0, 0]), |c| c.ip);
    farsight_api::ratelimit::ip_key(ip)
}

/// Applies the toggle and the page's rate class, keyed by client address
/// for every caller: public pages do not look at sessions.
fn gate<'a>(
    st: &'a WebState,
    client: Option<ClientIp>,
    class: Class,
) -> Result<Req<'a>, (Arc<LoadedConfig>, Fail)> {
    let cfg = st.api.config.current();
    if !cfg.config.access.public_ui {
        return Err((cfg, Fail::Absent));
    }
    let limit = class.limit(&cfg.config, None);
    if let Err((_, retry)) = st.api.limiter.check(class, &client_key(client), limit) {
        farsight_api::metrics::rate_limited(class);
        return Err((cfg, Fail::Limited(retry)));
    }
    Ok(Req { st, cfg })
}

impl Req<'_> {
    /// The config.
    pub fn config(&self) -> &Config {
        &self.cfg.config
    }

    /// Takes a render slot and a slot of the global read semaphore for the
    /// page's queries (which run one at a time), each waiting at most 2 s.
    pub async fn render_slots(
        &self,
    ) -> Result<(RenderSlot<'_>, tokio::sync::OwnedSemaphorePermit), Fail> {
        let cfg = self.config();
        let limit = cfg
            .public_ui
            .query_concurrency
            .min(cfg.rate_limit.query_concurrency) as usize;
        let slot = self
            .st
            .public
            .render
            .acquire(limit, RENDER_WAIT)
            .await
            .ok_or(Fail::Busy)?;
        let permit = tokio::time::timeout(
            farsight_api::PERMIT_WAIT,
            self.st.api.query_permits.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Fail::Busy)?
        .map_err(|_| Fail::Busy)?;
        Ok((slot, permit))
    }

    /// The exclusion list.
    pub async fn withheld(&self) -> Result<Withheld, Fail> {
        Ok(self
            .st
            .public
            .withheld(&self.st.api.pool, &self.cfg)
            .await?)
    }
}

pub(crate) fn parse_did(p: Result<Path<String>, PathRejection>) -> Result<Did, Fail> {
    let bad = || Fail::Bad {
        message: "The address does not name a DID. Handles go through search.".into(),
        link: None,
    };
    let Path(s) = p.map_err(|_| bad())?;
    if s.len() > MAX_DID_SEGMENT {
        return Err(bad());
    }
    Did::parse(&s).map_err(|_| bad())
}

fn parse_list(p: Result<Path<(String, String)>, PathRejection>) -> Result<(Did, RecordKey), Fail> {
    let bad = || Fail::Bad {
        message: "The address does not name a list: it needs the owner's DID and the list's key."
            .into(),
        link: None,
    };
    let Path((d, r)) = p.map_err(|_| bad())?;
    if d.len() > MAX_DID_SEGMENT {
        return Err(bad());
    }
    Ok((
        Did::parse(&d).map_err(|_| bad())?,
        RecordKey::parse(&r).map_err(|_| bad())?,
    ))
}

/// Runs one public request: gates, the page, the failure page, metrics.
async fn serve<'a, F, Fut>(
    st: &'a WebState,
    page: Page,
    client: Option<axum::Extension<ClientIp>>,
    class: Class,
    f: F,
) -> Response
where
    F: FnOnce(Req<'a>) -> Fut,
    Fut: std::future::Future<Output = Result<Response, Fail>> + 'a,
{
    let started = Instant::now();
    let resp = match gate(st, client.map(|c| c.0), class) {
        Err((cfg, f)) => fail(&cfg.config, f),
        Ok(req) => {
            let cfg = req.cfg.clone();
            match f(req).await {
                Ok(r) => r,
                Err(f) => fail(&cfg.config, f),
            }
        }
    };
    metrics::observe(page, resp.status(), started.elapsed());
    resp
}

// ---------------------------------------------------------------------------
// Routes

type St = State<Arc<WebState>>;
type Client = Option<axum::Extension<ClientIp>>;

/// The text page at `/` of an instance with neither UI (§8.6).
const API_ONLY: &str = "Farsight\n\nThis instance exposes an ATProto block-graph API.\nSee \
                        https://atproto.com for protocol details.\n";

/// `GET /`: the public home while the public UI is on; else the way to
/// the dashboard while the admin UI is on; else a few lines of text, so
/// that an API-only instance does not look broken. Decided per request.
/// Only the first is a public UI request (rate class, metrics).
async fn root(State(st): St, client: Client) -> Response {
    let cfg = st.api.config.current();
    if cfg.config.access.public_ui {
        return serve(&st, Page::Home, client, Class::PublicUi, |r| async move {
            pages::home(&r).await
        })
        .await;
    }
    if cfg.admin_auth() != AdminAuth::Disabled {
        // Not permanent and not stored: turning the public UI on changes
        // what `/` is.
        return crate::common::redirect("/admin");
    }
    (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=300"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        API_ONLY,
    )
        .into_response()
}

async fn search_route(State(st): St, client: Client, RawQuery(q): RawQuery) -> Response {
    serve(&st, Page::Search, client, Class::UiLookup, |r| async move {
        pages::search(&r, &Params::parse(q.as_deref().unwrap_or(""))).await
    })
    .await
}

async fn did_route(
    State(st): St,
    client: Client,
    path: Result<Path<String>, PathRejection>,
    RawQuery(q): RawQuery,
) -> Response {
    serve(&st, Page::Did, client, Class::PublicUi, |r| async move {
        let did = parse_did(path)?;
        pages::did(&r, &did, &Params::parse(q.as_deref().unwrap_or(""))).await
    })
    .await
}

async fn list_route(
    State(st): St,
    client: Client,
    path: Result<Path<(String, String)>, PathRejection>,
    RawQuery(q): RawQuery,
) -> Response {
    serve(&st, Page::List, client, Class::PublicUi, |r| async move {
        let (owner, rkey) = parse_list(path)?;
        pages::list(
            &r,
            &owner,
            &rkey,
            &Params::parse(q.as_deref().unwrap_or("")),
        )
        .await
    })
    .await
}

// The addresses the public pages had under `/public` (§8.6): redirected
// for one release while the public UI is on, then gone. One route per
// page; each target is a fixed prefix plus the matched parameters,
// re-encoded. `/public/card/{did}` is not among them: the script takes no
// redirected card.

/// Answers a request for an old public address: `to` while the public UI
/// is on, the bare 404 otherwise.
fn old_public_path(st: &WebState, to: Option<String>, query: Option<String>) -> Response {
    let started = Instant::now();
    let resp = match to {
        Some(to) if st.api.config.current().config.access.public_ui => {
            crate::common::moved(&to, query.as_deref())
        }
        _ => crate::common::not_found(),
    };
    metrics::observe(Page::Other, resp.status(), started.elapsed());
    resp
}

async fn old_home(State(st): St, RawQuery(q): RawQuery) -> Response {
    old_public_path(&st, Some("/".to_owned()), q)
}

async fn old_search(State(st): St, RawQuery(q): RawQuery) -> Response {
    old_public_path(&st, Some("/search".to_owned()), q)
}

async fn old_did(
    State(st): St,
    path: Result<Path<String>, PathRejection>,
    RawQuery(q): RawQuery,
) -> Response {
    old_public_path(&st, path.ok().map(|Path(did)| text::did_href(&did)), q)
}

async fn old_list(
    State(st): St,
    path: Result<Path<(String, String)>, PathRejection>,
    RawQuery(q): RawQuery,
) -> Response {
    let to = path
        .ok()
        .map(|Path((did, rkey))| text::list_href(&did, &rkey));
    old_public_path(&st, to, q)
}

async fn old_css(State(st): St) -> Response {
    old_public_path(&st, Some("/static/public.css".to_owned()), None)
}

async fn old_js(State(st): St) -> Response {
    old_public_path(&st, Some("/static/public.js".to_owned()), None)
}

async fn old_htmx(State(st): St) -> Response {
    old_public_path(&st, Some("/static/htmx.min.js".to_owned()), None)
}

async fn old_og_image(State(st): St) -> Response {
    old_public_path(&st, Some(OG_IMAGE_PATH.to_owned()), None)
}

/// Anything else under `/public/`: the public not-found page while the
/// public UI is on, with no redirect. That covers the paths withdrawn in
/// r21 (`/public/about` and the two history pages — their new home is
/// behind login, and a redirect would name it) and `/public/card/{did}`.
async fn other(State(st): St) -> Response {
    let started = Instant::now();
    let cfg = st.api.config.current();
    let resp = if cfg.config.access.public_ui {
        fail(
            &cfg.config,
            Fail::NotFound {
                title: "Not found".into(),
                message: "There is no page at this address.".into(),
                link: None,
            },
        )
    } else {
        crate::common::not_found()
    };
    metrics::observe(Page::Other, resp.status(), started.elapsed());
    resp
}

/// What crawlers may not fetch while the public UI is on and
/// `crawlable`: the admin UI and sign-in, the wizard, the API, search and
/// the card fragments (old addresses included), the health endpoints.
/// The old `/public/…` page addresses are left open on purpose, so that a
/// crawler sees their redirects. `Allow` comes last for crawlers that
/// take the first match rather than the longest.
const ROBOTS_CRAWLABLE: &str = "User-agent: *\nDisallow: /admin\nDisallow: /enter\nDisallow: \
                                /setup\nDisallow: /xrpc/\nDisallow: /search\nDisallow: \
                                /card/\nDisallow: /public/search\nDisallow: \
                                /public/card/\nDisallow: /health\nDisallow: /livez\nAllow: /\n";

/// The body of `/robots.txt`: nothing is offered to crawlers unless the
/// public UI is on and `crawlable`.
pub fn robots_body(cfg: &Config) -> &'static str {
    if cfg.access.public_ui && cfg.public_ui.crawlable {
        ROBOTS_CRAWLABLE
    } else {
        "User-agent: *\nDisallow: /\n"
    }
}

/// `GET /robots.txt`: served in every normal-mode configuration, the body
/// chosen from the config in force.
async fn robots(State(st): St) -> Response {
    let started = Instant::now();
    let cfg = st.api.config.current();
    let resp = (
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=300"),
        ],
        robots_body(&cfg.config),
    )
        .into_response();
    metrics::observe(Page::Robots, resp.status(), started.elapsed());
    resp
}

/// The public UI's routes, `/` and `/robots.txt`.
pub fn router() -> Router<Arc<WebState>> {
    Router::new()
        .route("/", get(root))
        .route("/search", get(search_route))
        .route("/did/{did}", get(did_route))
        .route("/list/{did}/{rkey}", get(list_route))
        .route("/card/{did}", get(card::route))
        .route("/public", get(old_home))
        .route("/public/search", get(old_search))
        .route("/public/did/{did}", get(old_did))
        .route("/public/list/{did}/{rkey}", get(old_list))
        .route("/public/static/public.css", get(old_css))
        .route("/public/static/public.js", get(old_js))
        .route("/public/static/htmx.min.js", get(old_htmx))
        .route("/public/static/og-default.png", get(old_og_image))
        .route("/public/{*rest}", get(other))
        .route("/robots.txt", get(robots))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn robots_follow_the_settings() {
        let mut c = Config::default();
        assert_eq!(robots_body(&c), "User-agent: *\nDisallow: /\n");
        c.public_ui.crawlable = true;
        // Crawlable but off: still nothing.
        assert_eq!(robots_body(&c), "User-agent: *\nDisallow: /\n");
        c.access.public_ui = true;
        let b = robots_body(&c);
        let lines: Vec<&str> = b.lines().collect();
        assert_eq!(lines[0], "User-agent: *");
        for closed in [
            "/admin",
            "/enter",
            "/setup",
            "/xrpc/",
            "/search",
            "/card/",
            "/public/search",
            "/public/card/",
            "/health",
            "/livez",
        ] {
            assert!(
                lines.contains(&format!("Disallow: {closed}").as_str()),
                "{closed}"
            );
        }
        // No rule is a prefix of a public page or of an old page address
        // that redirects to one.
        for open in [
            "/",
            "/did/did:plc:x",
            "/list/did:plc:x/k",
            "/static/public.css",
            "/public/did/did:plc:x",
        ] {
            assert!(
                !lines
                    .iter()
                    .filter_map(|l| l.strip_prefix("Disallow: "))
                    .any(|p| open.starts_with(p)),
                "{open}"
            );
        }
        // `Allow` comes after the narrower rules, for first-match parsers.
        assert_eq!(lines.last(), Some(&"Allow: /"));
        // Without the admin UI the body is the same: the paths are closed
        // either way.
        c.access.admin_ui = false;
        assert_eq!(robots_body(&c), b);
    }

    #[test]
    fn one_rule_for_hidden_and_excluded() {
        let w = Withheld {
            dids: Arc::new(["did:plc:excluded".to_owned()].into_iter().collect()),
            ids: Arc::new(vec![7]),
        };
        assert_eq!(
            w.reason("did:plc:excluded", Some(actor_status::ACTIVE)),
            Some(WithheldReason::OperatorExcluded)
        );
        for s in [
            actor_status::DEACTIVATED,
            actor_status::TAKENDOWN,
            actor_status::SUSPENDED,
            actor_status::DELETED,
        ] {
            assert_eq!(
                w.reason("did:plc:other", Some(s)),
                Some(WithheldReason::HiddenStatus)
            );
        }
        for s in [
            actor_status::ACTIVE,
            actor_status::THROTTLED,
            actor_status::DESYNCHRONIZED,
            actor_status::UNKNOWN,
        ] {
            assert_eq!(w.reason("did:plc:other", Some(s)), None);
        }
        // No row: shown (sections render empty with their coverage).
        assert_eq!(w.reason("did:plc:other", None), None);
    }

    #[test]
    fn headers_of_a_public_response() {
        let mut c = Config::default();
        let r = finish(StatusCode::OK.into_response(), &c, Cache::Public(30), true);
        assert_eq!(r.headers()[header::CACHE_CONTROL], "public, max-age=30");
        assert_eq!(r.headers()["x-robots-tag"], "noindex, nofollow");
        // Avatars are on by default: images may come from https origins.
        assert_eq!(r.headers()[header::CONTENT_SECURITY_POLICY], CSP_AVATARS);
        assert!(CSP_AVATARS.contains("img-src 'self' https:;"));
        assert!(CSP_AVATARS.contains("script-src 'self';") && !CSP_AVATARS.contains("unsafe"));
        c.public_ui.show_avatars = false;
        let r = finish(StatusCode::OK.into_response(), &c, Cache::Public(30), true);
        assert_eq!(r.headers()[header::CONTENT_SECURITY_POLICY], CSP);
        assert!(CSP.contains("img-src 'self';"));
        assert_eq!(r.headers()[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
        assert_eq!(r.headers()[header::REFERRER_POLICY], "same-origin");
        c.public_ui.crawlable = true;
        let r = finish(StatusCode::OK.into_response(), &c, Cache::Public(30), true);
        assert!(r.headers().get("x-robots-tag").is_none());
        // Search, cards and errors are never offered, on any setting.
        let r = finish(StatusCode::OK.into_response(), &c, Cache::NoStore, false);
        assert_eq!(r.headers()["x-robots-tag"], "noindex, nofollow");
        assert_eq!(r.headers()[header::CACHE_CONTROL], "no-store");
    }

    #[tokio::test]
    async fn render_gate_bounds_and_releases() {
        let g = RenderGate::default();
        let a = g.acquire(2, Duration::from_millis(10)).await.unwrap();
        let _b = g.acquire(2, Duration::from_millis(10)).await.unwrap();
        assert!(g.acquire(2, Duration::from_millis(20)).await.is_none());
        assert_eq!(g.active(), 2);
        drop(a);
        assert!(g.acquire(2, Duration::from_millis(20)).await.is_some());
        assert_eq!(g.active(), 1);
    }
}
