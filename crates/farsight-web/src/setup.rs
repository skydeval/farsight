//! Setup mode (design §8.2–§8.5): the setup-token gate and the ten-step
//! wizard. Nothing here touches the database except the storage step's
//! connection test; completion writes `config.toml` (first writer wins),
//! deletes the token and signals the server to switch to normal mode
//! in-process.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use askama::Template;
use axum::Router;
use axum::extract::{Form, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::Utc;
use farsight_api::clientip::{self, ClientIp, OriginalForwarding};
use farsight_core::config::{
    AccessConfig, Config, ProxyMode, ReadsMode, SweepSource, validate_trusted_proxy,
};
use ipnet::IpNet;
use tokio::sync::{Semaphore, watch};

use crate::common::{self, cookie, ct_eq, random_id, read_cookie, render_private};
use crate::setup_token::{self, SetupToken};

/// Setup session cookie (§8.3).
pub const SESSION_COOKIE: &str = "farsight_setup";
/// Failures per minute from one client before responses are delayed.
pub const FAILURES_PER_MIN: usize = 5;
/// Delay of a throttled failure.
pub const FAILURE_DELAY: Duration = Duration::from_secs(2);
/// Delayed responses held at once (beyond: immediate 429).
pub const MAX_DELAYED: usize = 64;

/// The ten steps (§8.4), by URL slug.
pub const STEPS: [(&str, &str); 10] = [
    ("token", "Setup token"),
    ("welcome", "Welcome"),
    ("identity", "Public identity"),
    ("firehose", "Firehose source"),
    ("backfill", "Backfill"),
    ("access", "Access"),
    ("proxy", "Reverse proxy"),
    ("storage", "Storage"),
    ("review", "Review"),
    ("done", "Done"),
];

/// The wizard's answers, held server-side per session until the final
/// write (§8.4).
#[derive(Debug, Clone)]
pub struct Wizard {
    /// Steps validated so far (by index).
    pub done: [bool; 10],
    /// `server.hostname`.
    pub hostname: String,
    /// `server.contact`.
    pub contact: String,
    /// `firehose.urls`.
    pub urls: Vec<String>,
    /// Last "Test connection" report.
    pub firehose_test: Vec<String>,
    /// Some tested instance offered v2.
    pub v2_seen: bool,
    /// `backfill.sweep.enabled`.
    pub sweep: bool,
    /// `backfill.sweep.source`.
    pub source: String,
    /// `backfill.per_host_rps`.
    pub per_host_rps: u32,
    /// `backfill.concurrency`.
    pub concurrency: u32,
    /// `backfill.sweep.max_repos_per_hour`.
    pub max_repos_per_hour: u64,
    /// `backfill.plc_url`.
    pub plc_url: String,
    /// `backfill.plc_seed_from_export`.
    pub plc_seed: bool,
    /// Disk available to Postgres, GB.
    pub disk_gb: u64,
    /// `backfill.backlinks.url`.
    pub backlinks_url: String,
    /// `access.reads`.
    pub reads: ReadsMode,
    /// `access.public_ui`.
    pub public_ui: bool,
    /// The operator confirmed, on the page that lists what becomes
    /// public, that the public UI is to be on.
    pub public_confirmed: bool,
    /// `access.admin_ui`.
    pub admin_ui: bool,
    /// `access.admin_did`, as entered.
    pub admin_did: String,
    /// The DID the access step last looked up, and what came back: the
    /// identity, or why it did not resolve.
    pub admin_seen: Option<(String, Result<crate::oauth::Identity, String>)>,
    /// The admin DID the operator confirmed (after seeing what it
    /// resolves to, or by "use anyway").
    pub admin_confirmed: Option<String>,
    /// The admin token, generated once per session, shown until saved.
    pub admin_token: String,
    /// The operator confirmed saving the admin token.
    pub admin_token_saved: bool,
    /// Proxy preset: `none`, `cloudflare`, `local`, `custom`.
    pub proxy_choice: String,
    /// `proxy.mode`.
    pub proxy_mode: ProxyMode,
    /// `proxy.trusted`.
    pub trusted: Vec<IpNet>,
    /// Public ranges outside the Cloudflare set were acknowledged.
    pub proxy_ack: bool,
    /// `storage.database_url`.
    pub dsn: String,
    /// Last storage test report.
    pub storage_report: Vec<String>,
    /// The storage test passed for `dsn`.
    pub storage_ok: bool,
}

impl Wizard {
    fn new(env: &[(String, String)]) -> Wizard {
        let d = Config::default();
        let env_dsn = env
            .iter()
            .find(|(k, _)| k == "FARSIGHT__STORAGE__DATABASE_URL")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        Wizard {
            done: [false; 10],
            hostname: String::new(),
            contact: String::new(),
            urls: d.firehose.urls.clone(),
            firehose_test: Vec::new(),
            v2_seen: false,
            sweep: d.backfill.sweep.enabled,
            source: "relay_collections".into(),
            per_host_rps: d.backfill.per_host_rps,
            concurrency: d.backfill.concurrency,
            max_repos_per_hour: d.backfill.sweep.max_repos_per_hour,
            plc_url: d.backfill.plc_url.clone(),
            plc_seed: d.backfill.plc_seed_from_export,
            disk_gb: 500,
            backlinks_url: String::new(),
            reads: ReadsMode::Public,
            // Both web interfaces are off until the operator ticks them.
            public_ui: false,
            public_confirmed: false,
            admin_ui: false,
            admin_did: String::new(),
            admin_seen: None,
            admin_confirmed: None,
            admin_token: farsight_api::auth::generate(farsight_api::auth::ADMIN_PREFIX),
            admin_token_saved: false,
            proxy_choice: "none".into(),
            proxy_mode: ProxyMode::None,
            trusted: Vec::new(),
            proxy_ack: false,
            dsn: env_dsn,
            storage_report: Vec::new(),
            storage_ok: false,
        }
    }

    /// `storage.budget_bytes`: 70% of the disk entered (§8.4 step 5).
    pub fn budget_bytes(&self) -> u64 {
        self.disk_gb.saturating_mul(1_000_000_000) / 10 * 7
    }

    /// The config this wizard would write.
    pub fn build_config(&self) -> Config {
        let mut c = Config::default();
        c.server.hostname = self.hostname.clone();
        c.server.contact = self.contact.clone();
        c.storage.database_url = self.dsn.clone();
        c.storage.budget_bytes = self.budget_bytes();
        c.firehose.urls = self.urls.clone();
        c.backfill.sweep.enabled = self.sweep;
        c.backfill.sweep.source = match self.source.as_str() {
            "relay_repos" => SweepSource::RelayRepos,
            "plc" => SweepSource::Plc,
            _ => SweepSource::RelayCollections,
        };
        c.backfill.per_host_rps = self.per_host_rps;
        c.backfill.concurrency = self.concurrency;
        c.backfill.sweep.max_repos_per_hour = self.max_repos_per_hour;
        c.backfill.plc_url = self.plc_url.clone();
        c.backfill.plc_seed_from_export = self.plc_seed;
        c.backfill.backlinks.url = self.backlinks_url.clone();
        c.access = AccessConfig {
            reads: self.reads,
            cors: c.access.cors,
            // Only with the operator's confirmation (§8.4 step 6).
            public_ui: self.public_ui && self.public_confirmed,
            admin_ui: self.admin_ui,
            admin_did: if self.admin_ui {
                self.admin_did.clone()
            } else {
                String::new()
            },
        };
        c.auth.admin_token_sha256 =
            farsight_api::auth::hex(&farsight_api::auth::sha256(&self.admin_token));
        c.proxy.mode = self.proxy_mode;
        c.proxy.trusted = self.trusted.clone();
        c
    }
}

/// One setup session.
#[derive(Debug)]
pub struct SetupSession {
    /// Last request.
    pub last_seen: Instant,
    /// CSRF token for its forms.
    pub csrf: String,
    /// The wizard.
    pub wizard: Wizard,
}

/// Setup-mode state.
#[derive(Debug)]
pub struct SetupState {
    /// `config.toml` path.
    pub config_path: PathBuf,
    /// `.setup-token` path.
    pub token_path: PathBuf,
    /// Environment captured at start-up.
    pub env: Vec<(String, String)>,
    /// Verified sessions by hashed id.
    pub sessions: Mutex<HashMap<[u8; 32], SetupSession>>,
    /// Recent token failures per resolved client.
    pub failures: Mutex<HashMap<IpAddr, VecDeque<Instant>>>,
    /// Bound on delayed failure responses.
    pub delayed: Arc<Semaphore>,
    /// Set to true once `config.toml` is written.
    pub completed: watch::Sender<bool>,
    /// Binary version.
    pub version: &'static str,
}

impl SetupState {
    /// A new state.
    pub fn new(
        config_path: PathBuf,
        env: Vec<(String, String)>,
        version: &'static str,
    ) -> (Arc<SetupState>, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        let token_path = setup_token::token_path(&config_path);
        (
            Arc::new(SetupState {
                config_path,
                token_path,
                env,
                sessions: Mutex::new(HashMap::new()),
                failures: Mutex::new(HashMap::new()),
                delayed: Arc::new(Semaphore::new(MAX_DELAYED)),
                completed: tx,
                version,
            }),
            rx,
        )
    }

    /// Whether a verified session was active in the last hour (postpones
    /// rotation, §8.3).
    pub fn session_active(&self) -> bool {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .any(|s| s.last_seen.elapsed() < setup_token::ACTIVE_WINDOW)
    }

    /// The token in force, rotating it if expired (a new token is printed
    /// and every setup session ends, §8.3). Returns whether it rotated.
    /// Called at boot and every minute.
    pub fn check_token(&self) -> std::io::Result<(SetupToken, bool)> {
        let (t, rotated) = setup_token::current_or_rotate(&self.token_path, self.session_active())?;
        if rotated {
            self.sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
        }
        Ok((t, rotated))
    }

    fn session_id(&self, headers: &HeaderMap) -> Option<[u8; 32]> {
        let raw = read_cookie(headers, SESSION_COOKIE)?;
        let id = common::sha256(&raw);
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        let s = sessions.get_mut(&id)?;
        s.last_seen = Instant::now();
        Some(id)
    }

    fn with_session<R>(&self, id: &[u8; 32], f: impl FnOnce(&mut SetupSession) -> R) -> Option<R> {
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions.get_mut(id).map(f)
    }

    fn record_failure(&self, ip: IpAddr) -> usize {
        let mut f = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        let q = f.entry(ip).or_default();
        let now = Instant::now();
        while q
            .front()
            .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60))
        {
            q.pop_front();
        }
        q.push_back(now);
        q.len()
    }
}

/// The setup router: `/setup/*` and static assets.
pub fn router(state: Arc<SetupState>) -> Router {
    Router::new()
        .route("/", get(|| async { common::redirect("/setup") }))
        .route("/setup", get(entry).post(submit_token))
        .route("/setup/firehose/test", post(firehose_test))
        .route("/setup/storage/test", post(storage_test))
        .route("/setup/proxy/preview", post(proxy_preview))
        .route("/setup/finish", post(finish))
        .route("/setup/{step}", get(show_step).post(save_step))
        .route("/static/farsight.css", get(common::css))
        .route("/static/htmx.min.js", get(common::htmx))
        .with_state(state)
}

/// A row of the step navigation.
#[derive(Debug, Clone)]
pub struct StepNav {
    /// Slug.
    pub slug: &'static str,
    /// Title.
    pub title: &'static str,
    /// `cur`, `done` or empty.
    pub class: &'static str,
}

/// The §10 projection table rows: (stage, data + indexes, with overhead).
pub const PROJECTION: [(&str, &str, &str); 4] = [
    ("Day one", "< 100 MB", "< 150 MB"),
    ("30 days, firehose only", "~2–8 GB", "~2.5–10 GB"),
    ("First sweep cycle complete", "~22–42 GB", "~28–52 GB"),
    ("Growth per year after", "~14–15 GB", "~17–19 GB"),
];

/// The setup page (one template for every step).
#[derive(Template)]
#[template(path = "setup.html")]
pub struct SetupPage {
    /// Current step slug.
    pub step: &'static str,
    /// Step title.
    pub title: &'static str,
    /// Navigation.
    pub nav: Vec<StepNav>,
    /// CSRF token.
    pub csrf: String,
    /// Error to show.
    pub error: Option<String>,
    /// Token expiry warning.
    pub expiry_warning: Option<String>,
    /// The wizard (empty before the token step).
    pub w: Wizard,
    /// Show the admin token (until saved).
    pub show_token: bool,
    /// Disk warning (§8.4 step 5).
    pub disk_warning: Option<String>,
    /// Budget, formatted.
    pub budget_text: String,
    /// Proxy preview lines.
    pub preview: Vec<String>,
    /// Trusted CIDRs, one per line.
    pub trusted_text: String,
    /// Firehose URLs, one per line.
    pub urls_text: String,
    /// Review TOML (secrets redacted).
    pub review: String,
    /// Show, in place of the access step's form, the page that lists
    /// what the public UI makes public and asks for confirmation.
    pub confirm_public: bool,
    /// Version.
    pub version: &'static str,
    /// The bundled Cloudflare set's date.
    pub cf_as_of: &'static str,
}

impl SetupPage {
    /// The §10 projection table.
    pub fn projection(&self) -> &'static [(&'static str, &'static str, &'static str)] {
        &PROJECTION
    }

    /// Whether `r` is the selected reads mode.
    pub fn reads_is(&self, r: &str) -> bool {
        reads_name(self.w.reads) == r
    }

    /// Whether `m` is the selected proxy mode.
    pub fn mode_is(&self, m: &str) -> bool {
        mode_name(self.w.proxy_mode) == m
    }

    /// What the entered admin DID resolved to, in words, when the access
    /// step has looked it up and found it.
    pub fn admin_found(&self) -> Option<String> {
        match self.w.admin_seen.as_ref()? {
            (did, Ok(i)) if *did == self.w.admin_did => Some(format!(
                "{}, hosted at {}",
                i.handle
                    .as_ref()
                    .map_or_else(|| "no handle".to_owned(), |h| h.to_string()),
                i.pds.as_deref().unwrap_or("no PDS named in its document")
            )),
            _ => None,
        }
    }

    /// Why the entered admin DID did not resolve, when it did not.
    pub fn admin_not_found(&self) -> Option<&str> {
        match self.w.admin_seen.as_ref()? {
            (did, Err(e)) if *did == self.w.admin_did => Some(e.as_str()),
            _ => None,
        }
    }

    /// Whether the entered admin DID is confirmed.
    pub fn admin_confirmed(&self) -> bool {
        !self.w.admin_did.is_empty()
            && self.w.admin_confirmed.as_deref() == Some(self.w.admin_did.as_str())
    }
}

/// Wire name of a reads mode.
pub fn reads_name(r: ReadsMode) -> &'static str {
    match r {
        ReadsMode::Public => "public",
        ReadsMode::ApiKey => "api_key",
        ReadsMode::Disabled => "disabled",
    }
}

/// Wire name of a proxy mode.
pub fn mode_name(m: ProxyMode) -> &'static str {
    match m {
        ProxyMode::None => "none",
        ProxyMode::Cloudflare => "cloudflare",
        ProxyMode::Forwarded => "forwarded",
    }
}

fn nav(cur: &str, done: &[bool; 10]) -> Vec<StepNav> {
    STEPS
        .iter()
        .enumerate()
        .map(|(i, (slug, title))| StepNav {
            slug,
            title,
            class: if *slug == cur {
                "cur"
            } else if done[i] {
                "done"
            } else {
                ""
            },
        })
        .collect()
}

fn step_index(slug: &str) -> Option<usize> {
    STEPS.iter().position(|(s, _)| *s == slug)
}

/// Redacts secrets in a config TOML for display (§8.4 step 9).
pub fn redacted_toml(c: &Config) -> String {
    let mut c = c.clone();
    if !c.auth.admin_token_sha256.is_empty() {
        c.auth.admin_token_sha256 = "<redacted>".into();
    }
    if !c.metrics.bearer_token_sha256.is_empty() {
        c.metrics.bearer_token_sha256 = "<redacted>".into();
    }
    c.storage.database_url = redact_dsn(&c.storage.database_url);
    farsight_core::config::to_toml(&c).unwrap_or_default()
}

/// Hides the password of a Postgres URL.
pub fn redact_dsn(dsn: &str) -> String {
    match url::Url::parse(dsn) {
        Ok(mut u) if u.password().is_some() => {
            let _ = u.set_password(Some("redacted"));
            u.to_string()
        }
        _ => dsn.to_owned(),
    }
}

fn expiry_warning(state: &SetupState) -> Option<String> {
    let t = setup_token::read(&state.token_path)?;
    let now = Utc::now();
    let exp = setup_token::expires_at(&t, now, state.session_active());
    let left = (exp - now).to_std().unwrap_or_default();
    (left < setup_token::WARN_BEFORE).then(|| {
        format!(
            "The setup token expires in {} unless setup stays active. Finish setup, or get the \
             new token from the logs afterwards.",
            common::human_secs(left.as_secs() as i64)
        )
    })
}

fn disk_warning(w: &Wizard) -> Option<String> {
    if !w.sweep || w.disk_gb >= 150 {
        return None;
    }
    let budget = w.budget_bytes() as f64 / 1e9;
    let pause = budget * 0.9;
    let (lo, hi) = (budget - 52.0, budget - 28.0);
    let runway = if hi <= 0.0 {
        "the first sweep may not complete within this budget".to_owned()
    } else {
        format!(
            "after a completed first sweep (~28–52 GB) about {:.1}–{:.1} years of growth \
             (~17–19 GB/year) remain before the budget gate engages",
            (lo.max(0.0)) / 19.0,
            hi / 17.0
        )
    };
    Some(format!(
        "{} GB is below the recommended 150 GB with the sweep on. The storage budget will be \
         {budget:.0} GB: the sweep pauses at 90% ({pause:.0} GB) and new list admissions are \
         deferred at 100%; {runway}.",
        w.disk_gb
    ))
}

/// A copy of a session's wizard and CSRF token, taken without holding the
/// sessions lock while a page is built.
fn snapshot(state: &SetupState, id: &[u8; 32]) -> Option<(Wizard, String)> {
    state.with_session(id, |s| (s.wizard.clone(), s.csrf.clone()))
}

fn page(
    state: &SetupState,
    step: &'static str,
    sess: Option<(Wizard, String)>,
    error: Option<String>,
) -> SetupPage {
    let has_session = sess.is_some();
    let (w, csrf) = sess.unwrap_or_else(|| (Wizard::new(&state.env), String::new()));
    let title = STEPS
        .iter()
        .find(|(s, _)| *s == step)
        .map_or("", |(_, t)| t);
    let review = if step == "review" {
        redacted_toml(&w.build_config())
    } else {
        String::new()
    };
    SetupPage {
        step,
        title,
        nav: nav(step, &w.done),
        csrf,
        error,
        expiry_warning: if has_session {
            expiry_warning(state)
        } else {
            None
        },
        show_token: !w.admin_token_saved,
        disk_warning: disk_warning(&w),
        budget_text: common::human_bytes(w.budget_bytes()),
        preview: Vec::new(),
        trusted_text: w
            .trusted
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
        urls_text: w.urls.join("\n"),
        review,
        confirm_public: false,
        version: state.version,
        cf_as_of: farsight_core::cloudflare::BUNDLED_AS_OF,
        w,
    }
}

fn setup_response(p: &SetupPage) -> Response {
    render_private(p)
}

async fn entry(State(st): State<Arc<SetupState>>, headers: HeaderMap) -> Response {
    match st.session_id(&headers) {
        Some(id) => {
            let next = st
                .with_session(&id, |s| {
                    (1..8)
                        .find(|i| !s.wizard.done[*i])
                        .map_or("review", |i| STEPS[i].0)
                })
                .unwrap_or("welcome");
            common::redirect(&format!("/setup/{next}"))
        }
        None => setup_response(&page(&st, "token", None, None)),
    }
}

async fn submit_token(
    State(st): State<Arc<SetupState>>,
    headers: HeaderMap,
    client: Option<axum::Extension<ClientIp>>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    if !common::same_origin(&headers) {
        return common::forbidden("cross-origin request refused");
    }
    let submitted = form.get("token").map(String::as_str).unwrap_or("");
    let token = match st.check_token() {
        Ok((t, _)) => t,
        Err(e) => {
            tracing::error!(error = %e, "setup token file unavailable");
            return (StatusCode::INTERNAL_SERVER_ERROR, "setup token unavailable").into_response();
        }
    };
    // The submitted token is always checked first, in constant time; a
    // correct token is never refused by any limiter (§8.3).
    if setup_token::matches(submitted, &token) {
        let raw = random_id();
        let id = common::sha256(&raw);
        let wizard = Wizard::new(&st.env);
        let mut wizard = wizard;
        wizard.done[0] = true;
        st.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                id,
                SetupSession {
                    last_seen: Instant::now(),
                    csrf: random_id(),
                    wizard,
                },
            );
        let secure = client.as_ref().is_some_and(|c| c.https);
        let mut r = common::redirect("/setup/welcome");
        r.headers_mut().append(
            header::SET_COOKIE,
            cookie(SESSION_COOKIE, &raw, "/setup", secure, None),
        );
        return r;
    }
    let ip = client.map(|c| c.ip).unwrap_or(IpAddr::from([0, 0, 0, 0]));
    let n = st.record_failure(ip);
    let p = page(
        &st,
        "token",
        None,
        Some("That setup token is not correct.".into()),
    );
    if n > FAILURES_PER_MIN {
        // Throttled: delayed response and sampled logging, bounded so the
        // delay cannot exhaust sockets; no global lockout.
        if n % 10 == 0 {
            tracing::warn!(client = %ip, failures_last_minute = n, "setup token failures");
        }
        match st.delayed.clone().try_acquire_owned() {
            Ok(_permit) => {
                tokio::time::sleep(FAILURE_DELAY).await;
            }
            Err(_) => {
                let mut r = setup_response(&p);
                *r.status_mut() = StatusCode::TOO_MANY_REQUESTS;
                r.headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
                return r;
            }
        }
    }
    let mut r = setup_response(&p);
    *r.status_mut() = StatusCode::UNAUTHORIZED;
    r
}

fn require_session(st: &SetupState, headers: &HeaderMap) -> Result<[u8; 32], Response> {
    st.session_id(headers)
        .ok_or_else(|| common::redirect("/setup"))
}

fn check_csrf(
    st: &SetupState,
    id: &[u8; 32],
    headers: &HeaderMap,
    form: &HashMap<String, String>,
) -> Result<(), Response> {
    if !common::same_origin(headers) {
        return Err(common::forbidden("cross-origin request refused"));
    }
    let ok = st
        .with_session(id, |s| form.get("csrf").is_some_and(|c| ct_eq(c, &s.csrf)))
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(common::forbidden("invalid form token; reload the page"))
    }
}

fn render_step(
    st: &SetupState,
    id: &[u8; 32],
    step: &'static str,
    error: Option<String>,
) -> Response {
    let p = page(st, step, snapshot(st, id), error);
    setup_response(&p)
}

async fn show_step(
    State(st): State<Arc<SetupState>>,
    Path(step): Path<String>,
    headers: HeaderMap,
    client: Option<axum::Extension<ClientIp>>,
    original: Option<axum::Extension<OriginalForwarding>>,
) -> Response {
    let id = match require_session(&st, &headers) {
        Ok(id) => id,
        Err(r) => return r,
    };
    let Some(i) = step_index(&step) else {
        return (StatusCode::NOT_FOUND, "no such step").into_response();
    };
    if i == 0 || i == 9 {
        return common::redirect("/setup");
    }
    // Every step before this one must be done (revisitable until Review).
    let blocked = st
        .with_session(&id, |s| (1..i).find(|j| !s.wizard.done[*j]))
        .flatten();
    if let Some(j) = blocked {
        return common::redirect(&format!("/setup/{}", STEPS[j].0));
    }
    let slug = STEPS[i].0;
    if slug == "proxy" {
        let mut p = page(&st, slug, snapshot(&st, &id), None);
        p.preview = preview_lines(
            client.map(|c| c.0),
            original.map(|o| o.0).unwrap_or_default(),
            p.w.proxy_mode,
            &p.w.trusted,
        );
        return setup_response(&p);
    }
    render_step(&st, &id, slug, None)
}

fn preview_lines(
    client: Option<ClientIp>,
    original: OriginalForwarding,
    mode: ProxyMode,
    trusted: &[IpNet],
) -> Vec<String> {
    let Some(c) = client else {
        return vec!["(no connection information)".into()];
    };
    let mut lines = vec![format!("This request's TCP peer: {}", c.peer)];
    if original.0.is_empty() {
        lines.push("Forwarding headers: none".into());
    } else {
        for (k, v) in &original.0 {
            lines.push(format!("{k}: {v}"));
        }
    }
    let mut h = HeaderMap::new();
    for (k, v) in &original.0 {
        if let (Ok(name), Ok(val)) = (
            axum::http::HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(v),
        ) {
            h.append(name, val);
        }
    }
    let ip = clientip::resolve(c.peer, &h, mode, trusted);
    lines.push(format!(
        "With these settings the client IP would be resolved as: {ip}"
    ));
    lines
}

fn parse_u64(form: &HashMap<String, String>, k: &str, min: u64) -> Result<u64, String> {
    let v = form.get(k).map(|s| s.trim()).unwrap_or("");
    let n: u64 = v
        .replace('_', "")
        .parse()
        .map_err(|_| format!("{k}: expected a whole number"))?;
    if n < min {
        return Err(format!("{k}: must be at least {min}"));
    }
    Ok(n)
}

/// `host[:port]` where host is a hostname, a single label or an IP.
pub fn valid_server_host(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() || s.contains('/') || s.contains("://") {
        return false;
    }
    let host = match s.rsplit_once(':') {
        Some((h, p)) if !h.contains(':') && p.parse::<u16>().is_ok() => h,
        _ => s,
    };
    host.parse::<IpAddr>().is_ok()
        || farsight_core::did::is_valid_hostname(host)
        || (!host.is_empty()
            && host.len() <= 63
            && host.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
}

fn parse_cidrs(text: &str) -> Result<Vec<IpNet>, String> {
    let mut out = Vec::new();
    for line in text
        .split(['\n', ',', ' '])
        .map(str::trim)
        .filter(|l| !l.is_empty())
    {
        let net: IpNet = line
            .parse()
            .or_else(|_| line.parse::<IpAddr>().map(IpNet::from))
            .map_err(|_| format!("{line} is not a CIDR range"))?;
        validate_trusted_proxy(&net)?;
        out.push(net);
    }
    Ok(out)
}

fn public_outside_cf(nets: &[IpNet]) -> Vec<IpNet> {
    nets.iter()
        .filter(|n| {
            let a = n.network();
            let private = match a {
                IpAddr::V4(v) => {
                    v.is_private() || v.is_loopback() || v.is_link_local() || v.octets()[0] == 100
                }
                IpAddr::V6(v) => {
                    let s = v.segments()[0];
                    v.is_loopback() || (s & 0xfe00) == 0xfc00 || (s & 0xffc0) == 0xfe80
                }
            };
            !private && !farsight_core::cloudflare::contains_net(n)
        })
        .copied()
        .collect()
}

fn apply_step(w: &mut Wizard, step: &str, f: &HashMap<String, String>) -> Result<(), String> {
    let get = |k: &str| f.get(k).map(|s| s.trim().to_owned()).unwrap_or_default();
    let on = |k: &str| {
        f.get(k)
            .is_some_and(|v| v == "on" || v == "true" || v == "1")
    };
    match step {
        "welcome" => {}
        "identity" => {
            let host = get("hostname");
            if !valid_server_host(&host) {
                return Err(
                    "Enter the public hostname (e.g. farsight.example.org), without \
                            scheme or path."
                        .into(),
                );
            }
            let contact = get("contact");
            if contact.is_empty() || contact.len() > 300 {
                return Err("Enter an admin contact (e.g. mailto:ops@example.org).".into());
            }
            w.hostname = host;
            w.contact = contact;
        }
        "firehose" => {
            let urls: Vec<String> = get("urls")
                .lines()
                .map(|l| l.trim().trim_end_matches('/').to_owned())
                .filter(|l| !l.is_empty())
                .collect();
            if urls.is_empty() {
                return Err("Enter at least one Jetstream URL.".into());
            }
            if let Some(bad) = urls
                .iter()
                .find(|u| !(u.starts_with("wss://") || u.starts_with("ws://")))
            {
                return Err(format!("{bad} is not a ws:// or wss:// URL."));
            }
            if urls != w.urls {
                w.firehose_test.clear();
                w.v2_seen = false;
            }
            w.urls = urls;
        }
        "backfill" => {
            w.sweep = on("sweep");
            let source = get("source");
            if !["relay_collections", "relay_repos", "plc"].contains(&source.as_str()) {
                return Err("Choose a sweep source.".into());
            }
            w.source = source;
            w.per_host_rps = parse_u64(f, "per_host_rps", 1)? as u32;
            w.concurrency = parse_u64(f, "concurrency", 1)? as u32;
            w.max_repos_per_hour = parse_u64(f, "max_repos_per_hour", 0)?;
            let plc = get("plc_url");
            if !plc.starts_with("https://") && !plc.starts_with("http://") {
                return Err("The PLC directory URL must be http(s)://.".into());
            }
            w.plc_url = plc;
            w.plc_seed = on("plc_seed");
            w.disk_gb = parse_u64(f, "disk_gb", 1)?;
            let bl = get("backlinks_url");
            if !bl.is_empty() && !bl.starts_with("https://") {
                return Err("The backlink source URL must be https://.".into());
            }
            w.backlinks_url = bl;
        }
        "access" => {
            w.reads = match get("reads").as_str() {
                "api_key" => ReadsMode::ApiKey,
                "disabled" => ReadsMode::Disabled,
                _ => ReadsMode::Public,
            };
            // A checkbox that is not ticked is not in the post.
            let public_ui = on("public_ui");
            if public_ui && w.reads != ReadsMode::Public {
                return Err(
                    "The public UI needs public read queries: choose \"Public\" above, or \
                     leave the public UI off."
                        .into(),
                );
            }
            if !public_ui {
                w.public_confirmed = false;
            }
            w.public_ui = public_ui;
            w.admin_ui = on("admin_ui");
            if !w.admin_token_saved && !on("token_saved") {
                return Err("Confirm that you saved the admin token.".into());
            }
            w.admin_token_saved = true;
            if !w.admin_ui {
                // The field is in the form either way; without the admin
                // UI what it holds is not used and not kept.
                w.admin_did.clear();
                w.admin_confirmed = None;
            } else {
                let did = get("admin_did");
                if !farsight_core::config::valid_admin_did(&did) {
                    return Err(
                        "Enter the admin DID: did:plc: followed by 24 characters, or \
                                did:web: followed by a hostname. A handle will not do."
                            .into(),
                    );
                }
                // Whether this DID is confirmed is decided in `save_step`,
                // which may have to resolve it first.
                if w.admin_confirmed.as_deref() != Some(did.as_str()) {
                    w.admin_confirmed = None;
                }
                let seen = w.admin_seen.as_ref().filter(|(d, _)| *d == did);
                match seen {
                    Some((_, Ok(_))) => w.admin_confirmed = Some(did.clone()),
                    Some((_, Err(_))) if on("use_anyway") => {
                        w.admin_confirmed = Some(did.clone());
                    }
                    _ => {}
                }
                w.admin_did = did;
            }
        }
        "proxy" => {
            let choice = get("proxy_choice");
            let (mode, trusted) = match choice.as_str() {
                "none" => (ProxyMode::None, Vec::new()),
                "cloudflare" => (ProxyMode::Cloudflare, farsight_core::cloudflare::bundled()),
                "local" => {
                    let nets = parse_cidrs(&get("local_cidr"))?;
                    if nets.is_empty() {
                        return Err("Enter the CIDR of your tunnel or local proxy.".into());
                    }
                    let mode = if get("local_kind") == "cloudflared" {
                        ProxyMode::Cloudflare
                    } else {
                        ProxyMode::Forwarded
                    };
                    (mode, nets)
                }
                "custom" => {
                    let nets = parse_cidrs(&get("custom_cidrs"))?;
                    if nets.is_empty() {
                        return Err("Enter at least one trusted CIDR.".into());
                    }
                    let mode = if get("custom_mode") == "cloudflare" {
                        ProxyMode::Cloudflare
                    } else {
                        ProxyMode::Forwarded
                    };
                    (mode, nets)
                }
                _ => return Err("Choose a reverse-proxy setup.".into()),
            };
            let public = public_outside_cf(&trusted);
            if !public.is_empty() && !on("proxy_ack") {
                return Err(format!(
                    "{} is public address space outside the bundled Cloudflare ranges. Trusting \
                     it lets anyone there choose their client IP. Tick the acknowledgement if it \
                     really is your proxy.",
                    public
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            w.proxy_choice = choice;
            w.proxy_mode = mode;
            w.trusted = trusted;
            w.proxy_ack = !public.is_empty();
        }
        "storage" => {
            let dsn = get("dsn");
            if dsn != w.dsn {
                w.storage_ok = false;
                w.storage_report.clear();
            }
            w.dsn = dsn;
            if !w.storage_ok {
                return Err("Run the connection test; it must pass before continuing.".into());
            }
        }
        "review" => {}
        _ => return Err("unknown step".into()),
    }
    Ok(())
}

async fn save_step(
    State(st): State<Arc<SetupState>>,
    Path(step): Path<String>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let id = match require_session(&st, &headers) {
        Ok(id) => id,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&st, &id, &headers, &form) {
        return r;
    }
    let Some(i) = step_index(&step).filter(|i| (1..9).contains(i)) else {
        return (StatusCode::NOT_FOUND, "no such step").into_response();
    };
    let slug = STEPS[i].0;
    // The second request of the access step when the public UI was
    // ticked: the confirmation page's own form. It carries no fields of
    // the step; it confirms what the session holds, and only once the
    // rest of the step stands.
    if slug == "access" && form.contains_key("confirm_public") {
        let confirmed = st
            .with_session(&id, |s| {
                let w = &mut s.wizard;
                let ready = w.public_ui
                    && w.admin_token_saved
                    && (!w.admin_ui || w.admin_confirmed.as_deref() == Some(w.admin_did.as_str()));
                if ready {
                    w.public_confirmed = true;
                    w.done[i] = true;
                }
                ready
            })
            .unwrap_or(false);
        return common::redirect(if confirmed {
            "/setup/proxy"
        } else {
            "/setup/access"
        });
    }
    let result = st
        .with_session(&id, |s| {
            let mut w = s.wizard.clone();
            let r = apply_step(&mut w, slug, &form);
            if r.is_ok() {
                s.wizard = w;
            }
            r
        })
        .unwrap_or_else(|| Err("session ended".into()));
    if let Err(e) = result {
        return render_step(&st, &id, slug, Some(e));
    }
    // The admin DID is shown resolved before the step advances (§8.4
    // step 6): the first submit of a DID looks it up, the second confirms
    // it. A DID that does not resolve advances only with "use anyway".
    if slug == "access" {
        let Some(w) = st.with_session(&id, |s| s.wizard.clone()) else {
            return common::redirect("/setup");
        };
        if w.admin_ui && w.admin_confirmed.as_deref() != Some(w.admin_did.as_str()) {
            let already_seen = w
                .admin_seen
                .as_ref()
                .is_some_and(|(d, _)| *d == w.admin_did);
            if !already_seen {
                let result = resolve_admin_did(&st, &w).await;
                st.with_session(&id, |s| {
                    s.wizard.admin_seen = Some((w.admin_did.clone(), result));
                });
            }
            st.with_session(&id, |s| s.wizard.done[i] = false);
            return render_step(&st, &id, slug, None);
        }
        // Turning the public UI on is confirmed on a page that lists what
        // becomes public (§8.4 step 6); the step is not done before that.
        if w.public_ui && !w.public_confirmed {
            st.with_session(&id, |s| s.wizard.done[i] = false);
            let mut p = page(&st, slug, snapshot(&st, &id), None);
            p.confirm_public = true;
            return setup_response(&p);
        }
    }
    st.with_session(&id, |s| s.wizard.done[i] = true);
    let next = STEPS[i + 1].0;
    if next == "done" {
        return common::redirect("/setup/review");
    }
    common::redirect(&format!("/setup/{next}"))
}

/// Looks the wizard's admin DID up through a safe client built from the
/// wizard's own answers (the PLC directory is step 5's). Setup mode has
/// no other outbound client; the caller holds a verified setup session.
async fn resolve_admin_did(st: &SetupState, w: &Wizard) -> Result<crate::oauth::Identity, String> {
    use farsight_core::net::{SafeClient, SafeClientConfig};
    let did = farsight_core::Did::parse(&w.admin_did).map_err(|e| e.to_string())?;
    let cfg = w.build_config();
    let safe = SafeClient::new(SafeClientConfig::from_config(&cfg, st.version));
    match tokio::time::timeout(
        Duration::from_secs(15),
        crate::oauth::identity(&safe, &cfg, &did),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => Err("the lookup timed out".into()),
    }
}

/// Subscribes to `url` for at most `limit` and reports events, lag and v2
/// support (§8.4 step 4).
pub async fn test_firehose(url: &str, limit: Duration) -> (Vec<String>, bool) {
    use farsight_ingest::conn::{self, ConnectError};
    use farsight_ingest::frame::{Frame, Protocol};
    use farsight_ingest::resume::Cursor;
    let mut lines = Vec::new();
    let (session, v2) = match conn::connect(url, Protocol::V2, Cursor::Live, true).await {
        Ok(s) => (Some(s), true),
        Err(ConnectError::NotOffered(code)) => {
            lines.push(format!("{url}: v2 not offered (HTTP {code}); trying v1"));
            match conn::connect(url, Protocol::V1, Cursor::Live, true).await {
                Ok(s) => (Some(s), false),
                Err(e) => {
                    lines.push(format!("{url}: v1 failed: {e}"));
                    (None, false)
                }
            }
        }
        Err(e) => {
            lines.push(format!("{url}: connection failed: {e}"));
            (None, false)
        }
    };
    let Some(mut s) = session else {
        return (lines, false);
    };
    let started = Instant::now();
    let mut events = 0u64;
    let mut last_lag: Option<f64> = None;
    while started.elapsed() < limit {
        let left = limit.saturating_sub(started.elapsed());
        match tokio::time::timeout(left, s.next_frame()).await {
            Ok(Some(Ok(Frame::Event(ev)))) => {
                events += 1;
                let now_us = Utc::now().timestamp_micros();
                last_lag = Some((now_us - ev.witness_us) as f64 / 1e6);
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => {
                lines.push(format!("{url}: read error: {e}"));
                break;
            }
            Ok(None) | Err(_) => break,
        }
    }
    s.close().await;
    lines.push(format!(
        "{url}: connected over {}, {events} events in {:.1} s{}",
        if v2 { "v2" } else { "v1" },
        started.elapsed().as_secs_f64().min(limit.as_secs_f64()),
        last_lag.map_or(String::new(), |l| format!(", lag {l:.1} s"))
    ));
    (lines, v2)
}

async fn firehose_test(
    State(st): State<Arc<SetupState>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let id = match require_session(&st, &headers) {
        Ok(id) => id,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&st, &id, &headers, &form) {
        return r;
    }
    let urls: Vec<String> = form
        .get("urls")
        .map(|t| {
            t.lines()
                .map(|l| l.trim().trim_end_matches('/').to_owned())
                .filter(|l| l.starts_with("ws://") || l.starts_with("wss://"))
                .collect()
        })
        .unwrap_or_default();
    if urls.is_empty() {
        return render_step(
            &st,
            &id,
            "firehose",
            Some("Enter at least one ws:// or wss:// URL.".into()),
        );
    }
    // ≤ 10 s in total (§8.4 step 4), split across the instances.
    let per = Duration::from_secs(10) / urls.len().max(1) as u32;
    let mut report = Vec::new();
    let mut any_v2 = false;
    for u in &urls {
        let (lines, v2) = test_firehose(u, per).await;
        report.extend(lines);
        any_v2 |= v2;
    }
    st.with_session(&id, |s| {
        s.wizard.urls = urls;
        s.wizard.firehose_test = report;
        s.wizard.v2_seen = any_v2;
    });
    render_step(&st, &id, "firehose", None)
}

/// Checks a Postgres DSN for the storage step (§8.4 step 8): connect,
/// version ≥ 15, and the database empty or holding Farsight migrations.
pub async fn test_storage(dsn: &str) -> (Vec<String>, bool) {
    use sqlx::Connection;
    let mut lines = Vec::new();
    let conn =
        tokio::time::timeout(Duration::from_secs(10), sqlx::PgConnection::connect(dsn)).await;
    let mut conn = match conn {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => return (vec![format!("Connection failed: {e}")], false),
        Err(_) => return (vec!["Connection timed out after 10 s.".into()], false),
    };
    let version: Result<i32, _> =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
            .fetch_one(&mut conn)
            .await;
    let version = match version {
        Ok(v) => v,
        Err(e) => {
            return (
                vec![format!("Reading the server version failed: {e}")],
                false,
            );
        }
    };
    lines.push(format!(
        "Connected; PostgreSQL {}.{}.",
        version / 10000,
        version % 10000
    ));
    if version < 150_000 {
        lines.push("PostgreSQL 15 or newer is required.".into());
        return (lines, false);
    }
    let tables: Result<i64, _> = sqlx::query_scalar(
        "SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')",
    )
    .fetch_one(&mut conn)
    .await;
    let tables = tables.unwrap_or(-1);
    if tables == 0 {
        lines.push("The database is empty; Farsight will create its schema.".into());
        let _ = conn.close().await;
        return (lines, true);
    }
    let ours: Option<i32> = farsight_storage_schema_version(&mut conn)
        .await
        .unwrap_or_default();
    let _ = conn.close().await;
    match ours {
        Some(v) => {
            lines.push(format!(
                "Existing Farsight database recognized (schema version {v}); its data is kept."
            ));
            if v > farsight_storage::SCHEMA_VERSION {
                lines.push(format!(
                    "Its schema is newer than this build ({}); use a newer Farsight image.",
                    farsight_storage::SCHEMA_VERSION
                ));
                return (lines, false);
            }
            (lines, true)
        }
        None => {
            lines.push(format!(
                "The database holds {tables} tables that are not Farsight's. Use an empty \
                 database."
            ));
            (lines, false)
        }
    }
}

async fn farsight_storage_schema_version(
    conn: &mut sqlx::PgConnection,
) -> Result<Option<i32>, sqlx::Error> {
    let exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('public.schema_version') IS NOT NULL
            AND to_regclass('public._sqlx_migrations') IS NOT NULL",
    )
    .fetch_one(&mut *conn)
    .await?;
    if !exists {
        return Ok(None);
    }
    sqlx::query_scalar("SELECT max(version) FROM schema_version")
        .fetch_one(&mut *conn)
        .await
}

async fn storage_test(
    State(st): State<Arc<SetupState>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let id = match require_session(&st, &headers) {
        Ok(id) => id,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&st, &id, &headers, &form) {
        return r;
    }
    let dsn = form
        .get("dsn")
        .map(|s| s.trim().to_owned())
        .unwrap_or_default();
    let (report, ok) = if dsn.is_empty() {
        (vec!["Enter a connection string.".to_owned()], false)
    } else {
        test_storage(&dsn).await
    };
    st.with_session(&id, |s| {
        s.wizard.dsn = dsn;
        s.wizard.storage_report = report;
        s.wizard.storage_ok = ok;
    });
    render_step(&st, &id, "storage", None)
}

async fn proxy_preview(
    State(st): State<Arc<SetupState>>,
    headers: HeaderMap,
    client: Option<axum::Extension<ClientIp>>,
    original: Option<axum::Extension<OriginalForwarding>>,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let id = match require_session(&st, &headers) {
        Ok(id) => id,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&st, &id, &headers, &form) {
        return r;
    }
    let mut w = st
        .with_session(&id, |s| s.wizard.clone())
        .unwrap_or_else(|| Wizard::new(&st.env));
    let mut f = form.clone();
    f.insert("proxy_ack".into(), "on".into());
    let err = apply_step(&mut w, "proxy", &f).err();
    let mut p = page(&st, "proxy", snapshot(&st, &id), err);
    p.w.proxy_choice = w.proxy_choice.clone();
    p.w.proxy_mode = w.proxy_mode;
    p.trusted_text = w
        .trusted
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    p.preview = preview_lines(
        client.map(|c| c.0),
        original.map(|o| o.0).unwrap_or_default(),
        w.proxy_mode,
        &w.trusted,
    );
    setup_response(&p)
}

/// The final page.
#[derive(Template)]
#[template(path = "setup_done.html")]
pub struct DonePage {
    /// The admin UI is on.
    pub ui: bool,
    /// The public UI is on.
    pub public: bool,
    /// Error, if the write failed.
    pub error: Option<String>,
    /// The admin account: the DID, and its handle when it verified.
    pub admin: String,
    /// The hostname can be an OAuth client (a public HTTPS domain name);
    /// otherwise the page explains loopback sign-in.
    pub hosted: bool,
}

async fn finish(
    State(st): State<Arc<SetupState>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let id = match require_session(&st, &headers) {
        Ok(id) => id,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&st, &id, &headers, &form) {
        return r;
    }
    let Some(w) = st.with_session(&id, |s| s.wizard.clone()) else {
        return common::redirect("/setup");
    };
    // Steps 2–8 must be validated; Review (9) has no form of its own.
    if let Some(j) = (1..8).find(|j| !w.done[*j]) {
        return common::redirect(&format!("/setup/{}", STEPS[j].0));
    }
    let config = w.build_config();
    let text = match farsight_core::config::to_toml(&config) {
        Ok(t) => t,
        Err(e) => return render_step(&st, &id, "review", Some(e.to_string())),
    };
    if let Err(e) = farsight_core::config::load_from_parts(Some(&text), &st.env) {
        return render_step(
            &st,
            &id,
            "review",
            Some(format!("The configuration is invalid: {e}")),
        );
    }
    if let Some(dir) = st.config_path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    // First writer wins (§8.3).
    match farsight_core::config::write_new(&st.config_path, &text) {
        Ok(true) => {}
        Ok(false) => {
            return render_private(&DonePage {
                ui: false,
                public: false,
                error: Some("Another setup session already completed setup.".into()),
                admin: String::new(),
                hosted: true,
            });
        }
        Err(e) => {
            return render_step(
                &st,
                &id,
                "review",
                Some(format!("Writing {} failed: {e}", st.config_path.display())),
            );
        }
    }
    setup_token::delete(&st.token_path);
    tracing::info!("setup token consumed; configuration written, switching to normal mode");
    st.sessions
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    let _ = st.completed.send(true);
    let mut r = render_private(&DonePage {
        ui: config.access.admin_ui,
        public: config.access.public_ui,
        error: None,
        admin: match w.admin_seen.as_ref() {
            Some((did, Ok(i))) if *did == w.admin_did => match &i.handle {
                Some(h) => format!("{did}, @{h}"),
                None => did.clone(),
            },
            _ => w.admin_did.clone(),
        },
        hosted: crate::oauth::hosted_possible(&config.server.hostname.to_ascii_lowercase()),
    });
    r.headers_mut().append(
        header::SET_COOKIE,
        cookie(SESSION_COOKIE, "", "/setup", false, Some(0)),
    );
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_hosts() {
        assert!(valid_server_host("farsight.example.org"));
        assert!(valid_server_host("10.0.0.5:8080"));
        assert!(valid_server_host("farsight-box"));
        assert!(!valid_server_host("https://x.example"));
        assert!(!valid_server_host("x.example/path"));
    }

    #[test]
    fn budget_is_70_percent() {
        let mut w = Wizard::new(&[]);
        w.disk_gb = 100;
        assert_eq!(w.budget_bytes(), 70_000_000_000);
        w.sweep = true;
        assert!(disk_warning(&w).is_some());
        w.disk_gb = 500;
        assert!(disk_warning(&w).is_none());
    }

    #[test]
    fn built_config_validates() {
        let mut w = Wizard::new(&[]);
        w.hostname = "farsight.example".into();
        w.contact = "mailto:ops@example".into();
        w.dsn = "postgres://u:p@db/farsight".into();
        // A new wizard has both web interfaces off.
        assert!(!w.admin_ui && !w.public_ui);
        w.admin_ui = true;
        w.admin_did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into();
        let text = farsight_core::config::to_toml(&w.build_config()).unwrap();
        let l = farsight_core::config::load_from_parts(Some(&text), &[]).unwrap();
        assert_eq!(
            l.admin_auth(),
            farsight_core::config::AdminAuth::Configured(w.admin_did.clone())
        );
        assert!(l.warnings.is_empty());
        assert!(text.contains("admin_ui = true"));
        // Without the admin UI no admin DID is written.
        w.admin_ui = false;
        let off = farsight_core::config::to_toml(&w.build_config()).unwrap();
        assert!(!off.contains("admin_did") && off.contains("admin_ui = false"));
        // The public UI is written only once confirmed.
        w.public_ui = true;
        assert!(!w.build_config().access.public_ui);
        w.public_confirmed = true;
        assert!(w.build_config().access.public_ui);
        assert_eq!(l.config.storage.budget_bytes, 350_000_000_000);
        assert!(redacted_toml(&l.config).contains("<redacted>"));
        assert!(!redacted_toml(&l.config).contains("u:p@"));
    }

    #[test]
    fn cidr_rules() {
        assert!(parse_cidrs("0.0.0.0/0").is_err());
        assert!(parse_cidrs("10.0.0.0/8\n172.18.0.1").is_ok());
        assert_eq!(
            public_outside_cf(&parse_cidrs("104.16.0.0/13").unwrap()).len(),
            0
        );
        assert_eq!(
            public_outside_cf(&parse_cidrs("203.0.113.0/24").unwrap()).len(),
            1
        );
    }
}
