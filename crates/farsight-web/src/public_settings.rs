//! The "Public UI" subsection of the admin Settings page (design §8.6):
//! a control for every `[public_ui]` key, the master `access.public_ui`
//! toggle, and the confirmation step in front of turning it on.
//!
//! Turning the public UI on is the one settings change that publishes
//! data to visitors without a login, so it never takes effect on the
//! first request: the operator is shown what becomes reachable and
//! confirms with a second request carrying a one-time token. That holds
//! for the form here and for the raw `config.toml` editor alike.
//!
//! Every save goes through the same loader as start-up, so a combination
//! the loader refuses (the public UI without `reads = "public"`) is refused
//! here too and nothing is written.
//!
//! The retired `show_history` key has no control. A save of this form
//! deletes it from the file: the save edits the file's existing table, and
//! a key the form does not name would otherwise stay there for ever.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use askama::Template;
use axum::extract::{Form, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use farsight_api::config_store::EditReport;
use farsight_core::config::{
    self, Config, MAX_EXCLUDED_DIDS, MAX_INSTANCE_DESCRIPTION, MAX_PUBLIC_CONTACT,
    validate_record_viewer_url,
};
use farsight_core::{ConfigDuration, Did};

use crate::common::{random_id, render_private};
use crate::pages::{Admin, Nav, WebState, check_form, gate, nav};
use crate::public::PendingEnable;

/// How long a confirmation page stays valid.
pub const CONFIRM_TTL: Duration = Duration::from_secs(600);

/// The values of the Public UI form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// `access.public_ui`.
    pub enabled: bool,
    /// `instance_description`.
    pub instance_description: String,
    /// `contact`.
    pub contact: String,
    /// `show_outgoing_blocks`.
    pub show_outgoing_blocks: bool,
    /// `show_top_blockers`.
    pub show_top_blockers: bool,
    /// `show_top_blocked`.
    pub show_top_blocked: bool,
    /// `show_opengraph_image`.
    pub show_opengraph_image: bool,
    /// `record_viewer_url`.
    pub record_viewer_url: String,
    /// `show_avatars`.
    pub show_avatars: bool,
    /// `avatar_thumbnails`.
    pub avatar_thumbnails: bool,
    /// `card_rps`.
    pub card_rps: u32,
    /// `card_burst`; never below `card_rps`.
    pub card_burst: u32,
    /// The burst as typed was below the rate and was raised to it.
    pub burst_raised: bool,
    /// `handle_warming_enabled`.
    pub handle_warming_enabled: bool,
    /// `handle_rps`.
    pub handle_rps: u32,
    /// `handle_pass_rps`.
    pub handle_pass_rps: u32,
    /// `dark_mode_default`: `light`, `dark` or `system`.
    pub dark_mode_default: String,
    /// `crawlable`.
    pub crawlable: bool,
    /// `rate_limit_rps`.
    pub rate_limit_rps: u32,
    /// `rate_limit_burst`.
    pub rate_limit_burst: u32,
    /// `query_concurrency`.
    pub query_concurrency: u32,
    /// `handle_cache_ttl`, as written (`1h`, `30m`).
    pub handle_cache_ttl: String,
    /// `excluded_dids`.
    pub excluded_dids: Vec<String>,
}

/// What the form shows: strings, so that a refused submission can be
/// shown again as it was typed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct View {
    /// Master toggle.
    pub enabled: bool,
    /// Description.
    pub instance_description: String,
    /// Contact.
    pub contact: String,
    /// Outgoing section.
    pub show_outgoing_blocks: bool,
    /// The home page's "Top blockers" lists.
    pub show_top_blockers: bool,
    /// The home page's "Most blocked" lists.
    pub show_top_blocked: bool,
    /// Preview image tag.
    pub show_opengraph_image: bool,
    /// Record viewer URL template.
    pub record_viewer_url: String,
    /// Avatars in profile cards.
    pub show_avatars: bool,
    /// Avatars as thumbnails from Bluesky's image service.
    pub avatar_thumbnails: bool,
    /// Profile cards per second, process-wide.
    pub card_rps: String,
    /// Burst of the same budget.
    pub card_burst: String,
    /// Handles verified in the background.
    pub handle_warming_enabled: bool,
    /// Handle verifications per second.
    pub handle_rps: String,
    /// Background handle checks per second; 0 = off.
    pub handle_pass_rps: String,
    /// Theme default.
    pub dark_mode_default: String,
    /// Crawlers.
    pub crawlable: bool,
    /// Page views per second.
    pub rate_limit_rps: String,
    /// Burst.
    pub rate_limit_burst: String,
    /// Concurrent renders.
    pub query_concurrency: String,
    /// Handle cache lifetime.
    pub handle_cache_ttl: String,
    /// Excluded DIDs, one per line.
    pub excluded_dids: String,
    /// The upper bound of `query_concurrency` in force.
    pub max_concurrency: u32,
}

impl View {
    /// The config in force.
    pub fn of(c: &Config) -> View {
        let p = &c.public_ui;
        View {
            enabled: c.access.public_ui,
            instance_description: p.instance_description.clone(),
            contact: p.contact.clone(),
            show_outgoing_blocks: p.show_outgoing_blocks,
            show_top_blockers: p.show_top_blockers,
            show_top_blocked: p.show_top_blocked,
            show_opengraph_image: p.show_opengraph_image,
            record_viewer_url: p.record_viewer_url.clone(),
            show_avatars: p.show_avatars,
            avatar_thumbnails: p.avatar_thumbnails,
            card_rps: p.card_rps.to_string(),
            card_burst: p.effective_card_burst().to_string(),
            handle_warming_enabled: p.handle_warming_enabled,
            handle_rps: p.handle_rps.to_string(),
            handle_pass_rps: p.handle_pass_rps.to_string(),
            dark_mode_default: p.dark_mode_default.as_str().to_owned(),
            crawlable: p.crawlable,
            rate_limit_rps: p.rate_limit_rps.to_string(),
            rate_limit_burst: p.rate_limit_burst.to_string(),
            query_concurrency: p.query_concurrency.to_string(),
            handle_cache_ttl: p.handle_cache_ttl.to_string(),
            excluded_dids: p.excluded_dids.join("\n"),
            max_concurrency: c.rate_limit.query_concurrency,
        }
    }

    /// A submitted form, as typed.
    pub fn submitted(form: &HashMap<String, String>, c: &Config) -> View {
        let s = |k: &str| form.get(k).cloned().unwrap_or_default();
        let b = |k: &str| form.contains_key(k);
        View {
            enabled: b("enabled"),
            instance_description: s("instance_description"),
            contact: s("contact"),
            show_outgoing_blocks: b("show_outgoing_blocks"),
            show_top_blockers: b("show_top_blockers"),
            show_top_blocked: b("show_top_blocked"),
            show_opengraph_image: b("show_opengraph_image"),
            record_viewer_url: s("record_viewer_url"),
            show_avatars: b("show_avatars"),
            avatar_thumbnails: b("avatar_thumbnails"),
            card_rps: s("card_rps"),
            card_burst: s("card_burst"),
            handle_warming_enabled: b("handle_warming_enabled"),
            // A form from before the field existed keeps the rate in force.
            handle_rps: form
                .get("handle_rps")
                .cloned()
                .unwrap_or_else(|| c.public_ui.handle_rps.to_string()),
            handle_pass_rps: form
                .get("handle_pass_rps")
                .cloned()
                .unwrap_or_else(|| c.public_ui.handle_pass_rps.to_string()),
            dark_mode_default: s("dark_mode_default"),
            crawlable: b("crawlable"),
            rate_limit_rps: s("rate_limit_rps"),
            rate_limit_burst: s("rate_limit_burst"),
            query_concurrency: s("query_concurrency"),
            handle_cache_ttl: s("handle_cache_ttl"),
            excluded_dids: s("excluded_dids"),
            max_concurrency: c.rate_limit.query_concurrency,
        }
    }
}

/// Parses the DID list of the multi-line field: one DID per line, blank
/// lines ignored, repeats dropped.
pub fn parse_excluded(text: &str) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for (i, line) in text.lines().enumerate() {
        let d = line.trim();
        if d.is_empty() {
            continue;
        }
        if Did::parse(d).is_err() {
            let shown: String = d.chars().take(80).collect();
            return Err(format!(
                "Excluded accounts, line {}: {shown:?} is not a DID. Enter one DID per line \
                 (did:plc:… or did:web:…); handles are not accepted.",
                i + 1
            ));
        }
        if seen.insert(d.to_owned()) {
            out.push(d.to_owned());
        }
    }
    if out.len() > MAX_EXCLUDED_DIDS {
        return Err(format!(
            "Excluded accounts: {} DIDs; at most {MAX_EXCLUDED_DIDS} are allowed.",
            out.len()
        ));
    }
    Ok(out)
}

fn number(field: &str, raw: &str, min: u32, max: u32) -> Result<u32, String> {
    match raw.trim().parse::<u32>() {
        Ok(n) if (min..=max).contains(&n) => Ok(n),
        _ => Err(format!(
            "{field} must be a whole number between {min} and {max}."
        )),
    }
}

impl Settings {
    /// Validates a submitted form.
    pub fn parse(v: &View) -> Result<Settings, String> {
        if v.instance_description.chars().count() > MAX_INSTANCE_DESCRIPTION {
            return Err(format!(
                "The description is longer than {MAX_INSTANCE_DESCRIPTION} characters."
            ));
        }
        if v.contact.chars().count() > MAX_PUBLIC_CONTACT {
            return Err(format!(
                "The contact is longer than {MAX_PUBLIC_CONTACT} characters."
            ));
        }
        if !matches!(v.dark_mode_default.as_str(), "light" | "dark" | "system") {
            return Err("The default theme must be light, dark or system.".into());
        }
        let ttl = v.handle_cache_ttl.trim();
        if ttl.parse::<ConfigDuration>().is_err() {
            return Err(
                "The handle cache lifetime must be a duration such as 30m, 1h or 1d.".into(),
            );
        }
        let viewer = v.record_viewer_url.trim();
        if !viewer.is_empty() {
            validate_record_viewer_url(viewer).map_err(|r| format!("Record viewer URL: {r}"))?;
        }
        let card_rps = number("Profile cards per second", &v.card_rps, 1, 1_000)?;
        let card_burst = number("Profile card burst", &v.card_burst, 1, 10_000)?;
        Ok(Settings {
            record_viewer_url: viewer.to_owned(),
            show_avatars: v.show_avatars,
            avatar_thumbnails: v.avatar_thumbnails,
            card_rps,
            // A burst below the rate is raised to it, as the loader does
            // for a hand-edited file; the save says so.
            card_burst: card_burst.max(card_rps),
            burst_raised: card_burst < card_rps,
            handle_warming_enabled: v.handle_warming_enabled,
            handle_rps: number(
                "Handle checks per second",
                &v.handle_rps,
                1,
                farsight_core::config::MAX_HANDLE_RPS,
            )?,
            handle_pass_rps: number(
                "Background handle checks per second",
                &v.handle_pass_rps,
                0,
                farsight_core::config::MAX_HANDLE_RPS,
            )?,
            enabled: v.enabled,
            instance_description: v.instance_description.replace("\r\n", "\n"),
            contact: v.contact.trim().to_owned(),
            show_outgoing_blocks: v.show_outgoing_blocks,
            show_top_blockers: v.show_top_blockers,
            show_top_blocked: v.show_top_blocked,
            show_opengraph_image: v.show_opengraph_image,
            dark_mode_default: v.dark_mode_default.clone(),
            crawlable: v.crawlable,
            rate_limit_rps: number("Page views per second", &v.rate_limit_rps, 1, 10_000)?,
            rate_limit_burst: number("Burst", &v.rate_limit_burst, 1, 100_000)?,
            query_concurrency: number(
                "Concurrent page renders",
                &v.query_concurrency,
                1,
                v.max_concurrency.max(1),
            )?,
            handle_cache_ttl: ttl.to_owned(),
            excluded_dids: parse_excluded(&v.excluded_dids)?,
        })
    }

    /// Writes the values into a `config.toml` table.
    pub fn apply(&self, t: &mut toml::Table) -> Result<(), String> {
        fn section<'a>(t: &'a mut toml::Table, name: &str) -> Result<&'a mut toml::Table, String> {
            t.entry(name)
                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                .as_table_mut()
                .ok_or_else(|| format!("`{name}` in config.toml is not a table"))
        }
        section(t, "access")?.insert("public_ui".into(), self.enabled.into());
        let p = section(t, "public_ui")?;
        p.insert(
            "instance_description".into(),
            self.instance_description.clone().into(),
        );
        p.insert("contact".into(), self.contact.clone().into());
        p.insert(
            "show_outgoing_blocks".into(),
            self.show_outgoing_blocks.into(),
        );
        p.insert("show_top_blockers".into(), self.show_top_blockers.into());
        p.insert("show_top_blocked".into(), self.show_top_blocked.into());
        // Retired: accepted by the loader, never written back.
        p.remove("show_history");
        p.insert(
            "record_viewer_url".into(),
            self.record_viewer_url.clone().into(),
        );
        p.insert("show_avatars".into(), self.show_avatars.into());
        p.insert("avatar_thumbnails".into(), self.avatar_thumbnails.into());
        p.insert("card_rps".into(), i64::from(self.card_rps).into());
        p.insert("card_burst".into(), i64::from(self.card_burst).into());
        p.insert(
            "handle_warming_enabled".into(),
            self.handle_warming_enabled.into(),
        );
        p.insert("handle_rps".into(), i64::from(self.handle_rps).into());
        p.insert(
            "handle_pass_rps".into(),
            i64::from(self.handle_pass_rps).into(),
        );
        p.insert(
            "show_opengraph_image".into(),
            self.show_opengraph_image.into(),
        );
        p.insert(
            "dark_mode_default".into(),
            self.dark_mode_default.clone().into(),
        );
        p.insert("crawlable".into(), self.crawlable.into());
        p.insert(
            "rate_limit_rps".into(),
            i64::from(self.rate_limit_rps).into(),
        );
        p.insert(
            "rate_limit_burst".into(),
            i64::from(self.rate_limit_burst).into(),
        );
        p.insert(
            "query_concurrency".into(),
            i64::from(self.query_concurrency).into(),
        );
        p.insert(
            "handle_cache_ttl".into(),
            self.handle_cache_ttl.clone().into(),
        );
        p.insert(
            "excluded_dids".into(),
            toml::Value::Array(
                self.excluded_dids
                    .iter()
                    .map(|d| toml::Value::String(d.clone()))
                    .collect(),
            ),
        );
        Ok(())
    }
}

/// A settings change that waits for confirmation.
#[derive(Debug, Clone)]
pub enum Change {
    /// The Public UI form.
    Form(Box<Settings>),
    /// The whole file, from the raw editor (secrets already put back).
    File(String),
}

/// What the confirmation page says will become reachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exposure {
    /// `server.hostname`.
    pub hostname: String,
    /// `server.contact`.
    pub server_contact: String,
    /// Profile cards will carry avatars, fetched by visitors' browsers
    /// from the accounts' own servers.
    pub show_avatars: bool,
    /// The avatars are thumbnails on Bluesky's image service instead.
    pub avatar_thumbnails: bool,
    /// Records will link to this viewer (its origin), if one is set.
    pub record_viewer: Option<String>,
    /// The outgoing section will be shown.
    pub show_outgoing: bool,
    /// The home page will list the accounts that block the most.
    pub top_blockers: bool,
    /// The home page will list the accounts that are blocked the most.
    pub top_blocked: bool,
    /// Crawlers will be allowed.
    pub crawlable: bool,
}

impl Exposure {
    /// From the config the change would produce.
    pub fn of(c: &Config) -> Exposure {
        Exposure {
            hostname: c.server.hostname.clone(),
            server_contact: c.server.contact.clone(),
            show_avatars: c.public_ui.show_avatars,
            avatar_thumbnails: c.public_ui.avatar_thumbnails,
            record_viewer: url::Url::parse(&c.public_ui.record_viewer_url.replace(['{', '}'], ""))
                .ok()
                .and_then(|u| u.host_str().map(str::to_owned)),
            show_outgoing: c.public_ui.show_outgoing_blocks,
            top_blockers: c.public_ui.show_top_blockers,
            top_blocked: c.public_ui.show_top_blocked,
            crawlable: c.public_ui.crawlable,
        }
    }
}

/// The confirmation page.
#[derive(Template)]
#[template(path = "public_ui_confirm.html")]
struct ConfirmPage {
    nav: Nav,
    csrf: String,
    token: String,
    e: Exposure,
}

/// The config a change would produce, loaded and validated exactly as at
/// start-up, without writing anything.
fn dry_run(st: &WebState, change: &Change) -> Result<Config, String> {
    let store = &st.api.config;
    if store.current().from_env_only {
        return Err(
            "The configuration is managed through the environment (FARSIGHT_SKIP_WIZARD); \
             change it there."
                .into(),
        );
    }
    let text = match change {
        Change::File(t) => t.clone(),
        Change::Form(s) => {
            let current = store.file_text().map_err(|e| e.to_string())?;
            let mut table: toml::Table = current
                .parse()
                .map_err(|e: toml::de::Error| e.to_string())?;
            s.apply(&mut table)?;
            toml::to_string_pretty(&table).map_err(|e| e.to_string())?
        }
    };
    config::load_from_parts(Some(&text), store.env())
        .map(|l| l.config)
        .map_err(|e| e.to_string())
}

async fn commit(st: &WebState, change: &Change) -> Result<EditReport, String> {
    let store = &st.api.config;
    let r = match change {
        Change::Form(s) => store.edit(|t| s.apply(t)).await,
        Change::File(text) => store.replace(text).await,
    }
    .map_err(|e| e.to_string())?;
    let _ = farsight_api::config_store::notify_config(&st.api.pool).await;
    Ok(r)
}

fn sweep_pending(st: &WebState) {
    st.public
        .pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .retain(|_, p| p.at.elapsed() < CONFIRM_TTL);
}

/// Whether `change` would turn the public UI on from off, and if so the
/// confirmation page to answer with. `Ok(None)`: no confirmation needed.
/// `Err`: the change would be refused; nothing is pending.
pub(crate) fn confirmation(
    st: &WebState,
    admin: &Admin,
    change: Change,
) -> Result<Option<Response>, String> {
    let next = dry_run(st, &change)?;
    let on_now = st.api.config.current().config.access.public_ui;
    if !next.access.public_ui || on_now {
        return Ok(None);
    }
    sweep_pending(st);
    let token = random_id();
    st.public
        .pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(
            token.clone(),
            PendingEnable {
                at: Instant::now(),
                csrf: admin.csrf.clone(),
                change,
            },
        );
    Ok(Some(render_private(&ConfirmPage {
        nav: nav(&Some(admin.clone())),
        csrf: admin.csrf.clone(),
        token,
        e: Exposure::of(&next),
    })))
}

fn outcome(
    st: &WebState,
    admin: &Admin,
    result: Result<EditReport, String>,
    typed: Option<View>,
    note: Option<String>,
) -> Response {
    let mut page = crate::pages::settings_base(st, admin);
    match result {
        Ok(report) => {
            let mut notice = if report.changed.is_empty() {
                "No changes.".to_owned()
            } else {
                format!("Saved. Changed: {}.", report.changed.join(", "))
            };
            if let Some(n) = note {
                notice.push(' ');
                notice.push_str(&n);
            }
            page.notice = Some(notice);
            page.restart = report.restart_required;
        }
        Err(e) => {
            page.error = Some(e);
            if let Some(v) = typed {
                page.p = v;
            }
        }
    }
    render_private(&page)
}

/// `POST /admin/settings/public-ui`: saves the Public UI form, or — when it
/// turns the public UI on — answers with the confirmation page and writes
/// nothing.
pub async fn save(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let admin = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if let Err(r) = check_form(&admin, &headers, &form) {
        return r;
    }
    let typed = View::submitted(&form, &st.api.config.current().config);
    let settings = match Settings::parse(&typed) {
        Ok(s) => s,
        Err(e) => return outcome(&st, &admin, Err(e), Some(typed), None),
    };
    let note = settings.burst_raised.then(|| {
        format!(
            "The profile card burst was below the rate and was raised to {}.",
            settings.card_burst
        )
    });
    let change = Change::Form(Box::new(settings));
    match confirmation(&st, &admin, change.clone()) {
        Err(e) => outcome(&st, &admin, Err(e), Some(typed), None),
        Ok(Some(confirm)) => confirm,
        Ok(None) => {
            let r = commit(&st, &change).await;
            outcome(&st, &admin, r, Some(typed), note)
        }
    }
}

/// `POST /admin/settings/public-ui/confirm`: the second request. Commits the
/// change the confirmation page was rendered for; a token that is
/// unknown, expired, already used or from another session commits
/// nothing.
pub async fn confirm(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let admin = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if let Err(r) = check_form(&admin, &headers, &form) {
        return r;
    }
    let token = form.get("confirm").cloned().unwrap_or_default();
    let pending = st
        .public
        .pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&token);
    let change = match pending {
        Some(p) if p.at.elapsed() < CONFIRM_TTL && crate::common::ct_eq(&p.csrf, &admin.csrf) => {
            p.change
        }
        _ => {
            let mut page = crate::pages::settings_base(&st, &admin);
            page.error = Some(
                "Nothing was changed: that confirmation is not valid (it was already used, has \
                 expired, or was never issued). Submit the settings again to get a new one."
                    .into(),
            );
            let mut r = render_private(&page);
            *r.status_mut() = StatusCode::BAD_REQUEST;
            return r;
        }
    };
    let r = commit(&st, &change).await;
    outcome(&st, &admin, r, None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    const D1: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const D2: &str = "did:web:example.com";

    #[test]
    fn excluded_list_parsing() {
        assert_eq!(
            parse_excluded(&format!("{D1}\n\n  {D2}  \r\n{D1}\n")).unwrap(),
            [D1, D2]
        );
        let e = parse_excluded(&format!("{D1}\nalice.example\n")).unwrap_err();
        assert!(e.contains("line 2") && e.contains("alice.example"), "{e}");
        let many: String = (0..=MAX_EXCLUDED_DIDS)
            .map(|i| format!("did:web:h{i}.example\n"))
            .collect();
        assert!(parse_excluded(&many).unwrap_err().contains("at most"));
        assert!(parse_excluded("").unwrap().is_empty());
    }

    #[test]
    fn form_round_trips_through_the_config() {
        let mut c = Config::default();
        c.public_ui.excluded_dids = vec![D1.into(), D2.into()];
        c.public_ui.instance_description = "one\n\ntwo".into();
        let s = Settings::parse(&View::of(&c)).unwrap();
        assert_eq!(s.excluded_dids, [D1, D2]);
        assert_eq!(s.handle_cache_ttl, "1h");
        let mut t = toml::Table::new();
        s.apply(&mut t).unwrap();
        let back: Config = toml::Value::Table(t).try_into().unwrap();
        assert_eq!(back.public_ui, c.public_ui);
        assert_eq!(back.access.public_ui, c.access.public_ui);
    }

    #[test]
    fn a_save_drops_the_retired_key_and_writes_the_new_ones() {
        // A file as the previous version's Settings page left it.
        let mut t: toml::Table =
            "[public_ui]\nshow_history = true\ncrawlable = true\n[server]\nbind = \"x\"\n"
                .parse()
                .unwrap();
        let mut c = Config::default();
        c.public_ui.record_viewer_url =
            "https://viewer.example/at/{authority}/{collection}/{rkey}".into();
        c.public_ui.show_avatars = false;
        c.public_ui.card_rps = 2;
        c.public_ui.card_burst = 5;
        c.public_ui.handle_warming_enabled = false;
        let s = Settings::parse(&View::of(&c)).unwrap();
        assert!(!s.burst_raised);
        s.apply(&mut t).unwrap();
        let p = t["public_ui"].as_table().unwrap();
        assert!(!p.contains_key("show_history"));
        assert_eq!(
            p["record_viewer_url"].as_str(),
            Some("https://viewer.example/at/{authority}/{collection}/{rkey}")
        );
        assert_eq!(p["show_avatars"].as_bool(), Some(false));
        assert_eq!(p["card_rps"].as_integer(), Some(2));
        assert_eq!(p["card_burst"].as_integer(), Some(5));
        assert_eq!(p["handle_warming_enabled"].as_bool(), Some(false));
        // Keys of other sections are left alone.
        assert_eq!(t["server"]["bind"].as_str(), Some("x"));
    }

    #[test]
    fn new_keys_are_validated_on_save() {
        let c = Config::default();
        let ok = View::of(&c);
        let e = Settings::parse(&View {
            record_viewer_url: "https://viewer.example/{authority}/{collection}".into(),
            ..ok.clone()
        })
        .unwrap_err();
        assert_eq!(e, "Record viewer URL: `{rkey}` is missing.");
        assert!(
            Settings::parse(&View {
                card_rps: "0".into(),
                ..ok.clone()
            })
            .is_err()
        );
        // A burst below the rate is raised, not refused.
        let s = Settings::parse(&View {
            card_rps: "6".into(),
            card_burst: "2".into(),
            ..ok.clone()
        })
        .unwrap();
        assert!(s.burst_raised);
        assert_eq!((s.card_rps, s.card_burst), (6, 6));
        // Empty: no viewer, records are plain text.
        assert_eq!(Settings::parse(&ok).unwrap().record_viewer_url, "");
    }

    #[test]
    fn form_validation() {
        let c = Config::default();
        let ok = View::of(&c);
        assert!(Settings::parse(&ok).is_ok());
        for bad in [
            View {
                rate_limit_rps: "0".into(),
                ..ok.clone()
            },
            View {
                rate_limit_burst: "many".into(),
                ..ok.clone()
            },
            View {
                query_concurrency: (c.rate_limit.query_concurrency + 1).to_string(),
                ..ok.clone()
            },
            View {
                dark_mode_default: "sepia".into(),
                ..ok.clone()
            },
            View {
                handle_cache_ttl: "soon".into(),
                ..ok.clone()
            },
            View {
                excluded_dids: "not-a-did".into(),
                ..ok.clone()
            },
            View {
                contact: "x".repeat(MAX_PUBLIC_CONTACT + 1),
                ..ok.clone()
            },
        ] {
            assert!(Settings::parse(&bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn unchecked_boxes_are_false() {
        let c = Config::default();
        let mut form = HashMap::new();
        form.insert("show_outgoing_blocks".to_owned(), "on".to_owned());
        let v = View::submitted(&form, &c);
        assert!(v.show_outgoing_blocks && !v.enabled && !v.crawlable && !v.show_opengraph_image);
        // Avatars default to on in the config; an unchecked box is still off.
        assert!(!v.show_avatars);
    }
}
