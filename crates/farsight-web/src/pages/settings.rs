//! The settings page: the config file with its secrets redacted, saving
//! it and rotating the admin token.

use std::collections::HashMap;
use std::sync::Arc;

use askama::Template;
use axum::extract::{Form, State};
use axum::http::HeaderMap;
use axum::response::Response;

use super::{Admin, Nav, Return, WebState, check_form, clear_admin_cookie, gate, nav, step_up};
use crate::common::render_private;
use crate::public_settings::SettingsError;
use farsight_api::config_store::EditError;

/// The settings page.
#[derive(Template)]
#[template(path = "settings.html")]
pub struct SettingsPage {
    /// Navigation.
    pub nav: Nav,
    /// The session's form token, posted back as the hidden `csrf` field
    /// of every form on the page.
    pub csrf: String,
    /// The config file, secrets redacted.
    pub text: String,
    /// SHA-256 of the config file as it is shown, in hex: the raw
    /// editor's form posts it back, and a save is stored only while the
    /// file is still that one.
    pub base: String,
    /// Keys set from the environment (locked).
    pub env_keys: Vec<String>,
    /// The config comes from the environment alone
    /// (`FARSIGHT_SKIP_WIZARD`): nothing can be saved from the page.
    pub env_managed: bool,
    /// What the save or rotation just posted did, in a green banner;
    /// `None` on a plain view.
    pub notice: Option<String>,
    /// Why it was refused, in a red banner. The submitted text stays in
    /// the editor.
    pub error: Option<String>,
    /// Keys that changed and need a restart.
    pub restart: Vec<String>,
    /// A new admin token, shown once.
    pub new_token: Option<String>,
    /// `access.admin_did` (empty when not set).
    pub admin_did: String,
    /// Its verified handle, when known.
    pub admin_handle: Option<String>,
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
        ("metrics", "bearer_token_sha256"),
    ] {
        if let Some(v) = t
            .get_mut(sec)
            .and_then(|s| s.as_table_mut())
            .and_then(|s| s.get_mut(key))
            && v.as_str().is_some_and(|s| !s.is_empty())
        {
            *v = toml::Value::String(REDACTED.into());
        }
    }
    if let Some(v) = t
        .get_mut("storage")
        .and_then(|s| s.as_table_mut())
        .and_then(|s| s.get_mut("database_url"))
        && let Some(s) = v.as_str()
    {
        *v = toml::Value::String(crate::setup::redact_dsn(s));
    }
    toml::to_string_pretty(&t).unwrap_or_else(|_| text.to_owned())
}

/// Puts the real secrets back where the submitted text still shows the
/// redacted placeholder.
fn unredact(submitted: &str, current: &str) -> Result<String, SettingsError> {
    let mut t: toml::Table = submitted.parse()?;
    let cur: toml::Table = current.parse()?;
    let lookup = |sec: &str, key: &str| cur.get(sec).and_then(|s| s.get(key)).cloned();
    for (sec, key) in [
        ("auth", "admin_token_sha256"),
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
        if (v.as_str() == Some(REDACTED)
            || (v.as_str().is_some() && v.as_str() == shown.as_deref()))
            && let Some(o) = original
        {
            *v = o;
        }
    }
    Ok(toml::to_string_pretty(&t)?)
}

pub(crate) fn settings_base(st: &WebState, s: &Admin) -> SettingsPage {
    let cur = st.api.config.current();
    let file = st.api.config.file_text().unwrap_or_default();
    SettingsPage {
        nav: nav(&Some(s.clone())),
        csrf: s.csrf.clone(),
        text: redact_file(&file),
        base: farsight_api::auth::hex(&farsight_api::auth::sha256(&file)),
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
        p: crate::public_settings::View::of(&cur.config),
    }
}

pub(super) async fn settings_page(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    match gate(&st, &headers).await {
        Ok(s) => {
            // Resolve the admin handle for display (cached; two-second
            // budget), so that `settings_base` finds it.
            let cur = st.api.config.current();
            if !cur.config.access.admin_did.is_empty() {
                st.oauth
                    .handle(&st.safe, &cur.config.access.admin_did)
                    .await;
            }
            render_private(&settings_base(&st, &s))
        }
        Err(r) => r,
    }
}

pub(super) async fn settings_save(
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
    // The file the editor showed: the one named by the form, or, for a
    // form without that field, the file as it is now.
    let base = form
        .get("base")
        .and_then(|b| farsight_api::auth::unhex(b))
        .and_then(|b| <[u8; 32]>::try_from(b).ok())
        .unwrap_or_else(|| farsight_api::auth::sha256(&current));
    let result = match unredact(&submitted, &current) {
        Ok(text) => {
            let change = crate::public_settings::Change::File { text, base };
            // An edit that touches a sensitive key is stored only after a
            // fresh sign-in.
            if crate::public_settings::is_sensitive(&st, &change)
                && let Err(r) = step_up(&s, Return::Settings)
            {
                return r;
            }
            // Turning the public UI on needs the operator's confirmation,
            // whichever editor asks for it.
            match crate::public_settings::confirmation(&st, &s, change.clone()) {
                Ok(Some(confirm)) => return confirm,
                Ok(None) => crate::public_settings::commit(&st, &change).await,
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(SettingsError::NotToml(Box::new(e))),
    };
    let mut page = settings_base(&st, &s);
    match result {
        Ok(report) => {
            let signed_out = token_changed(&report);
            page.notice = Some(if report.changed.is_empty() {
                "No changes.".into()
            } else if signed_out {
                format!(
                    "Saved. Changed: {}. {SIGNED_OUT}",
                    report.changed.join(", ")
                )
            } else {
                format!("Saved. Changed: {}.", report.changed.join(", "))
            });
            page.restart = report.restart_required;
            let mut r = render_private(&page);
            if signed_out {
                clear_admin_cookie(&mut r);
            }
            r
        }
        Err(e) => {
            page.error = Some(e.to_string());
            // The editor keeps what was typed, and stays tied to the
            // file that text was made from.
            page.text = submitted;
            page.base = farsight_api::auth::hex(&base);
            render_private(&page)
        }
    }
}

/// What a save says when it ended the sessions.
pub(crate) const SIGNED_OUT: &str =
    "The admin token changed, so every session was signed out; sign in again to continue.";

/// Whether a stored edit changed the admin token's hash.
pub(crate) fn token_changed(report: &farsight_api::config_store::EditReport) -> bool {
    report
        .changed
        .iter()
        .any(|k| k == farsight_api::config_store::ADMIN_TOKEN_KEY)
}

/// What follows every edit stored from the admin UI: the backfill process
/// is told, and if the admin token's hash changed, however the edit was
/// made, every admin session ends. Whoever held a session while the token
/// was replaced does not keep it.
pub(crate) async fn after_store(st: &WebState, report: &farsight_api::config_store::EditReport) {
    let _ = farsight_api::config_store::notify_config(&st.api.pool).await;
    if token_changed(report)
        && let Err(e) = farsight_storage::auth::delete_all_sessions(&st.api.pool).await
    {
        tracing::error!(error = %e, "the admin token changed but the sessions could not be ended");
    }
}

fn set_admin_token(t: &mut toml::Table, hash: String) -> Result<(), EditError> {
    let auth = t
        .entry("auth")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .ok_or("`auth` is not a table")?;
    auth.insert("admin_token_sha256".into(), toml::Value::String(hash));
    Ok(())
}

pub(super) async fn settings_token(
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
    if let Err(r) = step_up(&s, Return::Settings) {
        return r;
    }
    let token = farsight_api::auth::generate(farsight_api::auth::ADMIN_PREFIX);
    let hash = farsight_api::auth::hex(&farsight_api::auth::sha256(&token));
    let mut page = settings_base(&st, &s);
    let report = match st.api.config.edit(|t| set_admin_token(t, hash)).await {
        Ok(r) => r,
        Err(e) => {
            page.error = Some(e.to_string());
            return render_private(&page);
        }
    };
    // Rotation ends every session; this page shows the token once, then
    // the operator signs in again.
    after_store(&st, &report).await;
    let file = st.api.config.file_text().unwrap_or_default();
    page.text = redact_file(&file);
    page.base = farsight_api::auth::hex(&farsight_api::auth::sha256(&file));
    page.notice = Some("Admin token rotated. All sessions were signed out.".into());
    page.new_token = Some(token);
    let mut r = render_private(&page);
    clear_admin_cookie(&mut r);
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_round_trip() {
        let file = "[auth]\nadmin_token_sha256 = \"abc\"\n\
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
