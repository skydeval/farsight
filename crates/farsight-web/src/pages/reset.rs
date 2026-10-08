//! The reset page: back to setup mode, keeping the database.

use std::collections::HashMap;
use std::sync::Arc;

use askama::Template;
use axum::extract::{Form, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::Response;

use super::{
    MessagePage, Nav, Return, WebState, check_form, clear_admin_cookie, gate, nav, step_up,
};
use crate::common::{NO_STORE, render_private};

/// The reset page.
#[derive(Template)]
#[template(path = "reset.html")]
pub struct ResetPage {
    /// Navigation.
    pub nav: Nav,
    /// The session's form token, posted back as the hidden `csrf` field.
    pub csrf: String,
    /// `server.hostname`: what the operator must type to confirm.
    pub hostname: String,
    /// The config comes from the environment alone, so there is no file
    /// to remove: the page shows a note in place of the form.
    pub unavailable: bool,
    /// Why the last submit was refused (wrong hostname, a failed step),
    /// shown in a banner; `None` on a plain view.
    pub error: Option<String>,
}

pub(super) async fn reset_page(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
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

/// Why a reset stopped part-way.
#[derive(Debug, thiserror::Error)]
pub enum ResetError {
    /// Revoking the sessions or the API keys failed.
    #[error(transparent)]
    Storage(#[from] farsight_storage::StorageError),
    /// Removing the config file or writing the setup token failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Performs the reset: revokes all admin sessions and API keys, deletes
/// `config.toml`, writes a new setup token, notifies, and asks the server
/// to switch to setup mode. Database contents are kept.
pub async fn perform_reset(st: &WebState) -> Result<(), ResetError> {
    farsight_storage::auth::delete_all_sessions(&st.api.pool).await?;
    farsight_storage::auth::revoke_all_tokens(&st.api.pool).await?;
    // The keys stop working now, not at the next refresh.
    st.api.keys.evict_all();
    std::fs::remove_file(st.api.config.path())?;
    let t = crate::setup_token::rotate(&st.token_path)?;
    crate::setup_token::print(&t);
    let _ = farsight_api::config_store::notify_config(&st.api.pool).await;
    tracing::warn!("configuration reset by an admin; switching to setup mode");
    let _ = st.reset.send(true);
    Ok(())
}

pub(super) async fn reset_submit(
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
    // A reset ends every session and every key and opens the setup
    // wizard: a fresh sign-in first.
    if let Err(r) = step_up(&s, Return::Settings) {
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
                  database is kept. A new setup token is in the server log (docker \
                  compose logs farsight). Farsight is switching to setup mode."
            .into(),
        link: Some(("/setup".into(), "Open setup".into())),
    });
    clear_admin_cookie(&mut r);
    r.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    r
}
