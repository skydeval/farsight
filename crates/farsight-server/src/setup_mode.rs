//! Setup mode (see `docs/design/web-ui.md`): `/setup/*`, `/livez`,
//! `/health` (503 setup body) and static assets; `/xrpc/*` ⇒ `503
//! SetupRequired`. No database, no firehose. Ends when the wizard writes
//! `config.toml` or the process is asked to stop.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use farsight_api::clientip::{CfTracker, ProxyTrust};
use farsight_api::{IpLayer, client_ip_middleware};
use farsight_web::setup_token;
use tokio::sync::watch;

use crate::{ModeEnd, SETUP_BIND_ENV, VERSION, health};

/// The setup listener address: `FARSIGHT_SETUP_BIND` (an address, or an
/// IP combined with the default port), else `server.bind`'s default.
pub fn setup_bind(env: &[(String, String)]) -> String {
    let default = farsight_core::Config::default().server.bind;
    let port = default
        .rsplit_once(':')
        .map_or("8080", |(_, p)| p)
        .to_owned();
    match env
        .iter()
        .find(|(k, _)| k == SETUP_BIND_ENV)
        .map(|(_, v)| v.trim())
    {
        Some(v) if v.parse::<SocketAddr>().is_ok() => v.to_owned(),
        Some(v) if v.parse::<std::net::IpAddr>().is_ok() => {
            let ip: std::net::IpAddr = v.parse().expect("checked");
            SocketAddr::new(ip, port.parse().unwrap_or(8080)).to_string()
        }
        Some(v) if !v.is_empty() => {
            tracing::warn!(value = v, "ignoring unparseable FARSIGHT_SETUP_BIND");
            default
        }
        _ => default,
    }
}

/// Runs setup mode.
pub async fn run(
    config_path: PathBuf,
    env: Vec<(String, String)>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<ModeEnd, String> {
    let (state, mut completed) = farsight_web::SetupState::new(config_path, env.clone(), VERSION);
    // Print the token on every setup-mode boot, creating or rotating it
    // as needed.
    let (token, rotated) = state.check_token().map_err(|e| {
        format!(
            "cannot write the setup token next to the config ({}): {e}",
            state.token_path.display()
        )
    })?;
    if !rotated {
        setup_token::print(&token);
    }

    let layer = IpLayer {
        config: None,
        trust: Arc::new(ProxyTrust::default()),
        cf: Arc::new(CfTracker::default()),
    };
    let app = Router::new()
        .merge(farsight_web::setup::router(state.clone()))
        .merge(farsight_api::setup_router())
        .route("/health", get(health::setup_health))
        .route("/livez", get(health::livez))
        .layer(axum::middleware::from_fn_with_state(
            layer,
            client_ip_middleware,
        ));

    let bind = setup_bind(&env);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("binding the setup listener on {bind}: {e}"))?;
    tracing::info!(bind, "setup mode: serving the wizard");

    // Token housekeeping: expiry check every minute, re-print every 10.
    let ticker = {
        let state = state.clone();
        tokio::spawn(farsight_core::task::supervise("setup_token", move || {
            let state = state.clone();
            async move {
                let mut minute = tokio::time::interval(Duration::from_secs(60));
                let mut since_print = Duration::ZERO;
                minute.tick().await;
                loop {
                    minute.tick().await;
                    since_print += Duration::from_secs(60);
                    match state.check_token() {
                        Ok((_, true)) => since_print = Duration::ZERO,
                        Ok((t, false)) if since_print >= setup_token::REPRINT => {
                            since_print = Duration::ZERO;
                            setup_token::print(&t);
                        }
                        Ok(_) => {}
                        Err(e) => tracing::error!(error = %e, "setup token file"),
                    }
                }
            }
        }))
    };

    let mut stop = shutdown.clone();
    let mut done = completed.clone();
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        tokio::select! {
            _ = async { while !*stop.borrow() { if stop.changed().await.is_err() { break; } } } => {}
            _ = async { while !*done.borrow() { if done.changed().await.is_err() { break; } } } => {
                // Let the "done" page reach the browser first.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    });
    let r = server.await;
    ticker.abort();
    r.map_err(|e| format!("setup listener: {e}"))?;
    if *completed.borrow_and_update() {
        tracing::info!("setup complete; switching to normal mode in-process");
        return Ok(ModeEnd::Switch);
    }
    let _ = shutdown.borrow_and_update();
    Ok(ModeEnd::Shutdown)
}
