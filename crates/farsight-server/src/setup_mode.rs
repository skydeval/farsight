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
use farsight_core::listen;
use farsight_web::setup_token;
use tokio::sync::watch;

use crate::error::ServerError;
use crate::{ModeEnd, SETUP_BIND_ENV, VERSION, health};

/// The setup listener address: `FARSIGHT_SETUP_BIND` (an address, or an
/// IP combined with the default port), else `server.bind`'s default. A
/// value that is neither is an error: the variable is set to keep the
/// wizard off some network, and falling back to every interface would
/// do the opposite of what was asked.
pub fn setup_bind(env: &[(String, String)]) -> Result<String, String> {
    let default = farsight_core::Config::default().server.bind;
    let port = default
        .rsplit_once(':')
        .map_or("8080", |(_, p)| p)
        .to_owned();
    let Some(v) = env
        .iter()
        .find(|(k, _)| k == SETUP_BIND_ENV)
        .map(|(_, v)| v.trim())
        .filter(|v| !v.is_empty())
    else {
        return Ok(default);
    };
    if v.parse::<SocketAddr>().is_ok() {
        return Ok(v.to_owned());
    }
    match v.parse::<std::net::IpAddr>() {
        Ok(ip) => Ok(SocketAddr::new(ip, port.parse().unwrap_or(8080)).to_string()),
        Err(_) => Err(format!(
            "{SETUP_BIND_ENV} is {v:?}: expected an IP address, or an address with a port"
        )),
    }
}

/// Runs setup mode until `shutdown` flips ([`ModeEnd::Shutdown`]) or the
/// wizard has written `config_path` ([`ModeEnd::Switch`], and the caller
/// loads it and enters normal mode). Prints the setup token at start.
pub async fn run(
    config_path: PathBuf,
    env: Vec<(String, String)>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<ModeEnd, ServerError> {
    let (state, mut completed) = farsight_web::SetupState::new(config_path, env.clone(), VERSION);
    // Print the token on every setup-mode boot, creating or rotating it
    // as needed.
    let (token, rotated) = state.check_token().map_err(ServerError::io(format!(
        "cannot write the setup token next to the config ({})",
        state.token_path.display()
    )))?;
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

    let bind = setup_bind(&env).map_err(|e| {
        ServerError::io("the setup listener's address")(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            e,
        ))
    })?;
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(ServerError::io(format!(
            "binding the setup listener on {bind}"
        )))?;
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
    let (stopping_tx, stopping_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(
        listen::guarded(listener, listen::MAX_CONNECTIONS),
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
        let _ = stopping_tx.send(());
    })
    .into_future();
    // A connection still open after the drain time is left behind: the
    // exit, or the switch to normal mode, does not wait for it.
    let r = listen::drain_within(server, stopping_rx, listen::DRAIN)
        .await
        .unwrap_or(Ok(()));
    ticker.abort();
    r.map_err(ServerError::io("setup listener"))?;
    if *completed.borrow_and_update() {
        tracing::info!("setup complete; switching to normal mode in-process");
        return Ok(ModeEnd::Switch);
    }
    let _ = shutdown.borrow_and_update();
    Ok(ModeEnd::Shutdown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_setup_bind_that_cannot_be_read_is_refused_not_ignored() {
        let env = |v: &str| vec![(SETUP_BIND_ENV.to_owned(), v.to_owned())];
        assert_eq!(setup_bind(&env("127.0.0.1")).unwrap(), "127.0.0.1:8080");
        assert_eq!(
            setup_bind(&env("127.0.0.1:9000")).unwrap(),
            "127.0.0.1:9000"
        );
        assert_eq!(setup_bind(&env(" ::1 ")).unwrap(), "[::1]:8080");
        // Unset or empty: the default listener.
        assert!(setup_bind(&[]).unwrap().ends_with(":8080"));
        assert!(setup_bind(&env("")).unwrap().ends_with(":8080"));
        // Anything else is an error, never every interface.
        for bad in ["localhost", "127.0.0.1:", "127.0.0.1/8", "lo"] {
            assert!(setup_bind(&env(bad)).is_err(), "{bad}");
        }
    }
}
