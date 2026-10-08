//! `farsight`: the composition root (see `docs/design/README.md` and
//! `docs/design/web-ui.md`). Decides setup vs normal mode at start-up,
//! serves the setup wizard or ingest + API + UI, and switches between the
//! two in-process (setup → normal when the wizard finishes, normal →
//! setup on a config reset), independent of the container's restart
//! policy.
//!
//! `farsight setup-token [--rotate]` prints (or replaces) the setup token.
//! `farsight set-admin-did <did> [--force]` sets the admin account in
//! `config.toml`; `farsight admin-did` prints it.

#![warn(missing_docs)]

mod error;
mod health;
mod metrics_http;
mod normal;
mod setup_mode;
mod sort_indexes;
mod tasks;

use std::path::PathBuf;
use std::process::ExitCode;

use farsight_core::config::{self, StartMode};
use tokio::sync::watch;

/// Binary version (`getStats.service.version`, User-Agent).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Overrides the config path (default `/etc/farsight/config.toml`). Not a
/// `FARSIGHT__` key: it locates the file rather than setting a key in it.
pub const CONFIG_PATH_ENV: &str = "FARSIGHT_CONFIG";

/// Restricts the setup listener, e.g. `127.0.0.1` or `127.0.0.1:8080`.
pub const SETUP_BIND_ENV: &str = "FARSIGHT_SETUP_BIND";

/// How a mode ended without an error: what `main`'s loop does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeEnd {
    /// The process is stopping (signal).
    Shutdown,
    /// Enter the other mode in this process: setup mode ends this way
    /// when the wizard has written the config, normal mode when the
    /// config was reset.
    Switch,
}

fn config_path() -> PathBuf {
    std::env::var_os(CONFIG_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(config::DEFAULT_CONFIG_PATH))
}

fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,hyper=warn"));
    // Structured JSON logs.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .init();
}

fn setup_token_command(args: &[String]) -> ExitCode {
    let path = config_path();
    if path.exists() {
        eprintln!(
            "{} exists: Farsight is configured and has no setup token.",
            path.display()
        );
        return ExitCode::from(1);
    }
    let token_path = farsight_web::setup_token::token_path(&path);
    let rotate = args.iter().any(|a| a == "--rotate");
    let t = if rotate {
        farsight_web::setup_token::rotate(&token_path)
    } else {
        match farsight_web::setup_token::read(&token_path) {
            Some(t) => Ok(t),
            None => farsight_web::setup_token::rotate(&token_path),
        }
    };
    match t {
        Ok(t) => {
            println!("{}", t.token);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("reading {}: {e}", token_path.display());
            ExitCode::from(1)
        }
    }
}

/// The environment variable that sets `access.admin_did`.
const ADMIN_DID_ENV: &str = "FARSIGHT__ACCESS__ADMIN_DID";

fn env_admin_did() -> Option<String> {
    std::env::var(ADMIN_DID_ENV).ok().filter(|v| !v.is_empty())
}

/// `farsight admin-did`: the effective admin DID and where it comes from.
/// No network.
fn admin_did_command() -> ExitCode {
    if let Some(did) = env_admin_did() {
        println!("{did}");
        println!("(from the environment: {ADMIN_DID_ENV})");
        return ExitCode::SUCCESS;
    }
    let path = config_path();
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("(not set: there is no {})", path.display());
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("reading {}: {e}", path.display());
            return ExitCode::from(1);
        }
    };
    let did = text.parse::<toml::Table>().ok().and_then(|t| {
        t.get("access")
            .and_then(|a| a.get("admin_did"))
            .and_then(toml::Value::as_str)
            .filter(|d| !d.is_empty())
            .map(str::to_owned)
    });
    match did {
        Some(did) => {
            println!("{did}");
            println!("(from {})", path.display());
        }
        None => println!("(not set)"),
    }
    ExitCode::SUCCESS
}

/// `farsight set-admin-did <did> [--force]`: the way to change the admin
/// account, and the recovery when the account is lost or was mistyped.
/// Edits `config.toml` only; the running server does not re-read the
/// file, so the change applies at its next start.
fn set_admin_did_command(args: &[String]) -> ExitCode {
    let force = args.iter().any(|a| a == "--force");
    let dids: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let [did] = dids.as_slice() else {
        eprintln!("usage: farsight set-admin-did <did> [--force]");
        return ExitCode::from(2);
    };
    let path = config_path();
    if !path.exists() {
        eprintln!(
            "There is no {}: the configuration comes from the environment, or setup has not \
             run. Set {ADMIN_DID_ENV} and restart farsight.",
            path.display()
        );
        return ExitCode::from(1);
    }
    if env_admin_did().is_some() {
        eprintln!(
            "{ADMIN_DID_ENV} is set in the environment and overrides the file. Change it there \
             and restart farsight."
        );
        return ExitCode::from(1);
    }
    if !config::valid_admin_did(did) {
        eprintln!(
            "{did} is not an admin DID: expected did:plc: followed by 24 characters of a-z and \
             2-7, or did:web: followed by a hostname. A handle will not do."
        );
        return ExitCode::from(1);
    }
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("reading {}: {e}", path.display());
            return ExitCode::from(1);
        }
    };
    let env: Vec<(String, String)> = std::env::vars().collect();
    let new_text = match config::set_admin_did(&text, did) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{}: {e}", path.display());
            return ExitCode::from(1);
        }
    };
    // The file need not load before the edit; the result must.
    let loaded = match config::load_from_parts(Some(&new_text), &env) {
        Ok(l) => l,
        Err(e) => {
            eprintln!(
                "{} would not load with the admin DID set: {e}. Nothing was changed.",
                path.display()
            );
            return ExitCode::from(1);
        }
    };
    // Look the DID up before writing: a typo is cheaper to catch here.
    // `--force` skips the refusal, for a recovery while a directory is
    // down.
    let identity = resolve_for_cli(&loaded.config, did);
    if let Err(e) = &identity {
        if !force {
            eprintln!(
                "{did} could not be resolved ({e}). Nothing was changed. Use --force to set it \
                 anyway."
            );
            return ExitCode::from(1);
        }
        eprintln!("warning: {did} could not be resolved ({e}); setting it anyway (--force)");
    }
    if let Err(e) = config::write_replace(&path, &new_text) {
        eprintln!(
            "writing {} failed: {e}. Nothing was changed.",
            path.display()
        );
        return ExitCode::from(1);
    }
    println!("Admin DID set to {did}");
    match identity.ok().and_then(|i| i.handle) {
        Some(h) => println!("Handle: @{h}"),
        None => println!("Handle: none verified"),
    }
    println!();
    println!("Restart farsight to apply (`docker restart farsight`).");
    ExitCode::SUCCESS
}

fn resolve_for_cli(
    cfg: &config::Config,
    did: &str,
) -> Result<farsight_web::oauth::Identity, error::LookupError> {
    use farsight_core::net::{SafeClient, SafeClientConfig};
    use farsight_web::oauth::OAuthError;
    let parsed = farsight_core::Did::parse(did).map_err(OAuthError::from)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let safe = SafeClient::new(SafeClientConfig::from_config(cfg, VERSION));
        match tokio::time::timeout(
            std::time::Duration::from_secs(20),
            farsight_web::oauth::identity(&safe, &cfg.backfill.plc_url, &parsed),
        )
        .await
        {
            Ok(r) => Ok(r?),
            Err(_) => Err(OAuthError::TimedOut.into()),
        }
    })
}

fn shutdown_signal() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        let term = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut s) => {
                    s.recv().await;
                }
                Err(_) => std::future::pending::<()>().await,
            }
        };
        #[cfg(not(unix))]
        let term = std::future::pending::<()>();
        tokio::select! {
            _ = ctrl_c => {}
            _ = term => {}
        }
        tracing::info!("shutting down");
        let _ = tx.send(true);
    });
    rx
}

async fn run() -> ExitCode {
    let path = config_path();
    let env: Vec<(String, String)> = std::env::vars().collect();
    let shutdown = shutdown_signal();
    let metrics = metrics_http::install();
    loop {
        // An unparseable or invalid config exits non-zero and is never
        // rewritten.
        let mode = match config::load(&path, &env) {
            Ok(m) => m,
            Err(e) => {
                let source = if path.exists() {
                    path.display().to_string()
                } else {
                    "the environment (FARSIGHT_SKIP_WIZARD)".to_owned()
                };
                tracing::error!(error = %e, source, "invalid configuration");
                eprintln!("farsight: invalid configuration in {source}: {e}");
                return ExitCode::from(1);
            }
        };
        let end = match mode {
            StartMode::Setup => setup_mode::run(path.clone(), env.clone(), shutdown.clone()).await,
            StartMode::Normal(loaded) => {
                normal::run(
                    *loaded,
                    path.clone(),
                    env.clone(),
                    shutdown.clone(),
                    metrics.clone(),
                )
                .await
            }
        };
        match end {
            Ok(ModeEnd::Shutdown) => return ExitCode::SUCCESS,
            Ok(ModeEnd::Switch) => continue,
            Err(e) => {
                tracing::error!(error = %e, "fatal");
                eprintln!("farsight: {e}");
                return ExitCode::from(1);
            }
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("setup-token") {
        return setup_token_command(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("set-admin-did") {
        return set_admin_did_command(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("admin-did") {
        return admin_did_command();
    }
    if let Some(a) = args.first() {
        if a == "--version" || a == "-V" {
            println!("farsight {VERSION}");
            return ExitCode::SUCCESS;
        }
        eprintln!(
            "usage: farsight [setup-token [--rotate] | set-admin-did <did> [--force] | admin-did]"
        );
        return ExitCode::from(2);
    }
    init_logging();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("farsight: starting the runtime failed: {e}");
            return ExitCode::from(1);
        }
    };
    rt.block_on(run())
}

#[cfg(test)]
mod tests {
    #[test]
    fn compose_names_the_image_of_this_version() {
        let compose = include_str!("../../../compose.yml");
        let want = format!("image: ghcr.io/skydeval/farsight:{}", super::VERSION);
        assert!(
            compose.lines().any(|l| l.trim() == want),
            "compose.yml must name `{want}`"
        );
        assert!(!compose.contains("farsight:latest"));
    }
}
