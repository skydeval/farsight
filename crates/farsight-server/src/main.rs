//! `farsight`: the composition root (design §2.1, §8.2). Decides setup
//! vs normal mode at start-up, serves the setup wizard or ingest + API +
//! UI, and switches between the two in-process (setup → normal when the
//! wizard finishes, normal → setup on a config reset), independent of the
//! container's restart policy.
//!
//! `farsight setup-token [--rotate]` prints (or replaces) the setup token.

#![warn(missing_docs)]

mod health;
mod metrics_http;
mod normal;
mod setup_mode;
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

/// Restricts the setup listener (§8.3), e.g. `127.0.0.1` or
/// `127.0.0.1:8080`.
pub const SETUP_BIND_ENV: &str = "FARSIGHT_SETUP_BIND";

/// How a mode ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeEnd {
    /// The process is stopping (signal).
    Shutdown,
    /// Switch to the other mode.
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
    // Structured JSON logs (§13).
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
        // rewritten (§8.2).
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
    if let Some(a) = args.first() {
        if a == "--version" || a == "-V" {
            println!("farsight {VERSION}");
            return ExitCode::SUCCESS;
        }
        eprintln!("usage: farsight [setup-token [--rotate]]");
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
