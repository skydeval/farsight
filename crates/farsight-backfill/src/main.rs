//! `farsight-backfill`: the background backfill process (design §5). Same
//! image as the server, config volume read-only; idles until the server's
//! setup wizard has written a config.

use std::process::ExitCode;

use tokio::sync::watch;

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

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(a) = args.first() {
        if a == "--version" || a == "-V" {
            println!("farsight-backfill {}", farsight_backfill::VERSION);
            return ExitCode::SUCCESS;
        }
        eprintln!("usage: farsight-backfill");
        return ExitCode::from(2);
    }
    init_logging();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("farsight-backfill: starting the runtime failed: {e}");
            return ExitCode::from(1);
        }
    };
    rt.block_on(async {
        let shutdown = shutdown_signal();
        let metrics = farsight_backfill::install_metrics();
        match farsight_backfill::run(farsight_backfill::config_path(), shutdown, metrics).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                tracing::error!(error = %e, "fatal");
                eprintln!("farsight-backfill: {e}");
                ExitCode::from(1)
            }
        }
    })
}
