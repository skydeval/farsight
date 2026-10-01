//! Jetstream ingestion for Farsight: subscription (v2 preferred, v1
//! fallback), cursor persistence, gap and lag tracking, and the batching writer.
//!
//! ```text
//! ws reader ──► bounded channel (10k events) ──► single writer ──► farsight-storage::apply
//! ```
//!
//! [`Ingest::start`] spawns both tasks and returns an [`IngestHandle`].

#![warn(missing_docs)]

pub mod conn;
pub mod dict;
pub mod frame;
pub mod lag;
pub mod metrics;
pub mod reader;
pub mod resume;
pub mod stats;
pub mod writer;

use std::sync::Arc;
use std::time::Duration;

use farsight_core::Config;
use farsight_storage::counters::{CounterSink, FLUSH_INTERVAL};
use farsight_storage::keys::Limits;
use farsight_storage::txn::Gates;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub use reader::Control;
pub use stats::{IngestStats, StatsSnapshot};

/// Channel capacity between reader and writer (§6.2).
pub const CHANNEL_CAPACITY: usize = 10_000;

/// Ingest connection pool size (§6.2: own pool, 4 connections).
pub const POOL_SIZE: u32 = 4;

/// Fault injection for the Phase B harness: events from these DIDs fail
/// every apply attempt, exercising the poisoned-event path.
#[cfg(feature = "harness")]
#[derive(Debug, Default)]
pub struct FaultHook {
    /// DIDs whose events fail.
    pub poison_dids: std::sync::Mutex<std::collections::HashSet<String>>,
}

/// Ingest settings derived from the config.
#[derive(Debug, Clone)]
pub struct IngestConfig {
    /// Reader settings.
    pub reader: reader::ReaderConfig,
    /// Limits for `apply`.
    pub limits: Limits,
    /// Harness: receives a copy of every event read from the network.
    #[cfg(feature = "harness")]
    pub tap: Option<mpsc::UnboundedSender<frame::InEvent>>,
}

impl IngestConfig {
    /// From a loaded config.
    pub fn from_config(c: &Config) -> IngestConfig {
        let t = &c.firehose.tuning;
        IngestConfig {
            reader: reader::ReaderConfig {
                urls: c.firehose.urls.clone(),
                tuning: resume::Tuning {
                    gap_threshold: t.gap_threshold.get(),
                    failover_rewind_min: t.failover_rewind_min.get(),
                    failover_max_lag: t.failover_max_lag.get(),
                },
                stall_timeout: t.stall_timeout.get(),
                compress: true,
            },
            limits: Limits::from_config(c),
            #[cfg(feature = "harness")]
            tap: None,
        }
    }
}

/// A running ingest.
pub struct IngestHandle {
    /// Commands to the reader.
    pub control: mpsc::Sender<Control>,
    /// Live statistics.
    pub stats: Arc<IngestStats>,
    /// The counter sink (approximate counters).
    pub counters: Arc<CounterSink>,
    /// Fault injection (harness only).
    #[cfg(feature = "harness")]
    pub faults: Arc<FaultHook>,
    tasks: Vec<JoinHandle<()>>,
    flusher: JoinHandle<()>,
    pool: PgPool,
    limits: Limits,
}

impl IngestHandle {
    /// Stops reading, lets the writer drain, waits for both tasks and
    /// flushes the approximate counters one last time.
    pub async fn shutdown(self) {
        let _ = self.control.send(Control::Shutdown).await;
        for t in self.tasks {
            let _ = tokio::time::timeout(Duration::from_secs(30), t).await;
        }
        self.flusher.abort();
        if let Err(e) = self.counters.flush(&self.pool, &self.limits).await {
            tracing::warn!(error = %e, "final counter flush failed");
        }
    }
}

/// Entry point.
pub struct Ingest;

impl Ingest {
    /// Starts the reader, the writer and the counter flusher. `pool` should
    /// be the dedicated 4-connection ingest pool. Accounts left half-purged
    /// by a crash are purged first.
    pub async fn start(cfg: IngestConfig, pool: PgPool) -> Result<IngestHandle, String> {
        if cfg.reader.urls.is_empty() {
            return Err("firehose.urls is empty".into());
        }
        // One TLS provider for the websocket client; ignore "already set".
        let _ = rustls::crypto::ring::default_provider().install_default();
        let counters = Arc::new(CounterSink::new(0));
        let stats = Arc::new(IngestStats::default());
        #[cfg(feature = "harness")]
        let faults = Arc::new(FaultHook::default());

        let pending = farsight_storage::janitor::accounts_pending_purge(&pool, 1000)
            .await
            .map_err(|e| e.to_string())?;
        for did in &pending {
            farsight_storage::janitor::purge_account(&pool, &cfg.limits, &counters, did)
                .await
                .map_err(|e| e.to_string())?;
        }

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (ctl_tx, ctl_rx) = mpsc::channel(64);
        let writer = writer::Writer {
            pool: pool.clone(),
            limits: cfg.limits.clone(),
            gates: Gates::default(),
            counters: counters.clone(),
            stats: stats.clone(),
            #[cfg(feature = "harness")]
            faults: faults.clone(),
        };
        let reader = reader::Reader {
            cfg: cfg.reader.clone(),
            pool: pool.clone(),
            tx,
            control: ctl_rx,
            stats: stats.clone(),
            #[cfg(feature = "harness")]
            tap: cfg.tap.clone(),
            #[cfg(feature = "harness")]
            rewind: None,
            repairs: reader::Repairs::default(),
        };
        let flusher = {
            let counters = counters.clone();
            let pool = pool.clone();
            let limits = cfg.limits.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(FLUSH_INTERVAL);
                loop {
                    tick.tick().await;
                    if let Err(e) = counters.flush(&pool, &limits).await {
                        tracing::warn!(error = %e, "counter flush failed");
                    }
                }
            })
        };
        let tasks = vec![tokio::spawn(writer.run(rx)), tokio::spawn(reader.run())];
        Ok(IngestHandle {
            control: ctl_tx,
            stats,
            counters,
            #[cfg(feature = "harness")]
            faults,
            tasks,
            flusher,
            pool,
            limits: cfg.limits,
        })
    }
}
