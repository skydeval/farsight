//! Jetstream ingestion for Farsight: subscription (v2 preferred, v1
//! fallback), cursor persistence, gap and lag tracking, and the batching writer.
//!
//! ```text
//! ws reader ──► bounded channel (10k events) ──► single writer ──► farsight-storage::apply
//! ```
//!
//! [`Ingest::start`] spawns both tasks and returns an [`IngestHandle`].
//!
//! The reader and the writer are one pipeline: a batch the writer holds
//! exists nowhere else, and the reader's position is only as good as what
//! the writer committed. If either of them panics, the other ends with
//! it, the panic is logged and counted, and [`IngestHandle::failed`]
//! reports it; the embedding process then exits, and the next start
//! resumes from the persisted cursor. The counter flusher keeps no state
//! and is started again in place.

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
use farsight_storage::gates::SharedGates;
use farsight_storage::keys::Limits;
use sqlx::PgPool;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub use reader::Control;
pub use stats::{IngestStats, StatsSnapshot};

/// Channel capacity between reader and writer.
pub const CHANNEL_CAPACITY: usize = 10_000;

/// Ingest connection pool size (own pool, 4 connections).
pub const POOL_SIZE: u32 = 4;

/// Task name of the reader (panic metric, [`IngestHandle::failed`]).
pub const READER_TASK: &str = "ingest_reader";
/// Task name of the writer.
pub const WRITER_TASK: &str = "ingest_writer";
/// Task name of the counter flusher.
pub const FLUSHER_TASK: &str = "ingest_counter_flush";
/// The tasks ingest runs.
pub const TASKS: [&str; 3] = [READER_TASK, WRITER_TASK, FLUSHER_TASK];

/// Fault injection for the harness: events from these DIDs fail every
/// apply attempt, exercising the poisoned-event path.
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
    /// Write gates, shared with the budget monitor.
    pub gates: Arc<SharedGates>,
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
                seam: reader::SeamRepair {
                    before: t.seam_repair_before.get(),
                    after: t.seam_repair_after.get(),
                    delay: t.seam_repair_delay.get(),
                    catchup_margin: t.seam_repair_catchup_margin.get(),
                },
            },
            limits: Limits::from_config(c),
            gates: Arc::new(SharedGates::default()),
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
    failed: watch::Receiver<Option<&'static str>>,
    pool: PgPool,
    limits: Limits,
}

/// Runs one half of the pipeline; a panic is reported and published on
/// `failed` with the task's name.
async fn pipeline_task(
    name: &'static str,
    run: impl std::future::Future<Output = ()>,
    failed: watch::Sender<Option<&'static str>>,
) {
    if let Err(message) = farsight_core::task::catch(run).await {
        farsight_core::task::report_panic(name, &message);
        let _ = failed.send(Some(name));
    }
}

/// The dashboard / `getStats` firehose fields.
#[derive(Debug, Clone, PartialEq)]
pub struct FirehoseStatus {
    /// `firehoseConnected` (the persisted flag; disconnection is a
    /// synthetic gap).
    pub connected: bool,
    /// Protocol of the last committed batch.
    pub protocol: Option<frame::Protocol>,
    /// `lagSeconds`: now − `applied_through`.
    pub lag_seconds: Option<f64>,
    /// `sourceLagSeconds`: now − median rev time of the last 1000 events.
    pub source_lag_seconds: Option<f64>,
    /// `openGaps`: unhealed gaps.
    pub open_gaps: usize,
}

/// Reads the firehose status from `firehose_state`, the unhealed gap set
/// and (for source lag) the running ingest's in-memory window.
pub async fn firehose_status(
    pool: &PgPool,
    stats: Option<&IngestStats>,
) -> Result<FirehoseStatus, farsight_storage::StorageError> {
    let st = farsight_storage::firehose::read_state(pool).await?;
    let gaps = farsight_storage::firehose::unhealed_gaps(pool).await?;
    let now = chrono::Utc::now();
    Ok(FirehoseStatus {
        connected: st.connected,
        protocol: st.protocol.map(|p| match p {
            farsight_storage::codes::Protocol::V1 => frame::Protocol::V1,
            farsight_storage::codes::Protocol::V2 => frame::Protocol::V2,
        }),
        lag_seconds: st
            .applied_through
            .map(|a| ((now - a).num_milliseconds() as f64 / 1000.0).max(0.0)),
        source_lag_seconds: stats.and_then(|s| {
            let ms = s.source_lag_ms.load(std::sync::atomic::Ordering::Relaxed);
            (ms >= 0).then(|| ms as f64 / 1000.0)
        }),
        open_gaps: gaps.len(),
    })
}

impl IngestHandle {
    /// [`firehose_status`] for this ingest.
    pub async fn status(&self) -> Result<FirehoseStatus, farsight_storage::StorageError> {
        firehose_status(&self.pool, Some(&self.stats)).await
    }

    /// Resolves with the name of the pipeline task that panicked. The
    /// pipeline is over by then and is not started again in this process:
    /// the caller exits. Never resolves while ingest runs or after an
    /// orderly shutdown.
    pub fn failed(&self) -> impl std::future::Future<Output = &'static str> + Send + 'static {
        let mut rx = self.failed.clone();
        async move {
            loop {
                if let Some(name) = *rx.borrow_and_update() {
                    return name;
                }
                if rx.changed().await.is_err() {
                    return std::future::pending().await;
                }
            }
        }
    }

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
        stats
            .source_lag_ms
            .store(-1, std::sync::atomic::Ordering::Relaxed);
        #[cfg(feature = "harness")]
        let faults = Arc::new(FaultHook::default());

        let pending = farsight_storage::janitor::accounts_pending_purge(&pool, 1000)
            .await
            .map_err(|e| e.to_string())?;
        // One account whose purge fails does not keep the process from
        // starting: the failure is recorded and the purge is taken up
        // again by the daily task.
        for did in &pending {
            if let Err(e) =
                farsight_storage::janitor::purge_account(&pool, &cfg.limits, &counters, did).await
            {
                tracing::error!(%did, error = %e, "purging a deleted account failed");
                ::metrics::counter!(crate::metrics::STORAGE_ERRORS, "op" => "purge_account")
                    .increment(1);
                let _ = farsight_storage::auth::record_op_error(
                    &pool,
                    "ingest",
                    Some(did.as_str()),
                    &format!("purge_account failed: {e}"),
                )
                .await;
            }
        }

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (ctl_tx, ctl_rx) = mpsc::channel(64);
        let writer = writer::Writer {
            pool: pool.clone(),
            limits: cfg.limits.clone(),
            gates: cfg.gates.clone(),
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
            pending_inject: Vec::new(),
        };
        let flusher = {
            let counters = counters.clone();
            let pool = pool.clone();
            let limits = cfg.limits.clone();
            tokio::spawn(farsight_core::task::supervise(FLUSHER_TASK, move || {
                let (counters, pool, limits) = (counters.clone(), pool.clone(), limits.clone());
                async move {
                    let mut tick = tokio::time::interval(FLUSH_INTERVAL);
                    loop {
                        tick.tick().await;
                        if let Err(e) = counters.flush(&pool, &limits).await {
                            tracing::warn!(error = %e, "counter flush failed");
                        }
                    }
                }
            }))
        };
        let (failed_tx, failed) = watch::channel(None);
        let tasks = vec![
            tokio::spawn(pipeline_task(
                WRITER_TASK,
                writer.run(rx),
                failed_tx.clone(),
            )),
            tokio::spawn(pipeline_task(READER_TASK, reader.run(), failed_tx)),
        ];
        Ok(IngestHandle {
            control: ctl_tx,
            stats,
            counters,
            #[cfg(feature = "harness")]
            faults,
            tasks,
            flusher,
            failed,
            pool,
            limits: cfg.limits,
        })
    }
}
