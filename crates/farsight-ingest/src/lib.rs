//! Jetstream ingestion for Farsight: subscription (v2 preferred, v1
//! fallback), cursor persistence, gap and lag tracking, and the batching writer.
//!
//! ```text
//! ws reader ──► bounded channel (10k events) ──► single writer ──► farsight-storage::apply
//! ```
//!
//! [`Ingest::start`] spawns both tasks and returns an [`IngestHandle`].
//! Beside them run the seam repair task ([`seam`]), which reads the
//! recorded seam windows again and feeds the same writer, and a task
//! that keeps the gauges ([`gauges`]).
//!
//! The reader and the writer are one pipeline: a batch the writer holds
//! exists nowhere else, and the reader's position is only as good as what
//! the writer committed. If either of them panics, the other ends with
//! it, the panic is logged and counted, and [`IngestHandle::failed`]
//! reports it; the embedding process then exits, and the next start
//! resumes from the persisted cursor. The counter flusher, the seam
//! repair task and the gauge task keep no state of their own and are
//! started again in place.

#![warn(missing_docs)]

pub mod conn;
pub mod dict;
pub mod frame;
pub mod lag;
pub mod metrics;
pub mod reader;
pub mod resume;
pub mod seam;
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
/// Task name of the seam repair task.
pub const SEAM_TASK: &str = "ingest_seam_repair";
/// Task name of the gauge task.
pub const GAUGES_TASK: &str = "ingest_gauges";
/// The tasks ingest runs.
pub const TASKS: [&str; 5] = [
    READER_TASK,
    WRITER_TASK,
    FLUSHER_TASK,
    SEAM_TASK,
    GAUGES_TASK,
];

/// How often the gauge task sets the gauges.
pub const GAUGE_PERIOD: Duration = Duration::from_secs(1);
/// Longest the gauge task waits for one read of the database.
pub const GAUGE_READ_TIMEOUT: Duration = Duration::from_secs(2);

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
    /// Reader settings: instances, resume tuning, stall timeout, seam
    /// repair.
    pub reader: reader::ReaderConfig,
    /// Caps the writer hands to `apply` with every batch.
    pub limits: Limits,
    /// Write gates, shared with the budget monitor.
    pub gates: Arc<SharedGates>,
    /// Harness: receives a copy of every event read from the network.
    #[cfg(feature = "harness")]
    pub tap: Option<mpsc::UnboundedSender<frame::InEvent>>,
}

impl IngestConfig {
    /// Derives the settings from a loaded config: `firehose.urls`,
    /// `[firehose.tuning]` and the limits. zstd frames are always
    /// requested, and the gates are a fresh set for the caller to share.
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

/// A running ingest: the handles to command it, observe it and shut it
/// down.
pub struct IngestHandle {
    /// Commands to the reader.
    pub control: mpsc::Sender<Control>,
    /// Live statistics, updated by the reader and the writer.
    pub stats: Arc<IngestStats>,
    /// The counter sink (approximate counters).
    pub counters: Arc<CounterSink>,
    /// Fault injection (harness only).
    #[cfg(feature = "harness")]
    pub faults: Arc<FaultHook>,
    tasks: Vec<JoinHandle<()>>,
    flusher: JoinHandle<()>,
    gauges: JoinHandle<()>,
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

/// `now − applied`, in seconds, never negative.
pub fn lag_seconds(
    now: chrono::DateTime<chrono::Utc>,
    applied: chrono::DateTime<chrono::Utc>,
) -> f64 {
    ((now - applied).num_milliseconds() as f64 / 1000.0).max(0.0)
}

/// Keeps the firehose gauges: the lag, the open gaps, the seam windows
/// waiting for their re-read and the depth of the channel.
///
/// It runs beside the writer rather than in it, so the gauges go on while
/// the writer is held on one batch, which is when they matter. The lag is
/// measured from the last `applied_through` this task could read: while
/// the database does not answer it keeps growing with the clock.
pub async fn gauges(pool: PgPool, tx: mpsc::WeakSender<writer::Item>) {
    use crate::metrics as m;
    let mut applied = None;
    let mut tick = tokio::time::interval(GAUGE_PERIOD);
    loop {
        tick.tick().await;
        if let Some(tx) = tx.upgrade() {
            let depth = tx.max_capacity().saturating_sub(tx.capacity());
            ::metrics::gauge!(m::BUFFER_DEPTH).set(depth as f64);
        }
        let read = async {
            let state = farsight_storage::firehose::read_state(&pool).await?;
            let gaps = farsight_storage::firehose::unhealed_gaps(&pool).await?;
            let seams = farsight_storage::firehose::pending_seams(&pool).await?;
            Ok::<_, farsight_storage::StorageError>((state, gaps.len(), seams))
        };
        if let Ok(Ok((state, gaps, seams))) = tokio::time::timeout(GAUGE_READ_TIMEOUT, read).await {
            applied = state.applied_through.or(applied);
            ::metrics::gauge!(m::OPEN_GAPS).set(gaps as f64);
            ::metrics::gauge!(m::PENDING_SEAMS).set(seams as f64);
        }
        if let Some(a) = applied {
            ::metrics::gauge!(m::LAG).set(lag_seconds(chrono::Utc::now(), a));
        }
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
        lag_seconds: st.applied_through.map(|a| lag_seconds(now, a)),
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

    /// Stops reading, lets the writer drain, waits for the tasks and
    /// flushes the approximate counters one last time. `within` bounds
    /// the whole wait: a task still running then is aborted (a batch in
    /// flight rolls back and its cursor with it), and the flush gets what
    /// is left, at least one second.
    pub async fn shutdown(self, within: Duration) {
        let deadline = tokio::time::Instant::now() + within;
        let _ = tokio::time::timeout_at(deadline, self.control.send(Control::Shutdown)).await;
        for mut t in self.tasks {
            if tokio::time::timeout_at(deadline, &mut t).await.is_err() {
                tracing::warn!("an ingest task did not stop in time; aborting it");
                t.abort();
            }
        }
        self.flusher.abort();
        self.gauges.abort();
        let left = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .max(Duration::from_secs(1));
        match tokio::time::timeout(left, self.counters.flush(&self.pool, &self.limits)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "final counter flush failed"),
            Err(_) => tracing::warn!("final counter flush did not finish in time"),
        }
    }
}

/// Entry point: [`Ingest::start`].
pub struct Ingest;

impl Ingest {
    /// Starts the reader, the writer, the seam repair task, the gauge task
    /// and the counter flusher. `pool` should be the dedicated
    /// 4-connection ingest pool. Seam windows left open by a crash are
    /// closed first.
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

        // No session is connected yet, whatever the process before left
        // on record: one that was killed never cleared the flag, and
        // answers would say `firehoseConnected: true` for an instance
        // that is reading nothing.
        farsight_storage::firehose::set_connected(&pool, false)
            .await
            .map_err(|e| e.to_string())?;

        // A seam window still open belongs to a session that did not live
        // to close it: it ends where the stream got to.
        let state = farsight_storage::firehose::read_state(&pool)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(through) = state.applied_through {
            let seam = &cfg.reader.seam;
            farsight_storage::firehose::close_seams(&pool, through, seam.after, seam.delay)
                .await
                .map_err(|e| e.to_string())?;
        }

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (ctl_tx, ctl_rx) = mpsc::channel(64);
        let plain = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let repairer = seam::Repairer {
            pool: pool.clone(),
            tx: tx.clone(),
            stall_timeout: cfg.reader.stall_timeout,
            gap_threshold: cfg.reader.tuning.gap_threshold,
            plain: plain.clone(),
            #[cfg(feature = "harness")]
            tap: cfg.tap.clone(),
        };
        let repairs = reader::Repairs::hold(tokio::spawn(farsight_core::task::supervise(
            SEAM_TASK,
            move || repairer.clone().run(),
        )));
        let gauges = {
            let (pool, tx) = (pool.clone(), tx.downgrade());
            tokio::spawn(farsight_core::task::supervise(GAUGES_TASK, move || {
                gauges(pool.clone(), tx.clone())
            }))
        };
        let writer = writer::Writer {
            pool: pool.clone(),
            limits: cfg.limits.clone(),
            gates: cfg.gates.clone(),
            counters: counters.clone(),
            stats: stats.clone(),
            seam: cfg.reader.seam,
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
            repairs,
            pending_inject: Vec::new(),
            plain,
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
            gauges,
            failed,
            pool,
            limits: cfg.limits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lag_is_measured_from_the_last_position_and_never_negative() {
        let t = |s: i64| chrono::DateTime::<chrono::Utc>::from_timestamp(s, 0).unwrap();
        assert_eq!(lag_seconds(t(1_000), t(1_000)), 0.0);
        assert_eq!(lag_seconds(t(1_090), t(1_000)), 90.0);
        // A position ahead of the clock is no lag.
        assert_eq!(lag_seconds(t(1_000), t(1_200)), 0.0);
    }
}
