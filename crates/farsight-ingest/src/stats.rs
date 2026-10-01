//! In-process counters exposed to the embedding process (and the harness).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::frame::Protocol;

/// Live ingest statistics.
#[derive(Debug, Default)]
pub struct IngestStats {
    /// Batches committed (each with its cursor and clock row).
    pub batches: AtomicU64,
    /// Events handled by committed batches.
    pub events: AtomicU64,
    /// Writes that changed stored state.
    pub writes_applied: AtomicU64,
    /// Events dropped before apply (invalid, foreign listitem).
    pub dropped: AtomicU64,
    /// Poisoned events.
    pub poisoned: AtomicU64,
    /// Gaps recorded.
    pub gaps: AtomicU64,
    /// Sessions started.
    pub sessions: AtomicU64,
    /// Reconnects (any reason).
    pub reconnects: AtomicU64,
    /// Transient storage failures retried.
    pub transient_retries: AtomicU64,
    /// Seam repairs completed (see `reader::SEAM_DELAY`).
    pub seam_repairs: AtomicU64,
    /// Events re-read by seam repairs.
    pub seam_repair_events: AtomicU64,
    /// Latest source lag in milliseconds (`-1` = unmeasured).
    pub source_lag_ms: std::sync::atomic::AtomicI64,
    /// Protocol and URL of the current session.
    pub current: Mutex<Option<(String, Protocol)>>,
}

impl IngestStats {
    /// A snapshot of the counters.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            batches: self.batches.load(Ordering::Relaxed),
            events: self.events.load(Ordering::Relaxed),
            writes_applied: self.writes_applied.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            poisoned: self.poisoned.load(Ordering::Relaxed),
            gaps: self.gaps.load(Ordering::Relaxed),
            sessions: self.sessions.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            transient_retries: self.transient_retries.load(Ordering::Relaxed),
            seam_repairs: self.seam_repairs.load(Ordering::Relaxed),
            seam_repair_events: self.seam_repair_events.load(Ordering::Relaxed),
            current: self
                .current
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }
}

/// A copy of [`IngestStats`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// See [`IngestStats::batches`].
    pub batches: u64,
    /// See [`IngestStats::events`].
    pub events: u64,
    /// See [`IngestStats::writes_applied`].
    pub writes_applied: u64,
    /// See [`IngestStats::dropped`].
    pub dropped: u64,
    /// See [`IngestStats::poisoned`].
    pub poisoned: u64,
    /// See [`IngestStats::gaps`].
    pub gaps: u64,
    /// See [`IngestStats::sessions`].
    pub sessions: u64,
    /// See [`IngestStats::reconnects`].
    pub reconnects: u64,
    /// See [`IngestStats::transient_retries`].
    pub transient_retries: u64,
    /// See [`IngestStats::seam_repairs`].
    pub seam_repairs: u64,
    /// See [`IngestStats::seam_repair_events`].
    pub seam_repair_events: u64,
    /// See [`IngestStats::current`].
    pub current: Option<(String, Protocol)>,
}
