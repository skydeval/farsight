//! Seam repair (see `docs/design/firehose.md`): the re-read of the window
//! in which a resumed session passed from replay to the live tail.
//!
//! The windows are rows of `firehose_seams`. The reader has one written
//! before the first event of a resumed session is applied and closes it
//! when the session has caught up or ended, so a window is on record from
//! the moment it can matter and survives a restart. The task here reads
//! each closed window again once it is due and hands the events to the
//! writer; the row is deleted only when the read reached the end of the
//! window and the writer has applied what was read.
//!
//! A read that does not get there is not taken for one that did:
//!
//! - A read that **fails** (the connection cannot be opened, breaks,
//!   is closed, or the window does not fit the read's bounds) is tried
//!   again later, [`SEAM_REPAIR_ATTEMPTS`] times in all. Then the window
//!   is recorded as a gap, which lowers coverage until a repair cycle
//!   has re-read the repositories.
//! - A read that goes **silent** before an event past the window's end
//!   says nothing either way: a quiet stream looks the same as a stalled
//!   one. It is tried again later without being counted, and finishes
//!   once the stream has moved past the window.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use farsight_storage::codes::SeamTrigger;
use farsight_storage::firehose::{self, Seam};
use farsight_storage::ids::SeamId;
use sqlx::PgPool;
use tokio::sync::{mpsc, oneshot};

use crate::conn::{self, ConnectError};
use crate::frame::{Frame, InEvent, Protocol};
use crate::metrics as m;
use crate::resume::Cursor;
use crate::writer::{BATCH_MAX, Item};

/// Most events one seam repair reads before it counts as failed.
pub const SEAM_REPAIR_MAX_EVENTS: usize = 500_000;
/// Longest one seam repair reads for.
pub const SEAM_REPAIR_MAX_READ: Duration = Duration::from_secs(600);
/// Failed reads of a window before it is recorded as a gap.
pub const SEAM_REPAIR_ATTEMPTS: i32 = 5;
/// Wait after the first failed read; doubled after each further one.
pub const SEAM_RETRY_FIRST: Duration = Duration::from_secs(60);
/// Longest wait between two reads of a window.
pub const SEAM_RETRY_MAX: Duration = Duration::from_secs(15 * 60);
/// How long after a window's end a silent read still proves nothing. A
/// stream that has sent no event for longer than this past the end of a
/// window is not a quiet stream: its silent reads count as failed ones,
/// and the window becomes a gap like any other that cannot be read.
pub const SEAM_SILENT_MAX: Duration = Duration::from_secs(30 * 60);
/// How often the due windows are looked up.
pub const SEAM_POLL: Duration = Duration::from_secs(2);

/// Whether a resumed session has passed from replay to the live tail by
/// the event witnessed at `witness_us`, received at `now_us`: the event
/// is within `margin_us` of the clock, or it was witnessed after the
/// session connected at `connect_us`. The second holds for an instance
/// that stays further behind the clock than the margin, which the first
/// alone would never find caught up.
pub fn caught_up(witness_us: i64, now_us: i64, connect_us: i64, margin_us: i64) -> bool {
    now_us.saturating_sub(witness_us) <= margin_us || witness_us >= connect_us
}

/// One stretch of one instance's stream to read again: the due windows
/// of the instance, read in one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    /// The instance.
    pub url: String,
    /// The protocol to read it with.
    pub protocol: Protocol,
    /// Where the read starts, witness µs.
    pub from_us: i64,
    /// The read is finished by the first event witnessed after this.
    pub until_us: i64,
    /// The rows the read covers.
    pub ids: Vec<SeamId>,
    /// The trigger of the newest of them (the metric label).
    pub trigger: SeamTrigger,
    /// The most failed reads any of them has had.
    pub attempts: i32,
}

/// Groups due seam rows into one window per instance, from the earliest
/// start to the latest end: sessions that reconnect in quick succession
/// leave windows that overlap, and one read covers them all.
pub fn windows(due: &[Seam]) -> Vec<Window> {
    let mut by_url: BTreeMap<&str, Window> = BTreeMap::new();
    for s in due {
        let (from_us, until_us) = (s.from_at.timestamp_micros(), s.to_at.timestamp_micros());
        let protocol = match s.protocol {
            farsight_storage::codes::Protocol::V1 => Protocol::V1,
            farsight_storage::codes::Protocol::V2 => Protocol::V2,
        };
        match by_url.get_mut(s.source_url.as_str()) {
            None => {
                by_url.insert(
                    s.source_url.as_str(),
                    Window {
                        url: s.source_url.clone(),
                        protocol,
                        from_us,
                        until_us,
                        ids: vec![s.id],
                        trigger: s.trigger,
                        attempts: s.attempts,
                    },
                );
            }
            Some(w) => {
                w.from_us = w.from_us.min(from_us);
                w.until_us = w.until_us.max(until_us);
                w.ids.push(s.id);
                w.attempts = w.attempts.max(s.attempts);
                // Rows arrive oldest first: the last one is the newest.
                w.protocol = protocol;
                w.trigger = s.trigger;
            }
        }
    }
    by_url.into_values().collect()
}

/// How one read of a window ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    /// An event past the window's end arrived: everything the instance
    /// holds of the window was read. Carries the number of events.
    Finished(u64),
    /// The stream went silent before the window's end was passed.
    Silent,
    /// The read could not be made or broke off.
    Failed(String),
}

/// What follows a read of a window that has failed `attempts` times
/// before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    /// Hand the window to the writer as repaired.
    Done,
    /// Read again after `wait`; `failed` counts this read against the
    /// window.
    Retry {
        /// How long to wait.
        wait: Duration,
        /// Whether the read counts as a failed one.
        failed: bool,
    },
    /// Record the window as a gap.
    Abandon,
}

/// The wait before the next read of a window that has failed `failed`
/// times.
pub fn retry_wait(failed: i32) -> Duration {
    let doublings = u32::try_from(failed.saturating_sub(1)).unwrap_or(0).min(16);
    SEAM_RETRY_FIRST
        .saturating_mul(1 << doublings)
        .min(SEAM_RETRY_MAX)
}

/// Decides what follows a read of a window that ended `ended_ago` ago.
pub fn next_step(read: &Read, attempts: i32, ended_ago: Duration) -> Next {
    match read {
        Read::Finished(_) => Next::Done,
        // A stream may be quiet for a while after the window: the read
        // is tried again and not held against it.
        Read::Silent if ended_ago < SEAM_SILENT_MAX => Next::Retry {
            wait: retry_wait(attempts.max(1)),
            failed: false,
        },
        Read::Silent | Read::Failed(_) => {
            let failed = attempts.saturating_add(1);
            if failed >= SEAM_REPAIR_ATTEMPTS {
                Next::Abandon
            } else {
                Next::Retry {
                    wait: retry_wait(failed),
                    failed: true,
                }
            }
        }
    }
}

/// Whether a re-read asked to start at `from_us` whose first event was
/// witnessed at `first_us` replays the window from its start: the
/// instance announced no clamp, and the first event is within
/// `threshold` of the start (the rule a resumed session is judged by).
pub fn replays_window(from_us: i64, first_us: i64, clamped: bool, threshold: Duration) -> bool {
    let limit = i64::try_from(threshold.as_micros()).unwrap_or(i64::MAX);
    !clamped && first_us.saturating_sub(from_us) <= limit
}

/// The seam repair task's inputs.
#[derive(Clone)]
pub struct Repairer {
    /// Pool (reads and updates `firehose_seams`).
    pub pool: PgPool,
    /// The channel to the writer.
    pub tx: mpsc::Sender<Item>,
    /// Silence that ends a read (`firehose.tuning.stall_timeout`).
    pub stall_timeout: Duration,
    /// How far after the start of a window the first event of its
    /// re-read may be before the instance counts as no longer holding
    /// the window (`firehose.tuning.gap_threshold`, as for a resume).
    pub gap_threshold: Duration,
    /// Set once an instance stopped accepting the bundled dictionary:
    /// frames are then requested uncompressed.
    pub plain: Arc<AtomicBool>,
    /// Harness: a copy of every event handed to the writer.
    #[cfg(feature = "harness")]
    pub tap: Option<mpsc::UnboundedSender<InEvent>>,
}

impl Repairer {
    /// Runs until the writer goes away.
    pub async fn run(self) {
        loop {
            tokio::time::sleep(SEAM_POLL).await;
            let due = match firehose::due_seams(&self.pool).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(error = %e, "reading the due seam windows failed");
                    continue;
                }
            };
            for w in windows(&due) {
                if !self.repair(&w).await {
                    return;
                }
            }
        }
    }

    /// Reads one window and acts on how the read ended. Returns false
    /// when the writer is gone.
    async fn repair(&self, w: &Window) -> bool {
        tracing::info!(
            url = %w.url,
            from_us = w.from_us,
            until_us = w.until_us,
            sessions = w.ids.len(),
            trigger = w.trigger.label(),
            "seam repair started"
        );
        let Some(read) = self.read(w).await else {
            return false;
        };
        let now_us = chrono::Utc::now().timestamp_micros();
        let ended_ago =
            Duration::from_micros(u64::try_from(now_us.saturating_sub(w.until_us)).unwrap_or(0));
        match next_step(&read, w.attempts, ended_ago) {
            Next::Done => {
                let events = match read {
                    Read::Finished(n) => n,
                    _ => 0,
                };
                // The writer deletes the rows once it has applied the
                // events sent ahead of this. Until it says so they are
                // not taken up again; should it never get to them, they
                // come due once more.
                if let Err(e) =
                    firehose::defer_seams(&self.pool, &w.ids, SEAM_RETRY_FIRST, false).await
                {
                    tracing::warn!(error = %e, "putting a repaired seam off failed");
                }
                let (done, applied) = oneshot::channel();
                let repaired = Item::SeamsRepaired {
                    ids: w.ids.clone(),
                    trigger: w.trigger,
                    events,
                    done,
                };
                return self.tx.send(repaired).await.is_ok() && applied.await.is_ok();
            }
            Next::Retry { wait, failed } => {
                tracing::warn!(
                    url = %w.url,
                    outcome = ?read,
                    retry_secs = wait.as_secs(),
                    "seam repair did not finish; it is tried again"
                );
                if let Err(e) = firehose::defer_seams(&self.pool, &w.ids, wait, failed).await {
                    tracing::warn!(error = %e, "putting a seam repair off failed");
                }
            }
            Next::Abandon => {
                tracing::error!(
                    url = %w.url,
                    from_us = w.from_us,
                    until_us = w.until_us,
                    outcome = ?read,
                    "seam repair failed for good; the window is recorded as a gap"
                );
                // If this fails the rows stay due, and the window is read
                // and given up again.
                if let Err(e) = firehose::abandon_seams(&self.pool, &w.ids).await {
                    tracing::warn!(error = %e, "recording an unrepaired seam as a gap failed");
                }
            }
        }
        true
    }

    async fn connect(&self, w: &Window) -> Result<conn::Session, ConnectError> {
        let cursor = Cursor::TimeUs(w.from_us);
        let compress = !self.plain.load(Ordering::Relaxed);
        match conn::connect(&w.url, w.protocol, cursor, compress).await {
            // Both protocols take a timestamp cursor: an instance that no
            // longer offers the one the session spoke is read on the other.
            Err(ConnectError::NotOffered(_)) => {
                let other = match w.protocol {
                    Protocol::V1 => Protocol::V2,
                    Protocol::V2 => Protocol::V1,
                };
                conn::connect(&w.url, other, cursor, compress).await
            }
            r => r,
        }
    }

    /// Hands a chunk of re-read events to the writer. False when the
    /// writer is gone.
    async fn send(&self, events: Vec<InEvent>) -> bool {
        if events.is_empty() {
            return true;
        }
        #[cfg(feature = "harness")]
        let copy = events.clone();
        if self.tx.send(Item::Repair(events)).await.is_err() {
            return false;
        }
        // Tapped only once delivered, so the harness model never holds an
        // event the writer did not get.
        #[cfg(feature = "harness")]
        if let Some(tap) = &self.tap {
            for ev in copy {
                let _ = tap.send(ev);
            }
        }
        true
    }

    /// Reads `w` from its start until an event past its end, handing the
    /// events to the writer in batches as they arrive. `None` when the
    /// writer is gone.
    async fn read(&self, w: &Window) -> Option<Read> {
        let mut session = match self.connect(w).await {
            Ok(s) => s,
            Err(e) => return Some(Read::Failed(format!("connect: {e}"))),
        };
        let started = Instant::now();
        let mut buf: Vec<InEvent> = Vec::with_capacity(BATCH_MAX);
        let mut total = 0u64;
        let mut clamped = false;
        let read = loop {
            if total >= SEAM_REPAIR_MAX_EVENTS as u64 || started.elapsed() >= SEAM_REPAIR_MAX_READ {
                break Read::Failed(format!(
                    "the window was not finished within {total} events and {} s",
                    started.elapsed().as_secs()
                ));
            }
            match tokio::time::timeout(self.stall_timeout, session.next_frame()).await {
                Ok(Some(Ok(Frame::Event(ev)))) => {
                    // The instance starts where it still holds events. A
                    // read that starts late replays nothing of what it
                    // skipped, and an event past the window's end would
                    // then close a window that was never read.
                    if total == 0
                        && !replays_window(w.from_us, ev.witness_us, clamped, self.gap_threshold)
                    {
                        break Read::Failed(format!(
                            "the instance no longer holds the window: its first event is {} s after the start",
                            ev.witness_us.saturating_sub(w.from_us) / 1_000_000
                        ));
                    }
                    let past = ev.witness_us > w.until_us;
                    buf.push(ev);
                    total += 1;
                    if past {
                        break Read::Finished(total);
                    }
                    if buf.len() >= BATCH_MAX && !self.send(std::mem::take(&mut buf)).await {
                        session.close().await;
                        return None;
                    }
                }
                Ok(Some(Ok(Frame::Info { name, .. }))) => {
                    if name == crate::reader::INFO_OUTDATED_CURSOR {
                        clamped = true;
                    }
                }
                Ok(Some(Ok(Frame::Error { error, .. }))) => {
                    break Read::Failed(format!("error frame: {error}"));
                }
                Ok(Some(Err(e))) => {
                    // As in the reader: a v1 instance that changed its
                    // dictionary is read uncompressed from now on.
                    if session.protocol == Protocol::V1
                        && matches!(e, conn::ReadError::Decompress(_))
                    {
                        self.plain.store(true, Ordering::Relaxed);
                    }
                    break Read::Failed(format!("read: {e}"));
                }
                Ok(None) => break Read::Failed("the instance closed the session".into()),
                Err(_) => break Read::Silent,
            }
        };
        session.close().await;
        // What was read is applied whether or not the read finished: it
        // is a no-op where it was applied before.
        if !self.send(buf).await {
            return None;
        }
        metrics::counter!(m::SEAM_REPAIR_EVENTS).increment(total);
        Some(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    const S: i64 = 1_000_000;

    fn seam(id: i64, url: &str, from_s: i64, to_s: i64, attempts: i32) -> Seam {
        let at = |s: i64| DateTime::<Utc>::from_timestamp(s, 0).unwrap();
        Seam {
            id: SeamId::new(id),
            source_url: url.into(),
            protocol: farsight_storage::codes::Protocol::V2,
            trigger: SeamTrigger::Resume,
            from_at: at(from_s),
            to_at: at(to_s),
            attempts,
        }
    }

    #[test]
    fn a_session_has_caught_up_near_the_clock_or_past_its_connect() {
        let (connect, margin) = (1_000 * S, 5 * S);
        // An ordinary reconnect: the first events are seconds old.
        assert!(caught_up(999 * S, 1_001 * S, connect, margin));
        // A long replay: hours behind, then at the live tail.
        assert!(!caught_up(100 * S, 1_050 * S, connect, margin));
        assert!(caught_up(5_000 * S - 2 * S, 5_000 * S, connect, margin));
        // An instance that stays 40 s behind the clock never comes within
        // the margin; it has caught up when it delivers what it witnessed
        // after the connect.
        assert!(!caught_up(990 * S, 1_030 * S, connect, margin));
        assert!(caught_up(1_000 * S, 1_040 * S, connect, margin));
        assert!(caught_up(1_200 * S, 1_240 * S, connect, margin));
    }

    #[test]
    fn overlapping_windows_of_one_instance_are_read_once() {
        let mut flapping = seam(3, "wss://a", 130, 400, 0);
        flapping.trigger = SeamTrigger::ClampRecovery;
        flapping.protocol = farsight_storage::codes::Protocol::V1;
        let due = [
            seam(1, "wss://a", 100, 300, 2),
            seam(2, "wss://b", 50, 90, 0),
            flapping,
        ];
        assert_eq!(
            windows(&due),
            [
                Window {
                    url: "wss://a".into(),
                    protocol: Protocol::V1,
                    from_us: 100 * S,
                    until_us: 400 * S,
                    ids: vec![SeamId::new(1), SeamId::new(3)],
                    trigger: SeamTrigger::ClampRecovery,
                    attempts: 2,
                },
                Window {
                    url: "wss://b".into(),
                    protocol: Protocol::V2,
                    from_us: 50 * S,
                    until_us: 90 * S,
                    ids: vec![SeamId::new(2)],
                    trigger: SeamTrigger::Resume,
                    attempts: 0,
                },
            ]
        );
        assert!(windows(&[]).is_empty());
    }

    #[test]
    fn only_a_finished_read_closes_a_window() {
        let fresh = Duration::from_secs(60);
        assert_eq!(next_step(&Read::Finished(0), 0, fresh), Next::Done);
        assert_eq!(next_step(&Read::Finished(900), 4, fresh), Next::Done);
        // Silence soon after the window proves nothing: tried again, not
        // counted, not a gap.
        for attempts in [0, 1, SEAM_REPAIR_ATTEMPTS, 1_000] {
            assert!(matches!(
                next_step(&Read::Silent, attempts, fresh),
                Next::Retry { failed: false, .. }
            ));
        }
        // A failed read is counted, and the last one makes the gap.
        let failed = Read::Failed("connect: refused".into());
        let mut waits = Vec::new();
        for attempts in 0..SEAM_REPAIR_ATTEMPTS - 1 {
            match next_step(&failed, attempts, fresh) {
                Next::Retry { wait, failed: true } => waits.push(wait),
                other => panic!("{attempts}: {other:?}"),
            }
        }
        assert_eq!(waits[0], SEAM_RETRY_FIRST);
        assert!(waits.windows(2).all(|w| w[1] > w[0]), "{waits:?}");
        assert_eq!(
            next_step(&failed, SEAM_REPAIR_ATTEMPTS - 1, fresh),
            Next::Abandon
        );
        assert_eq!(next_step(&failed, i32::MAX, fresh), Next::Abandon);
    }

    #[test]
    fn a_stream_silent_long_after_the_window_is_counted_and_ends_in_a_gap() {
        let old = SEAM_SILENT_MAX;
        assert!(matches!(
            next_step(&Read::Silent, 0, old),
            Next::Retry { failed: true, .. }
        ));
        assert_eq!(
            next_step(&Read::Silent, SEAM_REPAIR_ATTEMPTS - 1, old),
            Next::Abandon
        );
        // Just inside the bound it still waits.
        assert!(matches!(
            next_step(
                &Read::Silent,
                SEAM_REPAIR_ATTEMPTS,
                old - Duration::from_secs(1)
            ),
            Next::Retry { failed: false, .. }
        ));
    }

    #[test]
    fn a_read_that_starts_after_the_window_does_not_replay_it() {
        let t = Duration::from_secs(300);
        // The first event is at the start, or a little after it.
        assert!(replays_window(1_000 * S, 1_000 * S, false, t));
        assert!(replays_window(1_000 * S, 1_300 * S, false, t));
        // Past the threshold: the instance started later than asked.
        assert!(!replays_window(1_000 * S, 1_301 * S, false, t));
        // An announced clamp, wherever the first event is.
        assert!(!replays_window(1_000 * S, 1_000 * S, true, t));
        // An instance that replays from before the start holds it all.
        assert!(replays_window(1_000 * S, 900 * S, false, t));
    }

    #[test]
    fn the_wait_between_reads_is_bounded() {
        assert_eq!(retry_wait(0), SEAM_RETRY_FIRST);
        assert_eq!(retry_wait(1), SEAM_RETRY_FIRST);
        assert_eq!(retry_wait(2), SEAM_RETRY_FIRST * 2);
        for failed in [5, 17, 1_000, i32::MAX, i32::MIN] {
            assert!(retry_wait(failed) <= SEAM_RETRY_MAX, "{failed}");
        }
        assert_eq!(retry_wait(i32::MAX), SEAM_RETRY_MAX);
    }
}
