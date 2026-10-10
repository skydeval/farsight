//! The reader task (see `docs/design/firehose.md`): connects to the
//! configured instances, detects the protocol, resumes from the
//! persisted cursor, enforces the stall timeout, records resume gaps,
//! fails over between instances, and feeds the bounded channel (a full
//! channel stops reading: TCP backpressure).

#[cfg(feature = "harness")]
use farsight_storage::codes::sql::PROTOCOL_V2;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use farsight_storage::codes::SeamTrigger;
use farsight_storage::firehose;
use sqlx::PgPool;
use tokio::sync::{mpsc, oneshot};

use crate::conn::{self, ConnectError, ReadError, Session};
use crate::frame::{Body, Frame, InEvent, Protocol, now_us};
use crate::lag::LagTracker;
use crate::metrics as m;
use crate::resume::{self, Cursor, GapRule, Persisted, Tuning};
use crate::seam;
use crate::stats::IngestStats;
use crate::writer::Item;

/// Consecutive failed sessions on one instance before failing over.
pub const FAILOVER_AFTER: u32 = 3;
/// Wait before the first reconnect; doubled after each one.
pub const BACKOFF_FIRST: Duration = Duration::from_millis(500);
/// Longest wait between reconnects.
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A session that delivered events and lasted this long was a healthy
/// one: the wait starts again from [`BACKOFF_FIRST`] after it.
pub const HEALTHY_SESSION: Duration = Duration::from_secs(60);
/// Most times the stall timeout is doubled for sessions that stay silent.
pub const PATIENCE_DOUBLINGS: u32 = 4;

/// How an attempt to read from an instance ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// The connection or the handshake failed.
    ConnectFailed,
    /// The session opened and ended without one event: the instance
    /// closed it, sent an error, or sent nothing for the stall timeout.
    Silent,
    /// The session delivered events for this long before it ended.
    Delivered(Duration),
    /// The session was dropped on command; nothing is learned from it.
    Killed,
}

impl Ended {
    /// How a session that was opened ended: on command if `reason` says
    /// so, otherwise by whether it `delivered` an event in the time it
    /// `lasted`.
    pub fn of(reason: ReconnectReason, delivered: bool, lasted: Duration) -> Ended {
        match reason {
            ReconnectReason::Kill => Ended::Killed,
            _ if delivered => Ended::Delivered(lasted),
            _ => Ended::Silent,
        }
    }
}

/// How long a session may stay silent before it counts as stalled, after
/// `silent` sessions in a row that ended that way without one event.
///
/// An instance replaying a stretch with few wanted events can send
/// nothing for longer than `stall_timeout`. Dropping the session then
/// resumes at the same cursor and meets the same silence, so each silent
/// stall doubles the wait, up to [`PATIENCE_DOUBLINGS`] times. A session
/// that delivers an event sets it back.
pub fn patience(stall_timeout: Duration, silent: u32) -> Duration {
    stall_timeout.saturating_mul(1 << silent.min(PATIENCE_DOUBLINGS))
}

/// What to do before the next attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Next {
    /// How long to wait.
    pub wait: Duration,
    /// Move to the next instance.
    pub failover: bool,
}

/// Reconnect pacing and failover: the wait between attempts and the count
/// of consecutive failed sessions on the current instance.
///
/// - The wait doubles with every reconnect, up to [`BACKOFF_MAX`], and
///   starts again after a session that was healthy for
///   [`HEALTHY_SESSION`]: a process that has run for months reconnects
///   as promptly as a new one, while an instance that drops every
///   session after a few events is still approached more and more slowly.
/// - A session is a failure if it could not be opened or delivered no
///   event. [`FAILOVER_AFTER`] failures in a row move on to the next
///   instance, if there is one. The wait is kept across a failover, so
///   that with every instance down the attempts still slow down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reconnects {
    backoff: Duration,
    failures: u32,
}

impl Default for Reconnects {
    fn default() -> Self {
        Reconnects {
            backoff: BACKOFF_FIRST,
            failures: 0,
        }
    }
}

impl Reconnects {
    /// Records how an attempt ended and says what comes next. `instances`
    /// is the number of configured instances.
    pub fn next(&mut self, ended: Ended, instances: usize) -> Next {
        match ended {
            Ended::Killed => {
                return Next {
                    wait: Duration::ZERO,
                    failover: false,
                };
            }
            Ended::ConnectFailed | Ended::Silent => self.failures += 1,
            Ended::Delivered(lasted) => {
                self.failures = 0;
                if lasted >= HEALTHY_SESSION {
                    self.backoff = BACKOFF_FIRST;
                }
            }
        }
        let failover = self.failures >= FAILOVER_AFTER && instances > 1;
        if failover {
            self.failures = 0;
        }
        let wait = self.backoff;
        self.backoff = (self.backoff * 2).min(BACKOFF_MAX);
        Next { wait, failover }
    }
}

/// Seam repair settings (`firehose.tuning.seam_repair_*`). Public instances
/// were observed to drop events witnessed within about a second of a cursor
/// resume — at the hand-over from replay to the live tail — while a later
/// replay of the same window returns them. The repair re-reads the window
/// from the session's connect to the moment it caught up to live, never the
/// whole resumed range (after a long rewind that would be hours of events).
/// The windows and their re-reads are in [`crate::seam`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeamRepair {
    /// Window start, before the connect (`seam_repair_before`).
    pub before: Duration,
    /// Window end, after catching up (`seam_repair_after`).
    pub after: Duration,
    /// Delay between catching up and the re-read (`seam_repair_delay`).
    pub delay: Duration,
    /// Caught up once an event's witness time is within this much of wall
    /// time (`seam_repair_catchup_margin`), or is past the connect.
    pub catchup_margin: Duration,
}

impl Default for SeamRepair {
    fn default() -> Self {
        SeamRepair {
            before: Duration::from_secs(150),
            after: Duration::from_secs(30),
            delay: Duration::from_secs(60),
            catchup_margin: Duration::from_secs(5),
        }
    }
}

/// Commands from the embedding process.
#[derive(Debug)]
pub enum Control {
    /// Drop the websocket now and reconnect (harness failure injection).
    KillSocket,
    /// Feed these events into the pipeline as if received (harness:
    /// poisoned events, synthesized `#sync`).
    Inject(Vec<InEvent>),
    /// Harness: drop the websocket and, before reconnecting, set the
    /// persisted cursor and `applied_through` back to `us` (v2 seq to 1),
    /// simulating an outage longer than the instance's retention.
    #[cfg(feature = "harness")]
    KillAndRewind {
        /// Witness µs to rewind to.
        us: i64,
    },
    /// Stop reading and end the reader task; the writer then drains what it
    /// was sent.
    Shutdown,
}

/// Reader configuration.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// `firehose.urls`, in failover order.
    pub urls: Vec<String>,
    /// Resume and gap thresholds (`firehose.tuning`).
    pub tuning: Tuning,
    /// `firehose.tuning.stall_timeout`.
    pub stall_timeout: Duration,
    /// Request zstd frames.
    pub compress: bool,
    /// Seam repair window and timing (`firehose.tuning.seam_repair_*`).
    pub seam: SeamRepair,
}

/// The reader task's inputs and state.
pub struct Reader {
    /// Instances, thresholds and timeouts.
    pub cfg: ReaderConfig,
    /// Pool (reads the persisted cursor).
    pub pool: PgPool,
    /// The bounded channel to the writer; while it is full the reader does
    /// not read.
    pub tx: mpsc::Sender<Item>,
    /// Commands from the embedding process ([`Control`]).
    pub control: mpsc::Receiver<Control>,
    /// Counters shared with the writer and the embedding process.
    pub stats: Arc<IngestStats>,
    /// Harness: a copy of every event received from the network.
    #[cfg(feature = "harness")]
    pub tap: Option<mpsc::UnboundedSender<InEvent>>,
    /// Harness: rewind requested by [`Control::KillAndRewind`].
    #[cfg(feature = "harness")]
    pub rewind: Option<i64>,
    /// The seam repair task; aborted when the reader goes away.
    pub repairs: Repairs,
    /// Events injected while the pipeline was full, sent next.
    pub pending_inject: Vec<InEvent>,
    /// Set once an instance stopped accepting the bundled dictionary:
    /// frames are then requested uncompressed. Shared with the seam
    /// repair task.
    pub plain: Arc<AtomicBool>,
}

/// The seam repair task, aborted on drop (it holds a sender to the
/// writer, which would otherwise keep the pipeline alive after shutdown).
#[derive(Debug, Default)]
pub struct Repairs(Vec<tokio::task::JoinHandle<()>>);

impl Repairs {
    /// Holds `task` until the reader goes away.
    pub fn hold(task: tokio::task::JoinHandle<()>) -> Repairs {
        Repairs(vec![task])
    }
}

impl Drop for Repairs {
    fn drop(&mut self) {
        for h in &self.0 {
            h.abort();
        }
    }
}

/// Why a reconnect is counted (the `reason` label of
/// `farsight_firehose_reconnects_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectReason {
    /// The session was dropped on command.
    Kill,
    /// Nothing arrived for the stall timeout.
    Stall,
    /// The instance closed the session.
    Closed,
    /// Reading a frame failed.
    Error,
    /// The instance sent an error frame.
    ServerError,
    /// The connection or the handshake failed.
    ConnectError,
    /// The reader moved to the next instance.
    Failover,
    /// The instance refused the cursor as too old.
    CursorTooOld,
}

impl ReconnectReason {
    /// The `reason` label of `farsight_firehose_reconnects_total`.
    pub fn label(self) -> &'static str {
        match self {
            ReconnectReason::Kill => "kill",
            ReconnectReason::Stall => "stall",
            ReconnectReason::Closed => "closed",
            ReconnectReason::Error => "error",
            ReconnectReason::ServerError => "server_error",
            ReconnectReason::ConnectError => "connect_error",
            ReconnectReason::Failover => "failover",
            ReconnectReason::CursorTooOld => "cursor_too_old",
        }
    }

    fn count(self) {
        metrics::counter!(m::RECONNECTS, "reason" => self.label()).increment(1);
    }
}

/// Sessions in a row that may end on a frame that cannot be read, at the
/// same position of the same instance, before the next session steps
/// past it. The first ends are taken for what they usually are, a fault
/// in transit that a reconnect clears; a frame that is unreadable every
/// time would otherwise come first on every resume, for good.
pub const UNREADABLE_RETRIES: u32 = 3;
/// Most frames a session steps past at one position. Past that the
/// stream itself is unreadable, which stepping does not cure.
pub const UNREADABLE_SKIP_MAX: u32 = 64;

/// The position a session resumes from, as a number that is the same on
/// every resume from that position: the `seq` or the time asked for.
/// `None` at the live tail.
/// Connects at the live tail after a refused cursor: `connect(compress)`,
/// and once more uncompressed when the instance answers that it no
/// longer knows the bundled dictionary. An instance may check the cursor
/// before the dictionary, so the second refusal can come only here; left
/// to the next attempt, that one would meet the refused cursor first
/// again, and the stream would never be read. Returns the session and
/// whether it is uncompressed for that reason.
async fn live_tail<S, F, Fut>(compress: bool, mut connect: F) -> (Result<S, ConnectError>, bool)
where
    F: FnMut(bool) -> Fut,
    Fut: std::future::Future<Output = Result<S, ConnectError>>,
{
    match connect(compress).await {
        Err(ConnectError::UnknownDictionary(_)) if compress => (connect(false).await, true),
        r => (r, false),
    }
}

fn resume_key(cursor: Cursor) -> Option<i64> {
    match cursor {
        Cursor::Live => None,
        Cursor::Seq(n) => Some(n),
        Cursor::TimeUs(t) => Some(t),
    }
}

/// The [`resume_key`] of the resume that follows a session whose last
/// delivered event is `ev`: its `seq` plus one on v2, its witness time
/// less the replay on v1 (see [`resume::plan`]).
fn key_after(ev: &InEvent) -> i64 {
    match ev.seq {
        Some(seq) => seq.saturating_add(1),
        None => ev.witness_us.saturating_sub(resume::us(resume::V1_REPLAY)),
    }
}

/// Positions at which sessions ended on a frame that could not be read:
/// per instance, the position and how many sessions in a row ended there.
#[derive(Debug, Default)]
struct Stuck(HashMap<String, (i64, u32)>);

impl Stuck {
    /// A session on `url` ended on an unreadable frame at `key`. Returns
    /// how many in a row have now ended there.
    fn failed(&mut self, url: &str, key: i64) -> u32 {
        let e = self.0.entry(url.to_owned()).or_insert((key, 0));
        if e.0 != key {
            *e = (key, 0);
        }
        e.1 = e.1.saturating_add(1);
        e.1
    }

    /// The position the next session on `url` steps past unreadable
    /// frames at, once [`UNREADABLE_RETRIES`] sessions have ended there.
    fn skip_at(&self, url: &str) -> Option<i64> {
        self.0
            .get(url)
            .filter(|(_, n)| *n >= UNREADABLE_RETRIES)
            .map(|(key, _)| *key)
    }

    /// A session on `url` got past the position.
    fn cleared(&mut self, url: &str) {
        self.0.remove(url);
    }
}

/// Whether a frame that cannot be read is stepped past: only at the
/// position sessions kept ending at (`skip_at`), and only
/// [`UNREADABLE_SKIP_MAX`] times in one session. `at` is where the
/// session stands: after its last delivered event, or where it resumed.
fn steps_past(at: Option<i64>, skip_at: Option<i64>, skipped: u32) -> bool {
    at.is_some() && at == skip_at && skipped < UNREADABLE_SKIP_MAX
}

/// Where a session resumed, for the handling of unreadable frames.
#[derive(Debug, Clone, Copy, Default)]
struct ResumedAt {
    /// The [`resume_key`] of the cursor sent.
    key: Option<i64>,
    /// Where a loss on this resume starts, witness µs
    /// ([`resume::Plan::refused_from_us`]).
    from_us: Option<i64>,
    /// The position to step past unreadable frames at, if sessions kept
    /// ending there.
    skip_at: Option<i64>,
}

/// Jetstream `#info` name: the cursor asked for was older than what the
/// instance keeps, and it resumed from its floor.
pub(crate) const INFO_OUTDATED_CURSOR: &str = "OutdatedCursor";

enum End {
    Reconnect(ReconnectReason),
    Shutdown,
}

/// What a session has delivered so far.
#[derive(Debug, Clone, Copy, Default)]
struct Seen {
    /// Whether it delivered an event.
    delivered: bool,
    /// Stream position of the last delivered event.
    position_us: Option<i64>,
    /// Whether the session's seam window is on record and still open:
    /// the session has not caught up with the live tail yet.
    seam_open: bool,
    /// The [`resume_key`] of a resume after the last delivered event.
    next_key: Option<i64>,
    /// The session ended on a frame that could not be read, at this
    /// position.
    failed_at: Option<i64>,
    /// Unreadable frames stepped past and not yet covered by a gap.
    skipped: u32,
    /// Unreadable frames stepped past in the whole session.
    skipped_total: u32,
    /// The session delivered an event after stepping past: the position
    /// is behind it.
    stepped: bool,
}

/// Which instance the reader is on, and how its attempts have gone.
struct Attempts {
    /// Index into `firehose.urls`, taken modulo their number.
    idx: usize,
    reconnects: Reconnects,
    lag: LagTracker,
    /// Lag of the instance failed over from, for the resume plan.
    prev_instance_lag: Option<Duration>,
}

/// The index of the furthest of `positions`, the instances' own cursors
/// in the order of `firehose.urls` (witness µs; `None` for an instance
/// never read): the first of them when two are equally far, and 0 when
/// none has a cursor.
fn start_index(positions: &[Option<i64>]) -> usize {
    let mut best: Option<(usize, i64)> = None;
    for (i, p) in positions.iter().enumerate() {
        if let Some(p) = *p
            && best.is_none_or(|(_, b)| p > b)
        {
            best = Some((i, p));
        }
    }
    best.map_or(0, |(i, _)| i)
}

fn set_connected_gauge(protocol: Option<Protocol>) {
    for p in [Protocol::V1, Protocol::V2] {
        let v = if Some(p) == protocol { 1.0 } else { 0.0 };
        metrics::gauge!(m::CONNECTED, "protocol" => p.label()).set(v);
    }
}

impl Reader {
    async fn barrier(&self) -> bool {
        let (ack, wait) = oneshot::channel();
        if self.tx.send(Item::Barrier(ack)).await.is_err() {
            return false;
        }
        wait.await.is_ok()
    }

    /// The resume inputs for instance `url`: that instance's own cursor
    /// from `firehose_cursors` if it has one — so a failback resumes
    /// exactly where the instance left off — with the global
    /// running-max `applied_through` as the gap reference, and the
    /// instance that position was last read from. An instance without a
    /// cursor is planned as a failover (timestamp rewind).
    async fn persisted(&self, url: &str) -> Persisted {
        let mut backoff = Duration::from_millis(200);
        let to_proto = |p: farsight_storage::codes::Protocol| match p {
            farsight_storage::codes::Protocol::V1 => Protocol::V1,
            farsight_storage::codes::Protocol::V2 => Protocol::V2,
        };
        loop {
            let state = firehose::read_state(&self.pool).await;
            let inst = firehose::instance_cursor(&self.pool, url).await;
            match (state, inst) {
                (Ok(s), Ok(inst)) => {
                    let applied_through_us = s.applied_through.map(|a| a.timestamp_micros());
                    return match inst {
                        Some(c) if c.cursor_seq.is_some() || c.cursor_us.is_some() => Persisted {
                            source_url: Some(c.source_url),
                            protocol: c.protocol.map(to_proto),
                            cursor_seq: c.cursor_seq,
                            cursor_us: c.cursor_us,
                            applied_through_us,
                            applied_from: s.source_url,
                        },
                        _ => Persisted {
                            source_url: None,
                            protocol: s.protocol.map(to_proto),
                            cursor_seq: None,
                            cursor_us: None,
                            applied_through_us,
                            applied_from: s.source_url,
                        },
                    };
                }
                (Err(e), _) | (_, Err(e)) => {
                    tracing::warn!(error = %e, "reading firehose_state failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    /// Detects the protocol and connects. Returns the session, the gap
    /// rule for its first event, whether anything was applied before,
    /// and where it resumed.
    async fn open(
        &self,
        url: &str,
        lag: Option<Duration>,
    ) -> Result<(Session, GapRule, bool, ResumedAt), ConnectError> {
        let p = self.persisted(url).await;
        let t = &self.cfg.tuning;
        let mut compress = self.cfg.compress && !self.plain.load(Ordering::Relaxed);
        let mut protocols = vec![Protocol::V2, Protocol::V1];
        while let Some(proto) = protocols.first().copied() {
            let plan = resume::plan(&p, url, proto, lag, t, now_us());
            match conn::connect(url, proto, plan.cursor, compress).await {
                Ok(s) => {
                    let at = ResumedAt {
                        key: resume_key(plan.cursor),
                        from_us: plan.refused_from_us,
                        skip_at: None,
                    };
                    return Ok((s, plan.gap, p.applied_through_us.is_some(), at));
                }
                Err(ConnectError::NotOffered(code)) if proto == Protocol::V2 => {
                    tracing::debug!(url, code, "v2 not offered; falling back to v1");
                    protocols.remove(0);
                }
                Err(ConnectError::UnknownDictionary(msg)) if compress => {
                    // The bundled dictionary was retired: stream
                    // uncompressed rather than not at all.
                    tracing::warn!(url, %msg, "zstd dictionary retired upstream; continuing uncompressed");
                    compress = false;
                    self.plain.store(true, Ordering::Relaxed);
                }
                Err(ConnectError::CursorTooOld(msg)) => {
                    // Resume at the live tail, with the gap from where
                    // the plan says a loss on this instance starts to
                    // the first live event.
                    tracing::warn!(url, %msg, "CursorTooOld; resuming at the live tail");
                    ReconnectReason::CursorTooOld.count();
                    let (s, plain) = live_tail(compress, |compress| {
                        conn::connect(url, proto, Cursor::Live, compress)
                    })
                    .await;
                    if plain {
                        tracing::warn!(
                            url,
                            "zstd dictionary retired upstream; continuing uncompressed"
                        );
                        self.plain.store(true, Ordering::Relaxed);
                    }
                    let s = s?;
                    // At the live tail there is no position to be stuck at.
                    let at = ResumedAt::default();
                    return Ok((s, plan.refused(), p.applied_through_us.is_some(), at));
                }
                Err(e) => return Err(e),
            }
        }
        Err(ConnectError::NotOffered(404))
    }

    /// Where a starting process connects first: the configured instance
    /// whose own cursor is furthest along, and the first one if none has
    /// a cursor. That is the instance the process was reading when it
    /// stopped: it resumes there by `seq`, with nothing to replay and no
    /// gap. Starting on the first instance again after a failover would
    /// resume a cursor as old as the failover.
    ///
    /// The furthest cursor, not the instance of the last batch: while an
    /// instance replays a stretch it was behind on, its batches are the
    /// last ones applied, and it is the one furthest back.
    async fn start_index(&self) -> usize {
        let mut positions = Vec::with_capacity(self.cfg.urls.len());
        for url in &self.cfg.urls {
            match firehose::instance_cursor(&self.pool, url).await {
                Ok(c) => positions.push(c.and_then(|c| c.cursor_us)),
                Err(e) => {
                    tracing::warn!(error = %e, "reading firehose_cursors failed; starting on the first instance");
                    return 0;
                }
            }
        }
        start_index(&positions)
    }

    /// Runs until shutdown or the writer goes away.
    pub async fn run(mut self) {
        let mut at = Attempts {
            idx: self.start_index().await,
            reconnects: Reconnects::default(),
            lag: LagTracker::new(),
            prev_instance_lag: None,
        };
        let mut current_url: Option<String> = None;
        // Sessions in a row that stalled without one event (see `patience`).
        let mut silent_stalls = 0u32;
        let mut stuck = Stuck::default();
        loop {
            // Drain the pipeline so the persisted cursor reflects every
            // event already read (a dropped connection reconnects from
            // the persisted cursor).
            if !self.barrier().await {
                return;
            }
            #[cfg(feature = "harness")]
            if let Some(us) = self.rewind.take() {
                let t = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(us)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                // Both the session copy and every instance's own cursor
                // (resumes read firehose_cursors).
                let r = sqlx::query(&format!(
                    "UPDATE firehose_state SET cursor_us = $1, applied_through = $2,
                       cursor_seq = CASE WHEN protocol = {PROTOCOL_V2} THEN 1 ELSE cursor_seq END
                     WHERE id = 1"
                ))
                .bind(us)
                .bind(t)
                .execute(&self.pool)
                .await;
                let r2 = sqlx::query(&format!(
                    "UPDATE firehose_cursors SET cursor_us = $1, last_applied_through = $2,
                       cursor_seq = CASE WHEN protocol = {PROTOCOL_V2} THEN 1 ELSE cursor_seq END"
                ))
                .bind(us)
                .bind(t)
                .execute(&self.pool)
                .await;
                tracing::warn!(us, ok = r.is_ok() && r2.is_ok(), "harness rewind applied");
            }
            let url = self.cfg.urls[at.idx % self.cfg.urls.len()].clone();
            let failover_lag =
                if current_url.as_deref() == Some(url.as_str()) || current_url.is_none() {
                    None
                } else {
                    at.prev_instance_lag
                };
            let opened = self.open(&url, failover_lag).await;
            let failover = current_url.as_deref().is_some_and(|u| u != url.as_str());
            let (mut session, rule, prior, mut resumed_at) = match opened {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(url, error = %e, "connect failed");
                    let wait = self.reconnect(
                        ReconnectReason::ConnectError,
                        Ended::ConnectFailed,
                        &mut at,
                    );
                    if !wait.is_zero() && self.sleep_or_shutdown(wait).await {
                        return;
                    }
                    continue;
                }
            };
            let protocol = session.protocol;
            tracing::info!(
                url,
                protocol = protocol.label(),
                "jetstream session started"
            );
            if current_url.as_deref() != Some(url.as_str()) {
                current_url = Some(url.clone());
            }
            self.stats.sessions.fetch_add(1, Ordering::Relaxed);
            *self.stats.current.lock().unwrap_or_else(|e| e.into_inner()) =
                Some((url.clone(), protocol));
            set_connected_gauge(Some(protocol));
            if self
                .tx
                .send(Item::Session {
                    url: url.clone(),
                    protocol,
                })
                .await
                .is_err()
            {
                return;
            }
            let started = Instant::now();
            let stall = patience(self.cfg.stall_timeout, silent_stalls);
            let mut seen = Seen::default();
            resumed_at.skip_at = stuck.skip_at(&url);
            let end = self
                .read_session(
                    &mut session,
                    rule,
                    prior,
                    failover,
                    &mut at.lag,
                    stall,
                    resumed_at,
                    &mut seen,
                )
                .await;
            let delivered = seen.delivered;
            let lasted = started.elapsed();
            if seen.stepped {
                stuck.cleared(&url);
            }
            if let Some(key) = seen.failed_at {
                let times = stuck.failed(&url, key);
                if times >= UNREADABLE_RETRIES {
                    tracing::warn!(
                        url,
                        times,
                        "sessions keep ending on a frame that cannot be read at the same \
                         position; the next one steps past it and records a gap"
                    );
                }
            }
            // A session that ends before it caught up leaves its seam
            // window open: it is closed where the session got to.
            if let (true, Some(through_us)) = (seen.seam_open, seen.position_us)
                && self.tx.send(Item::SeamClose { through_us }).await.is_err()
            {
                return;
            }
            if delivered {
                silent_stalls = 0;
            } else if matches!(end, End::Reconnect(ReconnectReason::Stall)) {
                silent_stalls += 1;
                tracing::warn!(
                    url,
                    waited_secs = stall.as_secs(),
                    next_secs = patience(self.cfg.stall_timeout, silent_stalls).as_secs(),
                    "the session sent nothing; the next one is given longer"
                );
            }
            session.close().await;
            set_connected_gauge(None);
            if self.tx.send(Item::Disconnected).await.is_err() {
                return;
            }
            match end {
                End::Shutdown => return,
                End::Reconnect(reason) => {
                    tracing::warn!(
                        url,
                        reason = reason.label(),
                        "jetstream session ended; reconnecting"
                    );
                    let ended = Ended::of(reason, delivered, lasted);
                    let wait = self.reconnect(reason, ended, &mut at);
                    if !wait.is_zero() && self.sleep_or_shutdown(wait).await {
                        return;
                    }
                }
            }
        }
    }

    /// Counts a reconnect for `reason`, records how the attempt ended and,
    /// when that is one failure too many on this instance, moves to the
    /// next one (keeping the lag measured on this one for the resume
    /// plan). Returns how long to wait before the next attempt.
    fn reconnect(&self, reason: ReconnectReason, ended: Ended, at: &mut Attempts) -> Duration {
        reason.count();
        self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
        let next = at.reconnects.next(ended, self.cfg.urls.len());
        if next.failover {
            at.prev_instance_lag = at.lag.instance_lag();
            at.lag.reset();
            at.idx += 1;
            ReconnectReason::Failover.count();
            tracing::warn!(next = %self.cfg.urls[at.idx % self.cfg.urls.len()], "failing over");
        }
        next.wait
    }

    async fn sleep_or_shutdown(&mut self, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => false,
            c = self.control.recv() => matches!(c, Some(Control::Shutdown) | None),
        }
    }

    /// Reads one session to its end. Returns why it ended; `seen` holds
    /// what it delivered.
    #[allow(clippy::too_many_arguments)]
    async fn read_session(
        &mut self,
        session: &mut Session,
        rule: GapRule,
        prior: bool,
        failover: bool,
        lag: &mut LagTracker,
        stall: Duration,
        resumed_at: ResumedAt,
        seen: &mut Seen,
    ) -> End {
        let mut clamped_notice = false;
        let mut since_gauge = 0u32;
        // The seam is where the replay hands over to the live tail: when
        // the session catches up, which for a long replay is long after the
        // resume. The window to read again starts before the connect.
        let connect_us = now_us();
        loop {
            let frame = tokio::select! {
                c = self.control.recv() => match c {
                    Some(Control::KillSocket) => return End::Reconnect(ReconnectReason::Kill),
                    #[cfg(feature = "harness")]
                    Some(Control::KillAndRewind { us }) => {
                        self.rewind = Some(us);
                        return End::Reconnect(ReconnectReason::Kill);
                    }
                    Some(Control::Inject(evs)) => {
                        for ev in evs {
                            // Injected events are re-stamped to the stream
                            // position, so a synthetic event never carries
                            // the cursor past a replay still in progress.
                            let ev = at_position(ev, seen.position_us);
                            if self.tx.send(Item::Event(ev)).await.is_err() {
                                return End::Shutdown;
                            }
                        }
                        continue;
                    }
                    Some(Control::Shutdown) | None => return End::Shutdown,
                },
                f = tokio::time::timeout(stall, session.next_frame()) => f,
            };
            let frame = match frame {
                Err(_) => return End::Reconnect(ReconnectReason::Stall),
                Ok(None) => return End::Reconnect(ReconnectReason::Closed),
                Ok(Some(Err(e))) => {
                    tracing::warn!(error = %e, "read error");
                    // Frames the bundled dictionary cannot expand: the
                    // instance changed it. A v2 instance says so at the
                    // handshake; a v1 instance does not, and is read
                    // uncompressed from the next session on.
                    if session.protocol == Protocol::V1
                        && matches!(e, ReadError::Decompress(_))
                        && !self.plain.swap(true, Ordering::Relaxed)
                    {
                        tracing::warn!(
                            "frames cannot be decompressed with the bundled dictionary; \
                             continuing uncompressed"
                        );
                    }
                    // A message that arrived whole and cannot be read, as
                    // opposed to a socket that broke.
                    if matches!(e, ReadError::Frame(_) | ReadError::Decompress(_)) {
                        let at = seen.next_key.or(resumed_at.key);
                        if steps_past(at, resumed_at.skip_at, seen.skipped_total) {
                            seen.skipped += 1;
                            seen.skipped_total += 1;
                            metrics::counter!(m::DROPPED, "reason" => m::DROPPED_UNREADABLE)
                                .increment(1);
                            tracing::warn!(
                                error = %e,
                                "a frame that could not be read on any attempt is stepped past"
                            );
                            continue;
                        }
                        seen.failed_at = at;
                    }
                    return End::Reconnect(ReconnectReason::Error);
                }
                Ok(Some(Ok(f))) => f,
            };
            let ev = match frame {
                Frame::Info { name, message } => {
                    tracing::warn!(%name, message = ?message, "jetstream #info");
                    if name == INFO_OUTDATED_CURSOR {
                        clamped_notice = true;
                    }
                    continue;
                }
                Frame::Error { error, message } => {
                    tracing::warn!(%error, message = ?message, "jetstream error frame");
                    return End::Reconnect(ReconnectReason::ServerError);
                }
                Frame::Event(ev) => ev,
            };
            // Frames stepped past are a loss between the position the
            // session stood at and this event, unless the resume itself
            // records a gap over it just below.
            let stepped_from = (seen.skipped > 0)
                .then(|| seen.position_us.or(resumed_at.from_us))
                .flatten();
            let mut gap_recorded = false;
            if !seen.delivered {
                seen.delivered = true;
                let resumed = resume::first_event(rule, ev.seq, ev.witness_us, clamped_notice);
                if let Some((from_us, to_us, cause)) = resumed.gap {
                    gap_recorded = true;
                    tracing::warn!(from_us, to_us, ?cause, "resume gap");
                    if self
                        .tx
                        .send(Item::Gap {
                            from_us,
                            to_us,
                            cause,
                        })
                        .await
                        .is_err()
                    {
                        return End::Shutdown;
                    }
                }
                if resumed.sequence_restarted {
                    tracing::warn!(
                        url = %session.url_base,
                        seq = ?ev.seq,
                        "the instance's sequence started again; its stored cursor is dropped"
                    );
                    let url = session.url_base.clone();
                    if self.tx.send(Item::ForgetSeq { url }).await.is_err() {
                        return End::Shutdown;
                    }
                }
                // After any resume with a prior position (skipped only on
                // the first-ever start), the seam window goes on record
                // ahead of the session's first event.
                if prior {
                    let trigger = SeamTrigger::of(resumed.gap.is_some(), failover);
                    let from_us = connect_us.saturating_sub(resume::us(self.cfg.seam.before));
                    let open = Item::SeamOpen {
                        url: session.url_base.clone(),
                        protocol: session.protocol,
                        trigger,
                        from_us,
                    };
                    if self.tx.send(open).await.is_err() {
                        return End::Shutdown;
                    }
                    seen.seam_open = true;
                }
            }
            if seen.skipped > 0 {
                let frames = std::mem::take(&mut seen.skipped);
                seen.stepped = true;
                if let (false, Some(from)) = (gap_recorded, stepped_from) {
                    let (from_us, to_us) = stepped_gap(from, ev.witness_us);
                    tracing::warn!(from_us, to_us, frames, "gap over frames stepped past");
                    let gap = Item::Gap {
                        from_us,
                        to_us,
                        cause: farsight_storage::codes::GapCause::Unreadable,
                    };
                    if self.tx.send(gap).await.is_err() {
                        return End::Shutdown;
                    }
                }
            }
            if let Body::Commit(op) = &ev.body {
                lag.record(ev.witness_us, i64::try_from(op.rev.micros()).unwrap_or(0));
            }
            since_gauge += 1;
            if since_gauge >= 100 {
                since_gauge = 0;
                let now = now_us();
                if let Some(s) = lag.source_lag_seconds(now) {
                    metrics::gauge!(m::SOURCE_LAG).set(s);
                    self.stats
                        .source_lag_ms
                        .store((s * 1000.0) as i64, Ordering::Relaxed);
                }
            }
            if seen.seam_open {
                let now = now_us();
                let margin = resume::us(self.cfg.seam.catchup_margin);
                if seam::caught_up(ev.witness_us, now, connect_us, margin) {
                    seen.seam_open = false;
                    let through_us = now.max(ev.witness_us);
                    tracing::info!(
                        connect_us,
                        caught_up_us = through_us,
                        lag_secs = (now - connect_us) / 1_000_000,
                        "session caught up; its seam window is closed"
                    );
                    if self.tx.send(Item::SeamClose { through_us }).await.is_err() {
                        return End::Shutdown;
                    }
                }
            }
            // Stay responsive to commands while the channel is full
            // (backpressure), without losing the event in hand: a kill
            // abandons it (never applied, so the reconnect re-reads it from
            // the persisted cursor); an injection waits for it.
            #[cfg(feature = "harness")]
            let tapped = ev.clone();
            let witness_us = ev.witness_us;
            let next_key = key_after(&ev);
            let tx = self.tx.clone();
            let send = tx.send(Item::Event(ev));
            tokio::pin!(send);
            loop {
                tokio::select! {
                    r = &mut send => {
                        if r.is_err() {
                            return End::Shutdown;
                        }
                        break;
                    }
                    c = self.control.recv() => match c {
                        Some(Control::KillSocket) => return End::Reconnect(ReconnectReason::Kill),
                        #[cfg(feature = "harness")]
                        Some(Control::KillAndRewind { us }) => {
                            self.rewind = Some(us);
                            return End::Reconnect(ReconnectReason::Kill);
                        }
                        Some(Control::Inject(evs)) => self.pending_inject.extend(evs),
                        Some(Control::Shutdown) | None => return End::Shutdown,
                    },
                }
            }
            // Tapped only once delivered, so the harness model never holds an
            // event the writer did not get.
            #[cfg(feature = "harness")]
            if let Some(tap) = &self.tap {
                let _ = tap.send(tapped);
            }
            seen.position_us = Some(witness_us);
            seen.next_key = Some(next_key);
            for ev in std::mem::take(&mut self.pending_inject) {
                let ev = at_position(ev, seen.position_us);
                if self.tx.send(Item::Event(ev)).await.is_err() {
                    return End::Shutdown;
                }
            }
        }
    }
}

/// The gap over frames stepped past: from where the session stood to
/// the first event read after them. If that event is not later (two
/// clocks, or a replay), the gap is the second before it: a gap is never
/// empty.
fn stepped_gap(from_us: i64, first_us: i64) -> (i64, i64) {
    if from_us < first_us {
        (from_us, first_us)
    } else {
        (first_us.saturating_sub(1_000_000), first_us)
    }
}

/// An injected event placed at the stream's current position (unchanged
/// before the session's first delivered event).
fn at_position(mut ev: InEvent, position_us: Option<i64>) -> InEvent {
    if let Some(us) = position_us {
        ev.witness_us = us;
    }
    ev
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_starting_process_connects_to_the_instance_whose_cursor_is_furthest_along() {
        // It had failed over to the second, which is where it was
        // reading: it starts there.
        assert_eq!(start_index(&[Some(1_000), Some(9_000)]), 1);
        assert_eq!(start_index(&[Some(9_000), Some(1_000)]), 0);
        // An instance never read has no say.
        assert_eq!(start_index(&[None, Some(1)]), 1);
        // Equally far, or nothing applied yet: the first.
        assert_eq!(start_index(&[Some(5), Some(5)]), 0);
        assert_eq!(start_index(&[None, None]), 0);
        assert_eq!(start_index(&[]), 0);
    }

    const LONG: Duration = Duration::from_secs(3600);
    const SHORT: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn the_live_tail_is_read_uncompressed_when_the_dictionary_is_gone() {
        use std::cell::RefCell;
        // The instance refuses compressed sessions and takes plain ones.
        let asked = RefCell::new(Vec::new());
        let retired = |compress: bool| {
            asked.borrow_mut().push(compress);
            async move {
                if compress {
                    Err(ConnectError::UnknownDictionary("retired".into()))
                } else {
                    Ok("session")
                }
            }
        };
        assert_eq!(live_tail(true, retired).await, (Ok("session"), true));
        assert_eq!(*asked.borrow(), [true, false]);
        // An instance that knows the dictionary is asked once.
        let known = |compress: bool| async move { Ok::<_, ConnectError>(compress) };
        assert_eq!(live_tail(true, known).await, (Ok(true), false));
        // Any other refusal is the answer, and nothing is tried again.
        let tries = RefCell::new(0);
        let down = |_: bool| {
            *tries.borrow_mut() += 1;
            async { Err::<(), _>(ConnectError::Transport("refused".into())) }
        };
        let (r, plain) = live_tail(true, down).await;
        assert_eq!(
            (r, plain, *tries.borrow()),
            (Err(ConnectError::Transport("refused".into())), false, 1)
        );
        // A session that was plain already has nothing to fall back to.
        let gone =
            |_: bool| async { Err::<(), _>(ConnectError::UnknownDictionary("retired".into())) };
        let (r, plain) = live_tail(false, gone).await;
        assert!(matches!(r, Err(ConnectError::UnknownDictionary(_))) && !plain);
    }

    fn event(seq: Option<i64>, witness_us: i64) -> InEvent {
        InEvent {
            seq,
            witness_us,
            body: Body::OtherCommit,
        }
    }

    #[test]
    fn a_position_has_the_same_key_however_the_session_got_there() {
        // v2: a session that delivered seq 500 and one resumed after it.
        assert_eq!(key_after(&event(Some(500), 9)), 501);
        assert_eq!(resume_key(Cursor::Seq(501)), Some(501));
        // v1: the resume asks for the replay before the last event.
        let w = 10_000_000_000;
        let replay = resume::us(resume::V1_REPLAY);
        assert_eq!(key_after(&event(None, w)), w - replay);
        assert_eq!(resume_key(Cursor::TimeUs(w - replay)), Some(w - replay));
        assert_eq!(resume_key(Cursor::Live), None);
    }

    #[test]
    fn a_frame_unreadable_at_one_position_is_stepped_past_after_three_sessions() {
        let mut stuck = Stuck::default();
        let (a, b) = ("wss://a.example", "wss://b.example");
        // Two sessions end at the same position: still taken for a fault
        // in transit.
        assert_eq!(stuck.failed(a, 501), 1);
        assert_eq!(stuck.skip_at(a), None);
        assert_eq!(stuck.failed(a, 501), 2);
        assert_eq!(stuck.skip_at(a), None);
        // The third: the next session steps past, there and only there.
        assert_eq!(stuck.failed(a, 501), 3);
        assert_eq!(stuck.skip_at(a), Some(501));
        assert_eq!(stuck.skip_at(b), None);
        assert!(steps_past(Some(501), stuck.skip_at(a), 0));
        assert!(!steps_past(Some(777), stuck.skip_at(a), 0));
        assert!(!steps_past(None, stuck.skip_at(a), 0));
        assert!(!steps_past(None, None, 0));
        // A failover in between does not forget the position.
        assert_eq!(stuck.failed(b, 90), 1);
        assert_eq!(stuck.skip_at(a), Some(501));
        // A failure somewhere else on the instance starts the count again.
        assert_eq!(stuck.failed(a, 640), 1);
        assert_eq!(stuck.skip_at(a), None);
        // Getting past clears it.
        for _ in 0..3 {
            stuck.failed(a, 640);
        }
        assert_eq!(stuck.skip_at(a), Some(640));
        stuck.cleared(a);
        assert_eq!(stuck.skip_at(a), None);
        // A stream that is unreadable frame after frame is not stepped
        // through.
        assert!(steps_past(Some(1), Some(1), UNREADABLE_SKIP_MAX - 1));
        assert!(!steps_past(Some(1), Some(1), UNREADABLE_SKIP_MAX));
    }

    #[test]
    fn the_gap_over_stepped_frames_is_never_empty() {
        assert_eq!(stepped_gap(100, 5_000_000), (100, 5_000_000));
        // The first readable event is not later than the position: the
        // second before it.
        assert_eq!(stepped_gap(9_000_000, 5_000_000), (4_000_000, 5_000_000));
        assert_eq!(stepped_gap(5_000_000, 5_000_000), (4_000_000, 5_000_000));
    }

    #[test]
    fn silent_stalls_double_the_patience_up_to_a_bound() {
        let base = Duration::from_secs(60);
        let secs: Vec<u64> = (0..7).map(|n| patience(base, n).as_secs()).collect();
        assert_eq!(secs, [60, 120, 240, 480, 960, 960, 960]);
        assert_eq!(patience(Duration::MAX, 3), Duration::MAX);
    }

    #[test]
    fn the_wait_starts_again_after_a_healthy_session() {
        let mut r = Reconnects::default();
        // Twenty disconnects over a long life, each after hours of events.
        for _ in 0..20 {
            assert_eq!(
                r.next(Ended::Delivered(LONG), 2),
                Next {
                    wait: BACKOFF_FIRST,
                    failover: false
                }
            );
        }
    }

    #[test]
    fn the_wait_grows_while_sessions_fail_or_end_early() {
        let mut r = Reconnects::default();
        let mut waits = Vec::new();
        for i in 0..10 {
            let ended = if i % 2 == 0 {
                Ended::Delivered(SHORT)
            } else {
                Ended::ConnectFailed
            };
            waits.push(r.next(ended, 1).wait);
        }
        assert_eq!(waits[0], BACKOFF_FIRST);
        assert!(waits.windows(2).all(|w| w[1] >= w[0]), "{waits:?}");
        assert_eq!(waits[9], BACKOFF_MAX);
        // One healthy session later the next reconnect is prompt again.
        assert_eq!(
            r.next(Ended::Delivered(HEALTHY_SESSION), 1).wait,
            BACKOFF_FIRST
        );
    }

    #[test]
    fn an_instance_that_sends_nothing_is_failed_over() {
        let mut r = Reconnects::default();
        // Accepts the socket, then silence until the stall timeout.
        assert!(!r.next(Ended::Silent, 2).failover);
        assert!(!r.next(Ended::Silent, 2).failover);
        assert!(r.next(Ended::Silent, 2).failover);
        // The count starts again on the next instance.
        assert!(!r.next(Ended::Silent, 2).failover);
        // Connect errors and silent sessions count alike.
        assert!(!r.next(Ended::ConnectFailed, 2).failover);
        assert!(r.next(Ended::Silent, 2).failover);
    }

    #[test]
    fn events_clear_the_failures_and_one_instance_never_fails_over() {
        let mut r = Reconnects::default();
        r.next(Ended::Silent, 2);
        r.next(Ended::Silent, 2);
        assert!(!r.next(Ended::Delivered(SHORT), 2).failover);
        assert!(!r.next(Ended::Silent, 2).failover);
        let mut one = Reconnects::default();
        for _ in 0..10 {
            assert!(!one.next(Ended::Silent, 1).failover);
        }
    }

    #[test]
    fn the_wait_is_kept_across_a_failover() {
        let mut r = Reconnects::default();
        let mut last = Duration::ZERO;
        // Every instance down: attempts keep slowing down.
        for _ in 0..12 {
            let n = r.next(Ended::ConnectFailed, 2);
            assert!(n.wait >= last);
            last = n.wait;
        }
        assert_eq!(last, BACKOFF_MAX);
    }

    #[test]
    fn a_killed_session_changes_nothing() {
        let mut r = Reconnects::default();
        r.next(Ended::Silent, 2);
        let before = r;
        assert_eq!(
            r.next(Ended::Killed, 2),
            Next {
                wait: Duration::ZERO,
                failover: false
            }
        );
        assert_eq!(r, before);
    }

    #[test]
    fn labels_are_the_metric_values() {
        use ReconnectReason as R;
        let reasons = [
            (R::Kill, "kill"),
            (R::Stall, "stall"),
            (R::Closed, "closed"),
            (R::Error, "error"),
            (R::ServerError, "server_error"),
            (R::ConnectError, "connect_error"),
            (R::Failover, "failover"),
            (R::CursorTooOld, "cursor_too_old"),
        ];
        for (r, label) in reasons {
            assert_eq!(r.label(), label);
        }
        use farsight_storage::codes::SeamTrigger;
        assert_eq!(SeamTrigger::of(true, false).label(), "clamp_recovery");
        assert_eq!(SeamTrigger::of(true, true).label(), "clamp_recovery");
        assert_eq!(SeamTrigger::of(false, true).label(), "failover");
        assert_eq!(SeamTrigger::of(false, false).label(), "resume");
    }

    #[test]
    fn only_a_kill_ends_a_session_without_a_verdict() {
        use ReconnectReason as R;
        assert_eq!(Ended::of(R::Kill, true, LONG), Ended::Killed);
        assert_eq!(Ended::of(R::Kill, false, SHORT), Ended::Killed);
        for r in [R::Stall, R::Closed, R::Error, R::ServerError] {
            assert_eq!(Ended::of(r, true, SHORT), Ended::Delivered(SHORT));
            assert_eq!(Ended::of(r, false, LONG), Ended::Silent);
        }
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        fn ended() -> impl Strategy<Value = Ended> {
            let lasted = prop_oneof![
                (0u64..200).prop_map(Duration::from_secs),
                (0u64..120_000).prop_map(Duration::from_millis),
                (any::<u64>(), 0u32..1_000_000_000).prop_map(|(s, n)| Duration::new(s, n)),
            ];
            prop_oneof![
                Just(Ended::ConnectFailed),
                Just(Ended::Silent),
                Just(Ended::Killed),
                lasted.prop_map(Ended::Delivered),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Over any sequence of session ends and any number of
            /// instances: a kill waits for nothing and changes nothing;
            /// every other wait lies within the backoff bounds, is the
            /// first one after a healthy session and otherwise at most
            /// double the one before; and the reader fails over exactly
            /// at the third failure in a row with another instance to go
            /// to, counting from the last failover or delivered event.
            #[test]
            fn pacing_and_failover_follow_the_rule(
                ends in prop::collection::vec(ended(), 0..80),
                instances in 1usize..5,
            ) {
                let mut r = Reconnects::default();
                let mut failures = 0u32;
                let mut expected_wait = BACKOFF_FIRST;
                for e in ends {
                    let before = r;
                    let next = r.next(e, instances);
                    if e == Ended::Killed {
                        prop_assert_eq!(next, Next { wait: Duration::ZERO, failover: false });
                        prop_assert_eq!(r, before);
                        continue;
                    }
                    match e {
                        Ended::Delivered(lasted) => {
                            failures = 0;
                            if lasted >= HEALTHY_SESSION {
                                expected_wait = BACKOFF_FIRST;
                            }
                        }
                        _ => failures += 1,
                    }
                    let failover = failures >= FAILOVER_AFTER && instances > 1;
                    if failover {
                        failures = 0;
                    }
                    prop_assert_eq!(next, Next { wait: expected_wait, failover });
                    prop_assert!((BACKOFF_FIRST..=BACKOFF_MAX).contains(&next.wait));
                    prop_assert!(instances > 1 || !next.failover);
                    expected_wait = (expected_wait * 2).min(BACKOFF_MAX);
                }
            }

            /// How a session ended is decided for every reason, with or
            /// without events, however long it lasted.
            #[test]
            fn every_session_end_has_a_verdict(
                reason in prop::sample::select(vec![
                    ReconnectReason::Kill,
                    ReconnectReason::Stall,
                    ReconnectReason::Closed,
                    ReconnectReason::Error,
                    ReconnectReason::ServerError,
                ]),
                delivered in any::<bool>(),
                lasted in (any::<u64>(), 0u32..1_000_000_000).prop_map(|(s, n)| Duration::new(s, n)),
            ) {
                let expected = if reason == ReconnectReason::Kill {
                    Ended::Killed
                } else if delivered {
                    Ended::Delivered(lasted)
                } else {
                    Ended::Silent
                };
                prop_assert_eq!(Ended::of(reason, delivered, lasted), expected);
            }
        }
    }
}
