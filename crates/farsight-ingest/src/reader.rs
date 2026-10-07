//! The reader task (see `docs/design/firehose.md`): connects to the
//! configured instances, detects the protocol, resumes from the
//! persisted cursor, enforces the stall timeout, records resume gaps,
//! fails over between instances, and feeds the bounded channel (a full
//! channel stops reading: TCP backpressure).

#[cfg(feature = "harness")]
use farsight_storage::codes::sql::PROTOCOL_V2;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use farsight_storage::codes::GapCause;
use farsight_storage::firehose;
use sqlx::PgPool;
use tokio::sync::{mpsc, oneshot};

use crate::conn::{self, ConnectError, Session};
use crate::frame::{Body, Frame, InEvent, Protocol, now_us};
use crate::lag::LagTracker;
use crate::metrics as m;
use crate::resume::{self, Cursor, GapRule, Persisted, Plan, Tuning};
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
/// Most events one seam repair collects before it applies what it has.
pub const SEAM_REPAIR_MAX_EVENTS: usize = 500_000;
/// Longest one seam repair reads for.
pub const SEAM_REPAIR_MAX_READ: Duration = Duration::from_secs(600);

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeamRepair {
    /// Window start, before the connect (`seam_repair_before`).
    pub before: Duration,
    /// Window end, after catching up (`seam_repair_after`).
    pub after: Duration,
    /// Delay between catching up and the re-read (`seam_repair_delay`).
    pub delay: Duration,
    /// Caught up once an event's witness time is within this much of wall
    /// time (`seam_repair_catchup_margin`).
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
    /// Pending seam repairs; aborted when the reader goes away.
    pub repairs: Repairs,
    /// Events injected while the pipeline was full, sent next.
    pub pending_inject: Vec<InEvent>,
}

/// Seam-repair tasks, aborted on drop (they hold a sender to the writer,
/// which would otherwise keep the pipeline alive after shutdown).
#[derive(Debug, Default)]
pub struct Repairs(Vec<tokio::task::JoinHandle<()>>);

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

/// What a seam repair follows (the `trigger` label of
/// `farsight_firehose_seam_repairs_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeamTrigger {
    /// A resume that recorded a gap.
    ClampRecovery,
    /// A resume on another instance.
    Failover,
    /// A resume on the same instance.
    Resume,
}

impl SeamTrigger {
    /// The `trigger` label of `farsight_firehose_seam_repairs_total`.
    pub fn label(self) -> &'static str {
        match self {
            SeamTrigger::ClampRecovery => "clamp_recovery",
            SeamTrigger::Failover => "failover",
            SeamTrigger::Resume => "resume",
        }
    }

    /// The trigger for a resume: `gap` if its first event recorded one,
    /// `failover` if it is on another instance than the session before.
    pub fn of(gap: bool, failover: bool) -> SeamTrigger {
        if gap {
            SeamTrigger::ClampRecovery
        } else if failover {
            SeamTrigger::Failover
        } else {
            SeamTrigger::Resume
        }
    }
}

/// Jetstream `#info` name: the cursor asked for was older than what the
/// instance keeps, and it resumed from its floor.
const INFO_OUTDATED_CURSOR: &str = "OutdatedCursor";

enum End {
    Reconnect(ReconnectReason),
    Shutdown,
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
    /// running-max `applied_through` as the gap reference. An instance
    /// without a cursor is planned as a failover (timestamp rewind).
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
                        },
                        _ => Persisted {
                            source_url: s.source_url.filter(|u| u != url),
                            protocol: s.protocol.map(to_proto),
                            cursor_seq: None,
                            cursor_us: None,
                            applied_through_us,
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

    /// Detects the protocol and connects. Returns the session and the gap
    /// rule for its first event.
    async fn open(
        &self,
        url: &str,
        lag: Option<Duration>,
    ) -> Result<(Session, GapRule, bool), ConnectError> {
        let p = self.persisted(url).await;
        let t = &self.cfg.tuning;
        let mut compress = self.cfg.compress;
        let mut protocols = vec![Protocol::V2, Protocol::V1];
        while let Some(proto) = protocols.first().copied() {
            let Plan { cursor, gap } = resume::plan(&p, url, proto, lag, t);
            match conn::connect(url, proto, cursor, compress).await {
                Ok(s) => return Ok((s, gap, p.applied_through_us.is_some())),
                Err(ConnectError::NotOffered(code)) if proto == Protocol::V2 => {
                    tracing::debug!(url, code, "v2 not offered; falling back to v1");
                    protocols.remove(0);
                }
                Err(ConnectError::UnknownDictionary(msg)) if compress => {
                    // The bundled dictionary was retired: stream
                    // uncompressed rather than not at all.
                    tracing::warn!(url, %msg, "zstd dictionary retired upstream; continuing uncompressed");
                    compress = false;
                }
                Err(ConnectError::CursorTooOld(msg)) => {
                    // Gap [applied_through, first live event], resume
                    // at the live tail.
                    tracing::warn!(url, %msg, "CursorTooOld; resuming at the live tail");
                    ReconnectReason::CursorTooOld.count();
                    let from = p.applied_through_us.unwrap_or(0);
                    let s = conn::connect(url, proto, Cursor::Live, compress).await?;
                    return Ok((
                        s,
                        GapRule::Always {
                            from_us: from,
                            cause: GapCause::CursorTooOld,
                        },
                        p.applied_through_us.is_some(),
                    ));
                }
                Err(e) => return Err(e),
            }
        }
        Err(ConnectError::NotOffered(404))
    }

    /// Runs until shutdown or the writer goes away.
    pub async fn run(mut self) {
        let mut at = Attempts {
            idx: 0,
            reconnects: Reconnects::default(),
            lag: LagTracker::new(),
            prev_instance_lag: None,
        };
        let mut current_url: Option<String> = None;
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
            let (mut session, rule, prior) = match opened {
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
            let (end, delivered) = self
                .read_session(&mut session, rule, prior, failover, &mut at.lag)
                .await;
            let lasted = started.elapsed();
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

    /// Re-reads `[connect − seam_repair_before, caught up +
    /// seam_repair_after]` once, `seam_repair_delay` after catching up,
    /// through `apply` without position state (LWW makes the duplicates
    /// stale no-ops), to recover events the instance dropped at the
    /// replay-to-live seam.
    fn spawn_seam_repair(
        &mut self,
        url: &str,
        protocol: Protocol,
        connect_us: i64,
        caught_up_us: i64,
        trigger: SeamTrigger,
    ) {
        self.repairs.0.retain(|h| !h.is_finished());
        let url = url.to_owned();
        let tx = self.tx.clone();
        let stats = self.stats.clone();
        let compress = self.cfg.compress;
        #[cfg(feature = "harness")]
        let tap = self.tap.clone();
        // The lossy hand-over lies between the session's connect (where the
        // instance fixes the end of its replay) and the moment the session
        // caught up to live. For an ordinary resume the two coincide; after
        // a long replay they can be minutes apart. Read
        // [connect − before, caught up + after] with a timestamp cursor (both
        // protocols accept µs).
        let cfg = self.cfg.seam;
        let until = caught_up_us.saturating_add(resume::us(cfg.after));
        let cursor = Cursor::TimeUs(connect_us.saturating_sub(resume::us(cfg.before)));
        tracing::info!(
            connect_us,
            caught_up_us,
            trigger = trigger.label(),
            "seam repair scheduled"
        );
        let handle = tokio::spawn(async move {
            tokio::time::sleep(cfg.delay).await;
            let mut s = match conn::connect(&url, protocol, cursor, compress).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "seam repair: connect failed");
                    return;
                }
            };
            let mut events = Vec::new();
            let read_started = Instant::now();
            loop {
                // The window ends where the instance says it does. One
                // that never reaches the end is read up to these bounds.
                if events.len() >= SEAM_REPAIR_MAX_EVENTS
                    || read_started.elapsed() >= SEAM_REPAIR_MAX_READ
                {
                    tracing::warn!(
                        events = events.len(),
                        "seam repair: window not finished within its bounds; applying what was read"
                    );
                    break;
                }
                match tokio::time::timeout(Duration::from_secs(30), s.next_frame()).await {
                    Ok(Some(Ok(Frame::Event(ev)))) => {
                        let past = ev.witness_us > until;
                        events.push(ev);
                        if past {
                            break;
                        }
                    }
                    Ok(Some(Ok(_))) => {}
                    _ => break,
                }
            }
            let n = events.len() as u64;
            // A repair never advances the cursor (see `Item::Repair`).
            #[cfg(feature = "harness")]
            let copy = events.clone();
            if tx.send(Item::Repair(events)).await.is_ok() {
                // Tapped only once delivered (see read_session).
                #[cfg(feature = "harness")]
                if let Some(tap) = &tap {
                    for ev in copy {
                        let _ = tap.send(ev);
                    }
                }
            }
            s.close().await;
            stats.seam_repairs.fetch_add(1, Ordering::Relaxed);
            stats.seam_repair_events.fetch_add(n, Ordering::Relaxed);
            metrics::counter!(m::SEAM_REPAIRS, "trigger" => trigger.label()).increment(1);
            metrics::counter!(m::SEAM_REPAIR_EVENTS).increment(n);
            tracing::info!(
                events = n,
                trigger = trigger.label(),
                "seam repair replayed"
            );
        });
        self.repairs.0.push(handle);
    }

    async fn sleep_or_shutdown(&mut self, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => false,
            c = self.control.recv() => matches!(c, Some(Control::Shutdown) | None),
        }
    }

    /// Reads one session to its end. Returns why it ended and whether it
    /// delivered an event.
    async fn read_session(
        &mut self,
        session: &mut Session,
        rule: GapRule,
        prior: bool,
        failover: bool,
        lag: &mut LagTracker,
    ) -> (End, bool) {
        let mut first = true;
        let mut clamped_notice = false;
        let mut since_gauge = 0u32;
        // The seam is where the replay hands over to the live tail: when
        // the session catches up, which for a long replay is long after the
        // resume. The repair is anchored there.
        let connect_us = now_us();
        let mut seam_trigger: Option<SeamTrigger> = None;
        // Stream position of the last delivered event: injected events are
        // re-stamped to it, so a synthetic event never carries the cursor
        // past a replay still in progress.
        let mut position_us: Option<i64> = None;
        loop {
            let frame = tokio::select! {
                c = self.control.recv() => match c {
                    Some(Control::KillSocket) => return (End::Reconnect(ReconnectReason::Kill), !first),
                    #[cfg(feature = "harness")]
                    Some(Control::KillAndRewind { us }) => {
                        self.rewind = Some(us);
                        return (End::Reconnect(ReconnectReason::Kill), !first);
                    }
                    Some(Control::Inject(evs)) => {
                        for ev in evs {
                            let ev = at_position(ev, position_us);
                            if self.tx.send(Item::Event(ev)).await.is_err() {
                                return (End::Shutdown, !first);
                            }
                        }
                        continue;
                    }
                    Some(Control::Shutdown) | None => return (End::Shutdown, !first),
                },
                f = tokio::time::timeout(self.cfg.stall_timeout, session.next_frame()) => f,
            };
            let frame = match frame {
                Err(_) => return (End::Reconnect(ReconnectReason::Stall), !first),
                Ok(None) => return (End::Reconnect(ReconnectReason::Closed), !first),
                Ok(Some(Err(e))) => {
                    tracing::warn!(error = %e, "read error");
                    return (End::Reconnect(ReconnectReason::Error), !first);
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
                    return (End::Reconnect(ReconnectReason::ServerError), !first);
                }
                Frame::Event(ev) => ev,
            };
            if first {
                first = false;
                let gap = resume::gap_for_first_event(
                    rule,
                    ev.witness_us,
                    clamped_notice,
                    &self.cfg.tuning,
                );
                // After any resume with a prior position (skipped only
                // on the first-ever start).
                if prior {
                    seam_trigger = Some(SeamTrigger::of(gap.is_some(), failover));
                }
                if let Some((from_us, to_us, cause)) = gap {
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
                        return (End::Shutdown, !first);
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
            if let Some(trigger) = seam_trigger {
                let now = now_us();
                let margin = resume::us(self.cfg.seam.catchup_margin);
                if now.saturating_sub(ev.witness_us) <= margin {
                    seam_trigger = None;
                    tracing::warn!(
                        connect_us,
                        caught_up_us = now,
                        lag_secs = (now - connect_us) / 1_000_000,
                        trigger = trigger.label(),
                        "session caught up; seam repair scheduled"
                    );
                    self.spawn_seam_repair(
                        &session.url_base,
                        session.protocol,
                        connect_us,
                        now,
                        trigger,
                    );
                }
            }
            // Stay responsive to commands while the channel is full
            // (backpressure), without losing the event in hand: a kill
            // abandons it (never applied, so the reconnect re-reads it from
            // the persisted cursor); an injection waits for it.
            #[cfg(feature = "harness")]
            let tapped = ev.clone();
            let witness_us = ev.witness_us;
            let tx = self.tx.clone();
            let send = tx.send(Item::Event(ev));
            tokio::pin!(send);
            loop {
                tokio::select! {
                    r = &mut send => {
                        if r.is_err() {
                            return (End::Shutdown, !first);
                        }
                        break;
                    }
                    c = self.control.recv() => match c {
                        Some(Control::KillSocket) => return (End::Reconnect(ReconnectReason::Kill), !first),
                        #[cfg(feature = "harness")]
                        Some(Control::KillAndRewind { us }) => {
                            self.rewind = Some(us);
                            return (End::Reconnect(ReconnectReason::Kill), !first);
                        }
                        Some(Control::Inject(evs)) => self.pending_inject.extend(evs),
                        Some(Control::Shutdown) | None => return (End::Shutdown, !first),
                    },
                }
            }
            // Tapped only once delivered, so the harness model never holds an
            // event the writer did not get.
            #[cfg(feature = "harness")]
            if let Some(tap) = &self.tap {
                let _ = tap.send(tapped);
            }
            position_us = Some(witness_us);
            for ev in std::mem::take(&mut self.pending_inject) {
                let ev = at_position(ev, position_us);
                if self.tx.send(Item::Event(ev)).await.is_err() {
                    return (End::Shutdown, !first);
                }
            }
        }
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

    const LONG: Duration = Duration::from_secs(3600);
    const SHORT: Duration = Duration::from_secs(2);

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
