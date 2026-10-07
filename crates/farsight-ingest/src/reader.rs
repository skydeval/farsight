//! The reader task (see `docs/design/firehose.md`): connects to the
//! configured instances, detects the protocol, resumes from the
//! persisted cursor, enforces the stall timeout, records resume gaps,
//! fails over between instances, and feeds the bounded channel (a full
//! channel stops reading: TCP backpressure).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use farsight_storage::codes::GapCause;
use farsight_storage::firehose;
use sqlx::PgPool;
use tokio::sync::{mpsc, oneshot};

use crate::conn::{self, ConnectError, Session};
use crate::frame::{Body, Frame, InEvent, Protocol};
use crate::lag::LagTracker;
use crate::metrics as m;
use crate::resume::{self, Cursor, GapRule, Persisted, Plan, Tuning};
use crate::stats::IngestStats;
use crate::writer::Item;

/// Consecutive failed sessions on one instance before failing over.
pub const FAILOVER_AFTER: u32 = 3;

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
    /// Stop.
    Shutdown,
}

/// Reader configuration.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// `firehose.urls`, in failover order.
    pub urls: Vec<String>,
    /// Tuning.
    pub tuning: Tuning,
    /// `firehose.tuning.stall_timeout`.
    pub stall_timeout: Duration,
    /// Request zstd frames.
    pub compress: bool,
    /// Seam repair.
    pub seam: SeamRepair,
}

/// The reader.
pub struct Reader {
    /// Config.
    pub cfg: ReaderConfig,
    /// Pool (reads the persisted cursor).
    pub pool: PgPool,
    /// To the writer.
    pub tx: mpsc::Sender<Item>,
    /// Commands.
    pub control: mpsc::Receiver<Control>,
    /// Stats.
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

enum End {
    Reconnect(&'static str),
    Shutdown,
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
                    metrics::counter!(m::RECONNECTS, "reason" => "cursor_too_old").increment(1);
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
        let mut idx = 0usize;
        let mut failures = 0u32;
        let mut lag = LagTracker::new();
        let mut prev_instance_lag: Option<Duration> = None;
        let mut current_url: Option<String> = None;
        let mut backoff = Duration::from_millis(500);
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
                let r = sqlx::query(
                    "UPDATE firehose_state SET cursor_us = $1, applied_through = $2,
                       cursor_seq = CASE WHEN protocol = 2 THEN 1 ELSE cursor_seq END
                     WHERE id = 1",
                )
                .bind(us)
                .bind(t)
                .execute(&self.pool)
                .await;
                let r2 = sqlx::query(
                    "UPDATE firehose_cursors SET cursor_us = $1, last_applied_through = $2,
                       cursor_seq = CASE WHEN protocol = 2 THEN 1 ELSE cursor_seq END",
                )
                .bind(us)
                .bind(t)
                .execute(&self.pool)
                .await;
                tracing::warn!(us, ok = r.is_ok() && r2.is_ok(), "harness rewind applied");
            }
            let url = self.cfg.urls[idx % self.cfg.urls.len()].clone();
            let failover_lag =
                if current_url.as_deref() == Some(url.as_str()) || current_url.is_none() {
                    None
                } else {
                    prev_instance_lag
                };
            let opened = self.open(&url, failover_lag).await;
            let failover = current_url.as_deref().is_some_and(|u| u != url.as_str());
            let (mut session, rule, prior) = match opened {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!(url, error = %e, "connect failed");
                    metrics::counter!(m::RECONNECTS, "reason" => "connect_error").increment(1);
                    self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                    failures += 1;
                    if failures >= FAILOVER_AFTER && self.cfg.urls.len() > 1 {
                        prev_instance_lag = lag.instance_lag();
                        lag.reset();
                        idx += 1;
                        failures = 0;
                        metrics::counter!(m::RECONNECTS, "reason" => "failover").increment(1);
                        tracing::warn!(next = %self.cfg.urls[idx % self.cfg.urls.len()], "failing over");
                    }
                    if self.sleep_or_shutdown(backoff).await {
                        return;
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(30));
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
            let end = self
                .read_session(&mut session, rule, prior, failover, &mut lag, &mut failures)
                .await;
            session.close().await;
            set_connected_gauge(None);
            if self.tx.send(Item::Disconnected).await.is_err() {
                return;
            }
            match end {
                End::Shutdown => return,
                End::Reconnect(reason) => {
                    tracing::warn!(url, reason, "jetstream session ended; reconnecting");
                    metrics::counter!(m::RECONNECTS, "reason" => reason).increment(1);
                    self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                    if failures >= FAILOVER_AFTER && self.cfg.urls.len() > 1 {
                        prev_instance_lag = lag.instance_lag();
                        lag.reset();
                        idx += 1;
                        failures = 0;
                        metrics::counter!(m::RECONNECTS, "reason" => "failover").increment(1);
                    }
                    if reason != "kill" {
                        if self.sleep_or_shutdown(backoff).await {
                            return;
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        }
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
        trigger: &'static str,
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
        let us = |d: Duration| i64::try_from(d.as_micros()).unwrap_or(i64::MAX);
        let until = caught_up_us + us(cfg.after);
        let cursor = Cursor::TimeUs(connect_us - us(cfg.before));
        tracing::info!(connect_us, caught_up_us, trigger, "seam repair scheduled");
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
            loop {
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
            metrics::counter!(m::SEAM_REPAIRS, "trigger" => trigger).increment(1);
            metrics::counter!(m::SEAM_REPAIR_EVENTS).increment(n);
            tracing::info!(events = n, trigger, "seam repair replayed");
        });
        self.repairs.0.push(handle);
    }

    async fn sleep_or_shutdown(&mut self, d: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => false,
            c = self.control.recv() => matches!(c, Some(Control::Shutdown) | None),
        }
    }

    async fn read_session(
        &mut self,
        session: &mut Session,
        rule: GapRule,
        prior: bool,
        failover: bool,
        lag: &mut LagTracker,
        failures: &mut u32,
    ) -> End {
        let mut first = true;
        let mut clamped_notice = false;
        let mut since_gauge = 0u32;
        // The seam is where the replay hands over to the live tail: when
        // the session catches up, which for a long replay is long after the
        // resume. The repair is anchored there.
        let connect_us = chrono::Utc::now().timestamp_micros();
        let mut seam_trigger: Option<&'static str> = None;
        // Stream position of the last delivered event: injected events are
        // re-stamped to it, so a synthetic event never carries the cursor
        // past a replay still in progress.
        let mut position_us: Option<i64> = None;
        loop {
            let frame = tokio::select! {
                c = self.control.recv() => match c {
                    Some(Control::KillSocket) => return End::Reconnect("kill"),
                    #[cfg(feature = "harness")]
                    Some(Control::KillAndRewind { us }) => {
                        self.rewind = Some(us);
                        return End::Reconnect("kill");
                    }
                    Some(Control::Inject(evs)) => {
                        for ev in evs {
                            let ev = at_position(ev, position_us);
                            if self.tx.send(Item::Event(ev)).await.is_err() {
                                return End::Shutdown;
                            }
                        }
                        continue;
                    }
                    Some(Control::Shutdown) | None => return End::Shutdown,
                },
                f = tokio::time::timeout(self.cfg.stall_timeout, session.next_frame()) => f,
            };
            let frame = match frame {
                Err(_) => return End::Reconnect("stall"),
                Ok(None) => {
                    if first {
                        *failures += 1;
                    }
                    return End::Reconnect("closed");
                }
                Ok(Some(Err(e))) => {
                    tracing::warn!(error = %e, "read error");
                    if first {
                        *failures += 1;
                    }
                    return End::Reconnect("error");
                }
                Ok(Some(Ok(f))) => f,
            };
            let ev = match frame {
                Frame::Info { name, message } => {
                    tracing::warn!(%name, message = ?message, "jetstream #info");
                    if name == "OutdatedCursor" {
                        clamped_notice = true;
                    }
                    continue;
                }
                Frame::Error { error, message } => {
                    tracing::warn!(%error, message = ?message, "jetstream error frame");
                    return End::Reconnect("server_error");
                }
                Frame::Event(ev) => ev,
            };
            if first {
                first = false;
                *failures = 0;
                let gap = resume::gap_for_first_event(
                    rule,
                    ev.witness_us,
                    clamped_notice,
                    &self.cfg.tuning,
                );
                // After any resume with a prior position (skipped only
                // on the first-ever start).
                if prior {
                    seam_trigger = Some(if gap.is_some() {
                        "clamp_recovery"
                    } else if failover {
                        "failover"
                    } else {
                        "resume"
                    });
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
                let now = chrono::Utc::now().timestamp_micros();
                if let Some(s) = lag.source_lag_seconds(now) {
                    metrics::gauge!(m::SOURCE_LAG).set(s);
                    self.stats
                        .source_lag_ms
                        .store((s * 1000.0) as i64, Ordering::Relaxed);
                }
            }
            if let Some(trigger) = seam_trigger {
                let now = chrono::Utc::now().timestamp_micros();
                let margin =
                    i64::try_from(self.cfg.seam.catchup_margin.as_micros()).unwrap_or(i64::MAX);
                if now - ev.witness_us <= margin {
                    seam_trigger = None;
                    tracing::warn!(
                        connect_us,
                        caught_up_us = now,
                        lag_secs = (now - connect_us) / 1_000_000,
                        trigger,
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
                            return End::Shutdown;
                        }
                        break;
                    }
                    c = self.control.recv() => match c {
                        Some(Control::KillSocket) => return End::Reconnect("kill"),
                        #[cfg(feature = "harness")]
                        Some(Control::KillAndRewind { us }) => {
                            self.rewind = Some(us);
                            return End::Reconnect("kill");
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
            position_us = Some(witness_us);
            for ev in std::mem::take(&mut self.pending_inject) {
                let ev = at_position(ev, position_us);
                if self.tx.send(Item::Event(ev)).await.is_err() {
                    return End::Shutdown;
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
