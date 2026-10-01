//! The reader task (§6.1, §6.3): connects to the configured instances,
//! detects the protocol, resumes from the persisted cursor, enforces the
//! stall timeout, records resume gaps, fails over between instances, and
//! feeds the bounded channel (a full channel stops reading: TCP
//! backpressure).

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

/// Seam repair: how long after a resumed session's first event the side
/// replay starts, and how far past that event it reads. Public instances
/// were observed (stage-2 Phase B) to drop events witnessed within about a
/// second of a timestamp-cursor resume — at the hand-over from replay to
/// the live tail — while a later replay of the same window returns them.
pub const SEAM_DELAY: Duration = Duration::from_secs(60);
/// See [`SEAM_DELAY`].
pub const SEAM_COVER_US: i64 = 30_000_000;

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

    async fn persisted(&self) -> Persisted {
        let mut backoff = Duration::from_millis(200);
        loop {
            match firehose::read_state(&self.pool).await {
                Ok(s) => {
                    return Persisted {
                        source_url: s.source_url,
                        protocol: s.protocol.map(|p| match p {
                            farsight_storage::codes::Protocol::V1 => Protocol::V1,
                            farsight_storage::codes::Protocol::V2 => Protocol::V2,
                        }),
                        cursor_seq: s.cursor_seq,
                        cursor_us: s.cursor_us,
                        applied_through_us: s.applied_through.map(|a| a.timestamp_micros()),
                    };
                }
                Err(e) => {
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
    ) -> Result<(Session, GapRule, Cursor), ConnectError> {
        let p = self.persisted().await;
        let t = &self.cfg.tuning;
        let mut compress = self.cfg.compress;
        let mut protocols = vec![Protocol::V2, Protocol::V1];
        while let Some(proto) = protocols.first().copied() {
            let Plan { cursor, gap } = resume::plan(&p, url, proto, lag, t);
            match conn::connect(url, proto, cursor, compress).await {
                Ok(s) => return Ok((s, gap, cursor)),
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
                    // §6.3: gap [applied_through, first live event], resume
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
                        Cursor::Live,
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
            // event already read (§6.2: a dropped connection reconnects
            // from the persisted cursor).
            if !self.barrier().await {
                return;
            }
            #[cfg(feature = "harness")]
            if let Some(us) = self.rewind.take() {
                let t = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(us)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                let r = sqlx::query(
                    "UPDATE firehose_state SET cursor_us = $1, applied_through = $2,
                       cursor_seq = CASE WHEN protocol = 2 THEN 1 ELSE cursor_seq END
                     WHERE id = 1",
                )
                .bind(us)
                .bind(t)
                .execute(&self.pool)
                .await;
                tracing::warn!(us, ok = r.is_ok(), "harness rewind applied");
            }
            let url = self.cfg.urls[idx % self.cfg.urls.len()].clone();
            let failover_lag =
                if current_url.as_deref() == Some(url.as_str()) || current_url.is_none() {
                    None
                } else {
                    prev_instance_lag
                };
            let opened = self.open(&url, failover_lag).await;
            let (mut session, rule, cursor) = match opened {
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
                .read_session(&mut session, rule, cursor, &mut lag, &mut failures)
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

    /// Re-reads `[cursor, resume moment + SEAM_COVER]` a minute later, through
    /// the normal pipeline (LWW makes the duplicates stale no-ops), to
    /// recover events the instance dropped at the resume seam.
    fn spawn_seam_repair(&mut self, url: &str, protocol: Protocol, cursor: Cursor, first_us: i64) {
        self.repairs.0.retain(|h| !h.is_finished());
        let url = url.to_owned();
        let tx = self.tx.clone();
        let stats = self.stats.clone();
        let compress = self.cfg.compress;
        #[cfg(feature = "harness")]
        let tap = self.tap.clone();
        // The seam is at the moment of the resume, not at the replay's first
        // event (which is up to 120 s earlier on v1).
        let until = first_us.max(chrono::Utc::now().timestamp_micros()) + SEAM_COVER_US;
        let handle = tokio::spawn(async move {
            tokio::time::sleep(SEAM_DELAY).await;
            let mut s = match conn::connect(&url, protocol, cursor, compress).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "seam repair: connect failed");
                    return;
                }
            };
            let mut n = 0u64;
            loop {
                match tokio::time::timeout(Duration::from_secs(30), s.next_frame()).await {
                    Ok(Some(Ok(Frame::Event(ev)))) => {
                        let past = ev.witness_us > until;
                        #[cfg(feature = "harness")]
                        if let Some(tap) = &tap {
                            let _ = tap.send(ev.clone());
                        }
                        if tx.send(Item::Event(ev)).await.is_err() {
                            break;
                        }
                        n += 1;
                        if past {
                            break;
                        }
                    }
                    Ok(Some(Ok(_))) => {}
                    _ => break,
                }
            }
            s.close().await;
            stats.seam_repairs.fetch_add(1, Ordering::Relaxed);
            stats.seam_repair_events.fetch_add(n, Ordering::Relaxed);
            tracing::info!(events = n, "seam repair replayed");
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
        cursor: Cursor,
        lag: &mut LagTracker,
        failures: &mut u32,
    ) -> End {
        let mut first = true;
        let mut clamped_notice = false;
        let mut since_gauge = 0u32;
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
                if cursor != Cursor::Live {
                    self.spawn_seam_repair(
                        &session.url_base,
                        session.protocol,
                        cursor,
                        ev.witness_us,
                    );
                }
                if let Some((from_us, to_us, cause)) = resume::gap_for_first_event(
                    rule,
                    ev.witness_us,
                    clamped_notice,
                    &self.cfg.tuning,
                ) {
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
                }
            }
            #[cfg(feature = "harness")]
            if let Some(tap) = &self.tap {
                let _ = tap.send(ev.clone());
            }
            if self.tx.send(Item::Event(ev)).await.is_err() {
                return End::Shutdown;
            }
        }
    }
}
