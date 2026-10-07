//! The single writer (see `docs/design/firehose.md`): batches events
//! (≤ 500 or 250 ms), applies each batch through `farsight-storage::apply`
//! in one transaction with the cursor, `applied_through` and the
//! `firehose_clock` row, and handles poisoned events.
//!
//! Storage calls outside the batch (session and gap bookkeeping, account
//! purges, the record of a poisoned event) go through
//! [`Writer::call_or_record`]: a transient error (the database is away)
//! is retried until it passes; any other error is tried
//! [`PERMANENT_ATTEMPTS`] times, then recorded as an operational error
//! and counted, and the writer goes on. Nothing the writer does waits
//! for ever on a call that cannot succeed.
//!
//! Ordering: every non-event item (session start, gap, disconnect,
//! barrier) first flushes the events before it, so gaps and the connected
//! flag are recorded in stream order.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use farsight_core::record::{CommitAction, CommitOp, Operation};
use farsight_core::{Collection, Did};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Write, WriteAction};
use farsight_storage::codes::GapCause;
use farsight_storage::counters::CounterSink;
use farsight_storage::error::StorageError;
use farsight_storage::firehose::{self, FirehoseProgress};
use farsight_storage::gates::SharedGates;
use farsight_storage::janitor;
use farsight_storage::keys::Limits;
use farsight_storage::repo_events::{RepoEvent, record_poisoned};
use farsight_storage::txn::ApplyReport;
use sqlx::PgPool;
use tokio::sync::{mpsc, oneshot};

use crate::frame::{Body, InEvent, Protocol};
use crate::metrics as m;
use crate::stats::IngestStats;

/// Batch size bound.
pub const BATCH_MAX: usize = 500;
/// Batch time bound.
pub const BATCH_WINDOW: Duration = Duration::from_millis(250);
/// Attempts of one event alone before it is poisoned.
pub const POISON_STRIKES: u32 = 3;
/// Attempts of a storage call that fails with an error that is not
/// transient, before the writer records the failure and goes on.
pub const PERMANENT_ATTEMPTS: u32 = 3;

/// What a storage call that failed should get next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// Try again after a pause, however often it takes.
    Again,
    /// Record the failure and go on.
    GiveUp,
}

/// Decides between another attempt and giving up: a transient error is
/// always retried; any other error until it has failed
/// [`PERMANENT_ATTEMPTS`] times (`failed` counts the attempts that ended
/// in an error that was not transient, this one included).
pub fn retry_decision(transient: bool, failed: u32) -> Retry {
    if transient || failed < PERMANENT_ATTEMPTS {
        Retry::Again
    } else {
        Retry::GiveUp
    }
}

/// What the reader sends the writer.
#[derive(Debug)]
pub enum Item {
    /// An event of the live session.
    Event(InEvent),
    /// Events re-read by a seam repair. Applied like any other event but
    /// never advance the cursor, `applied_through` or the clock: the live
    /// session may still be catching up behind them (a resume after an
    /// outage), and a cursor moved past un-replayed data would make the
    /// next reconnect skip it.
    Repair(Vec<InEvent>),
    /// A session to `url` speaking `protocol` started.
    Session {
        /// Instance URL (as configured).
        url: String,
        /// Negotiated protocol.
        protocol: Protocol,
    },
    /// A gap to record (witness µs).
    Gap {
        /// Start.
        from_us: i64,
        /// End.
        to_us: i64,
        /// Cause.
        cause: GapCause,
    },
    /// The session ended.
    Disconnected,
    /// Flush and acknowledge (the reader waits before reading the
    /// persisted cursor for a reconnect).
    Barrier(oneshot::Sender<()>),
}

/// Microseconds → `DateTime<Utc>`.
pub fn dt(us: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_micros(us).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

/// The writer task's inputs.
pub struct Writer {
    /// Ingest pool (4 connections).
    pub pool: PgPool,
    /// Limits.
    pub limits: Limits,
    /// Gates, published by the server's budget monitor and read once
    /// per batch.
    pub gates: Arc<SharedGates>,
    /// Counter sink (flushed by the caller's task).
    pub counters: Arc<CounterSink>,
    /// Shared stats.
    pub stats: Arc<IngestStats>,
    /// Fault injection (harness only).
    #[cfg(feature = "harness")]
    pub faults: Arc<crate::FaultHook>,
}

struct SessionState {
    url: String,
    protocol: Protocol,
    open_v1_gap: bool,
    close_v1_gap: bool,
}

/// Whether an error is transient (retried forever, never a poison strike):
/// connection loss, a restarting server, pool exhaustion, exhausted
/// deadlock retries (never count toward poisoned-event handling). Every
/// other error is permanent as far as the writer can tell: the same call
/// would fail the same way.
pub fn is_transient(e: &StorageError) -> bool {
    match e {
        StorageError::DeadlockRetriesExhausted(_) => true,
        StorageError::Db(sqlx::Error::Database(db)) => {
            let code = db.code().map(|c| c.into_owned()).unwrap_or_default();
            code.starts_with("08")
                || code.starts_with("57P")
                || code == "53300"
                || code == "40001"
                || code == "40P01"
        }
        StorageError::Db(
            sqlx::Error::Io(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::WorkerCrashed,
        ) => true,
        _ => false,
    }
}

fn op_label(op: &CommitOp) -> &'static str {
    match &op.action {
        CommitAction::Delete => "delete",
        CommitAction::Upsert {
            op: Operation::Update,
            ..
        } => "update",
        CommitAction::Upsert { .. } => "create",
    }
}

fn to_write(op: &CommitOp, witness_us: i64) -> Write {
    Write {
        author: op.author.clone(),
        collection: op.collection,
        rkey: op.rkey.clone(),
        stamp: op.rev.as_i64(),
        witness: Some(dt(witness_us)),
        action: match &op.action {
            CommitAction::Delete => WriteAction::Delete,
            CommitAction::Upsert { record, .. } => WriteAction::Upsert(record.clone()),
        },
    }
}

fn to_repo_event(ev: &InEvent) -> Option<RepoEvent> {
    let witness = dt(ev.witness_us);
    match &ev.body {
        Body::Identity(did) => Some(RepoEvent::Identity {
            did: did.clone(),
            witness,
        }),
        Body::Account {
            did,
            active,
            status,
        } => Some(RepoEvent::Account {
            did: did.clone(),
            witness,
            active: *active,
            status: status.clone(),
        }),
        Body::Sync(did) => Some(RepoEvent::Sync {
            did: did.clone(),
            witness,
        }),
        _ => None,
    }
}

/// The DID an event concerns (for poison bookkeeping).
fn event_did(ev: &InEvent) -> Option<&Did> {
    match &ev.body {
        Body::Commit(op) => Some(&op.author),
        Body::Identity(d) | Body::Sync(d) => Some(d),
        Body::Account { did, .. } => Some(did),
        _ => None,
    }
}

impl Writer {
    fn ctx(&self) -> ApplyCtx<'_> {
        ApplyCtx {
            limits: &self.limits,
            gates: self.gates.load(),
            counters: &self.counters,
        }
    }

    /// Runs until the channel closes.
    pub async fn run(self, mut rx: mpsc::Receiver<Item>) {
        let mut session: Option<SessionState> = None;
        let mut buf: Vec<InEvent> = Vec::with_capacity(BATCH_MAX);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            metrics::gauge!(m::BUFFER_DEPTH).set(rx.len() as f64);
            let item = tokio::select! {
                it = rx.recv() => it,
                _ = tick.tick() => {
                    self.refresh_gauges().await;
                    continue;
                }
            };
            let Some(item) = item else {
                self.flush(&mut buf, &mut session).await;
                return;
            };
            match item {
                Item::Event(ev) => {
                    buf.push(ev);
                    let deadline = tokio::time::Instant::now() + BATCH_WINDOW;
                    let mut pending_control = None;
                    while buf.len() < BATCH_MAX {
                        match tokio::time::timeout_at(deadline, rx.recv()).await {
                            Ok(Some(Item::Event(ev))) => buf.push(ev),
                            Ok(Some(other)) => {
                                pending_control = Some(other);
                                break;
                            }
                            Ok(None) | Err(_) => break,
                        }
                    }
                    self.flush(&mut buf, &mut session).await;
                    if let Some(ctl) = pending_control {
                        self.control(ctl, &mut buf, &mut session).await;
                    }
                }
                other => self.control(other, &mut buf, &mut session).await,
            }
        }
    }

    async fn control(
        &self,
        item: Item,
        buf: &mut Vec<InEvent>,
        session: &mut Option<SessionState>,
    ) {
        self.flush(buf, session).await;
        match item {
            Item::Event(_) => unreachable!("events are batched by run()"),
            Item::Repair(events) => {
                for chunk in events.chunks(BATCH_MAX) {
                    let mut b = chunk.to_vec();
                    self.flush_events(&mut b, session, false).await;
                }
            }
            Item::Session { url, protocol } => {
                let u = url.clone();
                // Given up, the connected flag stays as it was; the batches
                // of the session carry its URL and protocol themselves.
                self.call_or_record("mark_connected", None, || {
                    firehose::mark_connected(&self.pool, &u, protocol.storage())
                })
                .await;
                *session = Some(SessionState {
                    url,
                    protocol,
                    // Every interval spent on v1 is a sync_unavailable gap,
                    // opened at session start, closed when v2 takes over.
                    open_v1_gap: protocol == Protocol::V1,
                    close_v1_gap: protocol == Protocol::V2,
                });
            }
            Item::Gap {
                from_us,
                to_us,
                cause,
            } => {
                let recorded = self
                    .call_or_record("record_gap", None, || {
                        firehose::record_gap(&self.pool, dt(from_us), dt(to_us), cause)
                    })
                    .await;
                Self::gap_written("record_gap", recorded.is_some());
                self.stats.gaps.fetch_add(1, Ordering::Relaxed);
                self.refresh_gauges().await;
            }
            Item::Disconnected => {
                self.call_or_record("set_connected", None, || {
                    firehose::set_connected(&self.pool, false)
                })
                .await;
            }
            Item::Barrier(ack) => {
                let _ = ack.send(());
            }
        }
    }

    /// A gap that cannot be written must not be passed over: coverage is
    /// claimed from the recorded gaps, so going on would claim more than
    /// was witnessed. The writer stops instead (a panic here ends the
    /// process, as any writer panic does), and the next start resumes
    /// from the persisted cursor and meets the same gap again.
    fn gap_written(op: &'static str, written: bool) {
        assert!(
            written,
            "{op} failed permanently: a firehose gap could not be recorded"
        );
    }

    /// Makes a storage call, retrying as [`retry_decision`] says. Returns
    /// `None` when it was given up: the failure is then logged, counted
    /// in `farsight_ingest_storage_errors_total{op}` and recorded in
    /// `op_errors` (with `did`, when the call concerns one account), and
    /// the caller goes on without the call's effect.
    async fn call_or_record<F, Fut, T>(
        &self,
        op: &'static str,
        did: Option<&Did>,
        mut f: F,
    ) -> Option<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, StorageError>>,
    {
        let mut backoff = Duration::from_millis(200);
        let mut failed = 0u32;
        loop {
            let e = match f().await {
                Ok(v) => return Some(v),
                Err(e) => e,
            };
            let transient = is_transient(&e);
            if !transient {
                failed += 1;
            }
            match retry_decision(transient, failed) {
                Retry::Again => {
                    tracing::warn!(op, error = %e, transient, "ingest storage call failed; retrying");
                    if transient {
                        self.stats.transient_retries.fetch_add(1, Ordering::Relaxed);
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
                Retry::GiveUp => {
                    tracing::error!(
                        op,
                        did = did.map(Did::as_str),
                        error = %e,
                        "ingest storage call failed permanently; going on without it"
                    );
                    metrics::counter!(m::STORAGE_ERRORS, "op" => op).increment(1);
                    let message = format!("{op} failed permanently: {e}");
                    if let Err(e) = farsight_storage::auth::record_op_error(
                        &self.pool,
                        "ingest",
                        did.map(Did::as_str),
                        &message,
                    )
                    .await
                    {
                        tracing::warn!(error = %e, "recording the operational error failed");
                    }
                    return None;
                }
            }
        }
    }

    async fn refresh_gauges(&self) {
        if let Ok(st) = firehose::read_state(&self.pool).await {
            if let Some(a) = st.applied_through {
                let lag = (Utc::now() - a).num_microseconds().unwrap_or(0) as f64 / 1e6;
                metrics::gauge!(m::LAG).set(lag.max(0.0));
            }
        }
        if let Ok(gaps) = firehose::unhealed_gaps(&self.pool).await {
            metrics::gauge!(m::OPEN_GAPS).set(gaps.len() as f64);
        }
    }

    async fn flush(&self, buf: &mut Vec<InEvent>, session: &mut Option<SessionState>) {
        self.flush_events(buf, session, true).await;
    }

    /// Applies `buf` in one batch. `live` batches carry the firehose
    /// progress (cursor, `applied_through`, clock row); repair batches do
    /// not.
    async fn flush_events(
        &self,
        buf: &mut Vec<InEvent>,
        session: &mut Option<SessionState>,
        live: bool,
    ) {
        if buf.is_empty() {
            return;
        }
        let events = std::mem::take(buf);
        let Some(s) = session.as_mut() else {
            tracing::error!("events without a session; dropping {}", events.len());
            return;
        };
        let started = Instant::now();
        let first_us = events[0].witness_us;
        if live && s.open_v1_gap {
            let from = self
                .call_or_record("read_state", None, || firehose::read_state(&self.pool))
                .await
                .and_then(|st| st.applied_through)
                .unwrap_or_else(|| dt(first_us));
            let opened = self
                .call_or_record("open_sync_unavailable", None, || {
                    firehose::open_sync_unavailable(&self.pool, from)
                })
                .await;
            Self::gap_written("open_sync_unavailable", opened.is_some());
            s.open_v1_gap = false;
        }
        if live && s.close_v1_gap {
            let closed = self
                .call_or_record("close_sync_unavailable", None, || {
                    firehose::close_sync_unavailable(&self.pool, dt(first_us))
                })
                .await;
            Self::gap_written("close_sync_unavailable", closed.is_some());
            s.close_v1_gap = false;
        }

        let progress = FirehoseProgress {
            source_url: s.url.clone(),
            protocol: s.protocol.storage(),
            cursor_seq: events.iter().filter_map(|e| e.seq).max(),
            cursor_us: events.iter().map(|e| e.witness_us).max(),
            applied_through: dt(events
                .iter()
                .map(|e| e.witness_us)
                .max()
                .unwrap_or(first_us)),
        };
        let mut batch = Batch::new(Origin::Firehose);
        let mut labels: Vec<(Collection, &'static str)> = Vec::new();
        for ev in &events {
            match &ev.body {
                Body::Commit(op) => {
                    batch.writes.push(to_write(op, ev.witness_us));
                    labels.push((op.collection, op_label(op)));
                }
                Body::Rejected {
                    reason, collection, ..
                } => {
                    metrics::counter!(m::DROPPED, "reason" => reason.label()).increment(1);
                    let c = collection.map(|c| c.nsid()).unwrap_or("unknown");
                    metrics::counter!(m::EVENTS, "collection" => c, "op" => "unknown", "outcome" => "dropped")
                        .increment(1);
                    self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    if let Some(re) = to_repo_event(ev) {
                        batch.events.push(re);
                    }
                }
            }
        }
        if live {
            batch.firehose = Some(progress);
        }

        let n_writes = batch.writes.len();
        let n_events = batch.events.len();
        let apply_started = Instant::now();
        let report = match self.apply_or_poison(&events, batch).await {
            Some(r) => r,
            None => return,
        };
        let apply_secs = apply_started.elapsed().as_secs_f64();
        // An unknown DID becoming active is recorded only as a metric,
        // under the `account` collection.
        if report.unknown_activations > 0 {
            metrics::counter!(m::EVENTS, "collection" => "account", "op" => "activate", "outcome" => "applied")
                .increment(report.unknown_activations);
        }
        for (outcome, (c, op)) in report.write_outcomes.iter().zip(&labels) {
            metrics::counter!(m::EVENTS, "collection" => c.nsid(), "op" => *op, "outcome" => outcome.label())
                .increment(1);
        }
        if live {
            self.stats.batches.fetch_add(1, Ordering::Relaxed);
        }
        self.stats
            .events
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        self.stats
            .writes_applied
            .fetch_add(report.applied, Ordering::Relaxed);
        metrics::histogram!(m::BATCH_SECONDS).record(started.elapsed().as_secs_f64());
        let lag = (Utc::now()
            - dt(events
                .iter()
                .map(|e| e.witness_us)
                .max()
                .unwrap_or(first_us)))
        .num_microseconds()
        .unwrap_or(0) as f64
            / 1e6;
        if live {
            metrics::gauge!(m::LAG).set(lag.max(0.0));
        }
        // Purge accounts that became `deleted` (multi-transaction,
        // after the status commit; resumed at start-up if interrupted).
        let purge_started = Instant::now();
        // A purge given up leaves the account `deleted` with rows still
        // stored: its rows are already withheld by its status, and the
        // purge is taken up again by the daily task and at the next start
        // (`janitor::accounts_pending_purge`).
        for did in &report.deleted_accounts {
            self.call_or_record("purge_account", Some(did), || {
                janitor::purge_account(&self.pool, &self.limits, &self.counters, did)
            })
            .await;
        }
        let total = started.elapsed().as_secs_f64();
        if total > 5.0 {
            tracing::warn!(
                total_secs = total,
                apply_secs,
                purge_secs = purge_started.elapsed().as_secs_f64(),
                purged_accounts = report.deleted_accounts.len(),
                writes = n_writes,
                repo_events = n_events,
                deadlock_retries = report.deadlock_retries,
                "slow ingest batch"
            );
        }
    }

    #[cfg(feature = "harness")]
    fn injected(&self, batch: &Batch) -> bool {
        let set = self
            .faults
            .poison_dids
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if set.is_empty() {
            return false;
        }
        batch.writes.iter().any(|w| set.contains(w.author.as_str()))
            || batch.events.iter().any(|e| set.contains(e.did().as_str()))
    }

    #[cfg(not(feature = "harness"))]
    fn injected(&self, _batch: &Batch) -> bool {
        false
    }

    /// Applies with transient retries; returns `Err` only for a
    /// non-transient failure.
    async fn apply_resilient(&self, batch: &Batch) -> Result<ApplyReport, StorageError> {
        let mut backoff = Duration::from_millis(200);
        loop {
            if self.injected(batch) {
                return Err(StorageError::Invariant("injected fault (harness)".into()));
            }
            match apply::apply(&self.pool, &self.ctx(), batch).await {
                Ok(r) => {
                    for x in &r.refusals {
                        metrics::counter!(crate::metrics::ABUSE_CAPPED, "kind" => x.refusal.cap_type().label())
                            .increment(1);
                    }
                    return Ok(r);
                }
                Err(e) if is_transient(&e) => {
                    tracing::warn!(error = %e, "transient apply failure; retrying");
                    self.stats.transient_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Failing batch retried; then events applied one by one; an event
    /// failing 3 times alone is logged to `op_errors` and its DID gets a
    /// `resync` debt and a tier-1 re-list. The batch's progress is then
    /// persisted on its own.
    async fn apply_or_poison(&self, events: &[InEvent], batch: Batch) -> Option<ApplyReport> {
        let first = match self.apply_resilient(&batch).await {
            Ok(r) => return Some(r),
            Err(e) => e,
        };
        tracing::warn!(error = %first, "batch failed; retrying once");
        if let Ok(r) = self.apply_resilient(&batch).await {
            return Some(r);
        }
        tracing::warn!(
            "batch failed twice; applying its {} events one by one",
            events.len()
        );
        let mut merged = ApplyReport::default();
        let mut write_iter = batch.writes.iter();
        let mut event_iter = batch.events.iter();
        for ev in events {
            let mut single = Batch::new(Origin::Firehose);
            match &ev.body {
                Body::Commit(_) => {
                    if let Some(w) = write_iter.next() {
                        single.writes.push(w.clone());
                    }
                }
                Body::Identity(_) | Body::Account { .. } | Body::Sync(_) => {
                    if let Some(e) = event_iter.next() {
                        single.events.push(e.clone());
                    }
                }
                _ => continue,
            }
            let mut strikes = 0;
            let mut last_err = String::new();
            loop {
                match self.apply_resilient(&single).await {
                    Ok(r) => {
                        merged.applied += r.applied;
                        merged.write_outcomes.extend(r.write_outcomes);
                        merged.deleted_accounts.extend(r.deleted_accounts);
                        merged.unknown_activations += r.unknown_activations;
                        break;
                    }
                    Err(e) => {
                        strikes += 1;
                        last_err = e.to_string();
                        if strikes >= POISON_STRIKES {
                            break;
                        }
                    }
                }
            }
            if strikes >= POISON_STRIKES {
                if let Some(did) = event_did(ev) {
                    let msg = format!("poisoned event (witness {}): {last_err}", ev.witness_us);
                    tracing::error!(%did, "{msg}");
                    // Given up, the event has no `resync` debt; the
                    // operational error names its DID.
                    self.call_or_record("record_poisoned", Some(did), || {
                        record_poisoned(
                            &self.pool,
                            &self.limits,
                            &self.counters,
                            did,
                            &msg,
                            dt(ev.witness_us),
                        )
                    })
                    .await;
                }
                metrics::counter!(m::DROPPED, "reason" => "poisoned").increment(1);
                self.stats.poisoned.fetch_add(1, Ordering::Relaxed);
                if matches!(ev.body, Body::Commit(_)) {
                    merged
                        .write_outcomes
                        .push(farsight_storage::txn::WriteOutcome::Refused);
                }
            }
        }
        // Progress on its own, so the cursor never runs ahead of applied
        // (or poison-recorded) events.
        if batch.firehose.is_none() {
            return Some(merged);
        }
        let mut tail = Batch::new(Origin::Firehose);
        tail.firehose = batch.firehose.clone();
        match self.apply_resilient(&tail).await {
            Ok(_) => Some(merged),
            Err(e) => {
                tracing::error!(error = %e, "cannot persist progress after poisoned batch");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_errors_are_retried_without_end() {
        for failed in [0, 1, PERMANENT_ATTEMPTS, 10_000] {
            assert_eq!(retry_decision(true, failed), Retry::Again);
        }
    }

    #[test]
    fn a_permanent_error_is_given_up_after_its_attempts() {
        assert_eq!(retry_decision(false, 1), Retry::Again);
        assert_eq!(retry_decision(false, PERMANENT_ATTEMPTS - 1), Retry::Again);
        assert_eq!(retry_decision(false, PERMANENT_ATTEMPTS), Retry::GiveUp);
        assert_eq!(retry_decision(false, PERMANENT_ATTEMPTS + 1), Retry::GiveUp);
    }

    #[test]
    fn errors_are_classified() {
        // The database is away: wait for it.
        assert!(is_transient(&StorageError::Db(sqlx::Error::PoolTimedOut)));
        assert!(is_transient(&StorageError::Db(sqlx::Error::PoolClosed)));
        assert!(is_transient(&StorageError::Db(sqlx::Error::Io(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset)
        ))));
        assert!(is_transient(&StorageError::DeadlockRetriesExhausted(8)));
        // The call itself is wrong: the same call fails the same way.
        assert!(!is_transient(&StorageError::Invariant("bad row".into())));
        assert!(!is_transient(&StorageError::Db(sqlx::Error::RowNotFound)));
        assert!(!is_transient(&StorageError::Db(
            sqlx::Error::ColumnNotFound("x".into())
        )));
    }
}
