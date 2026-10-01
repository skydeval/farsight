//! The single writer (§6.2): batches events (≤ 500 or 250 ms), applies
//! each batch through `farsight-storage::apply` in one transaction with
//! the cursor, `applied_through` and the `firehose_clock` row, and handles
//! poisoned events.
//!
//! Ordering: every non-event item (session start, gap, disconnect,
//! barrier) first flushes the events before it, so gaps and the connected
//! flag are recorded in stream order.

use std::collections::BTreeMap;
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
use farsight_storage::janitor;
use farsight_storage::keys::Limits;
use farsight_storage::repo_events::{RepoEvent, record_poisoned};
use farsight_storage::txn::{ApplyReport, Gates};
use sqlx::PgPool;
use tokio::sync::{mpsc, oneshot};

use crate::frame::{Body, InEvent, Protocol};
use crate::metrics as m;
use crate::stats::IngestStats;

/// Batch size bound (§6.2).
pub const BATCH_MAX: usize = 500;
/// Batch time bound (§6.2).
pub const BATCH_WINDOW: Duration = Duration::from_millis(250);
/// Attempts of one event alone before it is poisoned (§6.2).
pub const POISON_STRIKES: u32 = 3;

/// What the reader sends the writer.
#[derive(Debug)]
pub enum Item {
    /// An event.
    Event(InEvent),
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
    /// Ingest pool (4 connections, §6.2).
    pub pool: PgPool,
    /// Limits.
    pub limits: Limits,
    /// Gates. Stage 2 runs with open gates; the budget monitor that feeds
    /// them is a server task (server stage).
    pub gates: Gates,
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
/// deadlock retries (§4.3: never count toward poisoned-event handling).
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
            gates: self.gates,
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
            Item::Session { url, protocol } => {
                let u = url.clone();
                self.retry_transient(|| {
                    firehose::mark_connected(&self.pool, &u, protocol.storage())
                })
                .await;
                *session = Some(SessionState {
                    url,
                    protocol,
                    // §6.5: every interval spent on v1 is a sync_unavailable
                    // gap, opened at session start, closed when v2 takes over.
                    open_v1_gap: protocol == Protocol::V1,
                    close_v1_gap: protocol == Protocol::V2,
                });
            }
            Item::Gap {
                from_us,
                to_us,
                cause,
            } => {
                self.retry_transient(|| {
                    firehose::record_gap(&self.pool, dt(from_us), dt(to_us), cause)
                })
                .await;
                self.stats.gaps.fetch_add(1, Ordering::Relaxed);
                self.refresh_gauges().await;
            }
            Item::Disconnected => {
                self.retry_transient(|| firehose::set_connected(&self.pool, false))
                    .await;
            }
            Item::Barrier(ack) => {
                let _ = ack.send(());
            }
        }
    }

    async fn retry_transient<F, Fut, T>(&self, mut f: F) -> T
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, StorageError>>,
    {
        let mut backoff = Duration::from_millis(200);
        loop {
            match f().await {
                Ok(v) => return v,
                Err(e) => {
                    tracing::warn!(error = %e, "ingest storage call failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
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
        if s.open_v1_gap {
            let from = self
                .retry_transient(|| firehose::read_state(&self.pool))
                .await
                .applied_through
                .unwrap_or_else(|| dt(first_us));
            self.retry_transient(|| firehose::open_sync_unavailable(&self.pool, from))
                .await;
            s.open_v1_gap = false;
        }
        if s.close_v1_gap {
            self.retry_transient(|| firehose::close_sync_unavailable(&self.pool, dt(first_us)))
                .await;
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
        batch.firehose = Some(progress);

        let n_writes = batch.writes.len();
        let n_events = batch.events.len();
        let apply_started = Instant::now();
        let report = match self.apply_or_poison(&events, batch).await {
            Some(r) => r,
            None => return,
        };
        let apply_secs = apply_started.elapsed().as_secs_f64();
        // §6.4 (r17): an unknown DID becoming active is recorded only as a
        // metric, under the `account` collection.
        if report.unknown_activations > 0 {
            metrics::counter!(m::EVENTS, "collection" => "account", "op" => "activate", "outcome" => "applied")
                .increment(report.unknown_activations);
        }
        for (outcome, (c, op)) in report.write_outcomes.iter().zip(&labels) {
            metrics::counter!(m::EVENTS, "collection" => c.nsid(), "op" => *op, "outcome" => outcome.label())
                .increment(1);
        }
        self.stats.batches.fetch_add(1, Ordering::Relaxed);
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
        metrics::gauge!(m::LAG).set(lag.max(0.0));
        // §7.4: purge accounts that became `deleted` (multi-transaction,
        // after the status commit; resumed at start-up if interrupted).
        let purge_started = Instant::now();
        for did in &report.deleted_accounts {
            self.retry_transient(|| {
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
                Ok(r) => return Ok(r),
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

    /// §6.2: failing batch retried; then events applied one by one; an
    /// event failing 3 times alone is logged to `op_errors` and its DID
    /// gets a `resync` debt and a tier-1 re-list. The batch's progress is
    /// then persisted on its own.
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
                    self.retry_transient(|| {
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

/// Per-collection counts of a batch's writes, for logs.
pub fn summarize(writes: &[Write]) -> BTreeMap<&'static str, usize> {
    let mut m = BTreeMap::new();
    for w in writes {
        *m.entry(w.collection.nsid()).or_insert(0) += 1;
    }
    m
}
