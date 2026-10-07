//! The in-memory global coverage snapshot (see
//! `docs/design/coverage.md`): refreshed on `NOTIFY farsight_coverage`,
//! every 10 s, and on every LISTEN reconnect, each time with one
//! `REPEATABLE READ` transaction, so a lost notification can only delay,
//! never falsify. A snapshot that could not be read again for
//! [`SNAPSHOT_MAX_AGE`] is handed out marked stale, which every scope
//! treats as the synthetic gap: what it says about the stream is not
//! known to hold any more.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use farsight_storage::codes::DebtReason;
use farsight_storage::coverage::{
    self, COVERAGE_CHANNEL, GlobalSnapshot, SNAPSHOT_MAX_AGE, SNAPSHOT_REFRESH,
};
use farsight_storage::keys::Limits;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::watch;

use crate::config_store::ConfigStore;
use crate::metrics as m;

/// The latest snapshot and when it was read, by this process's clock.
#[derive(Debug)]
struct Held {
    snapshot: Arc<GlobalSnapshot>,
    read: Instant,
    /// The same snapshot marked stale, made the first time it is asked
    /// for past its age.
    stale: Option<Arc<GlobalSnapshot>>,
}

/// Holds the latest snapshot.
#[derive(Debug, Default)]
pub struct SnapshotHolder {
    inner: RwLock<Option<Held>>,
}

impl SnapshotHolder {
    /// The latest snapshot (`None` until the first read succeeds). One
    /// older than [`SNAPSHOT_MAX_AGE`] comes back marked stale.
    pub fn get(&self) -> Option<Arc<GlobalSnapshot>> {
        self.get_at(Instant::now())
    }

    fn get_at(&self, now: Instant) -> Option<Arc<GlobalSnapshot>> {
        {
            let held = self.inner.read().unwrap_or_else(|e| e.into_inner());
            let h = held.as_ref()?;
            if now.saturating_duration_since(h.read) <= SNAPSHOT_MAX_AGE {
                return Some(h.snapshot.clone());
            }
            if let Some(s) = &h.stale {
                return Some(s.clone());
            }
        }
        let mut held = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let h = held.as_mut()?;
        // A refresh may have landed between the two locks.
        if now.saturating_duration_since(h.read) <= SNAPSHOT_MAX_AGE {
            return Some(h.snapshot.clone());
        }
        let stale = h.stale.get_or_insert_with(|| {
            Arc::new(GlobalSnapshot {
                stale: true,
                ..(*h.snapshot).clone()
            })
        });
        Some(stale.clone())
    }

    /// How long ago the held snapshot was read; `None` before the first.
    pub fn age(&self) -> Option<Duration> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|h| h.read.elapsed())
    }

    /// Publishes `snapshot` as read at `read`.
    fn put(&self, snapshot: GlobalSnapshot, read: Instant) {
        *self.inner.write().unwrap_or_else(|e| e.into_inner()) = Some(Held {
            snapshot: Arc::new(snapshot),
            read,
            stale: None,
        });
    }

    /// Reads a new snapshot and publishes it. Whether or not the read
    /// succeeds, the age of the snapshot now held is published in
    /// `farsight_coverage_snapshot_age_seconds`.
    pub async fn refresh(
        &self,
        pool: &PgPool,
        limits: &Limits,
    ) -> Result<(), farsight_storage::StorageError> {
        let read = Instant::now();
        let result = coverage::read_snapshot(pool, limits).await.map(|s| {
            publish_exception_gauges(&s);
            self.put(s, read);
        });
        self.publish_age();
        result
    }

    /// Sets `farsight_coverage_snapshot_age_seconds`.
    pub fn publish_age(&self) {
        if let Some(age) = self.age() {
            metrics::gauge!(m::SNAPSHOT_AGE).set(age.as_secs_f64());
        }
    }
}

fn publish_exception_gauges(s: &GlobalSnapshot) {
    let debt = |r| s.debt_counts.get(&r).copied().unwrap_or(0) as f64;
    for (kind, v) in [
        ("unreachableRepos", debt(DebtReason::Unreachable)),
        ("pendingResyncs", debt(DebtReason::Resync)),
        ("cappedAuthors", debt(DebtReason::Capped)),
        ("refusedAuthors", debt(DebtReason::Refused)),
        ("unavailableLists", s.lists.unavailable as f64),
        ("missingLists", s.lists.missing as f64),
        ("deferredLists", s.lists.deferred as f64),
        ("cappedLists", s.lists.capped as f64),
        ("excludedPendingLists", s.pending.excluded as f64),
    ] {
        metrics::gauge!(m::COVERAGE_EXCEPTIONS, "kind" => kind).set(v);
    }
}

/// How often `farsight_lists{state}` is recounted.
pub const LIST_GAUGE_PERIOD: Duration = Duration::from_secs(60);

async fn publish_list_gauges(pool: &PgPool) {
    let Ok(mut conn) = pool.acquire().await else {
        return;
    };
    match farsight_storage::queries::lists_by_state(&mut conn).await {
        Ok(rows) => {
            for s in farsight_storage::codes::TrackState::ALL {
                let n = rows.iter().find(|(t, _)| t == s).map_or(0, |(_, n)| *n);
                metrics::gauge!(m::LISTS, "state" => s.api_name()).set(n as f64);
            }
        }
        Err(e) => tracing::warn!(error = %e, "list state gauges"),
    }
}

/// Runs the refresher until `stop` flips to true. The first refresh has
/// already happened (the server reads one before serving).
pub async fn run_refresher(
    holder: Arc<SnapshotHolder>,
    pool: PgPool,
    config: Arc<ConfigStore>,
    mut stop: watch::Receiver<bool>,
) {
    let limits = || Limits::from_config(&config.current().config);
    let mut last_lists = Instant::now() - LIST_GAUGE_PERIOD;
    loop {
        if *stop.borrow() {
            return;
        }
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(mut l) => match l.listen(COVERAGE_CHANNEL).await {
                Ok(()) => l,
                Err(e) => {
                    tracing::warn!(error = %e, "LISTEN farsight_coverage failed");
                    holder.publish_age();
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, "coverage listener connect failed");
                holder.publish_age();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = stop.changed() => {}
                }
                continue;
            }
        };
        // Every (re)connect: a full refresh.
        if let Err(e) = holder.refresh(&pool, &limits()).await {
            tracing::warn!(error = %e, "snapshot refresh failed");
        }
        let mut tick = tokio::time::interval(SNAPSHOT_REFRESH);
        tick.tick().await;
        loop {
            tokio::select! {
                n = listener.try_recv() => match n {
                    Ok(Some(_)) => {
                        // Coalesce a burst of notifications into one read.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        while listener.next_buffered().is_some() {}
                        if let Err(e) = holder.refresh(&pool, &limits()).await {
                            tracing::warn!(error = %e, "snapshot refresh failed");
                        }
                    }
                    // Connection lost: reconnect (and refresh).
                    Ok(None) | Err(_) => break,
                },
                _ = tick.tick() => {
                    if let Err(e) = holder.refresh(&pool, &limits()).await {
                        tracing::warn!(error = %e, "snapshot refresh failed");
                    }
                    if last_lists.elapsed() >= LIST_GAUGE_PERIOD {
                        last_lists = Instant::now();
                        publish_list_gauges(&pool).await;
                    }
                }
                _ = stop.changed() => {
                    if *stop.borrow() {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use farsight_storage::coverage::{ListCounts, PendingEffects};
    use farsight_storage::firehose::FirehoseState;

    fn snapshot() -> GlobalSnapshot {
        let now = chrono::Utc::now();
        GlobalSnapshot {
            read_at: now,
            stale: false,
            firehose: FirehoseState {
                applied_through: Some(now),
                connected: true,
                ..FirehoseState::default()
            },
            baseline: None,
            gaps: Vec::new(),
            storage_refusal_active: false,
            debt_counts: Default::default(),
            lists: ListCounts::default(),
            pending: PendingEffects::default(),
        }
    }

    #[test]
    fn a_snapshot_that_is_not_read_again_goes_stale() {
        let holder = SnapshotHolder::default();
        assert!(holder.get().is_none() && holder.age().is_none());
        let read = Instant::now();
        holder.put(snapshot(), read);
        let lag = Duration::from_secs(300);
        // Within its age it is served as read.
        for age in [Duration::ZERO, SNAPSHOT_REFRESH, SNAPSHOT_MAX_AGE] {
            let s = holder.get_at(read + age).expect("held");
            assert!(!s.stale && !s.synthetic_gap(lag), "{age:?}");
        }
        // Past it the same snapshot is the synthetic gap, with its reason.
        let late = read + SNAPSHOT_MAX_AGE + Duration::from_millis(1);
        let s = holder.get_at(late).expect("held");
        assert!(s.stale && s.synthetic_gap(lag));
        assert_eq!(s.synthetic_gap_reason(lag), Some("coverage_stale"));
        assert!(!s.covered(Some(s.read_at), lag));
        // The marked copy is made once.
        assert!(Arc::ptr_eq(&s, &holder.get_at(late).expect("held")));
        // The next successful read ends it.
        holder.put(snapshot(), late);
        let s = holder.get_at(late + SNAPSHOT_REFRESH).expect("held");
        assert!(!s.stale && !s.synthetic_gap(lag));
    }
}
