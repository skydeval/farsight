//! The in-memory global coverage snapshot (design §3.7.1): refreshed on
//! `NOTIFY farsight_coverage`, every 10 s, and on every LISTEN reconnect,
//! each time with one `REPEATABLE READ` transaction, so a lost
//! notification can only delay, never falsify.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use farsight_storage::codes::DebtReason;
use farsight_storage::coverage::{self, COVERAGE_CHANNEL, GlobalSnapshot, SNAPSHOT_REFRESH};
use farsight_storage::keys::Limits;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::watch;

use crate::config_store::ConfigStore;
use crate::metrics as m;

/// Holds the latest snapshot.
#[derive(Debug, Default)]
pub struct SnapshotHolder {
    inner: RwLock<Option<Arc<GlobalSnapshot>>>,
}

impl SnapshotHolder {
    /// The latest snapshot (`None` until the first read succeeds).
    pub fn get(&self) -> Option<Arc<GlobalSnapshot>> {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Reads a new snapshot and publishes it.
    pub async fn refresh(
        &self,
        pool: &PgPool,
        limits: &Limits,
    ) -> Result<(), farsight_storage::StorageError> {
        let s = coverage::read_snapshot(pool, limits).await?;
        publish_exception_gauges(&s);
        *self.inner.write().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(s));
        Ok(())
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
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, "coverage listener connect failed");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = stop.changed() => {}
                }
                continue;
            }
        };
        // Every (re)connect: a full refresh (§3.7.1).
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
