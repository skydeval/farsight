//! The UI sort indexes (design §7.6): four indexes that order the UI's
//! row tables by shown time, built here, after the server has started
//! serving, and never by a migration. A build inside the migration
//! transaction would outlast a health check on a large table and be
//! rolled back by the restart, on every start.
//!
//! One task per server process:
//!
//! 1. takes a session advisory lock on a connection of its own, so two
//!    server processes never build at once;
//! 2. for each index, small tables first: valid → done; present but
//!    invalid (an interrupted build) → dropped, then as absent; absent →
//!    its size is estimated, and if the database plus the estimate would
//!    exceed `storage.budget_bytes` nothing is built and the task checks
//!    again later; otherwise `CREATE INDEX CONCURRENTLY`, which does not
//!    block writers;
//! 3. a failed build is retried later and recorded in `op_errors` once per
//!    run of failures.
//!
//! Each section switches to its new order when its flag is set
//! ([`SortIndexes`]); until then it keeps the order it had.

use std::sync::Arc;
use std::time::{Duration, Instant};

use farsight_api::config_store::ConfigStore;
use farsight_storage::ui_rows::{self, IndexState, Section, SortIndexes};
use farsight_web::ServerStatus;
use sqlx::{Connection, PgConnection, PgPool};
use tokio::sync::watch;

/// `farsight_ui_sort_indexes_ready`: how many of the four are valid.
pub const READY: &str = "farsight_ui_sort_indexes_ready";

/// How long the task waits before it looks again while the storage budget
/// has no room.
pub const HOLD_RETRY: Duration = Duration::from_secs(15 * 60);
/// How long it waits after a failed build, or while another process holds
/// the build lock.
pub const FAIL_RETRY: Duration = Duration::from_secs(10 * 60);

/// Harness-only: both waits in seconds, so that a hold and its release fit
/// in a test run.
const RETRY_ENV: &str = "FARSIGHT_HARNESS_SORT_RETRY_SECS";

fn retry(default: Duration) -> Duration {
    std::env::var(RETRY_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|s| *s > 0)
        .map_or(default, Duration::from_secs)
}

/// What the builder needs.
pub struct Builder {
    /// A pool for the short checks.
    pub pool: PgPool,
    /// The database, for the build connection.
    pub database_url: String,
    /// Live config (`storage.budget_bytes`).
    pub config: Arc<ConfigStore>,
    /// The flags the pages read.
    pub sort: Arc<SortIndexes>,
    /// Dashboard status.
    pub status: Arc<ServerStatus>,
}

/// Why a pass stopped before every index was valid.
enum Stop {
    /// The storage budget has no room for an index estimated at this many
    /// bytes.
    Held(u64),
    /// Another server process holds the build lock.
    Locked,
    /// A statement failed.
    Failed(String),
}

fn publish(sort: &SortIndexes) {
    metrics::gauge!(READY).set(sort.count() as f64);
}

/// Sets the flags from the database. Called once before the server starts
/// serving, so that a section whose index exists sorts by it from the
/// first request.
pub async fn load(pool: &PgPool, sort: &SortIndexes) -> Result<(), String> {
    let mut conn = pool.acquire().await.map_err(|e| e.to_string())?;
    ui_rows::load_states(&mut conn, sort)
        .await
        .map_err(|e| e.to_string())?;
    publish(sort);
    Ok(())
}

impl Builder {
    async fn pass(&self) -> Result<(), Stop> {
        let fail = |e: &dyn std::fmt::Display| Stop::Failed(e.to_string());
        // A connection of its own: the lock is the session's, and a build
        // may run for hours.
        let mut conn = PgConnection::connect(&self.database_url)
            .await
            .map_err(|e| fail(&e))?;
        sqlx::query("SET statement_timeout = 0")
            .execute(&mut conn)
            .await
            .map_err(|e| fail(&e))?;
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(ui_rows::BUILD_LOCK)
            .fetch_one(&mut conn)
            .await
            .map_err(|e| fail(&e))?;
        if !locked {
            // The other process's builds still switch this one's sections.
            let _ = ui_rows::load_states(&mut conn, &self.sort).await;
            publish(&self.sort);
            let _ = conn.close().await;
            return Err(Stop::Locked);
        }
        let result = self.build_all(&mut conn).await;
        // Closing the session releases the lock.
        let _ = conn.close().await;
        result
    }

    async fn build_all(&self, conn: &mut PgConnection) -> Result<(), Stop> {
        let fail = |e: farsight_storage::StorageError| Stop::Failed(e.to_string());
        for section in Section::ALL {
            let index = section.index();
            match ui_rows::index_state(conn, section).await.map_err(fail)? {
                IndexState::Valid => {
                    self.sort.set(section, true);
                    publish(&self.sort);
                    continue;
                }
                IndexState::Invalid => {
                    tracing::info!(index, "sort index: dropping an interrupted build");
                    ui_rows::drop_index(conn, section).await.map_err(fail)?;
                }
                IndexState::Absent => {}
            }
            let estimate = ui_rows::estimate_bytes(conn, section).await.map_err(fail)?;
            let database = ui_rows::database_bytes(conn).await.map_err(fail)?;
            let budget = self.config.current().config.storage.budget_bytes;
            if database.saturating_add(estimate) > budget {
                tracing::warn!(
                    index,
                    estimate_bytes = estimate,
                    database_bytes = database,
                    budget_bytes = budget,
                    "sort index: not built, the storage budget has no room; the table keeps its \
                     previous order"
                );
                return Err(Stop::Held(estimate));
            }
            self.status.update(|s| s.sort_held_bytes = None);
            tracing::info!(
                index,
                table = section.table(),
                estimate_bytes = estimate,
                "sort index: building in the background"
            );
            let started = Instant::now();
            ui_rows::create_index(conn, section).await.map_err(fail)?;
            // A build that lost a race with a conflicting row leaves the
            // index invalid without an error.
            if ui_rows::index_state(conn, section).await.map_err(fail)? != IndexState::Valid {
                return Err(Stop::Failed(format!(
                    "{index} is not valid after its build"
                )));
            }
            let bytes = ui_rows::index_bytes(conn, section).await.map_err(fail)?;
            self.sort.set(section, true);
            publish(&self.sort);
            tracing::info!(
                index,
                took_ms = started.elapsed().as_millis() as u64,
                index_bytes = bytes,
                ready = self.sort.count(),
                "sort index: built; its table now sorts by creation time"
            );
        }
        Ok(())
    }

    /// Runs until every index is valid or `stop` flips.
    pub async fn run(self, mut stop: watch::Receiver<bool>) {
        // One `op_errors` row per run of failures, not per attempt.
        let mut failing = false;
        loop {
            let wait = match self.pass().await {
                Ok(()) => {
                    self.status.update(|s| s.sort_held_bytes = None);
                    tracing::info!("sort indexes: all four ready");
                    return;
                }
                Err(Stop::Held(estimate)) => {
                    self.status.update(|s| s.sort_held_bytes = Some(estimate));
                    failing = false;
                    retry(HOLD_RETRY)
                }
                Err(Stop::Locked) => {
                    tracing::info!("sort indexes: another server process is building them");
                    retry(FAIL_RETRY)
                }
                Err(Stop::Failed(e)) => {
                    tracing::warn!(error = %e, "sort index: build failed; retrying later");
                    if !failing {
                        failing = true;
                        let _ = farsight_storage::auth::record_op_error(
                            &self.pool,
                            "task:sort_indexes",
                            None,
                            &e,
                        )
                        .await;
                    }
                    retry(FAIL_RETRY)
                }
            };
            if self.sort.count() == Section::ALL.len() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                r = stop.changed() => {
                    if r.is_err() || *stop.borrow() { return; }
                }
            }
        }
    }
}
