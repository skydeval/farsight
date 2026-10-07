//! Postgres storage for Farsight: migrations, the shared apply path
//! (locks, last-write-wins, list tracking), queries and coverage computation.
//!
//! Every writer in both binaries goes through [`apply::apply`] (see
//! `docs/design/README.md`): firehose and backfill admission rules
//! cannot drift.

#![warn(missing_docs)]

pub mod apply;
pub mod auth;
pub mod backfill_api;
pub mod codes;
pub mod counters;
pub mod coverage;
pub mod debts;
pub mod error;
pub mod firehose;
pub mod gates;
pub mod handles;
pub mod history;
pub mod ids;
pub mod janitor;
pub mod keys;
pub mod public;
pub mod queries;
pub mod queue;
pub mod recount;
pub mod repo_events;
pub mod top;
pub mod tracking;
pub mod transition;
pub mod txn;
pub mod ui_rows;

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub use error::{Result, StorageError};

/// The embedded migration set: `0001_initial.sql` creates the whole
/// schema.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// The schema version this build expects: the number of the last
/// migration, which each migration writes into `schema_version`.
/// `farsight-backfill` polls for it before starting work.
pub const SCHEMA_VERSION: i32 = 1;

/// How often `farsight-backfill` polls `schema_version`.
pub const SCHEMA_POLL: Duration = Duration::from_secs(5);

/// Connects a pool of at most `max_connections` connections to
/// `database_url`.
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await?)
}

/// Runs migrations (server only) and checks the result.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    MIGRATOR.run(pool).await?;
    match schema_version(pool).await? {
        Some(v) if v == SCHEMA_VERSION => Ok(()),
        found => Err(StorageError::SchemaVersion {
            found,
            expected: SCHEMA_VERSION,
        }),
    }
}

/// The stored schema version; `None` if the table does not exist yet.
pub async fn schema_version(pool: &PgPool) -> Result<Option<i32>> {
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.schema_version') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    if !exists {
        return Ok(None);
    }
    Ok(
        sqlx::query_scalar("SELECT max(version) FROM schema_version")
            .fetch_one(pool)
            .await?,
    )
}

/// Blocks until `schema_version` equals [`SCHEMA_VERSION`], polling every
/// [`SCHEMA_POLL`] (`farsight-backfill` startup).
pub async fn wait_for_schema(pool: &PgPool) -> Result<()> {
    loop {
        if schema_version(pool).await? == Some(SCHEMA_VERSION) {
            return Ok(());
        }
        tokio::time::sleep(SCHEMA_POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_matches_migration_count() {
        let versions: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();
        assert_eq!(versions.len(), SCHEMA_VERSION as usize);
        assert_eq!(versions.last().copied(), Some(i64::from(SCHEMA_VERSION)));
    }

    #[test]
    fn migrations_bump_schema_version() {
        for m in MIGRATOR.iter() {
            let expected = format!("UPDATE schema_version SET version = {};", m.version);
            assert!(
                m.sql.contains(&expected),
                "migration {} does not set its version",
                m.version
            );
        }
    }

    #[test]
    fn no_code_deletes_actors() {
        // Actors rows are never deleted. The migration adds a trigger
        // that refuses it at runtime; this checks no statement in the
        // crate tries.
        for (name, src) in [
            ("apply.rs", include_str!("apply.rs")),
            ("janitor.rs", include_str!("janitor.rs")),
            ("recount.rs", include_str!("recount.rs")),
            ("txn.rs", include_str!("txn.rs")),
            ("tracking.rs", include_str!("tracking.rs")),
            ("debts.rs", include_str!("debts.rs")),
            ("firehose.rs", include_str!("firehose.rs")),
            ("counters.rs", include_str!("counters.rs")),
            ("coverage.rs", include_str!("coverage.rs")),
            ("queries.rs", include_str!("queries.rs")),
            ("public.rs", include_str!("public.rs")),
            ("ui_rows.rs", include_str!("ui_rows.rs")),
            ("history.rs", include_str!("history.rs")),
            ("auth.rs", include_str!("auth.rs")),
            ("backfill_api.rs", include_str!("backfill_api.rs")),
            ("handles.rs", include_str!("handles.rs")),
        ] {
            let lower = src.to_ascii_lowercase();
            assert!(
                !lower.contains("delete from actors"),
                "{name} deletes actors rows"
            );
            assert!(
                !lower.contains("truncate actors"),
                "{name} truncates actors"
            );
        }
    }
}
