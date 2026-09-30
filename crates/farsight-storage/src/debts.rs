//! `relist_debt` (design §3.7.3): the single source of actor-level
//! coverage exceptions.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::{PgExecutor, PgPool};

use crate::codes::{CapType, DebtReason};
use crate::error::Result;

/// One debt row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Debt {
    /// Reason.
    pub reason: DebtReason,
    /// Cap or rate, for `capped` / `refused`.
    pub cap_type: Option<CapType>,
    /// Only a clean run with coverage point ≥ this clears the debt.
    pub since_witness: DateTime<Utc>,
}

/// Inserts a debt or raises its `since_witness` (outside an apply
/// transaction; e.g. `#sync`, poisoned events, account events).
pub async fn add_debt(
    pool: &PgPool,
    actor_id: i64,
    reason: DebtReason,
    cap_type: Option<CapType>,
    since_witness: DateTime<Utc>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO relist_debt (actor_id, reason, cap_type, since_witness)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (actor_id, reason) DO UPDATE
           SET since_witness = GREATEST(relist_debt.since_witness, EXCLUDED.since_witness),
               cap_type = COALESCE(EXCLUDED.cap_type, relist_debt.cap_type)",
    )
    .bind(actor_id)
    .bind(reason.code())
    .bind(cap_type.map(CapType::code))
    .bind(since_witness)
    .execute(&mut *tx)
    .await?;
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// A clean run (§5.2.1) with coverage point `point` deletes every debt of
/// the actor whose `since_witness ≤ point`. Returns rows deleted.
pub async fn clear_for_clean_run(
    pool: &PgPool,
    actor_id: i64,
    point: DateTime<Utc>,
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let n = sqlx::query("DELETE FROM relist_debt WHERE actor_id = $1 AND since_witness <= $2")
        .bind(actor_id)
        .bind(point)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if n > 0 {
        sqlx::query("SELECT pg_notify('farsight_coverage', '')")
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(n)
}

/// A `resync` debt that became terminal is replaced by `unreachable` (an
/// actor never counts twice for one cause, §3.7.3).
pub async fn replace_resync_with_unreachable(
    pool: &PgPool,
    actor_id: i64,
    since_witness: DateTime<Utc>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM relist_debt WHERE actor_id = $1 AND reason = $2")
        .bind(actor_id)
        .bind(DebtReason::Resync.code())
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO relist_debt (actor_id, reason, since_witness) VALUES ($1, $2, $3)
         ON CONFLICT (actor_id, reason) DO UPDATE
           SET since_witness = GREATEST(relist_debt.since_witness, EXCLUDED.since_witness)",
    )
    .bind(actor_id)
    .bind(DebtReason::Unreachable.code())
    .bind(since_witness)
    .execute(&mut *tx)
    .await?;
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Debts of the given actors (per-actor and per-reason reads used by
/// `checkBlocks` per-result coverage, §3.7.5 item 5). Actors without
/// debts are absent from the map.
pub async fn debts_for<'e>(
    ex: impl PgExecutor<'e>,
    actor_ids: &[i64],
) -> Result<HashMap<i64, Vec<Debt>>> {
    let rows: Vec<(i64, i16, Option<i16>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT actor_id, reason, cap_type, since_witness FROM relist_debt
         WHERE actor_id = ANY($1) ORDER BY actor_id, reason",
    )
    .bind(actor_ids)
    .fetch_all(ex)
    .await?;
    let mut out: HashMap<i64, Vec<Debt>> = HashMap::new();
    for (actor, reason, cap, since) in rows {
        let Some(reason) = DebtReason::from_code(reason) else {
            continue;
        };
        out.entry(actor).or_default().push(Debt {
            reason,
            cap_type: cap.and_then(CapType::from_code),
            since_witness: since,
        });
    }
    Ok(out)
}

/// Distinct actors per reason (the actor-level `exceptions` counts).
pub async fn counts_by_reason<'e>(ex: impl PgExecutor<'e>) -> Result<HashMap<DebtReason, i64>> {
    let rows: Vec<(i16, i64)> =
        sqlx::query_as("SELECT reason, count(DISTINCT actor_id) FROM relist_debt GROUP BY reason")
            .fetch_all(ex)
            .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(r, n)| DebtReason::from_code(r).map(|r| (r, n)))
        .collect())
}
