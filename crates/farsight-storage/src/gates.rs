//! Storage budget and hard ceiling (design §11.2): measuring, the gate
//! state machine with its hysteresis, and `storage_refusals` intervals.
//!
//! The budget monitor (a server task, every minute) calls
//! [`measure_database_bytes`], feeds the result to [`next_gate_state`],
//! hands the resulting [`crate::txn::Gates`] to every apply, and records
//! global refusal intervals with [`record_refusal_transition`].

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::error::Result;
use crate::txn::Gates;

/// Gate thresholds from `[storage]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// `storage.budget_bytes`.
    pub budget_bytes: u64,
    /// `storage.hard_ceiling_bytes` after defaulting.
    pub ceiling_bytes: u64,
}

/// Monitor state across measurements (hysteresis needs the previous one).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GateState {
    /// Gates handed to writers.
    pub gates: Gates,
    /// ≥ 90% of budget: the sweep pauses.
    pub sweep_paused: bool,
    /// ≥ 110% of budget: dashboard critical.
    pub critical: bool,
}

fn pct(bytes: u64, budget: u64, p: u64) -> bool {
    u128::from(bytes) * 100 >= u128::from(budget) * u128::from(p)
}

/// The next gate state for a measured size (§11.2):
/// - budget refusal starts at ≥ 100% and ends below 95% (hysteresis);
/// - the ceiling refusal starts at ≥ the hard ceiling and ends below
///   105% of the budget;
/// - the sweep pauses at ≥ 90%; critical at ≥ 110%.
pub fn next_gate_state(prev: GateState, bytes: u64, b: Budget) -> GateState {
    let budget_refusing = if prev.gates.budget_refusing {
        pct(bytes, b.budget_bytes, 95)
    } else {
        pct(bytes, b.budget_bytes, 100)
    };
    let ceiling_refusing = if prev.gates.ceiling_refusing {
        pct(bytes, b.budget_bytes, 105)
    } else {
        bytes >= b.ceiling_bytes
    };
    GateState {
        gates: Gates {
            budget_refusing,
            ceiling_refusing,
        },
        sweep_paused: pct(bytes, b.budget_bytes, 90),
        critical: pct(bytes, b.budget_bytes, 110),
    }
}

/// `pg_database_size` of the current database.
pub async fn measure_database_bytes(pool: &PgPool) -> Result<u64> {
    let n: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await?;
    Ok(u64::try_from(n).unwrap_or(0))
}

/// Opens or closes the global `storage_refusals` interval when the
/// "any global refusal active" condition changes (§11.2: network scope is
/// `partial` + `storage_refusal` while one is open). `witness` is the
/// current applied-through witness time.
pub async fn record_refusal_transition(
    pool: &PgPool,
    prev: GateState,
    next: GateState,
    witness: DateTime<Utc>,
) -> Result<()> {
    let was = prev.gates.budget_refusing || prev.gates.ceiling_refusing;
    let is = next.gates.budget_refusing || next.gates.ceiling_refusing;
    if was == is {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    if is {
        sqlx::query(
            "INSERT INTO storage_refusals (from_witness)
             SELECT $1 WHERE NOT EXISTS (SELECT 1 FROM storage_refusals WHERE to_witness IS NULL)",
        )
        .bind(witness)
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query("UPDATE storage_refusals SET to_witness = $1 WHERE to_witness IS NULL")
            .bind(witness)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("SELECT pg_notify('farsight_coverage', '')")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const B: Budget = Budget {
        budget_bytes: 1000,
        ceiling_bytes: 1150,
    };

    #[test]
    fn budget_hysteresis() {
        let s = next_gate_state(GateState::default(), 899, B);
        assert!(!s.sweep_paused && !s.gates.budget_refusing);
        let s = next_gate_state(s, 900, B);
        assert!(s.sweep_paused && !s.gates.budget_refusing);
        let s = next_gate_state(s, 1000, B);
        assert!(s.gates.budget_refusing && !s.gates.ceiling_refusing);
        // Still refusing between 95% and 100%.
        let s = next_gate_state(s, 960, B);
        assert!(s.gates.budget_refusing);
        let s = next_gate_state(s, 949, B);
        assert!(!s.gates.budget_refusing);
    }

    #[test]
    fn ceiling_hysteresis() {
        let s = next_gate_state(GateState::default(), 1150, B);
        assert!(s.gates.ceiling_refusing && s.gates.budget_refusing && s.critical);
        // Below the ceiling but ≥ 105% of budget: still refusing.
        let s = next_gate_state(s, 1060, B);
        assert!(s.gates.ceiling_refusing);
        let s = next_gate_state(s, 1049, B);
        assert!(!s.gates.ceiling_refusing && s.gates.budget_refusing);
    }
}
