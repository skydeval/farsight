//! What every backfill task shares: the pool, the live config, the network
//! layer, the resolver, the counter sink and the write gates.

use std::sync::{Arc, Mutex, RwLock};

use farsight_core::Config;
use farsight_storage::counters::CounterSink;
use farsight_storage::gates::{GateState, SharedGates};
use farsight_storage::keys::Limits;
use sqlx::PgPool;

use crate::net::Net;
use crate::resolve::Resolver;

/// Shared state of the backfill process.
pub struct Ctx {
    /// Backfill's own pool (separate from the server's).
    pub pool: PgPool,
    config: RwLock<Arc<Config>>,
    /// Outbound requests.
    pub net: Arc<Net>,
    /// DID resolution.
    pub resolver: Resolver,
    /// Approximate counters of backfill's writes (shard 2).
    pub counters: Arc<CounterSink>,
    /// Gates from backfill's own budget monitor (§11.2).
    pub gates: SharedGates,
    /// The last gate state (sweep pause at ≥ 90%).
    pub gate_state: Mutex<GateState>,
    /// This process's lease owner name.
    pub lease_owner: String,
    /// Binary version (User-Agent).
    pub version: &'static str,
}

impl Ctx {
    /// A context.
    pub fn new(
        pool: PgPool,
        config: Arc<Config>,
        net: Arc<Net>,
        resolver: Resolver,
        version: &'static str,
    ) -> Ctx {
        let mut b = [0u8; 6];
        let _ = getrandom::getrandom(&mut b);
        Ctx {
            pool,
            config: RwLock::new(config),
            net,
            resolver,
            counters: Arc::new(CounterSink::new(2)),
            gates: SharedGates::default(),
            gate_state: Mutex::new(GateState::default()),
            lease_owner: format!(
                "backfill-{}-{}",
                std::process::id(),
                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
            ),
            version,
        }
    }

    /// The config in force.
    pub fn cfg(&self) -> Arc<Config> {
        self.config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Replaces the config (reload on `NOTIFY farsight_config` / mtime).
    pub fn set_cfg(&self, c: Arc<Config>) {
        *self.config.write().unwrap_or_else(|e| e.into_inner()) = c;
    }

    /// Limits from the config in force.
    pub fn limits(&self) -> Limits {
        Limits::from_config(&self.cfg())
    }

    /// Whether the sweep must pause for storage (≥ 90% of budget, §11.2).
    pub fn sweep_paused_by_storage(&self) -> bool {
        self.gate_state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sweep_paused
    }
}
