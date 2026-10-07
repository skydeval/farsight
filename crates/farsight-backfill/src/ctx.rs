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

tokio::task_local! {
    /// The number of the job running in this task (set by the scheduler).
    pub static JOB: u64;
}

/// What separates the process name from the job number in a lease
/// owner's name.
pub const LEASE_JOB_SEP: char = '#';

/// Shared state of the backfill process.
pub struct Ctx {
    /// Backfill's own pool (separate from the server's).
    pub pool: PgPool,
    config: RwLock<Arc<Config>>,
    /// Every outbound request goes through it: the client, the per-host
    /// limits and the PLC directory's limiter.
    pub net: Arc<Net>,
    /// DID → PDS resolution with its caches.
    pub resolver: Resolver,
    /// Approximate counters of backfill's writes (shard 2).
    pub counters: Arc<CounterSink>,
    /// Gates from backfill's own budget monitor.
    pub gates: SharedGates,
    /// The last gate state (sweep pause at ≥ 90%).
    pub gate_state: Mutex<GateState>,
    /// The name this process holds its work under:
    /// `backfill-<pid>-<12 random hex digits>`, new at every start, so
    /// what an earlier process left behind is never mistaken for held and
    /// simply expires. It is `backfill_queue.claimed_by` of the entries
    /// whose jobs run here, and the stem of [`Ctx::lease_owner`].
    pub process: String,
    /// Binary version (User-Agent).
    pub version: &'static str,
}

impl Ctx {
    /// A context with open gates and a fresh process name. `counters` is
    /// the sink of backfill's writes ([`Ctx::counter_sink`]), which the
    /// resolver shares.
    pub fn new(
        pool: PgPool,
        config: Arc<Config>,
        net: Arc<Net>,
        resolver: Resolver,
        counters: Arc<CounterSink>,
        version: &'static str,
    ) -> Ctx {
        let mut b = [0u8; 6];
        let _ = getrandom::getrandom(&mut b);
        Ctx {
            pool,
            config: RwLock::new(config),
            net,
            resolver,
            counters,
            gates: SharedGates::default(),
            gate_state: Mutex::new(GateState::default()),
            process: format!(
                "backfill-{}-{}",
                std::process::id(),
                b.iter().map(|x| format!("{x:02x}")).collect::<String>()
            ),
            version,
        }
    }

    /// The counter sink of the backfill process: shard 2.
    pub fn counter_sink() -> Arc<CounterSink> {
        Arc::new(CounterSink::new(2))
    }

    /// `job_leases.lease_owner` of the job running in this task: the
    /// process name and, under the scheduler, the job's number. Each job
    /// holds its leases under its own name, so two jobs of this process
    /// that want the same DID exclude each other and neither releases
    /// the other's lease.
    pub fn lease_owner(&self) -> String {
        match JOB.try_with(|n| *n) {
            Ok(n) => format!("{}{LEASE_JOB_SEP}{n}", self.process),
            Err(_) => self.process.clone(),
        }
    }

    /// The config in force, as a shared snapshot. A reload swaps in a new
    /// `Arc`; a job keeps the one it took for as long as it holds it.
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

    /// Whether the sweep must pause for storage (≥ 90% of budget).
    pub fn sweep_paused_by_storage(&self) -> bool {
        self.gate_state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sweep_paused
    }
}
