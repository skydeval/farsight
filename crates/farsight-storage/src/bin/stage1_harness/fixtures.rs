//! Harness plumbing: Postgres container, per-stream databases, record
//! builders, apply helpers, row readers and the check recorder.

use std::process::Command;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use farsight_core::aturi::AtUri;
use farsight_core::record::{
    BlockRecord, ListBlockRecord, ListItemRecord, ListPurpose, ListRecord,
};
use farsight_core::{Collection, Did, Record, RecordKey, Tid};
pub use farsight_storage::ids::{ActorId, CycleId, ListId, Stamp};

/// Stands for a list the harness expected and did not find; no row has it.
pub const NO_LIST: ListId = ListId::new(-1);
/// The same for an actor.
pub const NO_ACTOR: ActorId = ActorId::new(-1);
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Write, WriteAction};
use farsight_storage::codes::TrackState;
use farsight_storage::counters::CounterSink;
use farsight_storage::keys::Limits;
use farsight_storage::txn::{ApplyReport, Gates};
use farsight_storage::{Result, StorageError};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

// ------------------------------------------------------------------ checks

/// Outcome of one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
    /// The check could not establish what it claims (e.g. a race that
    /// never happened). Never counted as a pass.
    Unverified,
}

/// One recorded check.
#[derive(Debug, Clone)]
pub struct Check {
    pub verdict: Verdict,
    pub what: String,
    pub detail: String,
}

/// Collects checks for one stream.
#[derive(Debug, Default)]
pub struct Checks {
    pub items: Vec<Check>,
}

impl Checks {
    pub fn check(&mut self, what: impl Into<String>, ok: bool, detail: impl Into<String>) -> bool {
        self.items.push(Check {
            verdict: if ok { Verdict::Pass } else { Verdict::Fail },
            what: what.into(),
            detail: detail.into(),
        });
        ok
    }

    pub fn eq<T: PartialEq + std::fmt::Debug>(
        &mut self,
        what: impl Into<String>,
        got: T,
        want: T,
    ) -> bool {
        let ok = got == want;
        let detail = if ok {
            format!("{got:?}")
        } else {
            format!("got {got:?}, want {want:?}")
        };
        self.check(what, ok, detail)
    }

    pub fn unverified(&mut self, what: impl Into<String>, detail: impl Into<String>) {
        self.items.push(Check {
            verdict: Verdict::Unverified,
            what: what.into(),
            detail: detail.into(),
        });
    }
}

// --------------------------------------------------------------- postgres

/// A Postgres 16 server for the run.
pub struct Server {
    pub admin_url: String,
    base_url: String,
    container: Option<(String, String)>,
    keep: bool,
}

fn run(cmd: &mut Command) -> std::result::Result<String, String> {
    let out = cmd.output().map_err(|e| format!("{cmd:?}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "{cmd:?} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

impl Server {
    /// Starts `postgres:16` in Docker with an ephemeral named volume.
    pub fn start_docker(image: &str, keep: bool) -> std::result::Result<Server, String> {
        let tag = format!("{}-{}", std::process::id(), Utc::now().timestamp());
        let name = format!("farsight-stage1-{tag}");
        let volume = format!("farsight-stage1-{tag}");
        run(Command::new("docker").args([
            "run",
            "-d",
            "--name",
            &name,
            "--label",
            "farsight.stage1-harness=1",
            "-e",
            "POSTGRES_PASSWORD=harness",
            "-e",
            "POSTGRES_USER=harness",
            "-p",
            "127.0.0.1::5432",
            "-v",
            &format!("{volume}:/var/lib/postgresql/data"),
            image,
        ]))?;
        let port_line = run(Command::new("docker").args(["port", &name, "5432/tcp"]))?;
        let port = port_line
            .lines()
            .next()
            .and_then(|l| l.rsplit(':').next())
            .ok_or_else(|| format!("cannot parse docker port output {port_line:?}"))?
            .to_owned();
        let base_url = format!("postgres://harness:harness@127.0.0.1:{port}");
        Ok(Server {
            admin_url: format!("{base_url}/postgres"),
            base_url,
            container: Some((name, volume)),
            keep,
        })
    }

    /// Uses an existing server (the URL's database is used for admin
    /// statements; the role must be allowed to CREATE DATABASE).
    pub fn existing(url: &str) -> Server {
        let base_url = match url.rfind('/') {
            Some(i) if i > "postgres://".len() => url[..i].to_owned(),
            _ => url.to_owned(),
        };
        Server {
            admin_url: url.to_owned(),
            base_url,
            container: None,
            keep: true,
        }
    }

    /// Waits until the server accepts TCP connections and queries.
    pub async fn wait_ready(&self, timeout: Duration) -> std::result::Result<(), String> {
        let start = Instant::now();
        let mut last = String::new();
        let mut ok_in_a_row = 0;
        while start.elapsed() < timeout {
            match PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect(&self.admin_url)
                .await
            {
                Ok(pool) => match sqlx::query("SELECT 1").execute(&pool).await {
                    Ok(_) => {
                        ok_in_a_row += 1;
                        // The image restarts once after init; require two
                        // successes a second apart.
                        if ok_in_a_row >= 2 {
                            return Ok(());
                        }
                    }
                    Err(e) => last = e.to_string(),
                },
                Err(e) => {
                    ok_in_a_row = 0;
                    last = e.to_string();
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(format!("postgres not ready after {timeout:?}: {last}"))
    }

    /// Creates a fresh database, migrates it and returns a pool.
    pub async fn fresh_db(&self, name: &str) -> Result<PgPool> {
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await?;
        sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
            .execute(&admin)
            .await?;
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin)
            .await?;
        admin.close().await;
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(&format!("{}/{name}", self.base_url))
            .await?;
        farsight_storage::migrate(&pool).await?;
        Ok(pool)
    }

    /// Removes the container and its volume unless `--keep`.
    pub fn stop(&self) {
        let Some((name, volume)) = &self.container else {
            return;
        };
        if self.keep {
            println!(
                "keeping container {name} (volume {volume}); remove with scripts/stage1-harness-teardown.sh"
            );
            return;
        }
        if let Err(e) = run(Command::new("docker").args(["rm", "-f", "-v", name])) {
            eprintln!("warning: {e}");
        }
        if let Err(e) = run(Command::new("docker").args(["volume", "rm", "-f", volume])) {
            eprintln!("warning: {e}");
        }
    }
}

// -------------------------------------------------------------- identities

const B32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// A deterministic `did:plc` from a short tag (letters) and a number.
pub fn plc(tag: &str, n: u64) -> Did {
    let mut s = String::with_capacity(24);
    for c in tag.chars().filter(|c| c.is_ascii_lowercase()).take(10) {
        s.push(c);
    }
    while s.len() < 11 {
        s.push('x');
    }
    let mut v = n;
    let mut tail = [b'a'; 13];
    for slot in tail.iter_mut().rev() {
        *slot = B32[(v % 32) as usize];
        v /= 32;
    }
    s.push_str(std::str::from_utf8(&tail).expect("ascii"));
    Did::parse(&format!("did:plc:{s}")).expect("valid generated did")
}

/// Base of generated revs: 2026-01-01T00:00:00Z in microseconds.
pub const REV_BASE_US: u64 = 1_767_225_600_000_000;

/// A TID-valued rev: `REV_BASE_US + n` microseconds, clock id 0.
pub fn rev(n: u64) -> Stamp {
    Stamp::from_tid(Tid::from_parts(REV_BASE_US + n, 0).expect("in range"))
}

/// `Utc::now()` truncated to microseconds (Postgres precision).
pub fn now_micros() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_micros(Utc::now().timestamp_micros()).expect("in range")
}

pub fn rk(s: &str) -> RecordKey {
    RecordKey::parse(s).expect("valid rkey")
}

pub fn list_uri(owner: &Did, rkey: &str) -> AtUri {
    AtUri::new(owner.clone(), Collection::List, rk(rkey))
}

// ---------------------------------------------------------------- builders

pub fn block(author: &Did, rkey: &str, subject: &Did, stamp: Stamp) -> Write {
    Write {
        author: author.clone(),
        collection: Collection::Block,
        rkey: rk(rkey),
        stamp,
        witness: None,
        action: WriteAction::Upsert(Record::Block(BlockRecord {
            subject: subject.clone(),
            created_at: None,
        })),
    }
}

pub fn listblock(author: &Did, rkey: &str, owner: &Did, list_rkey: &str, stamp: Stamp) -> Write {
    Write {
        author: author.clone(),
        collection: Collection::ListBlock,
        rkey: rk(rkey),
        stamp,
        witness: None,
        action: WriteAction::Upsert(Record::ListBlock(ListBlockRecord {
            subject: list_uri(owner, list_rkey),
            created_at: None,
        })),
    }
}

/// The avatar CID every fixture list names.
pub const LIST_AVATAR: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

pub fn list(owner: &Did, rkey: &str, stamp: Stamp) -> Write {
    Write {
        author: owner.clone(),
        collection: Collection::List,
        rkey: rk(rkey),
        stamp,
        witness: None,
        action: WriteAction::Upsert(Record::List(ListRecord {
            purpose: ListPurpose::Mod,
            name: Some(format!("list {rkey}")),
            description: Some(format!("about {rkey}")),
            avatar: Some(LIST_AVATAR.to_owned()),
            created_at: None,
        })),
    }
}

pub fn item(owner: &Did, rkey: &str, list_rkey: &str, subject: &Did, stamp: Stamp) -> Write {
    Write {
        author: owner.clone(),
        collection: Collection::ListItem,
        rkey: rk(rkey),
        stamp,
        witness: None,
        action: WriteAction::Upsert(Record::ListItem(ListItemRecord {
            subject: subject.clone(),
            list: list_uri(owner, list_rkey),
            created_at: None,
        })),
    }
}

pub fn delete(author: &Did, collection: Collection, rkey: &str, stamp: Stamp) -> Write {
    Write {
        author: author.clone(),
        collection,
        rkey: rk(rkey),
        stamp,
        witness: None,
        action: WriteAction::Delete,
    }
}

// ------------------------------------------------------------------ apply

/// Limits, gates and counters for one stream.
pub struct Env {
    pub pool: PgPool,
    pub limits: Limits,
    pub gates: Gates,
    pub counters: CounterSink,
}

impl Env {
    pub fn new(pool: PgPool, limits: Limits) -> Env {
        Env {
            pool,
            limits,
            gates: Gates::default(),
            counters: CounterSink::new(0),
        }
    }

    pub fn ctx(&self) -> ApplyCtx<'_> {
        ApplyCtx {
            limits: &self.limits,
            gates: self.gates,
            counters: &self.counters,
        }
    }

    /// Applies firehose writes (witness = now for each).
    pub async fn firehose(&self, writes: Vec<Write>) -> Result<ApplyReport> {
        let now = Utc::now();
        let mut b = Batch::new(Origin::Firehose);
        b.writes = writes
            .into_iter()
            .map(|mut w| {
                w.witness.get_or_insert(now);
                w
            })
            .collect();
        apply::apply(&self.pool, &self.ctx(), &b).await
    }

    /// Applies a listing page: every write stamped `stamp`, read now.
    pub async fn listing(&self, writes: Vec<Write>, stamp: Stamp) -> Result<ApplyReport> {
        self.listing_at(writes, stamp, Utc::now()).await
    }

    pub async fn listing_at(
        &self,
        writes: Vec<Write>,
        stamp: Stamp,
        read_at: DateTime<Utc>,
    ) -> Result<ApplyReport> {
        let mut b = Batch::new(Origin::Listing {
            stamp_read_at: read_at,
            deletes_only: false,
            host_confirmed: true,
        });
        b.writes = writes
            .into_iter()
            .map(|mut w| {
                w.stamp = stamp;
                w.witness = None;
                w
            })
            .collect();
        apply::apply(&self.pool, &self.ctx(), &b).await
    }

    // ---- readers ----

    pub async fn actor_id(&self, did: &Did) -> Result<Option<ActorId>> {
        Ok(sqlx::query_scalar("SELECT id FROM actors WHERE did = $1")
            .bind(did.as_str())
            .fetch_optional(&self.pool)
            .await?)
    }

    /// (subject DID, rev) of a stored block.
    pub async fn block_row(&self, author: &Did, rkey: &str) -> Result<Option<(String, Stamp)>> {
        Ok(sqlx::query_as(
            "SELECT s.did, b.rev FROM blocks b JOIN actors a ON a.id = b.author_id
             JOIN actors s ON s.id = b.subject_id WHERE a.did = $1 AND b.rkey = $2",
        )
        .bind(author.as_str())
        .bind(rkey)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// (list owner DID, list rkey, counted, rev) of a stored listblock.
    pub async fn listblock_row(
        &self,
        author: &Did,
        rkey: &str,
    ) -> Result<Option<(String, String, bool, Stamp)>> {
        Ok(sqlx::query_as(
            "SELECT o.did, l.rkey, b.counted, b.rev FROM list_blocks b
             JOIN actors a ON a.id = b.author_id JOIN lists l ON l.id = b.list_id
             JOIN actors o ON o.id = l.owner_id WHERE a.did = $1 AND b.rkey = $2",
        )
        .bind(author.as_str())
        .bind(rkey)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// (list rkey, subject DID, rev) of a stored listitem.
    pub async fn item_row(
        &self,
        owner: &Did,
        rkey: &str,
    ) -> Result<Option<(String, String, Stamp)>> {
        Ok(sqlx::query_as(
            "SELECT l.rkey, s.did, i.rev FROM list_items i JOIN actors a ON a.id = i.owner_id
             JOIN lists l ON l.id = i.list_id JOIN actors s ON s.id = i.subject_id
             WHERE a.did = $1 AND i.rkey = $2",
        )
        .bind(owner.as_str())
        .bind(rkey)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn tombstone(
        &self,
        c: Collection,
        author: &Did,
        rkey: &str,
    ) -> Result<Option<Stamp>> {
        Ok(sqlx::query_scalar(
            "SELECT t.rev FROM tombstones t JOIN actors a ON a.id = t.author_id
             WHERE t.collection = $1 AND a.did = $2 AND t.rkey = $3",
        )
        .bind(c.code())
        .bind(author.as_str())
        .bind(rkey)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn list_id(&self, owner: &Did, rkey: &str) -> Result<Option<ListId>> {
        Ok(sqlx::query_scalar(
            "SELECT l.id FROM lists l JOIN actors a ON a.id = l.owner_id
             WHERE a.did = $1 AND l.rkey = $2",
        )
        .bind(owner.as_str())
        .bind(rkey)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn list_view(&self, list_id: ListId) -> Result<ListView> {
        let r: ListRow = sqlx::query_as(
            "SELECT track_state, purge_then, listblock_count, item_count, admit_epoch,
                    admitted_at IS NOT NULL, retain_until IS NOT NULL, deferred_by,
                    fetched_witness, capped, record_state,
                    EXISTS (SELECT 1 FROM list_jobs j WHERE j.list_id = lists.id
                                                        AND j.admit_epoch = lists.admit_epoch),
                    (SELECT count(*) FROM list_sched_keys k WHERE k.list_id = lists.id),
                    next_retry_at IS NOT NULL
             FROM lists WHERE id = $1",
        )
        .bind(list_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(ListView {
            state: TrackState::from_code(r.0)
                .ok_or_else(|| StorageError::Invariant(format!("state {}", r.0)))?,
            purge_then: r.1.and_then(TrackState::from_code),
            listblock_count: r.2,
            item_count: r.3,
            admit_epoch: r.4,
            admitted: r.5,
            retaining: r.6,
            deferred_by: r.7,
            fetched_witness: r.8,
            capped: r.9,
            record_state: r.10,
            job_current: r.11,
            sched_keys: r.12,
            retry_scheduled: r.13,
        })
    }

    pub async fn debt(&self, did: &Did, reason: i16) -> Result<Option<Option<i16>>> {
        Ok(sqlx::query_scalar(
            "SELECT d.cap_type FROM relist_debt d JOIN actors a ON a.id = d.actor_id
             WHERE a.did = $1 AND d.reason = $2",
        )
        .bind(did.as_str())
        .bind(reason)
        .fetch_optional(&self.pool)
        .await?)
    }
}

type ListRow = (
    i16,
    Option<i16>,
    i32,
    i32,
    i32,
    bool,
    bool,
    Option<i16>,
    Option<DateTime<Utc>>,
    bool,
    i16,
    bool,
    i64,
    bool,
);

/// Tracking columns of a list, as the harness checks them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListView {
    pub state: TrackState,
    pub purge_then: Option<TrackState>,
    pub listblock_count: i32,
    pub item_count: i32,
    pub admit_epoch: i32,
    pub admitted: bool,
    pub retaining: bool,
    pub deferred_by: Option<i16>,
    pub fetched_witness: Option<DateTime<Utc>>,
    pub capped: bool,
    pub record_state: i16,
    /// A `list_jobs` row exists for the current epoch.
    pub job_current: bool,
    pub sched_keys: i64,
    pub retry_scheduled: bool,
}

// ---------------------------------------------------------------- random

/// xorshift64*: deterministic, dependency-free.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.max(1))
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// All permutations of `v` (Heap's algorithm; small inputs only).
pub fn permutations<T: Clone>(v: &[T]) -> Vec<Vec<T>> {
    fn heap<T: Clone>(k: usize, a: &mut Vec<T>, out: &mut Vec<Vec<T>>) {
        if k <= 1 {
            out.push(a.clone());
            return;
        }
        heap(k - 1, a, out);
        for i in 0..k - 1 {
            if k.is_multiple_of(2) {
                a.swap(i, k - 1);
            } else {
                a.swap(0, k - 1);
            }
            heap(k - 1, a, out);
        }
    }
    let mut a = v.to_vec();
    let mut out = Vec::new();
    heap(a.len(), &mut a, &mut out);
    out
}
