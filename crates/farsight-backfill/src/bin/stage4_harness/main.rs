//! `farsight-stage4-harness`: integration tests of backfill (repo jobs,
//! list jobs, the scheduler, the sweep, repairs and the feeder). Every
//! assertion reads real stored rows written by the real backfill code
//! paths (`repo::run`, `list_phase1::run`, `list_fetch::run`, the
//! scheduler, the sweep, the feeder, the budget monitor) running against a
//! fake PDS / PLC / relay on loopback; check 11 runs the real `farsight`
//! and `farsight-backfill` binaries against a live v2 Jetstream.
//!
//! Time-dependent rules (retry ladders, `pending_max_age`, terminal
//! failure) are driven by moving the stored timestamps back ("clock
//! injection"), never by changing the rules.

mod fake;
mod support;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use farsight_backfill::ctx::Ctx;
use farsight_backfill::jobs::{self, JobReq, JobResult, Outcome};
use farsight_backfill::net::{Client, Net};
use farsight_backfill::resolve::Resolver;
use farsight_backfill::scheduler::Scheduler;
use farsight_backfill::{feeder, sweep};
use farsight_core::config::SweepSource;
use farsight_core::record::parse_record;
use farsight_core::{Collection, Config, ConfigDuration, Did, RecordKey, Tid};
use farsight_storage::apply::{self, ApplyCtx, Batch, Origin, Write, WriteAction};
use farsight_storage::codes::Protocol;
use farsight_storage::firehose::FirehoseProgress;
use farsight_storage::repo_events::RepoEvent;
use farsight_storage::tracking::FireArgs;
use farsight_storage::transition::Event;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::sync::watch;

use crate::fake::{Repo, Shared, w};
use crate::support::{Checks, Pg, Proc, free_port};
use farsight_storage::ids::{ActorId, ListId, Stamp};

type Res<T> = Result<T, String>;

fn e(x: impl std::fmt::Display) -> String {
    x.to_string()
}

const LIVE_JETSTREAM: &str = "ws://127.0.0.1:16008";
const LIVE_SECS: u64 = 300;

// ------------------------------------------------------------ identities

/// A synthetic `did:plc` (24 base32 characters), as in the stage-3 harness.
fn did(prefix: &str, n: u64) -> String {
    assert_eq!(prefix.len(), 3);
    let digits: String = format!("{n:021}")
        .chars()
        .map(|c| (b'a' + (c as u8 - b'0')) as char)
        .collect();
    format!("did:plc:{prefix}{digits}")
}

static LAST_US: AtomicU64 = AtomicU64::new(0);

/// A fresh, strictly increasing TID at "now".
fn tid_now() -> String {
    let now = Utc::now().timestamp_micros() as u64;
    let mut last = LAST_US.load(Ordering::SeqCst);
    let us = loop {
        let next = last.max(now - 1) + 1;
        match LAST_US.compare_exchange(last, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break next,
            Err(cur) => last = cur,
        }
    };
    Tid::from_parts(us, 7).expect("tid").encode()
}

fn tid_at(t: DateTime<Utc>) -> String {
    Tid::from_parts(t.timestamp_micros() as u64, 3)
        .expect("tid")
        .encode()
}

const CREATED: &str = "2026-01-01T00:00:00.000Z";

fn block_v(subject: &str) -> Value {
    json!({"$type": "app.bsky.graph.block", "subject": subject, "createdAt": CREATED})
}
fn list_v(name: &str) -> Value {
    json!({"$type": "app.bsky.graph.list", "name": name, "purpose": "app.bsky.graph.defs#modlist", "createdAt": CREATED})
}
fn item_v(subject: &str, list: &str) -> Value {
    json!({"$type": "app.bsky.graph.listitem", "subject": subject, "list": list, "createdAt": CREATED})
}
fn lb_v(list: &str) -> Value {
    json!({"$type": "app.bsky.graph.listblock", "subject": list, "createdAt": CREATED})
}
fn list_uri(owner: &str, rkey: &str) -> String {
    format!("at://{owner}/app.bsky.graph.list/{rkey}")
}

// ------------------------------------------------------------- the world

struct H {
    ctx: Arc<Ctx>,
    world: Shared,
    pds: String,
    seq: AtomicI64,
}

impl H {
    fn pool(&self) -> &PgPool {
        &self.ctx.pool
    }

    fn set_cfg(&self, f: impl FnOnce(&mut Config)) {
        let mut c = (*self.ctx.cfg()).clone();
        f(&mut c);
        self.ctx.set_cfg(Arc::new(c));
    }

    /// A repo on the fake PDS, registered in the fake PLC.
    fn put_repo(&self, d: &str, records: Vec<(Collection, String, Value)>) {
        let mut wd = w(&self.world);
        let mut repo = Repo {
            rev: tid_now(),
            ..Repo::default()
        };
        for (k, rk, v) in records {
            repo.records.insert((k.nsid().to_owned(), rk), v);
        }
        wd.repos.insert(d.to_owned(), repo);
        wd.plc.insert(d.to_owned(), self.pds.clone());
    }

    fn edit_repo(&self, d: &str, f: impl FnOnce(&mut Repo)) {
        let mut wd = w(&self.world);
        if let Some(r) = wd.repos.get_mut(d) {
            f(r);
        }
    }

    fn hits(&self, method: &str, d: &str, coll: &str) -> usize {
        w(&self.world)
            .hits
            .iter()
            .filter(|(m, dd, c)| m == method && dd == d && (coll.is_empty() || c == coll))
            .count()
    }

    /// A firehose create (commit rev = a fresh TID, witness = now).
    async fn fh(
        &self,
        author: &str,
        k: Collection,
        rkey: &str,
        v: Value,
    ) -> Res<farsight_storage::txn::ApplyReport> {
        let a = Did::parse(author).map_err(e)?;
        let rec = parse_record(&a, k, &v).map_err(e)?;
        let mut b = Batch::new(Origin::Firehose);
        b.writes.push(Write {
            author: a,
            collection: k,
            rkey: RecordKey::parse(rkey).map_err(e)?,
            stamp: Stamp::from_tid(Tid::parse(&tid_now()).map_err(e)?),
            witness: Some(Utc::now()),
            action: WriteAction::Upsert(rec),
        });
        self.apply(&b).await
    }

    async fn apply(&self, b: &Batch) -> Res<farsight_storage::txn::ApplyReport> {
        let limits = self.ctx.limits();
        let actx = ApplyCtx {
            limits: &limits,
            gates: self.ctx.gates.load(),
            counters: &self.ctx.counters,
        };
        apply::apply(self.pool(), &actx, b).await.map_err(e)
    }

    /// One committed "ingest batch": firehose progress + a clock row.
    async fn advance(&self) -> Res<()> {
        let mut b = Batch::new(Origin::Firehose);
        b.firehose = Some(FirehoseProgress {
            source_url: "ws://harness.invalid".into(),
            protocol: Protocol::V2,
            cursor_seq: Some(self.seq.fetch_add(1, Ordering::SeqCst)),
            cursor_us: None,
            applied_through: Utc::now(),
        });
        self.apply(&b).await.map(|_| ())
    }

    /// A repo job the way the scheduler runs one (lease, run, release).
    async fn job(&self, d: &str, tier: i16, requester: &str) -> Res<JobResult> {
        let did = Did::parse(d).map_err(e)?;
        if !jobs::acquire_lease(self.pool(), d, &self.ctx.lease_owner)
            .await
            .map_err(e)?
        {
            return Err(format!("lease for {d} is held"));
        }
        let r = jobs::repo::run(
            &self.ctx,
            &JobReq {
                did,
                tier: farsight_storage::codes::Tier::from_code(tier).expect("a tier"),
                requester: farsight_storage::codes::RequesterKey::parse(requester)
                    .expect("a requester key"),
            },
        )
        .await;
        jobs::release_lease(self.pool(), d, &self.ctx.lease_owner).await;
        Ok(r)
    }

    /// The server's purge task (purge→X, then PD), which the harness
    /// runs in place of a server process.
    async fn purges(&self) -> Res<()> {
        farsight_storage::janitor::process_purges(
            self.pool(),
            &self.ctx.limits(),
            &self.ctx.counters,
            100,
        )
        .await
        .map(|_| ())
        .map_err(e)
    }

    async fn fire(&self, list: i64, ev: Event) -> Res<()> {
        farsight_storage::janitor::fire_event(
            self.pool(),
            &self.ctx.limits(),
            &self.ctx.counters,
            ListId::new(list),
            ev,
            FireArgs::default(),
        )
        .await
        .map(|_| ())
        .map_err(e)
    }

    async fn i64(&self, sql: &str) -> Res<i64> {
        sqlx::query_scalar::<_, i64>(sql)
            .fetch_one(self.pool())
            .await
            .map_err(|x| format!("{x}: {sql}"))
    }

    async fn opt_i64(&self, sql: &str) -> Res<Option<i64>> {
        sqlx::query_scalar::<_, Option<i64>>(sql)
            .fetch_optional(self.pool())
            .await
            .map(Option::flatten)
            .map_err(|x| format!("{x}: {sql}"))
    }

    async fn bool(&self, sql: &str) -> Res<bool> {
        sqlx::query_scalar::<_, bool>(sql)
            .fetch_one(self.pool())
            .await
            .map_err(|x| format!("{x}: {sql}"))
    }

    async fn exec(&self, sql: &str) -> Res<u64> {
        sqlx::query(sql)
            .execute(self.pool())
            .await
            .map(|r| r.rows_affected())
            .map_err(|x| format!("{x}: {sql}"))
    }

    async fn id(&self, d: &str) -> Res<i64> {
        self.i64(&format!("SELECT id FROM actors WHERE did = '{d}'"))
            .await
    }

    async fn list_id(&self, owner: &str, rkey: &str) -> Res<i64> {
        self.i64(&format!(
            "SELECT l.id FROM lists l JOIN actors a ON a.id = l.owner_id WHERE a.did = '{owner}' AND l.rkey = '{rkey}'"
        ))
        .await
    }

    async fn track(&self, list: i64) -> Res<i64> {
        self.i64(&format!(
            "SELECT track_state::BIGINT FROM lists WHERE id = {list}"
        ))
        .await
    }
}

fn base_config(dsn: &str, pds: &str, plc: &str) -> Config {
    let mut c = Config::default();
    c.storage.database_url = dsn.into();
    c.storage.budget_bytes = 70_000_000_000;
    c.backfill.plc_url = plc.into();
    c.backfill.relay_url = pds.into();
    c.backfill.concurrency = 16;
    c.backfill.per_host_rps = 2000;
    c.backfill.per_host_concurrency = 16;
    c.backfill.plc_rps = 2000;
    c.backfill.retry_schedule = vec![ConfigDuration::secs(1)];
    c.backfill.terminal_after = ConfigDuration::secs(5);
    c.backfill.sweep.enabled = false;
    c.backfill.sweep.source = SweepSource::RelayCollections;
    c.backfill.sweep.max_outstanding = 200;
    c.net.allow_http_hosts = vec!["127.0.0.1".into()];
    c
}

// ------------------------------------------------------------------ main

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let keep = args.iter().any(|a| a == "--keep");
    let skip_live = args.iter().any(|a| a == "--skip-live");
    let live_only = args.iter().any(|a| a == "--live-only");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async move {
        let mut c = Checks::default();
        let pg = match Pg::start(keep) {
            Ok(p) => p,
            Err(x) => {
                eprintln!("postgres: {x}");
                return ExitCode::from(2);
            }
        };
        let r = if live_only {
            match pg.wait_ready(Duration::from_secs(60)).await {
                Ok(()) => {
                    c.section("11. v2 ingest live");
                    check_live(&pg, &mut c).await
                }
                Err(x) => Err(x),
            }
        } else {
            run(&mut c, &pg, skip_live).await
        };
        pg.stop();
        if let Err(x) = r {
            c.check("harness ran to completion", false, x);
        }
        let (p, f, u) = c.counts();
        println!("\n== summary: {p} passed, {f} failed, {u} unverified");
        for i in c
            .items
            .iter()
            .filter(|i| i.verdict != support::Verdict::Pass)
        {
            println!("  [{}] {} — {}", support::tag(i.verdict), i.what, i.detail);
        }
        if f == 0 {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(1)
        }
    })
}

async fn run(c: &mut Checks, pg: &Pg, skip_live: bool) -> Res<()> {
    pg.wait_ready(Duration::from_secs(60)).await?;
    pg.create_db("bf").await?;
    let pool = pg.pool("bf", 40).await?;
    farsight_storage::migrate(&pool).await.map_err(e)?;
    let world: Shared = Arc::default();
    let (pds, plc) = fake::start(world.clone()).await?;
    let cfg = base_config(&pg.url("bf"), &pds, &plc);
    let net = Arc::new(Net::new(
        Client::Plain(reqwest::Client::new()),
        cfg.backfill.per_host_rps,
        cfg.backfill.per_host_concurrency,
        cfg.backfill.plc_rps,
        &cfg.backfill.plc_url,
    ));
    let resolver = Resolver::new(net.clone(), pool.clone(), &cfg, None);
    let ctx = Arc::new(Ctx::new(
        pool.clone(),
        Arc::new(cfg),
        net,
        resolver,
        "harness",
    ));
    let h = Arc::new(H {
        ctx,
        world,
        pds,
        seq: AtomicI64::new(1),
    });
    h.advance().await?;
    // The "server": one committed ingest batch per second keeps the clock
    // moving, and its purge task finishes purge→X transitions.
    let keeper = {
        let h = h.clone();
        tokio::spawn(async move {
            loop {
                let _ = h.advance().await;
                let _ = h.purges().await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let checks: Vec<(&str, u8)> = vec![
        (
            "6. scheduler fairness (DRR across requesters, high:normal 4:1)",
            6,
        ),
        ("1. repo job outcomes", 1),
        ("2. divergence check", 2),
        ("3. list phase 1", 3),
        ("4. list fetch run", 4),
        ("5. pending wall-clock timeout", 5),
        ("7. budget gate", 7),
        ("8. sweep cycle", 8),
        ("9. repair cycle", 9),
        ("10. debt feeder", 10),
        ("extra: subject discovery", 12),
        ("extra: hostile hosts and failing jobs", 13),
        ("extra: a relay without listReposByCollection", 14),
    ];
    for (name, n) in checks {
        c.section(name);
        let r = match n {
            1 => check_outcomes(&h, c).await,
            2 => check_divergence(&h, c).await,
            3 => check_phase1(&h, c).await,
            4 => check_fetch(&h, c).await,
            5 => check_pending_timeout(&h, c).await,
            6 => check_fairness(&h, c).await,
            7 => check_budget(&h, c).await,
            8 => check_sweep(&h, c).await,
            9 => check_repair(&h, c).await,
            10 => check_feeder(&h, c).await,
            13 => check_hostile(&h, c).await,
            14 => check_fallback(&h, c).await,
            _ => check_discovery(&h, c).await,
        };
        if let Err(x) = r {
            c.check(format!("{name}: ran to completion"), false, x);
        }
    }
    keeper.abort();
    c.section("11. v2 ingest live");
    if skip_live {
        c.unverified("v2 ingest live", "skipped (--skip-live)");
    } else if let Err(x) = check_live(pg, c).await {
        c.check("11. v2 ingest live: ran to completion", false, x);
    }
    Ok(())
}

// ------------------------------------------------------------- check 1

async fn check_outcomes(h: &H, c: &mut Checks) -> Res<()> {
    let (a, b, cc, d) = (did("rpa", 1), did("rpb", 1), did("rpc", 1), did("rpd", 1));
    // A cycle started before the jobs: each job settles its membership.
    let cycle = h
        .i64("INSERT INTO sweep_cycles (kind, source, collections, started_at, effective_start)
              VALUES (1, 'relay_collections', '{1,2,3,4}', now() - interval '1 minute', now() - interval '1 minute')
              RETURNING id")
        .await?;
    for x in [&a, &b, &cc, &d] {
        h.exec(&format!(
            "INSERT INTO cycle_outstanding (cycle_id, did, state) VALUES ({cycle}, '{x}', 1)"
        ))
        .await?;
    }
    // A: clean — 3 blocks and a list.
    let mut recs: Vec<(Collection, String, Value)> = (1..=3)
        .map(|i| (Collection::Block, tid_now(), block_v(&did("sub", i))))
        .collect();
    recs.push((Collection::List, tid_now(), list_v("a list")));
    h.put_repo(&a, recs);
    let r = h.job(&a, 1, "token:1").await?;
    let row: (i16, Option<DateTime<Utc>>, Option<i16>) = sqlx::query_as(
        "SELECT b.state, b.clean_witness, b.last_outcome FROM backfill_state b JOIN actors x ON x.id = b.actor_id WHERE x.did = $1",
    )
    .bind(&a)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let blocks = h
        .i64(&format!(
            "SELECT count(*) FROM blocks b JOIN actors x ON x.id = b.author_id WHERE x.did = '{a}'"
        ))
        .await?;
    let member = h
        .i64(&format!(
            "SELECT count(*) FROM cycle_outstanding WHERE cycle_id = {cycle} AND did = '{a}'"
        ))
        .await?;
    c.check(
        "clean: state done, last_outcome clean, clean_witness set, 3 blocks stored, membership settled",
        r.outcome == Outcome::Clean && row.0 == 3 && row.2 == Some(1) && row.1.is_some() && blocks == 3 && member == 0,
        format!("outcome {:?}, row {row:?}, blocks {blocks}, outstanding {member}", r.outcome),
    );
    // B: complete-with-debts — 8 blocks against a per-author cap of 5.
    h.set_cfg(|c| c.limits.blocks_per_author = 5);
    h.put_repo(
        &b,
        (1..=8)
            .map(|i| (Collection::Block, tid_now(), block_v(&did("sub", 10 + i))))
            .collect(),
    );
    let r = h.job(&b, 1, "token:1").await?;
    h.set_cfg(|c| c.limits.blocks_per_author = Config::default().limits.blocks_per_author);
    let row: (i16, Option<DateTime<Utc>>, Option<i16>) = sqlx::query_as(
        "SELECT b.state, b.clean_witness, b.last_outcome FROM backfill_state b JOIN actors x ON x.id = b.actor_id WHERE x.did = $1",
    )
    .bind(&b)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let blocks = h
        .i64(&format!(
            "SELECT count(*) FROM blocks b JOIN actors x ON x.id = b.author_id WHERE x.did = '{b}'"
        ))
        .await?;
    let debt = h.opt_i64(&format!("SELECT d.cap_type::BIGINT FROM relist_debt d JOIN actors x ON x.id = d.actor_id WHERE x.did = '{b}' AND d.reason = 3")).await?;
    let member = h
        .i64(&format!(
            "SELECT count(*) FROM cycle_outstanding WHERE cycle_id = {cycle} AND did = '{b}'"
        ))
        .await?;
    c.check(
        "complete-with-debts: 5 of 8 blocks stored, capped debt (blocks_per_author), no clean point, membership settled",
        r.outcome == Outcome::CompleteWithDebts && row.2 == Some(2) && row.1.is_none() && blocks == 5 && debt == Some(1) && member == 0,
        format!("outcome {:?}, row {row:?}, blocks {blocks}, capped debt cap_type {debt:?}, outstanding {member}", r.outcome),
    );
    // C: inactive — stored rows, then the account is deleted upstream.
    h.put_repo(&cc, vec![]);
    for i in 0..2 {
        h.fh(
            &cc,
            Collection::Block,
            &tid_now(),
            block_v(&did("sub", 30 + i)),
        )
        .await?;
    }
    let before = h
        .i64(&format!(
            "SELECT count(*) FROM blocks b JOIN actors x ON x.id = b.author_id WHERE x.did = '{cc}'"
        ))
        .await?;
    h.edit_repo(&cc, |r| r.repo_error = Some("RepoNotFound".into()));
    w(&h.world)
        .status
        .insert(cc.clone(), (false, Some("deleted".into())));
    let r = h.job(&cc, 1, "token:1").await?;
    let row: (Option<i16>, bool) = sqlx::query_as(
        "SELECT b.last_outcome, b.inactive_at_listing FROM backfill_state b JOIN actors x ON x.id = b.actor_id WHERE x.did = $1",
    )
    .bind(&cc)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let status = h
        .i64(&format!(
            "SELECT status::BIGINT FROM actors WHERE did = '{cc}'"
        ))
        .await?;
    let after = h
        .i64(&format!(
            "SELECT count(*) FROM blocks b JOIN actors x ON x.id = b.author_id WHERE x.did = '{cc}'"
        ))
        .await?;
    let member = h
        .i64(&format!(
            "SELECT count(*) FROM cycle_outstanding WHERE cycle_id = {cycle} AND did = '{cc}'"
        ))
        .await?;
    c.check(
        "inactive: relay says deleted ⇒ status hidden, rows purged, last_outcome inactive, inactive_at_listing, membership settled",
        r.outcome == Outcome::Inactive && row == (Some(3), true) && status != 0 && before == 2 && after == 0 && member == 0,
        format!("outcome {:?}, row {row:?}, status {status}, blocks {before}→{after}, outstanding {member}", r.outcome),
    );
    // D: failed — the PDS is unreachable.
    w(&h.world)
        .plc
        .insert(d.clone(), "http://127.0.0.1:1".into());
    let r = h.job(&d, 3, "system:sweep").await?;
    let row: (i16, i32, bool) = sqlx::query_as(
        "SELECT b.state, b.attempts, b.next_attempt_at > now() FROM backfill_state b JOIN actors x ON x.id = b.actor_id WHERE x.did = $1",
    )
    .bind(&d)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let queued = h.bool(&format!("SELECT EXISTS (SELECT 1 FROM backfill_queue q JOIN actors x ON x.id = q.actor_id WHERE x.did = '{d}' AND q.kind = 1 AND q.tier = 3 AND q.not_before > now())")).await?;
    let member = h
        .opt_i64(&format!(
            "SELECT state::BIGINT FROM cycle_outstanding WHERE cycle_id = {cycle} AND did = '{d}'"
        ))
        .await?;
    c.check(
        "failed (not yet terminal): state failed, attempt counted, retry queued with backoff, membership still outstanding",
        matches!(r.outcome, Outcome::Failed { terminal: false, .. }) && row == (4, 1, true) && queued && member == Some(1),
        format!("outcome {:?}, row {row:?}, queued {queued}, membership {member:?}", r.outcome),
    );
    // Failing longer than terminal_after (clock injection) ⇒ terminal.
    h.exec(&format!(
        "DELETE FROM backfill_queue WHERE actor_id = (SELECT id FROM actors WHERE did = '{d}')"
    ))
    .await?;
    h.exec(&format!("UPDATE backfill_state SET first_failed_at = now() - interval '1 hour' WHERE actor_id = (SELECT id FROM actors WHERE did = '{d}')")).await?;
    let r = h.job(&d, 3, "system:sweep").await?;
    let debt = h.bool(&format!("SELECT EXISTS (SELECT 1 FROM relist_debt x JOIN actors a ON a.id = x.actor_id WHERE a.did = '{d}' AND x.reason = 1)")).await?;
    let member = h
        .opt_i64(&format!(
            "SELECT state::BIGINT FROM cycle_outstanding WHERE cycle_id = {cycle} AND did = '{d}'"
        ))
        .await?;
    let ft = h
        .i64(&format!(
            "SELECT failed_terminal FROM sweep_cycles WHERE id = {cycle}"
        ))
        .await?;
    let requeued = h.bool(&format!("SELECT EXISTS (SELECT 1 FROM backfill_queue q JOIN actors x ON x.id = q.actor_id WHERE x.did = '{d}')")).await?;
    c.check(
        "failed (terminal): unreachable debt, membership terminal, failed_terminal counted, no further queue entry",
        matches!(r.outcome, Outcome::Failed { terminal: true, .. }) && debt && member == Some(2) && ft == 1 && !requeued,
        format!("outcome {:?}, debt {debt}, membership {member:?}, failed_terminal {ft}, requeued {requeued}", r.outcome),
    );
    let done = h
        .i64(&format!("SELECT done FROM sweep_cycles WHERE id = {cycle}"))
        .await?;
    c.check(
        "cycle bookkeeping: done = 3 (clean, cwd, inactive)",
        done == 3,
        format!("done {done}"),
    );
    h.exec(&format!(
        "DELETE FROM cycle_outstanding WHERE cycle_id = {cycle}"
    ))
    .await?;
    h.exec(&format!("DELETE FROM sweep_cycles WHERE id = {cycle}"))
        .await?;
    Ok(())
}

// ------------------------------------------------------------- check 2

async fn check_divergence(h: &H, c: &mut Checks) -> Res<()> {
    let (o, x) = (did("dva", 1), did("dvb", 1));
    let lrk = tid_now();
    let l_uri = list_uri(&o, &lrk);
    let (b1, b2, bx) = (tid_now(), tid_now(), tid_now());
    let (i1, i2) = (tid_now(), tid_now());
    // Stored first, through the firehose: list, items, blocks (+ bx, which
    // the PDS no longer has).
    h.fh(&o, Collection::List, &lrk, list_v("diverging"))
        .await?;
    h.fh(&x, Collection::ListBlock, &tid_now(), lb_v(&l_uri))
        .await?;
    h.fh(
        &o,
        Collection::ListItem,
        &i1,
        item_v(&did("sub", 40), &l_uri),
    )
    .await?;
    h.fh(
        &o,
        Collection::ListItem,
        &i2,
        item_v(&did("sub", 41), &l_uri),
    )
    .await?;
    for (rk, s) in [(&b1, 42), (&b2, 43), (&bx, 44)] {
        h.fh(&o, Collection::Block, rk, block_v(&did("sub", s)))
            .await?;
    }
    let lid = h.list_id(&o, &lrk).await?;
    h.put_repo(
        &o,
        vec![
            (Collection::List, lrk.clone(), list_v("diverging")),
            (
                Collection::ListItem,
                i1.clone(),
                item_v(&did("sub", 40), &l_uri),
            ),
            (
                Collection::ListItem,
                i2.clone(),
                item_v(&did("sub", 41), &l_uri),
            ),
            (Collection::Block, b1.clone(), block_v(&did("sub", 42))),
            (Collection::Block, b2.clone(), block_v(&did("sub", 43))),
        ],
    );
    // Phase 1 and a fetch run make the list ready (the transition where DV
    // matters most: it must never be served empty as ready).
    jobs::list_phase1::run(&h.ctx, ListId::new(lid)).await;
    let oid = h.id(&o).await?;
    h.exec(&format!(
        "DELETE FROM backfill_queue WHERE actor_id = {oid} AND kind = 2"
    ))
    .await?;
    jobs::list_fetch::run(&h.ctx, ActorId::new(oid), &Did::parse(&o).map_err(e)?).await;
    let state_before = h.track(lid).await?;
    let epoch_before = h
        .i64(&format!(
            "SELECT admit_epoch::BIGINT FROM lists WHERE id = {lid}"
        ))
        .await?;
    let items_before = h
        .i64(&format!(
            "SELECT count(*) FROM list_items WHERE list_id = {lid}"
        ))
        .await?;
    // The previous listing saw a rev above what the PDS now reports.
    let future = Tid::parse(&tid_at(Utc::now() + chrono::Duration::days(1)))
        .map_err(e)?
        .as_i64();
    h.exec(&format!(
        "INSERT INTO backfill_state (actor_id, state, backfill_rev) VALUES ({oid}, 3, {future})
         ON CONFLICT (actor_id) DO UPDATE SET backfill_rev = {future}"
    ))
    .await?;
    // The re-listing fails part-way (blocks answer 500), so the state
    // between the purge and a complete re-list is observable.
    h.edit_repo(&o, |r| {
        r.fail_collection = Some(Collection::Block.nsid().into())
    });
    let r = h.job(&o, 1, "token:1").await?;
    // Right after the job the list is purging (or, if the purge task got
    // there first, already re-admitted) — never ready.
    let state_after = h.track(lid).await?;
    h.purges().await?;
    let state_pd = h.track(lid).await?;
    let epoch_after = h
        .i64(&format!(
            "SELECT admit_epoch::BIGINT FROM lists WHERE id = {lid}"
        ))
        .await?;
    let items_after = h
        .i64(&format!(
            "SELECT count(*) FROM list_items WHERE list_id = {lid}"
        ))
        .await?;
    let blocks = h
        .i64(&format!(
            "SELECT count(*) FROM blocks WHERE author_id = {oid}"
        ))
        .await?;
    let resync = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM relist_debt WHERE actor_id = {oid} AND reason = 2)"
        ))
        .await?;
    c.check(
        "DV fired on the owner's ready list: purge→untracked (never ready empty), PD re-admits it (pending, new epoch), items purged",
        state_before == 2 && matches!(state_after, 5 | 1) && state_pd == 1 && epoch_after > epoch_before && items_before == 2 && items_after == 0,
        format!("track_state {state_before}→{state_after}→(PD) {state_pd}, epoch {epoch_before}→{epoch_after}, items {items_before}→{items_after}"),
    );
    c.check(
        "authored rows purged and a resync debt added",
        blocks == 0 && resync && matches!(r.outcome, Outcome::Failed { .. }),
        format!(
            "blocks {blocks}, resync debt {resync}, outcome {:?}",
            r.outcome
        ),
    );
    // The listing restarts: the next run lists the PDS's state.
    h.edit_repo(&o, |r| r.fail_collection = None);
    h.exec(&format!(
        "DELETE FROM backfill_queue WHERE actor_id = {oid}"
    ))
    .await?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let r = h.job(&o, 1, "token:1").await?;
    let keys: Vec<String> =
        sqlx::query_scalar("SELECT rkey FROM blocks WHERE author_id = $1 ORDER BY rkey")
            .bind(oid)
            .fetch_all(h.pool())
            .await
            .map_err(e)?;
    let resync = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM relist_debt WHERE actor_id = {oid} AND reason = 2)"
        ))
        .await?;
    let rev: Option<i64> = h
        .opt_i64(&format!(
            "SELECT backfill_rev FROM backfill_state WHERE actor_id = {oid}"
        ))
        .await?;
    let pds_rev = Tid::parse(&w(&h.world).repos[&o].rev).map_err(e)?.as_i64();
    c.check(
        "listing restarts: blocks = the PDS's (bx gone), clean, resync debt cleared, backfill_rev = R",
        r.outcome == Outcome::Clean && keys == vec![b1.clone(), b2.clone()] && !resync && rev == Some(pds_rev),
        format!("outcome {:?}, blocks {keys:?}, resync {resync}, rev {rev:?} vs {pds_rev}", r.outcome),
    );
    Ok(())
}

// ------------------------------------------------------------- check 3

async fn check_phase1(h: &H, c: &mut Checks) -> Res<()> {
    let (f, g) = (did("pha", 1), did("phb", 1));
    h.put_repo(&f, vec![]);
    let rk = tid_now();
    h.fh(
        &g,
        Collection::ListBlock,
        &tid_now(),
        lb_v(&list_uri(&f, &rk)),
    )
    .await?;
    let lid = h.list_id(&f, &rk).await?;
    let admitted = h.track(lid).await?;
    let job = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM list_jobs WHERE list_id = {lid})"
        ))
        .await?;
    c.check(
        "a listblock admits its list: pending with a phase-1 job",
        admitted == 1 && job,
        format!("track_state {admitted}, list_jobs {job}"),
    );
    jobs::list_phase1::run(&h.ctx, ListId::new(lid)).await;
    let nf = h.track(lid).await?;
    h.purges().await?;
    let first = h.track(lid).await?;
    c.check(
        "record not found ⇒ NF: pending → purge→missing → (PD) missing",
        matches!(nf, 5 | 6) && first == 6,
        format!("track_state {nf} → {first}"),
    );
    let ladder = h.ctx.cfg().backfill.missing_retry.len();
    let mut states = vec![first];
    for _ in 0..ladder {
        // Clock injection: the next re-check is due.
        h.exec(&format!(
            "UPDATE lists SET next_retry_at = now() - interval '1 second' WHERE id = {lid}"
        ))
        .await?;
        jobs::list_phase1::run(&h.ctx, ListId::new(lid)).await;
        h.purges().await?;
        states.push(h.track(lid).await?);
    }
    let dead_at = states.iter().position(|s| *s == 7);
    c.check(
        format!("each re-check of the {ladder}-step missing_retry ladder stays missing; the last fires NFx ⇒ dead"),
        dead_at == Some(ladder) && states[..ladder].iter().all(|s| *s == 6),
        format!("states {states:?}"),
    );
    // An existing list passes phase 1 and enqueues the owner's fetch.
    let (f2, g2) = (did("phc", 1), did("phd", 1));
    let rk2 = tid_now();
    h.put_repo(&f2, vec![(Collection::List, rk2.clone(), list_v("exists"))]);
    h.fh(
        &g2,
        Collection::ListBlock,
        &tid_now(),
        lb_v(&list_uri(&f2, &rk2)),
    )
    .await?;
    let lid2 = h.list_id(&f2, &rk2).await?;
    jobs::list_phase1::run(&h.ctx, ListId::new(lid2)).await;
    let (passed, rec): (bool, i16) =
        sqlx::query_as("SELECT phase1_epoch = admit_epoch, record_state FROM lists WHERE id = $1")
            .bind(lid2)
            .fetch_one(h.pool())
            .await
            .map_err(e)?;
    let job = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM list_jobs WHERE list_id = {lid2})"
        ))
        .await?;
    let fetch = h.bool(&format!("SELECT EXISTS (SELECT 1 FROM backfill_queue q JOIN actors a ON a.id = q.actor_id WHERE a.did = '{f2}' AND q.kind = 2 AND q.requester = 'system:lists')")).await?;
    let state = h.track(lid2).await?;
    c.check(
        "existing list: record stored, phase 1 passed (phase1_epoch = admit_epoch), phase-1 job gone, list_fetch enqueued, still pending",
        passed && rec == 1 && !job && fetch && state == 1,
        format!("passed {passed}, record_state {rec}, list_jobs {job}, fetch queued {fetch}, state {state}"),
    );
    Ok(())
}

// ------------------------------------------------------------- check 4

async fn check_fetch(h: &H, c: &mut Checks) -> Res<()> {
    let (o, x) = (did("lfa", 1), did("lfb", 1));
    let rks: Vec<String> = (0..3).map(|_| tid_now()).collect();
    let mut recs = Vec::new();
    let sizes = [2u64, 2, 3];
    let mut n = 50;
    for (rk, size) in rks.iter().zip(sizes) {
        recs.push((Collection::List, rk.clone(), list_v("fetched")));
        for _ in 0..size {
            n += 1;
            recs.push((
                Collection::ListItem,
                tid_now(),
                item_v(&did("sub", n), &list_uri(&o, rk)),
            ));
        }
    }
    h.put_repo(&o, recs);
    let mut ids = Vec::new();
    for rk in &rks {
        h.fh(
            &x,
            Collection::ListBlock,
            &tid_now(),
            lb_v(&list_uri(&o, rk)),
        )
        .await?;
        let id = h.list_id(&o, rk).await?;
        jobs::list_phase1::run(&h.ctx, ListId::new(id)).await;
        ids.push(id);
    }
    let oid = h.id(&o).await?;
    let queued = h
        .i64(&format!(
            "SELECT count(*) FROM backfill_queue WHERE actor_id = {oid} AND kind = 2"
        ))
        .await?;
    c.check(
        "three phase-1 passes collapse into one waiting list_fetch",
        queued == 1,
        format!("entries {queued}"),
    );
    // The scheduler claims the entry by deleting it, then runs the fetch.
    h.exec(&format!(
        "DELETE FROM backfill_queue WHERE actor_id = {oid} AND kind = 2"
    ))
    .await?;
    h.set_cfg(|c| c.limits.list_items_per_list = 2);
    let before = h.hits("listRecords", &o, Collection::ListItem.nsid());
    let od = Did::parse(&o).map_err(e)?;
    let r = jobs::list_fetch::run(&h.ctx, ActorId::new(oid), &od).await;
    let pages = h.hits("listRecords", &o, Collection::ListItem.nsid()) - before;
    let rows: Vec<(i16, Option<DateTime<Utc>>, bool, i64)> = sqlx::query_as(
        "SELECT l.track_state, l.fetched_witness, l.capped, (SELECT count(*) FROM list_items i WHERE i.list_id = l.id)
         FROM lists l WHERE l.id = ANY($1) ORDER BY l.id",
    )
    .bind(&ids)
    .fetch_all(h.pool())
    .await
    .map_err(e)?;
    let runs = h
        .i64(&format!(
            "SELECT count(*) FROM list_fetch_runs WHERE owner_id = {oid} AND outcome = 1"
        ))
        .await?;
    let witnesses: HashSet<Option<DateTime<Utc>>> = rows.iter().map(|r| r.1).collect();
    c.check(
        "all three claimed and promoted together by one run (ready, one fetched_witness), one listing pass over listitems",
        rows.iter().all(|r| r.0 == 2) && witnesses.len() == 1 && !witnesses.contains(&None) && runs == 1 && pages == 1,
        format!("outcome {:?}, lists {rows:?}, runs {runs}, listRecords pages {pages}", r.outcome),
    );
    c.check(
        "the 3-item list is capped at list_items_per_list = 2; the others are complete",
        rows[2].2 && rows[2].3 == 2 && !rows[0].2 && rows[0].3 == 2 && !rows[1].2,
        format!("{rows:?}"),
    );
    // The cap is raised; a refresh run stores every item and clears capped.
    h.set_cfg(|c| c.limits.list_items_per_list = Config::default().limits.list_items_per_list);
    h.exec(&format!(
        "UPDATE lists SET refresh_requested = true WHERE id = {}",
        ids[2]
    ))
    .await?;
    h.exec(&format!(
        "UPDATE list_fetch_runs SET started_at = started_at - interval '1 day', finished_at = finished_at - interval '1 day' WHERE owner_id = {oid}"
    ))
    .await?;
    let r = jobs::list_fetch::run(&h.ctx, ActorId::new(oid), &od).await;
    let (capped, refresh, items): (bool, bool, i64) = sqlx::query_as(
        "SELECT capped, refresh_requested, (SELECT count(*) FROM list_items i WHERE i.list_id = l.id) FROM lists l WHERE id = $1",
    )
    .bind(ids[2])
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    c.check(
        "refresh_requested run clears capped and stores all 3 items",
        !capped && !refresh && items == 3,
        format!(
            "outcome {:?}, capped {capped}, refresh_requested {refresh}, items {items}",
            r.outcome
        ),
    );
    Ok(())
}

// ------------------------------------------------------------- check 5

async fn check_pending_timeout(h: &H, c: &mut Checks) -> Res<()> {
    let (o, x) = (did("pta", 1), did("ptb", 1));
    let rk = tid_now();
    h.put_repo(&o, vec![(Collection::List, rk.clone(), list_v("slow"))]);
    h.fh(
        &x,
        Collection::ListBlock,
        &tid_now(),
        lb_v(&list_uri(&o, &rk)),
    )
    .await?;
    let lid = h.list_id(&o, &rk).await?;
    // Clock injection: admitted more than pending_max_age (3 h) ago.
    h.exec(&format!(
        "UPDATE lists SET admitted_at = admitted_at - interval '4 hours' WHERE id = {lid}"
    ))
    .await?;
    let admitted: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT admitted_at FROM lists WHERE id = $1")
            .bind(lid)
            .fetch_one(h.pool())
            .await
            .map_err(e)?;
    let keys_before = h
        .i64(&format!(
            "SELECT count(*) FROM list_sched_keys WHERE list_id = {lid}"
        ))
        .await?;
    let unavailable_before = h
        .i64("SELECT count(*) FROM lists WHERE track_state = 4")
        .await?;
    let now = jobs::db_now(h.pool()).await.map_err(e)?;
    let n = jobs::list_phase1::pending_timeouts(&h.ctx, now)
        .await
        .map_err(e)?;
    let state = h.track(lid).await?;
    let admitted_after: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT admitted_at FROM lists WHERE id = $1")
            .bind(lid)
            .fetch_one(h.pool())
            .await
            .map_err(e)?;
    let job = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM list_jobs WHERE list_id = {lid})"
        ))
        .await?;
    let keys_after = h
        .i64(&format!(
            "SELECT count(*) FROM list_sched_keys WHERE list_id = {lid}"
        ))
        .await?;
    let unavailable_after = h
        .i64("SELECT count(*) FROM lists WHERE track_state = 4")
        .await?;
    c.check(
        "FT fires: pending → unavailable, counted in unavailableLists",
        n >= 1 && state == 4 && unavailable_after == unavailable_before + 1,
        format!("fired {n}, state {state}, unavailable {unavailable_before}→{unavailable_after}"),
    );
    c.check(
        "lane position kept: admitted_at, list_jobs row and scheduling keys unchanged",
        admitted == admitted_after && job && keys_before == keys_after && keys_before > 0,
        format!("admitted {admitted:?}→{admitted_after:?}, list_jobs {job}, sched keys {keys_before}→{keys_after}"),
    );
    Ok(())
}

// ------------------------------------------------------------- check 6

async fn check_fairness(h: &H, c: &mut Checks) -> Res<()> {
    // token:101 500 (100 high), token:102 300 (50 high), token:103 200 (none).
    let mut dids = Vec::new();
    let mut reqs = Vec::new();
    let mut pris: Vec<i16> = Vec::new();
    let mut meta: HashMap<String, (String, bool)> = HashMap::new();
    for i in 0..1000u64 {
        let (r, high) = match i {
            0..500 => ("token:101", i < 100),
            500..800 => ("token:102", i < 550),
            _ => ("token:103", false),
        };
        let d = did("fra", i);
        meta.insert(d.clone(), (r.to_owned(), high));
        dids.push(d);
        reqs.push(r.to_owned());
        pris.push(i16::from(high));
    }
    h.exec("DELETE FROM backfill_queue").await?;
    sqlx::query("INSERT INTO actors (did) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING")
        .bind(&dids)
        .execute(h.pool())
        .await
        .map_err(e)?;
    sqlx::query(
        "INSERT INTO backfill_queue (actor_id, kind, tier, priority, requester)
         SELECT a.id, 1, 1, x.p, x.r FROM unnest($1::text[], $2::text[], $3::int2[]) AS x(d, r, p)
         JOIN actors a ON a.did = x.d",
    )
    .bind(&dids)
    .bind(&reqs)
    .bind(&pris)
    .execute(h.pool())
    .await
    .map_err(e)?;
    h.set_cfg(|c| c.backfill.concurrency = 1);
    let sched = Scheduler::new_dry(h.ctx.clone());
    let (stop_tx, stop) = watch::channel(false);
    let task = tokio::spawn(sched.clone().run(stop));
    let start = Instant::now();
    loop {
        let left = h
            .i64("SELECT count(*) FROM backfill_queue WHERE tier = 1")
            .await?;
        if left == 0 || start.elapsed() > Duration::from_secs(180) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = stop_tx.send(true);
    let _ = task.await;
    h.set_cfg(|c| c.backfill.concurrency = 16);
    let order: Vec<String> = sqlx::query_scalar(
        "SELECT a.did FROM backfill_state b JOIN actors a ON a.id = b.actor_id
         WHERE a.did LIKE 'did:plc:fra%' ORDER BY b.backfilled_at",
    )
    .fetch_all(h.pool())
    .await
    .map_err(e)?;
    c.check(
        "every request picked exactly once",
        order.len() == 1000,
        format!("{} picks", order.len()),
    );
    // While all three requesters have work (the first 600 picks), equal
    // cost ⇒ equal shares.
    let mut share: BTreeMap<String, usize> = BTreeMap::new();
    for d in order.iter().take(600) {
        *share.entry(meta[d].0.clone()).or_default() += 1;
    }
    let even = share.values().all(|n| (*n as i64 - 200).abs() <= 3);
    c.check(
        "cost-based DRR: 200 ± 3 each of the first 600 picks",
        even && share.len() == 3,
        format!("{share:?}"),
    );
    // Within a requester: four high, then one normal.
    let seq = |r: &str, n: usize| -> Vec<bool> {
        order
            .iter()
            .filter(|d| meta[*d].0 == r)
            .take(n)
            .map(|d| meta[d].1)
            .collect()
    };
    let a = seq("token:101", 50);
    let pattern: Vec<bool> = (0..50).map(|i| i % 5 != 4).collect();
    let b = seq("token:102", 62);
    let b_high = b.iter().filter(|x| **x).count();
    c.check(
        "high before normal 4:1 within a requester (token:101: HHHHN×10; token:102: 50 high in its first 62)",
        a == pattern && b_high == 50,
        format!(
            "token:101 first 50: {}; token:102 high in first 62: {b_high}",
            a.iter().map(|x| if *x { 'H' } else { 'N' }).collect::<String>()
        ),
    );
    h.exec("DELETE FROM backfill_state WHERE actor_id IN (SELECT id FROM actors WHERE did LIKE 'did:plc:fra%')").await?;
    Ok(())
}

// ------------------------------------------------------------- check 7

async fn check_budget(h: &H, c: &mut Checks) -> Res<()> {
    let (k, adm) = (did("bga", 1), did("bgb", 1));
    let mut olds = HashMap::new();
    for (d, s) in [(&k, 60), (&adm, 70)] {
        // A stored block the PDS no longer has, then two new ones there.
        let old = tid_now();
        h.fh(d, Collection::Block, &old, block_v(&did("sub", s)))
            .await?;
        olds.insert(d.clone(), old);
        h.put_repo(
            d,
            vec![
                (Collection::Block, tid_now(), block_v(&did("sub", s + 1))),
                (Collection::Block, tid_now(), block_v(&did("sub", s + 2))),
            ],
        );
    }
    // Budget below current usage (≈105%, under the 115% ceiling).
    let bytes = farsight_storage::gates::measure_database_bytes(h.pool())
        .await
        .map_err(e)?;
    h.set_cfg(|c| c.storage.budget_bytes = bytes * 100 / 105);
    farsight_backfill::budget_monitor(&h.ctx).await;
    let g = h.ctx.gates.load();
    c.check(
        "budget monitor: usage ≥ 100% closes the budget gate (not the ceiling)",
        g.budget_refusing && !g.ceiling_refusing,
        format!("{g:?} at {bytes} bytes"),
    );
    let r = h.job(&k, 2, "system:firehose").await?;
    let kid = h.id(&k).await?;
    let keys: Vec<String> = sqlx::query_scalar("SELECT rkey FROM blocks WHERE author_id = $1")
        .bind(kid)
        .fetch_all(h.pool())
        .await
        .map_err(e)?;
    let refused = h
        .opt_i64(&format!(
            "SELECT cap_type::BIGINT FROM relist_debt WHERE actor_id = {kid} AND reason = 4"
        ))
        .await?;
    let row: (Option<i16>, bool) = sqlx::query_as(
        "SELECT last_outcome, clean_witness IS NULL FROM backfill_state WHERE actor_id = $1",
    )
    .bind(kid)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    c.check(
        "tier-2 job runs deletes-only: the stale block deleted, inserts skipped, refused debt, complete-with-debts (no clean point)",
        r.outcome == Outcome::CompleteWithDebts && keys.is_empty() && refused.is_some() && row == (Some(2), true),
        format!("outcome {:?}, blocks {keys:?}, refused debt cap_type {refused:?}, (last_outcome, no clean point) {row:?}", r.outcome),
    );
    let r = h.job(&adm, 1, "admin").await?;
    let aid = h.id(&adm).await?;
    let n = h
        .i64(&format!(
            "SELECT count(*) FROM blocks WHERE author_id = {aid}"
        ))
        .await?;
    let has_old = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM blocks WHERE author_id = {aid} AND rkey = '{}')",
            olds[&adm]
        ))
        .await?;
    let refused = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM relist_debt WHERE actor_id = {aid} AND reason = 4)"
        ))
        .await?;
    c.check(
        "admin-requested job runs normally under the budget gate: both inserts stored, stale block deleted, clean",
        r.outcome == Outcome::Clean && n == 2 && !has_old && !refused,
        format!("outcome {:?}, blocks {n}, stale present {has_old}, refused debt {refused}", r.outcome),
    );
    h.set_cfg(|c| c.storage.budget_bytes = 70_000_000_000);
    farsight_backfill::budget_monitor(&h.ctx).await;
    let g = h.ctx.gates.load();
    c.check(
        "gate reopens below 95%",
        !g.budget_refusing && !g.ceiling_refusing,
        format!("{g:?}"),
    );
    Ok(())
}

// ------------------------------------------------------------- check 8

async fn check_sweep(h: &H, c: &mut Checks) -> Res<()> {
    // 1000 canned DIDs over the three swept collections (overlapping, as
    // real repos are); 10 resolve to an unreachable PDS.
    let all: Vec<String> = (0..1000).map(|i| did("swp", i)).collect();
    let failing: HashSet<String> = (0..10).map(|i| did("swp", i * 7)).collect();
    {
        let mut wd = w(&h.world);
        wd.collections_supported = true;
        for (i, d) in all.iter().enumerate() {
            let k = if i < 700 {
                Collection::Block
            } else if i < 900 {
                Collection::ListBlock
            } else {
                Collection::List
            };
            wd.by_collection
                .entry(k.nsid().to_owned())
                .or_default()
                .insert(d.clone());
            if i % 10 == 3 && k == Collection::Block {
                // Also in listblock's directory (union de-duplication).
                wd.by_collection
                    .entry(Collection::ListBlock.nsid().to_owned())
                    .or_default()
                    .insert(d.clone());
            }
            if failing.contains(d) {
                wd.plc.insert(d.clone(), "http://127.0.0.1:1".into());
            } else {
                wd.repos.insert(
                    d.clone(),
                    Repo {
                        rev: tid_now(),
                        ..Repo::default()
                    },
                );
                wd.plc.insert(d.clone(), h.pds.clone());
            }
        }
    }
    let cap = 200i64;
    h.set_cfg(|c| {
        c.backfill.sweep.enabled = true;
        c.backfill.sweep.max_outstanding = cap as u64;
        c.backfill.concurrency = 32;
    });
    let sw = Arc::new(sweep::Sweep::default());
    // The first tick starts the cycle and enumerates one page before any
    // member runs: the page asks for exactly the room left.
    sweep::tick(&h.ctx, &sw).await.map_err(|e| e.to_string())?;
    let first_fill = h
        .i64("SELECT count(*) FROM cycle_outstanding o JOIN sweep_cycles c ON c.id = o.cycle_id WHERE c.kind = 1 AND c.completed_at IS NULL AND o.state = 1")
        .await?;
    let sched = Scheduler::new(h.ctx.clone());
    let (stop_tx, stop) = watch::channel(false);
    let task = tokio::spawn(sched.clone().run(stop));
    let start = Instant::now();
    let mut max_out = first_fill;
    let mut passed_failed = false;
    let mut cycle: Option<i64> = None;
    let mut pages_seen = HashSet::new();
    while start.elapsed() < Duration::from_secs(300) {
        sweep::tick(&h.ctx, &sw).await.map_err(|e| e.to_string())?;
        if cycle.is_none() {
            cycle = h
                .opt_i64("SELECT max(id) FROM sweep_cycles WHERE kind = 1")
                .await?;
        }
        if let Some(id) = cycle {
            let out = h
                .i64(&format!(
                    "SELECT count(*) FROM cycle_outstanding WHERE cycle_id = {id} AND state = 1"
                ))
                .await?;
            max_out = max_out.max(out);
            let cp: Option<String> =
                sqlx::query_scalar("SELECT checkpoint FROM sweep_cycles WHERE id = $1")
                    .bind(id)
                    .fetch_one(h.pool())
                    .await
                    .map_err(e)?;
            pages_seen.insert(cp.clone());
            // The checkpoint moved on while a failed member (first page)
            // is still outstanding, waiting for its retry.
            let failed_waiting = sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM cycle_outstanding WHERE cycle_id = $1 AND state = 1 AND did = ANY($2)",
            )
            .bind(id)
            .bind(failing.iter().cloned().collect::<Vec<_>>())
            .fetch_one(h.pool())
            .await
            .map_err(e)?;
            if failed_waiting > 0 && pages_seen.len() >= 3 {
                passed_failed = true;
            }
            let done: bool = h
                .bool(&format!(
                    "SELECT completed_at IS NOT NULL FROM sweep_cycles WHERE id = {id}"
                ))
                .await?;
            if done {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let _ = stop_tx.send(true);
    let _ = task.await;
    let id = cycle.ok_or("no sweep cycle started")?;
    let row: (String, bool, bool, bool, i64, i64) = sqlx::query_as(
        "SELECT c.source, c.enumerated_at IS NOT NULL, c.completed_at IS NOT NULL,
                c.effective_start >= f.first_applied_at, c.done, c.failed_terminal
         FROM sweep_cycles c, firehose_state f WHERE c.id = $1",
    )
    .bind(id)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    c.check(
        "effective_start ≥ first_applied_at; source relay_collections",
        row.3 && row.0 == "relay_collections",
        format!("{row:?}"),
    );
    c.check(
        format!("cycle_outstanding fills to the cap ({cap}) and never exceeds it"),
        first_fill == cap && max_out <= cap,
        format!(
            "first page filled {first_fill}; max outstanding observed while draining {max_out}"
        ),
    );
    c.check(
        "the checkpoint advanced past failed members while they waited for their retry",
        passed_failed,
        format!("{} distinct checkpoints observed", pages_seen.len()),
    );
    let described: HashSet<String> = w(&h.world)
        .hits
        .iter()
        .filter(|(m, _, _)| m == "describeRepo")
        .map(|(_, d, _)| d.clone())
        .collect();
    let covered = all
        .iter()
        .filter(|d| !failing.contains(*d))
        .all(|d| described.contains(d));
    let unreachable = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM relist_debt x JOIN actors a ON a.id = x.actor_id WHERE x.reason = 1 AND a.did = ANY($1)",
    )
    .bind(failing.iter().cloned().collect::<Vec<_>>())
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let leftover = h
        .i64(&format!(
            "SELECT count(*) FROM cycle_outstanding WHERE cycle_id = {id}"
        ))
        .await?;
    c.check(
        "drains: every reachable DID listed (describeRepo), the 10 unreachable terminal with unreachable debts, completed when all terminal",
        row.1 && row.2 && covered && row.5 == 10 && unreachable == 10 && leftover == 0,
        format!(
            "enumerated {}, completed {}, all reachable described {covered}, failed_terminal {}, unreachable debts {unreachable}, done {}, rows left {leftover}",
            row.1, row.2, row.5, row.4
        ),
    );
    h.set_cfg(|c| c.backfill.sweep.enabled = false);
    Ok(())
}

// ------------------------------------------------------------- check 9

async fn check_repair(h: &H, c: &mut Checks) -> Res<()> {
    let recent: Vec<String> = (0..5).map(|i| did("rra", i)).collect();
    let old: Vec<String> = (0..5).map(|i| did("rrb", i)).collect();
    let back = did("rrc", 1);
    let back_list = tid_now();
    {
        let mut wd = w(&h.world);
        wd.listed.clear();
        for d in &recent {
            wd.listed.push((d.clone(), tid_now(), true));
        }
        let stale = tid_at(Utc::now() - chrono::Duration::days(3));
        for d in &old {
            wd.listed.push((d.clone(), stale.clone(), true));
        }
        // Reactivated during the gap: active at the relay, old rev.
        wd.listed.push((back.clone(), stale.clone(), true));
    }
    for d in recent.iter().chain(&old) {
        h.put_repo(d, vec![]);
    }
    h.put_repo(
        &back,
        vec![(Collection::List, back_list.clone(), list_v("returning"))],
    );
    // Farsight holds `back` inactive with an unavailable list (OI).
    let lb_author = did("rrd", 1);
    h.fh(
        &lb_author,
        Collection::ListBlock,
        &tid_now(),
        lb_v(&list_uri(&back, &back_list)),
    )
    .await?;
    let back_lid = h.list_id(&back, &back_list).await?;
    let bid = h.id(&back).await?;
    let mut b = Batch::new(Origin::Firehose);
    b.events.push(RepoEvent::Account {
        did: Did::parse(&back).map_err(e)?,
        witness: Utc::now(),
        active: false,
        status: Some("deactivated".into()),
    });
    h.apply(&b).await?;
    // Phase 1 found the owner inactive: OI ⇒ unavailable.
    h.fire(back_lid, Event::OwnerInactive).await?;
    let state0 = h.track(back_lid).await?;
    // A closed gap and an open one.
    let closed = h
        .i64("INSERT INTO firehose_gaps (from_at, to_at, cause) VALUES (now() - interval '10 minutes', now() - interval '9 minutes', 1) RETURNING id")
        .await?;
    let open = h
        .i64("INSERT INTO firehose_gaps (from_at, to_at, cause) VALUES (now() - interval '1 minute', NULL, 1) RETURNING id")
        .await?;
    let hits_before: HashMap<String, usize> = recent
        .iter()
        .chain(&old)
        .chain([&back])
        .map(|d| (d.clone(), h.hits("describeRepo", d, "")))
        .collect();
    let sw = Arc::new(sweep::Sweep::default());
    let sched = Scheduler::new(h.ctx.clone());
    let (stop_tx, stop) = watch::channel(false);
    let task = tokio::spawn(sched.clone().run(stop));
    let start = Instant::now();
    let mut cycle = None;
    let mut resync_seen = false;
    while start.elapsed() < Duration::from_secs(120) {
        sweep::tick(&h.ctx, &sw).await.map_err(|e| e.to_string())?;
        cycle = h
            .opt_i64("SELECT max(id) FROM sweep_cycles WHERE kind = 2")
            .await?;
        resync_seen |= h
            .bool(&format!(
                "SELECT EXISTS (SELECT 1 FROM relist_debt WHERE actor_id = {bid} AND reason = 2)"
            ))
            .await?;
        if let Some(id) = cycle {
            if h.bool(&format!(
                "SELECT completed_at IS NOT NULL FROM sweep_cycles WHERE id = {id}"
            ))
            .await?
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = stop_tx.send(true);
    let _ = task.await;
    let id = cycle.ok_or("no repair cycle started")?;
    let (source, total, done): (String, Option<i64>, bool) = sqlx::query_as(
        "SELECT source, total_est, completed_at IS NOT NULL FROM sweep_cycles WHERE id = $1",
    )
    .bind(id)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let listed = |d: &String| h.hits("describeRepo", d, "") > hits_before[d];
    let recent_ok = recent.iter().all(listed);
    let old_skipped = !old.iter().any(listed);
    c.check(
        "repair candidates: rev time ≥ from − slack (5 recent) plus the reactivated repo; old revs skipped",
        source == "relay_repos" && total == Some(6) && recent_ok && old_skipped && listed(&back),
        format!("source {source}, enumerated {total:?}, recent listed {recent_ok}, old skipped {old_skipped}, reactivated listed {}", listed(&back)),
    );
    let state1 = h.track(back_lid).await?;
    let status = h
        .i64(&format!(
            "SELECT status::BIGINT FROM actors WHERE id = {bid}"
        ))
        .await?;
    c.check(
        "reactivation: resync debt added, OA on its unavailable list, account active again",
        state0 == 4 && state1 != 4 && resync_seen && status == 0,
        format!("list {state0}→{state1}, resync debt seen {resync_seen}, status {status}"),
    );
    let (healed_c, rc): (bool, Option<i64>) = sqlx::query_as(
        "SELECT healed_at IS NOT NULL, repair_cycle_id FROM firehose_gaps WHERE id = $1",
    )
    .bind(closed)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let healed_o = h
        .bool(&format!(
            "SELECT healed_at IS NOT NULL FROM firehose_gaps WHERE id = {open}"
        ))
        .await?;
    c.check(
        "on completion the closed gap gets healed_at (by this cycle); the open gap waits",
        done && healed_c && rc == Some(id) && !healed_o,
        format!("completed {done}, closed healed {healed_c} by {rc:?}, open healed {healed_o}"),
    );
    // A repair requested through admin.startRepair: the server inserts the
    // bare cycle row (this SQL mirrors farsight-api's start_repair_cycle);
    // the backfill adopts it (S_C, listRepos, gap claim) and heals.
    h.exec(&format!(
        "UPDATE firehose_gaps SET to_at = now() WHERE id = {open}"
    ))
    .await?;
    let from: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT from_at FROM firehose_gaps WHERE id = $1")
            .bind(open)
            .fetch_one(h.pool())
            .await
            .map_err(e)?;
    let manual = sqlx::query_scalar::<_, i64>(
        "INSERT INTO sweep_cycles (kind, source, collections, started_at, repair_from)
         VALUES (2, 'relay_repos', '{1,2,3,4}', now(), $1) RETURNING id",
    )
    .bind(from)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let sched = Scheduler::new(h.ctx.clone());
    let (stop_tx, stop) = watch::channel(false);
    let task = tokio::spawn(sched.clone().run(stop));
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(120) {
        sweep::tick(&h.ctx, &sw).await.map_err(|e| e.to_string())?;
        if h.bool(&format!(
            "SELECT completed_at IS NOT NULL FROM sweep_cycles WHERE id = {manual}"
        ))
        .await?
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = stop_tx.send(true);
    let _ = task.await;
    let (src, witness, done): (String, bool, bool) = sqlx::query_as(
        "SELECT source, effective_start_witness IS NOT NULL, completed_at IS NOT NULL FROM sweep_cycles WHERE id = $1",
    )
    .bind(manual)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    let (healed, by): (bool, Option<i64>) = sqlx::query_as(
        "SELECT healed_at IS NOT NULL, repair_cycle_id FROM firehose_gaps WHERE id = $1",
    )
    .bind(open)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    c.check(
        "an admin.startRepair cycle row is adopted (S_C set, listRepos) and heals the now-closed gap",
        src == "relay_repos" && witness && done && healed && by == Some(manual),
        format!("source {src}, S_C set {witness}, completed {done}, gap healed {healed} by {by:?}"),
    );
    Ok(())
}

// ------------------------------------------------------------ check 10

async fn debt(h: &H, d: &str, reason: i16, cap: Option<i16>, since: &str) -> Res<i64> {
    let id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO actors (did) VALUES ($1) ON CONFLICT (did) DO UPDATE SET did = EXCLUDED.did RETURNING id",
    )
    .bind(d)
    .fetch_one(h.pool())
    .await
    .map_err(e)?;
    sqlx::query(&format!(
        "INSERT INTO relist_debt (actor_id, reason, cap_type, since_witness) VALUES ($1, $2, $3, {since})"
    ))
    .bind(id)
    .bind(reason)
    .bind(cap)
    .execute(h.pool())
    .await
    .map_err(e)?;
    Ok(id)
}

async fn fed(h: &H, id: i64) -> Res<bool> {
    h.bool(&format!(
        "SELECT EXISTS (SELECT 1 FROM backfill_queue WHERE actor_id = {id} AND kind = 1 AND tier = 1 AND requester = 'system:resync')"
    ))
    .await
}

async fn check_feeder(h: &H, c: &mut Checks) -> Res<()> {
    let clock = "(SELECT max(witness_at) FROM firehose_clock)";
    let cases: Vec<(&str, i64, bool)> = {
        let mut v = Vec::new();
        // resync: eligible; covered by a later run's point ⇒ not.
        let id = debt(
            h,
            &did("fda", 1),
            2,
            None,
            &format!("{clock} - interval '1 minute'"),
        )
        .await?;
        v.push(("resync, no run since", id, true));
        let id = debt(
            h,
            &did("fda", 2),
            2,
            None,
            &format!("{clock} - interval '10 minutes'"),
        )
        .await?;
        h.exec(&format!("INSERT INTO backfill_state (actor_id, state, backfilled_at, backfilled_witness, last_outcome) VALUES ({id}, 3, now() - interval '2 hours', {clock}, 2)")).await?;
        v.push((
            "resync, a later complete-with-debts run covered it",
            id,
            false,
        ));
        // unreachable: retry due vs not yet.
        let id = debt(h, &did("fdb", 1), 1, None, clock).await?;
        h.exec(&format!("INSERT INTO backfill_state (actor_id, state, attempts, next_attempt_at) VALUES ({id}, 4, 3, now() - interval '1 minute')")).await?;
        v.push(("unreachable, retry due", id, true));
        let id = debt(h, &did("fdb", 2), 1, None, clock).await?;
        h.exec(&format!("INSERT INTO backfill_state (actor_id, state, attempts, next_attempt_at) VALUES ({id}, 4, 3, now() + interval '1 hour')")).await?;
        v.push(("unreachable, retry not due", id, false));
        // capped (per-author): under 90% vs at 95%.
        let id = debt(h, &did("fdc", 1), 3, Some(1), clock).await?;
        v.push(("capped blocks_per_author, author under 90%", id, true));
        let id = debt(h, &did("fdc", 2), 3, Some(1), clock).await?;
        let cap = Config::default().limits.blocks_per_author;
        h.exec(&format!(
            "UPDATE actors SET authored_blocks = {} WHERE id = {id}",
            cap * 95 / 100
        ))
        .await?;
        v.push((
            "capped blocks_per_author, author at 95% (hysteresis)",
            id,
            false,
        ));
        // capped (daily rate): once per UTC day.
        let id = debt(h, &did("fdd", 1), 3, Some(6), clock).await?;
        v.push(("capped intern rate, not run today", id, true));
        let id = debt(h, &did("fdd", 2), 3, Some(6), clock).await?;
        h.exec(&format!("INSERT INTO backfill_state (actor_id, state, backfilled_at, last_outcome) VALUES ({id}, 3, now(), 2)")).await?;
        v.push(("capped intern rate, already re-listed today", id, false));
        // refused: waits on its specific bucket bit.
        let id = debt(h, &did("fde", 1), 4, Some(8), clock).await?;
        v.push(("refused host_list_items, bucket open", id, true));
        let id = debt(h, &did("fde", 2), 4, Some(8), clock).await?;
        h.exec("INSERT INTO pds_hosts (host, cap_key, large) VALUES ('pds.masked.test', 'masked.test', false) ON CONFLICT DO NOTHING").await?;
        h.exec(&format!("UPDATE actors SET pds_host_id = (SELECT id FROM pds_hosts WHERE host = 'pds.masked.test') WHERE id = {id}")).await?;
        h.exec("INSERT INTO host_usage (bucket, capped_mask) VALUES ('d:masked.test', 2) ON CONFLICT (bucket) DO UPDATE SET capped_mask = 2").await?;
        v.push(("refused host_list_items, bucket still capped", id, false));
        v
    };
    // A list whose phase-1 pass found the system queue full: fed as a
    // list_fetch.
    let owner = did("fdl", 1);
    let oid = sqlx::query_scalar::<_, i64>("INSERT INTO actors (did) VALUES ($1) RETURNING id")
        .bind(&owner)
        .fetch_one(h.pool())
        .await
        .map_err(e)?;
    h.exec(&format!(
        "INSERT INTO lists (owner_id, rkey, record_state, purpose, name, listblock_count, track_state, admitted_at, admit_epoch, phase1_epoch)
         VALUES ({oid}, '{}', 1, 1, 'q', 1, 1, now(), 1, 1)",
        tid_now()
    ))
    .await?;
    let n = feeder::pass(&h.ctx).await.map_err(e)?;
    let mut wrong = Vec::new();
    for (what, id, want) in &cases {
        if fed(h, *id).await? != *want {
            wrong.push(format!(
                "{what}: expected {}",
                if *want { "fed" } else { "waiting" }
            ));
        }
    }
    let list_fed = h.bool(&format!("SELECT EXISTS (SELECT 1 FROM backfill_queue WHERE actor_id = {oid} AND kind = 2 AND requester = 'system:lists')")).await?;
    c.check(
        format!("one pass: each reason's eligible debts enqueued as system:resync re-lists, ineligible ones wait ({} cases)", cases.len()),
        wrong.is_empty(),
        if wrong.is_empty() { format!("{n} enqueued") } else { wrong.join("; ") },
    );
    c.check(
        "phase-1-passed list without a queue entry gets its list_fetch",
        list_fed,
        format!("{list_fed}"),
    );
    // No loop: a re-list that just ended complete-with-debts is not re-fed.
    let capped_ok = cases[4].1;
    h.exec(&format!(
        "DELETE FROM backfill_queue WHERE actor_id = {capped_ok}"
    ))
    .await?;
    h.exec(&format!("INSERT INTO backfill_state (actor_id, state, backfilled_at, backfilled_witness, last_outcome) VALUES ({capped_ok}, 3, now(), {clock}, 2) ON CONFLICT (actor_id) DO UPDATE SET backfilled_at = now(), last_outcome = 2")).await?;
    feeder::pass(&h.ctx).await.map_err(e)?;
    let refed = fed(h, capped_ok).await?;
    let mut still = Vec::new();
    for (what, id, want) in &cases {
        if !*want && fed(h, *id).await? {
            still.push(*what);
        }
    }
    c.check(
        "second pass: no re-feed of a debt whose re-list just ran; ineligible still waiting",
        !refed && still.is_empty(),
        format!("re-fed {refed}; wrongly fed {still:?}"),
    );
    // The fed resync re-list runs clean and clears the debt.
    let r_did = did("fda", 1);
    h.put_repo(
        &r_did,
        vec![(Collection::Block, tid_now(), block_v(&did("sub", 90)))],
    );
    h.exec(&format!(
        "DELETE FROM backfill_queue WHERE actor_id = {}",
        cases[0].1
    ))
    .await?;
    let r = h.job(&r_did, 1, "system:resync").await?;
    let left = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM relist_debt WHERE actor_id = {})",
            cases[0].1
        ))
        .await?;
    feeder::pass(&h.ctx).await.map_err(e)?;
    let again = fed(h, cases[0].1).await?;
    c.check(
        "the resync re-list runs clean, clears the debt, and nothing is fed again",
        r.outcome == Outcome::Clean && !left && !again,
        format!("outcome {:?}, debt left {left}, re-fed {again}", r.outcome),
    );
    // System queue full: debts stay counted, nothing is enqueued.
    let waiting = h
        .i64("SELECT count(*) FROM backfill_queue WHERE requester = 'system:resync'")
        .await?;
    h.set_cfg(|c| c.backfill.system_queue_cap = waiting as u64);
    let fresh = debt(h, &did("fdf", 1), 2, None, clock).await?;
    feeder::pass(&h.ctx).await.map_err(e)?;
    let fed_full = fed(h, fresh).await?;
    let kept = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM relist_debt WHERE actor_id = {fresh})"
        ))
        .await?;
    h.set_cfg(|c| c.backfill.system_queue_cap = Config::default().backfill.system_queue_cap);
    feeder::pass(&h.ctx).await.map_err(e)?;
    let fed_after = fed(h, fresh).await?;
    c.check(
        "at system_queue_cap the feeder enqueues nothing and the debt stays; fed once capacity frees",
        !fed_full && kept && fed_after,
        format!("fed at cap {fed_full}, debt kept {kept}, fed after {fed_after}"),
    );
    Ok(())
}

// ------------------------------------------------------- extra: discovery

async fn check_discovery(h: &H, c: &mut Checks) -> Res<()> {
    let x = did("dsx", 1);
    let (blocker, stale, owner, lbauthor) =
        (did("dsa", 1), did("dsb", 1), did("dsc", 1), did("dsd", 1));
    let (brk, srk, lrk, irk, lbrk) = (tid_now(), tid_now(), tid_now(), tid_now(), tid_now());
    let l_uri = list_uri(&owner, &lrk);
    h.put_repo(
        &blocker,
        vec![(Collection::Block, brk.clone(), block_v(&x))],
    );
    // The index still lists a block its author has since deleted.
    h.put_repo(&stale, vec![]);
    h.put_repo(
        &owner,
        vec![
            (Collection::List, lrk.clone(), list_v("names x")),
            (Collection::ListItem, irk.clone(), item_v(&x, &l_uri)),
        ],
    );
    h.put_repo(
        &lbauthor,
        vec![(Collection::ListBlock, lbrk.clone(), lb_v(&l_uri))],
    );
    {
        let mut wd = w(&h.world);
        let b = Collection::Block.nsid().to_owned();
        wd.backlinks
            .push((x.clone(), b.clone(), blocker.clone(), brk.clone()));
        wd.backlinks
            .push((x.clone(), b, stale.clone(), srk.clone()));
        wd.backlinks.push((
            x.clone(),
            Collection::ListItem.nsid().into(),
            owner.clone(),
            irk.clone(),
        ));
        wd.backlinks.push((
            l_uri.clone(),
            Collection::ListBlock.nsid().into(),
            lbauthor.clone(),
            lbrk.clone(),
        ));
    }
    let base = h.pds.clone();
    h.set_cfg(|c| c.backfill.backlinks.url = base);
    let r = jobs::discovery::run(
        &h.ctx,
        &Did::parse(&x).map_err(e)?,
        farsight_storage::codes::RequesterKey::Token(7),
    )
    .await;
    h.set_cfg(|c| c.backfill.backlinks.url = String::new());
    let xid = h.id(&x).await?;
    let row: Option<(i16, bool, i32)> = sqlx::query_as(
        "SELECT state, truncated, refs_found FROM discovery_state WHERE actor_id = $1",
    )
    .bind(xid)
    .fetch_optional(h.pool())
    .await
    .map_err(e)?;
    let blocks: Vec<String> = sqlx::query_scalar(
        "SELECT a.did FROM blocks b JOIN actors a ON a.id = b.author_id WHERE b.subject_id = $1 ORDER BY 1",
    )
    .bind(xid)
    .fetch_all(h.pool())
    .await
    .map_err(e)?;
    let lid = h.list_id(&owner, &lrk).await?;
    let item = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM list_items WHERE list_id = {lid})"
        ))
        .await?;
    let lb = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM list_blocks WHERE list_id = {lid})"
        ))
        .await?;
    let named = h
        .bool(&format!(
            "SELECT EXISTS (SELECT 1 FROM subject_lists WHERE actor_id = {xid} AND list_id = {lid})"
        ))
        .await?;
    let scopes = h
        .i64(&format!(
            "SELECT count(*) FROM subject_coverage WHERE actor_id = {xid}"
        ))
        .await?;
    c.check(
        "verified references applied (the block, the listblock), the stale index entry is not, the list naming X is recorded",
        blocks == vec![blocker.clone()] && lb && named,
        format!("outcome {:?}, blockers {blocks:?}, listblock {lb}, listitem stored {item}, subject_lists {named}", r.outcome),
    );
    c.check(
        "untruncated completion: discovery_state completed, subject coverage confirmed for both scopes",
        row.is_some_and(|(st, tr, n)| st == 3 && !tr && n == 4) && scopes == 2 && r.outcome == Outcome::Clean,
        format!("discovery_state {row:?}, subject_coverage rows {scopes}"),
    );
    Ok(())
}

// ------------------------------------------------------- hostile hosts

/// A PDS that answers every request with `429` and the largest
/// `Retry-After` a number can hold.
async fn start_hostile_pds() -> Res<String> {
    async fn refuse() -> axum::response::Response {
        use axum::response::IntoResponse;
        (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "18446744073709551615")],
            axum::Json(json!({"error": "RateLimitExceeded"})),
        )
            .into_response()
    }
    let app = axum::Router::new().fallback(refuse);
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(e)?;
    let addr = l.local_addr().map_err(e)?;
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    Ok(format!("http://{addr}"))
}

/// Queues a tier-1 repo job for `d`.
async fn queue_repo(h: &H, d: &str, requester: &str) -> Res<()> {
    sqlx::query("INSERT INTO actors (did) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(d)
        .execute(h.pool())
        .await
        .map_err(e)?;
    sqlx::query(
        "INSERT INTO backfill_queue (actor_id, kind, tier, priority, requester)
         SELECT id, 1, 1, 0, $2 FROM actors WHERE did = $1",
    )
    .bind(d)
    .bind(requester)
    .execute(h.pool())
    .await
    .map_err(e)?;
    Ok(())
}

async fn backfilled(h: &H, d: &str) -> Res<bool> {
    h.bool(&format!(
        "SELECT EXISTS (SELECT 1 FROM backfill_state b JOIN actors a ON a.id = b.actor_id
                        WHERE a.did = '{d}' AND b.last_outcome IN (1, 2))"
    ))
    .await
}

/// Polls `f` until it is true or `secs` passed.
async fn eventually<F, Fut>(secs: u64, f: F) -> Res<bool>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Res<bool>>,
{
    let start = Instant::now();
    loop {
        if f().await? {
            return Ok(true);
        }
        if start.elapsed() > Duration::from_secs(secs) {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn check_hostile(h: &H, c: &mut Checks) -> Res<()> {
    h.exec("DELETE FROM backfill_queue").await?;
    let hostile = start_hostile_pds().await?;
    let host = farsight_backfill::net::host_key(&url::Url::parse(&hostile).map_err(e)?);
    let (bad, good, boom) = (did("hsa", 1), did("hsb", 1), did("hsc", 1));
    w(&h.world).plc.insert(bad.clone(), hostile.clone());
    for d in [&good, &boom] {
        h.put_repo(
            d,
            vec![(Collection::Block, tid_now(), block_v(&did("sub", 9)))],
        );
    }
    let sched = Scheduler::new(h.ctx.clone());
    sched.panic_for(&boom, true);
    let (stop_tx, stop) = watch::channel(false);
    let task = tokio::spawn(sched.clone().run(stop));
    queue_repo(h, &bad, "token:108").await?;
    queue_repo(h, &boom, "token:108").await?;
    queue_repo(h, &good, "token:108").await?;

    // The hostile host's answer has been taken in once it is cooling.
    let cooled = eventually(30, || async {
        Ok(h.ctx.net.hosts.cooling(&host).is_some())
    })
    .await?;
    let cooling = h.ctx.net.hosts.cooling(&host);
    c.check(
        "a host answering 429 with Retry-After 18446744073709551615 is cooled down for at most an hour",
        cooled && cooling.is_some_and(|s| s <= 3600),
        format!("cooling for {cooling:?} s"),
    );
    let good_done = eventually(30, || backfilled(h, &good)).await?;
    // The panicking job was claimed (its queue row is gone) and ended.
    let claimed = eventually(30, || async {
        let queued = h
            .i64(&format!(
                "SELECT count(*) FROM backfill_queue q JOIN actors a ON a.id = q.actor_id WHERE a.did = '{boom}'"
            ))
            .await?;
        Ok(queued == 0)
    })
    .await?;
    // The job on the hostile host is retried while the scheduler runs:
    // end its retries, then nothing may be left in flight.
    h.exec(&format!(
        "DELETE FROM backfill_queue WHERE actor_id IN (SELECT id FROM actors WHERE did = '{bad}')"
    ))
    .await?;
    let idle = eventually(30, || async { Ok(sched.in_flight() == ([0, 0, 0], 0)) }).await?;
    c.check(
        "a job that panics, and a job refused by a hostile host, give back their worker and their in-flight marker; other jobs are served meanwhile",
        claimed && good_done && idle,
        format!(
            "panicking job claimed {claimed}; job on the healthy host done {good_done}; in flight at the end {:?}",
            sched.in_flight()
        ),
    );
    // The DID whose job panicked can be run again.
    sched.panic_for(&boom, false);
    h.exec(&format!("DELETE FROM job_leases WHERE did = '{boom}'"))
        .await?;
    queue_repo(h, &boom, "token:108").await?;
    let again = eventually(30, || backfilled(h, &boom)).await?;
    c.check(
        "the DID whose job panicked is dispatched again and completes",
        again,
        format!("in flight {:?}", sched.in_flight()),
    );
    let _ = stop_tx.send(true);
    let _ = task.await;
    h.exec(&format!(
        "DELETE FROM backfill_queue WHERE actor_id IN (SELECT id FROM actors WHERE did IN ('{bad}', '{boom}', '{good}'))"
    ))
    .await?;
    Ok(())
}

// -------------------------------------------- relay without the listing

/// Ends every open cycle and makes a new full cycle due.
async fn retire_cycles(h: &H) -> Res<()> {
    h.exec("DELETE FROM cycle_outstanding").await?;
    h.exec("UPDATE sweep_cycles SET completed_at = now() WHERE completed_at IS NULL")
        .await?;
    h.exec("UPDATE sweep_cycles SET started_at = started_at - interval '400 days' WHERE kind = 1")
        .await?;
    Ok(())
}

async fn open_full_cycle(h: &H) -> Res<Option<(i64, String, Option<String>)>> {
    sqlx::query_as(
        "SELECT id, source, checkpoint FROM sweep_cycles
         WHERE kind = 1 AND completed_at IS NULL ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(h.pool())
    .await
    .map_err(e)
}

async fn check_fallback(h: &H, c: &mut Checks) -> Res<()> {
    let listed: Vec<String> = (0..5).map(|i| did("fbk", i)).collect();
    {
        let mut wd = w(&h.world);
        wd.collections_supported = false;
        wd.listed = listed
            .iter()
            .map(|d| (d.clone(), tid_now(), true))
            .collect();
    }
    retire_cycles(h).await?;
    h.set_cfg(|c| {
        c.backfill.sweep.enabled = true;
        c.backfill.sweep.source = SweepSource::RelayCollections;
        c.backfill.sweep.full_every_days = 1;
        c.backfill.sweep.max_outstanding = 5000;
        c.backfill.repair.auto_start = false;
    });
    let sw = sweep::Sweep::default();
    // The relay answers the probe with 501 MethodNotImplemented.
    sweep::tick(&h.ctx, &sw).await.map_err(|e| e.to_string())?;
    let first = open_full_cycle(h).await?;
    let members = h
        .i64("SELECT count(*) FROM cycle_outstanding WHERE did LIKE 'did:plc:fbk%'")
        .await?;
    c.check(
        "a relay without listReposByCollection: the full cycle starts on relay_repos and enumerates listRepos",
        first.as_ref().is_some_and(|(_, s, _)| s == "relay_repos") && members == 5,
        format!("cycle {first:?}; {members} of 5 listed repos outstanding"),
    );

    // The relay gains the method: the next cycle is probed again, with
    // the same process state, and uses it.
    retire_cycles(h).await?;
    w(&h.world).collections_supported = true;
    sweep::tick(&h.ctx, &sw).await.map_err(|e| e.to_string())?;
    let second = open_full_cycle(h).await?;
    c.check(
        "the relay is probed before every full cycle: once it has the method, the next cycle is relay_collections again",
        second.as_ref().is_some_and(|(id, s, _)| {
            s == "relay_collections" && Some(*id) != first.as_ref().map(|f| f.0)
        }),
        format!("{second:?}"),
    );

    // The relay loses the method while the cycle enumerates.
    w(&h.world).collections_supported = false;
    h.exec("DELETE FROM cycle_outstanding WHERE did LIKE 'did:plc:fbk%'")
        .await?;
    sweep::tick(&h.ctx, &sw).await.map_err(|e| e.to_string())?;
    let switched = open_full_cycle(h).await?;
    sweep::tick(&h.ctx, &sw).await.map_err(|e| e.to_string())?;
    let after = open_full_cycle(h).await?;
    let members = h
        .i64("SELECT count(*) FROM cycle_outstanding WHERE did LIKE 'did:plc:fbk%'")
        .await?;
    c.check(
        "a cycle whose relay stops offering listReposByCollection continues with relay_repos from its start, in the same cycle",
        switched.as_ref().map(|s| (s.0, s.1.as_str(), s.2.is_none()))
            == second.as_ref().map(|s| (s.0, "relay_repos", true))
            && members == 5,
        format!("after the refused page {switched:?}; a tick later {after:?}, {members} of 5 listed repos outstanding"),
    );

    retire_cycles(h).await?;
    w(&h.world).collections_supported = true;
    h.set_cfg(|c| {
        c.backfill.sweep.enabled = false;
        c.backfill.sweep.full_every_days = 0;
        c.backfill.sweep.max_outstanding = 200;
        c.backfill.repair.auto_start = true;
    });
    Ok(())
}

// ------------------------------------------------------------ check 11

async fn check_live(pg: &Pg, c: &mut Checks) -> Res<()> {
    pg.create_db("live").await?;
    let dir = std::env::temp_dir().join(format!("farsight-stage4-live-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(e)?;
    let (web, metrics, bmetrics) = (free_port()?, free_port()?, free_port()?);
    let token = "stage4-admin-token";
    let token_hash: String = Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let cfg = format!(
        r#"[server]
hostname = "farsight.test"
contact = "mailto:ops@farsight.test"
bind = "127.0.0.1:{web}"

[storage]
database_url = "{}"
budget_bytes = 70000000000

[firehose]
urls = ["{LIVE_JETSTREAM}"]

[access]
reads = "public"

[auth]
admin_token_sha256 = "{token_hash}"

[metrics]
bind = "127.0.0.1:{metrics}"
backfill_bind = "127.0.0.1:{bmetrics}"

[backfill]
concurrency = 4

[backfill.sweep]
enabled = false
"#,
        pg.url("live")
    );
    std::fs::write(dir.join("config.toml"), cfg).map_err(e)?;
    // The backfill starts first: it must wait for the server's migrations.
    let mut backfill = Proc::start("farsight-backfill", &dir, &[])?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let waited = backfill.log().contains("waiting for the schema");
    let mut server = Proc::start("farsight", &dir, &[])?;
    let pool = pg.pool("live", 4).await?;
    let start = Instant::now();
    let http = reqwest::Client::new();
    while start.elapsed() < Duration::from_secs(LIVE_SECS) {
        tokio::time::sleep(Duration::from_secs(15)).await;
        if !server.running() || !backfill.running() {
            break;
        }
    }
    let s_alive = server.running();
    let b_alive = backfill.running();
    c.check(
        "server + backfill up for the whole window; backfill waited for the schema before starting work",
        s_alive && b_alive && waited,
        format!(
            "server alive {s_alive}, backfill alive {b_alive}, waited {waited}\nserver log: {}\nbackfill log: {}",
            server.log_tail(5),
            backfill.log_tail(5)
        ),
    );
    let st: Option<(Option<i16>, bool, bool)> = sqlx::query_as(
        "SELECT protocol, first_applied_at IS NOT NULL, applied_through > now() - interval '1 minute' FROM firehose_state WHERE id = 1",
    )
    .fetch_optional(&pool)
    .await
    .map_err(e)?;
    let sync_gaps: i64 = sqlx::query_scalar("SELECT count(*) FROM firehose_gaps WHERE cause = 4")
        .fetch_one(&pool)
        .await
        .map_err(e)?;
    c.check(
        "firehose_state.protocol = 2, applying, no sync_unavailable gap",
        st == Some((Some(2), true, true)) && sync_gaps == 0,
        format!("state {st:?}, sync_unavailable gaps {sync_gaps}"),
    );
    // #sync: observed if a system:resync re-list was queued by ingest
    // during the window (account reactivations also produce these, so the
    // synthetic event below is what the check relies on).
    let observed: i64 = sqlx::query_scalar("SELECT count(*) FROM relist_debt WHERE reason = 2")
        .fetch_one(&pool)
        .await
        .map_err(e)?;
    let sd = did("syn", 1);
    let limits = farsight_storage::keys::Limits::defaults();
    let sink = farsight_storage::counters::CounterSink::new(3);
    let mut b = Batch::new(Origin::Firehose);
    b.events.push(RepoEvent::Sync {
        did: Did::parse(&sd).map_err(e)?,
        witness: Utc::now(),
    });
    apply::apply(
        &pool,
        &ApplyCtx {
            limits: &limits,
            gates: Default::default(),
            counters: &sink,
        },
        &b,
    )
    .await
    .map_err(e)?;
    let (debt, queued): (bool, bool) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM relist_debt d JOIN actors a ON a.id = d.actor_id WHERE a.did = $1 AND d.reason = 2),
                EXISTS (SELECT 1 FROM backfill_queue q JOIN actors a ON a.id = q.actor_id WHERE a.did = $1 AND q.requester = 'system:resync' AND q.tier = 1)",
    )
    .bind(&sd)
    .fetch_one(&pool)
    .await
    .map_err(e)?;
    c.check(
        "#sync path: a (synthesized) #sync adds a resync debt and a tier-1 system:resync re-list",
        debt && queued,
        format!("debt {debt}, queued {queued}; resync debts seen during the live window: {observed} (not attributable to #sync alone)"),
    );
    let outcomes: Vec<(Option<i16>, i64)> =
        sqlx::query_as("SELECT last_outcome, count(*) FROM backfill_state GROUP BY 1 ORDER BY 1")
            .fetch_all(&pool)
            .await
            .map_err(e)?;
    let done: i64 = outcomes
        .iter()
        .filter(|(o, _)| matches!(o, Some(1..=3)))
        .map(|(_, n)| n)
        .sum();
    c.check(
        "the backfill process ran real repo jobs against live PDSes (tier-2 active authors)",
        done > 0,
        format!("backfill_state by last_outcome: {outcomes:?}"),
    );
    let m = http
        .get(format!("http://127.0.0.1:{bmetrics}/metrics"))
        .send()
        .await
        .map_err(e)?
        .text()
        .await
        .map_err(e)?;
    let have: Vec<&str> = farsight_backfill::metrics::ALL
        .iter()
        .copied()
        .filter(|n| m.contains(n))
        .collect();
    c.check(
        "backfill metrics endpoint serves the backfill series",
        m.contains("farsight_backfill_queue_depth") && m.contains("farsight_backfill_repos_total"),
        format!("present: {have:?}"),
    );
    let stats = http
        .get(format!(
            "http://127.0.0.1:{web}/xrpc/app.nearhorizon.farsight.query.getStats"
        ))
        .send()
        .await
        .map_err(e)?
        .json::<Value>()
        .await
        .map_err(e)?;
    let reasons = stats
        .pointer("/freshness/coverage/reasons")
        .cloned()
        .unwrap_or(Value::Null);
    let blocked: Vec<&str> = [
        "sync_events_unavailable",
        "firehose_gap",
        "firehose_disconnected",
    ]
    .into_iter()
    .filter(|r| reasons.as_array().is_some_and(|a| a.iter().any(|x| x == r)))
    .collect();
    c.check(
        "coverage only waits on the sweep (complete reachable once a sweep completes): no sync/gap/disconnect reasons",
        blocked.is_empty() && reasons.is_array(),
        format!("reasons {reasons}"),
    );
    server.stop();
    backfill.stop();
    Ok(())
}
