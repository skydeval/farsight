//! `farsight-ingest-harness`: integration tests of ingest — real
//! Jetstream into a real Postgres through `farsight-ingest` and
//! `farsight-storage::apply`. Built only with `--features harness`.
//!
//! ```text
//! farsight-ingest-harness --mode a [--minutes 15]
//! farsight-ingest-harness --mode b [--minutes 60] [--check-every 15] [--kill-every 20]
//!                         [--pg-restart-every 60]
//! common: [--jetstream wss://…]… [--database-url URL] [--metrics 127.0.0.1:9464] [--keep]
//! ```
//!
//! `--mode a` is a bounded conformance run; `--mode b` is a soak with
//! failure injection. Both compare against an **independent** reference
//! connection and an independent LWW model of the received events, and
//! read every invariant from the stored rows.

mod model;
mod support;

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use farsight_core::Config;
use farsight_ingest::frame::{Body, InEvent, Protocol};
use farsight_ingest::metrics as im;
use farsight_ingest::resume::Cursor;
use farsight_ingest::{Control, Ingest, IngestConfig, IngestHandle, conn};
use farsight_storage::codes::{DebtReason, GapCause};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::mpsc;

use support::{Checks, Pg, Scrape, Verdict, scrape, sum_of, tag};

struct Args {
    mode: char,
    minutes: u64,
    check_every: u64,
    kill_every: u64,
    pg_restart_every: u64,
    jetstream: Vec<String>,
    database_url: Option<String>,
    metrics: String,
    keep: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        mode: 'a',
        minutes: 0,
        check_every: 15,
        kill_every: 20,
        pg_restart_every: 60,
        jetstream: Vec::new(),
        database_url: None,
        metrics: "127.0.0.1:9464".into(),
        keep: false,
    };
    let mut it = std::env::args().skip(1);
    let num = |v: Option<String>, f: &str| -> Result<u64, String> {
        v.ok_or(format!("{f} needs a value"))?
            .parse()
            .map_err(|_| format!("{f}: not a number"))
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--mode" => {
                a.mode = match it.next().as_deref() {
                    Some("a") | Some("A") => 'a',
                    Some("b") | Some("B") => 'b',
                    Some("p") | Some("P") => 'p',
                    _ => return Err("--mode a|b".into()),
                }
            }
            "--minutes" => a.minutes = num(it.next(), "--minutes")?,
            "--check-every" => a.check_every = num(it.next(), "--check-every")?,
            "--kill-every" => a.kill_every = num(it.next(), "--kill-every")?,
            "--pg-restart-every" => a.pg_restart_every = num(it.next(), "--pg-restart-every")?,
            "--jetstream" => a
                .jetstream
                .push(it.next().ok_or("--jetstream needs a URL")?),
            "--database-url" => {
                a.database_url = Some(it.next().ok_or("--database-url needs a URL")?)
            }
            "--metrics" => a.metrics = it.next().ok_or("--metrics needs host:port")?,
            "--keep" => a.keep = true,
            "-h" | "--help" => {
                println!("see the module docs in src/bin/ingest_harness/main.rs");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    if a.minutes == 0 {
        a.minutes = if a.mode == 'a' { 15 } else { 60 };
    }
    Ok(a)
}

// ------------------------------------------------------------- reference

/// An independent connection to the same instance, live tail, never
/// interrupted by the harness: what "every event" means for loss checks.
struct Reference {
    events: Arc<Mutex<Vec<InEvent>>>,
    /// Continuous intervals (witness µs) the reference saw end to end.
    intervals: Arc<Mutex<Vec<(i64, i64)>>>,
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    protocol: Arc<Mutex<Option<Protocol>>>,
}

fn start_reference(url: String) -> Reference {
    let events = Arc::new(Mutex::new(Vec::new()));
    let intervals = Arc::new(Mutex::new(Vec::new()));
    let protocol = Arc::new(Mutex::new(None));
    let (stop, mut stop_rx) = tokio::sync::watch::channel(false);
    let (ev2, iv2, p2) = (events.clone(), intervals.clone(), protocol.clone());
    let task = tokio::spawn(async move {
        while !*stop_rx.borrow() {
            let mut s = match conn::connect(&url, Protocol::V2, Cursor::Live, true).await {
                Ok(s) => s,
                Err(conn::ConnectError::NotOffered(_)) => {
                    match conn::connect(&url, Protocol::V1, Cursor::Live, true).await {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("reference connect failed: {e}");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("reference connect failed: {e}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            *p2.lock().unwrap() = Some(s.protocol);
            let mut first: Option<i64> = None;
            let mut last: Option<i64> = None;
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    f = tokio::time::timeout(Duration::from_secs(60), s.next_frame()) => match f {
                        Ok(Some(Ok(farsight_ingest::frame::Frame::Event(ev)))) => {
                            first.get_or_insert(ev.witness_us);
                            last = Some(ev.witness_us);
                            ev2.lock().unwrap().push(ev);
                        }
                        Ok(Some(Ok(_))) => {}
                        _ => break,
                    }
                }
            }
            if let (Some(f), Some(l)) = (first, last) {
                iv2.lock().unwrap().push((f, l));
            }
            s.close().await;
        }
    });
    Reference {
        events,
        intervals,
        stop,
        task,
        protocol,
    }
}

impl Reference {
    async fn stop(self) -> (Vec<InEvent>, Vec<(i64, i64)>, Option<Protocol>) {
        let _ = self.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), self.task).await;
        let ev = std::mem::take(&mut *self.events.lock().unwrap());
        let iv = self.intervals.lock().unwrap().clone();
        let p = *self.protocol.lock().unwrap();
        (ev, iv, p)
    }
}

// ----------------------------------------------------- NOTIFY / cursor log

#[derive(Debug, Clone)]
struct StateSample {
    source_url: Option<String>,
    cursor_seq: Option<i64>,
    cursor_us: Option<i64>,
    applied_us: Option<i64>,
}

struct Watcher {
    notifications: Arc<Mutex<u64>>,
    samples: Arc<Mutex<Vec<StateSample>>>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_watcher(pool: PgPool) -> Result<Watcher, sqlx::Error> {
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen("farsight_coverage").await?;
    let notifications = Arc::new(Mutex::new(0u64));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let (n2, s2) = (notifications.clone(), samples.clone());
    let task = tokio::spawn(async move {
        loop {
            match listener.recv().await {
                Ok(_) => {
                    *n2.lock().unwrap() += 1;
                    if let Ok(st) = farsight_storage::firehose::read_state(&pool).await {
                        s2.lock().unwrap().push(StateSample {
                            source_url: st.source_url,
                            cursor_seq: st.cursor_seq,
                            cursor_us: st.cursor_us,
                            applied_us: st.applied_through.map(|a| a.timestamp_micros()),
                        });
                    }
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    });
    Ok(Watcher {
        notifications,
        samples,
        task,
    })
}

// ----------------------------------------------------------- shared checks

/// Persisted cursor and applied-through never go backwards (per source).
fn check_monotonic(c: &mut Checks, samples: &[StateSample], label: &str) {
    let mut bad = Vec::new();
    for w in samples.windows(2) {
        let (a, b) = (&w[0], &w[1]);
        if a.source_url != b.source_url {
            continue;
        }
        if let (Some(x), Some(y)) = (a.cursor_seq, b.cursor_seq) {
            if y < x {
                bad.push(format!("seq {x}→{y}"));
            }
        }
        if let (Some(x), Some(y)) = (a.cursor_us, b.cursor_us) {
            if y < x {
                bad.push(format!("cursor_us {x}→{y}"));
            }
        }
        if let (Some(x), Some(y)) = (a.applied_us, b.applied_us) {
            if y < x {
                bad.push(format!("applied {x}→{y}"));
            }
        }
    }
    c.check(
        format!("{label}: persisted cursor and applied_through never decrease"),
        bad.is_empty() && samples.len() > 1,
        format!(
            "{} samples; violations {:?}",
            samples.len(),
            bad.iter().take(5).collect::<Vec<_>>()
        ),
    );
}

async fn clock_monotonic(pool: &PgPool, since_us: i64) -> Result<(i64, i64), sqlx::Error> {
    sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE witness_at < prev) FROM (
           SELECT witness_at, lag(witness_at) OVER (ORDER BY server_at) AS prev
           FROM firehose_clock WHERE server_at >= to_timestamp($1::float8 / 1e6)) x",
    )
    .bind(since_us as f64)
    .fetch_one(pool)
    .await
}

const CONSISTENCY: [(&str, &str); 7] = [
    (
        "listblock_count = counted rows",
        "SELECT count(*) FROM lists l WHERE l.listblock_count <>
           (SELECT count(*) FROM list_blocks b WHERE b.list_id = l.id AND b.counted)",
    ),
    (
        "item_count = item rows",
        "SELECT count(*) FROM lists l WHERE l.item_count <>
           (SELECT count(*) FROM list_items i WHERE i.list_id = l.id)",
    ),
    (
        "authored_blocks = block rows",
        "SELECT count(*) FROM actors a WHERE a.authored_blocks <>
           (SELECT count(*) FROM blocks b WHERE b.author_id = a.id)",
    ),
    (
        "authored_listblocks / fetch_triggers = rows",
        "SELECT count(*) FROM actors a WHERE a.authored_listblocks <>
           (SELECT count(*) FROM list_blocks b WHERE b.author_id = a.id)
           OR a.fetch_triggers <>
           (SELECT count(*) FROM list_blocks b WHERE b.author_id = a.id AND b.counted)",
    ),
    (
        "owned_items = item rows",
        "SELECT count(*) FROM actors a WHERE a.owned_items <>
           (SELECT count(*) FROM list_items i WHERE i.owner_id = a.id)",
    ),
    (
        "pending/ready/unavailable only while count > 0",
        "SELECT count(*) FROM lists WHERE track_state IN (1, 2, 4) AND listblock_count = 0",
    ),
    (
        "no stored row at or below its tombstone's rev (LWW)",
        "SELECT (SELECT count(*) FROM blocks b JOIN tombstones t ON t.collection = 1
                   AND t.author_id = b.author_id AND t.rkey = b.rkey WHERE b.rev <= t.rev)
              + (SELECT count(*) FROM list_blocks b JOIN tombstones t ON t.collection = 2
                   AND t.author_id = b.author_id AND t.rkey = b.rkey WHERE b.rev <= t.rev)
              + (SELECT count(*) FROM list_items b JOIN tombstones t ON t.collection = 4
                   AND t.author_id = b.owner_id AND t.rkey = b.rkey WHERE b.rev <= t.rev)
              + (SELECT count(*) FROM lists b JOIN tombstones t ON t.collection = 3
                   AND t.author_id = b.owner_id AND t.rkey = b.rkey
                   WHERE b.record_state = 1 AND b.rev <= t.rev)",
    ),
];

async fn consistency(pool: &PgPool, c: &mut Checks, label: &str) -> Result<(), sqlx::Error> {
    for (what, sql) in CONSISTENCY {
        let n: i64 = sqlx::query_scalar(sql).fetch_one(pool).await?;
        c.check(
            format!("{label}: {what}"),
            n == 0,
            format!("{n} violations"),
        );
    }
    Ok(())
}

/// No-loss: every reference event inside the overlap of the reference's
/// continuous intervals and ingest's own received range (minus a margin
/// at each edge) was also received by ingest.
fn check_no_loss(
    c: &mut Checks,
    label: &str,
    tapped: &[InEvent],
    reference: &[InEvent],
    intervals: &[(i64, i64)],
) {
    let margin = 30_000_000;
    // Ingest's own coverage: from its first live event to its last.
    let tmin = tapped
        .iter()
        .map(|e| e.witness_us)
        .min()
        .unwrap_or(i64::MAX);
    let tmax = tapped
        .iter()
        .map(|e| e.witness_us)
        .max()
        .unwrap_or(i64::MIN);
    let got: HashSet<String> = tapped.iter().filter_map(model::event_key).collect();
    let mut compared = 0usize;
    let mut missing = Vec::new();
    for ev in reference {
        let inside = intervals.iter().any(|(a, b)| {
            ev.witness_us >= (*a).max(tmin) + margin && ev.witness_us <= (*b).min(tmax) - margin
        });
        if !inside {
            continue;
        }
        let Some(k) = model::event_key(ev) else {
            continue;
        };
        compared += 1;
        if !got.contains(&k) {
            missing.push(format!("{k} @{}", ev.witness_us));
        }
    }
    if !missing.is_empty() {
        // Missing events per minute of witness time, for diagnosis.
        let mut per_min: BTreeMap<i64, usize> = BTreeMap::new();
        for m in &missing {
            if let Some(us) = m.rsplit('@').next().and_then(|x| x.parse::<i64>().ok()) {
                *per_min.entry(us / 60_000_000).or_insert(0) += 1;
            }
        }
        let fmt: Vec<String> = per_min
            .iter()
            .map(|(m, n)| {
                let t = chrono::DateTime::<Utc>::from_timestamp(m * 60, 0).unwrap_or_default();
                format!("{}:{n}", t.format("%H:%M"))
            })
            .collect();
        println!("  {label}: missing per minute (UTC): {}", fmt.join(" "));
    }
    if compared == 0 {
        c.unverified(
            format!("{label}: no event lost relative to the reference connection"),
            "no overlap to compare",
        );
    } else {
        c.check(
            format!("{label}: no event lost relative to the reference connection"),
            missing.is_empty(),
            format!(
                "{compared} reference events compared in the overlap [{tmin}+30s, {tmax}-30s]; {} missing {:?}",
                missing.len(),
                missing.iter().take(3).collect::<Vec<_>>()
            ),
        );
    }
}

async fn check_model(
    pool: &PgPool,
    c: &mut Checks,
    label: &str,
    tapped: &[InEvent],
) -> Result<(), sqlx::Error> {
    let mut m = model::Model::default();
    m.apply_events(tapped);
    let d = model::compare(pool, &m).await?;
    c.check(
        format!("{label}: stored rows equal the LWW replay of every received event (no loss, no duplicates)"),
        d.mismatches.is_empty() && d.compared > 0,
        format!(
            "{} commits replayed, {} keys compared ({} excluded: purged accounts), {} mismatches {:?}",
            m.commits,
            d.compared,
            d.excluded_purged,
            d.mismatches.len(),
            d.mismatches.iter().filter(|s| !s.is_empty()).take(3).collect::<Vec<_>>()
        ),
    );
    Ok(())
}

fn check_metrics(c: &mut Checks, early: &Scrape, late: &Scrape, run_secs: f64) {
    let missing: Vec<&str> = im::ALL
        .iter()
        .copied()
        .filter(|n| {
            !late.keys().any(|k| {
                support::series_name(k) == *n
                    || support::series_name(k).starts_with(&format!("{n}_"))
            })
        })
        .collect();
    c.check(
        "metrics: all ingest metrics exposed",
        missing.is_empty(),
        format!("missing {missing:?}"),
    );
    let mut regress = Vec::new();
    for (k, v) in early {
        let n = support::series_name(k);
        let counter = n.ends_with("_total")
            || n.ends_with("_count")
            || n.ends_with("_sum")
            || n.ends_with("_bucket");
        if counter {
            if let Some(v2) = late.get(k) {
                if v2 < v {
                    regress.push(format!("{k}: {v} → {v2}"));
                }
            }
        }
    }
    c.check(
        "metrics: counters monotonic between scrapes",
        regress.is_empty(),
        format!("{regress:?}"),
    );
    let nan: Vec<&String> = late
        .iter()
        .filter(|(_, v)| v.is_nan())
        .map(|(k, _)| k)
        .collect();
    c.check("metrics: no NaN values", nan.is_empty(), format!("{nan:?}"));
    let lag = late.get(im::LAG).copied().unwrap_or(f64::NAN);
    c.check(
        "metrics: lag gauge non-negative and bounded by the run",
        lag >= 0.0 && lag <= run_secs + 60.0,
        format!("{lag:.2} s (run {run_secs:.0} s)"),
    );
    let events = sum_of(late, im::EVENTS).unwrap_or(0.0);
    let batches = late
        .get(&format!("{}_count", im::BATCH_SECONDS))
        .copied()
        .unwrap_or(0.0);
    c.check(
        "metrics: events and batches non-degenerate",
        events > 0.0 && batches > 0.0,
        format!("events_total {events}, batch_seconds_count {batches}"),
    );
}

// ------------------------------------------------------------------- runs

struct Run {
    pool: PgPool,
    ingest: Option<IngestHandle>,
    tapped: Arc<Mutex<Vec<InEvent>>>,
    tap_task: tokio::task::JoinHandle<()>,
    watcher: Watcher,
    started: Instant,
    start_us: i64,
}

async fn start_run(pg: &Pg, cfg: &Config) -> Result<Run, String> {
    let pool = pg.pool(8).await.map_err(|e| e.to_string())?;
    farsight_storage::migrate(&pool)
        .await
        .map_err(|e| e.to_string())?;
    let ingest_pool = pg
        .pool(farsight_ingest::POOL_SIZE)
        .await
        .map_err(|e| e.to_string())?;
    let watcher = start_watcher(pool.clone())
        .await
        .map_err(|e| e.to_string())?;
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let tapped = Arc::new(Mutex::new(Vec::new()));
    let t2 = tapped.clone();
    let tap_task = tokio::spawn(async move {
        while let Some(ev) = tap_rx.recv().await {
            t2.lock().unwrap().push(ev);
        }
    });
    let mut icfg = IngestConfig::from_config(cfg);
    icfg.tap = Some(tap_tx);
    let start_us = Utc::now().timestamp_micros();
    let ingest = Ingest::start(icfg, ingest_pool).await?;
    Ok(Run {
        pool,
        ingest: Some(ingest),
        tapped,
        tap_task,
        watcher,
        started: Instant::now(),
        start_us,
    })
}

/// Waits for a session newer than `sessions`, then for two batches after it
/// (batches alone can still come from a queued backlog before the reconnect).
async fn wait_reconnect(ingest: &IngestHandle, sessions: u64, timeout: Duration) -> bool {
    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        let s = ingest.stats.snapshot();
        if s.sessions > sessions {
            let left = timeout.saturating_sub(t0.elapsed());
            return wait_batches(ingest, s.batches + 2, left).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

async fn wait_batches(ingest: &IngestHandle, at_least: u64, timeout: Duration) -> bool {
    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        if ingest.stats.snapshot().batches >= at_least {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

async fn mode_a(pg: &Pg, cfg: &Config, args: &Args, c: &mut Checks) -> Result<(), String> {
    let url = cfg.firehose.urls[0].clone();
    let reference = start_reference(url.clone());
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut run = start_run(pg, cfg).await?;
    let total = Duration::from_secs(args.minutes * 60);
    let ingest = run.ingest.as_ref().expect("running");
    if !wait_batches(ingest, 1, Duration::from_secs(120)).await {
        return Err("no batch committed within 2 minutes".into());
    }

    // Halfway: kill the websocket (same-instance reconnect).
    tokio::time::sleep(total / 2).await;
    let before = ingest.stats.snapshot();
    println!(
        "  killing the websocket (sessions {}, batches {})",
        before.sessions, before.batches
    );
    // Scrape while the first session is up (check 6 reads its gauge).
    let early = scrape(&args.metrics).await?;
    ingest
        .control
        .send(Control::KillSocket)
        .await
        .map_err(|e| e.to_string())?;
    tokio::time::sleep(total / 2).await;
    let after = ingest.stats.snapshot();
    let late = scrape(&args.metrics).await?;
    let ingest = run.ingest.take().expect("running");
    let final_stats = ingest.stats.snapshot();
    let status = ingest.status().await.map_err(|e| e.to_string())?;
    ingest.shutdown().await;
    let (ref_events, ref_intervals, ref_protocol) = reference.stop().await;
    run.tap_task.abort();
    run.watcher.task.abort();
    let tapped = std::mem::take(&mut *run.tapped.lock().unwrap());
    let run_secs = run.started.elapsed().as_secs_f64();

    println!("\n== conformance checks ==");
    // 1, 2: monotonic cursor and applied_through.
    let samples = run.watcher.samples.lock().unwrap().clone();
    check_monotonic(c, &samples, "1–2");
    let (rows, back) = clock_monotonic(&run.pool, run.start_us)
        .await
        .map_err(|e| e.to_string())?;
    c.check(
        "2: applied_through (per clock row) non-decreasing",
        back == 0 && rows > 0,
        format!("{rows} rows, {back} decreases"),
    );
    // 3: reconnect without loss or duplicates.
    c.check(
        "3: same-instance reconnect happened",
        after.sessions > before.sessions,
        format!("sessions {} → {}", before.sessions, after.sessions),
    );
    let gaps = farsight_storage::firehose::all_gaps(&run.pool)
        .await
        .map_err(|e| e.to_string())?;
    let resume_gaps: Vec<_> = gaps
        .iter()
        .filter(|g| g.cause != GapCause::SyncUnavailable)
        .collect();
    c.check(
        "3: no resume gap after a clean same-instance reconnect",
        resume_gaps.is_empty(),
        format!("{resume_gaps:?}"),
    );
    check_no_loss(c, "3", &tapped, &ref_events, &ref_intervals);
    check_model(&run.pool, c, "3", &tapped)
        .await
        .map_err(|e| e.to_string())?;
    consistency(&run.pool, c, "3 (stored-state invariants)")
        .await
        .map_err(|e| e.to_string())?;
    // 4: one clock row per batch.
    let clock_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM firehose_clock")
        .fetch_one(&run.pool)
        .await
        .map_err(|e| e.to_string())?;
    c.check(
        "4: firehose_clock rows = batch commits ± 1",
        (clock_rows - final_stats.batches as i64).abs() <= 1,
        format!("{clock_rows} rows, {} batches", final_stats.batches),
    );
    // 5: NOTIFY per batch commit.
    let notes = *run.watcher.notifications.lock().unwrap();
    c.check(
        "5: NOTIFY farsight_coverage received for every batch commit",
        notes >= final_stats.batches,
        format!("{notes} notifications, {} batches", final_stats.batches),
    );
    // 6: protocol reported truthfully.
    let st = farsight_storage::firehose::read_state(&run.pool)
        .await
        .map_err(|e| e.to_string())?;
    let negotiated = final_stats.current.as_ref().map(|(_, p)| *p);
    let stored = st.protocol.map(|p| match p {
        farsight_storage::codes::Protocol::V1 => Protocol::V1,
        farsight_storage::codes::Protocol::V2 => Protocol::V2,
    });
    let metric_on = negotiated.and_then(|p| {
        early
            .get(&format!("{}{{protocol=\"{}\"}}", im::CONNECTED, p.label()))
            .copied()
    });
    c.check(
        "6: firehose_state.protocol = negotiated protocol = metric label",
        negotiated.is_some() && stored == negotiated && metric_on == Some(1.0) && ref_protocol == negotiated,
        format!("negotiated {negotiated:?}, stored {stored:?}, metric {metric_on:?}, reference {ref_protocol:?}"),
    );
    // 7: metrics.
    check_metrics(c, &early, &late, run_secs);
    // Dashboard fields produced from firehose_state and the open-gap set.
    c.check(
        "dashboard fields: connected, protocol, lag, source lag, open gaps",
        status.connected
            && status.protocol == negotiated
            && status.lag_seconds.is_some_and(|l| (0.0..60.0).contains(&l))
            && status.source_lag_seconds.is_some_and(|l| l >= 0.0)
            && status.open_gaps == gaps.iter().filter(|g| g.healed_witness.is_none()).count(),
        format!("{status:?}"),
    );
    summary(
        &final_stats,
        &late,
        run_secs,
        &gaps,
        ref_events.len(),
        tapped.len(),
    );
    Ok(())
}

fn summary(
    s: &farsight_ingest::StatsSnapshot,
    m: &Scrape,
    secs: f64,
    gaps: &[farsight_storage::firehose::Gap],
    ref_events: usize,
    tapped: usize,
) {
    println!("\n== summary ==");
    println!("  run {:.0} s, instance {:?}", secs, s.current);
    println!(
        "  events {} ({:.2}/s), batches {}, writes applied {}, dropped {}, poisoned {}",
        s.events,
        s.events as f64 / secs,
        s.batches,
        s.writes_applied,
        s.dropped,
        s.poisoned
    );
    println!(
        "  sessions {}, reconnects {}, transient retries {}, gaps recorded {}, seam repairs {} ({} events re-read)",
        s.sessions, s.reconnects, s.transient_retries, s.gaps, s.seam_repairs, s.seam_repair_events
    );
    println!("  received by ingest {tapped}, by the reference {ref_events}");
    let mut buckets: Vec<(f64, f64)> = m
        .iter()
        .filter(|(k, _)| k.starts_with(&format!("{}_bucket", im::BATCH_SECONDS)))
        .filter_map(|(k, v)| {
            let le = k.split("le=\"").nth(1)?.split('"').next()?;
            Some((
                if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().ok()?
                },
                *v,
            ))
        })
        .collect();
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    println!(
        "  batch latency (cumulative): {}",
        buckets
            .iter()
            .map(|(le, n)| format!("≤{le}s:{n}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let mut by_outcome: BTreeMap<String, f64> = BTreeMap::new();
    for (k, v) in m.iter().filter(|(k, _)| k.starts_with(im::EVENTS)) {
        if let Some(o) = k
            .split("outcome=\"")
            .nth(1)
            .and_then(|x| x.split('"').next())
        {
            *by_outcome.entry(o.to_owned()).or_insert(0.0) += v;
        }
    }
    println!("  events by outcome: {by_outcome:?}");
    for (k, v) in m.iter().filter(|(k, _)| {
        k.starts_with(im::SEAM_REPAIRS)
            || k.starts_with(im::SEAM_REPAIR_EVENTS)
            || k.contains("collection=\"account\"")
    }) {
        println!("  {k} = {v}");
    }
    for g in gaps {
        println!(
            "  gap {:?}: {} → {:?} healed {:?}",
            g.cause, g.from_at, g.to_at, g.healed_witness
        );
    }
}

fn synthetic_commit(did: &farsight_core::Did, n: u64, witness_us: i64) -> InEvent {
    let json = serde_json::json!({
        "rev": farsight_core::Tid::from_parts(witness_us as u64, 0).expect("tid").encode(),
        "operation": "create",
        "collection": "app.bsky.graph.block",
        "rkey": format!("harness{n}"),
        "record": {"$type": "app.bsky.graph.block", "subject": did.as_str()}
    });
    let op =
        farsight_core::record::parse_commit(did.as_str(), &json).expect("valid synthetic commit");
    InEvent {
        seq: None,
        witness_us,
        body: Body::Commit(op),
    }
}

async fn debt_of(pool: &PgPool, did: &farsight_core::Did) -> Option<i16> {
    sqlx::query_scalar(
        "SELECT d.reason FROM relist_debt d JOIN actors a ON a.id = d.actor_id
         WHERE a.did = $1 AND d.reason = 2",
    )
    .bind(did.as_str())
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

async fn queued(pool: &PgPool, did: &farsight_core::Did) -> Option<(i16, String)> {
    sqlx::query_as(
        "SELECT q.tier, q.requester FROM backfill_queue q JOIN actors a ON a.id = q.actor_id
         WHERE a.did = $1",
    )
    .bind(did.as_str())
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

async fn mode_b(pg: &Pg, cfg: &Config, args: &Args, c: &mut Checks) -> Result<(), String> {
    let url = cfg.firehose.urls[0].clone();
    let reference = start_reference(url.clone());
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut run = start_run(pg, cfg).await?;
    let ingest = run.ingest.as_ref().expect("running");
    if !wait_batches(ingest, 1, Duration::from_secs(120)).await {
        return Err("no batch committed within 2 minutes".into());
    }
    let total = Duration::from_secs(args.minutes * 60);
    let check_every = Duration::from_secs(args.check_every * 60);
    let kill_every = Duration::from_secs(args.kill_every * 60);
    let restart_every = Duration::from_secs(args.pg_restart_every * 60);
    let rewind_at = total / 3;
    let poison_at = total / 2;
    let (mut next_check, mut next_kill, mut next_restart) =
        (check_every, kill_every, restart_every);
    let (mut did_rewind, mut did_poison) = (false, false);
    let mut replay_slice: Option<i64> = None;
    let mut counted_snapshot: HashSet<(i64, String)> = HashSet::new();
    let mut period = 0u32;
    let mut excluded_from_monotonic: Vec<(i64, i64)> = Vec::new();
    let t0 = Instant::now();
    let early = scrape(&args.metrics).await?;
    // Ctrl-C / SIGINT ends the soak early but still runs the final checks.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let stop = stop.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                println!("interrupt: ending the soak early, running final checks");
                stop.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        });
    }

    while t0.elapsed() < total && !stop.load(std::sync::atomic::Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let el = t0.elapsed();
        let ingest = run.ingest.as_ref().expect("running");
        if el >= next_kill {
            next_kill += kill_every;
            let before = ingest.stats.snapshot();
            println!("[{:>5}s] websocket kill", el.as_secs());
            let _ = ingest.control.send(Control::KillSocket).await;
            let ok = wait_reconnect(ingest, before.sessions, Duration::from_secs(300)).await;
            let after = ingest.stats.snapshot();
            c.check(
                format!("kill @{}s: reconnected and committing again", el.as_secs()),
                ok && after.sessions > before.sessions,
                format!(
                    "sessions {} → {}, batches {} → {}",
                    before.sessions, after.sessions, before.batches, after.batches
                ),
            );
        }
        if pg.can_restart() && el >= next_restart {
            next_restart += restart_every;
            let before = ingest.stats.snapshot();
            let st = farsight_storage::firehose::read_state(&run.pool).await.ok();
            println!("[{:>5}s] postgres restart", el.as_secs());
            pg.restart()?;
            pg.wait_ready(Duration::from_secs(120)).await?;
            let ok = wait_batches(ingest, before.batches + 2, Duration::from_secs(180)).await;
            let after = ingest.stats.snapshot();
            let st2 = farsight_storage::firehose::read_state(&run.pool).await.ok();
            let applied_ok = match (
                st.and_then(|s| s.applied_through),
                st2.and_then(|s| s.applied_through),
            ) {
                (Some(a), Some(b)) => b >= a,
                _ => false,
            };
            c.check(
                format!(
                    "pg restart @{}s: ingest recovered, applied_through did not regress",
                    el.as_secs()
                ),
                ok && applied_ok,
                format!(
                    "batches {} → {}, transient retries {} → {}",
                    before.batches,
                    after.batches,
                    before.transient_retries,
                    after.transient_retries
                ),
            );
        }
        if !did_rewind && el >= rewind_at {
            did_rewind = true;
            // Simulated outage longer than retention: probe the floor, then
            // rewind to an hour before it.
            let floor = probe_floor(&url).await;
            match floor {
                Some(floor_us) => {
                    let target = floor_us - 3_600_000_000;
                    let gaps_before: HashSet<farsight_storage::ids::GapId> =
                        farsight_storage::firehose::all_gaps(&run.pool)
                            .await
                            .map(|g| g.iter().map(|x| x.id).collect())
                            .unwrap_or_default();
                    println!(
                        "[{:>5}s] rewind to {target} (instance floor {floor_us})",
                        el.as_secs()
                    );
                    let rewind_start = Utc::now().timestamp_micros();
                    let _ = ingest
                        .control
                        .send(Control::KillAndRewind { us: target })
                        .await;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    let gaps = farsight_storage::firehose::all_gaps(&run.pool)
                        .await
                        .map_err(|e| e.to_string())?;
                    // all_gaps sorts by from_at, and this gap starts in the
                    // past: identify new gaps by id, not position.
                    let new: Vec<_> = gaps
                        .iter()
                        .filter(|g| !gaps_before.contains(&g.id))
                        .collect();
                    let expected_cause = GapCause::Heuristic;
                    let ok = new.iter().any(|g| {
                        g.cause == expected_cause
                            && g.from_at.timestamp_micros() == target
                            && g.to_at
                                .is_some_and(|t| t.timestamp_micros() >= floor_us - 60_000_000)
                    });
                    c.check(
                        "simulated cursor-too-old: gap recorded with the right cause and bounds",
                        ok,
                        format!("new gaps {new:?}; expected cause {expected_cause:?} from {target} to ≈ floor {floor_us}"),
                    );
                    excluded_from_monotonic.push((rewind_start, Utc::now().timestamp_micros()));
                    // Reconnect mid-replay, after the outage's seam repair:
                    // the replay must continue where it was, not from the
                    // repair's (newer) events.
                    tokio::time::sleep(Duration::from_secs(45)).await;
                    println!(
                        "[{:>5}s] kill during the outage replay",
                        t0.elapsed().as_secs()
                    );
                    let _ = ingest.control.send(Control::KillSocket).await;
                    replay_slice = Some(floor_us + 6 * 3_600_000_000);
                }
                None => c.unverified(
                    "simulated cursor-too-old",
                    "could not probe the instance floor",
                ),
            }
        }
        if !did_poison && el >= poison_at {
            did_poison = true;
            let bad = model::synthetic_did(1);
            let syncd = model::synthetic_did(2);
            let before = ingest.stats.snapshot();
            ingest
                .faults
                .poison_dids
                .lock()
                .unwrap()
                .insert(bad.to_string());
            let now = Utc::now().timestamp_micros();
            let evs = vec![
                synthetic_commit(&bad, 1, now),
                synthetic_commit(&bad, 2, now + 1),
                InEvent {
                    seq: None,
                    witness_us: now + 2,
                    body: Body::Sync(syncd.clone()),
                },
            ];
            println!(
                "[{:>5}s] injecting 2 poisoned commits and 1 synthesized #sync",
                el.as_secs()
            );
            let _ = ingest.control.send(Control::Inject(evs)).await;
            tokio::time::sleep(Duration::from_secs(20)).await;
            let after = ingest.stats.snapshot();
            let op_errors: i64 =
                sqlx::query_scalar("SELECT count(*) FROM op_errors WHERE did = $1")
                    .bind(bad.as_str())
                    .fetch_one(&run.pool)
                    .await
                    .map_err(|e| e.to_string())?;
            let snap = farsight_storage::coverage::read_snapshot(
                &run.pool,
                &farsight_storage::keys::Limits::from_config(cfg),
            )
            .await
            .map_err(|e| e.to_string())?;
            c.check(
                "poison: 3 strikes then op_errors, resync debt, tier-1 system:resync re-list",
                after.poisoned >= before.poisoned + 2
                    && op_errors >= 2
                    && debt_of(&run.pool, &bad).await == Some(DebtReason::Resync.code())
                    && queued(&run.pool, &bad).await == Some((1, "system:resync".to_owned())),
                format!(
                    "poisoned {} → {}, op_errors {op_errors}, debt {:?}, queue {:?}, pendingResyncs {:?}",
                    before.poisoned,
                    after.poisoned,
                    debt_of(&run.pool, &bad).await,
                    queued(&run.pool, &bad).await,
                    snap.debt_counts.get(&DebtReason::Resync)
                ),
            );
            c.check(
                "#sync (synthesized, not observed): resync debt and tier-1 re-list",
                debt_of(&run.pool, &syncd).await == Some(DebtReason::Resync.code())
                    && queued(&run.pool, &syncd).await == Some((1, "system:resync".to_owned())),
                format!(
                    "debt {:?}, queue {:?}",
                    debt_of(&run.pool, &syncd).await,
                    queued(&run.pool, &syncd).await
                ),
            );
            ingest.faults.poison_dids.lock().unwrap().clear();
        }
        if el >= next_check {
            next_check += check_every;
            period += 1;
            println!("[{:>5}s] period {period} checks", el.as_secs());
            let label = format!("period {period}");
            consistency(&run.pool, c, &label)
                .await
                .map_err(|e| e.to_string())?;
            let counted: Vec<(i64, String)> =
                sqlx::query_as("SELECT author_id, rkey FROM list_blocks WHERE counted")
                    .fetch_all(&run.pool)
                    .await
                    .map_err(|e| e.to_string())?;
            let now_counted: HashSet<(i64, String)> = counted.into_iter().collect();
            let uncounted: Vec<(i64, String)> =
                sqlx::query_as("SELECT author_id, rkey FROM list_blocks WHERE NOT counted")
                    .fetch_all(&run.pool)
                    .await
                    .map_err(|e| e.to_string())?;
            let flipped = uncounted
                .iter()
                .filter(|k| counted_snapshot.contains(*k))
                .count();
            c.check(
                format!("{label}: no counted → uncounted flip"),
                flipped == 0,
                format!("{flipped} flips; {} counted rows", now_counted.len()),
            );
            counted_snapshot = now_counted;
            let ttl = farsight_storage::keys::Limits::from_config(cfg).tombstone_ttl;
            let purged = farsight_storage::janitor::purge_tombstones(&run.pool, Utc::now(), ttl)
                .await
                .map_err(|e| e.to_string())?;
            let old: i64 = sqlx::query_scalar("SELECT count(*) FROM tombstones WHERE deleted_at < now() - $1 * interval '1 second'")
                .bind(ttl.as_secs_f64())
                .fetch_one(&run.pool)
                .await
                .map_err(|e| e.to_string())?;
            c.check(
                format!("{label}: tombstone TTL enforced"),
                old == 0,
                format!("{purged} purged, {old} older than TTL remain"),
            );
            let s = run.ingest.as_ref().expect("running").stats.snapshot();
            println!(
                "  events {} batches {} reconnects {} gaps {} poisoned {}",
                s.events, s.batches, s.reconnects, s.gaps, s.poisoned
            );
        }
    }

    let late = scrape(&args.metrics).await?;
    let ingest = run.ingest.take().expect("running");
    let final_stats = ingest.stats.snapshot();
    ingest.shutdown().await;
    let (ref_events, ref_intervals, _) = reference.stop().await;
    run.tap_task.abort();
    run.watcher.task.abort();
    let tapped = std::mem::take(&mut *run.tapped.lock().unwrap());
    let run_secs = run.started.elapsed().as_secs_f64();
    println!("\n== soak: final checks ==");
    let samples: Vec<StateSample> = run.watcher.samples.lock().unwrap().clone();
    if excluded_from_monotonic.is_empty() {
        check_monotonic(c, &samples, "whole run");
    } else {
        c.unverified(
            "whole run: cursor monotonic",
            "the simulated outage deliberately rewinds the persisted cursor; checked in the conformance run",
        );
    }
    check_no_loss(c, "whole run", &tapped, &ref_events, &ref_intervals);
    if let Some(start) = replay_slice {
        check_replay_slice(c, &url, start, &tapped).await;
    }
    check_model(&run.pool, c, "whole run", &tapped)
        .await
        .map_err(|e| e.to_string())?;
    consistency(&run.pool, c, "final")
        .await
        .map_err(|e| e.to_string())?;
    let clock_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM firehose_clock")
        .fetch_one(&run.pool)
        .await
        .map_err(|e| e.to_string())?;
    c.check(
        "clock rows = batch commits ± restarts",
        (clock_rows - final_stats.batches as i64).abs()
            <= 1 + (args.minutes / args.pg_restart_every.max(1)) as i64,
        format!("{clock_rows} rows, {} batches", final_stats.batches),
    );
    check_metrics(c, &early, &late, run_secs);
    let gaps = farsight_storage::firehose::all_gaps(&run.pool)
        .await
        .map_err(|e| e.to_string())?;
    summary(
        &final_stats,
        &late,
        run_secs,
        &gaps,
        ref_events.len(),
        tapped.len(),
    );
    Ok(())
}

/// Mode P: does the instance replay everything from a timestamp cursor?
/// A continuous reference R, a live session A closed after `a_secs`, then
/// a session B resumed at A's last witness − 120 s (exactly what ingest
/// does on v1). Every R event in the shared window must appear in A ∪ B.
async fn mode_p(cfg: &Config, rounds: u64, c: &mut Checks) -> Result<(), String> {
    let url = cfg.firehose.urls[0].clone();
    for round in 1..=rounds {
        let reference = start_reference(url.clone());
        tokio::time::sleep(Duration::from_secs(5)).await;
        let mut a = match conn::connect(&url, Protocol::V2, Cursor::Live, true).await {
            Ok(s) => s,
            Err(_) => conn::connect(&url, Protocol::V1, Cursor::Live, true)
                .await
                .map_err(|e| e.to_string())?,
        };
        let proto = a.protocol;
        let mut got: Vec<InEvent> = Vec::new();
        let a_end = Instant::now() + Duration::from_secs(60);
        while Instant::now() < a_end {
            if let Ok(Some(Ok(farsight_ingest::frame::Frame::Event(ev)))) =
                tokio::time::timeout(Duration::from_secs(5), a.next_frame()).await
            {
                got.push(ev);
            }
        }
        a.close().await;
        let a_last = got.iter().map(|e| e.witness_us).max().unwrap_or(0);
        let a_count = got.len();
        let requested = a_last - 120_000_000;
        let mut b = conn::connect(&url, proto, Cursor::TimeUs(requested), true)
            .await
            .map_err(|e| e.to_string())?;
        let mut b_first: Option<i64> = None;
        let mut b_count = 0;
        let b_end = Instant::now() + Duration::from_secs(60);
        while Instant::now() < b_end {
            if let Ok(Some(Ok(farsight_ingest::frame::Frame::Event(ev)))) =
                tokio::time::timeout(Duration::from_secs(5), b.next_frame()).await
            {
                b_first.get_or_insert(ev.witness_us);
                b_count += 1;
                got.push(ev);
            }
        }
        b.close().await;
        let (ref_events, ref_iv, _) = reference.stop().await;
        let missing_before = |got: &[InEvent]| -> Vec<String> {
            let have: HashSet<String> = got.iter().filter_map(model::event_key).collect();
            ref_events
                .iter()
                .filter(|e| {
                    e.witness_us > a_last - 60_000_000 && e.witness_us < a_last + 30_000_000
                })
                .filter_map(model::event_key)
                .filter(|k| !have.contains(k))
                .collect()
        };
        let seam_missing = missing_before(&got);
        // C: replay the same window again, now that it is well behind the
        // live tip. Does the archive hold what B's seam dropped?
        let mut cc = conn::connect(&url, proto, Cursor::TimeUs(requested), true)
            .await
            .map_err(|e| e.to_string())?;
        let mut c_events: Vec<InEvent> = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(10), cc.next_frame()).await {
                Ok(Some(Ok(farsight_ingest::frame::Frame::Event(ev)))) => {
                    let done = ev.witness_us > a_last + 40_000_000;
                    c_events.push(ev);
                    if done {
                        break;
                    }
                }
                Ok(Some(Ok(_))) => {}
                _ => break,
            }
        }
        cc.close().await;
        let c_keys: HashSet<String> = c_events.iter().filter_map(model::event_key).collect();
        let recovered = seam_missing.iter().filter(|k| c_keys.contains(*k)).count();
        println!(
            "  round {round}: {} missing near the seam after B; a later replay C ({} events) holds {recovered} of them",
            seam_missing.len(),
            c_events.len()
        );
        println!(
            "  round {round}: A {a_count} events (last {a_last}), B requested {requested}, first {b_first:?} ({} s after request), {b_count} events",
            b_first.map(|f| (f - requested) / 1_000_000).unwrap_or(-1)
        );
        check_no_loss(
            c,
            &format!("probe round {round} ({})", proto.label()),
            &got,
            &ref_events,
            &ref_iv,
        );
    }
    Ok(())
}

/// After a simulated outage: an independent connection replays a 20-minute
/// slice from inside the replayed window; every commit in it must have
/// reached ingest (a replay skipped after a reconnect would miss it).
async fn check_replay_slice(c: &mut Checks, url: &str, start_us: i64, tapped: &[InEvent]) {
    let end_us = start_us + 20 * 60_000_000;
    let got: HashSet<String> = tapped.iter().filter_map(model::event_key).collect();
    let mut s = match conn::connect(url, Protocol::V2, Cursor::TimeUs(start_us), true).await {
        Ok(s) => s,
        Err(_) => match conn::connect(url, Protocol::V1, Cursor::TimeUs(start_us), true).await {
            Ok(s) => s,
            Err(e) => {
                c.unverified("outage replay slice fully applied", format!("connect: {e}"));
                return;
            }
        },
    };
    let (mut compared, mut missing) = (0usize, Vec::new());
    loop {
        match tokio::time::timeout(Duration::from_secs(30), s.next_frame()).await {
            Ok(Some(Ok(farsight_ingest::frame::Frame::Event(ev)))) => {
                if ev.witness_us > end_us {
                    break;
                }
                if let (Body::Commit(_), Some(k)) = (&ev.body, model::event_key(&ev)) {
                    compared += 1;
                    if !got.contains(&k) {
                        missing.push(k);
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    s.close().await;
    c.check(
        "outage replay slice fully applied (no replay skipped after a mid-replay reconnect)",
        compared > 0 && missing.is_empty(),
        format!(
            "{compared} commits in [{start_us}, {end_us}]; {} missing {:?}",
            missing.len(),
            missing.iter().take(3).collect::<Vec<_>>()
        ),
    );
}

/// The instance's retention floor: the witness time of the first event
/// replayed from a cursor far in the past.
async fn probe_floor(url: &str) -> Option<i64> {
    let ancient = Cursor::TimeUs(1_000_000_000_000_000);
    let mut s = match conn::connect(url, Protocol::V2, ancient, true).await {
        Ok(s) => s,
        Err(_) => conn::connect(url, Protocol::V1, ancient, true).await.ok()?,
    };
    let floor = loop {
        match tokio::time::timeout(Duration::from_secs(30), s.next_frame()).await {
            Ok(Some(Ok(farsight_ingest::frame::Frame::Event(ev)))) => break Some(ev.witness_us),
            Ok(Some(Ok(_))) => continue,
            _ => break None,
        }
    };
    s.close().await;
    floor
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    // Library warnings (slow batches, reconnects, gaps) to stderr.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
    let mut cfg = Config::default();
    if !args.jetstream.is_empty() {
        cfg.firehose.urls = args.jetstream.clone();
    }
    let addr = match args.metrics.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: --metrics: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = im::install(addr) {
        eprintln!("error: metrics exporter: {e}");
        std::process::exit(2);
    }
    let pg = match &args.database_url {
        Some(u) => Pg::existing(u),
        None => match Pg::start_docker(args.keep) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(2);
            }
        },
    };
    let code = match pg.wait_ready(Duration::from_secs(90)).await {
        Err(e) => {
            eprintln!("error: {e}");
            2
        }
        Ok(()) => {
            println!(
                "== stage-2 ingest harness: mode {}, {} min, instances {:?} ==",
                args.mode.to_ascii_uppercase(),
                args.minutes,
                cfg.firehose.urls
            );
            let mut c = Checks::default();
            let r = if args.mode == 'p' {
                mode_p(&cfg, args.minutes.max(1), &mut c).await
            } else if args.mode == 'a' {
                mode_a(&pg, &cfg, &args, &mut c).await
            } else {
                mode_b(&pg, &cfg, &args, &mut c).await
            };
            if let Err(e) = r {
                c.check("run completed", false, e);
            }
            let (p, f, u) = c.counts();
            let verdict = if f > 0 {
                Verdict::Fail
            } else if u > 0 {
                Verdict::Unverified
            } else {
                Verdict::Pass
            };
            println!(
                "\nRESULT: {} ({p} passed, {f} failed, {u} unverified)",
                tag(verdict)
            );
            if verdict == Verdict::Pass { 0 } else { 1 }
        }
    };
    pg.stop();
    std::process::exit(code);
}
