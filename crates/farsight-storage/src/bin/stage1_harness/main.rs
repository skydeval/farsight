//! `farsight-stage1-harness`: integration tests of the core and storage
//! crates. Built only with `--features harness`.
//!
//! Starts a throwaway Postgres 16 container (ephemeral named volume),
//! creates one fresh, migrated database per stream, drives curated event
//! streams through `farsight-storage::apply` and the janitor, and checks
//! the invariants after each. Prints PASS / FAIL / UNVERIFIED per check
//! and per stream, then a summary. Exit code 0 only if every check
//! passed.
//!
//! ```text
//! cargo run -p farsight-storage --features harness --bin farsight-stage1-harness -- [options]
//!   --database-url URL   use an existing server instead of Docker
//!   --image IMAGE        Docker image (default postgres:16)
//!   --keep               keep the container and volume afterwards
//!   --only N[,N…]        run only these stream numbers
//!   --verbose            print passing checks too
//! ```

mod fixtures;
mod streams_lww;
mod streams_more;
mod streams_table;

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use farsight_storage::keys::Limits;

use fixtures::{Checks, Env, Server, Verdict};

type StreamFn = for<'a> fn(
    &'a mut Env,
    &'a mut Checks,
) -> Pin<Box<dyn Future<Output = farsight_storage::Result<()>> + 'a>>;

macro_rules! stream {
    ($f:path) => {{
        fn wrap<'a>(
            env: &'a mut Env,
            c: &'a mut Checks,
        ) -> Pin<Box<dyn Future<Output = farsight_storage::Result<()>> + 'a>> {
            Box::pin($f(env, c))
        }
        wrap as StreamFn
    }};
}

struct Stream {
    n: u32,
    name: &'static str,
    run: StreamFn,
}

fn streams() -> Vec<Stream> {
    vec![
        Stream {
            n: 1,
            name: "LWW ordering (all six permutations)",
            run: stream!(streams_lww::s1_lww),
        },
        Stream {
            n: 2,
            name: "refusal tombstone at E − 1",
            run: stream!(streams_lww::s2_refusal_tombstone),
        },
        Stream {
            n: 3,
            name: "refused listitem never lost",
            run: stream!(streams_lww::s3_refused_item),
        },
        Stream {
            n: 4,
            name: "transition table (every cell)",
            run: stream!(streams_table::s4_transition_table),
        },
        Stream {
            n: 5,
            name: "counter drift under 10k-listblock churn",
            run: stream!(streams_more::s5_counter_drift),
        },
        Stream {
            n: 6,
            name: "counted stickiness and capped re-evaluation",
            run: stream!(streams_more::s6_counted_stickiness),
        },
        Stream {
            n: 7,
            name: "tombstone TTL and the 72 h stamp invariant",
            run: stream!(streams_more::s7_tombstone_ttl),
        },
        Stream {
            n: 8,
            name: "lock order under contention",
            run: stream!(streams_more::s8_lock_contention),
        },
        Stream {
            n: 9,
            name: "coverage plumbing: NOTIFY, clock, gaps, snapshot",
            run: stream!(streams_more::s9_coverage_plumbing),
        },
        Stream {
            n: 10,
            name: "account/identity/sync events, queue collapse, poison recording",
            run: stream!(streams_more::s10_repo_events),
        },
        Stream {
            n: 11,
            name: "per-instance cursors",
            run: stream!(streams_more::s11_instance_cursors),
        },
        Stream {
            n: 12,
            name: "reactivation: OA only under the list lock",
            run: stream!(streams_more::s12_reactivation_lock),
        },
        Stream {
            n: 13,
            name: "seam windows: recorded, read again, or recorded as a gap",
            run: stream!(streams_more::s13_seam_windows),
        },
    ]
}

struct Args {
    database_url: Option<String>,
    image: String,
    keep: bool,
    only: Option<Vec<u32>>,
    verbose: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        database_url: None,
        image: "postgres:16".to_owned(),
        keep: false,
        only: None,
        verbose: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--database-url" => {
                a.database_url = Some(it.next().ok_or("--database-url needs a value")?)
            }
            "--image" => a.image = it.next().ok_or("--image needs a value")?,
            "--keep" => a.keep = true,
            "--verbose" => a.verbose = true,
            "--only" => {
                let v = it.next().ok_or("--only needs a value")?;
                let ns = v
                    .split(',')
                    .map(|s| {
                        s.trim()
                            .parse::<u32>()
                            .map_err(|_| format!("bad stream number {s:?}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                a.only = Some(ns);
            }
            "-h" | "--help" => {
                println!("see the module docs in src/bin/stage1_harness/main.rs");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(a)
}

fn tag(v: Verdict) -> &'static str {
    match v {
        Verdict::Pass => "PASS",
        Verdict::Fail => "FAIL",
        Verdict::Unverified => "UNVERIFIED",
    }
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
    let server = match &args.database_url {
        Some(url) => Server::existing(url),
        None => match Server::start_docker(&args.image, args.keep) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("error: starting postgres: {e}");
                std::process::exit(2);
            }
        },
    };
    let code = run(&server, &args).await;
    server.stop();
    std::process::exit(code);
}

async fn run(server: &Server, args: &Args) -> i32 {
    if let Err(e) = server.wait_ready(Duration::from_secs(90)).await {
        eprintln!("error: {e}");
        return 2;
    }
    let mut summary = Vec::new();
    for s in streams() {
        if let Some(only) = &args.only
            && !only.contains(&s.n)
        {
            continue;
        }
        println!("\n== stream {}: {} ==", s.n, s.name);
        let started = Instant::now();
        let mut checks = Checks::default();
        let outcome = match server.fresh_db(&format!("farsight_stage1_s{}", s.n)).await {
            Ok(pool) => {
                let mut env = Env::new(pool, Limits::defaults());
                let r = (s.run)(&mut env, &mut checks).await;
                env.pool.close().await;
                r
            }
            Err(e) => Err(e),
        };
        if let Err(e) = &outcome {
            checks.check("stream ran to completion", false, format!("error: {e}"));
        }
        for c in &checks.items {
            if args.verbose || c.verdict != Verdict::Pass {
                println!("  [{}] {} — {}", tag(c.verdict), c.what, c.detail);
            }
        }
        let pass = checks
            .items
            .iter()
            .filter(|c| c.verdict == Verdict::Pass)
            .count();
        let fail = checks
            .items
            .iter()
            .filter(|c| c.verdict == Verdict::Fail)
            .count();
        let unv = checks
            .items
            .iter()
            .filter(|c| c.verdict == Verdict::Unverified)
            .count();
        let verdict = if fail > 0 {
            Verdict::Fail
        } else if unv > 0 {
            Verdict::Unverified
        } else {
            Verdict::Pass
        };
        println!(
            "  -> {} ({pass} passed, {fail} failed, {unv} unverified; {:.1}s)",
            tag(verdict),
            started.elapsed().as_secs_f64()
        );
        summary.push((s.n, s.name, verdict, pass, fail, unv));
    }
    println!("\n== summary ==");
    for (n, name, v, p, f, u) in &summary {
        println!("  stream {n}: {:<10} {name} ({p}/{f}/{u})", tag(*v));
    }
    let all_pass = summary.iter().all(|s| s.2 == Verdict::Pass);
    println!(
        "\n{}",
        if all_pass {
            "ALL STREAMS PASSED"
        } else {
            "NOT ALL STREAMS PASSED (see FAIL / UNVERIFIED above)"
        }
    );
    if all_pass { 0 } else { 1 }
}
