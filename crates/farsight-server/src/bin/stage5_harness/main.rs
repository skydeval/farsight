//! `farsight-stage5-harness`: integration tests of the history write path
//! (see `docs/design/history.md`), through the real apply path and janitor
//! against a database of its own. Every assertion reads the rows the
//! writers stored.
//!
//! `--keep` keeps the Postgres container.

#[allow(dead_code)]
#[path = "../stage3_harness/seed.rs"]
mod seed;
mod store;
#[allow(dead_code)]
#[path = "../stage3_harness/support.rs"]
mod support;

use std::time::Duration;

use crate::support::{Checks, Pg};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let keep = std::env::args().any(|a| a == "--keep");
    println!("== farsight stage-5 harness: history write path");
    let pg = match Pg::start(keep) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("postgres: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    let mut c = Checks::default();
    let result = async {
        pg.wait_ready(Duration::from_secs(60)).await?;
        store::run(&mut c, &pg).await
    }
    .await;
    if let Err(e) = &result {
        c.check("harness ran to completion", false, e.clone());
    }
    pg.stop();
    let (p, f, u) = c.counts();
    for i in c
        .items
        .iter()
        .filter(|i| i.verdict != support::Verdict::Pass)
    {
        println!(
            "  {} {} — {}",
            support::tag(i.verdict),
            i.what,
            support::truncate(&i.detail, 300)
        );
    }
    println!(
        "RESULT: {} ({p} passed, {f} failed, {u} unverified)",
        if f == 0 { "PASS" } else { "FAIL" }
    );
    if f == 0 {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(1)
    }
}
