//! The handle sync (see `docs/design/lodestar.md` and
//! `docs/design/web-ui.md`): with a lodestar instance
//! (`backfill.lodestar_url`), a background worker that keeps the stored
//! handle of every account Farsight holds in step with lodestar's own
//! verification, which re-checks every handle each week.
//!
//! - **Identity changes first.** Every [`DUE_EVERY`], the accounts an
//!   identity event named (`handle_due`) between [`GRACE`] and
//!   [`CATCHUP`] ago are asked for in one request. An answer settles one
//!   only if lodestar checked the handle after the event; the rest are
//!   asked again next time. What lodestar has not settled by
//!   [`CATCHUP`] is left to the handle pass, which checks it itself, or,
//!   with the pass off, to the next page that shows the account.
//! - **Then the walk.** `actors` in id order, [`lodestar::BATCH`]
//!   accounts a request, deactivated and deleted accounts left out. For
//!   each account lodestar can say something about, the stored answer is
//!   replaced when it differs, or when lodestar checked the handle since
//!   it was stored; the stored answer is dated by lodestar's check, so
//!   it goes stale on lodestar's weekly cycle. Accounts it cannot say
//!   anything about (a claim not checked yet, a `did:web` it has not
//!   seen) are left as they are.
//! - **Its own pace.** `public_ui.lodestar_sync_rps` requests a second,
//!   0 = off. After the last account the walk rests [`LAP_REST`].
//! - **It backs off.** A batch lodestar cannot answer (it is catching
//!   up, not at the directory's tip, or unreachable) is asked again after
//!   a wait, a minute at first and up to half an hour.
//!
//! Nothing is fetched from anywhere but lodestar. The position of the
//! walk is memory: a restart begins at the first account again.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use farsight_core::Did;
use farsight_core::config::Config;
use farsight_core::lodestar::{self, Answer};
use farsight_storage::handles::{self as store, SyncRow};
use tokio::sync::watch;

use super::handles::from_lodestar;
use super::pass::{Checked, record};
use crate::pages::WebState;

/// An identity change is first asked about this long after it was noted:
/// lodestar sees the change seconds after the directory.
pub const GRACE: Duration = Duration::from_secs(10);
/// How long the sync asks lodestar about an identity change before it
/// leaves it to the handle pass.
pub const CATCHUP: Duration = Duration::from_secs(300);
/// How often the identity changes waiting are asked about.
pub const DUE_EVERY: Duration = Duration::from_secs(30);
/// How long the walk rests after reaching the last account.
pub const LAP_REST: Duration = Duration::from_secs(3600);
/// How often an idle or switched-off sync looks again.
pub const IDLE_POLL: Duration = Duration::from_secs(5);
/// Deadline of one batch request.
pub const DEADLINE: Duration = Duration::from_secs(30);
/// First and longest wait after a batch lodestar could not answer.
pub const BACKOFF: (Duration, Duration) = (Duration::from_secs(60), Duration::from_secs(1800));

/// `farsight_handle_sync_total{outcome}`: accounts the sync looked at,
/// by whether their stored answer was replaced (`written`), already
/// agreed (`unchanged`), or lodestar could not say (`unknown`).
pub const SYNC: &str = "farsight_handle_sync_total";
/// `farsight_handle_sync_position`: the `actors.id` the walk has reached.
pub const POSITION: &str = "farsight_handle_sync_position";
/// `farsight_handle_sync_laps_total`: walks completed since start.
pub const LAPS: &str = "farsight_handle_sync_laps_total";

/// Registers the series at zero.
pub fn register() {
    for label in ["written", "unchanged", "unknown"] {
        ::metrics::counter!(SYNC, "outcome" => label).increment(0);
    }
    ::metrics::counter!(LAPS).increment(0);
    ::metrics::gauge!(POSITION).set(0.0);
}

/// Whether the sync runs under `cfg`: a lodestar instance is named and
/// `public_ui.lodestar_sync_rps` is not 0.
pub fn active(cfg: &Config) -> bool {
    lodestar::base(&cfg.backfill.lodestar_url).is_some() && cfg.public_ui.lodestar_sync_rps > 0
}

/// What lodestar's answer about an account means for its stored handle.
#[derive(Debug)]
struct Decided {
    /// The answer to store.
    found: Checked,
    /// The date to store it under: lodestar's check, or now.
    at: Option<DateTime<Utc>>,
    /// When lodestar last checked the handle at all.
    checked: Option<DateTime<Utc>>,
}

/// What lodestar's answer about an account means for its stored handle;
/// `None`: lodestar cannot say.
fn decide(answer: &Answer) -> Option<Decided> {
    let (_, found, at) = from_lodestar(answer)?;
    let checked = match answer {
        Answer::Account(a) => a.handle_checked_at,
        _ => None,
    };
    Some(Decided {
        found: found.into(),
        at,
        checked,
    })
}

/// Whether storing `found`, checked at `at`, changes what is stored.
fn changes(
    stored: Option<&(String, DateTime<Utc>)>,
    found: &Checked,
    at: Option<DateTime<Utc>>,
) -> bool {
    let newer = |t: &DateTime<Utc>| at.is_some_and(|a| a > *t);
    match (found, stored) {
        (Checked::Unknown, _) => false,
        (_, None) => true,
        (Checked::Handle(h), Some((s, t))) => s != h || newer(t),
        (Checked::Gone, Some((s, t))) => !s.is_empty() || newer(t),
        // The store keeps the claim itself while it is recent enough.
        (Checked::Unresolved(_), Some((s, _))) => !s.is_empty(),
    }
}

/// Whether lodestar's answer is about the identity as it stands after an
/// event noted at `asked`: its check of the handle came later, or the
/// identity is `gone` (lodestar has no such DID, or it is tombstoned),
/// which no check changes.
fn after_change(checked: Option<DateTime<Utc>>, gone: bool, asked: DateTime<Utc>) -> bool {
    gone || checked.is_some_and(|c| c >= asked)
}

/// Whether lodestar says the identity is gone for good.
fn gone(answer: &Answer) -> bool {
    match answer {
        Answer::NotFound => true,
        Answer::Account(a) => a.status == lodestar::Status::Tombstoned,
        Answer::Unavailable => false,
    }
}

/// Asks lodestar about `dids` in one request. `None`: it gave nothing to
/// go by.
async fn ask(st: &WebState, base: &str, dids: &[&str]) -> Option<HashMap<String, Answer>> {
    let url = lodestar::accounts_url(base)?;
    let body = lodestar::accounts_body(dids.iter().copied());
    let r = tokio::time::timeout(DEADLINE, st.safe.post_json(&url, body))
        .await
        .ok()?
        .ok()?;
    let answers: Option<HashMap<String, Answer>> =
        lodestar::read_batch(r.status, &r.body).map(|a| a.into_iter().collect());
    super::metrics::lodestar_read(answers.is_some());
    answers
}

fn count(outcome: &'static str, n: u64) {
    if n > 0 {
        ::metrics::counter!(SYNC, "outcome" => outcome).increment(n);
    }
}

/// Why a step did nothing.
#[derive(Debug)]
enum Stalled {
    /// Lodestar gave nothing to go by.
    Lodestar,
    /// The database could not be read or written.
    Storage(farsight_storage::StorageError),
}

impl From<farsight_storage::StorageError> for Stalled {
    fn from(e: farsight_storage::StorageError) -> Stalled {
        Stalled::Storage(e)
    }
}

impl From<sqlx::Error> for Stalled {
    fn from(e: sqlx::Error) -> Stalled {
        Stalled::Storage(e.into())
    }
}

/// Asks about the identity changes waiting; `Ok(false)` when there were
/// none.
async fn serve_due(st: &WebState, cfg: &Config, base: &str) -> Result<bool, Stalled> {
    let rows = {
        let mut conn = st.api.pool.acquire().await?;
        store::due_window(
            &mut conn,
            lodestar::BATCH as i64,
            GRACE.as_secs() as i64,
            CATCHUP.as_secs() as i64,
        )
        .await?
    };
    if rows.is_empty() {
        return Ok(false);
    }
    let dids: Vec<&str> = rows.iter().map(|(d, _)| d.as_str()).collect();
    let answers = ask(st, base, &dids).await.ok_or(Stalled::Lodestar)?;
    for (raw, asked) in &rows {
        let (Ok(did), Some(answer)) = (Did::parse(raw), answers.get(raw)) else {
            continue;
        };
        let Some(d) = decide(answer) else {
            continue;
        };
        if !after_change(d.checked, gone(answer), *asked) {
            continue;
        }
        record(st, cfg, &did, &d.found, d.at).await?;
        let mut conn = st.api.pool.acquire().await?;
        store::due_done(&mut conn, &did).await?;
    }
    Ok(true)
}

/// Where the walk is.
#[derive(Debug, Default)]
struct Walk {
    /// The last `actors.id` looked at.
    cursor: i64,
    /// When the walk last reached the end.
    rested_since: Option<Instant>,
}

/// One step of the walk: the next batch of accounts, asked about and
/// stored. `Ok(false)` while the walk rests.
async fn walk_step(
    st: &WebState,
    cfg: &Config,
    base: &str,
    walk: &mut Walk,
) -> Result<bool, Stalled> {
    if let Some(since) = walk.rested_since {
        if since.elapsed() < LAP_REST {
            return Ok(false);
        }
        walk.rested_since = None;
    }
    let rows: Vec<SyncRow> = {
        let mut conn = st.api.pool.acquire().await?;
        store::sync_batch(&mut conn, walk.cursor, lodestar::BATCH as i64).await?
    };
    let Some(last) = rows.last().map(|r| r.id) else {
        walk.cursor = 0;
        walk.rested_since = Some(Instant::now());
        ::metrics::counter!(LAPS).increment(1);
        tracing::info!("handle sync: reached the last account");
        return Ok(false);
    };
    let dids: Vec<&str> = rows.iter().map(|r| r.did.as_str()).collect();
    let answers = ask(st, base, &dids).await.ok_or(Stalled::Lodestar)?;
    let (mut written, mut unchanged, mut unknown) = (0, 0, 0);
    for row in &rows {
        let decided = answers.get(&row.did).and_then(decide);
        let (Ok(did), Some(Decided { found, at, .. })) = (Did::parse(&row.did), decided) else {
            unknown += 1;
            continue;
        };
        if found == Checked::Unknown {
            unknown += 1;
        } else if changes(row.stored.as_ref(), &found, at) {
            record(st, cfg, &did, &found, at).await?;
            written += 1;
        } else {
            unchanged += 1;
        }
    }
    count("written", written);
    count("unchanged", unchanged);
    count("unknown", unknown);
    walk.cursor = last;
    ::metrics::gauge!(POSITION).set(last as f64);
    Ok(true)
}

async fn pause(stop: &mut watch::Receiver<bool>, d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => false,
        r = stop.changed() => r.is_err() || *stop.borrow(),
    }
}

/// The next wait after a step lodestar could not answer.
fn backed_off(last: Duration) -> Duration {
    (last * 2).clamp(BACKOFF.0, BACKOFF.1)
}

/// The worker: one long-running task, started with the server's
/// background tasks. Ends when `stop` flips.
pub async fn run(st: Arc<WebState>, mut stop: watch::Receiver<bool>) {
    let mut walk = Walk::default();
    let mut backoff = Duration::ZERO;
    let mut due_at: Option<Instant> = None;
    let mut was_on = false;
    loop {
        if *stop.borrow() {
            return;
        }
        let loaded = st.api.config.current();
        let cfg = &loaded.config;
        let base = match lodestar::base(&cfg.backfill.lodestar_url) {
            Some(b) if active(cfg) => b.to_owned(),
            _ => {
                was_on = false;
                if pause(&mut stop, IDLE_POLL).await {
                    return;
                }
                continue;
            }
        };
        if !was_on {
            was_on = true;
            tracing::info!(lodestar = %base, from = walk.cursor, "handle sync: running");
        }
        let tick = Duration::from_secs_f64(1.0 / f64::from(cfg.public_ui.lodestar_sync_rps));
        let mut asked = false;
        let mut result = Ok(());
        if due_at.is_none_or(|t| t.elapsed() >= DUE_EVERY) {
            due_at = Some(Instant::now());
            match serve_due(&st, cfg, &base).await {
                Ok(a) => asked = a,
                Err(e) => result = Err(e),
            }
        }
        if result.is_ok() && !asked {
            match walk_step(&st, cfg, &base, &mut walk).await {
                Ok(a) => asked = a,
                Err(e) => result = Err(e),
            }
        }
        let wait = match result {
            Ok(()) => {
                backoff = Duration::ZERO;
                if asked { tick } else { IDLE_POLL }
            }
            Err(Stalled::Lodestar) => {
                backoff = backed_off(backoff);
                tracing::warn!(
                    wait_secs = backoff.as_secs(),
                    "handle sync: lodestar could not answer; waiting"
                );
                backoff
            }
            Err(Stalled::Storage(e)) => {
                tracing::warn!(error = %e, "handle sync: the database could not be used");
                IDLE_POLL
            }
        };
        if pause(&mut stop, wait).await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn what_is_written() {
        let old = t("2026-10-01T00:00:00Z");
        let new = t("2026-10-08T00:00:00Z");
        let h = |s: &str| Checked::Handle(s.into());
        let stored = |s: &str, at| Some((s.to_owned(), at));
        // Nothing stored: anything lodestar can say is written.
        assert!(changes(None, &h("a.example"), Some(new)));
        assert!(changes(None, &Checked::Gone, None));
        assert!(!changes(None, &Checked::Unknown, None));
        // The same handle, checked again since: written, with the new date.
        assert!(changes(
            stored("a.example", old).as_ref(),
            &h("a.example"),
            Some(new)
        ));
        assert!(!changes(
            stored("a.example", new).as_ref(),
            &h("a.example"),
            Some(new)
        ));
        assert!(!changes(
            stored("a.example", new).as_ref(),
            &h("a.example"),
            Some(old)
        ));
        // Another handle, or none any more.
        assert!(changes(
            stored("a.example", new).as_ref(),
            &h("b.example"),
            Some(old)
        ));
        assert!(changes(
            stored("a.example", new).as_ref(),
            &Checked::Gone,
            None
        ));
        assert!(!changes(stored("", new).as_ref(), &Checked::Gone, None));
        // An unreachable claim touches only a shown handle.
        let u = Checked::Unresolved("a.example".into());
        assert!(changes(stored("a.example", new).as_ref(), &u, None));
        assert!(!changes(stored("", new).as_ref(), &u, None));
    }

    #[test]
    fn an_identity_change_waits_for_lodestars_check_after_it() {
        let event = t("2026-10-19T06:00:00Z");
        assert!(!after_change(Some(t("2026-10-19T05:59:59Z")), false, event));
        assert!(after_change(Some(t("2026-10-19T06:00:03Z")), false, event));
        // Never checked: settled only for an identity that is gone.
        assert!(!after_change(None, false, event));
        assert!(after_change(None, true, event));
        assert!(gone(&Answer::NotFound));
        assert!(!gone(&Answer::Unavailable));
    }

    #[test]
    fn the_wait_doubles_between_a_minute_and_half_an_hour() {
        let mut d = Duration::ZERO;
        let mut seen = Vec::new();
        for _ in 0..7 {
            d = backed_off(d);
            seen.push(d.as_secs());
        }
        assert_eq!(seen, [60, 120, 240, 480, 960, 1800, 1800]);
    }
}
