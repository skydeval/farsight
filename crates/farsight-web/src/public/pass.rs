//! The handle pass (see `docs/design/web-ui.md`): a background worker
//! that checks the handle of every account Farsight holds, so that a
//! page's rows have their handles before anyone opens it.
//!
//! - **Identity changes first.** The firehose writer notes every account
//!   an identity event names in `handle_due`; the pass serves those
//!   before anything else, oldest first.
//! - **Then the walk.** `actors` in id order, a window of ids at a time,
//!   taking the accounts with no stored answer, or a stored "nothing to
//!   show" older than [`NONE_RETRY`]. Deactivated and deleted accounts
//!   are skipped: no table shows them. A verified handle is not checked
//!   again by the walk; identity events and page views do that.
//! - **Its own pace.** `public_ui.handle_pass_rps` checks a second, 0 =
//!   off (the default). Each check is up to two outbound requests. The
//!   budget of pages and cards (`handle_rps`) is not drawn on.
//! - **It backs off.** When more than half of a batch could not be
//!   established (hosts refusing or timing out), the pass waits, a
//!   minute at first and up to half an hour, before it goes on. Not
//!   when the failures are mostly handles under one domain: one host
//!   that is down says nothing about the others.
//! - A check that establishes nothing (no answer from the directory or
//!   the handle's host, no address for it, a timeout) stores nothing the
//!   first time: the account is checked once more after the batch, or
//!   after the wait. A second such check is stored as "nothing to show",
//!   dated so that it is due again in a day.
//!
//! The position of the walk is memory: a restart begins at the first
//! account again and passes over the ones already answered without a
//! request.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use farsight_core::Did;
use farsight_core::config::Config;
use farsight_core::net::OutboundClient;
use farsight_storage::handles as store;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use super::handles::{NEGATIVE_TTL, claimed_handle, document_url};
use crate::pages::{WebState, resolve_handle};

/// Ids one query of the walk looks at.
pub const WINDOW: i64 = 20_000;
/// Most accounts taken from one window at a time.
pub const BATCH: i64 = 50;
/// Most accounts kept in memory for a second check.
pub const RETRY_CAP: usize = 5_000;
/// Most windows one step of the walk reads before it yields.
pub const WINDOWS_PER_STEP: usize = 50;
/// Most queued identity changes served in one batch.
pub const DUE_BATCH: i64 = 50;
/// Most checks in flight at once.
pub const IN_FLIGHT: usize = 32;
/// Deadline of one check.
pub const CHECK_DEADLINE: Duration = Duration::from_secs(8);
/// A stored "nothing to show" is checked again by the walk after this.
pub const NONE_RETRY: Duration = Duration::from_secs(7 * 24 * 3600);
/// How long the walk rests after reaching the last account.
pub const LAP_REST: Duration = Duration::from_secs(600);
/// How often an idle or switched-off pass looks again.
pub const IDLE_POLL: Duration = Duration::from_secs(5);
/// A queued identity change whose check established nothing waits this
/// long before it is served again.
pub const DUE_RETRY_SECS: i64 = 3600;
/// First and longest wait after a batch that mostly failed.
pub const BACKOFF: (Duration, Duration) = (Duration::from_secs(60), Duration::from_secs(1800));

/// `farsight_handle_pass_total{outcome}`.
pub const PASS: &str = "farsight_handle_pass_total";
/// `farsight_handle_pass_position`: the `actors.id` the walk has reached.
pub const POSITION: &str = "farsight_handle_pass_position";
/// `farsight_handle_pass_laps_total`: walks completed since start.
pub const LAPS: &str = "farsight_handle_pass_laps_total";

/// What a check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checked {
    /// The handle, verified in both directions.
    Handle(String),
    /// Definitely nothing to show: the DID has no document, the document
    /// names no handle, or the handle it names belongs to another DID.
    Gone,
    /// The document names this handle, and the handle could not be
    /// resolved back.
    Unresolved(String),
    /// Nothing could be established: the directory or the host did not
    /// answer, refused, or the deadline passed.
    Unknown,
}

impl Checked {
    /// Metric label.
    pub fn label(&self) -> &'static str {
        match self {
            Checked::Handle(_) => "handle",
            Checked::Gone => "gone",
            Checked::Unresolved(_) => "unresolved",
            Checked::Unknown => "unknown",
        }
    }
}

/// Registers the series at zero.
pub fn register() {
    for label in ["handle", "gone", "unresolved", "unknown"] {
        ::metrics::counter!(PASS, "outcome" => label).increment(0);
    }
    ::metrics::counter!(LAPS).increment(0);
    ::metrics::gauge!(POSITION).set(0.0);
}

/// Checks the handle of `did`: its document, then the handle it names,
/// resolved back.
///
/// With an answer that is not a verified handle comes why, for the log.
async fn check(st: &WebState, cfg: &Config, did: &Did) -> (Checked, Option<String>) {
    let Some(url) = document_url(cfg, did) else {
        return (Checked::Gone, None);
    };
    let r = match st.safe.get(&url).await {
        Ok(r) => r,
        Err(e) => return (Checked::Unknown, Some(format!("document: {e}"))),
    };
    let doc = match r.status {
        200 => serde_json::from_slice::<serde_json::Value>(&r.body).ok(),
        404 | 410 => return (Checked::Gone, None),
        other => return (Checked::Unknown, Some(format!("document: HTTP {other}"))),
    };
    let Some(doc) = doc else {
        return (Checked::Unknown, Some("document: not JSON".into()));
    };
    let Some(claim) = claimed_handle(&doc) else {
        return (Checked::Gone, None);
    };
    match resolve_handle(&st.safe, &claim).await {
        Ok(back) if back == *did => (Checked::Handle(claim), None),
        Ok(_) => (Checked::Gone, None),
        // The handle's host answered, and not with this DID: the claim
        // stands unproven. Anything else (no answer, no address, a
        // timeout) proves nothing about the account.
        Err(e) if answered(&e) => (Checked::Unresolved(claim), Some(e)),
        Err(e) => (Checked::Unknown, Some(e)),
    }
}

/// The domain a handle that could not be resolved is under (`pds.example`
/// for `alice.pds.example`), read from the resolution's error text.
fn failed_under(why: &str) -> Option<String> {
    let handle = why.strip_prefix("could not resolve ")?.split(':').next()?;
    handle.split_once('.').map(|(_, parent)| parent.to_owned())
}

/// Whether the checks of a batch that established nothing point at one
/// place: more than half of them are handles under the same domain. One
/// host that is down says nothing about the rest, and the pass goes on.
fn one_host(failed: &[Option<String>]) -> bool {
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for under in failed.iter().flatten() {
        *counts.entry(under.as_str()).or_default() += 1;
    }
    counts
        .values()
        .max()
        .is_some_and(|most| most * 2 > failed.len())
}

/// Whether a failed handle resolution got an answer from the handle's
/// host (an HTTP status, or a body that is not a DID), as opposed to no
/// answer at all.
fn answered(error: &str) -> bool {
    error.contains(": HTTP ") || error.ends_with("did not resolve to a DID")
}

/// Checks that established nothing, counted: every [`LOG_EVERY`]th is
/// logged with its reason.
static UNESTABLISHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// One in this many checks that established nothing is logged.
pub const LOG_EVERY: u64 = 50;

/// Stores what a check found. The memory cache is touched only where it
/// already holds the account: the pass must not push the accounts pages
/// are showing out of it.
async fn record(
    st: &WebState,
    cfg: &Config,
    did: &Did,
    found: &Checked,
) -> farsight_storage::Result<()> {
    let mut conn = st.api.pool.acquire().await?;
    match found {
        Checked::Handle(h) => store::store(&mut conn, did.as_str(), h).await?,
        Checked::Gone => store::store_gone(&mut conn, did.as_str()).await?,
        Checked::Unresolved(claim) => {
            store::store_unresolved(&mut conn, did.as_str(), claim).await?;
        }
        Checked::Unknown => store::store_unknown(&mut conn, did.as_str()).await?,
    }
    drop(conn);
    let cache = &st.public.handles;
    if cache.lookup_stale(did.as_str()).is_some() {
        match found {
            Checked::Handle(h) => cache.insert(
                did.as_str(),
                Some(h.clone()),
                cfg.public_ui.handle_cache_ttl.get(),
            ),
            Checked::Gone => cache.insert(did.as_str(), None, NEGATIVE_TTL),
            Checked::Unresolved(_) | Checked::Unknown => {}
        }
    }
    Ok(())
}

/// One account to check; `queued` when it came from `handle_due`,
/// `again` when an earlier check of it in this run established nothing.
#[derive(Debug)]
struct Item {
    did: String,
    queued: bool,
    again: bool,
}

/// Checks one account and stores the answer. Returns the account when
/// nothing was established and nothing was stored: the caller checks it
/// once more later. With it comes the domain the unanswered handle is
/// under, where the failure was the handle's host.
async fn check_one(st: Arc<WebState>, item: Item) -> Option<(String, Option<String>)> {
    let cfg = st.api.config.current();
    let cfg = &cfg.config;
    let Ok(did) = Did::parse(&item.did) else {
        return None;
    };
    let (found, why) = tokio::time::timeout(CHECK_DEADLINE, check(&st, cfg, &did))
        .await
        .unwrap_or((Checked::Unknown, Some("deadline".into())));
    let under = why.as_deref().and_then(failed_under);
    if let Some(why) = why {
        let n = UNESTABLISHED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if n % LOG_EVERY == 0 {
            tracing::warn!(
                outcome = found.label(),
                why,
                seen = n + 1,
                "handle pass: a check established nothing (one in {LOG_EVERY} is logged)"
            );
        }
    }
    ::metrics::counter!(PASS, "outcome" => found.label()).increment(1);
    // A first check that established nothing stores nothing: the cause
    // is as likely here (the resolver, the network) as there, and the
    // account is checked again after the pass has waited.
    if found == Checked::Unknown && !item.again && !item.queued {
        return Some((item.did, under));
    }
    if let Err(e) = record(&st, cfg, &did, &found).await {
        tracing::warn!(error = %e, "handle pass: an answer could not be stored");
        return None;
    }
    if item.queued {
        let done = match st.api.pool.acquire().await {
            Ok(mut conn) if found == Checked::Unknown => {
                store::due_later(&mut conn, did.as_str(), DUE_RETRY_SECS).await
            }
            Ok(mut conn) => store::due_done(&mut conn, did.as_str()).await,
            Err(e) => Err(e.into()),
        };
        if let Err(e) = done {
            tracing::warn!(error = %e, "handle pass: the queue could not be updated");
        }
    }
    None
}

/// How far the pass and the list filler have got since the server
/// started, for the dashboard.
#[derive(Debug, Default)]
pub struct Progress {
    /// The `actors.id` the walk has reached.
    pub cursor: AtomicI64,
    /// The newest `actors.id` when the walk last looked.
    pub last: AtomicI64,
    /// Walks completed.
    pub laps: AtomicU64,
    /// Rounds of the list filler completed.
    pub list_rounds: AtomicU64,
}

/// Where the walk is.
#[derive(Debug, Default)]
struct Walk {
    /// The last `actors.id` looked at.
    cursor: i64,
    /// When the walk last reached the end.
    rested_since: Option<Instant>,
}

/// The next accounts to check: queued identity changes, else the next of
/// the walk. Empty when there is nothing to do now.
async fn next_items(st: &WebState, walk: &mut Walk) -> farsight_storage::Result<Vec<Item>> {
    let mut conn = st.api.pool.acquire().await?;
    let due = store::due_take(&mut conn, DUE_BATCH).await?;
    if !due.is_empty() {
        return Ok(due
            .into_iter()
            .map(|did| Item {
                did,
                queued: true,
                again: false,
            })
            .collect());
    }
    if let Some(since) = walk.rested_since {
        if since.elapsed() < LAP_REST {
            return Ok(Vec::new());
        }
        walk.rested_since = None;
        walk.cursor = 0;
    }
    let last = store::last_actor_id(&mut conn).await?;
    st.public.progress.last.store(last, Ordering::Relaxed);
    for _ in 0..WINDOWS_PER_STEP {
        if walk.cursor >= last {
            walk.rested_since = Some(Instant::now());
            st.public.progress.laps.fetch_add(1, Ordering::Relaxed);
            ::metrics::counter!(LAPS).increment(1);
            tracing::info!(accounts = last, "handle pass: reached the last account");
            return Ok(Vec::new());
        }
        let (rows, next) = store::pass_batch(
            &mut conn,
            walk.cursor,
            WINDOW,
            BATCH,
            NONE_RETRY.as_secs() as i64,
        )
        .await?;
        walk.cursor = next;
        st.public.progress.cursor.store(next, Ordering::Relaxed);
        ::metrics::gauge!(POSITION).set(next as f64);
        if !rows.is_empty() {
            return Ok(rows
                .into_iter()
                .map(|did| Item {
                    did,
                    queued: false,
                    again: false,
                })
                .collect());
        }
    }
    // Nothing due in these windows; the next step goes on from here.
    Ok(Vec::new())
}

async fn pause(stop: &mut watch::Receiver<bool>, d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => false,
        r = stop.changed() => r.is_err() || *stop.borrow(),
    }
}

/// The next wait after a batch that mostly failed.
fn backed_off(last: Duration) -> Duration {
    (last * 2).clamp(BACKOFF.0, BACKOFF.1)
}

/// The worker: one long-running task, started with the server's
/// background tasks. Ends when `stop` flips.
pub async fn run(st: Arc<WebState>, mut stop: watch::Receiver<bool>) {
    let slots = Arc::new(Semaphore::new(IN_FLIGHT));
    let mut walk = Walk::default();
    let mut backoff = Duration::ZERO;
    let mut was_on = false;
    // Accounts whose first check established nothing, for one more.
    let mut retry: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    loop {
        if *stop.borrow() {
            return;
        }
        let rps = st.api.config.current().config.public_ui.handle_pass_rps;
        if rps == 0 {
            was_on = false;
            if pause(&mut stop, IDLE_POLL).await {
                return;
            }
            continue;
        }
        if !was_on {
            was_on = true;
            tracing::info!(rps, from = walk.cursor, "handle pass: running");
        }
        // Second checks come after the wait that followed their first,
        // a batch at a time, before the walk goes on.
        let items = if backoff.is_zero() && !retry.is_empty() {
            let n = retry.len().min(BATCH as usize);
            retry
                .drain(..n)
                .map(|did| Item {
                    did,
                    queued: false,
                    again: true,
                })
                .collect()
        } else {
            match next_items(&st, &mut walk).await {
                Ok(items) => items,
                Err(e) => {
                    tracing::warn!(error = %e, "handle pass: the next accounts could not be read");
                    Vec::new()
                }
            }
        };
        if items.is_empty() {
            // Between windows with nothing due the walk moves at once;
            // resting or failing, it waits.
            let wait = if walk.rested_since.is_none() && walk.cursor > 0 {
                Duration::from_millis(20)
            } else {
                IDLE_POLL
            };
            if pause(&mut stop, wait).await {
                return;
            }
            continue;
        }
        let tick = Duration::from_secs_f64(1.0 / f64::from(rps));
        let mut checks = JoinSet::new();
        for item in items {
            if pause(&mut stop, tick).await {
                return;
            }
            let Ok(slot) = slots.clone().acquire_owned().await else {
                return;
            };
            let st2 = st.clone();
            checks.spawn(async move {
                let unsettled = check_one(st2, item).await;
                drop(slot);
                unsettled
            });
        }
        let mut all = 0u32;
        let mut failed: Vec<Option<String>> = Vec::new();
        while let Some(r) = checks.join_next().await {
            all += 1;
            if let Ok(Some((did, under))) = r {
                failed.push(under);
                if retry.len() < RETRY_CAP {
                    retry.push_back(did);
                }
            }
        }
        let unknown = failed.len() as u32;
        if all >= 20 && unknown * 2 > all && !one_host(&failed) {
            backoff = backed_off(backoff);
            tracing::warn!(
                failed = unknown,
                of = all,
                wait_secs = backoff.as_secs(),
                "handle pass: most checks established nothing; waiting"
            );
            if pause(&mut stop, backoff).await {
                return;
            }
        } else if all > 0 {
            backoff = Duration::ZERO;
        }
    }
}

/// How long the list filler waits between two lists.
pub const LIST_EVERY: Duration = Duration::from_secs(1);
/// How long it rests after the last list.
pub const LIST_LAP_REST: Duration = Duration::from_secs(3600);

/// Reads the record of every list stored before descriptions were kept
/// (`lists.about_read = false`), one a second, while the handle pass is
/// on. A list whose owner's server does not answer is left for the next
/// round, an hour after this one ends.
pub async fn run_lists(st: Arc<WebState>, mut stop: watch::Receiver<bool>) {
    let mut cursor = 0i64;
    loop {
        if *stop.borrow() {
            return;
        }
        let cfg = st.api.config.current();
        if cfg.config.public_ui.handle_pass_rps == 0 {
            if pause(&mut stop, IDLE_POLL).await {
                return;
            }
            continue;
        }
        let batch = match st.api.pool.acquire().await {
            Ok(mut conn) => farsight_storage::queries::lists_unread(&mut conn, cursor, 50).await,
            Err(e) => Err(e.into()),
        };
        let batch = match batch {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "list filler: the next lists could not be read");
                Vec::new()
            }
        };
        if batch.is_empty() {
            cursor = 0;
            st.public
                .progress
                .list_rounds
                .fetch_add(1, Ordering::Relaxed);
            if pause(&mut stop, LIST_LAP_REST).await {
                return;
            }
            continue;
        }
        for (id, owner, rkey) in batch {
            cursor = id;
            if pause(&mut stop, LIST_EVERY).await {
                return;
            }
            let Ok(did) = Did::parse(&owner) else {
                continue;
            };
            let Some((description, avatar)) =
                super::card::list_about_unbudgeted(&st, &cfg.config, &did, &rkey).await
            else {
                continue;
            };
            let stored = match st.api.pool.acquire().await {
                Ok(mut conn) => {
                    farsight_storage::queries::list_about_fill(
                        &mut conn,
                        id,
                        description.as_deref(),
                        avatar.as_deref(),
                    )
                    .await
                }
                Err(e) => Err(e.into()),
            };
            if let Err(e) = stored {
                tracing::warn!(error = %e, "list filler: a description could not be stored");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_answer_from_the_host_leaves_a_claim_unresolved() {
        assert!(answered("could not resolve a.example: HTTP 404"));
        assert!(answered("a.example did not resolve to a DID"));
        assert!(!answered(
            "could not resolve a.example: transport error: error sending request"
        ));
        assert!(!answered("could not resolve a.example: request timed out"));
    }

    #[test]
    fn failures_under_one_domain_are_one_host() {
        let under = |h: &str| failed_under(&format!("could not resolve {h}: request timed out"));
        assert_eq!(under("a.pds.example").as_deref(), Some("pds.example"));
        assert_eq!(failed_under("document: HTTP 429"), None);
        let dead: Vec<_> = (0..30)
            .map(|i| under(&format!("u{i}.pds.example")))
            .collect();
        assert!(one_host(&dead));
        let mixed: Vec<_> = (0..30)
            .map(|i| under(&format!("u.host{i}.example")))
            .collect();
        assert!(!one_host(&mixed));
        // The directory failing names no handle at all.
        assert!(!one_host(&vec![None; 30]));
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
