//! The dashboard, its htmx fragment and the bar's alerts, with coverage
//! in words.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use askama::Template;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use chrono::{DateTime, Utc};
use farsight_api::handlers::{self, SweepState};
use farsight_api::params::Params;
use farsight_storage::codes::{CycleSource, GapCause, Protocol};
use farsight_storage::coverage::Level;
use farsight_storage::ids::CycleId;
use serde_json::Value;

use super::{Nav, WebState, gate, nav};
use crate::common::{self, render_private};

// ---------------------------------------------------------------------------
// Coverage in words

/// A coverage reason code (`coverage.reasons`) in words for an admin
/// page. A code this build does not know reads as an unrecognized
/// condition, treated as partial.
pub fn reason_words(r: &str) -> &'static str {
    match r {
        "sweep_incomplete" => "the first full sweep has not completed",
        "firehose_gap" => "a firehose gap is not repaired yet",
        "firehose_disconnected" => "the firehose is disconnected",
        "coverage_stale" => "the coverage state could not be read from the database",
        "firehose_lagging" => "the firehose is more than 5 minutes behind",
        "sync_events_unavailable" => "the Jetstream instance is v1 (no #sync events)",
        "storage_refusal" => "the storage budget is refusing writes",
        "list_pending" => "a list is still being fetched",
        "list_pending_historical" => "a newly admitted list has listblocks older than the stream",
        "list_capped" => "a list's stored members hit a cap",
        "list_unavailable" => "a list could not be fetched (retrying)",
        "list_missing" => "a list record was not found",
        "list_deferred" => "a list's admission is deferred by a storage gate",
        "list_not_tracked" => "nobody listblocks this list, so its members are not stored",
        "discovery_truncated" => "backlink discovery stopped at its reference cap",
        "party_debt" => "an account in this answer is waiting for a re-list",
        _ => "an unrecognized condition (treated as partial)",
    }
}

/// A `freshness` object in words (dashboard and lookups).
pub fn coverage_words(f: &Value) -> String {
    let c = &f["coverage"];
    let level = c["level"].as_str().unwrap_or("partial");
    let head = match level {
        "complete" => "Complete",
        "assisted" => "Assisted (via backlink discovery)",
        _ => "Partial",
    };
    let since = c["completeSince"]
        .as_str()
        .map(|s| format!(" since {}", short_ts(s)))
        .unwrap_or_default();
    let reasons: Vec<&str> = c["reasons"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(reason_words)
                .collect()
        })
        .unwrap_or_default();
    let indexed = f["indexedAt"]
        .as_str()
        .map(|s| format!(" Reflects everything witnessed up to {}.", short_ts(s)))
        .unwrap_or_default();
    if reasons.is_empty() {
        format!("{head}{since}.{indexed}")
    } else {
        let joiner = if level == Level::Complete.api_name() {
            "; excluding: "
        } else {
            ": "
        };
        format!("{head}{since}{joiner}{}.{indexed}", reasons.join("; "))
    }
}

fn short_ts(s: &str) -> String {
    DateTime::parse_from_rfc3339(s)
        .map(|t| {
            t.with_timezone(&Utc)
                .format("%Y-%m-%d %H:%M:%S UTC")
                .to_string()
        })
        .unwrap_or_else(|_| s.to_owned())
}

// ---------------------------------------------------------------------------
// Dashboard

/// One labelled value.
#[derive(Debug, Clone)]
pub struct Stat {
    /// What is measured, as the page words it.
    pub label: String,
    /// The value already formatted for display: a count with thousands
    /// separators, a size, a percentage, or words.
    pub value: String,
}

/// One API endpoint on the dashboard.
#[derive(Debug, Clone)]
pub struct ApiRow {
    /// `query.checkBlocks`.
    pub name: &'static str,
    /// Requests answered since the server started, whatever the status,
    /// with thousands separators (as are the next two).
    pub requests: String,
    /// Errors other than rate limits.
    pub errors: String,
    /// Rate-limited requests.
    pub limited: String,
    /// When it was last called.
    pub last: Option<crate::public::text::Stamp>,
    /// It has been called at all.
    pub used: bool,
}

/// A warning banner.
#[derive(Debug, Clone)]
pub struct Warning {
    /// `bad` or empty.
    pub class: &'static str,
    /// The banner's text. Plain; a `YYYY-MM-DD HH:MM UTC` in it is shown
    /// in the browser's timezone.
    pub text: String,
}

/// Everything the dashboard shows.
#[derive(Debug, Clone, Default)]
pub struct DashboardData {
    /// Warnings (Cloudflare share, budget, v1, gaps).
    pub warnings: Vec<Warning>,
    /// Work still catching up, in words; empty when there is none.
    pub catching: Vec<Stat>,
    /// `getStats.counts`: blocks, listblocks, lists, tracked lists, list
    /// items and accounts known.
    pub counts: Vec<Stat>,
    /// The firehose as `getStats` reports it: connected, protocol, lag
    /// and source lag in seconds, then how many unhealed gaps are closed
    /// (repairable) and how many are still open.
    pub firehose: Vec<Stat>,
    /// Every unhealed gap as `from → to (cause)`, with `open` for a gap
    /// that has no end yet.
    pub gaps: Vec<String>,
    /// The latest sweep cycle (source, state, progress, ETA), the queue
    /// depth of each tier and repos listed in the last hour.
    pub backfill: Vec<Stat>,
    /// `coverage.exceptions` of the global scope under the API's own
    /// field names, and `pendingLists`.
    pub exceptions: Vec<Stat>,
    /// The global `freshness` object as a sentence ([`coverage_words`]).
    pub coverage: String,
    /// Database size, budget, share of the budget, hard ceiling and the
    /// history tables' size. Empty until the budget monitor has measured
    /// once.
    pub storage: Vec<Stat>,
    /// Lists per tracking state, labelled with the state's API name.
    pub lists: Vec<Stat>,
    /// API endpoints and what each has been asked since start.
    pub api: Vec<ApiRow>,
    /// When that counting began.
    pub api_since: Option<crate::public::text::Stamp>,
    /// The ten cap buckets that interned most over their lifetime, as
    /// (bucket, accounts interned, "N blocks · M items", `capped` or
    /// empty).
    pub buckets: Vec<(String, String, String, String)>,
}

/// The repair cycle under way before a new one is asked for, as (id,
/// accounts re-read so far): asking again joins it.
pub(super) async fn repair_under_way(st: &WebState) -> Option<(CycleId, i64)> {
    let mut conn = st.api.pool.acquire().await.ok()?;
    farsight_storage::queries::repair_running(&mut conn)
        .await
        .ok()
        .flatten()
        .map(|r| (r.0, r.1))
}

/// The warning about firehose gaps, if one is due. A closed gap is healed
/// by a repair cycle: `repair` is the one under way, as (id, accounts
/// re-read so far). A gap that is open because the firehose is down
/// closes when it reconnects, and only then can be repaired. The open
/// interval of a v1 firehose is the v1 warning's subject, not this one's.
fn gap_warning(
    repairable: i64,
    open: i64,
    repair: Option<(CycleId, i64)>,
    switches: (bool, bool),
) -> Option<String> {
    let (paused, auto) = switches;
    let s = |n: i64| if n == 1 { "" } else { "s" };
    let closed = match (repairable, repair) {
        (0, _) => None,
        (r, Some((id, done))) if paused => Some(format!(
            "{r} firehose gap{} waiting on repair cycle {id}, which is paused after re-reading \
             {} accounts (Operations → resume repair).",
            if r == 1 { " is" } else { "s are" },
            crate::public::text::thousands(done)
        )),
        (r, Some((id, done))) => Some(format!(
            "{r} firehose gap{} being repaired: repair cycle {id} has re-read {} accounts so \
             far. A repair re-reads every account that changed during the gap, so a long gap \
             takes days; this alert clears when it finishes.",
            if r == 1 { " is" } else { "s are" },
            crate::public::text::thousands(done)
        )),
        (r, None) if auto => Some(format!(
            "{r} firehose gap{} can be repaired: a repair cycle starts by itself shortly.",
            s(r)
        )),
        (r, None) => Some(format!(
            "{r} firehose gap{} can be repaired: start a repair cycle (Operations → start repair).",
            s(r)
        )),
    };
    let still = (open > 0).then(|| {
        if closed.is_some() {
            format!(
                "{open} more {} still open and cannot be repaired yet.",
                if open == 1 { "is" } else { "are" }
            )
        } else {
            format!(
                "{open} firehose gap{} still open: it closes when the firehose reconnects, and \
                 can be repaired after that.",
                if open == 1 { " is" } else { "s are" }
            )
        }
    });
    match (closed, still) {
        (None, None) => None,
        (Some(c), Some(o)) => Some(format!("{c} {o}")),
        (c, o) => c.or(o),
    }
}

/// A duration in words, to two units and no less than a minute: "1 day
/// 4 hours".
fn long_secs(s: i64) -> String {
    let unit = |n: i64, w: &str| format!("{n} {w}{}", if n == 1 { "" } else { "s" });
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3600, (s % 3600) / 60);
    if d > 0 && h > 0 {
        format!("{} {}", unit(d, "day"), unit(h, "hour"))
    } else if d > 0 {
        unit(d, "day")
    } else if h > 0 && m > 0 {
        format!("{} {}", unit(h, "hour"), unit(m, "minute"))
    } else if h > 0 {
        unit(h, "hour")
    } else {
        unit(m.max(1), "minute")
    }
}

/// One line of the dashboard's "Catching up": the share done of `total`
/// and the time the rest takes.
fn catching_line(done: i64, total: i64, what: &str, left_secs: i64) -> String {
    // Never "100.0%" while something is left.
    let share = (done.max(0) as f64 / total.max(1) as f64 * 100.0).min(99.9);
    format!(
        "{share:.1}% {what}, about {} left",
        long_secs(left_secs.max(0))
    )
}

/// The handle pass's line: none while it is off, before its first step
/// and once it has reached the last account. The time left is what the
/// accounts ahead take at the configured rate; accounts already answered
/// are passed over, so it is an upper bound.
fn handles_line(rps: u32, cursor: i64, last: i64, laps: u64) -> Option<String> {
    if rps == 0 || laps > 0 || last <= 0 || cursor >= last {
        return None;
    }
    Some(catching_line(
        cursor,
        last,
        "of accounts checked",
        (last - cursor) / i64::from(rps),
    ))
}

/// A count for an admin page, with thousands separators.
pub(crate) fn count(label: &str, n: i64) -> Stat {
    stat(label, crate::public::text::thousands(n))
}

pub(super) fn stat(label: &str, value: impl ToString) -> Stat {
    Stat {
        label: label.into(),
        value: value.to_string(),
    }
}

/// Why the dashboard's data could not be read. The text is shown on the
/// dashboard.
#[derive(Debug, thiserror::Error)]
pub enum DashboardError {
    /// `getStats` failed, with its message.
    #[error("{0}")]
    Api(String),
    /// A statement failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// The storage layer failed.
    #[error(transparent)]
    Storage(#[from] farsight_storage::StorageError),
}

async fn dashboard_data(st: &WebState) -> Result<DashboardData, DashboardError> {
    let stats = handlers::get_stats(&st.api, &Params::default())
        .await
        .map_err(|e| DashboardError::Api(e.message))?
        .body;
    let mut d = DashboardData::default();
    let c = &stats["counts"];
    for (k, l) in [
        ("blocks", "Blocks"),
        ("listBlocks", "Listblocks"),
        ("lists", "Lists"),
        ("trackedLists", "Tracked lists"),
        ("listItems", "List items"),
        ("actors", "Accounts known"),
    ] {
        d.counts.push(count(l, c[k].as_i64().unwrap_or(0)));
    }
    let f = &stats["firehose"];
    let connected = f["connected"].as_bool().unwrap_or(false);
    d.firehose
        .push(stat("Connected", if connected { "yes" } else { "no" }));
    let protocol = f["protocol"].as_str().unwrap_or("—");
    d.firehose.push(stat("Protocol", protocol));
    d.firehose.push(stat(
        "Lag",
        f["lagSeconds"]
            .as_f64()
            .map_or("—".into(), |s| format!("{s:.1} s")),
    ));
    d.firehose.push(stat(
        "Source lag",
        f["sourceLagSeconds"]
            .as_f64()
            .map_or("—".into(), |s| format!("{s:.1} s")),
    ));
    // Unhealed gaps are of three kinds: closed ones, which a repair
    // cycle heals; the interval of a v1 firehose, open until a v2
    // session takes over; and one open because the firehose is down.
    let (mut repairable, mut v1_open, mut other_open) = (0, 0, 0);
    if let Some(gaps) = stats["detail"]["gaps"].as_array() {
        for g in gaps {
            if g["to"].is_string() {
                repairable += 1;
            } else if g["cause"] == GapCause::SyncUnavailable.api_name() {
                v1_open += 1;
            } else {
                other_open += 1;
            }
            d.gaps.push(format!(
                "{} → {} ({})",
                g["from"].as_str().map(short_ts).unwrap_or_default(),
                g["to"]
                    .as_str()
                    .map(short_ts)
                    .unwrap_or_else(|| "open".into()),
                g["cause"].as_str().unwrap_or("")
            ));
        }
    }
    d.firehose.push(count("Gaps to repair", repairable));
    d.firehose
        .push(count("Gaps still open", v1_open + other_open));
    let b = &stats["backfill"];
    if let Some(s) = b.get("sweep") {
        d.backfill.push(stat(
            "Sweep",
            format!(
                "cycle {} · {} · {}",
                s["cycle"],
                s["source"].as_str().unwrap_or(""),
                s["state"].as_str().unwrap_or("")
            ),
        ));
        if let Some(p) = s["progress"].as_f64() {
            d.backfill
                .push(stat("Progress", format!("{:.1}%", p * 100.0)));
        }
        if let Some(e) = s["etaSeconds"].as_i64() {
            d.backfill.push(stat("ETA", common::human_secs(e)));
        }
        // The latest cycle is a repair while one runs; its own line is
        // added below, from the cycle itself.
        if s["state"] != SweepState::Completed.api_name()
            && s["source"] != CycleSource::RelayRepos.as_str()
            && let Some(p) = s["progress"].as_f64()
        {
            let mut line = format!("{:.1}% of accounts swept", (p * 100.0).min(99.9));
            if s["state"] == SweepState::Paused.api_name() {
                line.push_str(", paused");
            } else if let Some(e) = s["etaSeconds"].as_i64() {
                line.push_str(&format!(", about {} left", long_secs(e)));
            }
            d.catching.push(stat("History", line));
        }
    } else {
        d.backfill.push(stat("Sweep", "not started"));
    }
    let q = stats["detail"]["queueByTier"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for (i, l) in ["Queue: on-demand", "Queue: active", "Queue: sweep"]
        .iter()
        .enumerate()
    {
        d.backfill
            .push(count(l, q.get(i).and_then(Value::as_i64).unwrap_or(0)));
    }
    d.backfill
        .push(count("Repos/hour", b["reposPerHour"].as_i64().unwrap_or(0)));
    let ex = &stats["freshness"]["coverage"]["exceptions"];
    if let Some(m) = ex.as_object() {
        for (k, v) in m {
            d.exceptions.push(count(k, v.as_i64().unwrap_or(0)));
        }
    }
    d.exceptions.push(count(
        "pendingLists",
        stats["freshness"]["coverage"]["pendingLists"]
            .as_i64()
            .unwrap_or(0),
    ));
    d.coverage = coverage_words(&stats["freshness"]);
    let thousands = |n: u64| crate::public::text::thousands(i64::try_from(n).unwrap_or(i64::MAX));
    d.api = st
        .api
        .usage
        .snapshot()
        .into_iter()
        .map(|u| ApiRow {
            name: u.endpoint.name(),
            requests: thousands(u.requests),
            errors: thousands(u.errors),
            limited: thousands(u.limited),
            last: u.last.map(crate::public::text::Stamp::of),
            used: u.requests > 0,
        })
        .collect();
    d.api_since = Some(crate::public::text::Stamp::of(st.api.usage.since));

    let mut conn = st.api.pool.acquire().await?;
    let repair = farsight_storage::queries::repair_running(&mut conn).await?;
    if let Some((_, done, listed)) = repair {
        d.catching.push(stat(
            "Gap repair",
            format!(
                "{} accounts re-read so far{}",
                crate::public::text::thousands(done),
                if listed {
                    ""
                } else {
                    "; how many are left is not known until the relay's list has been read"
                }
            ),
        ));
    }
    let rps = st.api.config.current().config.public_ui.handle_pass_rps;
    let at = &st.public.progress;
    if let Some(line) = handles_line(
        rps,
        at.cursor.load(Ordering::Relaxed),
        at.last.load(Ordering::Relaxed),
        at.laps.load(Ordering::Relaxed),
    ) {
        d.catching.push(stat("Handles", line));
    }
    for (s, n) in farsight_storage::queries::lists_by_state(&mut conn).await? {
        d.lists.push(count(s.api_name(), n));
    }
    for (bucket, interned, blocks, items, _, _, mask) in
        farsight_storage::queries::top_buckets(&mut conn, 10).await?
    {
        d.buckets.push((
            bucket,
            crate::public::text::thousands(interned),
            format!(
                "{} blocks · {} items",
                crate::public::text::thousands(blocks),
                crate::public::text::thousands(items)
            ),
            if mask != 0 {
                "capped".into()
            } else {
                String::new()
            },
        ));
    }
    drop(conn);

    let s = st.status.get();
    if let Some(db) = s.db_bytes {
        let ratio = if s.budget_bytes > 0 {
            db as f64 / s.budget_bytes as f64
        } else {
            0.0
        };
        d.storage.push(stat("Database", common::human_bytes(db)));
        d.storage
            .push(stat("Budget", common::human_bytes(s.budget_bytes)));
        d.storage
            .push(stat("Of budget", format!("{:.1}%", ratio * 100.0)));
        d.storage
            .push(stat("Hard ceiling", common::human_bytes(s.ceiling_bytes)));
        if let Some(h) = s.history_bytes {
            d.storage
                .push(stat("History (in the budget)", common::human_bytes(h)));
        }
        if s.gate.critical {
            d.warnings.push(Warning {
                class: "bad",
                text: format!(
                    "Storage is at {:.0}% of the budget (critical). Creates and updates are \
                     refused; raise storage.budget_bytes after adding disk. Postgres does not \
                     shrink after deletes without VACUUM FULL or pg_repack.",
                    ratio * 100.0
                ),
            });
        } else if s.gate.gates.budget_refusing || s.gate.gates.ceiling_refusing {
            d.warnings.push(Warning {
                class: "bad",
                text: format!(
                    "Storage budget reached ({:.0}%): new list admissions are deferred and \
                     writes from non-large hosts are refused (and counted). Raise the budget \
                     after adding disk; `VACUUM FULL` or `pg_repack` reclaims space after deletes.",
                    ratio * 100.0
                ),
            });
        } else if s.gate.sweep_paused {
            d.warnings.push(Warning {
                class: "",
                text: format!(
                    "Storage at {:.0}% of budget: the sweep is paused.",
                    ratio * 100.0
                ),
            });
        } else if ratio >= 0.8 {
            d.warnings.push(Warning {
                class: "",
                text: format!("Storage at {:.0}% of budget.", ratio * 100.0),
            });
        }
    }
    if let Some(g) = s.growth_warning {
        d.warnings.push(Warning { class: "", text: g });
    }
    if let Some(share) = st.api.cf.warning() {
        d.warnings.push(Warning {
            class: "bad",
            text: format!(
                "{:.0}% of requests in the last 5 minutes came from Cloudflare edges, but \
                 Cloudflare is not trusted: every client shares a few rate-limit buckets. Set \
                 the reverse proxy to Cloudflare in Settings (proxy.mode, proxy.trusted).",
                share * 100.0
            ),
        });
    }
    if !connected {
        d.warnings.push(Warning {
            class: "bad",
            text: "The firehose is disconnected; coverage is partial until it reconnects.".into(),
        });
    }
    if protocol == Protocol::V1.api_name() {
        d.warnings.push(Warning {
            class: "",
            text: "The firehose is v1: coverage is capped at partial (sync_events_unavailable). \
                   Use a v2 Jetstream for complete coverage. The time spent on v1 is recorded \
                   as one gap, which stays open, and cannot be repaired, until a v2 Jetstream \
                   takes over."
                .into(),
        });
    }
    let switches = {
        let cfg = st.api.config.current();
        (
            cfg.config.backfill.repair.paused,
            cfg.config.backfill.repair.auto_start,
        )
    };
    if let Some(text) = gap_warning(repairable, other_open, repair.map(|r| (r.0, r.1)), switches) {
        d.warnings.push(Warning { class: "", text });
    }
    Ok(d)
}

/// The dashboard.
#[derive(Template)]
#[template(path = "dashboard.html")]
pub struct DashboardPage {
    /// Navigation.
    pub nav: Nav,
    /// What the page shows; all empty when `error` is set.
    pub d: DashboardData,
    /// Why the data could not be read, shown in place of it.
    pub error: Option<String>,
}

/// The htmx fragment: the dashboard's body without the page around it,
/// which the page swaps in when it polls.
#[derive(Template)]
#[template(path = "dashboard_fragment.html")]
pub struct DashboardFragment {
    /// What the fragment shows; all empty when `error` is set.
    pub d: DashboardData,
    /// Why the data could not be read, shown in place of it.
    pub error: Option<String>,
}

pub(super) async fn dashboard(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    let s = match gate(&st, &headers).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    let (d, error) = match dashboard_data(&st).await {
        Ok(d) => (d, None),
        Err(e) => (DashboardData::default(), Some(e.to_string())),
    };
    render_private(&DashboardPage {
        nav: nav(&Some(s)),
        d,
        error,
    })
}

/// Tells the admin that the tables do not all sort by creation
/// time yet: the sort indexes are still being built, or are held because
/// the storage budget has no room for them.
fn sort_warning(st: &WebState, d: &mut DashboardData) {
    let ready = st.sort.count();
    if ready == 4 {
        return;
    }
    let text = match st.status.get().sort_held_bytes {
        Some(need) => format!(
            "Sorting by creation time: {ready} of 4 indexes ready; the rest are not built, the \
             storage budget has no room (needs ~{}). A table without its index lists its \
             rows by account, not by creation time. Raise storage.budget_bytes after adding disk.",
            common::human_bytes(need)
        ),
        None => format!(
            "Sorting by creation time: {ready} of 4 indexes ready. They build in the \
             background; until its index is ready a table lists its rows by account."
        ),
    };
    d.warnings.push(Warning { class: "", text });
}

pub(super) async fn dashboard_fragment(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) = gate(&st, &headers).await {
        return r;
    }
    let (d, error) = match dashboard_data(&st).await {
        Ok(d) => (d, None),
        Err(e) => (DashboardData::default(), Some(e.to_string())),
    };
    render_private(&DashboardFragment { d, error })
}

/// What the bar's "Alerts" holds: the warnings and the coverage
/// sentence. Every admin page's bar loads it, and again every 30 s.
#[derive(Template)]
#[template(path = "alerts_fragment.html")]
pub struct AlertsFragment {
    /// Warnings (Cloudflare share, budget, v1, gaps, sort indexes).
    pub warnings: Vec<Warning>,
    /// The global `freshness` object as a sentence; empty when it could
    /// not be read.
    pub coverage: String,
    /// Why the data could not be read. Counts as something that needs
    /// attention: the bar's badge stays visible.
    pub error: Option<String>,
}

pub(super) async fn alerts(State(st): State<Arc<WebState>>, headers: HeaderMap) -> Response {
    if let Err(r) = gate(&st, &headers).await {
        return r;
    }
    let (mut d, error) = match dashboard_data(&st).await {
        Ok(d) => (d, None),
        Err(e) => (DashboardData::default(), Some(e.to_string())),
    };
    sort_warning(&st, &mut d);
    render_private(&AlertsFragment {
        warnings: d.warnings,
        coverage: d.coverage,
        error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gap_warnings() {
        let (manual, auto) = ((false, false), (false, true));
        // The open interval of a v1 firehose alone raises none.
        assert_eq!(gap_warning(0, 0, None, auto), None);
        assert_eq!(
            gap_warning(1, 0, None, manual).as_deref(),
            Some(
                "1 firehose gap can be repaired: start a repair cycle (Operations → start repair)."
            )
        );
        // With automatic repairs on there is nothing for the admin to do.
        assert_eq!(
            gap_warning(1, 0, None, auto).as_deref(),
            Some("1 firehose gap can be repaired: a repair cycle starts by itself shortly.")
        );
        assert!(
            gap_warning(0, 1, None, auto)
                .unwrap()
                .starts_with("1 firehose gap is still open")
        );
        assert!(
            gap_warning(2, 1, None, manual)
                .unwrap()
                .ends_with("1 more is still open and cannot be repaired yet.")
        );
        // A repair under way is not something to start.
        let running = gap_warning(2, 0, Some((CycleId::new(2), 136_442)), auto).unwrap();
        assert!(running.starts_with(
            "2 firehose gaps are being repaired: repair cycle 2 has re-read 136,442 accounts"
        ));
        assert!(!running.contains("start a repair"));
        // Held, it says so.
        let held = gap_warning(2, 0, Some((CycleId::new(2), 136_442)), (true, true)).unwrap();
        assert!(held.contains("repair cycle 2, which is paused") && held.contains("resume repair"));
        // A repair with nothing closed to heal says nothing.
        assert_eq!(gap_warning(0, 0, Some((CycleId::new(2), 5)), auto), None);
    }

    #[test]
    fn catching_up_lines() {
        assert_eq!(long_secs(100_800), "1 day 4 hours");
        assert_eq!(long_secs(86_400 * 12), "12 days");
        assert_eq!(long_secs(3_660), "1 hour 1 minute");
        assert_eq!(long_secs(59), "1 minute");
        assert_eq!(
            handles_line(10, 500_000, 10_000_000, 0).as_deref(),
            Some("5.0% of accounts checked, about 10 days 23 hours left")
        );
        // Off, not started, at the end, or a walk already completed.
        assert_eq!(handles_line(0, 5, 10, 0), None);
        assert_eq!(handles_line(10, 0, 0, 0), None);
        assert_eq!(handles_line(10, 10, 10, 0), None);
        assert_eq!(handles_line(10, 5, 10, 1), None);
        // Something left is never shown as all done.
        assert_eq!(
            handles_line(10, 9_999_999, 10_000_000, 0).as_deref(),
            Some("99.9% of accounts checked, about 1 minute left")
        );
    }

    #[test]
    fn coverage_sentence() {
        let f = json!({"indexedAt": "2026-10-01T00:00:00Z", "coverage": {
            "level": "partial", "reasons": ["sweep_incomplete", "sync_events_unavailable"]}});
        let s = coverage_words(&f);
        assert!(s.starts_with("Partial: the first full sweep"));
        assert!(s.contains("v1"));
        let f = json!({"coverage": {"level": "complete", "completeSince": "2026-09-01T00:00:00Z",
            "reasons": []}});
        assert_eq!(
            coverage_words(&f),
            "Complete since 2026-09-01 00:00:00 UTC."
        );
    }
}
