//! Lists the configured AppView no longer serves (see
//! `docs/design/list-indexing.md`, `[backfill.list_takedowns]`).
//!
//! An AppView's moderation can take a list down while its record stays in
//! its owner's repository; the AppView then stops enforcing it. With the
//! checks on, each list that is subscribed to as a block list is asked
//! about (`app.bsky.graph.getList`, one item) on a schedule. A list the
//! AppView says it does not have, twice and at least `confirm_after`
//! apart, while Farsight holds its record, is marked taken down
//! (`lists.appview_absent_since`). Farsight keeps counting it; the API
//! says which lists are marked, and the public pages show the mark and do
//! not open the list. A list served again is cleared at the next check.
//!
//! Every answer other than "served" or "not found" changes nothing but
//! the time of the next check: an AppView that is down, slow or limiting
//! never marks a list.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use farsight_core::config::ListTakedownsConfig;
use farsight_storage::takedowns::{self, Due, Seen};
use serde_json::Value;
use tokio::sync::watch;
use url::Url;

use crate::ctx::Ctx;
use crate::metrics as m;

/// How often a pass starts (after the last one ended).
pub const PASS_EVERY: Duration = Duration::from_secs(15);
/// A pass reads the lists the rate allows in this long.
const PASS_SPAN_SECS: u32 = 60;
/// Most lists one pass reads.
const PASS_MAX: i64 = 5_000;

/// What the AppView said of one list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// It serves the list.
    Served,
    /// It says it has no such list.
    NotServed,
    /// It asks for fewer requests: the pass ends.
    RateLimited,
    /// Nothing to go by, in words.
    Inconclusive(String),
}

impl Answer {
    /// The `outcome` label of [`m::LIST_TAKEDOWN_CHECKS`].
    pub fn label(&self) -> &'static str {
        match self {
            Answer::Served => "served",
            Answer::NotServed => "not_served",
            Answer::RateLimited => "rate_limited",
            Answer::Inconclusive(_) => "inconclusive",
        }
    }
}

/// Reads the AppView's answer to `getList`. "Not found" is a `400` whose
/// error is `InvalidRequest` or `NotFound` and whose message says the
/// list was not found, as the Bluesky AppView answers for a list it has
/// taken down and for one that never existed alike.
pub fn read_answer(status: u16, body: &[u8]) -> Answer {
    let v: Option<Value> = serde_json::from_slice(body).ok();
    match status {
        200 => match v.as_ref().and_then(|v| v.get("list")) {
            Some(Value::Object(_)) => Answer::Served,
            _ => Answer::Inconclusive("HTTP 200 without a list".into()),
        },
        429 => Answer::RateLimited,
        400 => {
            let field = |k: &str| {
                v.as_ref()
                    .and_then(|v| v.get(k))
                    .and_then(Value::as_str)
                    .unwrap_or("")
            };
            let error = field("error");
            if matches!(error, "InvalidRequest" | "NotFound")
                && field("message")
                    .to_ascii_lowercase()
                    .contains("list not found")
            {
                Answer::NotServed
            } else {
                Answer::Inconclusive(format!("HTTP 400 {error}").trim_end().to_owned())
            }
        }
        s => Answer::Inconclusive(format!("HTTP {s}")),
    }
}

/// The schedule, from `[backfill.list_takedowns]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// `confirm_after`.
    pub confirm_after: chrono::Duration,
    /// `recheck`.
    pub recheck: chrono::Duration,
    /// `recheck_taken_down`.
    pub recheck_taken_down: chrono::Duration,
}

impl Timing {
    /// The schedule `t` sets.
    pub fn of(t: &ListTakedownsConfig) -> Timing {
        let d = |c: farsight_core::ConfigDuration| {
            chrono::Duration::from_std(c.get()).unwrap_or(chrono::Duration::MAX)
        };
        Timing {
            confirm_after: d(t.confirm_after),
            recheck: d(t.recheck),
            recheck_taken_down: d(t.recheck_taken_down),
        }
    }
}

/// What a list's columns become after `answer` at `now`. `created_at` is
/// the record's own `createdAt`.
pub fn decide(
    was: &Seen,
    created_at: Option<DateTime<Utc>>,
    answer: &Answer,
    now: DateTime<Utc>,
    t: &Timing,
) -> Seen {
    let later = |d: chrono::Duration| Some(now.checked_add_signed(d).unwrap_or(now));
    match answer {
        Answer::Served => Seen {
            misses: 0,
            checked_at: Some(now),
            absent_since: None,
            due_at: later(t.recheck),
        },
        Answer::RateLimited | Answer::Inconclusive(_) => Seen {
            due_at: later(t.confirm_after),
            ..*was
        },
        Answer::NotServed => {
            // A list the AppView may not have indexed yet.
            let young = created_at.is_some_and(|c| now - c < t.confirm_after);
            if young {
                return Seen {
                    due_at: later(t.confirm_after),
                    ..*was
                };
            }
            if was.absent_since.is_some() {
                return Seen {
                    misses: was.misses.saturating_add(1).max(2),
                    checked_at: Some(now),
                    absent_since: was.absent_since,
                    due_at: later(t.recheck_taken_down),
                };
            }
            match (was.misses, was.checked_at) {
                (n, Some(first)) if n >= 1 && now - first >= t.confirm_after => Seen {
                    misses: n.saturating_add(1),
                    checked_at: Some(now),
                    absent_since: Some(now),
                    due_at: later(t.recheck_taken_down),
                },
                // A second miss too soon after the first: the first
                // stands, and the list is asked again once it may count.
                (n, Some(first)) if n >= 1 => Seen {
                    due_at: Some(
                        first
                            .checked_add_signed(t.confirm_after)
                            .unwrap_or(now)
                            .max(now),
                    ),
                    ..*was
                },
                _ => Seen {
                    misses: 1,
                    checked_at: Some(now),
                    absent_since: None,
                    due_at: later(t.confirm_after),
                },
            }
        }
    }
}

/// `GET {appview}/xrpc/app.bsky.graph.getList?list=<uri>&limit=1`.
pub fn get_list_url(appview: &str, owner_did: &str, rkey: &str) -> Option<Url> {
    let base = appview.trim().trim_end_matches('/');
    let mut u = Url::parse(&format!("{base}/xrpc/app.bsky.graph.getList")).ok()?;
    u.query_pairs_mut()
        .append_pair(
            "list",
            &format!("at://{owner_did}/app.bsky.graph.list/{rkey}"),
        )
        .append_pair("limit", "1");
    Some(u)
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// Lists asked about.
    pub checks: i32,
    /// Answers that gave nothing to go by (a `429` among them).
    pub errors: i32,
    /// Lists marked in this pass.
    pub marked: u32,
    /// Lists whose mark this pass cleared (served again, no longer
    /// subscribed to, or the checks turned off).
    pub cleared: u64,
    /// The pass ended at a `429`.
    pub rate_limited: bool,
}

/// One pass: with the checks off, clears every mark; on, clears the marks
/// of lists no longer looked at, then asks about the lists that are due,
/// as many as `rps` allows in a minute, under the AppView's limiter.
/// Stops between two lists when `stop` flips.
pub async fn pass(
    ctx: &Ctx,
    stop: &watch::Receiver<bool>,
) -> Result<Report, farsight_storage::StorageError> {
    let cfg = ctx.cfg();
    let t = &cfg.backfill.list_takedowns;
    let mut report = Report::default();
    if !t.enabled {
        report.cleared = takedowns::clear_all(&ctx.pool).await?;
        publish_count(ctx).await;
        return Ok(report);
    }
    report.cleared = takedowns::clear_left(&ctx.pool).await?;
    let timing = Timing::of(t);
    let start = crate::jobs::db_now(&ctx.pool).await?;
    let began = Instant::now();
    let limit = i64::from(t.rps.saturating_mul(PASS_SPAN_SECS)).clamp(1, PASS_MAX);
    let due = takedowns::due(&ctx.pool, start, limit).await?;
    let mut last_error: Option<String> = None;
    for d in due {
        if *stop.borrow() {
            break;
        }
        let answer = ask(ctx, &t.appview_url, &d).await;
        metrics::counter!(m::LIST_TAKEDOWN_CHECKS, "outcome" => answer.label()).increment(1);
        report.checks += 1;
        let now =
            start + chrono::Duration::from_std(began.elapsed()).unwrap_or(chrono::Duration::zero());
        let next = decide(&d.was, d.created_at, &answer, now, &timing);
        match &answer {
            Answer::Inconclusive(why) => {
                report.errors += 1;
                last_error = Some(why.clone());
            }
            Answer::RateLimited => {
                report.errors += 1;
                last_error = Some("HTTP 429".into());
            }
            _ => {}
        }
        if next.absent_since.is_some() && d.was.absent_since.is_none() {
            report.marked += 1;
            tracing::info!(owner = %d.owner_did, rkey = %d.rkey, "list marked taken down: the AppView no longer serves it");
        } else if next.absent_since.is_none() && d.was.absent_since.is_some() {
            report.cleared += 1;
            tracing::info!(owner = %d.owner_did, rkey = %d.rkey, "list served again: taken-down mark cleared");
        }
        takedowns::record(&ctx.pool, d.id, &next).await?;
        if answer == Answer::RateLimited {
            report.rate_limited = true;
            break;
        }
    }
    takedowns::record_pass(
        &ctx.pool,
        report.checks,
        report.errors,
        last_error.as_deref(),
    )
    .await?;
    publish_count(ctx).await;
    Ok(report)
}

async fn ask(ctx: &Ctx, appview: &str, d: &Due) -> Answer {
    let Some(url) = get_list_url(appview, &d.owner_did, &d.rkey) else {
        return Answer::Inconclusive("the list's address could not be formed".into());
    };
    match ctx.net.appview_get(&url).await {
        Ok((status, body)) => read_answer(status, &body),
        Err(e) => Answer::Inconclusive(e.kind().to_owned()),
    }
}

async fn publish_count(ctx: &Ctx) {
    if let Ok(mut conn) = ctx.pool.acquire().await
        && let Ok(n) = takedowns::count(&mut conn).await
    {
        metrics::gauge!(m::LISTS_TAKEN_DOWN).set(n as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(h: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-21T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + chrono::Duration::hours(h)
    }

    fn timing() -> Timing {
        Timing::of(&ListTakedownsConfig::default())
    }

    #[test]
    fn the_appviews_answers() {
        let nf = br#"{"error":"InvalidRequest","message":"List not found"}"#;
        assert_eq!(read_answer(400, nf), Answer::NotServed);
        assert_eq!(
            read_answer(400, br#"{"error":"NotFound","message":"list not found"}"#),
            Answer::NotServed
        );
        assert_eq!(
            read_answer(200, br#"{"list":{"uri":"at://x"},"items":[]}"#),
            Answer::Served
        );
        assert!(matches!(read_answer(200, b"{}"), Answer::Inconclusive(_)));
        assert!(matches!(
            read_answer(
                400,
                br#"{"error":"InvalidRequest","message":"Error: limit must be <= 100"}"#
            ),
            Answer::Inconclusive(_)
        ));
        assert!(matches!(
            read_answer(
                400,
                br#"{"error":"BlockedActor","message":"List not found"}"#
            ),
            Answer::Inconclusive(_)
        ));
        assert!(matches!(read_answer(404, nf), Answer::Inconclusive(_)));
        assert!(matches!(
            read_answer(502, b"<html>"),
            Answer::Inconclusive(_)
        ));
        assert_eq!(read_answer(429, b""), Answer::RateLimited);
    }

    #[test]
    fn two_misses_an_hour_apart_mark_a_list() {
        let t = timing();
        let old = Some(at(-48));
        let first = decide(&Seen::default(), old, &Answer::NotServed, at(0), &t);
        assert_eq!(first.misses, 1);
        assert_eq!(first.absent_since, None);
        assert_eq!(first.due_at, Some(at(1)));
        // Too soon: the first miss stands and the list is asked again
        // when the second may count.
        let soon = decide(
            &first,
            old,
            &Answer::NotServed,
            at(0) + chrono::Duration::minutes(10),
            &t,
        );
        assert_eq!((soon.misses, soon.checked_at), (1, Some(at(0))));
        assert_eq!(soon.absent_since, None);
        assert_eq!(soon.due_at, Some(at(1)));
        let second = decide(&first, old, &Answer::NotServed, at(1), &t);
        assert_eq!(second.absent_since, Some(at(1)));
        assert_eq!(second.misses, 2);
        assert_eq!(second.due_at, Some(at(7)));
        // Still not served: the mark keeps its date.
        let again = decide(&second, old, &Answer::NotServed, at(7), &t);
        assert_eq!(again.absent_since, Some(at(1)));
        assert_eq!(again.due_at, Some(at(13)));
    }

    #[test]
    fn a_served_answer_clears_everything() {
        let t = timing();
        let marked = Seen {
            misses: 3,
            checked_at: Some(at(0)),
            absent_since: Some(at(-5)),
            due_at: Some(at(6)),
        };
        let back = decide(&marked, None, &Answer::Served, at(6), &t);
        assert_eq!(
            back,
            Seen {
                misses: 0,
                checked_at: Some(at(6)),
                absent_since: None,
                due_at: Some(at(30)),
            }
        );
        // A miss followed by a served answer starts over.
        let first = decide(&Seen::default(), None, &Answer::NotServed, at(0), &t);
        let served = decide(&first, None, &Answer::Served, at(1), &t);
        let miss = decide(&served, None, &Answer::NotServed, at(2), &t);
        assert_eq!((miss.misses, miss.absent_since), (1, None));
    }

    #[test]
    fn nothing_to_go_by_changes_nothing_but_the_next_check() {
        let t = timing();
        let first = decide(&Seen::default(), None, &Answer::NotServed, at(0), &t);
        for a in [Answer::Inconclusive("HTTP 502".into()), Answer::RateLimited] {
            let n = decide(&first, None, &a, at(2), &t);
            assert_eq!(
                (n.misses, n.checked_at, n.absent_since),
                (1, Some(at(0)), None)
            );
            assert_eq!(n.due_at, Some(at(3)));
        }
    }

    #[test]
    fn a_young_list_is_not_counted_missing() {
        let t = timing();
        let young = Some(at(0) - chrono::Duration::minutes(20));
        let n = decide(&Seen::default(), young, &Answer::NotServed, at(0), &t);
        assert_eq!((n.misses, n.absent_since), (0, None));
        assert_eq!(n.due_at, Some(at(1)));
    }

    #[test]
    fn the_url() {
        assert_eq!(
            get_list_url("https://public.api.bsky.app/", "did:plc:abc", "3lkw")
                .unwrap()
                .as_str(),
            "https://public.api.bsky.app/xrpc/app.bsky.graph.getList?list=at%3A%2F%2Fdid%3Aplc%3Aabc%2Fapp.bsky.graph.list%2F3lkw&limit=1"
        );
    }
}
