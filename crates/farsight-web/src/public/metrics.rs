//! Public UI metrics (design §13). `page` is the matched route, never the
//! path, so the label set is fixed.

use std::time::Duration;

use axum::http::StatusCode;

use super::WithheldReason;
use super::handles::Outcome;

/// `farsight_public_ui_requests_total{page,status}`.
pub const REQUESTS: &str = "farsight_public_ui_requests_total";
/// `farsight_public_ui_duration_seconds{page}`.
pub const DURATION: &str = "farsight_public_ui_duration_seconds";
/// `farsight_public_ui_handle_resolutions_total{outcome}`.
pub const HANDLE_RESOLUTIONS: &str = "farsight_public_ui_handle_resolutions_total";
/// `farsight_public_ui_withheld_total{reason}`.
pub const WITHHELD: &str = "farsight_public_ui_withheld_total";

/// The matched route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    /// `/public`.
    Home,
    /// `/public/about`.
    About,
    /// `/public/search`.
    Search,
    /// `/public/did/{did}`.
    Did,
    /// `/public/did/{did}/history`.
    DidHistory,
    /// `/public/list/{did}/{rkey}`.
    List,
    /// `/public/list/{did}/{rkey}/history`.
    ListHistory,
    /// `/robots.txt`.
    Robots,
    /// Anything else under `/public/`.
    Other,
}

impl Page {
    /// Every page.
    pub const ALL: [Page; 9] = [
        Page::Home,
        Page::About,
        Page::Search,
        Page::Did,
        Page::DidHistory,
        Page::List,
        Page::ListHistory,
        Page::Robots,
        Page::Other,
    ];

    /// Metric label.
    pub fn label(self) -> &'static str {
        match self {
            Page::Home => "home",
            Page::About => "about",
            Page::Search => "search",
            Page::Did => "did",
            Page::DidHistory => "did_history",
            Page::List => "list",
            Page::ListHistory => "list_history",
            Page::Robots => "robots",
            Page::Other => "other",
        }
    }
}

/// The status class of a response: `2xx`, `3xx`, `4xx` or `5xx`.
pub fn status_class(s: StatusCode) -> &'static str {
    match s.as_u16() {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

/// Counts one response.
pub fn observe(page: Page, status: StatusCode, took: Duration) {
    ::metrics::counter!(REQUESTS, "page" => page.label(), "status" => status_class(status))
        .increment(1);
    ::metrics::histogram!(DURATION, "page" => page.label()).record(took.as_secs_f64());
}

/// Counts one handle resolution.
pub fn handle_resolution(o: Outcome) {
    ::metrics::counter!(HANDLE_RESOLUTIONS, "outcome" => o.label()).increment(1);
}

/// Counts one withheld page: a request for an account's or a list's page
/// that got the notice instead.
pub fn withheld(r: WithheldReason) {
    ::metrics::counter!(WITHHELD, "reason" => r.label()).increment(1);
}

/// Registers every series at zero so it is visible before first use.
pub fn register() {
    for p in Page::ALL {
        ::metrics::counter!(REQUESTS, "page" => p.label(), "status" => "2xx").increment(0);
    }
    for o in Outcome::ALL {
        ::metrics::counter!(HANDLE_RESOLUTIONS, "outcome" => o.label()).increment(0);
    }
    for r in [
        WithheldReason::HiddenStatus,
        WithheldReason::OperatorExcluded,
    ] {
        ::metrics::counter!(WITHHELD, "reason" => r.label()).increment(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_buckets() {
        assert_eq!(status_class(StatusCode::OK), "2xx");
        assert_eq!(status_class(StatusCode::SEE_OTHER), "3xx");
        assert_eq!(status_class(StatusCode::NOT_FOUND), "4xx");
        assert_eq!(status_class(StatusCode::TOO_MANY_REQUESTS), "4xx");
        assert_eq!(status_class(StatusCode::SERVICE_UNAVAILABLE), "5xx");
    }

    #[test]
    fn page_labels_are_the_documented_set() {
        let labels: Vec<&str> = Page::ALL.iter().map(|p| p.label()).collect();
        assert_eq!(
            labels,
            [
                "home",
                "about",
                "search",
                "did",
                "did_history",
                "list",
                "list_history",
                "robots",
                "other"
            ]
        );
    }
}
