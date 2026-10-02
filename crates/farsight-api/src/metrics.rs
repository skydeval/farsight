//! API metric names (design §13). The exporter is installed by the server.

/// `farsight_query_requests_total{endpoint,status}`.
pub const QUERY_REQUESTS: &str = "farsight_query_requests_total";
/// `farsight_query_duration_seconds{endpoint}`.
pub const QUERY_DURATION: &str = "farsight_query_duration_seconds";
/// `farsight_rate_limited_total{class}`.
pub const RATE_LIMITED: &str = "farsight_rate_limited_total";
/// `farsight_coverage_exceptions{kind}`.
pub const COVERAGE_EXCEPTIONS: &str = "farsight_coverage_exceptions";
/// `farsight_lists{state}`.
pub const LISTS: &str = "farsight_lists";

/// Every API metric.
pub const ALL: [&str; 5] = [
    QUERY_REQUESTS,
    QUERY_DURATION,
    RATE_LIMITED,
    COVERAGE_EXCEPTIONS,
    LISTS,
];

/// Counts one rate-limited request of `class`.
pub fn rate_limited(class: crate::ratelimit::Class) {
    metrics::counter!(RATE_LIMITED, "class" => class.label()).increment(1);
}

/// Registers every series at zero so it is visible before first use.
pub fn register() {
    use crate::ratelimit::Class;
    for c in [
        Class::AnonRead,
        Class::KeyRead,
        Class::AdminBackfill,
        Class::KeyBackfill,
        Class::UiLookup,
        Class::UiLogin,
        Class::PublicUi,
        Class::PublicCard,
    ] {
        metrics::counter!(RATE_LIMITED, "class" => c.label()).increment(0);
    }
    for s in farsight_storage::codes::TrackState::ALL {
        metrics::gauge!(LISTS, "state" => s.api_name()).set(0.0);
    }
    for e in crate::Endpoint::ALL {
        metrics::counter!(QUERY_REQUESTS, "endpoint" => e.label(), "status" => "200").increment(0);
    }
}
