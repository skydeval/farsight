//! Ingest metrics (design §13) and the Prometheus exporter.

use std::net::SocketAddr;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

/// `farsight_firehose_connected{protocol}` (gauge, 0/1).
pub const CONNECTED: &str = "farsight_firehose_connected";
/// `farsight_firehose_lag_seconds` (gauge): now − `applied_through`.
pub const LAG: &str = "farsight_firehose_lag_seconds";
/// `farsight_firehose_source_lag_seconds` (gauge).
pub const SOURCE_LAG: &str = "farsight_firehose_source_lag_seconds";
/// `farsight_firehose_events_total{collection,op,outcome}` (counter).
pub const EVENTS: &str = "farsight_firehose_events_total";
/// `farsight_firehose_reconnects_total{reason}` (counter).
pub const RECONNECTS: &str = "farsight_firehose_reconnects_total";
/// `farsight_firehose_open_gaps` (gauge).
pub const OPEN_GAPS: &str = "farsight_firehose_open_gaps";
/// `farsight_ingest_batch_seconds` (histogram).
pub const BATCH_SECONDS: &str = "farsight_ingest_batch_seconds";
/// `farsight_ingest_buffer_depth` (gauge).
pub const BUFFER_DEPTH: &str = "farsight_ingest_buffer_depth";
/// `farsight_ingest_dropped_total{reason}` (counter).
pub const DROPPED: &str = "farsight_ingest_dropped_total";

/// Every stage-2 metric name.
pub const ALL: [&str; 9] = [
    CONNECTED,
    LAG,
    SOURCE_LAG,
    EVENTS,
    RECONNECTS,
    OPEN_GAPS,
    BATCH_SECONDS,
    BUFFER_DEPTH,
    DROPPED,
];

/// Registers descriptions (HELP lines).
pub fn describe() {
    describe_gauge!(
        CONNECTED,
        "1 while the Jetstream session is up, by protocol"
    );
    describe_gauge!(
        LAG,
        Unit::Seconds,
        "now minus the applied-through witness time"
    );
    describe_gauge!(
        SOURCE_LAG,
        Unit::Seconds,
        "now minus the median rev commit time of the last 1000 events"
    );
    describe_counter!(
        EVENTS,
        "firehose commit events by collection, op and outcome (applied, stale, refused, dropped)"
    );
    describe_counter!(RECONNECTS, "Jetstream reconnects by reason");
    describe_gauge!(OPEN_GAPS, "unhealed firehose gaps");
    describe_histogram!(
        BATCH_SECONDS,
        Unit::Seconds,
        "wall time to apply and commit one ingest batch"
    );
    describe_gauge!(
        BUFFER_DEPTH,
        "events waiting between the reader and the writer"
    );
    describe_counter!(
        DROPPED,
        "events dropped before apply by reason (invalid, foreign_listitem, poisoned)"
    );
    register_zeroes();
}

/// Registers every known label set at zero, so dashboards and the
/// harness see each series before its first event.
pub fn register_zeroes() {
    for reason in ["invalid", "foreign_listitem", "poisoned"] {
        metrics::counter!(DROPPED, "reason" => reason).increment(0);
    }
    for reason in [
        "connect_error",
        "stall",
        "closed",
        "error",
        "server_error",
        "kill",
        "failover",
        "cursor_too_old",
    ] {
        metrics::counter!(RECONNECTS, "reason" => reason).increment(0);
    }
    for c in farsight_core::nsid::INDEXED_COLLECTIONS {
        for op in ["create", "update", "delete"] {
            for outcome in ["applied", "stale", "refused", "dropped"] {
                metrics::counter!(EVENTS, "collection" => c, "op" => op, "outcome" => outcome)
                    .increment(0);
            }
        }
    }
    for p in ["v1", "v2"] {
        metrics::gauge!(CONNECTED, "protocol" => p).set(0.0);
    }
    metrics::gauge!(LAG).set(0.0);
    metrics::gauge!(SOURCE_LAG).set(0.0);
    metrics::gauge!(OPEN_GAPS).set(0.0);
    metrics::gauge!(BUFFER_DEPTH).set(0.0);
}

/// Installs the global recorder with an HTTP listener on `bind`
/// (`metrics.bind`). Must run inside a Tokio runtime.
pub fn install(bind: SocketAddr) -> Result<(), String> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(bind)
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(BATCH_SECONDS.to_owned()),
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )
        .map_err(|e| e.to_string())?
        .install()
        .map_err(|e| e.to_string())?;
    describe();
    Ok(())
}
