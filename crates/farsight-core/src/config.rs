//! Configuration schema and loading (see `docs/design/operations.md`,
//! `docs/design/web-ui.md` and `docs/design/security.md`).
//!
//! Sources, in increasing precedence: built-in defaults, `config.toml`,
//! then `FARSIGHT__<SECTION>__<KEY>` environment variables (one `__` per
//! nesting level, e.g. `FARSIGHT__BACKFILL__SWEEP__ENABLED`; list values
//! comma-separated). Environment values are typed by the default value at
//! the same path, so an unknown key is an error rather than silently
//! ignored.
//!
//! Every key has a default except those that differ per deployment:
//! `server.hostname`, `server.contact`, `storage.database_url` and
//! `auth.admin_token_sha256` are required. `access.admin_did` is not: a
//! value used only at sign-in never stops the process. A fresh install
//! trusts no proxy until the
//! operator says so: the `[proxy]` defaults are `mode = "none"`,
//! `trusted = []`.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use crate::duration::ConfigDuration;

/// Default config file location.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/farsight/config.toml";

/// Environment variable prefix for overrides.
pub const ENV_PREFIX: &str = "FARSIGHT__";

/// `FARSIGHT_SKIP_WIZARD`: build config from the environment when no file
/// exists, never entering setup mode.
pub const SKIP_WIZARD_ENV: &str = "FARSIGHT_SKIP_WIZARD";

/// Configuration errors. Any of these makes the process exit non-zero
/// (an invalid config never enters setup mode and is never rewritten).
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("reading {path}: {source}")]
    Io {
        /// Path of the config file that could not be read.
        path: String,
        /// The I/O error.
        source: std::io::Error,
    },
    /// The file is not valid TOML.
    #[error("parsing config.toml: {0}")]
    Toml(String),
    /// The merged config does not match the schema.
    #[error("invalid config: {0}")]
    Schema(String),
    /// A `FARSIGHT__*` variable names no config key.
    #[error("unknown config key in environment variable {0}")]
    UnknownEnvKey(String),
    /// A `FARSIGHT__*` value could not be converted to the key's type.
    #[error("environment variable {var}: cannot parse {value:?} as {expected}")]
    EnvValue {
        /// Full name of the variable, prefix included (`FARSIGHT__…`).
        var: String,
        /// Its raw value, as found in the environment.
        value: String,
        /// TOML type of the key's default value, which the raw value must
        /// convert to: `integer`, `float`, `boolean`, …
        expected: &'static str,
    },
    /// Required keys are missing (listed as dotted paths).
    #[error("missing required config keys: {}", .0.join(", "))]
    MissingKeys(Vec<String>),
    /// A value is present but not acceptable.
    #[error("invalid value for {key}: {reason}")]
    Invalid {
        /// Dotted key path.
        key: String,
        /// The rule the value breaks, worded for the operator.
        reason: String,
    },
}

fn invalid(key: &str, reason: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        key: key.to_owned(),
        reason: reason.into(),
    }
}

/// The complete configuration: one field per table of `config.toml`. A key
/// the schema does not know is refused at every level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// `[server]`.
    pub server: ServerConfig,
    /// `[storage]`.
    pub storage: StorageConfig,
    /// `[firehose]`.
    pub firehose: FirehoseConfig,
    /// `[backfill]`.
    pub backfill: BackfillConfig,
    /// `[access]`.
    pub access: AccessConfig,
    /// `[public_ui]`.
    pub public_ui: PublicUiConfig,
    /// `[auth]`.
    pub auth: AuthConfig,
    /// `[proxy]`.
    pub proxy: ProxyConfig,
    /// `[limits]`.
    pub limits: LimitsConfig,
    /// `[net]`.
    pub net: NetConfig,
    /// `[rate_limit]`.
    pub rate_limit: RateLimitConfig,
    /// `[metrics]`.
    pub metrics: MetricsConfig,
}

/// `[server]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Listen address of the API and the web UI, as `host:port`.
    pub bind: String,
    /// Public hostname (absolute links, User-Agent). Required.
    pub hostname: String,
    /// Operator contact (shown in `getStats` and User-Agent). Required.
    pub contact: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind: "0.0.0.0:8080".to_owned(),
            hostname: String::new(),
            contact: String::new(),
        }
    }
}

/// `[storage]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Postgres DSN. Required.
    pub database_url: String,
    /// Storage budget in bytes (`pg_database_size`).
    pub budget_bytes: u64,
    /// Hard ceiling in bytes; 0 means 115% of `budget_bytes`. Must exceed
    /// the budget after defaulting.
    pub hard_ceiling_bytes: u64,
    /// How long a deletion's tombstone is kept before the janitor deletes
    /// it.
    pub tombstone_ttl: ConfigDuration,
    /// Record removed blocks, listblocks and list memberships.
    pub block_history_enabled: bool,
    /// How long history rows are kept; `"0s"` keeps them forever.
    pub block_history_retention: ConfigDuration,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            database_url: String::new(),
            budget_bytes: 70_000_000_000,
            hard_ceiling_bytes: 0,
            tombstone_ttl: ConfigDuration::days(7),
            block_history_enabled: true,
            block_history_retention: ConfigDuration::days(365),
        }
    }
}

impl StorageConfig {
    /// The hard ceiling after defaulting (0 ⇒ 115% of the budget).
    pub fn effective_hard_ceiling(&self) -> u64 {
        if self.hard_ceiling_bytes == 0 {
            self.budget_bytes.saturating_mul(115) / 100
        } else {
            self.hard_ceiling_bytes
        }
    }
}

/// `[firehose]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FirehoseConfig {
    /// Jetstream instances, in failover order.
    pub urls: Vec<String>,
    /// `[firehose.tuning]`.
    pub tuning: FirehoseTuning,
}

impl Default for FirehoseConfig {
    fn default() -> Self {
        FirehoseConfig {
            // Bluesky's two public v2 instances, in failover order.
            urls: vec![
                "wss://jetstream.us-east.bsky.network".to_owned(),
                "wss://jetstream.us-west.bsky.network".to_owned(),
            ],
            tuning: FirehoseTuning::default(),
        }
    }
}

/// `[firehose.tuning]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FirehoseTuning {
    /// On a failover, a first event later than the requested cursor by
    /// more than this counts as a clamp (the instance no longer had the
    /// position) and opens a gap. On a resume by `seq`, a first event
    /// later than the stored cursor by more than this means the stream
    /// did not continue, and opens a gap too.
    pub gap_threshold: ConfigDuration,
    /// Smallest rewind on failover: the new instance is asked for the
    /// applied position minus the larger of this and its lag plus 5
    /// minutes.
    pub failover_rewind_min: ConfigDuration,
    /// Largest lag of the new instance for which such a rewind is trusted
    /// to leave no gap.
    pub failover_max_lag: ConfigDuration,
    /// Lag beyond which the synthetic gap exists.
    pub synthetic_gap_lag: ConfigDuration,
    /// Silence on the connection that ends a session.
    pub stall_timeout: ConfigDuration,
    /// Seam repair window start, before the session's connect.
    pub seam_repair_before: ConfigDuration,
    /// Seam repair window end, after the session caught up.
    pub seam_repair_after: ConfigDuration,
    /// Delay between catching up and the seam repair.
    pub seam_repair_delay: ConfigDuration,
    /// A session has caught up once an event's witness time is within this
    /// much of wall time, or is later than the session's connect.
    pub seam_repair_catchup_margin: ConfigDuration,
}

impl Default for FirehoseTuning {
    fn default() -> Self {
        FirehoseTuning {
            gap_threshold: ConfigDuration::secs(300),
            failover_rewind_min: ConfigDuration::mins(10),
            failover_max_lag: ConfigDuration::mins(30),
            synthetic_gap_lag: ConfigDuration::mins(5),
            stall_timeout: ConfigDuration::secs(60),
            seam_repair_before: ConfigDuration::secs(150),
            seam_repair_after: ConfigDuration::secs(30),
            seam_repair_delay: ConfigDuration::secs(60),
            seam_repair_catchup_margin: ConfigDuration::secs(5),
        }
    }
}

/// `[backfill]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackfillConfig {
    /// Worker pool size; positive. The backfill process's database pool is
    /// this plus 8 connections.
    pub concurrency: u32,
    /// Requests per second towards any one PDS host.
    pub per_host_rps: u32,
    /// Base URL of the PLC directory: `did:plc` resolution, and the export
    /// the `plc` sweep source reads.
    pub plc_url: String,
    /// Requests per second towards the PLC directory, in total.
    pub plc_rps: u32,
    /// Seed PDS resolution from the PLC export.
    pub plc_seed_from_export: bool,
    /// Base URL of the relay whose listings enumerate repositories.
    pub relay_url: String,
    /// `requestBackfill` freshness window.
    pub request_fresh_window: ConfigDuration,
    /// Minimum interval between list fetch runs per owner.
    pub owner_fetch_cooldown: ConfigDuration,
    /// Repo job retry backoff; the last step repeats.
    pub retry_schedule: Vec<ConfigDuration>,
    /// Failing for this long makes a repo job terminal.
    pub terminal_after: ConfigDuration,
    /// `missing` list re-check schedule.
    pub missing_retry: Vec<ConfigDuration>,
    /// Failed fetch attempts before FT.
    pub list_fetch_max_attempts: u32,
    /// Phase-1 error retry schedule.
    pub phase1_retry: Vec<ConfigDuration>,
    /// Wall-clock cap on one list fetch run.
    pub list_fetch_max_duration: ConfigDuration,
    /// Wall-clock cap on one attempt of a repo job. An attempt that
    /// reaches it stops where it is and goes on later.
    pub repo_job_max_duration: ConfigDuration,
    /// Margin before a gap's start when a repair chooses the accounts to
    /// re-read: those whose rev is at or after `from − repair_slack − lag`.
    pub repair_slack: ConfigDuration,
    /// In-memory seen-set cap for out-of-order listings.
    pub seen_set_cap: u64,
    /// Queue entries per system requester.
    pub system_queue_cap: u64,
    /// Concurrent requests per host.
    pub per_host_concurrency: u32,
    /// Requests per second towards all hosts of one registrable domain
    /// together. Large hosts are exempt.
    pub per_domain_rps: u32,
    /// Concurrent requests towards all hosts of one registrable domain
    /// together. Large hosts are exempt.
    pub per_domain_concurrency: u32,
    /// Workers kept for tier 1 (on-demand work and list jobs): tiers 2
    /// and 3 never use them. At most `concurrency − 1` are kept.
    pub on_demand_reserved: u32,
    /// Guaranteed shares of tiers 1, 2, 3 in percent.
    pub tier_shares: Vec<u32>,
    /// `[backfill.sweep]`.
    pub sweep: SweepConfig,
    /// `[backfill.repair]`.
    pub repair: RepairConfig,
    /// `[backfill.backlinks]`.
    pub backlinks: BacklinksConfig,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        BackfillConfig {
            concurrency: 32,
            per_host_rps: 10,
            plc_url: "https://plc.directory".to_owned(),
            plc_rps: 10,
            plc_seed_from_export: false,
            relay_url: "https://bsky.network".to_owned(),
            request_fresh_window: ConfigDuration::hours(1),
            owner_fetch_cooldown: ConfigDuration::mins(10),
            retry_schedule: vec![
                ConfigDuration::hours(1),
                ConfigDuration::hours(6),
                ConfigDuration::hours(24),
                ConfigDuration::days(1),
            ],
            terminal_after: ConfigDuration::days(7),
            missing_retry: vec![
                ConfigDuration::hours(1),
                ConfigDuration::hours(24),
                ConfigDuration::days(7),
            ],
            list_fetch_max_attempts: 2,
            phase1_retry: vec![
                ConfigDuration::mins(5),
                ConfigDuration::mins(20),
                ConfigDuration::hours(1),
            ],
            list_fetch_max_duration: ConfigDuration::hours(1),
            repo_job_max_duration: ConfigDuration::hours(1),
            repair_slack: ConfigDuration::hours(1),
            seen_set_cap: 2_000_000,
            system_queue_cap: 50_000,
            per_host_concurrency: 4,
            per_domain_rps: 20,
            per_domain_concurrency: 8,
            on_demand_reserved: 4,
            tier_shares: vec![60, 25, 15],
            sweep: SweepConfig::default(),
            repair: RepairConfig::default(),
            backlinks: BacklinksConfig::default(),
        }
    }
}

/// `backfill.sweep.source`: where the sweep enumerates repositories from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SweepSource {
    /// `com.atproto.sync.listReposByCollection` (default).
    RelayCollections,
    /// `com.atproto.sync.listRepos`.
    RelayRepos,
    /// PLC `/export`.
    Plc,
}

/// `[backfill.repair]`: repairs of firehose gaps. A repair re-reads
/// every account whose repository changed during the gap, so after a
/// long gap it runs for days.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RepairConfig {
    /// Whether a repair starts by itself when a gap has closed. Off, a
    /// closed gap waits for `admin.startRepair`.
    pub auto_start: bool,
    /// Whether a repair under way is held: it reads nothing new and
    /// keeps its place.
    pub paused: bool,
}

impl Default for RepairConfig {
    fn default() -> Self {
        RepairConfig {
            auto_start: true,
            paused: false,
        }
    }
}

/// `[backfill.sweep]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SweepConfig {
    /// Whether the systematic sweep runs.
    pub enabled: bool,
    /// Where the sweep enumerates repositories from.
    pub source: SweepSource,
    /// Pacing cap; 0 = host-bounded.
    pub max_repos_per_hour: u64,
    /// Periodic full cycle interval; 0 = never.
    pub full_every_days: u32,
    /// Bound on outstanding cycle members.
    pub max_outstanding: u64,
}

impl Default for SweepConfig {
    fn default() -> Self {
        SweepConfig {
            enabled: true,
            source: SweepSource::RelayCollections,
            max_repos_per_hour: 0,
            full_every_days: 0,
            max_outstanding: 10_000,
        }
    }
}

/// `[backfill.backlinks]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BacklinksConfig {
    /// Backlink index URL; empty = discovery disabled.
    pub url: String,
    /// Reference cap across discovery steps.
    pub max_refs: u64,
    /// Allowance for backlink-index lag.
    pub lag_allowance: ConfigDuration,
}

impl Default for BacklinksConfig {
    fn default() -> Self {
        BacklinksConfig {
            url: String::new(),
            max_refs: 200_000,
            lag_allowance: ConfigDuration::mins(5),
        }
    }
}

/// `access.reads`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadsMode {
    /// Anonymous reads allowed.
    Public,
    /// Reads need an API key.
    ApiKey,
    /// Reads disabled except for the admin.
    Disabled,
}

/// `[access]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccessConfig {
    /// Who may call the read queries.
    pub reads: ReadsMode,
    /// Send `Access-Control-Allow-Origin: *` on reads.
    pub cors: bool,
    /// Serve the public UI at the root. Requires `reads = "public"`;
    /// independent of `admin_ui`.
    pub public_ui: bool,
    /// Serve the admin UI under `/admin`, with its sign-in at `/enter`.
    /// Applied at start only: no in-process edit may change it.
    pub admin_ui: bool,
    /// The DID of the one account that may sign in to the admin UI.
    /// Empty = not set (see [`AdminAuth`]).
    pub admin_did: String,
}

impl Default for AccessConfig {
    fn default() -> Self {
        AccessConfig {
            reads: ReadsMode::Public,
            cors: true,
            public_ui: false,
            admin_ui: true,
            admin_did: String::new(),
        }
    }
}

/// `public_ui.dark_mode_default`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemeDefault {
    /// Light unless the visitor chooses otherwise.
    Light,
    /// Dark unless the visitor chooses otherwise.
    Dark,
    /// Follow the visitor's system preference.
    System,
}

impl ThemeDefault {
    /// The spelling in `config.toml`: `light`, `dark` or `system`.
    pub fn as_str(self) -> &'static str {
        match self {
            ThemeDefault::Light => "light",
            ThemeDefault::Dark => "dark",
            ThemeDefault::System => "system",
        }
    }
}

/// Most entries `public_ui.excluded_dids` may hold.
pub const MAX_EXCLUDED_DIDS: usize = 10_000;
/// Upper bound of `public_ui.handle_rps`.
pub const MAX_HANDLE_RPS: u32 = 200;
/// Longest `public_ui.instance_description`, in characters.
pub const MAX_INSTANCE_DESCRIPTION: usize = 2_000;
/// Longest `public_ui.contact`, in characters.
pub const MAX_PUBLIC_CONTACT: usize = 200;

/// `[public_ui]`: what the public UI shows. Every key applies without a
/// restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PublicUiConfig {
    /// Plain text shown on the public home page; empty = the default
    /// text.
    pub instance_description: String,
    /// Contact shown on public pages; empty = `server.contact`.
    pub contact: String,
    /// The DID page shows the subject's own blocks.
    pub show_outgoing_blocks: bool,
    /// The home page lists the accounts that block the most.
    pub show_top_blockers: bool,
    /// The home page lists the accounts that are blocked the most.
    pub show_top_blocked: bool,
    /// Emit the `og:image` tag for the static card.
    pub show_opengraph_image: bool,
    /// Theme a visitor gets before choosing one.
    pub dark_mode_default: ThemeDefault,
    /// Let crawlers index the public pages.
    pub crawlable: bool,
    /// Public page views per second per client address.
    pub rate_limit_rps: u32,
    /// Burst of the public page view limit; positive.
    pub rate_limit_burst: u32,
    /// Concurrent public page renders; at most
    /// `rate_limit.query_concurrency`.
    pub query_concurrency: u32,
    /// How long a verified handle stays in the memory cache. The stored
    /// copy outlives it and refills it.
    pub handle_cache_ttl: ConfigDuration,
    /// DIDs the public pages withhold; at most [`MAX_EXCLUDED_DIDS`].
    pub excluded_dids: Vec<String>,
    /// URL template of a record viewer; empty = records are not links.
    /// Placeholders: `{authority}`, `{collection}`, `{rkey}`.
    pub record_viewer_url: String,
    /// Profile cards carry the account's avatar, which the visitor's
    /// browser fetches from the account's own server.
    pub show_avatars: bool,
    /// Avatars and list images are named on Bluesky's image service as
    /// small thumbnails, not on the account's own server as the
    /// original upload. Off by default. Only with `show_avatars`.
    pub avatar_thumbnails: bool,
    /// Profile cards the process fetches per second, for all visitors
    /// together.
    pub card_rps: u32,
    /// Burst of the same budget; at least `card_rps` (see
    /// [`PublicUiConfig::effective_card_burst`]).
    pub card_burst: u32,
    /// A background worker verifies the handles of accounts that pages
    /// had to show as bare DIDs, so that a later view shows the handle.
    /// Governs the admin pages too, and works with the public UI off.
    pub handle_warming_enabled: bool,
    /// Handle verifications the process starts per second, for pages,
    /// cards and the background worker together; 1 to
    /// [`MAX_HANDLE_RPS`]. Each one is up to two outbound requests.
    pub handle_rps: u32,
    /// Handle checks a second by the handle pass, which works through
    /// every account Farsight holds in the background; 0 (the default)
    /// = off, at most [`MAX_HANDLE_RPS`]. Its own pace: `handle_rps` is
    /// not drawn on. Each check is up to two outbound requests.
    pub handle_pass_rps: u32,
}

impl PublicUiConfig {
    /// Burst of the handle budget: the rate, and never under 10.
    pub fn handle_burst(&self) -> u32 {
        self.handle_rps.max(10)
    }

    /// `card_burst`, raised to `card_rps` when it was set lower (the
    /// loader warns).
    pub fn effective_card_burst(&self) -> u32 {
        self.card_burst.max(self.card_rps)
    }
}

impl Default for PublicUiConfig {
    fn default() -> Self {
        PublicUiConfig {
            instance_description: String::new(),
            contact: String::new(),
            show_outgoing_blocks: false,
            show_top_blockers: false,
            show_top_blocked: false,
            show_opengraph_image: true,
            dark_mode_default: ThemeDefault::System,
            crawlable: false,
            rate_limit_rps: 5,
            rate_limit_burst: 20,
            query_concurrency: 8,
            handle_cache_ttl: ConfigDuration::hours(1),
            excluded_dids: Vec::new(),
            record_viewer_url: String::new(),
            show_avatars: true,
            avatar_thumbnails: false,
            card_rps: 4,
            card_burst: 8,
            handle_warming_enabled: true,
            handle_rps: 20,
            handle_pass_rps: 0,
        }
    }
}

/// Longest `public_ui.record_viewer_url`, in characters.
pub const MAX_RECORD_VIEWER_URL: usize = 500;
/// The placeholders of `public_ui.record_viewer_url`.
pub const RECORD_VIEWER_PLACEHOLDERS: [&str; 3] = ["{authority}", "{collection}", "{rkey}"];

/// Checks a non-empty `public_ui.record_viewer_url`. The message names
/// the rule that failed; the same check runs at load and on a Settings
/// save.
pub fn validate_record_viewer_url(v: &str) -> Result<(), String> {
    if v.chars().count() > MAX_RECORD_VIEWER_URL {
        return Err(format!("longer than {MAX_RECORD_VIEWER_URL} characters."));
    }
    if v.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("must not contain spaces or control characters.".into());
    }
    for p in RECORD_VIEWER_PLACEHOLDERS {
        if !v.contains(p) {
            return Err(format!("`{p}` is missing."));
        }
    }
    let mut bare = v.to_owned();
    for p in RECORD_VIEWER_PLACEHOLDERS {
        bare = bare.replace(p, "x");
    }
    if bare.contains('{') || bare.contains('}') {
        return Err(
            "`{` and `}` may only be used for {authority}, {collection} and {rkey}.".into(),
        );
    }
    let origin_end = v
        .find("://")
        .and_then(|i| v[i + 3..].find('/').map(|j| i + 3 + j))
        .unwrap_or(v.len());
    if v[..origin_end].contains('{') {
        return Err(
            "the scheme, host and port must be fixed text; placeholders belong after the \
             first `/` of the path (a query needs a path before it: \
             https://viewer.example/?u=…)."
                .into(),
        );
    }
    match url::Url::parse(&bare) {
        Ok(u)
            if matches!(u.scheme(), "https" | "http")
                && u.host_str().is_some_and(|h| !h.is_empty())
                && u.username().is_empty()
                && u.password().is_none() =>
        {
            Ok(())
        }
        _ => Err("must be an absolute http or https URL with a host and no credentials.".into()),
    }
}

/// `[auth]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// SHA-256 of the admin token, hex. Required.
    pub admin_token_sha256: String,
}

/// Warning logged in the unconfigured state.
pub const UNCONFIGURED_WARNING: &str = "admin sign-in is not configured: set access.admin_did (farsight set-admin-did, or \
     FARSIGHT__ACCESS__ADMIN_DID) and restart";

/// How (and whether) anyone can sign in to the admin UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminAuth {
    /// `access.admin_ui = false`: no admin UI.
    Disabled,
    /// `access.admin_did` is set: OAuth sign-in as that DID.
    Configured(String),
    /// No admin DID: nobody can sign in.
    Unconfigured,
}

/// Whether `s` is an admin DID the config accepts: `did:plc:` followed by
/// 24 characters of `[a-z2-7]`, or `did:web:` followed by a hostname (no
/// port, no path segments).
pub fn valid_admin_did(s: &str) -> bool {
    if let Some(id) = s.strip_prefix("did:plc:") {
        return id.len() == 24
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b));
    }
    match s.strip_prefix("did:web:") {
        Some(host) => host == host.to_ascii_lowercase() && crate::did::is_valid_hostname(host),
        None => false,
    }
}

/// `proxy.mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyMode {
    /// No proxy: the TCP peer is the client.
    None,
    /// Cloudflare: `CF-Connecting-IP` from trusted peers.
    Cloudflare,
    /// Generic `X-Forwarded-For` from trusted peers.
    Forwarded,
}

/// `[proxy]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyConfig {
    /// Which header, if any, names the client address when the TCP peer is
    /// in `trusted`.
    pub mode: ProxyMode,
    /// Trusted proxy CIDRs (validated by [`validate_trusted_proxy`]).
    pub trusted: Vec<IpNet>,
    /// Opt-in daily refresh of Cloudflare ranges.
    pub cloudflare_refresh: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            mode: ProxyMode::None,
            trusted: Vec::new(),
            cloudflare_refresh: false,
        }
    }
}

/// `[limits]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// Grace for `retained` lists (then **GE** fires).
    pub list_grace: ConfigDuration,
    /// Owner-caused re-admissions per UTC day.
    pub owner_readmissions_per_day: u32,
    /// Wall-clock bound on `pending`.
    pub pending_max_age: ConfigDuration,
    /// Pending lists taking effect per owner key.
    pub pending_effects_per_owner_key: u32,
    /// `unresolved` bucket block cap.
    pub unresolved_blocks: u64,
    /// `unresolved` bucket list-item cap.
    pub unresolved_list_items: u64,
    /// Daily admissions per bucket key.
    pub bucket_admissions_per_day: u64,
    /// Daily admissions per DID key.
    pub did_admissions_per_day: u64,
    /// Extra shared CDN/anycast ranges excluded as address buckets.
    pub cdn_ranges_extra: Vec<IpNet>,
    /// Stored items per list.
    pub list_items_per_list: u64,
    /// Stored items per list owner, over all of the owner's lists.
    pub list_items_per_owner: u64,
    /// Stored blocks per author.
    pub blocks_per_author: u64,
    /// Counted listblocks per author (trigger cap).
    pub listblock_fetch_triggers_per_author: u64,
    /// Hosts exempt from bucket caps (glob `*.` prefix allowed).
    pub large_hosts: Vec<String>,
    /// Per-bucket block cap.
    pub host_blocks: u64,
    /// Per-bucket list-item cap.
    pub host_list_items: u64,
    /// Per-bucket listblock cap.
    pub host_listblocks: u64,
    /// Per-bucket list cap.
    pub host_lists: u64,
    /// Stored listblocks per author.
    pub listblocks_per_author: u64,
    /// Stored list records per author.
    pub lists_per_author: u64,
    /// `unresolved` bucket listblock cap.
    pub unresolved_listblocks: u64,
    /// Daily interning per DID or requester cause key.
    pub intern_per_did_per_day: u64,
    /// Daily interning per bucket cause key.
    pub intern_per_bucket_per_day: u64,
    /// Lifetime interning per non-large bucket.
    pub host_interned_lifetime: u64,
    /// `unresolved` bucket list cap.
    pub unresolved_lists: u64,
    /// Daily history rows per DID admission key, all three history tables
    /// together.
    pub history_per_did_per_day: u64,
    /// Daily history rows per bucket admission key.
    pub history_per_bucket_per_day: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        LimitsConfig {
            list_grace: ConfigDuration::days(7),
            owner_readmissions_per_day: 4,
            pending_max_age: ConfigDuration::hours(3),
            pending_effects_per_owner_key: 5,
            unresolved_blocks: 1_000_000,
            unresolved_list_items: 500_000,
            bucket_admissions_per_day: 20_000,
            did_admissions_per_day: 200,
            cdn_ranges_extra: Vec::new(),
            list_items_per_list: 1_000_000,
            list_items_per_owner: 2_000_000,
            blocks_per_author: 1_000_000,
            listblock_fetch_triggers_per_author: 5_000,
            large_hosts: vec!["*.host.bsky.network".to_owned()],
            host_blocks: 20_000_000,
            host_list_items: 5_000_000,
            host_listblocks: 2_000_000,
            host_lists: 200_000,
            listblocks_per_author: 100_000,
            lists_per_author: 10_000,
            unresolved_listblocks: 200_000,
            intern_per_did_per_day: 1_000_000,
            intern_per_bucket_per_day: 5_000_000,
            host_interned_lifetime: 5_000_000,
            unresolved_lists: 20_000,
            history_per_did_per_day: 10_000,
            history_per_bucket_per_day: 200_000,
        }
    }
}

/// Whether `host` matches one of `patterns`: exactly, or `*.suffix`
/// matching any subdomain of `suffix`. Case does not matter.
pub fn host_matches(patterns: &[String], host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    patterns.iter().any(|pat| {
        let pat = pat.to_ascii_lowercase();
        match pat.strip_prefix("*.") {
            Some(suffix) => host.len() > suffix.len() + 1 && host.ends_with(&format!(".{suffix}")),
            None => host == pat,
        }
    })
}

impl LimitsConfig {
    /// Whether `host` matches `large_hosts` (exact, or `*.suffix` matching
    /// any subdomain of `suffix`).
    pub fn is_large_host(&self, host: &str) -> bool {
        host_matches(&self.large_hosts, host)
    }
}

/// `[net]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct NetConfig {
    /// Hosts the safe client may reach over plain `http` (development).
    pub allow_http_hosts: Vec<String>,
}

/// `[rate_limit]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Anonymous read queries per second per client address.
    pub anon_rps: u32,
    /// Burst of the anonymous read limit.
    pub anon_burst: u32,
    /// Read queries per second per API key, unless the key has its own
    /// rate.
    pub key_rps: u32,
    /// Burst of the API-key read limit.
    pub key_burst: u32,
    /// Admin `requestBackfill` rate.
    pub admin_backfill_rps: u32,
    /// API-key `requestBackfill` rate.
    pub key_backfill_rps: u32,
    /// UI lookup rate (anonymous).
    pub ui_lookup_rps: u32,
    /// Concurrent API requests; also sizes the API connection pool. Read at
    /// start only.
    pub query_concurrency: u32,
    /// Read query `statement_timeout`.
    pub query_timeout: ConfigDuration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        RateLimitConfig {
            anon_rps: 10,
            anon_burst: 50,
            key_rps: 100,
            key_burst: 500,
            admin_backfill_rps: 20,
            key_backfill_rps: 5,
            ui_lookup_rps: 1,
            query_concurrency: 32,
            query_timeout: ConfigDuration::secs(5),
        }
    }
}

/// `[metrics]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    /// The server's metrics listener, as `host:port`. Loopback unless
    /// set: the compose file sets it to all interfaces, which there are
    /// the compose network's.
    pub bind: String,
    /// The backfill process's metrics listener, as `host:port`; loopback
    /// unless set, like `bind`.
    pub backfill_bind: String,
    /// Optional bearer token hash; empty = no auth.
    pub bearer_token_sha256: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        MetricsConfig {
            bind: "127.0.0.1:9464".to_owned(),
            backfill_bind: "127.0.0.1:9465".to_owned(),
            bearer_token_sha256: String::new(),
        }
    }
}

/// How the process should start.
#[derive(Debug)]
pub enum StartMode {
    /// No config file and `FARSIGHT_SKIP_WIZARD` unset: run the wizard.
    Setup,
    /// A valid config, from the file or the environment.
    Normal(Box<LoadedConfig>),
}

/// A validated config plus provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedConfig {
    /// The merged config: defaults, then the file, then the environment.
    pub config: Config,
    /// Dotted keys set from the environment (locked in the settings UI).
    pub env_keys: Vec<String>,
    /// Non-fatal findings (e.g. public proxy ranges).
    pub warnings: Vec<String>,
    /// True when built from the environment without a file.
    pub from_env_only: bool,
}

impl LoadedConfig {
    /// The admin sign-in state of this config.
    pub fn admin_auth(&self) -> AdminAuth {
        let c = &self.config;
        if !c.access.admin_ui {
            AdminAuth::Disabled
        } else if !c.access.admin_did.is_empty() {
            AdminAuth::Configured(c.access.admin_did.clone())
        } else {
            AdminAuth::Unconfigured
        }
    }
}

/// Decides the start mode from the config path and the environment.
pub fn load(path: &Path, env: &[(String, String)]) -> Result<StartMode, ConfigError> {
    if path.exists() {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        return load_from_parts(Some(&text), env).map(|l| StartMode::Normal(Box::new(l)));
    }
    let skip = env
        .iter()
        .any(|(k, v)| k == SKIP_WIZARD_ENV && !v.is_empty() && v != "0" && v != "false");
    if skip {
        return load_from_parts(None, env).map(|l| StartMode::Normal(Box::new(l)));
    }
    Ok(StartMode::Setup)
}

/// What is wrong with a file that is not TOML: the line and the
/// parser's message. The line itself is not quoted, as the parser's own
/// rendering does: the offending line may be the one that holds the
/// database password, and this text goes to the log.
fn toml_error(text: &str, e: &toml::de::Error) -> String {
    match e.span() {
        Some(span) => {
            let upto = span.start.min(text.len());
            let line = text.as_bytes()[..upto]
                .iter()
                .filter(|b| **b == b'\n')
                .count()
                + 1;
            format!("line {line}: {}", e.message())
        }
        None => e.message().to_owned(),
    }
}

/// Builds and validates a config from optional file contents and the
/// environment. `None` means env-only (`FARSIGHT_SKIP_WIZARD`).
pub fn load_from_parts(
    file: Option<&str>,
    env: &[(String, String)],
) -> Result<LoadedConfig, ConfigError> {
    let mut table: toml::Table = match file {
        Some(text) => text
            .parse::<toml::Table>()
            .map_err(|e| ConfigError::Toml(toml_error(text, &e)))?,
        None => toml::Table::new(),
    };
    let env_keys = apply_env_overrides(&mut table, env)?;
    let config: Config = toml::Value::Table(table)
        .try_into()
        .map_err(|e: toml::de::Error| ConfigError::Schema(e.to_string()))?;
    let warnings = config.validate()?;
    let mut loaded = LoadedConfig {
        config,
        env_keys,
        warnings,
        from_env_only: file.is_none(),
    };
    if loaded.admin_auth() == AdminAuth::Unconfigured {
        loaded.warnings.push(UNCONFIGURED_WARNING.to_owned());
    }
    Ok(loaded)
}

fn defaults_value() -> toml::Value {
    // Serializing the defaults cannot fail: every field is a TOML type.
    toml::Value::try_from(Config::default()).expect("defaults serialize")
}

fn type_name(v: &toml::Value) -> &'static str {
    match v {
        toml::Value::String(_) => "string",
        toml::Value::Integer(_) => "integer",
        toml::Value::Float(_) => "float",
        toml::Value::Boolean(_) => "boolean",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "list",
        toml::Value::Table(_) => "table",
    }
}

fn coerce(var: &str, raw: &str, hint: &toml::Value) -> Result<toml::Value, ConfigError> {
    let bad = |expected: &'static str| ConfigError::EnvValue {
        var: var.to_owned(),
        value: raw.to_owned(),
        expected,
    };
    match hint {
        toml::Value::String(_) => Ok(toml::Value::String(raw.to_owned())),
        toml::Value::Integer(_) => raw
            .trim()
            .replace('_', "")
            .parse::<i64>()
            .map(toml::Value::Integer)
            .map_err(|_| bad("integer")),
        toml::Value::Float(_) => raw
            .trim()
            .parse::<f64>()
            .map(toml::Value::Float)
            .map_err(|_| bad("float")),
        toml::Value::Boolean(_) => match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(toml::Value::Boolean(true)),
            "false" | "0" | "no" | "off" => Ok(toml::Value::Boolean(false)),
            _ => Err(bad("boolean")),
        },
        toml::Value::Array(items) => {
            if raw.trim().is_empty() {
                return Ok(toml::Value::Array(Vec::new()));
            }
            let elem_hint = items
                .first()
                .cloned()
                .unwrap_or_else(|| toml::Value::String(String::new()));
            raw.split(',')
                .map(|part| coerce(var, part.trim(), &elem_hint))
                .collect::<Result<Vec<_>, _>>()
                .map(toml::Value::Array)
        }
        other => Err(bad(type_name(other))),
    }
}

/// Applies `FARSIGHT__*` overrides to a parsed TOML table. Returns the
/// dotted keys that were set. Variables without the double-underscore
/// prefix (e.g. `FARSIGHT_SKIP_WIZARD`) are not config keys and are
/// ignored.
pub fn apply_env_overrides(
    table: &mut toml::Table,
    env: &[(String, String)],
) -> Result<Vec<String>, ConfigError> {
    let defaults = defaults_value();
    let mut set = BTreeSet::new();
    for (var, raw) in env {
        let Some(rest) = var.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        let path: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
        if path.iter().any(String::is_empty) {
            return Err(ConfigError::UnknownEnvKey(var.clone()));
        }
        let mut hint = &defaults;
        for seg in &path {
            hint = hint
                .get(seg.as_str())
                .ok_or_else(|| ConfigError::UnknownEnvKey(var.clone()))?;
        }
        if hint.is_table() {
            return Err(ConfigError::UnknownEnvKey(var.clone()));
        }
        let value = coerce(var, raw, hint)?;
        let (last, parents) = path.split_last().expect("path is non-empty");
        let mut cur = &mut *table;
        for seg in parents {
            let entry = cur
                .entry(seg.clone())
                .or_insert_with(|| toml::Value::Table(toml::Table::new()));
            cur = entry.as_table_mut().ok_or_else(|| ConfigError::Invalid {
                key: path.join("."),
                reason: format!("`{seg}` in config.toml is not a table"),
            })?;
        }
        cur.insert(last.clone(), value);
        set.insert(path.join("."));
    }
    Ok(set.into_iter().collect())
}

/// Validates a proxy CIDR: refuses `0.0.0.0/0`, `::/0`, and prefixes
/// shorter than /8 (IPv4) or /24 (IPv6).
pub fn validate_trusted_proxy(net: &IpNet) -> Result<(), String> {
    match net {
        IpNet::V4(n) if n.prefix_len() < 8 => Err(format!(
            "{net} is broader than /8; trusting it would allow client-IP forgery"
        )),
        IpNet::V6(n) if n.prefix_len() < 24 => Err(format!(
            "{net} is broader than /24; trusting it would allow client-IP forgery"
        )),
        _ => Ok(()),
    }
}

/// Whether `net` is outside the private, loopback, link-local and
/// carrier-grade NAT ranges.
pub(crate) fn is_public_net(net: &IpNet) -> bool {
    match net {
        IpNet::V4(n) => {
            let a = n.network();
            !(a.is_private() || a.is_loopback() || a.is_link_local() || a.octets()[0] == 100)
        }
        IpNet::V6(n) => {
            let a = n.network();
            let first = a.segments()[0];
            !(a.is_loopback() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80)
        }
    }
}

/// Warning logged at start when the database is reached with the
/// password `compose.yml` falls back to.
pub const DEFAULT_DB_PASSWORD_WARNING: &str = "storage.database_url uses the bundled Postgres's default password (`farsight`): \
     POSTGRES_PASSWORD was not set before the first start. Postgres is not published by \
     compose.yml, so only the compose network can reach it; set a password of your own all \
     the same (ALTER ROLE farsight PASSWORD …, then POSTGRES_PASSWORD, then restart)";

/// Whether `database_url` signs in as `farsight` with the password
/// `farsight`: what `compose.yml` uses when `POSTGRES_PASSWORD` is unset.
fn has_default_db_password(database_url: &str) -> bool {
    url::Url::parse(database_url)
        .is_ok_and(|u| u.username() == "farsight" && u.password() == Some("farsight"))
}

/// The most workers `backfill.concurrency` accepts: each holds a
/// database connection.
pub const MAX_BACKFILL_CONCURRENCY: u32 = 512;

/// How long after it was read a listing stamp may still be applied (see
/// `docs/design/backfill.md`). `storage.tombstone_ttl` is at least this.
pub const LISTING_STAMP_VALIDITY: std::time::Duration = std::time::Duration::from_secs(72 * 3600);

/// Whether `s` is an absolute `http` or `https` URL with a host and no
/// credentials.
fn is_http_url(s: &str) -> bool {
    match url::Url::parse(s) {
        Ok(u) => {
            matches!(u.scheme(), "http" | "https")
                && u.host_str().is_some_and(|h| !h.is_empty())
                && u.username().is_empty()
                && u.password().is_none()
        }
        Err(_) => false,
    }
}

fn is_hex_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

impl Config {
    /// Checks every rule the loader enforces. Returns non-fatal warnings.
    pub fn validate(&self) -> Result<Vec<String>, ConfigError> {
        let mut missing = Vec::new();
        let mut require = |key: &str, value: &str| {
            if value.trim().is_empty() {
                missing.push(key.to_owned());
            }
        };
        require("server.hostname", self.server.hostname.as_str());
        require("server.contact", self.server.contact.as_str());
        require("storage.database_url", self.storage.database_url.as_str());
        require(
            "auth.admin_token_sha256",
            self.auth.admin_token_sha256.as_str(),
        );
        if !missing.is_empty() {
            return Err(ConfigError::MissingKeys(missing));
        }
        // Both go into the `User-Agent` of every outbound request, and
        // onto pages: one line of text each.
        for (key, value) in [
            ("server.hostname", &self.server.hostname),
            ("server.contact", &self.server.contact),
        ] {
            if value.chars().any(char::is_control) {
                return Err(invalid(
                    key,
                    "must be one line of text without control characters",
                ));
            }
        }
        if !self.access.admin_did.is_empty() && !valid_admin_did(&self.access.admin_did) {
            return Err(invalid(
                "access.admin_did",
                "expected did:plc: followed by 24 characters of a-z and 2-7, or did:web: followed \
                 by a hostname (no port, no path)",
            ));
        }
        if !is_hex_sha256(&self.auth.admin_token_sha256) {
            return Err(invalid(
                "auth.admin_token_sha256",
                "expected 64 hex characters",
            ));
        }
        if !self.metrics.bearer_token_sha256.is_empty()
            && !is_hex_sha256(&self.metrics.bearer_token_sha256)
        {
            return Err(invalid(
                "metrics.bearer_token_sha256",
                "expected 64 hex characters",
            ));
        }
        if self.storage.budget_bytes == 0 {
            return Err(invalid("storage.budget_bytes", "must be positive"));
        }
        if self.storage.effective_hard_ceiling() <= self.storage.budget_bytes {
            return Err(invalid(
                "storage.hard_ceiling_bytes",
                format!(
                    "{} must exceed storage.budget_bytes ({})",
                    self.storage.effective_hard_ceiling(),
                    self.storage.budget_bytes
                ),
            ));
        }
        if self.firehose.urls.is_empty() {
            return Err(invalid(
                "firehose.urls",
                "at least one Jetstream URL is required",
            ));
        }
        for u in &self.firehose.urls {
            if !(u.starts_with("wss://") || u.starts_with("ws://")) {
                return Err(invalid(
                    "firehose.urls",
                    format!("{u} is not a ws:// or wss:// URL"),
                ));
            }
        }
        for (key, list) in [
            ("backfill.retry_schedule", &self.backfill.retry_schedule),
            ("backfill.missing_retry", &self.backfill.missing_retry),
            ("backfill.phase1_retry", &self.backfill.phase1_retry),
        ] {
            if list.is_empty() {
                return Err(invalid(key, "must not be empty"));
            }
        }
        if self.backfill.tier_shares.len() != 3
            || self.backfill.tier_shares.iter().sum::<u32>() != 100
        {
            return Err(invalid(
                "backfill.tier_shares",
                "expected three shares summing to 100",
            ));
        }
        let b = &self.backfill;
        if b.concurrency == 0 || b.concurrency > MAX_BACKFILL_CONCURRENCY {
            return Err(invalid(
                "backfill.concurrency",
                format!("must be between 1 and {MAX_BACKFILL_CONCURRENCY}"),
            ));
        }
        // A zero wait would have a failing job, or a list that is not
        // there, tried again without pause.
        for (key, list) in [
            ("backfill.retry_schedule", &b.retry_schedule),
            ("backfill.missing_retry", &b.missing_retry),
            ("backfill.phase1_retry", &b.phase1_retry),
        ] {
            if list.iter().any(|d| d.get().is_zero()) {
                return Err(invalid(key, "every step must be longer than 0s"));
            }
        }
        for (key, value) in [
            ("backfill.per_host_rps", b.per_host_rps),
            ("backfill.per_host_concurrency", b.per_host_concurrency),
            ("backfill.per_domain_rps", b.per_domain_rps),
            ("backfill.per_domain_concurrency", b.per_domain_concurrency),
            ("backfill.plc_rps", b.plc_rps),
            (
                "backfill.list_fetch_max_attempts",
                b.list_fetch_max_attempts,
            ),
        ] {
            if value == 0 {
                return Err(invalid(key, "must be positive"));
            }
        }
        if b.system_queue_cap == 0 {
            return Err(invalid("backfill.system_queue_cap", "must be positive"));
        }
        for (key, value) in [
            ("backfill.terminal_after", b.terminal_after),
            (
                "backfill.list_fetch_max_duration",
                b.list_fetch_max_duration,
            ),
            ("backfill.repo_job_max_duration", b.repo_job_max_duration),
        ] {
            if value.get().is_zero() {
                return Err(invalid(key, "must be longer than 0s"));
            }
        }
        for (key, value, required) in [
            ("backfill.relay_url", b.relay_url.as_str(), true),
            ("backfill.plc_url", b.plc_url.as_str(), true),
            ("backfill.backlinks.url", b.backlinks.url.as_str(), false),
        ] {
            if (required || !value.is_empty()) && !is_http_url(value) {
                return Err(invalid(
                    key,
                    "expected an http:// or https:// URL with a host and no credentials",
                ));
            }
        }
        // A tombstone has to outlive every listing stamp that could still
        // be applied: a listing read before a delete would otherwise put
        // the record back once the tombstone is gone.
        if self.storage.tombstone_ttl.get() < LISTING_STAMP_VALIDITY {
            return Err(invalid(
                "storage.tombstone_ttl",
                "must be at least 72h, the time a listing may still be applied after it was read",
            ));
        }
        self.validate_bounds()?;
        self.validate_public_ui()?;
        let mut warnings = Vec::new();
        if self.public_ui.card_burst < self.public_ui.card_rps {
            warnings.push(format!(
                "public_ui.card_burst ({}) is below public_ui.card_rps ({}); using {} as the burst",
                self.public_ui.card_burst, self.public_ui.card_rps, self.public_ui.card_rps
            ));
        }
        for net in &self.proxy.trusted {
            validate_trusted_proxy(net).map_err(|r| invalid("proxy.trusted", r))?;
            if is_public_net(net) && !crate::cloudflare::contains_net(net) {
                warnings.push(format!(
                    "proxy.trusted contains public range {net}; confirm it belongs to your proxy"
                ));
            }
        }
        if self.proxy.mode != ProxyMode::None && self.proxy.trusted.is_empty() {
            warnings.push(
                "proxy.mode is set but proxy.trusted is empty: forwarding headers are ignored"
                    .to_owned(),
            );
        }
        if has_default_db_password(&self.storage.database_url) {
            warnings.push(DEFAULT_DB_PASSWORD_WARNING.to_owned());
        }
        let connections = self.database_connections();
        if connections > POSTGRES_DEFAULT_CONNECTIONS {
            warnings.push(format!(
                "rate_limit.query_concurrency ({}) and backfill.concurrency ({}) make up to \
                 {connections} database connections, more than Postgres allows by default \
                 ({POSTGRES_DEFAULT_CONNECTIONS}); raise max_connections in Postgres to at least \
                 that, or lower one of the two",
                self.rate_limit.query_concurrency, self.backfill.concurrency
            ));
        }
        Ok(warnings)
    }
}

/// `max_connections` of a Postgres nobody configured, the bundled one
/// included.
pub const POSTGRES_DEFAULT_CONNECTIONS: u32 = 100;

/// Connections the two processes open beyond the two configured pools:
/// 8 spare in each, 4 for ingest, 4 for the periodic tasks and 1 for
/// the sort-index builder.
pub const FIXED_DATABASE_CONNECTIONS: u32 = 25;

impl Config {
    /// The most database connections the server and the backfill process
    /// hold together under this config.
    pub fn database_connections(&self) -> u32 {
        self.rate_limit
            .query_concurrency
            .saturating_add(self.backfill.concurrency)
            .saturating_add(FIXED_DATABASE_CONNECTIONS)
    }
}

/// The longest any duration in the config may be. Every one of them is
/// added to a time somewhere, and a value near the largest a duration
/// can hold would overflow there.
pub const MAX_CONFIG_DURATION: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

impl Config {
    /// The bounds of the durations and limits a wrong value of which
    /// would make answers claim more than is known, stop a subsystem, or
    /// overflow.
    fn validate_bounds(&self) -> Result<(), ConfigError> {
        const SEC: Duration = Duration::from_secs(1);
        const HOUR: Duration = Duration::from_secs(3600);
        const DAY: Duration = Duration::from_secs(24 * 3600);
        let t = &self.firehose.tuning;
        let b = &self.backfill;
        // (key, value, at least, at most)
        let bounded: [(&str, ConfigDuration, Duration, Duration); 20] = [
            // Answers call the index current for this long after the
            // last applied event: hours of it would hide an outage.
            (
                "firehose.tuning.synthetic_gap_lag",
                t.synthetic_gap_lag,
                SEC,
                HOUR,
            ),
            // Zero would end every session at once; too long would
            // leave a dead one in place.
            ("firehose.tuning.stall_timeout", t.stall_timeout, SEC, HOUR),
            ("firehose.tuning.gap_threshold", t.gap_threshold, SEC, DAY),
            (
                "firehose.tuning.failover_rewind_min",
                t.failover_rewind_min,
                Duration::ZERO,
                DAY,
            ),
            (
                "firehose.tuning.failover_max_lag",
                t.failover_max_lag,
                Duration::ZERO,
                DAY,
            ),
            (
                "firehose.tuning.seam_repair_before",
                t.seam_repair_before,
                Duration::ZERO,
                DAY,
            ),
            (
                "firehose.tuning.seam_repair_after",
                t.seam_repair_after,
                Duration::ZERO,
                DAY,
            ),
            (
                "firehose.tuning.seam_repair_delay",
                t.seam_repair_delay,
                Duration::ZERO,
                DAY,
            ),
            (
                "firehose.tuning.seam_repair_catchup_margin",
                t.seam_repair_catchup_margin,
                Duration::ZERO,
                DAY,
            ),
            (
                "storage.tombstone_ttl",
                self.storage.tombstone_ttl,
                Duration::ZERO,
                MAX_CONFIG_DURATION,
            ),
            (
                "storage.block_history_retention",
                self.storage.block_history_retention,
                Duration::ZERO,
                MAX_CONFIG_DURATION,
            ),
            (
                "backfill.request_fresh_window",
                b.request_fresh_window,
                Duration::ZERO,
                MAX_CONFIG_DURATION,
            ),
            (
                "backfill.owner_fetch_cooldown",
                b.owner_fetch_cooldown,
                Duration::ZERO,
                MAX_CONFIG_DURATION,
            ),
            (
                "backfill.terminal_after",
                b.terminal_after,
                SEC,
                MAX_CONFIG_DURATION,
            ),
            (
                "backfill.list_fetch_max_duration",
                b.list_fetch_max_duration,
                SEC,
                7 * DAY,
            ),
            (
                "backfill.repo_job_max_duration",
                b.repo_job_max_duration,
                SEC,
                7 * DAY,
            ),
            (
                "backfill.repair_slack",
                b.repair_slack,
                Duration::ZERO,
                MAX_CONFIG_DURATION,
            ),
            (
                "backfill.backlinks.lag_allowance",
                b.backlinks.lag_allowance,
                Duration::ZERO,
                DAY,
            ),
            (
                "limits.list_grace",
                self.limits.list_grace,
                Duration::ZERO,
                MAX_CONFIG_DURATION,
            ),
            (
                "limits.pending_max_age",
                self.limits.pending_max_age,
                SEC,
                MAX_CONFIG_DURATION,
            ),
        ];
        for (key, value, least, most) in bounded {
            let v = value.get();
            if v < least || v > most {
                return Err(invalid(
                    key,
                    format!(
                        "must be between {} and {}",
                        ConfigDuration::secs(least.as_secs()),
                        ConfigDuration::secs(most.as_secs())
                    ),
                ));
            }
        }
        for (key, list) in [
            ("backfill.retry_schedule", &b.retry_schedule),
            ("backfill.missing_retry", &b.missing_retry),
            ("backfill.phase1_retry", &b.phase1_retry),
        ] {
            if list.iter().any(|d| d.get() > MAX_CONFIG_DURATION) {
                return Err(invalid(
                    key,
                    format!(
                        "no step may be longer than {}",
                        ConfigDuration::secs(MAX_CONFIG_DURATION.as_secs())
                    ),
                ));
            }
        }
        let q = self.rate_limit.query_timeout.get();
        if q.is_zero() || q > HOUR {
            return Err(invalid(
                "rate_limit.query_timeout",
                "must be longer than 0s and at most 1h",
            ));
        }
        if self.rate_limit.query_concurrency == 0 {
            return Err(invalid("rate_limit.query_concurrency", "must be positive"));
        }
        // Enumeration runs only below this bound: at zero no cycle, full
        // or repair, would ever enumerate.
        if b.sweep.max_outstanding == 0 {
            return Err(invalid(
                "backfill.sweep.max_outstanding",
                "must be positive",
            ));
        }
        Ok(())
    }
}

impl Config {
    /// The public UI rules: the access combination, and the bounds of
    /// `[public_ui]`. The same check runs at load and at every settings
    /// save.
    fn validate_public_ui(&self) -> Result<(), ConfigError> {
        if self.access.public_ui && self.access.reads != ReadsMode::Public {
            return Err(invalid(
                "access.public_ui",
                "the public UI needs access.reads = \"public\"; change access.reads, or turn \
                 access.public_ui off",
            ));
        }
        let p = &self.public_ui;
        if p.excluded_dids.len() > MAX_EXCLUDED_DIDS {
            return Err(invalid(
                "public_ui.excluded_dids",
                format!(
                    "{} entries; at most {MAX_EXCLUDED_DIDS} are allowed",
                    p.excluded_dids.len()
                ),
            ));
        }
        for d in &p.excluded_dids {
            if let Err(e) = crate::Did::parse(d) {
                return Err(invalid(
                    "public_ui.excluded_dids",
                    format!("{d:?} is not a DID: {e}"),
                ));
            }
        }
        if p.instance_description.chars().count() > MAX_INSTANCE_DESCRIPTION {
            return Err(invalid(
                "public_ui.instance_description",
                format!("at most {MAX_INSTANCE_DESCRIPTION} characters"),
            ));
        }
        if p.contact.chars().count() > MAX_PUBLIC_CONTACT {
            return Err(invalid(
                "public_ui.contact",
                format!("at most {MAX_PUBLIC_CONTACT} characters"),
            ));
        }
        if p.rate_limit_rps == 0 {
            return Err(invalid("public_ui.rate_limit_rps", "must be positive"));
        }
        if p.rate_limit_burst == 0 {
            return Err(invalid("public_ui.rate_limit_burst", "must be positive"));
        }
        if p.query_concurrency == 0 {
            return Err(invalid("public_ui.query_concurrency", "must be positive"));
        }
        if p.card_rps == 0 {
            return Err(invalid("public_ui.card_rps", "must be positive"));
        }
        if p.card_burst == 0 {
            return Err(invalid("public_ui.card_burst", "must be positive"));
        }
        if p.handle_rps == 0 || p.handle_rps > MAX_HANDLE_RPS {
            return Err(invalid("public_ui.handle_rps", "must be between 1 and 200"));
        }
        if p.handle_pass_rps > MAX_HANDLE_RPS {
            return Err(invalid(
                "public_ui.handle_pass_rps",
                "must be between 0 and 200",
            ));
        }
        if !p.record_viewer_url.is_empty() {
            validate_record_viewer_url(&p.record_viewer_url)
                .map_err(|r| invalid("public_ui.record_viewer_url", r))?;
        }
        // Checked only while the public UI is on: a config without
        // `[public_ui]`, with a lowered `rate_limit.query_concurrency`,
        // must load with the defaults of a feature it does not use.
        if self.access.public_ui && p.query_concurrency > self.rate_limit.query_concurrency {
            return Err(invalid(
                "public_ui.query_concurrency",
                format!(
                    "must be at most rate_limit.query_concurrency ({})",
                    self.rate_limit.query_concurrency
                ),
            ));
        }
        Ok(())
    }
}

/// Serializes a config as TOML for `config.toml`. An unset
/// `access.admin_did` is left out rather than written empty.
pub fn to_toml(config: &Config) -> Result<String, ConfigError> {
    let mut table =
        toml::Table::try_from(config).map_err(|e| ConfigError::Schema(e.to_string()))?;
    if let Some(t) = table.get_mut("access").and_then(toml::Value::as_table_mut)
        && t.get("admin_did").and_then(toml::Value::as_str) == Some("")
    {
        t.remove("admin_did");
    }
    toml::to_string_pretty(&table).map_err(|e| ConfigError::Schema(e.to_string()))
}

/// Sets `access.admin_did` in a `config.toml` text and returns the new
/// text. The file need not load before the edit.
pub fn set_admin_did(text: &str, did: &str) -> Result<String, String> {
    if !valid_admin_did(did) {
        return Err(format!("{did} is not a did:plc or did:web DID"));
    }
    let mut table: toml::Table = text.parse().map_err(|e: toml::de::Error| e.to_string())?;
    table
        .entry("access")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .ok_or("`access` is not a table")?
        .insert("admin_did".to_owned(), toml::Value::String(did.to_owned()));
    toml::to_string_pretty(&table).map_err(|e| e.to_string())
}

fn temp_path(path: &Path) -> std::path::PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(format!(".tmp-{}-{}", std::process::id(), unique_suffix()));
    path.with_file_name(name)
}

fn unique_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    t ^ N.fetch_add(1, Ordering::Relaxed).rotate_left(32)
}

fn write_private(path: &Path, text: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    f.write_all(text.as_bytes())?;
    f.sync_all()
}

/// Writes `text` to `path` only if `path` does not exist yet (first
/// writer wins): the content goes to a temp file (0600) that is then
/// hard-linked into place, which fails if another writer got there
/// first. Returns `Ok(false)` in that case.
pub fn write_new(path: &Path, text: &str) -> std::io::Result<bool> {
    let tmp = temp_path(path);
    write_private(&tmp, text)?;
    let linked = std::fs::hard_link(&tmp, path);
    let _ = std::fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

/// Atomically replaces `path` with `text` (temp file, 0600, then rename;
/// settings edits).
pub fn write_replace(path: &Path, text: &str) -> std::io::Result<()> {
    let tmp = temp_path(path);
    write_private(&tmp, text)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    const TOKEN_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const ADMIN_DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    fn complete() -> Config {
        let mut c = Config::default();
        c.server.hostname = "farsight.test".to_owned();
        c.server.contact = "mailto:ops@farsight.test".to_owned();
        c.storage.database_url = "postgres://farsight:pw@postgres:5432/farsight".to_owned();
        c.auth.admin_token_sha256 = TOKEN_HASH.to_owned();
        c.access.admin_did = ADMIN_DID.to_owned();
        c
    }

    #[test]
    fn defaults_match_design() {
        let c = Config::default();
        assert_eq!(c.server.bind, "0.0.0.0:8080");
        assert_eq!(c.storage.budget_bytes, 70_000_000_000);
        assert_eq!(c.storage.effective_hard_ceiling(), 80_500_000_000);
        assert_eq!(c.storage.tombstone_ttl, ConfigDuration::days(7));
        assert!(c.storage.block_history_enabled);
        assert_eq!(c.storage.block_history_retention, ConfigDuration::days(365));
        assert_eq!(c.limits.history_per_did_per_day, 10_000);
        assert_eq!(c.limits.history_per_bucket_per_day, 200_000);
        assert!(!c.access.public_ui);
        let p = &c.public_ui;
        assert!(!p.show_outgoing_blocks && p.show_opengraph_image && p.show_avatars);
        assert!(p.record_viewer_url.is_empty());
        assert_eq!((p.card_rps, p.card_burst), (4, 8));
        assert!(p.handle_warming_enabled);
        assert_eq!((p.handle_rps, p.handle_burst()), (20, 20));
        assert_eq!(p.handle_pass_rps, 0);
        assert!(!p.crawlable && p.excluded_dids.is_empty());
        assert_eq!(p.dark_mode_default, ThemeDefault::System);
        assert_eq!((p.rate_limit_rps, p.rate_limit_burst), (5, 20));
        assert_eq!(p.query_concurrency, 8);
        assert_eq!(p.handle_cache_ttl, ConfigDuration::hours(1));
        assert_eq!(
            c.firehose.urls,
            [
                "wss://jetstream.us-east.bsky.network",
                "wss://jetstream.us-west.bsky.network"
            ]
        );
        assert_eq!(c.backfill.tier_shares, [60, 25, 15]);
        assert_eq!(c.backfill.retry_schedule.len(), 4);
        assert_eq!(c.backfill.sweep.source, SweepSource::RelayCollections);
        assert_eq!(c.backfill.backlinks.max_refs, 200_000);
        assert_eq!(c.limits.listblock_fetch_triggers_per_author, 5_000);
        assert_eq!(c.limits.did_admissions_per_day, 200);
        assert_eq!(c.limits.large_hosts, ["*.host.bsky.network"]);
        assert_eq!(c.rate_limit.query_timeout, ConfigDuration::secs(5));
        assert_eq!(c.metrics.backfill_bind, "127.0.0.1:9465");
        assert_eq!(c.firehose.tuning.synthetic_gap_lag, ConfigDuration::mins(5));
        assert_eq!(
            c.firehose.tuning.seam_repair_before,
            ConfigDuration::secs(150)
        );
        assert_eq!(
            c.firehose.tuning.seam_repair_after,
            ConfigDuration::secs(30)
        );
        assert_eq!(
            c.firehose.tuning.seam_repair_delay,
            ConfigDuration::secs(60)
        );
        assert_eq!(
            c.firehose.tuning.seam_repair_catchup_margin,
            ConfigDuration::secs(5)
        );
    }

    #[test]
    fn toml_round_trip() {
        let c = complete();
        let text = toml::to_string(&c).unwrap();
        let back = load_from_parts(Some(&text), &[]).unwrap();
        assert_eq!(back.config, c);
        // The bare defaults round-trip too (validation aside).
        let d = Config::default();
        let text = toml::to_string(&d).unwrap();
        let parsed: Config = toml::from_str(&text).unwrap();
        assert_eq!(parsed, d);
    }

    #[test]
    fn design_example_parses() {
        let text = r#"
[server]
bind = "0.0.0.0:8080"
hostname = "farsight.example"
contact = "mailto:ops@example"

[storage]
database_url = "postgres://farsight:x@postgres:5432/farsight"
budget_bytes = 70_000_000_000
hard_ceiling_bytes = 0
tombstone_ttl = "7d"

[backfill]
retry_schedule = ["1h", "6h", "24h", "1d"]
seen_set_cap = 2_000_000

[backfill.sweep]
enabled = true
source = "relay_collections"

[auth]
admin_token_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"

[access]
admin_did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"

[proxy]
mode = "cloudflare"
trusted = ["173.245.48.0/20"]

[firehose.tuning]
gap_threshold = "300s"
"#;
        let loaded = load_from_parts(Some(text), &[]).unwrap();
        assert_eq!(loaded.config.proxy.mode, ProxyMode::Cloudflare);
        assert_eq!(loaded.config.proxy.trusted.len(), 1);
        // A range inside the bundled Cloudflare set is not warned about;
        // other public space is a warning, not an error.
        assert!(loaded.warnings.is_empty(), "{:?}", loaded.warnings);
        let other = text.replace("173.245.48.0/20", "203.0.113.0/24");
        let loaded = load_from_parts(Some(&other), &[]).unwrap();
        assert!(!loaded.warnings.is_empty());
    }

    #[test]
    fn unknown_keys_rejected() {
        let e = load_from_parts(Some("[server]\nbindd = \"x\"\n"), &[]).unwrap_err();
        assert!(matches!(e, ConfigError::Schema(_)));
        let e = load_from_parts(Some("[nope]\n"), &[]).unwrap_err();
        assert!(matches!(e, ConfigError::Schema(_)));
        // The message names the key.
        let e = load_from_parts(Some("[public_ui]\nshow_everything = true\n"), &[]).unwrap_err();
        assert!(matches!(e, ConfigError::Schema(_)));
        assert!(e.to_string().contains("`show_everything`"), "{e}");
    }

    #[test]
    fn env_overrides_nested_and_lists() {
        let text = toml::to_string(&complete()).unwrap();
        let loaded = load_from_parts(
            Some(&text),
            &env(&[
                ("FARSIGHT__BACKFILL__SWEEP__ENABLED", "false"),
                ("FARSIGHT__BACKFILL__SWEEP__MAX_OUTSTANDING", "25_000"),
                (
                    "FARSIGHT__FIREHOSE__URLS",
                    "wss://a.example, wss://b.example",
                ),
                ("FARSIGHT__FIREHOSE__TUNING__STALL_TIMEOUT", "90s"),
                ("FARSIGHT__BACKFILL__RETRY_SCHEDULE", "30m,2h"),
                ("FARSIGHT__BACKFILL__TIER_SHARES", "50,30,20"),
                ("FARSIGHT__PROXY__TRUSTED", "10.0.0.0/8,fd00::/64"),
                ("FARSIGHT__PROXY__MODE", "forwarded"),
                ("FARSIGHT__LIMITS__LARGE_HOSTS", ""),
                ("FARSIGHT_SKIP_WIZARD", "1"),
                ("UNRELATED", "x"),
            ]),
        )
        .unwrap();
        let c = &loaded.config;
        assert!(!c.backfill.sweep.enabled);
        assert_eq!(c.backfill.sweep.max_outstanding, 25_000);
        assert_eq!(c.firehose.urls, ["wss://a.example", "wss://b.example"]);
        assert_eq!(c.firehose.tuning.stall_timeout, ConfigDuration::secs(90));
        assert_eq!(
            c.backfill.retry_schedule,
            [ConfigDuration::mins(30), ConfigDuration::hours(2)]
        );
        assert_eq!(c.backfill.tier_shares, [50, 30, 20]);
        assert_eq!(c.proxy.trusted.len(), 2);
        assert_eq!(c.proxy.mode, ProxyMode::Forwarded);
        assert!(c.limits.large_hosts.is_empty());
        assert!(
            loaded
                .env_keys
                .contains(&"backfill.sweep.enabled".to_owned())
        );
        assert!(
            loaded
                .env_keys
                .contains(&"firehose.tuning.stall_timeout".to_owned())
        );
        assert!(!loaded.from_env_only);
    }

    #[test]
    fn env_errors() {
        let text = toml::to_string(&complete()).unwrap();
        for (k, v) in [
            ("FARSIGHT__BACKFILL__NOPE", "1"),
            ("FARSIGHT__BACKFILL__SWEEP", "1"),
            ("FARSIGHT____X", "1"),
            ("FARSIGHT__ACCESS__NOPE", "disabled"),
        ] {
            assert!(matches!(
                load_from_parts(Some(&text), &env(&[(k, v)])),
                Err(ConfigError::UnknownEnvKey(_))
            ));
        }
        assert!(matches!(
            load_from_parts(
                Some(&text),
                &env(&[("FARSIGHT__BACKFILL__CONCURRENCY", "many")])
            ),
            Err(ConfigError::EnvValue { .. })
        ));
        assert!(matches!(
            load_from_parts(
                Some(&text),
                &env(&[("FARSIGHT__BACKFILL__SWEEP__ENABLED", "maybe")])
            ),
            Err(ConfigError::EnvValue { .. })
        ));
        assert!(matches!(
            load_from_parts(
                Some(&text),
                &env(&[("FARSIGHT__STORAGE__TOMBSTONE_TTL", "7x")])
            ),
            Err(ConfigError::Schema(_))
        ));
    }

    #[test]
    fn skip_wizard_env_only() {
        let e =
            load_from_parts(None, &env(&[("FARSIGHT__SERVER__HOSTNAME", "h.test")])).unwrap_err();
        let ConfigError::MissingKeys(keys) = e else {
            panic!("expected MissingKeys")
        };
        assert_eq!(
            keys,
            [
                "server.contact",
                "storage.database_url",
                "auth.admin_token_sha256"
            ]
        );
        let ok = load_from_parts(
            None,
            &env(&[
                ("FARSIGHT__SERVER__HOSTNAME", "h.test"),
                ("FARSIGHT__SERVER__CONTACT", "mailto:x@h.test"),
                ("FARSIGHT__STORAGE__DATABASE_URL", "postgres://x"),
                ("FARSIGHT__AUTH__ADMIN_TOKEN_SHA256", TOKEN_HASH),
                ("FARSIGHT__ACCESS__ADMIN_UI", "false"),
            ]),
        )
        .unwrap();
        assert!(ok.from_env_only);
        assert!(!ok.config.access.admin_ui);
        assert_eq!(ok.admin_auth(), AdminAuth::Disabled);
    }

    #[test]
    fn start_mode() {
        let missing = Path::new("/nonexistent/farsight/config.toml");
        assert!(matches!(load(missing, &[]), Ok(StartMode::Setup)));
        assert!(matches!(
            load(missing, &env(&[(SKIP_WIZARD_ENV, "1")])),
            Err(ConfigError::MissingKeys(_))
        ));
        assert!(matches!(
            load(missing, &env(&[(SKIP_WIZARD_ENV, "0")])),
            Ok(StartMode::Setup)
        ));
    }

    #[test]
    fn ceiling_must_exceed_budget() {
        let mut c = complete();
        c.storage.hard_ceiling_bytes = c.storage.budget_bytes;
        assert!(matches!(c.validate(), Err(ConfigError::Invalid { .. })));
        c.storage.hard_ceiling_bytes = c.storage.budget_bytes + 1;
        assert!(c.validate().is_ok());
        c.storage.hard_ceiling_bytes = 0;
        assert!(c.validate().is_ok());
        c.storage.budget_bytes = 1;
        // 115% of 1 byte rounds down to 1: not above the budget.
        assert!(c.validate().is_err());
    }

    #[test]
    fn proxy_validation() {
        for bad in ["0.0.0.0/0", "::/0", "10.0.0.0/7", "2001::/23"] {
            let mut c = complete();
            c.proxy.trusted = vec![bad.parse().unwrap()];
            assert!(c.validate().is_err(), "accepted {bad}");
        }
        for ok in [
            "10.0.0.0/8",
            "172.16.0.0/12",
            "127.0.0.1/32",
            "2001:db8::/32",
        ] {
            let mut c = complete();
            c.proxy.trusted = vec![ok.parse().unwrap()];
            assert!(c.validate().is_ok(), "rejected {ok}");
        }
        let mut c = complete();
        c.proxy.trusted = vec!["10.0.0.0/8".parse().unwrap()];
        assert!(c.validate().unwrap().is_empty());
    }

    #[test]
    fn the_default_database_password_is_warned_about() {
        let mut c = complete();
        assert!(c.validate().unwrap().is_empty());
        c.storage.database_url = "postgres://farsight:farsight@postgres:5432/farsight".into();
        assert_eq!(c.validate().unwrap(), [DEFAULT_DB_PASSWORD_WARNING]);
        // A syntax error names its line and never quotes it.
        let broken = "[storage]\ndatabase_url = \"postgres://farsight:s3cret@db/f\n";
        let e = load_from_parts(Some(broken), &[]).unwrap_err().to_string();
        assert!(e.contains("line 2"), "{e}");
        assert!(!e.contains("s3cret"), "{e}");
        // Pools larger than Postgres takes by default are named.
        let mut wide = complete();
        assert_eq!(wide.database_connections(), 89);
        wide.backfill.concurrency = 64;
        let w = wide.validate().unwrap();
        assert!(
            w.len() == 1 && w[0].contains("121 database connections"),
            "{w:?}"
        );
        // Another user, or no password in the URL: nothing to say.
        c.storage.database_url = "postgres://other:farsight@postgres:5432/farsight".into();
        assert!(c.validate().unwrap().is_empty());
        c.storage.database_url = "postgres://farsight@postgres:5432/farsight".into();
        assert!(c.validate().unwrap().is_empty());
    }

    #[test]
    fn hostname_and_contact_are_one_line_of_text() {
        type Edit = fn(&mut Config);
        let refused: [(&str, Edit); 4] = [
            ("server.contact", |c| {
                c.server.contact = "mailto:ops@farsight.test\nX-Injected: 1".into()
            }),
            ("server.contact", |c| c.server.contact = "ops\u{0}".into()),
            ("server.hostname", |c| {
                c.server.hostname = "farsight.test\r".into()
            }),
            ("server.hostname", |c| {
                c.server.hostname = "farsight\u{7f}.test".into()
            }),
        ];
        for (key, edit) in refused {
            let mut c = complete();
            edit(&mut c);
            let e = c.validate().unwrap_err().to_string();
            assert!(
                e.contains(key) && e.contains("control characters"),
                "{key}: {e}"
            );
        }
        // Text in any script is fine.
        let mut c = complete();
        c.server.contact = "Kontakt: börje@exempel.example".into();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn backfill_keys_that_would_loop_or_stall_are_refused() {
        type Edit = fn(&mut Config);
        let refused: [(&str, Edit); 15] = [
            ("backfill.retry_schedule", |c| {
                c.backfill.retry_schedule = vec![ConfigDuration::secs(0)]
            }),
            ("backfill.missing_retry", |c| {
                c.backfill.missing_retry[1] = ConfigDuration::secs(0)
            }),
            ("backfill.phase1_retry", |c| {
                c.backfill.phase1_retry = vec![ConfigDuration::secs(0)]
            }),
            ("backfill.system_queue_cap", |c| {
                c.backfill.system_queue_cap = 0
            }),
            ("backfill.concurrency", |c| c.backfill.concurrency = 0),
            ("backfill.concurrency", |c| {
                c.backfill.concurrency = MAX_BACKFILL_CONCURRENCY + 1
            }),
            ("backfill.per_host_rps", |c| c.backfill.per_host_rps = 0),
            ("backfill.per_domain_concurrency", |c| {
                c.backfill.per_domain_concurrency = 0
            }),
            ("backfill.repo_job_max_duration", |c| {
                c.backfill.repo_job_max_duration = ConfigDuration::secs(0)
            }),
            ("backfill.relay_url", |c| {
                c.backfill.relay_url = "ftp://relay.example".into()
            }),
            ("backfill.relay_url", |c| {
                c.backfill.relay_url = String::new()
            }),
            ("backfill.plc_url", |c| {
                c.backfill.plc_url = "plc.directory".into()
            }),
            ("backfill.plc_url", |c| {
                c.backfill.plc_url = "https://user:pw@plc.example".into()
            }),
            ("backfill.backlinks.url", |c| {
                c.backfill.backlinks.url = "file:///etc/passwd".into()
            }),
            ("storage.tombstone_ttl", |c| {
                c.storage.tombstone_ttl = ConfigDuration::hours(71)
            }),
        ];
        for (key, edit) in refused {
            let mut c = complete();
            edit(&mut c);
            let e = c.validate().unwrap_err().to_string();
            assert!(e.contains(key), "{key}: {e}");
        }
        let bounds: [(&str, Edit); 9] = [
            // Days behind and still "complete".
            ("firehose.tuning.synthetic_gap_lag", |c| {
                c.firehose.tuning.synthetic_gap_lag = ConfigDuration::days(3650)
            }),
            ("firehose.tuning.synthetic_gap_lag", |c| {
                c.firehose.tuning.synthetic_gap_lag = ConfigDuration::secs(0)
            }),
            // Every session ended at once.
            ("firehose.tuning.stall_timeout", |c| {
                c.firehose.tuning.stall_timeout = ConfigDuration::secs(0)
            }),
            // No cycle enumerates.
            ("backfill.sweep.max_outstanding", |c| {
                c.backfill.sweep.max_outstanding = 0
            }),
            // Past what a time can be moved by.
            ("storage.tombstone_ttl", |c| {
                c.storage.tombstone_ttl = ConfigDuration::secs(u64::MAX / 2)
            }),
            ("backfill.terminal_after", |c| {
                c.backfill.terminal_after = ConfigDuration::secs(u64::MAX / 2)
            }),
            ("backfill.retry_schedule", |c| {
                c.backfill.retry_schedule = vec![ConfigDuration::secs(u64::MAX / 2)]
            }),
            ("rate_limit.query_timeout", |c| {
                c.rate_limit.query_timeout = ConfigDuration::secs(0)
            }),
            ("rate_limit.query_concurrency", |c| {
                c.rate_limit.query_concurrency = 0
            }),
        ];
        for (key, edit) in bounds {
            let mut c = complete();
            edit(&mut c);
            let e = c.validate().unwrap_err().to_string();
            assert!(e.contains(key), "{key}: {e}");
        }
        let accepted: [Edit; 5] = [
            |c| c.backfill.concurrency = MAX_BACKFILL_CONCURRENCY,
            |c| c.backfill.relay_url = "http://127.0.0.1:2470".into(),
            |c| c.backfill.backlinks.url = "https://links.example/".into(),
            |c| c.storage.tombstone_ttl = ConfigDuration::hours(72),
            // More workers kept than there are: all but one are kept.
            |c| c.backfill.on_demand_reserved = 10_000,
        ];
        for edit in accepted {
            let mut c = complete();
            edit(&mut c);
            assert!(c.validate().is_ok());
        }
        // The metrics listeners are loopback unless set.
        let c = Config::default();
        assert!(c.metrics.bind.starts_with("127.0.0.1:"));
        assert!(c.metrics.backfill_bind.starts_with("127.0.0.1:"));
    }

    #[test]
    fn public_ui_needs_public_reads() {
        // The public UI is fine with the admin UI on or off.
        for admin_ui in [true, false] {
            let mut c = complete();
            c.access.public_ui = true;
            c.access.admin_ui = admin_ui;
            assert!(c.validate().is_ok(), "{admin_ui}");
        }
        for reads in [ReadsMode::ApiKey, ReadsMode::Disabled] {
            let mut c = complete();
            c.access.public_ui = true;
            c.access.reads = reads;
            let e = c.validate().unwrap_err().to_string();
            assert!(e.contains("access.reads"), "{reads:?}: {e}");
            // The same combination is fine while the public UI is off.
            c.access.public_ui = false;
            assert!(c.validate().is_ok());
        }
        let text = toml::to_string(&complete()).unwrap();
        assert!(matches!(
            load_from_parts(
                Some(&text),
                &env(&[
                    ("FARSIGHT__ACCESS__PUBLIC_UI", "true"),
                    ("FARSIGHT__ACCESS__READS", "api_key")
                ])
            ),
            Err(ConfigError::Invalid { .. })
        ));
    }

    #[test]
    fn public_ui_bounds() {
        let mut c = complete();
        c.public_ui.excluded_dids = vec!["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".to_owned()];
        assert!(c.validate().is_ok());
        c.public_ui.excluded_dids.push("alice.example".to_owned());
        assert!(c.validate().is_err());
        let mut c = complete();
        c.public_ui.excluded_dids =
            vec!["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".to_owned(); MAX_EXCLUDED_DIDS + 1];
        assert!(c.validate().is_err());
        c.public_ui.excluded_dids.truncate(MAX_EXCLUDED_DIDS);
        assert!(c.validate().is_ok());
        let mut c = complete();
        c.public_ui.query_concurrency = c.rate_limit.query_concurrency + 1;
        // Not a load failure while the public UI is off: a config that
        // lowers the global bound and does not use the public UI loads.
        assert!(c.validate().is_ok());
        c.access.public_ui = true;
        assert!(c.validate().is_err());
        let mut c = complete();
        c.rate_limit.query_concurrency = 4;
        assert!(c.validate().is_ok());
        c.public_ui.query_concurrency = 0;
        assert!(c.validate().is_err());
        let mut c = complete();
        c.public_ui.instance_description = "x".repeat(MAX_INSTANCE_DESCRIPTION + 1);
        assert!(c.validate().is_err());
        let mut c = complete();
        c.public_ui.rate_limit_rps = 0;
        assert!(c.validate().is_err());
        let text = "[public_ui]\ndark_mode_default = \"sepia\"\n";
        assert!(matches!(
            load_from_parts(Some(text), &[]),
            Err(ConfigError::Schema(_))
        ));
    }

    #[test]
    fn record_viewer_url_rules() {
        let ok = validate_record_viewer_url;
        assert!(ok("https://viewer.example/at/{authority}/{collection}/{rkey}").is_ok());
        assert!(ok("http://viewer.example:8080/?u=at://{authority}/{collection}/{rkey}").is_ok());
        assert!(ok("https://viewer.example/{authority}/{collection}/{rkey}?again={rkey}").is_ok());
        let e = ok("https://viewer.example/at/{authority}/{collection}").unwrap_err();
        assert!(e.contains("`{rkey}` is missing"), "{e}");
        assert!(ok("https://viewer.example/{authority}/{collection}/{rkey}/{did}").is_err());
        assert!(ok("https://{authority}.example/{collection}/{rkey}").is_err());
        assert!(ok("https://viewer.example?u={authority}/{collection}/{rkey}").is_err());
        assert!(ok("{authority}/{collection}/{rkey}").is_err());
        assert!(ok("javascript:alert('{authority}{collection}{rkey}')").is_err());
        assert!(ok("ftp://viewer.example/{authority}/{collection}/{rkey}").is_err());
        assert!(ok("https://u:p@viewer.example/{authority}/{collection}/{rkey}").is_err());
        assert!(ok("https://viewer.example/ {authority}/{collection}/{rkey}").is_err());
        let long = format!(
            "https://viewer.example/{}/{{authority}}/{{collection}}/{{rkey}}",
            "x".repeat(MAX_RECORD_VIEWER_URL)
        );
        assert!(ok(&long).is_err());
        // An invalid value in the file is an invalid config.
        let mut c = complete();
        c.public_ui.record_viewer_url = "https://viewer.example/".into();
        assert!(matches!(c.validate(), Err(ConfigError::Invalid { .. })));
        c.public_ui.record_viewer_url =
            "https://viewer.example/at/{authority}/{collection}/{rkey}".into();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn card_budget_bounds() {
        let mut c = complete();
        assert!(c.validate().unwrap().is_empty());
        assert_eq!(c.public_ui.effective_card_burst(), 8);
        c.public_ui.card_rps = 10;
        c.public_ui.card_burst = 3;
        let w = c.validate().unwrap();
        assert!(w.len() == 1 && w[0].contains("card_burst"), "{w:?}");
        assert_eq!(c.public_ui.effective_card_burst(), 10);
        c.public_ui.card_rps = 0;
        assert!(c.validate().is_err());
        c.public_ui.card_rps = 1;
        c.public_ui.card_burst = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn large_hosts_glob() {
        let l = LimitsConfig::default();
        assert!(l.is_large_host("morel.us-east.host.bsky.network"));
        assert!(l.is_large_host("MOREL.us-east.host.bsky.network"));
        assert!(!l.is_large_host("host.bsky.network"));
        assert!(!l.is_large_host("evilhost.bsky.network"));
        assert!(!l.is_large_host("pds.example.com"));
    }

    #[test]
    fn admin_did_syntax() {
        for ok in [
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
            "did:plc:z72i7hdynmk6r22z27h6tvur",
            "did:web:alice.example",
            "did:web:pds.alice.example",
        ] {
            assert!(valid_admin_did(ok), "{ok}");
        }
        for bad in [
            "",
            "alice.example",
            "did:plc:short",
            "did:plc:AAAAAAAAAAAAAAAAAAAAAAAA",
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaa1",
            "did:web:localhost",
            "did:web:alice.example%3A8080",
            "did:web:alice.example:path",
            "did:web:Alice.Example",
            "did:web:10.0.0.1",
            "did:key:z6Mk",
        ] {
            assert!(!valid_admin_did(bad), "{bad}");
        }
        let mut c = complete();
        c.access.admin_did = "did:plc:nope".into();
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("access.admin_did")
        );
    }

    #[test]
    fn admin_auth_states() {
        let file = |c: &Config, envs: &[(&str, &str)]| {
            load_from_parts(Some(&to_toml(c).unwrap()), &env(envs)).unwrap()
        };
        // Configured.
        let l = file(&complete(), &[]);
        assert_eq!(l.admin_auth(), AdminAuth::Configured(ADMIN_DID.into()));
        assert!(l.warnings.is_empty());
        // No admin DID: unconfigured, and it loads.
        let mut none = complete();
        none.access.admin_did.clear();
        let l = file(&none, &[]);
        assert_eq!(l.admin_auth(), AdminAuth::Unconfigured);
        assert_eq!(l.warnings, [UNCONFIGURED_WARNING]);
        // The DID from the environment configures it.
        let l = file(&none, &[("FARSIGHT__ACCESS__ADMIN_DID", ADMIN_DID)]);
        assert_eq!(l.admin_auth(), AdminAuth::Configured(ADMIN_DID.into()));
        assert!(l.warnings.is_empty());
        // Admin UI off: the DID does not matter.
        none.access.admin_ui = false;
        let l = file(&none, &[]);
        assert_eq!(l.admin_auth(), AdminAuth::Disabled);
        assert!(l.warnings.is_empty());
        // Env-only without a DID: unconfigured.
        let l = load_from_parts(
            None,
            &env(&[
                ("FARSIGHT__SERVER__HOSTNAME", "h.test"),
                ("FARSIGHT__SERVER__CONTACT", "mailto:x@h.test"),
                ("FARSIGHT__STORAGE__DATABASE_URL", "postgres://x"),
                ("FARSIGHT__AUTH__ADMIN_TOKEN_SHA256", TOKEN_HASH),
            ]),
        )
        .unwrap();
        assert_eq!(l.admin_auth(), AdminAuth::Unconfigured);
        // A malformed DID fails the load.
        assert!(matches!(
            load_from_parts(
                Some(&to_toml(&none).unwrap()),
                &env(&[("FARSIGHT__ACCESS__ADMIN_DID", "alice.example")])
            ),
            Err(ConfigError::Invalid { .. })
        ));
    }

    #[test]
    fn unset_admin_did_is_not_written() {
        let mut c = complete();
        let text = to_toml(&c).unwrap();
        assert!(text.contains("admin_did = \"did:plc:"));
        c.access.admin_did.clear();
        c.access.admin_ui = false;
        assert!(!to_toml(&c).unwrap().contains("admin_did"));
    }

    #[test]
    fn set_admin_did_edits_the_file_text() {
        let old = "[access]\ncors = false\n\n[auth]\nadmin_token_sha256 = \"x\"\n";
        let text = set_admin_did(old, ADMIN_DID).unwrap();
        assert!(text.contains(&format!("admin_did = \"{ADMIN_DID}\"")));
        assert!(text.contains("admin_token_sha256") && text.contains("cors = false"));
        // An existing DID is replaced.
        let text = set_admin_did(&text, "did:web:alice.example").unwrap();
        assert!(text.contains("did:web:alice.example") && !text.contains(ADMIN_DID));
        // A file with no [access] table gets one.
        assert!(set_admin_did("", ADMIN_DID).unwrap().contains("[access]"));
        assert!(set_admin_did(old, "alice.example").is_err());
    }
}
