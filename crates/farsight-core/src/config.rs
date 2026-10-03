//! Configuration schema and loading (design §16, §8.2, §9.2).
//!
//! Sources, in increasing precedence: built-in defaults, `config.toml`,
//! then `FARSIGHT__<SECTION>__<KEY>` environment variables (one `__` per
//! nesting level, e.g. `FARSIGHT__BACKFILL__SWEEP__ENABLED`; list values
//! comma-separated). Environment values are typed by the default value at
//! the same path, so an unknown key is an error rather than silently
//! ignored.
//!
//! §16 is an example file. Where it shows a concrete value, that value is
//! the default here. Where it shows a placeholder (`hostname =
//! "farsight.example"`, `database_url = "postgres://farsight:…"`, the
//! `auth` hashes), the key has no default and is required; see
//! [`REQUIRED_KEYS`]. The one deliberate difference is `[proxy]`: §16 shows
//! a Cloudflare example (`mode = "cloudflare"`, a truncated range list),
//! but a fresh install trusts no proxy until the operator says so, so the
//! defaults are `mode = "none"`, `trusted = []`.

use std::collections::BTreeSet;
use std::path::Path;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use crate::duration::ConfigDuration;

/// Default config file location (design §8.2).
pub const DEFAULT_CONFIG_PATH: &str = "/etc/farsight/config.toml";

/// Environment variable prefix for overrides.
pub const ENV_PREFIX: &str = "FARSIGHT__";

/// `FARSIGHT_SKIP_WIZARD`: build config from the environment when no file
/// exists, never entering setup mode.
pub const SKIP_WIZARD_ENV: &str = "FARSIGHT_SKIP_WIZARD";

/// Keys with no usable default. `access.admin_did` is not among them: a
/// value used only at sign-in never stops the process (design §16).
pub const REQUIRED_KEYS: [&str; 4] = [
    "server.hostname",
    "server.contact",
    "storage.database_url",
    "auth.admin_token_sha256",
];

/// Configuration errors. Any of these makes the process exit non-zero
/// (design §8.2: an invalid config never enters setup mode and is never
/// rewritten).
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The file could not be read.
    #[error("reading {path}: {source}")]
    Io {
        /// The path.
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
        /// The variable.
        var: String,
        /// Its value.
        value: String,
        /// The expected type.
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
        /// Why.
        reason: String,
    },
}

fn invalid(key: &str, reason: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        key: key.to_owned(),
        reason: reason.into(),
    }
}

/// The complete configuration (design §16).
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
    /// Listen address.
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
    /// Storage budget in bytes (`pg_database_size`), §11.2.
    pub budget_bytes: u64,
    /// Hard ceiling in bytes; 0 means 115% of `budget_bytes`. Must exceed
    /// the budget after defaulting.
    pub hard_ceiling_bytes: u64,
    /// Tombstone TTL (§7.3).
    pub tombstone_ttl: ConfigDuration,
    /// Record removed blocks, listblocks and list memberships (§7.7, §7.8).
    pub block_history_enabled: bool,
    /// How long history rows are kept; `"0s"` keeps them forever (§7.7).
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
            urls: vec!["wss://jetstream2.us-east.bsky.network".to_owned()],
            tuning: FirehoseTuning::default(),
        }
    }
}

/// `[firehose.tuning]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FirehoseTuning {
    /// v1 heuristic gap threshold (§6.3).
    pub gap_threshold: ConfigDuration,
    /// Minimum failover rewind (§6.3).
    pub failover_rewind_min: ConfigDuration,
    /// Maximum instance lag for a gapless failover (§6.3).
    pub failover_max_lag: ConfigDuration,
    /// Lag beyond which the synthetic gap exists (§3.7.1).
    pub synthetic_gap_lag: ConfigDuration,
    /// No-message stall timeout (§6.3).
    pub stall_timeout: ConfigDuration,
    /// Seam repair window start, before the session's connect (§6.3).
    pub seam_repair_before: ConfigDuration,
    /// Seam repair window end, after the session caught up (§6.3).
    pub seam_repair_after: ConfigDuration,
    /// Delay between catching up and the seam repair (§6.3).
    pub seam_repair_delay: ConfigDuration,
    /// A session has caught up once an event's witness time is within this
    /// much of wall time (§6.3).
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
    /// Worker pool size (§5.3).
    pub concurrency: u32,
    /// Per-host request rate.
    pub per_host_rps: u32,
    /// PLC directory URL.
    pub plc_url: String,
    /// PLC request rate.
    pub plc_rps: u32,
    /// Seed PDS resolution from the PLC export (§5.4).
    pub plc_seed_from_export: bool,
    /// Relay URL (verify at release, §18).
    pub relay_url: String,
    /// `requestBackfill` freshness window (§3.3).
    pub request_fresh_window: ConfigDuration,
    /// Minimum interval between list fetch runs per owner (§5.5).
    pub owner_fetch_cooldown: ConfigDuration,
    /// Repo job retry backoff; the last step repeats (§5.2).
    pub retry_schedule: Vec<ConfigDuration>,
    /// Failing for this long makes a repo job terminal (§5.2).
    pub terminal_after: ConfigDuration,
    /// `missing` list re-check schedule (§5.5).
    pub missing_retry: Vec<ConfigDuration>,
    /// Failed fetch attempts before FT (§5.5).
    pub list_fetch_max_attempts: u32,
    /// Phase-1 error retry schedule (§5.5).
    pub phase1_retry: Vec<ConfigDuration>,
    /// Wall-clock cap on one list fetch run (§5.5).
    pub list_fetch_max_duration: ConfigDuration,
    /// Repair candidate slack (§7.5).
    pub repair_slack: ConfigDuration,
    /// In-memory seen-set cap for out-of-order listings (§5.2).
    pub seen_set_cap: u64,
    /// Queue entries per system requester (§5.3).
    pub system_queue_cap: u64,
    /// Concurrent requests per host (§5.3).
    pub per_host_concurrency: u32,
    /// Guaranteed shares of tiers 1, 2, 3 in percent (§5.3).
    pub tier_shares: Vec<u32>,
    /// `[backfill.sweep]`.
    pub sweep: SweepConfig,
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
            repair_slack: ConfigDuration::hours(1),
            seen_set_cap: 2_000_000,
            system_queue_cap: 50_000,
            per_host_concurrency: 4,
            tier_shares: vec![60, 25, 15],
            sweep: SweepConfig::default(),
            backlinks: BacklinksConfig::default(),
        }
    }
}

/// Sweep enumeration source (§5.4).
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

/// `[backfill.sweep]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SweepConfig {
    /// Whether the systematic sweep runs.
    pub enabled: bool,
    /// Enumeration source.
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
    /// Backlink index URL; empty = discovery disabled (§5.6).
    pub url: String,
    /// Reference cap across discovery steps.
    pub max_refs: u64,
    /// Allowance for backlink-index lag (§3.7.1).
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

/// `access.reads` (§3.5).
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

/// The retired `access.ui` key.
pub const RETIRED_UI: &str = "access.ui";
/// The one value of the retired `access.ui` that still means something:
/// without `access.admin_ui`, it switches the admin UI off.
pub const RETIRED_UI_DISABLED: &str = "disabled";

/// `[access]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccessConfig {
    /// Read access mode.
    pub reads: ReadsMode,
    /// Retired (replaced by `admin_ui`): accepted with any value, warned
    /// about, never written. Read only by the loader, which takes
    /// `"disabled"` for `admin_ui = false` when `admin_ui` is absent.
    #[serde(skip_serializing)]
    pub ui: Option<String>,
    /// Send `Access-Control-Allow-Origin: *` on reads.
    pub cors: bool,
    /// Serve the public UI at the root (§8.6). Requires `reads =
    /// "public"`; independent of `admin_ui`.
    pub public_ui: bool,
    /// Serve the admin UI under `/admin`, with its sign-in at `/enter`
    /// (§8.6). Applied at start only: no in-process edit may change it.
    pub admin_ui: bool,
    /// The DID of the one account that may sign in to the admin UI
    /// (§8.6). Empty = not set (see [`AdminAuth`]).
    pub admin_did: String,
}

impl Default for AccessConfig {
    fn default() -> Self {
        AccessConfig {
            reads: ReadsMode::Public,
            ui: None,
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
    /// The config value.
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
/// Longest `public_ui.instance_description`, in characters.
pub const MAX_INSTANCE_DESCRIPTION: usize = 2_000;
/// Longest `public_ui.contact`, in characters.
pub const MAX_PUBLIC_CONTACT: usize = 200;

/// `[public_ui]` (§16): what the public UI shows. Every key applies
/// without a restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PublicUiConfig {
    /// Plain text shown on `/public`; empty = the default text.
    pub instance_description: String,
    /// Contact shown on public pages; empty = `server.contact`.
    pub contact: String,
    /// The DID page shows the subject's own blocks.
    pub show_outgoing_blocks: bool,
    /// Retired (§16): history pages are admin-only. Accepted so that a
    /// file written by an earlier version still loads; the value has no
    /// effect, a warning is logged, and the key is never written back.
    #[serde(skip_serializing)]
    pub show_history: Option<bool>,
    /// Emit the `og:image` tag for the static card.
    pub show_opengraph_image: bool,
    /// Theme a visitor gets before choosing one.
    pub dark_mode_default: ThemeDefault,
    /// Let crawlers index the public pages.
    pub crawlable: bool,
    /// Public page views per second per client address.
    pub rate_limit_rps: u32,
    /// Burst of the same class.
    pub rate_limit_burst: u32,
    /// Concurrent public page renders; at most
    /// `rate_limit.query_concurrency`.
    pub query_concurrency: u32,
    /// How long a verified handle is cached.
    pub handle_cache_ttl: ConfigDuration,
    /// DIDs the public pages withhold; at most [`MAX_EXCLUDED_DIDS`].
    pub excluded_dids: Vec<String>,
    /// URL template of a record viewer; empty = records are not links.
    /// Placeholders: `{authority}`, `{collection}`, `{rkey}`.
    pub record_viewer_url: String,
    /// Profile cards carry the account's avatar, which the visitor's
    /// browser fetches from the account's own server.
    pub show_avatars: bool,
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
}

impl PublicUiConfig {
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
            show_history: None,
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
            card_rps: 4,
            card_burst: 8,
            handle_warming_enabled: true,
        }
    }
}

/// The retired `public_ui.show_history` key.
pub const RETIRED_SHOW_HISTORY: &str = "public_ui.show_history";
/// What the loader says when the retired key is present.
pub const RETIRED_SHOW_HISTORY_WARNING: &str =
    "`public_ui.show_history` no longer has any effect; history pages are admin-only.";

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
    /// bcrypt hash of the pre-OAuth admin password. Retired: read only
    /// by the migration page (see [`AdminAuth::Migration`]); otherwise
    /// accepted and ignored.
    pub admin_password_bcrypt: String,
}

/// Warning logged when `auth.admin_password_bcrypt` is present and unused.
pub const RETIRED_PASSWORD_WARNING: &str = "auth.admin_password_bcrypt is set and ignored: admin sign-in uses access.admin_did; \
     remove the key";
/// Warning logged in the migration state.
pub const MIGRATION_WARNING: &str = "admin sign-in needs migration: access.admin_did is not set; open /enter and set it \
     with the existing admin password";
/// Warning logged in the unconfigured state.
pub const UNCONFIGURED_WARNING: &str = "admin sign-in is not configured: set access.admin_did (farsight set-admin-did, or \
     FARSIGHT__ACCESS__ADMIN_DID) and restart";

/// How (and whether) anyone can sign in to the admin UI (design §8.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminAuth {
    /// `access.admin_ui = false`: no admin UI.
    Disabled,
    /// `access.admin_did` is set: OAuth sign-in as that DID.
    Configured(String),
    /// A file config from before OAuth sign-in: a password hash and no
    /// admin DID. `/enter` serves the migration page.
    Migration,
    /// No admin DID and no migration path: nobody can sign in.
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

/// `proxy.mode` (§9.1).
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
    /// Proxy mode.
    pub mode: ProxyMode,
    /// Trusted proxy CIDRs (§9.2 validation applies).
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

/// `[limits]` (§4, §11).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    /// Grace for `retained` lists (§4.4 GE).
    pub list_grace: ConfigDuration,
    /// Owner-caused re-admissions per UTC day (§4.4).
    pub owner_readmissions_per_day: u32,
    /// Wall-clock bound on `pending` (§3.7.4).
    pub pending_max_age: ConfigDuration,
    /// Pending lists taking effect per owner key (§3.7.4).
    pub pending_effects_per_owner_key: u32,
    /// `unresolved` bucket block cap (§11.2).
    pub unresolved_blocks: u64,
    /// `unresolved` bucket list-item cap.
    pub unresolved_list_items: u64,
    /// Daily admissions per bucket key (§11.1).
    pub bucket_admissions_per_day: u64,
    /// Daily admissions per DID key (§11.1).
    pub did_admissions_per_day: u64,
    /// Extra shared CDN/anycast ranges excluded as address buckets (§11.2).
    pub cdn_ranges_extra: Vec<IpNet>,
    /// Items per list (§4.7).
    pub list_items_per_list: u64,
    /// Items per owner (§4.7).
    pub list_items_per_owner: u64,
    /// Stored blocks per author (§11.1).
    pub blocks_per_author: u64,
    /// Counted listblocks per author (trigger cap, §4.2).
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
    /// Daily interning per DID or requester cause key (§11.2).
    pub intern_per_did_per_day: u64,
    /// Daily interning per bucket cause key (§11.2).
    pub intern_per_bucket_per_day: u64,
    /// Lifetime interning per non-large bucket (§11.2).
    pub host_interned_lifetime: u64,
    /// `unresolved` bucket list cap.
    pub unresolved_lists: u64,
    /// Daily history rows per DID admission key, all three history tables
    /// together (§7.7).
    pub history_per_did_per_day: u64,
    /// Daily history rows per bucket admission key (§7.7).
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

impl LimitsConfig {
    /// Whether `host` matches `large_hosts` (exact, or `*.suffix` matching
    /// any subdomain of `suffix`).
    pub fn is_large_host(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        self.large_hosts.iter().any(|pat| {
            let pat = pat.to_ascii_lowercase();
            match pat.strip_prefix("*.") {
                Some(suffix) => {
                    host.len() > suffix.len() + 1 && host.ends_with(&format!(".{suffix}"))
                }
                None => host == pat,
            }
        })
    }
}

/// `[net]`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct NetConfig {
    /// Hosts the safe client may reach over plain `http` (development).
    pub allow_http_hosts: Vec<String>,
}

/// `[rate_limit]` (§3.6).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Anonymous read rate.
    pub anon_rps: u32,
    /// Anonymous read burst.
    pub anon_burst: u32,
    /// API-key read rate.
    pub key_rps: u32,
    /// API-key read burst.
    pub key_burst: u32,
    /// Admin `requestBackfill` rate.
    pub admin_backfill_rps: u32,
    /// API-key `requestBackfill` rate.
    pub key_backfill_rps: u32,
    /// UI lookup rate (anonymous).
    pub ui_lookup_rps: u32,
    /// Concurrent read queries.
    pub query_concurrency: u32,
    /// Read query `statement_timeout`.
    pub query_timeout: ConfigDuration,
    /// Concurrent bcrypt verifications.
    pub bcrypt_concurrency: u32,
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
            bcrypt_concurrency: 2,
        }
    }
}

/// `[metrics]` (§13).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    /// Server metrics listener.
    pub bind: String,
    /// Backfill metrics listener.
    pub backfill_bind: String,
    /// Optional bearer token hash; empty = no auth.
    pub bearer_token_sha256: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        MetricsConfig {
            bind: "0.0.0.0:9464".to_owned(),
            backfill_bind: "0.0.0.0:9465".to_owned(),
            bearer_token_sha256: String::new(),
        }
    }
}

/// How the process should start (design §8.2).
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
    /// The config.
    pub config: Config,
    /// Dotted keys set from the environment (locked in the settings UI).
    pub env_keys: Vec<String>,
    /// Non-fatal findings (e.g. public proxy ranges; §9.2 "acknowledged
    /// warning").
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
        } else if !self.from_env_only && !c.auth.admin_password_bcrypt.trim().is_empty() {
            AdminAuth::Migration
        } else {
            AdminAuth::Unconfigured
        }
    }
}

/// Where a config is coming from; determines which checks apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    /// `config.toml` (plus env overrides).
    File,
    /// `FARSIGHT_SKIP_WIZARD=1` with no file.
    EnvOnly,
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

/// Builds and validates a config from optional file contents and the
/// environment. `None` means env-only (`FARSIGHT_SKIP_WIZARD`).
pub fn load_from_parts(
    file: Option<&str>,
    env: &[(String, String)],
) -> Result<LoadedConfig, ConfigError> {
    let mut table: toml::Table = match file {
        Some(text) => text
            .parse::<toml::Table>()
            .map_err(|e| ConfigError::Toml(e.to_string()))?,
        None => toml::Table::new(),
    };
    let env_keys = apply_env_overrides(&mut table, env)?;
    let ui_warning = resolve_admin_ui(&mut table);
    let config: Config = toml::Value::Table(table)
        .try_into()
        .map_err(|e: toml::de::Error| ConfigError::Schema(e.to_string()))?;
    let source = if file.is_some() {
        ConfigSource::File
    } else {
        ConfigSource::EnvOnly
    };
    let mut warnings = config.validate()?;
    warnings.extend(ui_warning);
    let mut loaded = LoadedConfig {
        config,
        env_keys,
        warnings,
        from_env_only: source == ConfigSource::EnvOnly,
    };
    let has_password = !loaded.config.auth.admin_password_bcrypt.trim().is_empty();
    match loaded.admin_auth() {
        AdminAuth::Migration => loaded.warnings.push(MIGRATION_WARNING.to_owned()),
        AdminAuth::Unconfigured => loaded.warnings.push(UNCONFIGURED_WARNING.to_owned()),
        AdminAuth::Configured(_) | AdminAuth::Disabled if has_password => {
            loaded.warnings.push(RETIRED_PASSWORD_WARNING.to_owned());
        }
        _ => {}
    }
    Ok(loaded)
}

/// Decides `access.admin_ui` on the merged table (file, then environment)
/// and says what the retired `access.ui` came to (§16):
///
/// 1. `admin_ui` present: its value; `ui` is ignored.
/// 2. else `ui = "disabled"`: off.
/// 3. else: on (the default).
///
/// Run before the table is deserialized, which is the only point where
/// "absent" and "default" can be told apart.
fn resolve_admin_ui(table: &mut toml::Table) -> Option<String> {
    let access = table.get_mut("access")?.as_table_mut()?;
    let ui = access.get("ui")?.as_str()?.to_owned();
    if access.contains_key("admin_ui") {
        return Some(
            "`access.ui` is retired and ignored; `access.admin_ui` is in force. Remove \
             `access.ui`."
                .to_owned(),
        );
    }
    Some(match ui.as_str() {
        RETIRED_UI_DISABLED => {
            access.insert("admin_ui".to_owned(), toml::Value::Boolean(false));
            "`access.ui` is retired; \"disabled\" is read as `access.admin_ui = false`. Replace \
             it with that."
                .to_owned()
        }
        "public_read" => "`access.ui` is retired. The admin UI is on (`access.admin_ui`) and \
                          every admin page now needs sign-in: the dashboard and lookups are no \
                          longer public. Remove `access.ui`."
            .to_owned(),
        "auth_all" => "`access.ui` is retired. The admin UI is on (`access.admin_ui`); without \
                       a session its pages redirect to /enter instead of answering 404. Remove \
                       `access.ui`."
            .to_owned(),
        other => format!(
            "`access.ui` is retired and \"{other}\" was never one of its values; it is \
             ignored. The admin UI is on (`access.admin_ui`). Remove `access.ui`."
        ),
    })
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
        if path.join(".") == RETIRED_SHOW_HISTORY {
            // Retired: still typed and accepted, never locked, so that a
            // Settings save may drop the key from the file.
            let value = coerce(var, raw, &toml::Value::Boolean(false))?;
            table
                .entry("public_ui")
                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                .as_table_mut()
                .ok_or_else(|| ConfigError::Invalid {
                    key: RETIRED_SHOW_HISTORY.to_owned(),
                    reason: "`public_ui` in config.toml is not a table".to_owned(),
                })?
                .insert("show_history".to_owned(), value);
            continue;
        }
        if path.join(".") == RETIRED_UI {
            // Retired, like `show_history`: env-managed instances set it
            // (`FARSIGHT__ACCESS__UI=disabled`) and must keep loading. Any
            // string; never locked.
            table
                .entry("access")
                .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                .as_table_mut()
                .ok_or_else(|| ConfigError::Invalid {
                    key: RETIRED_UI.to_owned(),
                    reason: "`access` in config.toml is not a table".to_owned(),
                })?
                .insert("ui".to_owned(), toml::Value::String(raw.clone()));
            continue;
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

/// Validates a proxy CIDR (design §9.2): refuses `0.0.0.0/0`, `::/0`, and
/// prefixes shorter than /8 (IPv4) or /24 (IPv6).
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

fn is_public_net(net: &IpNet) -> bool {
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
        if self.backfill.concurrency == 0 {
            return Err(invalid("backfill.concurrency", "must be positive"));
        }
        self.validate_public_ui()?;
        let mut warnings = Vec::new();
        if self.public_ui.show_history.is_some() {
            warnings.push(RETIRED_SHOW_HISTORY_WARNING.to_owned());
        }
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
        Ok(warnings)
    }
}

impl Config {
    /// The public UI rules (§3.5, §16): the access combination, and the
    /// bounds of `[public_ui]`. The same check runs at load and at every
    /// settings save.
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
        if !p.record_viewer_url.is_empty() {
            validate_record_viewer_url(&p.record_viewer_url)
                .map_err(|r| invalid("public_ui.record_viewer_url", r))?;
        }
        // Checked only while the public UI is on: a config written before
        // `[public_ui]` existed, with a lowered `rate_limit.query_concurrency`,
        // must keep loading with the defaults of a feature it does not use.
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
/// `access.admin_did` and an unset (retired) `auth.admin_password_bcrypt`
/// are left out rather than written empty.
pub fn to_toml(config: &Config) -> Result<String, ConfigError> {
    let mut table =
        toml::Table::try_from(config).map_err(|e| ConfigError::Schema(e.to_string()))?;
    for (section, key) in [("access", "admin_did"), ("auth", "admin_password_bcrypt")] {
        if let Some(t) = table.get_mut(section).and_then(toml::Value::as_table_mut) {
            if t.get(key).and_then(toml::Value::as_str) == Some("") {
                t.remove(key);
            }
        }
    }
    toml::to_string_pretty(&table).map_err(|e| ConfigError::Schema(e.to_string()))
}

/// Where the pre-OAuth copy of `config.toml` is kept for a rollback.
pub fn pre_oauth_backup_path(path: &Path) -> std::path::PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".pre-oauth");
    path.with_file_name(name)
}

/// What [`set_admin_did`] did to a config file's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminDidEdit {
    /// The new file text.
    pub text: String,
    /// The file held a password hash and no admin DID (the migration
    /// state): the caller writes the backup before the new text.
    pub was_migration: bool,
    /// The retired password key was removed.
    pub removed_password: bool,
}

/// Sets `access.admin_did` in a `config.toml` text and removes the retired
/// `auth.admin_password_bcrypt` unless `keep_password` (the key is locked
/// by the environment). The file need not load before the edit.
pub fn set_admin_did(text: &str, did: &str, keep_password: bool) -> Result<AdminDidEdit, String> {
    if !valid_admin_did(did) {
        return Err(format!("{did} is not a did:plc or did:web DID"));
    }
    let mut table: toml::Table = text.parse().map_err(|e: toml::de::Error| e.to_string())?;
    let had_did = table
        .get("access")
        .and_then(|a| a.get("admin_did"))
        .and_then(toml::Value::as_str)
        .is_some_and(|d| !d.is_empty());
    let has_password = table
        .get("auth")
        .and_then(|a| a.get("admin_password_bcrypt"))
        .and_then(toml::Value::as_str)
        .is_some_and(|p| !p.trim().is_empty());
    table
        .entry("access")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .ok_or("`access` is not a table")?
        .insert("admin_did".to_owned(), toml::Value::String(did.to_owned()));
    let mut removed_password = false;
    if !keep_password {
        if let Some(auth) = table.get_mut("auth").and_then(toml::Value::as_table_mut) {
            removed_password = auth.remove("admin_password_bcrypt").is_some();
        }
    }
    Ok(AdminDidEdit {
        text: toml::to_string_pretty(&table).map_err(|e| e.to_string())?,
        was_migration: !had_did && has_password,
        removed_password,
    })
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

/// Writes `text` to `path` only if `path` does not exist yet (design
/// §8.3 "first writer wins"): the content goes to a temp file (0600) that
/// is then hard-linked into place, which fails if another writer got
/// there first. Returns `Ok(false)` in that case.
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
/// settings edits, §8.6).
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
        assert!(p.show_history.is_none() && p.record_viewer_url.is_empty());
        assert_eq!((p.card_rps, p.card_burst), (4, 8));
        assert!(p.handle_warming_enabled);
        assert!(!p.crawlable && p.excluded_dids.is_empty());
        assert_eq!(p.dark_mode_default, ThemeDefault::System);
        assert_eq!((p.rate_limit_rps, p.rate_limit_burst), (5, 20));
        assert_eq!(p.query_concurrency, 8);
        assert_eq!(p.handle_cache_ttl, ConfigDuration::hours(1));
        assert_eq!(c.firehose.urls, ["wss://jetstream2.us-east.bsky.network"]);
        assert_eq!(c.backfill.tier_shares, [60, 25, 15]);
        assert_eq!(c.backfill.retry_schedule.len(), 4);
        assert_eq!(c.backfill.sweep.source, SweepSource::RelayCollections);
        assert_eq!(c.backfill.backlinks.max_refs, 200_000);
        assert_eq!(c.limits.listblock_fetch_triggers_per_author, 5_000);
        assert_eq!(c.limits.did_admissions_per_day, 200);
        assert_eq!(c.limits.large_hosts, ["*.host.bsky.network"]);
        assert_eq!(c.rate_limit.query_timeout, ConfigDuration::secs(5));
        assert_eq!(c.metrics.backfill_bind, "0.0.0.0:9465");
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
        // A range inside the bundled Cloudflare set is not warned about
        // (§9.2); other public space is a warning, not an error.
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
                ("FARSIGHT__ACCESS__UI", "disabled"),
            ]),
        )
        .unwrap();
        assert!(ok.from_env_only);
        assert!(!ok.config.access.admin_ui);
        assert_eq!(ok.admin_auth(), AdminAuth::Disabled);
        assert!(!ok.env_keys.iter().any(|k| k == RETIRED_UI), "never locked");
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
        // Not a load failure while the public UI is off: an older config
        // with a lowered global bound keeps loading.
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
    fn retired_show_history_loads_with_a_warning() {
        // A file as the previous version's Settings page wrote it.
        let mut c = complete();
        c.public_ui.show_history = None;
        let mut text = to_toml(&c).unwrap();
        assert!(!text.contains("show_history"), "never written back");
        text = text.replace("[public_ui]\n", "[public_ui]\nshow_history = true\n");
        assert!(text.contains("show_history = true"));
        let l = load_from_parts(Some(&text), &[]).unwrap();
        assert_eq!(l.config.public_ui.show_history, Some(true));
        assert_eq!(l.warnings, [RETIRED_SHOW_HISTORY_WARNING]);
        // Without the key there is no warning.
        let l = load_from_parts(Some(&to_toml(&c).unwrap()), &[]).unwrap();
        assert!(l.warnings.is_empty(), "{:?}", l.warnings);
        // The environment form is accepted, typed and not locked.
        let env = vec![(
            "FARSIGHT__PUBLIC_UI__SHOW_HISTORY".to_owned(),
            "false".to_owned(),
        )];
        let l = load_from_parts(Some(&to_toml(&c).unwrap()), &env).unwrap();
        assert_eq!(l.config.public_ui.show_history, Some(false));
        assert!(l.env_keys.is_empty());
        assert_eq!(l.warnings, [RETIRED_SHOW_HISTORY_WARNING]);
        let env = vec![(
            "FARSIGHT__PUBLIC_UI__SHOW_HISTORY".to_owned(),
            "perhaps".to_owned(),
        )];
        assert!(load_from_parts(Some(&to_toml(&c).unwrap()), &env).is_err());
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
        // A file from before OAuth sign-in: migration, and it loads.
        let mut old = complete();
        old.access.admin_did.clear();
        old.auth.admin_password_bcrypt = "$2b$12$abcdefghijklmnopqrstuv".into();
        let l = file(&old, &[]);
        assert_eq!(l.admin_auth(), AdminAuth::Migration);
        assert_eq!(l.warnings, [MIGRATION_WARNING]);
        // The DID from the environment configures it; the password is
        // then ignored and warned about.
        let l = file(&old, &[("FARSIGHT__ACCESS__ADMIN_DID", ADMIN_DID)]);
        assert_eq!(l.admin_auth(), AdminAuth::Configured(ADMIN_DID.into()));
        assert_eq!(l.warnings, [RETIRED_PASSWORD_WARNING]);
        // Neither key: unconfigured, and it loads.
        let mut none = complete();
        none.access.admin_did.clear();
        let l = file(&none, &[]);
        assert_eq!(l.admin_auth(), AdminAuth::Unconfigured);
        assert_eq!(l.warnings, [UNCONFIGURED_WARNING]);
        // Admin UI off: neither key matters.
        none.access.admin_ui = false;
        let l = file(&none, &[]);
        assert_eq!(l.admin_auth(), AdminAuth::Disabled);
        assert!(l.warnings.is_empty());
        // Env-only with a password hash and no DID: unconfigured, never
        // migration (there is no file to write).
        let l = load_from_parts(
            None,
            &env(&[
                ("FARSIGHT__SERVER__HOSTNAME", "h.test"),
                ("FARSIGHT__SERVER__CONTACT", "mailto:x@h.test"),
                ("FARSIGHT__STORAGE__DATABASE_URL", "postgres://x"),
                ("FARSIGHT__AUTH__ADMIN_TOKEN_SHA256", TOKEN_HASH),
                ("FARSIGHT__AUTH__ADMIN_PASSWORD_BCRYPT", "$2b$12$x"),
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

    /// The retired `access.ui` (§16): any value loads and is warned
    /// about; only `"disabled"` without `admin_ui` still decides anything;
    /// the key is never written.
    #[test]
    fn retired_ui_key() {
        let base = to_toml(&complete()).unwrap();
        assert!(base.contains("admin_ui = true") && !base.contains("\nui ="));
        let with = |access: &str, env_pairs: &[(&str, &str)]| {
            let text = base
                .replace("admin_ui = true\n", "")
                .replace("[access]\n", &format!("[access]\n{access}"));
            load_from_parts(Some(&text), &env(env_pairs)).unwrap()
        };
        let warned = |l: &LoadedConfig| {
            l.warnings
                .iter()
                .filter(|w| w.contains("access.ui"))
                .count()
        };
        // Neither key: on, no warning.
        let l = with("", &[]);
        assert!(l.config.access.admin_ui && warned(&l) == 0);
        // The three old values, and one that never existed.
        for (ui, on) in [
            ("public_read", true),
            ("auth_all", true),
            ("disabled", false),
            ("garbage", true),
        ] {
            let l = with(&format!("ui = \"{ui}\"\n"), &[]);
            assert_eq!(l.config.access.admin_ui, on, "{ui}");
            assert_eq!(warned(&l), 1, "{ui}: {:?}", l.warnings);
            assert_eq!(l.config.access.ui.as_deref(), Some(ui));
            assert!(
                !to_toml(&l.config).unwrap().contains("\nui ="),
                "never written"
            );
        }
        // `admin_ui` wins over `ui`, from the file or the environment,
        // and over an environment `ui` too.
        let l = with("ui = \"disabled\"\nadmin_ui = true\n", &[]);
        assert!(l.config.access.admin_ui && warned(&l) == 1);
        let l = with(
            "ui = \"disabled\"\n",
            &[("FARSIGHT__ACCESS__ADMIN_UI", "true")],
        );
        assert!(l.config.access.admin_ui);
        assert!(l.env_keys.iter().any(|k| k == "access.admin_ui"));
        let l = with("admin_ui = true\n", &[("FARSIGHT__ACCESS__UI", "disabled")]);
        assert!(l.config.access.admin_ui && warned(&l) == 1);
        // The environment's `ui` beats the file's, as for every key.
        let l = with(
            "ui = \"public_read\"\n",
            &[("FARSIGHT__ACCESS__UI", "disabled")],
        );
        assert!(!l.config.access.admin_ui);
        assert_eq!(l.admin_auth(), AdminAuth::Disabled);
    }

    #[test]
    fn unset_admin_keys_are_not_written() {
        let mut c = complete();
        let text = to_toml(&c).unwrap();
        assert!(text.contains("admin_did = \"did:plc:"));
        assert!(!text.contains("admin_password_bcrypt"));
        c.access.admin_did.clear();
        c.access.admin_ui = false;
        assert!(!to_toml(&c).unwrap().contains("admin_did"));
    }

    #[test]
    fn set_admin_did_edits_the_file_text() {
        let old = "[access]\nui = \"auth_all\"\n\n[auth]\nadmin_token_sha256 = \"x\"\nadmin_password_bcrypt = \"$2b$12$x\"\n";
        let e = set_admin_did(old, ADMIN_DID, false).unwrap();
        assert!(e.was_migration && e.removed_password);
        assert!(e.text.contains(&format!("admin_did = \"{ADMIN_DID}\"")));
        assert!(!e.text.contains("admin_password_bcrypt"));
        assert!(e.text.contains("admin_token_sha256") && e.text.contains("auth_all"));
        // The password stays when the environment locks it.
        let e = set_admin_did(old, ADMIN_DID, true).unwrap();
        assert!(e.was_migration && !e.removed_password);
        assert!(e.text.contains("admin_password_bcrypt"));
        // Changing an existing DID is not a migration.
        let e2 = set_admin_did(&e.text, "did:web:alice.example", false).unwrap();
        assert!(!e2.was_migration);
        assert!(e2.text.contains("did:web:alice.example") && !e2.text.contains(ADMIN_DID));
        // A file with no [access] table gets one.
        assert!(
            set_admin_did("", ADMIN_DID, false)
                .unwrap()
                .text
                .contains("[access]")
        );
        assert!(set_admin_did(old, "alice.example", false).is_err());
        assert_eq!(
            pre_oauth_backup_path(Path::new("/etc/farsight/config.toml")),
            Path::new("/etc/farsight/config.toml.pre-oauth")
        );
    }
}
