//! The live configuration and its edits (the admin Settings page; see
//! `docs/design/web-ui.md` and `docs/design/operations.md`).
//!
//! Handlers read [`ConfigStore::current`] per request, so hot-reloadable
//! keys apply as soon as an edit is stored. Edits rewrite `config.toml`
//! atomically, are validated with the same loader as start-up, refuse keys
//! set from the environment (locked), and report which changed keys need a
//! restart. The caller sends `NOTIFY farsight_config` afterwards.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use farsight_core::config::{self, LoadedConfig};

/// Channel name for config-change notifications.
pub const CONFIG_CHANNEL: &str = "farsight_config";

/// Keys (or key prefixes ending in `.`) that apply without a restart:
/// everything the API and UI read per request, `[public_ui]` included.
/// Anything else — bind addresses, the database, firehose and storage
/// settings (`block_history_enabled` among them), `[limits]` used by
/// the running ingest — needs a restart of `farsight`.
/// `farsight-backfill` reloads `[backfill]` itself on `NOTIFY
/// farsight_config`.
pub const HOT_KEYS: &[&str] = &[
    "access.",
    "public_ui.",
    "auth.",
    "proxy.",
    "server.contact",
    "backfill.",
    "rate_limit.anon_rps",
    "rate_limit.anon_burst",
    "rate_limit.key_rps",
    "rate_limit.key_burst",
    "rate_limit.admin_backfill_rps",
    "rate_limit.key_backfill_rps",
    "rate_limit.ui_lookup_rps",
    "rate_limit.query_timeout",
];

/// Whether a dotted key applies without a restart.
pub fn is_hot(key: &str) -> bool {
    HOT_KEYS.iter().any(|h| {
        if let Some(prefix) = h.strip_suffix('.') {
            key == prefix || key.starts_with(h)
        } else {
            key == *h
        }
    })
}

/// The key of the admin token's hash. A stored edit that changes it ends
/// every admin session.
pub const ADMIN_TOKEN_KEY: &str = "auth.admin_token_sha256";

fn reads_rank(m: config::ReadsMode) -> u8 {
    match m {
        config::ReadsMode::Disabled => 0,
        config::ReadsMode::ApiKey => 1,
        config::ReadsMode::Public => 2,
    }
}

/// The public-UI switches that show more, or to more readers, when on.
const PUBLIC_UI_WIDENING: [&str; 6] = [
    "public_ui.show_outgoing_blocks",
    "public_ui.show_top_blockers",
    "public_ui.show_top_blocked",
    "public_ui.crawlable",
    "public_ui.show_avatars",
    "public_ui.avatar_thumbnails",
];

/// The dotted keys by which `new` differs from `old` in a way the admin
/// UI treats as sensitive (it asks for a fresh sign-in before storing
/// them):
///
/// - every `auth.*`, `net.*`, `proxy.*` and `metrics.*` key;
/// - `storage.database_url`, `server.hostname`, `firehose.urls`,
///   `backfill.plc_url`, `backfill.relay_url`, `backfill.backlinks.url`
///   and `public_ui.record_viewer_url`;
/// - an `access.*` key that lets more callers in (`reads` towards
///   `public`, `cors` or `public_ui` switched on);
/// - a public-UI switch that shows more ([`PUBLIC_UI_WIDENING`]) turned
///   on, and an account taken off `public_ui.excluded_dids`.
///
/// With these a session could outlive its own end: a token it knows, a
/// database, directory, relay, firehose or backlink source it runs, a
/// proxy header it can forge, a host the outbound client may newly
/// reach, links on public pages to a site of its choosing, or data
/// served to people who could not read it before.
pub fn sensitive_changes(old: &config::Config, new: &config::Config) -> Vec<String> {
    let flat = |c: &config::Config| {
        toml::Value::try_from(c)
            .map(|v| flatten(&v))
            .unwrap_or_default()
    };
    let (o, n) = (flat(old), flat(new));
    let mut out: BTreeSet<String> = o
        .keys()
        .chain(n.keys())
        .filter(|k| o.get(*k) != n.get(*k))
        .filter(|k| {
            ["auth.", "net.", "proxy.", "metrics."]
                .iter()
                .any(|p| k.starts_with(p))
                || matches!(
                    k.as_str(),
                    "storage.database_url"
                        | "server.hostname"
                        | "firehose.urls"
                        | "backfill.plc_url"
                        | "backfill.relay_url"
                        | "backfill.backlinks.url"
                        | "public_ui.record_viewer_url"
                )
        })
        .cloned()
        .collect();
    for key in PUBLIC_UI_WIDENING {
        let on = |m: &BTreeMap<String, String>| m.get(key).is_some_and(|v| v == "true");
        if on(&n) && !on(&o) {
            out.insert(key.to_owned());
        }
    }
    if old
        .public_ui
        .excluded_dids
        .iter()
        .any(|d| !new.public_ui.excluded_dids.contains(d))
    {
        out.insert("public_ui.excluded_dids".into());
    }
    if reads_rank(new.access.reads) > reads_rank(old.access.reads) {
        out.insert("access.reads".into());
    }
    if new.access.cors && !old.access.cors {
        out.insert("access.cors".into());
    }
    if new.access.public_ui && !old.access.public_ui {
        out.insert("access.public_ui".into());
    }
    out.into_iter().collect()
}

/// Refusal: a settings save tried to change `access.admin_did`.
pub const ADMIN_DID_READ_ONLY: &str =
    "the admin DID cannot be changed here; use `farsight set-admin-did` and restart farsight";

/// Refusal: an edit whose result has another `access.admin_ui` than the
/// config in force. The key is applied at start only: an edit made in the
/// admin UI would remove the page it was made from, and a change waiting
/// in the file must not go live as a side effect of another edit.
pub const ADMIN_UI_NEEDS_RESTART: &str = "`access.admin_ui` requires a restart to change: set it in config.toml and restart \
     farsight. If config.toml was already edited by hand, restart to apply that, or undo it; \
     until then no setting can be saved from the running server";

/// Why an edit was refused.
#[derive(Debug, thiserror::Error)]
pub enum EditError {
    /// Config built from the environment (`FARSIGHT_SKIP_WIZARD`): managed
    /// externally.
    #[error("the configuration is managed through the environment (FARSIGHT_SKIP_WIZARD)")]
    ManagedExternally,
    /// The edit touches keys set from the environment.
    #[error("locked by environment variables: {}", .0.join(", "))]
    EnvLocked(Vec<String>),
    /// The edited text is not TOML, or the start-up loader refuses it;
    /// carries the parser's or loader's message. Also what a plain-message
    /// refusal of an edit closure becomes.
    #[error("{0}")]
    Invalid(String),
    /// The edit would change who may sign in to the admin UI.
    #[error("{0}")]
    AdminDid(&'static str),
    /// The edit would switch the admin UI on or off.
    #[error("{0}")]
    AdminUi(&'static str),
    /// The file is no longer the one the edit was made from.
    #[error(
        "the configuration was changed in the meantime (in another tab, or from the Operations \
         page); nothing was saved. Reload the page and make the change again"
    )]
    Changed,
    /// Reading or writing the file failed.
    #[error("{0}")]
    Io(String),
}

/// An edit's closure may refuse with a plain message: the result is not
/// accepted ([`EditError::Invalid`]).
impl From<&str> for EditError {
    fn from(message: &str) -> EditError {
        EditError::Invalid(message.to_owned())
    }
}

/// What an edit changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EditReport {
    /// Dotted keys whose effective value changed.
    pub changed: Vec<String>,
    /// Of those, keys that need a restart.
    pub restart_required: Vec<String>,
}

/// The live configuration: the loaded config in force, the path of the
/// `config.toml` it was read from, and the environment captured at
/// start-up, against which every edit is loaded again. Edits are
/// serialized by an internal lock.
#[derive(Debug)]
pub struct ConfigStore {
    path: PathBuf,
    env: Vec<(String, String)>,
    current: RwLock<Arc<LoadedConfig>>,
    edit_lock: tokio::sync::Mutex<()>,
}

/// Flattens a TOML value into dotted keys (arrays are leaves).
pub fn flatten(v: &toml::Value) -> BTreeMap<String, String> {
    fn walk(prefix: &str, v: &toml::Value, out: &mut BTreeMap<String, String>) {
        match v {
            toml::Value::Table(t) => {
                for (k, v) in t {
                    let key = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{prefix}.{k}")
                    };
                    walk(&key, v, out);
                }
            }
            other => {
                out.insert(prefix.to_owned(), other.to_string());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk("", v, &mut out);
    out
}

fn effective(l: &LoadedConfig) -> BTreeMap<String, String> {
    toml::Value::try_from(&l.config)
        .map(|v| flatten(&v))
        .unwrap_or_default()
}

impl ConfigStore {
    /// A store for a loaded config read from `path` with environment
    /// `env`.
    pub fn new(path: PathBuf, env: Vec<(String, String)>, loaded: LoadedConfig) -> ConfigStore {
        ConfigStore {
            path,
            env,
            current: RwLock::new(Arc::new(loaded)),
            edit_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// The config in force, as a shared snapshot. A stored edit swaps in a
    /// new `Arc`, so a snapshot a request holds never changes under it.
    pub fn current(&self) -> Arc<LoadedConfig> {
        self.current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Path of the `config.toml` that edits read and rewrite.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The environment captured at start-up.
    pub fn env(&self) -> &[(String, String)] {
        &self.env
    }

    /// The file's current text.
    pub fn file_text(&self) -> Result<String, EditError> {
        std::fs::read_to_string(&self.path).map_err(|e| EditError::Io(e.to_string()))
    }

    /// Edits the file's TOML table with `f`, validates, and stores it.
    pub async fn edit(
        &self,
        f: impl FnOnce(&mut toml::Table) -> Result<(), EditError>,
    ) -> Result<EditReport, EditError> {
        let _guard = self.edit_lock.lock().await;
        if self.current().from_env_only {
            return Err(EditError::ManagedExternally);
        }
        let text = self.file_text()?;
        let mut table: toml::Table = text
            .parse()
            .map_err(|e: toml::de::Error| EditError::Invalid(e.to_string()))?;
        f(&mut table)?;
        let new_text =
            toml::to_string_pretty(&table).map_err(|e| EditError::Invalid(e.to_string()))?;
        self.store_locked(&text, &new_text)
    }

    /// Replaces the whole file with `new_text` (settings page), refusing
    /// changes to environment-locked keys.
    pub async fn replace(&self, new_text: &str) -> Result<EditReport, EditError> {
        self.replace_from(None, new_text).await
    }

    /// [`ConfigStore::replace`] for a text that was written from the
    /// file as it was when its SHA-256 was `base`: if the file is another
    /// one by now, nothing is stored ([`EditError::Changed`]). A whole
    /// file submitted from an older copy would otherwise undo every edit
    /// made since. The check and the write happen under the edit lock.
    pub async fn replace_from(
        &self,
        base: Option<&[u8; 32]>,
        new_text: &str,
    ) -> Result<EditReport, EditError> {
        let _guard = self.edit_lock.lock().await;
        if self.current().from_env_only {
            return Err(EditError::ManagedExternally);
        }
        let text = self.file_text()?;
        if base.is_some_and(|b| *b != crate::auth::sha256(&text)) {
            return Err(EditError::Changed);
        }
        self.store_locked(&text, new_text)
    }

    fn store_locked(&self, old_text: &str, new_text: &str) -> Result<EditReport, EditError> {
        let old_file: toml::Value = old_text
            .parse::<toml::Table>()
            .map(toml::Value::Table)
            .map_err(|e| EditError::Invalid(e.to_string()))?;
        let new_file: toml::Value = new_text
            .parse::<toml::Table>()
            .map(toml::Value::Table)
            .map_err(|e| EditError::Invalid(e.to_string()))?;
        let (of, nf) = (flatten(&old_file), flatten(&new_file));
        let file_changed: BTreeSet<&String> = of
            .keys()
            .chain(nf.keys())
            .filter(|k| of.get(*k) != nf.get(*k))
            .collect();
        let cur = self.current();
        let locked: Vec<String> = cur
            .env_keys
            .iter()
            .filter(|k| file_changed.contains(k))
            .cloned()
            .collect();
        if !locked.is_empty() {
            return Err(EditError::EnvLocked(locked));
        }
        let loaded = config::load_from_parts(Some(new_text), &self.env)
            .map_err(|e| EditError::Invalid(e.to_string()))?;
        if loaded.config.access.admin_ui != cur.config.access.admin_ui {
            return Err(EditError::AdminUi(ADMIN_UI_NEEDS_RESTART));
        }
        // A stolen session must not become permanent control, and a typo
        // must not lock the operator out of the page they are on.
        if loaded.config.access.admin_did != cur.config.access.admin_did {
            return Err(EditError::AdminDid(ADMIN_DID_READ_ONLY));
        }
        config::write_replace(&self.path, new_text).map_err(|e| EditError::Io(e.to_string()))?;
        let (oe, ne) = (effective(&cur), effective(&loaded));
        let changed: Vec<String> = oe
            .keys()
            .chain(ne.keys())
            .filter(|k| oe.get(*k) != ne.get(*k))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let restart_required = changed.iter().filter(|k| !is_hot(k)).cloned().collect();
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(loaded);
        Ok(EditReport {
            changed,
            restart_required,
        })
    }
}

/// Sends `NOTIFY farsight_config`.
pub async fn notify_config(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_notify($1, '')")
        .bind(CONFIG_CHANNEL)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_keys() {
        assert!(is_hot("access.reads"));
        assert!(is_hot("access.public_ui"));
        assert!(is_hot("public_ui.excluded_dids"));
        assert!(!is_hot("storage.block_history_enabled"));
        assert!(is_hot("backfill.sweep.enabled"));
        assert!(is_hot("rate_limit.anon_rps"));
        assert!(!is_hot("rate_limit.query_concurrency"));
        assert!(!is_hot("storage.database_url"));
        assert!(!is_hot("accessx"));
    }

    #[test]
    fn sensitive_changes_are_named_and_nothing_else_is() {
        use farsight_core::config::{Config, ReadsMode};
        let base = Config::default();
        let with = |f: &dyn Fn(&mut Config)| {
            let mut c = base.clone();
            f(&mut c);
            sensitive_changes(&base, &c)
        };
        assert!(with(&|_| {}).is_empty());
        assert_eq!(
            with(&|c| c.auth.admin_token_sha256 = "ab".repeat(32)),
            [ADMIN_TOKEN_KEY]
        );
        assert_eq!(
            with(&|c| c.backfill.plc_url = "https://plc.evil.example".into()),
            ["backfill.plc_url"]
        );
        assert_eq!(
            with(&|c| c.backfill.relay_url = "https://relay.evil.example".into()),
            ["backfill.relay_url"]
        );
        assert_eq!(
            with(&|c| c.net.allow_http_hosts = vec!["198.51.100.1".into()]),
            ["net.allow_http_hosts"]
        );
        // What else outlives a session.
        assert_eq!(
            with(&|c| c.storage.database_url = "postgres://x@db.evil.example/f".into()),
            ["storage.database_url"]
        );
        assert_eq!(
            with(&|c| c.server.hostname = "evil.example".into()),
            ["server.hostname"]
        );
        assert_eq!(
            with(&|c| c.firehose.urls = vec!["wss://jet.evil.example".into()]),
            ["firehose.urls"]
        );
        assert_eq!(
            with(&|c| c.backfill.backlinks.url = "https://links.evil.example".into()),
            ["backfill.backlinks.url"]
        );
        assert_eq!(
            with(&|c| c.metrics.bearer_token_sha256 = "cd".repeat(32)),
            ["metrics.bearer_token_sha256"]
        );
        assert_eq!(
            with(&|c| c.metrics.bind = "0.0.0.0:9090".into()),
            ["metrics.bind"]
        );
        assert_eq!(
            with(&|c| c.proxy.trusted = vec!["203.0.113.0/24".parse().unwrap()]),
            ["proxy.trusted"]
        );
        assert_eq!(
            with(&|c| c.public_ui.record_viewer_url = "https://v.evil.example/{rkey}".into()),
            ["public_ui.record_viewer_url"]
        );
        // Everything else is an ordinary change.
        assert!(with(&|c| c.server.contact = "mailto:x@example.com".into()).is_empty());
        assert!(with(&|c| c.backfill.plc_rps += 1).is_empty());
        assert!(with(&|c| c.public_ui.instance_description = "x".into()).is_empty());
        assert!(with(&|c| c.rate_limit.anon_rps += 1).is_empty());
        // The public UI: only what shows more.
        let mut shut = base.clone();
        for f in [
            |c: &mut Config| c.public_ui.show_outgoing_blocks = false,
            |c: &mut Config| c.public_ui.show_top_blockers = false,
            |c: &mut Config| c.public_ui.show_top_blocked = false,
            |c: &mut Config| c.public_ui.crawlable = false,
            |c: &mut Config| c.public_ui.show_avatars = false,
            |c: &mut Config| c.public_ui.avatar_thumbnails = false,
        ] {
            f(&mut shut);
        }
        shut.public_ui.excluded_dids = vec!["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into()];
        let mut shown = shut.clone();
        shown.public_ui.show_outgoing_blocks = true;
        shown.public_ui.show_top_blockers = true;
        shown.public_ui.show_top_blocked = true;
        shown.public_ui.crawlable = true;
        shown.public_ui.show_avatars = true;
        shown.public_ui.avatar_thumbnails = true;
        shown.public_ui.excluded_dids.clear();
        assert_eq!(
            sensitive_changes(&shut, &shown),
            [
                "public_ui.avatar_thumbnails",
                "public_ui.crawlable",
                "public_ui.excluded_dids",
                "public_ui.show_avatars",
                "public_ui.show_outgoing_blocks",
                "public_ui.show_top_blocked",
                "public_ui.show_top_blockers",
            ]
        );
        assert!(sensitive_changes(&shown, &shut).is_empty());
        // Withholding one more account is not a widening.
        let mut more = shut.clone();
        more.public_ui
            .excluded_dids
            .push("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".into());
        assert!(sensitive_changes(&shut, &more).is_empty());
        // Access: only the direction that lets more callers in.
        let mut closed = base.clone();
        closed.access.reads = ReadsMode::Disabled;
        closed.access.cors = false;
        closed.access.public_ui = false;
        let mut open = closed.clone();
        open.access.reads = ReadsMode::Public;
        open.access.cors = true;
        open.access.public_ui = true;
        assert_eq!(
            sensitive_changes(&closed, &open),
            ["access.cors", "access.public_ui", "access.reads"]
        );
        assert!(sensitive_changes(&open, &closed).is_empty());
        let mut keyed = closed.clone();
        keyed.access.reads = ReadsMode::ApiKey;
        assert_eq!(sensitive_changes(&closed, &keyed), ["access.reads"]);
        assert_eq!(sensitive_changes(&keyed, &open).len(), 3);
        assert!(sensitive_changes(&open, &keyed).is_empty());
    }

    #[tokio::test]
    async fn a_whole_file_from_an_older_copy_is_not_stored() {
        let dir =
            std::env::temp_dir().join(format!("farsight-config-store-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let file = |contact: &str, anon: u32| {
            format!(
                "[server]\nhostname = \"farsight.test\"\ncontact = \"{contact}\"\n\
                 [storage]\ndatabase_url = \"postgres://u:p@db/f\"\n\
                 [auth]\nadmin_token_sha256 = \"{}\"\n\
                 [rate_limit]\nanon_rps = {anon}\n",
                "ab".repeat(32)
            )
        };
        let first = file("mailto:a@farsight.test", 10);
        std::fs::write(&path, &first).unwrap();
        let loaded = config::load_from_parts(Some(&first), &[]).unwrap();
        let store = ConfigStore::new(path.clone(), Vec::new(), loaded);
        let base = crate::auth::sha256(&first);
        // Another tab saves in between.
        store
            .replace(&file("mailto:a@farsight.test", 20))
            .await
            .unwrap();
        // The text made from the first copy would put `anon_rps` back.
        let stale = file("mailto:b@farsight.test", 10);
        assert!(matches!(
            store.replace_from(Some(&base), &stale).await,
            Err(EditError::Changed)
        ));
        assert_eq!(store.current().config.rate_limit.anon_rps, 20);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            file("mailto:a@farsight.test", 20)
        );
        // From the copy in force it is stored.
        let base = crate::auth::sha256(&store.file_text().unwrap());
        let report = store
            .replace_from(Some(&base), &file("mailto:b@farsight.test", 20))
            .await
            .unwrap();
        assert_eq!(report.changed, ["server.contact"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flatten_keys() {
        let v: toml::Value = toml::from_str("[a]\nb = 1\n[a.c]\nd = [1, 2]\n").unwrap();
        let f = flatten(&v);
        assert_eq!(f.get("a.b").map(String::as_str), Some("1"));
        assert_eq!(f.get("a.c.d").map(String::as_str), Some("[1, 2]"));
    }
}
