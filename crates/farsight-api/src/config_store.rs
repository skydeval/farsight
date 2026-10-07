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
        let _guard = self.edit_lock.lock().await;
        if self.current().from_env_only {
            return Err(EditError::ManagedExternally);
        }
        let text = self.file_text()?;
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
    fn flatten_keys() {
        let v: toml::Value = toml::from_str("[a]\nb = 1\n[a.c]\nd = [1, 2]\n").unwrap();
        let f = flatten(&v);
        assert_eq!(f.get("a.b").map(String::as_str), Some("1"));
        assert_eq!(f.get("a.c.d").map(String::as_str), Some("[1, 2]"));
    }
}
