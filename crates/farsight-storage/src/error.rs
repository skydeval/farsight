//! Storage errors.

/// Postgres SQLSTATE for a detected deadlock.
pub const DEADLOCK_SQLSTATE: &str = "40P01";

/// Errors from the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// An error from sqlx: the connection, the pool, or a statement
    /// Postgres refused.
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    /// Running migrations failed.
    #[error("migrations: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// Every retry of a transaction hit a deadlock abort. Deadlock retries
    /// never count toward poisoned-event handling; callers must not treat
    /// this as a poisoned event.
    #[error("deadlock retries exhausted after {0} attempts")]
    DeadlockRetriesExhausted(u32),
    /// The lists a batch has to lock changed between reading them and
    /// locking them. The transaction is rolled back and run again; like a
    /// deadlock abort this says nothing about the batch's events.
    #[error("the lock set of the batch changed while it was being taken")]
    LockSetChanged,
    /// A listing stamp read more than 72 h ago. The job must restart with a
    /// fresh stamp.
    #[error("listing stamp read at {0} is older than 72 h")]
    StaleStamp(chrono::DateTime<chrono::Utc>),
    /// A listing read from a host that was not confirmed with the
    /// directory would have removed stored rows. Nothing was written; the
    /// job confirms the host and lists again.
    #[error("a listing from an unconfirmed host would remove stored rows")]
    UnconfirmedHost,
    /// The database schema is not the version this build expects.
    #[error("schema version {found:?}, expected {expected}")]
    SchemaVersion {
        /// Stored version (None if the table is missing).
        found: Option<i32>,
        /// Compiled-in version.
        expected: i32,
    },
    /// An internal invariant was violated (a bug or a corrupted row).
    #[error("invariant violated: {0}")]
    Invariant(String),
}

impl StorageError {
    /// Whether this is a deadlock abort (SQLSTATE 40P01).
    pub fn is_deadlock(&self) -> bool {
        match self {
            StorageError::Db(sqlx::Error::Database(db)) => {
                db.code().as_deref() == Some(DEADLOCK_SQLSTATE)
            }
            _ => false,
        }
    }
}

impl StorageError {
    /// Whether the transaction was aborted for a reason that running it
    /// again resolves: a deadlock, or a lock set that changed under it.
    pub fn is_retryable_abort(&self) -> bool {
        self.is_deadlock() || matches!(self, StorageError::LockSetChanged)
    }
}

/// The crate's result type: the error is [`StorageError`] unless another is
/// named.
pub type Result<T, E = StorageError> = std::result::Result<T, E>;
