//! Why a mode of the server ends in failure, and why a periodic task's
//! run or a refresh of the Cloudflare ranges fails.

/// Why setup mode or normal mode ended in failure. The process logs it
/// and exits non-zero.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// The storage layer failed.
    #[error(transparent)]
    Storage(#[from] farsight_storage::StorageError),
    /// A statement failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// A start-up step failed; `step` names it.
    #[error("{step}: {source}")]
    Step {
        /// The step, in the words of the log.
        step: &'static str,
        /// Why it failed.
        #[source]
        source: Box<ServerError>,
    },
    /// Ingest did not start.
    #[error("starting ingest: {0}")]
    Ingest(String),
    /// A listener could not be bound or stopped serving, or the setup
    /// token's file could not be written; `what` says which.
    #[error("{what}: {source}")]
    Io {
        /// What was being done.
        what: String,
        /// Why it failed.
        #[source]
        source: std::io::Error,
    },
    /// An ingest task panicked. Ingest resumes from its stored position
    /// only in a new process.
    #[error(
        "the ingest task {0} panicked; exiting so that ingest resumes from its stored position"
    )]
    IngestPanicked(&'static str),
}

impl ServerError {
    /// Wraps the failure of the start-up step `step`.
    pub fn step<E: Into<ServerError>>(step: &'static str) -> impl FnOnce(E) -> ServerError {
        move |e| ServerError::Step {
            step,
            source: Box::new(e.into()),
        }
    }

    /// Wraps an I/O failure of `what`.
    pub fn io(what: impl Into<String>) -> impl FnOnce(std::io::Error) -> ServerError {
        move |source| ServerError::Io {
            what: what.into(),
            source,
        }
    }
}

/// Why a run of a periodic task failed. The text goes to the log and to
/// the operator's error list.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// A statement failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// The storage layer failed.
    #[error(transparent)]
    Storage(#[from] farsight_storage::StorageError),
    /// The run panicked, with the panic's message.
    #[error("panicked: {0}")]
    Panicked(String),
}

/// Why the Cloudflare ranges could not be refreshed.
#[derive(Debug, thiserror::Error)]
pub enum CloudflareError {
    /// The request could not be made, or failed.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// A list was fetched and is not usable.
    #[error("{url}: {problem}")]
    List {
        /// The list's URL.
        url: &'static str,
        /// What is wrong with it.
        problem: String,
    },
}

/// Why the admin DID could not be looked up from the command line.
#[derive(Debug, thiserror::Error)]
pub enum LookupError {
    /// The runtime for the lookup could not be started.
    #[error(transparent)]
    Runtime(#[from] std::io::Error),
    /// The lookup itself failed.
    #[error(transparent)]
    Identity(#[from] farsight_web::oauth::OAuthError),
}
