//! Numeric codes of the SMALLINT enum columns (see
//! `docs/design/storage.md`).

/// Lets a code enum travel through sqlx as its integer column type, so
/// queries bind and decode the enum itself. A stored value outside the
/// enum is a decode error.
macro_rules! sqlx_code {
    ($name:ident, $repr:ty) => {
        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <$repr as sqlx::Type<sqlx::Postgres>>::type_info()
            }

            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <$repr as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        impl sqlx::postgres::PgHasArrayType for $name {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <$repr as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }

        impl<'q> sqlx::Encode<'q, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> std::result::Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                <$repr as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.sql_value(), buf)
            }
        }

        impl<'r> sqlx::Decode<'r, sqlx::Postgres> for $name {
            fn decode(
                value: sqlx::postgres::PgValueRef<'r>,
            ) -> std::result::Result<Self, sqlx::error::BoxDynError> {
                let raw = <$repr as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
                $name::from_sql_value(&raw)
                    .ok_or_else(|| format!("unknown {} {raw:?}", stringify!($name)).into())
            }
        }
    };
}
pub(crate) use sqlx_code;

macro_rules! code_enum {
    ($(#[$m:meta])* $name:ident { $($(#[$vm:meta])* $var:ident = $val:literal),+ $(,)? }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name { $($(#[$vm])* $var),+ }

        impl $name {
            /// Every variant, in code order.
            pub const ALL: &'static [$name] = &[$($name::$var),+];

            /// The stored code.
            pub const fn code(self) -> i16 {
                match self { $($name::$var => $val),+ }
            }

            /// The variant of a stored code; `None` for a value no
            /// variant has.
            pub const fn from_code(code: i16) -> Option<$name> {
                match code { $($val => Some($name::$var),)+ _ => None }
            }

            fn sql_value(&self) -> i16 {
                self.code()
            }

            fn from_sql_value(code: &i16) -> Option<$name> {
                $name::from_code(*code)
            }
        }

        sqlx_code!($name, i16);
    };
}

code_enum! {
    /// `lists.track_state`.
    TrackState {
        /// No counted listblock.
        Untracked = 0,
        /// Admitted in the current epoch, not promoted yet.
        Pending = 1,
        /// Promoted by a completed fetch run.
        Ready = 2,
        /// Was ready, lost its last counted listblock, within grace.
        Retained = 3,
        /// Errors, timeouts or owner inactive; retrying.
        Unavailable = 4,
        /// Items being deleted; `purge_then` is the target.
        Purging = 5,
        /// Counted listblocks exist; record not found; retrying.
        Missing = 6,
        /// Record known deleted, or not found through all retries.
        Dead = 7,
        /// Admission refused by a gate (see `deferred_by`).
        Deferred = 8,
    }
}

impl TrackState {
    /// "Tracked": only these accept listitem writes.
    pub fn is_tracked(self) -> bool {
        matches!(
            self,
            TrackState::Pending
                | TrackState::Ready
                | TrackState::Retained
                | TrackState::Unavailable
        )
    }

    /// States in which a list is "waiting" for phase 1 or a fetch run and so
    /// has `list_sched_keys` lanes.
    pub fn is_waiting(self) -> bool {
        matches!(
            self,
            TrackState::Pending | TrackState::Unavailable | TrackState::Missing
        )
    }

    /// The state an API name stands for; `None` for any other text.
    pub fn from_api_name(s: &str) -> Option<TrackState> {
        TrackState::ALL.iter().copied().find(|t| t.api_name() == s)
    }

    /// API name (the `state` field).
    pub fn api_name(self) -> &'static str {
        match self {
            TrackState::Untracked => "untracked",
            TrackState::Pending => "pending",
            TrackState::Ready => "ready",
            TrackState::Retained => "retained",
            TrackState::Unavailable => "unavailable",
            TrackState::Purging => "purging",
            TrackState::Missing => "missing",
            TrackState::Dead => "dead",
            TrackState::Deferred => "deferred",
        }
    }
}

code_enum! {
    /// `lists.record_state`.
    RecordState {
        /// Never seen the `list` record (placeholder row).
        Unknown = 0,
        /// The record exists.
        Present = 1,
        /// The record was deleted.
        Deleted = 2,
    }
}

code_enum! {
    /// `lists.deferred_by`: which gate deferred the list.
    DeferCause {
        /// Storage budget ≥ 100%.
        Budget = 1,
        /// The database is at or over the hard ceiling
        /// (`storage.hard_ceiling_bytes`).
        Ceiling = 2,
        /// Owner bucket over `host_list_items` (phase 1 of a list job).
        HostCap = 3,
        /// Found record refused by `lists_per_author` / `host_lists`.
        ListsCap = 4,
        /// Owner re-admission budget.
        OwnerReadmissions = 5,
    }
}

code_enum! {
    /// `relist_debt.reason`.
    DebtReason {
        /// Terminal failure or reconcile skipped.
        Unreachable = 1,
        /// `#sync`, desynchronized, reactivation, divergence, poisoned event.
        Resync = 2,
        /// A per-author cap or a daily rate refused or uncounted a record.
        Capped = 3,
        /// A host-bucket cap or the storage budget refused a write.
        Refused = 4,
    }
}

code_enum! {
    /// `relist_debt.cap_type`: which cap or rate caused a `capped` /
    /// `refused` debt (the feeder waits on it).
    CapType {
        /// `limits.blocks_per_author`.
        BlocksPerAuthor = 1,
        /// `limits.listblocks_per_author`.
        ListblocksPerAuthor = 2,
        /// `limits.lists_per_author`.
        ListsPerAuthor = 3,
        /// `limits.listblock_fetch_triggers_per_author` (uncounted rows).
        TriggerCap = 4,
        /// Daily admission rate (uncounted rows).
        AdmissionRate = 5,
        /// Daily intern rate.
        InternRate = 6,
        /// Bucket `host_blocks`.
        HostBlocks = 7,
        /// Bucket `host_list_items`.
        HostListItems = 8,
        /// Bucket `host_listblocks`.
        HostListblocks = 9,
        /// Bucket `host_lists`.
        HostLists = 10,
        /// Bucket lifetime intern bound.
        InternLifetime = 11,
        /// The database is at or over `storage.budget_bytes`.
        Budget = 12,
        /// The database is at or over the hard ceiling.
        Ceiling = 13,
        /// Deletes-only listing (budget gate on a job).
        DeletesOnly = 14,
    }
}

impl CapType {
    /// Daily-rate cap types: eligible once per UTC day.
    pub fn is_daily_rate(self) -> bool {
        matches!(self, CapType::AdmissionRate | CapType::InternRate)
    }

    /// The `kind` label of `farsight_abuse_capped_total`.
    pub fn label(self) -> &'static str {
        match self {
            CapType::BlocksPerAuthor => "blocks_per_author",
            CapType::ListblocksPerAuthor => "listblocks_per_author",
            CapType::ListsPerAuthor => "lists_per_author",
            CapType::TriggerCap => "listblock_fetch_triggers_per_author",
            CapType::AdmissionRate => "admission_rate",
            CapType::InternRate => "intern_rate",
            CapType::HostBlocks => "host_blocks",
            CapType::HostListItems => "host_list_items",
            CapType::HostListblocks => "host_listblocks",
            CapType::HostLists => "host_lists",
            CapType::InternLifetime => "host_interned_lifetime",
            CapType::Budget => "budget",
            CapType::Ceiling => "ceiling",
            CapType::DeletesOnly => "deletes_only",
        }
    }
}

code_enum! {
    /// `firehose_gaps.cause`.
    GapCause {
        /// v2 `CursorTooOld`.
        CursorTooOld = 1,
        /// v1 heuristic (first event − cursor > threshold).
        Heuristic = 2,
        /// Cross-instance failover without a safe rewind.
        Failover = 3,
        /// An interval spent on v1 (no `#sync`).
        SyncUnavailable = 4,
        /// A seam window whose re-read could not be finished.
        SeamUnrepaired = 5,
        /// Frames that could not be read at one position, stepped past
        /// after repeated attempts.
        Unreadable = 6,
    }
}

impl GapCause {
    /// The `cause` of a gap in `getStats`.
    pub const fn api_name(self) -> &'static str {
        match self {
            GapCause::CursorTooOld => "CursorTooOld",
            GapCause::Heuristic => "Heuristic",
            GapCause::Failover => "Failover",
            GapCause::SyncUnavailable => "SyncUnavailable",
            GapCause::SeamUnrepaired => "SeamUnrepaired",
            GapCause::Unreadable => "Unreadable",
        }
    }
}

code_enum! {
    /// `firehose_seams.trigger`: what kind of resume a seam window
    /// follows.
    SeamTrigger {
        /// A resume on the same instance.
        Resume = 1,
        /// A resume on another instance.
        Failover = 2,
        /// A resume that recorded a gap.
        ClampRecovery = 3,
    }
}

impl SeamTrigger {
    /// The `trigger` label of `farsight_firehose_seam_repairs_total`.
    pub const fn label(self) -> &'static str {
        match self {
            SeamTrigger::Resume => "resume",
            SeamTrigger::Failover => "failover",
            SeamTrigger::ClampRecovery => "clamp_recovery",
        }
    }

    /// The trigger for a resume: `gap` if its first event recorded one,
    /// `failover` if it is on another instance than the session before.
    pub const fn of(gap: bool, failover: bool) -> SeamTrigger {
        if gap {
            SeamTrigger::ClampRecovery
        } else if failover {
            SeamTrigger::Failover
        } else {
            SeamTrigger::Resume
        }
    }
}

code_enum! {
    /// `firehose_state.protocol`.
    Protocol {
        /// Jetstream v1 `/subscribe`.
        V1 = 1,
        /// Jetstream v2 `subscribeEvents`.
        V2 = 2,
    }
}

impl Protocol {
    /// The `protocol` of the firehose in `getStats`.
    pub const fn api_name(self) -> &'static str {
        match self {
            Protocol::V1 => "v1",
            Protocol::V2 => "v2",
        }
    }
}

code_enum! {
    /// `backfill_state.last_outcome`.
    RunOutcome {
        /// Everything listed, nothing refused, account active.
        Clean = 1,
        /// Listed to the end; shortfalls recorded as debts.
        CompleteWithDebts = 2,
        /// Relay-confirmed inactive.
        Inactive = 3,
        /// The run ended in an error.
        Failed = 4,
    }
}

code_enum! {
    /// `sweep_cycles.kind`.
    CycleKind {
        /// A sweep over every repository its source enumerates.
        Full = 1,
        /// A repair of firehose gaps: the accounts whose repository changed
        /// during them are listed again.
        Repair = 2,
    }
}

code_enum! {
    /// `subject_coverage.scope`.
    SubjectScope {
        /// Direct blocks: `block` records that name the subject.
        Block = 1,
        /// listitem → list → listblock chain.
        ListChain = 2,
    }
}

code_enum! {
    /// `actors.status`: the account status the relay last reported.
    ActorStatus {
        /// `active`, or no status given.
        Active = 0,
        /// `deactivated` (hidden).
        Deactivated = 1,
        /// `takendown` (hidden).
        Takendown = 2,
        /// `suspended` (hidden).
        Suspended = 3,
        /// `deleted` (hidden, purged).
        Deleted = 4,
        /// `throttled` (shown).
        Throttled = 5,
        /// `desynchronized` (shown, re-listed).
        Desynchronized = 6,
        /// Any other upstream value (shown).
        Unknown = 7,
    }
}

impl ActorStatus {
    /// Hidden statuses: rows excluded unless `includeInactive`.
    pub const fn is_hidden(self) -> bool {
        matches!(
            self,
            ActorStatus::Deactivated
                | ActorStatus::Takendown
                | ActorStatus::Suspended
                | ActorStatus::Deleted
        )
    }

    /// Maps an upstream status string (`None` = active).
    pub fn from_upstream(status: Option<&str>) -> ActorStatus {
        match status {
            None | Some("active") => ActorStatus::Active,
            Some("deactivated") => ActorStatus::Deactivated,
            Some("takendown") => ActorStatus::Takendown,
            Some("suspended") => ActorStatus::Suspended,
            Some("deleted") => ActorStatus::Deleted,
            Some("throttled") => ActorStatus::Throttled,
            Some("desynchronized") => ActorStatus::Desynchronized,
            Some(_) => ActorStatus::Unknown,
        }
    }
}

code_enum! {
    /// `backfill_state.state`: where an actor's repo job stands.
    BackfillState {
        /// Never backfilled.
        Never = 0,
        /// Waiting in the queue.
        Queued = 1,
        /// A repo job holds the lease.
        Running = 2,
        /// Finished (clean, complete-with-debts or inactive).
        Done = 3,
        /// Failed; `next_attempt_at` says when it is retried.
        Failed = 4,
    }
}

code_enum! {
    /// `discovery_state.state`: where a subject's discovery stands. An
    /// actor never asked about has no row.
    DiscoveryRun {
        /// Waiting in the queue.
        Queued = 1,
        /// A discovery job is reading the backlink index.
        Running = 2,
        /// Finished.
        Done = 3,
        /// Failed.
        Failed = 4,
    }
}

code_enum! {
    /// `backfill_queue.kind` and `backfill_cursors.job_kind` (cursors
    /// exist for the first two only).
    JobKind {
        /// Full repo job.
        Repo = 1,
        /// List fetch run for an owner.
        ListFetch = 2,
        /// Subject discovery.
        Discovery = 3,
    }
}

code_enum! {
    /// `backfill_queue.tier`: the scheduler's three classes of work,
    /// most urgent first. A waiting entry asked for again takes the more
    /// urgent tier.
    Tier {
        /// On-demand requests and list work.
        OnDemand = 1,
        /// Authors the firehose showed active.
        Active = 2,
        /// Sweep and repair members and their retries.
        Sweep = 3,
    }
}

impl Tier {
    /// Position in the scheduler's per-tier arrays (`0..3`).
    pub const fn index(self) -> usize {
        match self {
            Tier::OnDemand => 0,
            Tier::Active => 1,
            Tier::Sweep => 2,
        }
    }

    /// The tier at a position of the scheduler's per-tier arrays.
    pub const fn from_index(i: usize) -> Option<Tier> {
        match i {
            0 => Some(Tier::OnDemand),
            1 => Some(Tier::Active),
            2 => Some(Tier::Sweep),
            _ => None,
        }
    }

    /// The `tier` label of the backfill metrics (`1`, `2`, `3`).
    pub const fn label(self) -> &'static str {
        match self {
            Tier::OnDemand => "1",
            Tier::Active => "2",
            Tier::Sweep => "3",
        }
    }
}

code_enum! {
    /// `backfill_queue.priority`; the higher code runs first within a
    /// requester.
    Priority {
        /// `normal`.
        Normal = 0,
        /// `high`.
        High = 1,
    }
}

code_enum! {
    /// `list_fetch_runs.outcome`; a running run has none.
    FetchOutcome {
        /// Every claimed list was fetched.
        Ok = 1,
        /// The run failed.
        Failed = 2,
        /// The owner's account is inactive.
        OwnerInactive = 3,
        /// Closed without finishing (the owner was deleted, or the run
        /// was abandoned).
        Cancelled = 4,
    }
}

code_enum! {
    /// `cycle_outstanding.state`.
    MemberState {
        /// Enumerated, not finished: waiting, running or retrying.
        Outstanding = 1,
        /// Failed for good; kept until the cycle completes.
        Terminal = 2,
    }
}

/// A value stored as text that has a closed set of spellings.
macro_rules! text_enum {
    ($(#[$m:meta])* $name:ident { $($(#[$vm:meta])* $var:ident = $val:literal),+ $(,)? }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name { $($(#[$vm])* $var),+ }

        impl $name {
            /// Every variant.
            pub const ALL: &'static [$name] = &[$($name::$var),+];

            /// The stored spelling.
            pub const fn as_str(self) -> &'static str {
                match self { $($name::$var => $val),+ }
            }

            /// The variant of a stored spelling; `None` for any other
            /// text.
            pub fn parse(s: &str) -> Option<$name> {
                match s { $($val => Some($name::$var),)+ _ => None }
            }

            fn sql_value(&self) -> &'static str {
                self.as_str()
            }

            fn from_sql_value(s: &str) -> Option<$name> {
                $name::parse(s)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        sqlx_code!($name, &str);
    };
}

text_enum! {
    /// `sweep_cycles.source`: what a cycle enumerates.
    CycleSource {
        /// `com.atproto.sync.listReposByCollection` on the relay.
        RelayCollections = "relay_collections",
        /// `com.atproto.sync.listRepos` on the relay.
        RelayRepos = "relay_repos",
        /// The PLC directory's `/export`.
        Plc = "plc",
        /// Every DID the index already knows: a repair started while
        /// the relay was unreachable. It heals no gap.
        KnownDids = "known_dids",
    }
}

impl From<farsight_core::config::SweepSource> for CycleSource {
    fn from(s: farsight_core::config::SweepSource) -> CycleSource {
        use farsight_core::config::SweepSource;
        match s {
            SweepSource::RelayCollections => CycleSource::RelayCollections,
            SweepSource::RelayRepos => CycleSource::RelayRepos,
            SweepSource::Plc => CycleSource::Plc,
        }
    }
}

/// Who asked for a queue entry (`backfill_queue.requester`). The same
/// spelling is the cause key that discovery's interning is charged to.
///
/// Keys order as their stored text does (`admin`, then the `system:`
/// keys by name, then tokens by the digits of their id): the
/// scheduler breaks ties between equally charged requesters by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequesterKey {
    /// `system:sweep`: members of a full cycle.
    Sweep,
    /// `system:repair`: members of a repair cycle.
    Repair,
    /// `system:firehose`: authors the firehose showed active.
    Firehose,
    /// `system:lists`: list work (phase 1 and fetch runs).
    Lists,
    /// `system:resync`: re-lists after `#sync`, a poisoned event or a
    /// reactivation.
    Resync,
    /// `admin`: the admin token or the admin UI.
    Admin,
    /// `token:<id>`: the API key with this `api_tokens.id`.
    Token(i32),
}

impl RequesterKey {
    /// The variant of a stored spelling; `None` for any other text.
    pub fn parse(s: &str) -> Option<RequesterKey> {
        match s {
            "system:sweep" => Some(RequesterKey::Sweep),
            "system:repair" => Some(RequesterKey::Repair),
            "system:firehose" => Some(RequesterKey::Firehose),
            "system:lists" => Some(RequesterKey::Lists),
            "system:resync" => Some(RequesterKey::Resync),
            "admin" => Some(RequesterKey::Admin),
            _ => {
                let id = s.strip_prefix("token:")?;
                // Only the spelling `Display` writes: no sign, no
                // leading zero.
                let n: i32 = id.parse().ok()?;
                (n.to_string() == id).then_some(RequesterKey::Token(n))
            }
        }
    }

    /// Whether an API key asked (`token:<id>`).
    pub const fn is_token(self) -> bool {
        matches!(self, RequesterKey::Token(_))
    }

    fn sql_value(&self) -> String {
        self.to_string()
    }

    fn from_sql_value(s: &str) -> Option<RequesterKey> {
        RequesterKey::parse(s)
    }
}

impl Ord for RequesterKey {
    fn cmp(&self, other: &RequesterKey) -> std::cmp::Ordering {
        self.to_string().cmp(&other.to_string())
    }
}

impl PartialOrd for RequesterKey {
    fn partial_cmp(&self, other: &RequesterKey) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for RequesterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequesterKey::Sweep => f.write_str("system:sweep"),
            RequesterKey::Repair => f.write_str("system:repair"),
            RequesterKey::Firehose => f.write_str("system:firehose"),
            RequesterKey::Lists => f.write_str("system:lists"),
            RequesterKey::Resync => f.write_str("system:resync"),
            RequesterKey::Admin => f.write_str("admin"),
            RequesterKey::Token(id) => write!(f, "token:{id}"),
        }
    }
}

sqlx_code!(RequesterKey, String);

/// Codes as they are written into SQL text. A query whose plan depends
/// on the planner seeing a constant (a partial-index predicate, an
/// `IN` list) interpolates these instead of binding a parameter; every
/// one is computed from its enum.
pub mod sql {
    use super::*;

    /// Renders codes as an SQL list such as `(1, 2, 3)`.
    const fn list<const N: usize>(codes: [i16; N]) -> ([u8; 64], usize) {
        let mut out = [0u8; 64];
        let mut n = 0;
        out[n] = b'(';
        n += 1;
        let mut i = 0;
        while i < N {
            if i > 0 {
                out[n] = b',';
                out[n + 1] = b' ';
                n += 2;
            }
            let c = codes[i];
            assert!(c >= 0 && c < 100, "codes have one or two digits");
            if c >= 10 {
                out[n] = b'0' + (c / 10) as u8;
                n += 1;
            }
            out[n] = b'0' + (c % 10) as u8;
            n += 1;
            i += 1;
        }
        out[n] = b')';
        (out, n + 1)
    }

    macro_rules! codes {
        ($($(#[$m:meta])* $name:ident = $e:expr;)+) => {
            $($(#[$m])* pub const $name: i16 = $e.code();)+
        };
    }

    macro_rules! sets {
        ($($(#[$m:meta])* $name:ident = [$($e:expr),+ $(,)?];)+) => {
            $($(#[$m])*
            pub const $name: &str = {
                const BUF: ([u8; 64], usize) = list([$($e.code()),+]);
                match std::str::from_utf8(BUF.0.split_at(BUF.1).0) {
                    Ok(s) => s,
                    Err(_) => panic!("an SQL list is ASCII"),
                }
            };)+
        };
    }

    codes! {
        /// `lists.track_state` = untracked.
        TRACK_UNTRACKED = TrackState::Untracked;
        /// `lists.track_state` = pending.
        TRACK_PENDING = TrackState::Pending;
        /// `lists.track_state` = ready.
        TRACK_READY = TrackState::Ready;
        /// `lists.track_state` = retained.
        TRACK_RETAINED = TrackState::Retained;
        /// `lists.track_state` = unavailable.
        TRACK_UNAVAILABLE = TrackState::Unavailable;
        /// `lists.track_state` = purging.
        TRACK_PURGING = TrackState::Purging;
        /// `lists.track_state` = missing.
        TRACK_MISSING = TrackState::Missing;
        /// `lists.track_state` = deferred.
        TRACK_DEFERRED = TrackState::Deferred;
        /// `lists.record_state` = unknown (a placeholder row).
        RECORD_UNKNOWN = RecordState::Unknown;
        /// `lists.record_state` = present.
        RECORD_PRESENT = RecordState::Present;
        /// `lists.record_state` = deleted.
        RECORD_DELETED = RecordState::Deleted;
        /// `actors.status` = active.
        ACTOR_ACTIVE = ActorStatus::Active;
        /// `backfill_state.state` = queued.
        REPO_QUEUED = BackfillState::Queued;
        /// `backfill_state.state` = running.
        REPO_RUNNING = BackfillState::Running;
        /// `backfill_state.state` = done.
        REPO_DONE = BackfillState::Done;
        /// `backfill_state.state` = failed.
        REPO_FAILED = BackfillState::Failed;
        /// `backfill_state.last_outcome` = inactive.
        RUN_INACTIVE = RunOutcome::Inactive;
        /// `backfill_state.last_outcome` = failed.
        RUN_FAILED = RunOutcome::Failed;
        /// `discovery_state.state` = queued.
        DISCOVERY_QUEUED = DiscoveryRun::Queued;
        /// `discovery_state.state` = running.
        DISCOVERY_RUNNING = DiscoveryRun::Running;
        /// `discovery_state.state` = done.
        DISCOVERY_DONE = DiscoveryRun::Done;
        /// `discovery_state.state` = failed.
        DISCOVERY_FAILED = DiscoveryRun::Failed;
        /// `backfill_queue.kind` / `backfill_cursors.job_kind` = repo.
        JOB_REPO = JobKind::Repo;
        /// `backfill_queue.kind` / `backfill_cursors.job_kind` = list fetch.
        JOB_LIST_FETCH = JobKind::ListFetch;
        /// `backfill_queue.tier` = 1.
        TIER_ON_DEMAND = Tier::OnDemand;
        /// `backfill_queue.tier` = 2.
        TIER_ACTIVE = Tier::Active;
        /// `backfill_queue.tier` = 3.
        TIER_SWEEP = Tier::Sweep;
        /// `list_fetch_runs.outcome` = cancelled.
        FETCH_CANCELLED = FetchOutcome::Cancelled;
        /// `sweep_cycles.kind` = full.
        CYCLE_FULL = CycleKind::Full;
        /// `sweep_cycles.kind` = repair.
        CYCLE_REPAIR = CycleKind::Repair;
        /// `cycle_outstanding.state` = outstanding.
        MEMBER_OUTSTANDING = MemberState::Outstanding;
        /// `cycle_outstanding.state` = terminal.
        MEMBER_TERMINAL = MemberState::Terminal;
        /// `relist_debt.reason` = capped.
        DEBT_CAPPED = DebtReason::Capped;
        /// `subject_coverage.scope` = block.
        SCOPE_BLOCK = SubjectScope::Block;
        /// `subject_coverage.scope` = list chain.
        SCOPE_LIST_CHAIN = SubjectScope::ListChain;
        /// `firehose_cursors.protocol` = v2.
        PROTOCOL_V2 = Protocol::V2;
    }

    sets! {
        /// Tracked lists ([`TrackState::is_tracked`]): pending, ready,
        /// retained, unavailable.
        TRACKED = [
            TrackState::Pending,
            TrackState::Ready,
            TrackState::Retained,
            TrackState::Unavailable,
        ];
        /// Lists whose items are served: ready, retained.
        TRACK_SERVED = [TrackState::Ready, TrackState::Retained];
        /// Tracked lists not fetched yet in their epoch: pending,
        /// unavailable.
        TRACK_UNFETCHED = [TrackState::Pending, TrackState::Unavailable];
        /// Lists waiting for phase 1 or a fetch run
        /// ([`TrackState::is_waiting`]): pending, unavailable, missing.
        TRACK_WAITING = [
            TrackState::Pending,
            TrackState::Unavailable,
            TrackState::Missing,
        ];
        /// Lists a subject's coverage reports as not indexed yet:
        /// pending, unavailable, missing, deferred.
        TRACK_UNINDEXED = [
            TrackState::Pending,
            TrackState::Unavailable,
            TrackState::Missing,
            TrackState::Deferred,
        ];
        /// The one-member list `(unavailable)`.
        TRACK_ONLY_UNAVAILABLE = [TrackState::Unavailable];
        /// Hidden statuses ([`ActorStatus::is_hidden`]): deactivated,
        /// takendown, suspended, deleted.
        HIDDEN = [
            ActorStatus::Deactivated,
            ActorStatus::Takendown,
            ActorStatus::Suspended,
            ActorStatus::Deleted,
        ];
        /// Hidden statuses when suspended accounts are shown.
        HIDDEN_BUT_SUSPENDED = [
            ActorStatus::Deactivated,
            ActorStatus::Takendown,
            ActorStatus::Deleted,
        ];
        /// Hidden statuses when taken-down accounts are shown.
        HIDDEN_BUT_TAKENDOWN = [
            ActorStatus::Deactivated,
            ActorStatus::Suspended,
            ActorStatus::Deleted,
        ];
        /// Accounts that are gone by their owner's act: deactivated,
        /// deleted. No setting shows these.
        GONE = [ActorStatus::Deactivated, ActorStatus::Deleted];
        /// Outcomes of a repo job that listed the repo to its end:
        /// clean, complete with debts, inactive.
        RUN_LISTED = [
            RunOutcome::Clean,
            RunOutcome::CompleteWithDebts,
            RunOutcome::Inactive,
        ];
        /// Debts that leave an author's records unlisted: unreachable,
        /// resync.
        DEBT_UNLISTED = [DebtReason::Unreachable, DebtReason::Resync];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every enum's stored codes, written out: a renumbering fails here.
    #[test]
    fn stored_codes_are_pinned() {
        fn pin<T: Copy + PartialEq + std::fmt::Debug>(
            all: &[T],
            code: fn(T) -> i16,
            from: fn(i16) -> Option<T>,
            want: &[(T, i16)],
        ) {
            assert_eq!(all.len(), want.len(), "a variant is not pinned");
            for (v, c) in want {
                assert_eq!(code(*v), *c, "{v:?}");
                assert_eq!(from(*c), Some(*v), "{c}");
            }
            assert_eq!(from(99), None);
            assert_eq!(from(-1), None);
        }
        use TrackState as T;
        pin(
            T::ALL,
            T::code,
            T::from_code,
            &[
                (T::Untracked, 0),
                (T::Pending, 1),
                (T::Ready, 2),
                (T::Retained, 3),
                (T::Unavailable, 4),
                (T::Purging, 5),
                (T::Missing, 6),
                (T::Dead, 7),
                (T::Deferred, 8),
            ],
        );
        use RecordState as R;
        pin(
            R::ALL,
            R::code,
            R::from_code,
            &[(R::Unknown, 0), (R::Present, 1), (R::Deleted, 2)],
        );
        use DeferCause as D;
        pin(
            D::ALL,
            D::code,
            D::from_code,
            &[
                (D::Budget, 1),
                (D::Ceiling, 2),
                (D::HostCap, 3),
                (D::ListsCap, 4),
                (D::OwnerReadmissions, 5),
            ],
        );
        use DebtReason as B;
        pin(
            B::ALL,
            B::code,
            B::from_code,
            &[
                (B::Unreachable, 1),
                (B::Resync, 2),
                (B::Capped, 3),
                (B::Refused, 4),
            ],
        );
        use CapType as C;
        pin(
            C::ALL,
            C::code,
            C::from_code,
            &[
                (C::BlocksPerAuthor, 1),
                (C::ListblocksPerAuthor, 2),
                (C::ListsPerAuthor, 3),
                (C::TriggerCap, 4),
                (C::AdmissionRate, 5),
                (C::InternRate, 6),
                (C::HostBlocks, 7),
                (C::HostListItems, 8),
                (C::HostListblocks, 9),
                (C::HostLists, 10),
                (C::InternLifetime, 11),
                (C::Budget, 12),
                (C::Ceiling, 13),
                (C::DeletesOnly, 14),
            ],
        );
        use GapCause as G;
        pin(
            G::ALL,
            G::code,
            G::from_code,
            &[
                (G::CursorTooOld, 1),
                (G::Heuristic, 2),
                (G::Failover, 3),
                (G::SyncUnavailable, 4),
                (G::SeamUnrepaired, 5),
                (G::Unreadable, 6),
            ],
        );
        pin(
            SeamTrigger::ALL,
            SeamTrigger::code,
            SeamTrigger::from_code,
            &[
                (SeamTrigger::Resume, 1),
                (SeamTrigger::Failover, 2),
                (SeamTrigger::ClampRecovery, 3),
            ],
        );
        pin(
            Protocol::ALL,
            Protocol::code,
            Protocol::from_code,
            &[(Protocol::V1, 1), (Protocol::V2, 2)],
        );
        use RunOutcome as O;
        pin(
            O::ALL,
            O::code,
            O::from_code,
            &[
                (O::Clean, 1),
                (O::CompleteWithDebts, 2),
                (O::Inactive, 3),
                (O::Failed, 4),
            ],
        );
        pin(
            CycleKind::ALL,
            CycleKind::code,
            CycleKind::from_code,
            &[(CycleKind::Full, 1), (CycleKind::Repair, 2)],
        );
        pin(
            SubjectScope::ALL,
            SubjectScope::code,
            SubjectScope::from_code,
            &[(SubjectScope::Block, 1), (SubjectScope::ListChain, 2)],
        );
        use ActorStatus as A;
        pin(
            A::ALL,
            A::code,
            A::from_code,
            &[
                (A::Active, 0),
                (A::Deactivated, 1),
                (A::Takendown, 2),
                (A::Suspended, 3),
                (A::Deleted, 4),
                (A::Throttled, 5),
                (A::Desynchronized, 6),
                (A::Unknown, 7),
            ],
        );
        use BackfillState as S;
        pin(
            S::ALL,
            S::code,
            S::from_code,
            &[
                (S::Never, 0),
                (S::Queued, 1),
                (S::Running, 2),
                (S::Done, 3),
                (S::Failed, 4),
            ],
        );
        use DiscoveryRun as V;
        pin(
            V::ALL,
            V::code,
            V::from_code,
            &[
                (V::Queued, 1),
                (V::Running, 2),
                (V::Done, 3),
                (V::Failed, 4),
            ],
        );
        pin(
            JobKind::ALL,
            JobKind::code,
            JobKind::from_code,
            &[
                (JobKind::Repo, 1),
                (JobKind::ListFetch, 2),
                (JobKind::Discovery, 3),
            ],
        );
        pin(
            Tier::ALL,
            Tier::code,
            Tier::from_code,
            &[(Tier::OnDemand, 1), (Tier::Active, 2), (Tier::Sweep, 3)],
        );
        pin(
            Priority::ALL,
            Priority::code,
            Priority::from_code,
            &[(Priority::Normal, 0), (Priority::High, 1)],
        );
        use FetchOutcome as F;
        pin(
            F::ALL,
            F::code,
            F::from_code,
            &[
                (F::Ok, 1),
                (F::Failed, 2),
                (F::OwnerInactive, 3),
                (F::Cancelled, 4),
            ],
        );
        pin(
            MemberState::ALL,
            MemberState::code,
            MemberState::from_code,
            &[(MemberState::Outstanding, 1), (MemberState::Terminal, 2)],
        );
    }

    #[test]
    fn api_names_are_pinned() {
        for (c, name) in [
            (GapCause::CursorTooOld, "CursorTooOld"),
            (GapCause::Heuristic, "Heuristic"),
            (GapCause::Failover, "Failover"),
            (GapCause::SyncUnavailable, "SyncUnavailable"),
            (GapCause::SeamUnrepaired, "SeamUnrepaired"),
            (GapCause::Unreadable, "Unreadable"),
        ] {
            assert_eq!(c.api_name(), name);
            // The name is the variant's, as `getStats` has always shown it.
            assert_eq!(format!("{c:?}"), name);
        }
        assert_eq!(Protocol::V1.api_name(), "v1");
        assert_eq!(Protocol::V2.api_name(), "v2");
        for s in TrackState::ALL {
            assert_eq!(TrackState::from_api_name(s.api_name()), Some(*s));
        }
        assert_eq!(TrackState::from_api_name("Ready"), None);
        assert_eq!(TrackState::from_api_name(""), None);
    }

    #[test]
    fn tiers_index_the_scheduler_arrays() {
        for t in Tier::ALL {
            assert_eq!(Tier::from_index(t.index()), Some(*t));
            assert_eq!(t.index(), usize::try_from(t.code()).unwrap() - 1);
            assert_eq!(t.label(), t.code().to_string());
        }
        assert_eq!(Tier::from_index(3), None);
        assert!(Tier::OnDemand < Tier::Active && Tier::Active < Tier::Sweep);
        assert!(Priority::High > Priority::Normal);
    }

    #[test]
    fn cycle_sources_are_pinned() {
        use CycleSource as S;
        let want = [
            (S::RelayCollections, "relay_collections"),
            (S::RelayRepos, "relay_repos"),
            (S::Plc, "plc"),
            (S::KnownDids, "known_dids"),
        ];
        assert_eq!(S::ALL.len(), want.len());
        for (v, s) in want {
            assert_eq!(v.as_str(), s);
            assert_eq!(S::parse(s), Some(v));
            assert_eq!(v.to_string(), s);
        }
        assert_eq!(S::parse("relay"), None);
        assert_eq!(S::parse(""), None);
        // The configured sources keep the spelling of the config key.
        use farsight_core::config::SweepSource;
        for (cfg, s) in [
            (SweepSource::RelayCollections, "relay_collections"),
            (SweepSource::RelayRepos, "relay_repos"),
            (SweepSource::Plc, "plc"),
        ] {
            assert_eq!(S::from(cfg).as_str(), s);
        }
    }

    #[test]
    fn requester_keys_are_pinned() {
        use RequesterKey as K;
        for (v, s) in [
            (K::Sweep, "system:sweep"),
            (K::Repair, "system:repair"),
            (K::Firehose, "system:firehose"),
            (K::Lists, "system:lists"),
            (K::Resync, "system:resync"),
            (K::Admin, "admin"),
            (K::Token(3), "token:3"),
            (K::Token(0), "token:0"),
        ] {
            assert_eq!(v.to_string(), s);
            assert_eq!(K::parse(s), Some(v));
        }
        for bad in [
            "",
            "system:",
            "system:other",
            "token:",
            "token:x",
            "token:03",
            "token:+3",
            "token:3 ",
            "Admin",
            "token:99999999999",
        ] {
            assert_eq!(K::parse(bad), None, "{bad:?}");
        }
        // The order is the order of the stored text.
        let mut keys = [
            K::Token(9),
            K::Sweep,
            K::Token(10),
            K::Admin,
            K::Lists,
            K::Resync,
            K::Repair,
            K::Firehose,
        ];
        keys.sort();
        let mut text: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        assert_eq!(
            text,
            [
                "admin",
                "system:firehose",
                "system:lists",
                "system:repair",
                "system:resync",
                "system:sweep",
                "token:10",
                "token:9"
            ]
        );
        text.sort();
        assert_eq!(text, keys.iter().map(|k| k.to_string()).collect::<Vec<_>>());
        assert!(K::Token(1).is_token());
        assert!(!K::Admin.is_token());
    }

    #[test]
    fn sql_fragments_come_from_the_enums() {
        assert_eq!(sql::TRACKED, "(1, 2, 3, 4)");
        assert_eq!(sql::TRACK_SERVED, "(2, 3)");
        assert_eq!(sql::TRACK_UNFETCHED, "(1, 4)");
        assert_eq!(sql::TRACK_WAITING, "(1, 4, 6)");
        assert_eq!(sql::TRACK_UNINDEXED, "(1, 4, 6, 8)");
        assert_eq!(sql::TRACK_ONLY_UNAVAILABLE, "(4)");
        assert_eq!(sql::HIDDEN, "(1, 2, 3, 4)");
        assert_eq!(sql::HIDDEN_BUT_SUSPENDED, "(1, 2, 4)");
        assert_eq!(sql::HIDDEN_BUT_TAKENDOWN, "(1, 3, 4)");
        assert_eq!(sql::GONE, "(1, 4)");
        assert_eq!(sql::RUN_LISTED, "(1, 2, 3)");
        assert_eq!(sql::DEBT_UNLISTED, "(1, 2)");
        assert_eq!(sql::TRACK_PURGING, 5);
        assert_eq!(sql::JOB_LIST_FETCH, 2);
        // The sets agree with the predicates the Rust side uses.
        let list = |f: fn(TrackState) -> bool| {
            let v: Vec<String> = TrackState::ALL
                .iter()
                .filter(|s| f(**s))
                .map(|s| s.code().to_string())
                .collect();
            format!("({})", v.join(", "))
        };
        assert_eq!(sql::TRACKED, list(TrackState::is_tracked));
        assert_eq!(sql::TRACK_WAITING, list(TrackState::is_waiting));
        let hidden: Vec<String> = ActorStatus::ALL
            .iter()
            .filter(|s| s.is_hidden())
            .map(|s| s.code().to_string())
            .collect();
        assert_eq!(sql::HIDDEN, format!("({})", hidden.join(", ")));
    }

    /// Whether `line` compares or assigns a code column with a bare
    /// number, as SQL writes it (`kind = 2`, `status NOT IN (1, 4)`).
    fn bare_code(line: &str) -> bool {
        const COLUMNS: [&str; 17] = [
            "track_state",
            "record_state",
            "purge_then",
            "deferred_by",
            "job_kind",
            "kind",
            "tier",
            "priority",
            "state",
            "status",
            "last_outcome",
            "outcome",
            "reason",
            "cap_type",
            "scope",
            "protocol",
            "cause",
        ];
        let bytes = line.as_bytes();
        for col in COLUMNS {
            let mut from = 0;
            while let Some(at) = line[from..].find(col) {
                let start = from + at;
                let end = start + col.len();
                from = end;
                let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
                if start > 0 && word(bytes[start - 1]) {
                    continue;
                }
                let rest = line[end..].trim_start();
                let value = ["NOT IN", "IN", "<>", "="]
                    .iter()
                    .find_map(|op| rest.strip_prefix(op))
                    .map(|v| v.trim_start().trim_start_matches('('));
                // `==` is Rust, not SQL.
                if value.is_some_and(|v| v.starts_with(|c: char| c.is_ascii_digit())) {
                    return true;
                }
            }
        }
        false
    }

    #[test]
    fn the_scan_for_bare_codes_finds_them() {
        for hit in [
            "WHERE l.track_state IN (2, 3) AND x",
            "AND kind = 2",
            "a.status NOT IN (1, 4)",
            "SET state = 3, backfilled_at = now()",
            "c.kind <> 2",
            "WHERE tier=1",
        ] {
            assert!(bare_code(hit), "{hit}");
        }
        for miss in [
            "WHERE l.track_state IN {TRACK_SERVED}",
            "AND kind = $2",
            "if r.status == 200 {",
            "WHERE cycle_id = 1",
            "AND kind = {JOB_REPO}",
            "retry_state = 1",
        ] {
            assert!(!bare_code(miss), "{miss}");
        }
    }

    /// No SQL in the workspace's library and binary sources writes a
    /// code as a bare number: each comes from its enum, bound as a
    /// parameter or interpolated from [`sql`]. The harness binaries and
    /// unit tests are left out; they check stored values independently.
    #[test]
    fn no_sql_names_a_code_by_number() {
        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for e in std::fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    if p.file_name().is_some_and(|n| n != "bin") {
                        walk(&p, out);
                    }
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push(p);
                }
            }
        }
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_owned();
        let mut files = Vec::new();
        for e in std::fs::read_dir(&crates).unwrap() {
            let src = e.unwrap().path().join("src");
            if src.is_dir() {
                walk(&src, &mut files);
            }
        }
        assert!(files.len() > 50, "the sources were not found");
        let mut found = Vec::new();
        for f in files {
            let text = std::fs::read_to_string(&f).unwrap();
            let code = text.split("#[cfg(test)]\nmod ").next().unwrap();
            for (n, line) in code.lines().enumerate() {
                let t = line.trim();
                // Rust statements and comments are not SQL.
                if t.starts_with("//") || t.starts_with("let ") || t.ends_with(';') {
                    continue;
                }
                if bare_code(t) {
                    found.push(format!("{}:{}: {t}", f.display(), n + 1));
                }
            }
        }
        assert!(found.is_empty(), "bare codes in SQL:\n{}", found.join("\n"));
    }

    /// Every column that stores a code has a CHECK in the schema that
    /// allows exactly the codes of its enum. An enum that gains a code
    /// fails here until the schema allows it, and so does a CHECK the
    /// schema names that is not compared below.
    #[test]
    fn the_schema_checks_every_code() {
        use crate::history::Cause;
        use crate::top::Kind;
        use farsight_core::{Collection, ListPurpose};

        fn numbers(codes: impl IntoIterator<Item = i16>) -> String {
            let mut v: Vec<i16> = codes.into_iter().collect();
            v.sort_unstable();
            v.dedup();
            let v: Vec<String> = v.iter().map(i16::to_string).collect();
            v.join(", ")
        }
        fn texts<'a>(spellings: impl IntoIterator<Item = &'a str>) -> String {
            let v: Vec<String> = spellings.into_iter().map(|s| format!("'{s}'")).collect();
            v.join(", ")
        }
        macro_rules! of {
            ($e:ident) => {
                numbers($e::ALL.iter().map(|v| v.code()))
            };
        }
        let one_of = |column: &str, codes: String| format!("{column} IN ({codes})");

        // The enums whose `from_code` is written by hand know no code
        // their `ALL` leaves out.
        let collections = numbers(Collection::ALL.iter().map(|c| c.code()));
        let purposes = numbers(ListPurpose::ALL.iter().map(|p| p.code()));
        assert_eq!(
            numbers((-1..100).filter_map(|c| Collection::from_code(c).map(Collection::code))),
            collections
        );
        assert_eq!(
            numbers((-1..100).map(|c| ListPurpose::from_code(c).code())),
            purposes
        );
        // `list_deleted` is a cause of removed list memberships only.
        let causes = of!(Cause);
        let causes_of_records = numbers(
            Cause::ALL
                .iter()
                .filter(|c| **c != Cause::ListDeleted)
                .map(|c| c.code()),
        );
        // Cursors exist for repo jobs and list fetches only.
        let cursor_kinds = numbers([JobKind::Repo.code(), JobKind::ListFetch.code()]);

        let want = [
            ("actors_status_code", one_of("status", of!(ActorStatus))),
            (
                "lists_record_state_code",
                one_of("record_state", of!(RecordState)),
            ),
            (
                "lists_deferred_by_code",
                one_of("deferred_by", of!(DeferCause)),
            ),
            ("lists_purpose_code", one_of("purpose", purposes)),
            (
                "lists_track_state_code",
                one_of("track_state", of!(TrackState)),
            ),
            (
                "lists_purge_then_code",
                one_of("purge_then", of!(TrackState)),
            ),
            (
                "tombstones_collection_code",
                one_of("collection", collections.clone()),
            ),
            (
                "firehose_state_protocol_code",
                one_of("protocol", of!(Protocol)),
            ),
            (
                "firehose_cursors_protocol_code",
                one_of("protocol", of!(Protocol)),
            ),
            ("firehose_gaps_cause_code", one_of("cause", of!(GapCause))),
            (
                "firehose_seams_protocol_code",
                one_of("protocol", of!(Protocol)),
            ),
            (
                "firehose_seams_trigger_code",
                one_of("trigger", of!(SeamTrigger)),
            ),
            (
                "backfill_state_state_code",
                one_of("state", of!(BackfillState)),
            ),
            (
                "backfill_state_last_outcome_code",
                one_of("last_outcome", of!(RunOutcome)),
            ),
            (
                "backfill_cursors_collection_code",
                one_of("collection", collections.clone()),
            ),
            (
                "backfill_cursors_job_kind_code",
                one_of("job_kind", cursor_kinds),
            ),
            ("backfill_queue_kind_code", one_of("kind", of!(JobKind))),
            ("backfill_queue_tier_code", one_of("tier", of!(Tier))),
            (
                "backfill_queue_priority_code",
                one_of("priority", of!(Priority)),
            ),
            (
                "list_fetch_runs_outcome_code",
                one_of("outcome", of!(FetchOutcome)),
            ),
            (
                "discovery_state_state_code",
                one_of("state", of!(DiscoveryRun)),
            ),
            ("sweep_cycles_kind_code", one_of("kind", of!(CycleKind))),
            (
                "sweep_cycles_source_code",
                one_of("source", texts(CycleSource::ALL.iter().map(|s| s.as_str()))),
            ),
            (
                "sweep_cycles_collections_code",
                format!("collections <@ ARRAY[{collections}]::SMALLINT[]"),
            ),
            (
                "cycle_outstanding_state_code",
                one_of("state", of!(MemberState)),
            ),
            (
                "subject_coverage_scope_code",
                one_of("scope", of!(SubjectScope)),
            ),
            ("relist_debt_reason_code", one_of("reason", of!(DebtReason))),
            (
                "relist_debt_cap_type_code",
                one_of("cap_type", of!(CapType)),
            ),
            (
                "blocks_history_cause_code",
                one_of("cause", causes_of_records.clone()),
            ),
            (
                "list_blocks_history_cause_code",
                one_of("cause", causes_of_records),
            ),
            ("list_items_history_cause_code", one_of("cause", causes)),
            (
                "top_lists_kind_code",
                one_of("kind", texts(Kind::ALL.iter().map(|k| k.key()))),
            ),
        ];
        let mut schema = String::new();
        for m in crate::MIGRATOR.iter() {
            schema.push_str(&m.sql);
        }
        for (name, check) in &want {
            let line = format!("  CONSTRAINT {name} CHECK ({check})");
            assert!(schema.contains(&line), "the schema lacks `{line}`");
        }
        let named = schema
            .lines()
            .filter(|l| l.starts_with("  CONSTRAINT "))
            .count();
        assert_eq!(named, want.len(), "a CHECK is not compared with its enum");
    }

    #[test]
    fn tracked_matches_design_table() {
        let tracked: Vec<TrackState> = TrackState::ALL
            .iter()
            .copied()
            .filter(|s| s.is_tracked())
            .collect();
        assert_eq!(
            tracked,
            [
                TrackState::Pending,
                TrackState::Ready,
                TrackState::Retained,
                TrackState::Unavailable
            ]
        );
    }

    #[test]
    fn statuses() {
        assert!(ActorStatus::from_upstream(Some("takendown")).is_hidden());
        assert!(!ActorStatus::from_upstream(Some("throttled")).is_hidden());
        assert!(!ActorStatus::from_upstream(Some("weird")).is_hidden());
        assert_eq!(ActorStatus::from_upstream(None), ActorStatus::Active);
    }
}
