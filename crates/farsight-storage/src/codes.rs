//! Numeric codes of the SMALLINT enum columns (see
//! `docs/design/storage.md`).

macro_rules! code_enum {
    ($(#[$m:meta])* $name:ident { $($(#[$vm:meta])* $var:ident = $val:literal),+ $(,)? }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name { $($(#[$vm])* $var),+ }

        impl $name {
            /// All variants.
            pub const ALL: &'static [$name] = &[$($name::$var),+];

            /// The stored code.
            pub fn code(self) -> i16 {
                match self { $($name::$var => $val),+ }
            }

            /// Decodes a stored code.
            pub fn from_code(code: i16) -> Option<$name> {
                match code { $($val => Some($name::$var),)+ _ => None }
            }
        }
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
        /// Hard ceiling.
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
        /// Global storage budget.
        Budget = 12,
        /// Hard ceiling.
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

code_enum! {
    /// `backfill_state.last_outcome`.
    RunOutcome {
        /// Everything listed, nothing refused, account active.
        Clean = 1,
        /// Listed to the end; shortfalls recorded as debts.
        CompleteWithDebts = 2,
        /// Relay-confirmed inactive.
        Inactive = 3,
        /// Error.
        Failed = 4,
    }
}

code_enum! {
    /// `sweep_cycles.kind`.
    CycleKind {
        /// Full sweep.
        Full = 1,
        /// Repair cycle.
        Repair = 2,
    }
}

code_enum! {
    /// `subject_coverage.scope`.
    SubjectScope {
        /// Direct blocks.
        Block = 1,
        /// listitem → list → listblock chain.
        ListChain = 2,
    }
}

/// `actors.status` codes.
pub mod actor_status {
    /// `active`.
    pub const ACTIVE: i16 = 0;
    /// `deactivated` (hidden).
    pub const DEACTIVATED: i16 = 1;
    /// `takendown` (hidden).
    pub const TAKENDOWN: i16 = 2;
    /// `suspended` (hidden).
    pub const SUSPENDED: i16 = 3;
    /// `deleted` (hidden, purged).
    pub const DELETED: i16 = 4;
    /// `throttled` (shown).
    pub const THROTTLED: i16 = 5;
    /// `desynchronized` (shown, re-listed).
    pub const DESYNCHRONIZED: i16 = 6;
    /// Any other upstream value (shown).
    pub const UNKNOWN: i16 = 7;

    /// Hidden statuses: rows excluded unless `includeInactive`.
    pub fn is_hidden(code: i16) -> bool {
        matches!(code, DEACTIVATED | TAKENDOWN | SUSPENDED | DELETED)
    }

    /// Maps an upstream status string (`None` = active).
    pub fn from_upstream(status: Option<&str>) -> i16 {
        match status {
            None | Some("active") => ACTIVE,
            Some("deactivated") => DEACTIVATED,
            Some("takendown") => TAKENDOWN,
            Some("suspended") => SUSPENDED,
            Some("deleted") => DELETED,
            Some("throttled") => THROTTLED,
            Some("desynchronized") => DESYNCHRONIZED,
            Some(_) => UNKNOWN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_round_trip() {
        for s in TrackState::ALL {
            assert_eq!(TrackState::from_code(s.code()), Some(*s));
        }
        for c in CapType::ALL {
            assert_eq!(CapType::from_code(c.code()), Some(*c));
        }
        assert_eq!(TrackState::from_code(99), None);
        assert_eq!(TrackState::Untracked.code(), 0);
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
        assert!(actor_status::is_hidden(actor_status::from_upstream(Some(
            "takendown"
        ))));
        assert!(!actor_status::is_hidden(actor_status::from_upstream(Some(
            "throttled"
        ))));
        assert!(!actor_status::is_hidden(actor_status::from_upstream(Some(
            "weird"
        ))));
        assert_eq!(actor_status::from_upstream(None), actor_status::ACTIVE);
    }
}
