//! Typed row ids and the last-write-wins stamp.
//!
//! Every value here is stored in a `BIGINT` (or `INT`) column, and
//! several travel together through one signature: an actor and a list, a
//! cycle and a queue entry, a run and its owner, a stamp and an author.
//! Each kind is its own type, so handing one where another is expected
//! does not compile. None converts from or compares with a bare integer:
//! [`new`](ActorId::new) and [`get`](ActorId::get) are the only way in and
//! out, and they are meant for the SQL boundary, for log fields and for
//! tests.
//!
//! The types are transparent to sqlx: they bind and decode as their
//! integer, alone, in a row tuple, or as an array for `= ANY($1)`.

use std::fmt;

use farsight_core::Tid;

macro_rules! id_type {
    ($(#[$m:meta])* $name:ident, $repr:ty) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, sqlx::Type)]
        #[sqlx(transparent)]
        pub struct $name($repr);

        impl $name {
            /// Wraps a stored value.
            pub const fn new(value: $repr) -> $name {
                $name(value)
            }

            /// The stored value.
            pub const fn get(self) -> $repr {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

id_type!(
    /// `actors.id`: one DID, interned once and never deleted. Every other
    /// table names an actor by this id under a role-specific column:
    /// `author_id`, `subject_id`, `owner_id`, `list_owner_id`, `actor_id`.
    ActorId,
    i64
);

id_type!(
    /// `lists.id`: one list, the pair (owner, rkey). The row may be a
    /// placeholder for a list only named by a listblock, and a placeholder
    /// nothing refers to any more is deleted, so the id is not stable
    /// across a list's whole life the way an [`ActorId`] is. `list_id`
    /// columns, `list_jobs` and `list_sched_keys` hold it.
    ListId,
    i64
);

id_type!(
    /// `sweep_cycles.id`: one full or repair sweep. Queue entries and
    /// `cycle_outstanding` rows name the cycle they belong to by it, and a
    /// repaired firehose gap names the cycle that repaired it.
    CycleId,
    i64
);

id_type!(
    /// `pds_hosts.id`: one PDS host name. `actors.pds_host_id` holds it.
    /// The column is `INT`, unlike the other ids.
    HostId,
    i32
);

id_type!(
    /// `list_fetch_runs.id`: one phase-2 run over an owner's listitems.
    /// `lists.fetch_run_id` names the run a list is being fetched by, and
    /// the run's own `backfill_cursors` rows carry it as their `run_id`.
    RunId,
    i64
);

id_type!(
    /// The id a repo job gives itself for one run: a random non-negative
    /// number drawn when the job starts, not a row of any table.
    /// `backfill_state.current_run_id` holds the current one, and the
    /// job's `backfill_cursors` rows carry it as their `run_id`, so a
    /// later job never adopts an earlier job's cursor.
    RepoRunId,
    i64
);

id_type!(
    /// `firehose_gaps.id`: one interval the firehose reader did not
    /// witness.
    GapId,
    i64
);

id_type!(
    /// `backfill_queue.id`: one waiting job. The row is deleted when the
    /// job is picked, so the id lives only as long as the wait.
    QueueId,
    i64
);

id_type!(
    /// The id of a row in one of the three history tables
    /// (`blocks_history`, `list_blocks_history`, `list_items_history`).
    /// Each table numbers its own rows, so the id means something only
    /// together with the table it came from.
    HistoryId,
    i64
);

id_type!(
    /// `op_errors.id`: one logged operational error. Ids rise with time,
    /// so `admin.listErrors` pages backwards by it.
    OpErrorId,
    i64
);

id_type!(
    /// The value last-write-wins orders by, stored in every `rev` column
    /// and in `tombstones.rev`.
    ///
    /// A firehose write carries its commit's rev, a listing write the rev
    /// the repo had when the listing began, and both are a TID read as
    /// its 63-bit integer ([`Stamp::from_tid`]): microseconds since the
    /// epoch in the high bits, the clock id in the low ten. Integer order
    /// is therefore TID order, and a later commit of one repo has a
    /// greater stamp. A discovery write carries [`Stamp::ZERO`], which
    /// loses to every real one.
    ///
    /// An upsert applies only if its stamp is strictly greater than both
    /// the stored row's and the key's tombstone's; an equal stamp is
    /// skipped (see [`lww_upsert_wins`](crate::txn::lww_upsert_wins)).
    Stamp,
    i64
);

impl Stamp {
    /// The stamp of a discovery write: lower than any TID's.
    pub const ZERO: Stamp = Stamp(0);

    /// The stamp of a commit rev or a listing rev.
    pub fn from_tid(tid: Tid) -> Stamp {
        Stamp(tid.as_i64())
    }

    /// The stamp just below this one. A refusal tombstone for an update
    /// at `E` is written at `E − 1`: a listing stamped below `E` loses to
    /// it and one stamped `E` or later wins.
    pub const fn pred(self) -> Stamp {
        Stamp(self.0 - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        assert_eq!(ActorId::new(7).get(), 7);
        assert_eq!(ListId::new(i64::MAX).get(), i64::MAX);
        assert_eq!(HostId::new(3).get(), 3_i32);
        assert_eq!(Stamp::new(-1).get(), -1);
        assert_eq!(CycleId::new(9), CycleId::new(9));
        assert_ne!(RunId::new(1), RunId::new(2));
    }

    #[test]
    fn display_prints_the_number() {
        assert_eq!(ActorId::new(42).to_string(), "42");
        assert_eq!(ListId::new(-5).to_string(), "-5");
        assert_eq!(HostId::new(12).to_string(), "12");
        assert_eq!(GapId::new(0).to_string(), "0");
        assert_eq!(QueueId::new(1_000_000).to_string(), "1000000");
        assert_eq!(format!("{:>5}", HistoryId::new(3)), "    3");
        assert_eq!(Stamp::new(3_222_222_222_222).to_string(), "3222222222222");
    }

    #[test]
    fn stamp_order_is_integer_order() {
        assert!(Stamp::new(5) > Stamp::new(4));
        assert!(Stamp::new(4) < Stamp::new(5));
        assert!(Stamp::ZERO < Stamp::new(1));
        assert_eq!(Stamp::new(5).max(Stamp::new(9)), Stamp::new(9));
        let mut v = vec![Stamp::new(3), Stamp::ZERO, Stamp::new(2)];
        v.sort();
        assert_eq!(v, [Stamp::ZERO, Stamp::new(2), Stamp::new(3)]);
        // A missing stored rev orders below every stamp.
        assert!(None < Some(Stamp::ZERO));
    }

    #[test]
    fn stamp_follows_tid_order() {
        let early = Tid::from_parts(1_700_000_000_000_000, 3).map(Stamp::from_tid);
        let late = Tid::from_parts(1_700_000_000_000_001, 0).map(Stamp::from_tid);
        assert!(early.is_some() && early < late);
        let tid = Tid::from_parts(1_700_000_000_000_000, 3);
        assert_eq!(tid.map(|t| Stamp::from_tid(t).get()), tid.map(Tid::as_i64));
    }

    #[test]
    fn pred_is_one_below() {
        assert_eq!(Stamp::new(100).pred(), Stamp::new(99));
        assert!(Stamp::new(100).pred() < Stamp::new(100));
    }
}
