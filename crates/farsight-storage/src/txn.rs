//! The per-transaction context shared by every writer: advisory locks,
//! author lookup, interning with the daily intern rate, gates, debts and
//! tombstones. Everything here runs inside a caller-owned transaction.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, NaiveDate, Utc};
use farsight_core::{Collection, Did, RecordKey};
use sqlx::PgConnection;

use crate::codes::{ActorStatus, CapType, DebtReason, TrackState};
use crate::counters::{Deltas, stat};
use crate::error::Result;
use crate::ids::{ActorId, ListId, Stamp};
use crate::keys::{self, CapKind, HostFacts, Limits};

/// Global write gates, supplied by the budget monitor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Gates {
    /// Storage budget ≥ 100%: creates and updates from non-large or
    /// unresolved authors are refused.
    pub budget_refusing: bool,
    /// Hard ceiling reached: all creates and updates are refused.
    pub ceiling_refusing: bool,
}

/// Why a write (or an intern) was not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// A per-author cap or a daily rate: `capped` debt.
    Capped(CapType),
    /// A bucket cap, the budget or the ceiling: `refused` debt.
    Refused(CapType),
}

impl Refusal {
    /// The `relist_debt.reason` the refusal is recorded under: `capped` or
    /// `refused`.
    pub fn reason(self) -> DebtReason {
        match self {
            Refusal::Capped(_) => DebtReason::Capped,
            Refusal::Refused(_) => DebtReason::Refused,
        }
    }

    /// Which cap, rate or gate refused the write; stored as the debt's
    /// `cap_type`.
    pub fn cap_type(self) -> CapType {
        match self {
            Refusal::Capped(c) | Refusal::Refused(c) => c,
        }
    }
}

/// What an author (or requester) is charged against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cause {
    /// Cause key for rates: an admission key or a requester key.
    pub key: String,
    /// Buckets for host usage (empty for large hosts and requesters).
    pub buckets: Vec<String>,
    /// Exempt from bucket caps and the budget gate.
    pub large: bool,
    /// OR of the buckets' `capped_mask`.
    pub mask: i16,
}

/// A record author, as seen by this transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorInfo {
    /// `actors.id`.
    pub id: ActorId,
    /// The author's DID; the transaction caches the value under it.
    pub did: Did,
    /// The admission key the author's daily rates are charged to: the one
    /// stored on `actors`, or the one derived by [`keys::admission_key`].
    pub key: String,
    /// The `host_usage` buckets the author's stored rows count in; empty on
    /// a large host.
    pub buckets: Vec<String>,
    /// Resolved on a large host.
    pub large: bool,
    /// OR of the buckets' `capped_mask`.
    pub mask: i16,
    /// `actors.status`, as read with the row.
    pub status: ActorStatus,
}

impl AuthorInfo {
    /// The author as a charging cause.
    pub fn cause(&self) -> Cause {
        Cause {
            key: self.key.clone(),
            buckets: self.buckets.clone(),
            large: self.large,
            mask: self.mask,
        }
    }
}

/// One list tracking transition applied in a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionRecord {
    /// `lists.id`.
    pub list_id: ListId,
    /// The event that fired.
    pub event: crate::transition::Event,
    /// State before.
    pub from: TrackState,
    /// State after.
    pub to: TrackState,
    /// Effects applied.
    pub effects: Vec<crate::transition::Effect>,
}

/// What happened to one write (`farsight_firehose_events_total` outcome).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteOutcome {
    /// Changed stored state (deletes always record a tombstone).
    Applied,
    /// Lost to LWW (older or equal stamp).
    Stale,
    /// Refused by a cap, rate, gate or deletes-only mode.
    Refused,
    /// Not stored and not a refusal: a listitem whose list is untracked.
    Dropped,
}

impl WriteOutcome {
    /// The `outcome` label of `farsight_firehose_events_total`.
    pub fn label(self) -> &'static str {
        match self {
            WriteOutcome::Applied => "applied",
            WriteOutcome::Stale => "stale",
            WriteOutcome::Refused => "refused",
            WriteOutcome::Dropped => "dropped",
        }
    }
}

/// One refused or uncounted write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusalRecord {
    /// The repo the write was for.
    pub author: Did,
    /// The collection of the record that was refused or left uncounted.
    pub collection: Collection,
    /// Its record key.
    pub rkey: String,
    /// Which cap, rate or gate it met, and whether the debt is `capped` or
    /// `refused`.
    pub refusal: Refusal,
}

/// What a transaction did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyReport {
    /// Writes that changed stored state (incl. firehose deletes, which
    /// always record a tombstone).
    pub applied: u64,
    /// Upserts skipped by LWW (stale or equal stamp).
    pub stale: u64,
    /// Upserts refused by a cap, rate, gate or deletes-only mode.
    pub refused: u64,
    /// Listblocks stored uncounted (trigger cap or admission rate).
    pub uncounted: u64,
    /// Listitems refused because their list is not tracked (no debt).
    pub untracked_items: u64,
    /// Rows removed by range/whole reconcile.
    pub reconciled: u64,
    /// Debts inserted or raised.
    pub debts: u64,
    /// Refusal tombstones written (rev `E − 1`).
    pub refusal_tombstones: u64,
    /// Every refusal, for diagnostics and tests.
    pub refusals: Vec<RefusalRecord>,
    /// Tracking transitions, in order.
    pub transitions: Vec<TransitionRecord>,
    /// Aborted attempts retried before success: deadlocks, and lock sets
    /// that changed while they were being taken.
    pub deadlock_retries: u32,
    /// Per write, in batch order: what happened to it.
    pub write_outcomes: Vec<WriteOutcome>,
    /// Non-commit events applied (identity, account, sync).
    pub repo_events: u64,
    /// Account events older than the stored `status_at` (replays), skipped.
    pub stale_repo_events: u64,
    /// Unknown DIDs that became active: counted only.
    pub unknown_activations: u64,
    /// Authors whose `actors` row this transaction created.
    pub new_authors: Vec<ActorId>,
    /// `resync` debts raised by `#sync` / account events.
    pub resyncs: u64,
    /// Accounts that became `deleted`: the caller purges them after commit
    /// (multi-transaction, see `janitor::purge_account`).
    pub deleted_accounts: Vec<Did>,
    /// Whether `NOTIFY farsight_coverage` was sent.
    pub notified: bool,
}

/// Per-transaction state.
pub struct Txn<'c> {
    /// The transaction's connection.
    pub conn: &'c mut PgConnection,
    /// Limits in force.
    pub limits: &'c Limits,
    /// The budget and ceiling gates as the caller read them before the
    /// transaction began.
    pub gates: Gates,
    /// Current UTC day (database clock).
    pub today: NaiveDate,
    /// `firehose_state.applied_through` at transaction start: the witness
    /// stamped on debts caused by non-firehose work.
    pub clock_witness: Option<DateTime<Utc>>,
    /// The database's `now()` at transaction start (the witness of a
    /// listing write while the clock is still undefined).
    pub now: DateTime<Utc>,
    /// The history row written last (see `history`).
    pub(crate) last_history: Option<crate::history::Written>,
    authors: HashMap<Did, AuthorInfo>,
    /// The list locks this transaction holds; `true` = exclusive.
    held_lists: BTreeMap<i64, bool>,
    /// Counter deltas, merged into the sink only after commit.
    pub deltas: Deltas,
    /// Send `NOTIFY farsight_coverage` before commit.
    pub notify: bool,
    /// What the transaction has done so far; [`Txn::finish`] returns it.
    pub report: ApplyReport,
}

impl<'c> Txn<'c> {
    /// Starts the context: sets `READ COMMITTED` and reads the
    /// database's UTC day and the current applied-through witness.
    pub async fn start(
        conn: &'c mut PgConnection,
        limits: &'c Limits,
        gates: Gates,
    ) -> Result<Txn<'c>> {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *conn)
            .await?;
        let (today, clock_witness, now): (NaiveDate, Option<DateTime<Utc>>, DateTime<Utc>) =
            sqlx::query_as(
                "SELECT (now() AT TIME ZONE 'UTC')::date,
                    (SELECT applied_through FROM firehose_state WHERE id = 1), now()",
            )
            .fetch_one(&mut *conn)
            .await?;
        Ok(Txn {
            conn,
            limits,
            gates,
            today,
            clock_witness,
            now,
            last_history: None,
            authors: HashMap::new(),
            held_lists: BTreeMap::new(),
            deltas: Deltas::default(),
            notify: false,
            report: ApplyReport::default(),
        })
    }

    /// Ends the context, returning the report and the counter deltas.
    pub fn finish(self) -> (ApplyReport, Deltas) {
        (self.report, self.deltas)
    }

    /// Takes transaction-scoped author locks, ascending by key.
    pub async fn lock_authors(&mut self, keys: &BTreeSet<i64>) -> Result<()> {
        for k in keys {
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(*k)
                .execute(&mut *self.conn)
                .await?;
        }
        Ok(())
    }

    /// Takes list locks ascending by key; `true` = exclusive.
    pub async fn lock_lists(&mut self, keys: &BTreeMap<i64, bool>) -> Result<()> {
        for (k, exclusive) in keys {
            let sql = if *exclusive {
                "SELECT pg_advisory_xact_lock($1)"
            } else {
                "SELECT pg_advisory_xact_lock_shared($1)"
            };
            sqlx::query(sql).bind(*k).execute(&mut *self.conn).await?;
            *self.held_lists.entry(*k).or_insert(false) |= *exclusive;
        }
        Ok(())
    }

    /// Whether this transaction holds the list lock `key` exclusively.
    pub fn holds_list_exclusive(&self, key: i64) -> bool {
        self.held_lists.get(&key).copied().unwrap_or(false)
    }

    /// Takes intern locks, ascending by key, for those of `dids` that have
    /// no `actors` row yet. Every `actors` insert by `apply` then happens
    /// under its DID's intern lock, so concurrent batches never wait on each
    /// other's uncommitted inserts in the unique index. Call after the list
    /// locks.
    pub async fn lock_new_dids(&mut self, dids: &[&Did]) -> Result<()> {
        if dids.is_empty() {
            return Ok(());
        }
        let wanted: Vec<String> = dids.iter().map(|d| d.as_str().to_owned()).collect();
        let existing: Vec<String> =
            sqlx::query_scalar("SELECT did FROM actors WHERE did = ANY($1)")
                .bind(&wanted)
                .fetch_all(&mut *self.conn)
                .await?;
        let existing: BTreeSet<&str> = existing.iter().map(String::as_str).collect();
        let keys: BTreeSet<i64> = wanted
            .iter()
            .filter(|d| !existing.contains(d.as_str()))
            .map(|d| keys::intern_lock_key(d))
            .collect();
        for k in &keys {
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(*k)
                .execute(&mut *self.conn)
                .await?;
        }
        Ok(())
    }

    /// `actors.id` for a DID, without creating it.
    pub async fn actor_id(&mut self, did: &Did) -> Result<Option<ActorId>> {
        Ok(sqlx::query_scalar("SELECT id FROM actors WHERE did = $1")
            .bind(did.as_str())
            .fetch_optional(&mut *self.conn)
            .await?)
    }

    /// The author's row (created if absent, charged to its own key but
    /// never refused: a debt needs it), key, buckets and mask. Cached
    /// for the transaction; the caller must hold author(did).
    pub async fn author(&mut self, did: &Did) -> Result<AuthorInfo> {
        if let Some(a) = self.authors.get(did) {
            return Ok(a.clone());
        }
        let row: Option<AuthorRow> = sqlx::query_as(
            "SELECT a.id, a.admission_key, a.resolve_failures, a.pds_host_id IS NOT NULL,
                        h.cap_key, h.ip_bucket, COALESCE(h.large, false), a.status
                 FROM actors a LEFT JOIN pds_hosts h ON h.id = a.pds_host_id
                 WHERE a.did = $1",
        )
        .bind(did.as_str())
        .fetch_optional(&mut *self.conn)
        .await?;
        let (id, facts, status) = match row {
            Some((
                id,
                admission_key,
                resolve_failures,
                resolved,
                cap_key,
                ip_bucket,
                large,
                status,
            )) => (
                id,
                HostFacts {
                    admission_key,
                    resolve_failures,
                    cap_key,
                    ip_bucket,
                    large,
                    resolved,
                },
                status,
            ),
            None => {
                let facts = HostFacts::default();
                let key = keys::admission_key(did, &facts);
                let buckets = keys::buckets(did, &facts);
                // Charged, never refused.
                self.charge_intern(&key, None).await?;
                let id = self.insert_actor(did).await?;
                self.deltas.host(&buckets, CapKind::Interned, 1);
                self.report.new_authors.push(id);
                (id, facts, crate::codes::ActorStatus::Active)
            }
        };
        let key = keys::admission_key(did, &facts);
        let buckets = keys::buckets(did, &facts);
        let large = keys::is_large(&facts);
        let mask = self.bucket_mask(&buckets).await?;
        let info = AuthorInfo {
            id,
            did: did.clone(),
            key,
            buckets,
            large,
            mask,
            status,
        };
        self.authors.insert(did.clone(), info.clone());
        Ok(info)
    }

    async fn insert_actor(&mut self, did: &Did) -> Result<ActorId> {
        let inserted: Option<ActorId> = sqlx::query_scalar(
            "INSERT INTO actors (did) VALUES ($1) ON CONFLICT (did) DO NOTHING RETURNING id",
        )
        .bind(did.as_str())
        .fetch_optional(&mut *self.conn)
        .await?;
        match inserted {
            Some(id) => {
                self.deltas.stat(stat::ACTORS, 1);
                Ok(id)
            }
            None => Ok(sqlx::query_scalar("SELECT id FROM actors WHERE did = $1")
                .bind(did.as_str())
                .fetch_one(&mut *self.conn)
                .await?),
        }
    }

    /// OR of `host_usage.capped_mask` over `buckets`.
    pub async fn bucket_mask(&mut self, buckets: &[String]) -> Result<i16> {
        if buckets.is_empty() {
            return Ok(0);
        }
        let mask: i16 = sqlx::query_scalar(
            "SELECT COALESCE(bit_or(capped_mask), 0)::SMALLINT FROM host_usage
             WHERE bucket = ANY($1)",
        )
        .bind(buckets)
        .fetch_one(&mut *self.conn)
        .await?;
        Ok(mask)
    }

    /// Charges one intern to `key` for today. With `limit = Some(n)` the
    /// charge succeeds only while fewer than `n` were charged today;
    /// `None` charges unconditionally. Charges are never refunded.
    pub async fn charge_intern(&mut self, key: &str, limit: Option<i64>) -> Result<bool> {
        match limit {
            Some(n) if n <= 0 => Ok(false),
            Some(n) => {
                let r: Option<i64> = sqlx::query_scalar(
                    "INSERT INTO intern_rate (key, utc_day, n) VALUES ($1, $2, 1)
                     ON CONFLICT (key, utc_day) DO UPDATE SET n = intern_rate.n + 1
                       WHERE intern_rate.n < $3
                     RETURNING n",
                )
                .bind(key)
                .bind(self.today)
                .bind(n)
                .fetch_optional(&mut *self.conn)
                .await?;
                Ok(r.is_some())
            }
            None => {
                sqlx::query(
                    "INSERT INTO intern_rate (key, utc_day, n) VALUES ($1, $2, 1)
                     ON CONFLICT (key, utc_day) DO UPDATE SET n = intern_rate.n + 1",
                )
                .bind(key)
                .bind(self.today)
                .execute(&mut *self.conn)
                .await?;
                Ok(true)
            }
        }
    }

    /// Charges one admission to `key` for today, if under its daily limit.
    /// Exact: runs inside the apply transaction.
    pub async fn charge_admission(&mut self, key: &str) -> Result<bool> {
        let limit = self.limits.admission_limit(key);
        if limit <= 0 {
            return Ok(false);
        }
        let r: Option<i32> = sqlx::query_scalar(
            "INSERT INTO admission_rate (key, utc_day, admissions) VALUES ($1, $2, 1)
             ON CONFLICT (key, utc_day) DO UPDATE SET admissions = admission_rate.admissions + 1
               WHERE admission_rate.admissions < $3
             RETURNING admissions",
        )
        .bind(key)
        .bind(self.today)
        .bind(limit)
        .fetch_optional(&mut *self.conn)
        .await?;
        Ok(r.is_some())
    }

    /// Interns a subject / member / owner DID, charged to `cause`: the
    /// lifetime bound of a non-large cause bucket, then the daily rate.
    pub async fn intern_actor(
        &mut self,
        did: &Did,
        cause: &Cause,
    ) -> Result<Result<ActorId, Refusal>> {
        if let Some(a) = self.authors.get(did) {
            return Ok(Ok(a.id));
        }
        if let Some(id) = self.actor_id(did).await? {
            return Ok(Ok(id));
        }
        if !cause.large && cause.mask & CapKind::Interned.bit() != 0 {
            return Ok(Err(Refusal::Refused(CapType::InternLifetime)));
        }
        let limit = self.limits.intern_limit(&cause.key);
        if !self.charge_intern(&cause.key, Some(limit)).await? {
            return Ok(Err(Refusal::Capped(CapType::InternRate)));
        }
        let id = self.insert_actor(did).await?;
        self.deltas.host(&cause.buckets, CapKind::Interned, 1);
        Ok(Ok(id))
    }

    /// Interns the `lists` row for `(owner, rkey)`, creating a placeholder
    /// (`record_state = unknown`) charged to `cause` if absent. The caller
    /// must hold list(L) exclusive (the placeholder cleanup takes the same
    /// lock).
    pub async fn intern_list(
        &mut self,
        owner: &Did,
        rkey: &RecordKey,
        cause: &Cause,
    ) -> Result<Result<ListId, Refusal>> {
        let owner_id = match self.intern_actor(owner, cause).await? {
            Ok(id) => id,
            Err(r) => return Ok(Err(r)),
        };
        let existing: Option<ListId> =
            sqlx::query_scalar("SELECT id FROM lists WHERE owner_id = $1 AND rkey = $2")
                .bind(owner_id)
                .bind(rkey.as_str())
                .fetch_optional(&mut *self.conn)
                .await?;
        if let Some(id) = existing {
            return Ok(Ok(id));
        }
        if !cause.large && cause.mask & CapKind::Interned.bit() != 0 {
            return Ok(Err(Refusal::Refused(CapType::InternLifetime)));
        }
        let limit = self.limits.intern_limit(&cause.key);
        if !self.charge_intern(&cause.key, Some(limit)).await? {
            return Ok(Err(Refusal::Capped(CapType::InternRate)));
        }
        let id: ListId = sqlx::query_scalar(
            "INSERT INTO lists (owner_id, rkey) VALUES ($1, $2)
             ON CONFLICT (owner_id, rkey) DO UPDATE SET rkey = EXCLUDED.rkey
             RETURNING id",
        )
        .bind(owner_id)
        .bind(rkey.as_str())
        .fetch_one(&mut *self.conn)
        .await?;
        // Placeholder rows are charged to the writer's bucket
        // (`stored_listblocks` includes placeholder rows).
        self.deltas.host(&cause.buckets, CapKind::Listblocks, 1);
        self.deltas.host(&cause.buckets, CapKind::Interned, 1);
        Ok(Ok(id))
    }

    /// Global and bucket gates for a create/update of `kind`.
    pub fn gate(&self, cause: &Cause, kind: CapKind) -> Option<Refusal> {
        if self.gates.ceiling_refusing {
            return Some(Refusal::Refused(CapType::Ceiling));
        }
        if cause.large {
            return None;
        }
        if self.gates.budget_refusing {
            return Some(Refusal::Refused(CapType::Budget));
        }
        if cause.mask & kind.bit() != 0 {
            let cap = match kind {
                CapKind::Blocks => CapType::HostBlocks,
                CapKind::Items => CapType::HostListItems,
                CapKind::Listblocks => CapType::HostListblocks,
                CapKind::Lists => CapType::HostLists,
                CapKind::Interned => CapType::InternLifetime,
            };
            return Some(Refusal::Refused(cap));
        }
        None
    }

    /// Inserts a debt or raises its `since_witness`.
    pub async fn add_debt(
        &mut self,
        actor_id: ActorId,
        reason: DebtReason,
        cap_type: Option<CapType>,
        witness: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let since = witness
            .or(self.clock_witness)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        sqlx::query(
            "INSERT INTO relist_debt (actor_id, reason, cap_type, since_witness)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (actor_id, reason) DO UPDATE
               SET since_witness = GREATEST(relist_debt.since_witness, EXCLUDED.since_witness),
                   cap_type = COALESCE(EXCLUDED.cap_type, relist_debt.cap_type)",
        )
        .bind(actor_id)
        .bind(reason.code())
        .bind(cap_type.map(CapType::code))
        .bind(since)
        .execute(&mut *self.conn)
        .await?;
        self.report.debts += 1;
        self.notify = true;
        Ok(())
    }

    /// Records a refusal: debt plus report entry.
    pub async fn refuse(
        &mut self,
        author: &AuthorInfo,
        collection: Collection,
        rkey: &RecordKey,
        refusal: Refusal,
        witness: Option<DateTime<Utc>>,
    ) -> Result<()> {
        self.add_debt(
            author.id,
            refusal.reason(),
            Some(refusal.cap_type()),
            witness,
        )
        .await?;
        self.report.refused += 1;
        self.report.refusals.push(RefusalRecord {
            author: author.did.clone(),
            collection,
            rkey: rkey.as_str().to_owned(),
            refusal,
        });
        Ok(())
    }

    /// The tombstone rev for a key, if any.
    pub async fn tombstone_rev(
        &mut self,
        collection: Collection,
        author_id: ActorId,
        rkey: &RecordKey,
    ) -> Result<Option<Stamp>> {
        Ok(sqlx::query_scalar(
            "SELECT rev FROM tombstones WHERE collection = $1 AND author_id = $2 AND rkey = $3",
        )
        .bind(collection.code())
        .bind(author_id)
        .bind(rkey.as_str())
        .fetch_optional(&mut *self.conn)
        .await?)
    }

    /// Upserts a tombstone with `rev = max(existing, rev)`; the TTL
    /// clock restarts only when the rev rises.
    pub async fn put_tombstone(
        &mut self,
        collection: Collection,
        author_id: ActorId,
        rkey: &RecordKey,
        rev: Stamp,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO tombstones (collection, author_id, rkey, rev) VALUES ($1, $2, $3, $4)
             ON CONFLICT (collection, author_id, rkey) DO UPDATE SET
               deleted_at = CASE WHEN EXCLUDED.rev > tombstones.rev THEN now()
                                 ELSE tombstones.deleted_at END,
               rev = GREATEST(tombstones.rev, EXCLUDED.rev)",
        )
        .bind(collection.code())
        .bind(author_id)
        .bind(rkey.as_str())
        .bind(rev)
        .execute(&mut *self.conn)
        .await?;
        Ok(())
    }

    /// Writes a refusal tombstone for an update at rev `e`: rev `e − 1`,
    /// so listings stamped `R < e` are refused and `R ≥ e` apply.
    pub async fn put_refusal_tombstone(
        &mut self,
        collection: Collection,
        author_id: ActorId,
        rkey: &RecordKey,
        e: Stamp,
    ) -> Result<()> {
        self.put_tombstone(collection, author_id, rkey, e.pred())
            .await?;
        self.report.refusal_tombstones += 1;
        Ok(())
    }

    /// Queues `NOTIFY farsight_coverage` (delivered at commit).
    pub async fn send_notify(&mut self) -> Result<()> {
        sqlx::query("SELECT pg_notify('farsight_coverage', '')")
            .execute(&mut *self.conn)
            .await?;
        self.report.notified = true;
        Ok(())
    }
}

/// `actors` + `pds_hosts` columns read by [`Txn::author`].
type AuthorRow = (
    ActorId,
    Option<String>,
    i32,
    bool,
    Option<String>,
    Option<String>,
    bool,
    ActorStatus,
);

/// The LWW upsert rule: apply iff `w` beats the stored row's rev and
/// the tombstone's rev (absent ones pass). Equal ⇒ skip.
pub fn lww_upsert_wins(w: Stamp, row_rev: Option<Stamp>, tombstone_rev: Option<Stamp>) -> bool {
    row_rev.is_none_or(|r| w > r) && tombstone_rev.is_none_or(|t| w > t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lww_rule() {
        let s = Stamp::new;
        assert!(lww_upsert_wins(s(5), None, None));
        assert!(lww_upsert_wins(s(5), Some(s(4)), Some(s(4))));
        assert!(!lww_upsert_wins(s(5), Some(s(5)), None));
        assert!(!lww_upsert_wins(s(5), None, Some(s(5))));
        assert!(!lww_upsert_wins(s(5), Some(s(6)), None));
        // Refusal tombstone at E − 1: R = E applies, R = E − 1 does not.
        let e = s(100);
        assert!(lww_upsert_wins(e, None, Some(e.pred())));
        assert!(!lww_upsert_wins(e.pred(), None, Some(e.pred())));
    }

    #[test]
    fn refusal_mapping() {
        assert_eq!(
            Refusal::Capped(CapType::InternRate).reason(),
            DebtReason::Capped
        );
        assert_eq!(
            Refusal::Refused(CapType::Budget).reason(),
            DebtReason::Refused
        );
    }
}
