# Two-stage indexing of lists

Farsight stores `app.bsky.graph.listitem` records only for lists that
at least one `app.bsky.graph.listblock` targets. Most lists on the
network are never used as block lists, and their members are of no use
to a block index; storing them would multiply the size of the database
for nothing. A list is therefore indexed in two stages: a listblock
*admits* the list, and a fetch of the owner's repository then fills in
its members.

This page defines the state of a list, the counter that drives it, the
locks that order concurrent writers, the transition function, and the
caps on list size. The rules here are normative: the storage layer
implements the transition function as a pure function with one match
arm per cell of the table below.

## List state

Every list Farsight knows has a row in `lists`. The column
`lists.track_state` (`SMALLINT`) holds one of nine states. This table
is the single definition of the states and of "tracked"; the
[transition function](#the-transition-function) is the single
definition of how a list moves between them.

| Code | State | Tracked | Meaning | Items | Listitem writes |
|---|---|---|---|---|---|
| 0 | `untracked` | no | no counted listblock | none | refused |
| 1 | `pending` | yes | admitted in the current epoch; not promoted yet | filling | applied |
| 2 | `ready` | yes | promoted by a completed fetch run (`fetched_at` set) | yes | applied |
| 3 | `retained` | yes | was `ready`, lost its last counted listblock, within grace | yes | applied |
| 4 | `unavailable` | yes | fetch errors exhausted, `limits.pending_max_age` exceeded, or owner inactive; retrying | partial | applied |
| 5 | `purging` | no | items being deleted; `purge_then` is the target | draining | refused |
| 6 | `missing` | no | counted listblocks exist; the list record was not found; retrying | none | refused |
| 7 | `dead` | no | record known deleted, or not found through all retries | none | refused |
| 8 | `deferred` | no | admission refused by a gate (`deferred_by` says which) | none | refused |

A list with no row is `untracked`. The API reports the state by name
in `getListMembers`; a list that is `purging` is reported as `pending`
when the purge will end in a re-admission (`purge_then = untracked`
and `listblock_count > 0`), otherwise as `untracked`.

Columns beside the state:

| Column | Meaning |
|---|---|
| `listblock_count` | the counter (next section) |
| `record_state` | whether the `app.bsky.graph.list` record is known: `unknown` (0, a placeholder row), `present` (1), `deleted` (2) |
| `capped` | the stored item set is cut by a cap |
| `refresh_requested` | a refresh run is wanted for a `ready` or `retained` list |
| `admit_epoch` | incremented on every admission; results of work begun in an earlier epoch are discarded |
| `admitted_at` | time of the current admission |
| `phase1_epoch` | the epoch for which the record check and gates last ran |
| `fetch_run_id`, `fetch_run_epoch` | the claim of the fetch run working on the list |
| `fetched_at`, `fetched_witness` | when the promoting run completed, and its coverage point |
| `retain_until` | end of grace for a `retained` list |
| `purge_then` | target state after a purge |
| `deferred_by` | the gate that deferred the list |
| `next_retry_at` | next retry for `missing` and `unavailable` lists, and for lists deferred by the owner's re-admission budget; for a list deferred by a host cap or the lists cap, the time before which it is not looked at again (an hour after it was deferred) |
| `item_count` | stored items |

`deferred_by` codes:

| Code | Cause |
|---|---|
| 1 | storage budget at or over 100% |
| 2 | hard storage ceiling |
| 3 | the owner's host bucket is over `limits.host_list_items` |
| 4 | the found list record was refused by `limits.lists_per_author` or `limits.host_lists` (a found record that the budget or the ceiling refuses is deferred with code 1 or 2) |
| 5 | the owner's re-admission budget |

The list jobs that perform the record check (phase 1) and the fetch
runs are described in [backfill.md](backfill.md#list-jobs); the gates
and host buckets in [security.md](security.md#aggregate-bounds).

## The listblock counter

`lists.listblock_count` is the exact number of **counted** live
`list_blocks` rows targeting the list. The events **+** (0 → ≥ 1) and
**−** (≥ 1 → 0) of the transition function are its edges.

A `list_blocks` row is counted unless its author was over a limit when
the row was written (`list_blocks.counted = false`). Uncounted rows
are stored, and are returned by queries for lists that are tracked for
other reasons, but can never admit a list. This charges the excess of
an abusive author to that author (the exception `cappedAuthors`, see
[coverage.md](coverage.md)), not to the list.

Rules:

- Increment only when an
  `INSERT … ON CONFLICT DO NOTHING RETURNING` inserted a counted row;
  decrement only when a `DELETE … RETURNING` removed one.
- A listblock update that changes the subject decrements the old list
  and increments the new one in the same transaction.
- **Every** path that deletes `list_blocks` rows (firehose delete,
  range or whole reconcile, account purge) uses one function. It
  deletes a set of one author's rows with one statement and then
  applies the count change list by list. That
  function also writes `list_blocks_history` (see
  [history.md](history.md#when-a-row-is-written)): its caller names the cause of the
  removal, and the purges name none. Counted and uncounted rows are
  recorded alike.

### When a row is counted

`actors.fetch_triggers` is the number of the author's live counted
listblocks. On insert, the row is counted if and only if

1. `fetch_triggers < limits.listblock_fetch_triggers_per_author`
   (5,000), **and**
2. if counting it would admit the list, the author's admission key
   still has daily rate left in
   `admission_rate(key, utc_day, admissions)`. The rate is then
   incremented in the same transaction. Counting would admit the list
   when `listblock_count` is 0 and the list is `untracked` with a
   record not known deleted, or `purging` towards a state other than
   `dead`.

The trigger cap is checked first. A row that fails either condition
is stored uncounted and the author gets a `capped` re-list debt
naming the cap or rate that applied. Listblocks on lists that are
already tracked never consume rate. The daily limit depends on the kind of admission
key: `limits.bucket_admissions_per_day` (20,000) for a key that is a
host bucket, `limits.did_admissions_per_day` (200) for a key that is a
DID, which includes the key `unresolved:<did>` of a `did:plc` author
whose host is not resolved. Keys are defined in
[security.md](security.md#admission-keys-and-daily-rates).

The flag is **sticky per (author, rkey)**. Re-lists and rewrites of
the same record with the same subject keep the stored value, so no
rewrite can flip a counted row and cause a spurious 1 → 0. Deleting a
counted row decrements `fetch_triggers`.

A **subject-changing update** is a delete of the old row plus a new
insert for these rules: `counted` and the rate charge are decided
afresh for the new subject, `witnessed_at` is stamped as for an insert
(the event's witness time, or NULL for a listing), and `sched_key` is
set to the author's current key. The removal of the old row is
recorded in history.

If the new version of a **subject-changing** update is **refused** (by
`limits.listblocks_per_author`, `limits.host_listblocks`, the intern
rate, the storage budget or the hard ceiling; exceeding the trigger
cap or the admission rate stores the row uncounted, which is not a
refusal), the old row is deleted and a refusal tombstone with rev
`E − 1` is written, where `E` is the rev of the update. Farsight
therefore never keeps reporting the superseded target, and a later
listing at rev ≥ `E` can still apply the new version (tombstones are
in [storage.md](storage.md#tombstones)).

An update that names the **same** target and is refused by a gate (a
closed host bucket, the budget, the ceiling) leaves the stored row as
it is. The row still says what the repository says: the target did
not change, only its `createdAt` and rev are not brought up to date.
The refusal is counted and recorded as a `refused` debt like any
other. The same holds for a block that names the same subject and a
listitem that names the same list and subject.

There is one exception to stickiness. A clean run of an author who
holds a `capped` debt re-evaluates that author's uncounted rows in
rkey order, under the author lock and the exclusive locks of the lists
concerned, by the same two conditions as an insert. A row that passes
is flipped to counted, its `sched_key` set to the author's current
key, and the list's counter incremented. The re-evaluation stops at
the trigger cap; when the admission rate is exhausted it goes on, since rows on
lists already tracked need no rate. The debt is cleared only if no
uncounted row remains. The flip goes from uncounted to counted only,
so it cannot cause a spurious 1 → 0.

### In responses

`listblockCount` counts counted rows only. `getIncomingListBlocks`
returns every stored listblock on a tracked list, counted or not: an
uncounted listblock is still a real block.

### Nightly recount

Once a day a task recounts `listblock_count` and `item_count` exactly,
in batches of 1,000 lists, each list under its exclusive list lock. A
drifted counter is repaired; where the repair takes `listblock_count`
across zero, **+** or **−** is fired on the list. Any drift is
recorded as an operational error.

## Locks

Writers are ordered by PostgreSQL transaction-scoped advisory locks
(`pg_advisory_xact_lock`, `pg_advisory_xact_lock_shared`). There are
three classes of key:

| Class | Key |
|---|---|
| author | `hash64("a:" \|\| did)` |
| list | `hash64("l:" \|\| owner_did \|\| "/" \|\| rkey)` |
| intern | `hash64("i:" \|\| did)` |

`hash64` is the first 8 bytes of SHA-256, big-endian, as a signed
64-bit integer. It is stable across processes and builds, which the
protocol requires: the server and the backfill process must derive the
same key.

### Lock order

**A transaction takes author locks first, in ascending key order, then
list locks in ascending key order, then intern locks in ascending key
order.** The order is global; every writer in both processes follows
it.

Intern locks are taken for every DID the transaction may create an
`actors` row for (authors, subjects, members, list owners) and that
has no row yet. Every insert into `actors` happens under the intern
lock of its DID, so two transactions never wait on each other's
uncommitted inserts into the unique index on `actors.did`.

### Finding the lock set

Creates and updates carry their targets in the record, so their list
keys are known before the transaction starts. Deletes,
subject-changing updates, reconciles and purges learn their list keys
from stored rows: they take the author locks, read the affected rows
(which are stable, since every writer of those rows holds the same
author lock), then take the sorted list locks. A batch takes the
union. This discovery only reads: an author without an `actors` row
has no stored rows. After the list locks, the transaction checks which
of its DIDs have no `actors` row and takes their intern locks, sorted.

One lock set is not stable under the author lock: the `unavailable`
lists of an account that an `account` event reactivates, on which
**OA** fires. A list turns `unavailable` under its list lock alone (a
list job's timeout), so one can do so after the batch read the set
and before it holds its list locks. The batch therefore reads the set
again once the list locks are held. If a list in it is not locked,
the transaction is rolled back and run again from the start, like
after a deadlock abort, so the list is locked in its place in the
order. **OA** itself fires only on lists whose lock the transaction
holds; a list that turns `unavailable` later still is ordered after
the batch and keeps its own retry.

### Locks per write

| Write | Locks |
|---|---|
| listblock create, update or delete by A on L | author(A); list(L) exclusive (both lists on a subject change) |
| listitem create, update or delete by O in L | author(O); list(L) shared (both lists on a list change) |
| `list` record write by O for L | author(O); list(L) exclusive |
| list-job record check | author(O); list(L) exclusive |
| a list event fired on one list outside a write (promotion, a gate closing or reopening, a retry) | list(L) exclusive |
| purge batch of L | author(O); list(L) exclusive |
| reactivation of O (`account` event, active) | author(O); list(L) exclusive for the `unavailable` lists of O, the 500 with the lowest ids (the others keep their own retry) |
| account purge of D | author(D); then, per batch, the list locks it touches, at most 500 |
| claim of a fetch run for O | list(L) exclusive for the lists it claims, at most 500 |
| discovery write of `subject_lists` for (X, L) | list(L) shared |
| placeholder-list cleanup of L | list(L) exclusive |
| creating the `actors` row of D | intern(D), after all author and list locks |

### Row locks on `lists`

A `lists` row is updated by writers that hold different author locks:
the state flip runs under the lock of the *listblock's author*, the
item-count update under that of the *list's owner*. These updates
touch only tracking and counter columns, never the last-writer-wins
columns of the record, and always happen after the list lock for that
row is held, so two writers of the same `lists` row are already
serialized by the list lock.

Deadlock aborts remain possible in principle (hash collisions,
autovacuum). Every apply transaction retries on SQLSTATE `40P01` and
on a lock set that changed while it was taken, and neither kind of
retry counts toward poisoned-event handling.

## The transition function

The transition function takes the list's state, `record_state`,
`listblock_count` (after the change that fired the event) and
`purge_then`, plus one outside fact, whether the owner has
re-admissions left today, and an event. It returns the new state, the
new `purge_then` and the actions to apply. It runs, and its actions
are applied, under list(L) exclusive, plus author(owner) where items
are touched.

### Events

| Event | Meaning |
|---|---|
| **+** | counted listblocks 0 → ≥ 1 |
| **−** | counted listblocks ≥ 1 → 0 |
| **RP** | list record present (a firehose or backfill apply, or the record check found it) |
| **RD** | list record deleted (firehose delete, purge of the owner's account) |
| **NF** | the record check did not find the record (after re-resolving the owner) |
| **NFx** | not-found retries exhausted |
| **GF** | a gate fails at the record check or before a run starts: storage budget, hard ceiling, the owner's host cap, the lists cap. Carries the cause stored in `deferred_by`. |
| **GO** | the gate reopened |
| **OK** | a run promotes L |
| **FT** | a run fails terminally (attempts or duration), or `limits.pending_max_age` is exceeded |
| **OI** | the owner is inactive at the record check or during a run |
| **OA** | the owner was reactivated |
| **DV** | the owner's repository diverged |
| **GE** | grace expired |
| **PD** | purge done |

### Actions

- **admit**: `admit_epoch += 1`; `admitted_at = now()`;
  `fetch_run_id = NULL`; `fetched_at = NULL`; `state = pending`;
  enqueue the record check (it always runs for a new epoch) and
  rebuild the list's scheduling lanes.
- **admit (charged)**: an owner-caused re-admission. If the owner has
  budget left (`limits.owner_readmissions_per_day`, 4 per UTC day):
  charge one, then **admit**. Over budget: `state = deferred`,
  `deferred_by = 5`, `next_retry_at` = the start of the next UTC day;
  a daily retry task fires **GO** on such lists. The charge is one
  conditional statement on the owner's row, so two of an owner's
  lists that are re-admitted at the same moment cannot both take the
  last of the budget: the one that finds it gone is deferred.
- **purge→X**: `state = purging`; `purge_then = X`;
  `fetch_run_id = NULL`. The janitor deletes the list's items in
  batches of 10,000 rows, each batch under author(owner) and list(L)
  exclusive and each re-checking that the list is still `purging`,
  adjusting `item_count`, `actors.owned_items` and host usage as it
  goes. Then it fires **PD**.
- **start grace** / **clear grace**:
  `retain_until = now() + limits.list_grace` (7 days) /
  `retain_until = NULL`.
- **promote**: `fetched_at = now()`; `fetched_witness` = the run's
  coverage point; `fetch_attempts = 0`; the claim and `next_retry_at`
  cleared.
- **refresh done**: `refresh_requested = false`; `fetched_witness` =
  the run's coverage point; `capped` cleared if the run refused no
  item; the claim cleared. The state does not change.

Only two re-admissions are owner-caused and charged: **RP** on a
`dead` list (the owner re-created a deleted list) and the re-admission
that follows **DV** (the owner's repository diverged). First
admissions, **GO**, **OA**, **PD** and **RP** on a `missing` list (the
record was there after all) are never charged. An outsider who toggles
a listblock (− then +) is charged through the admission rate of the
[counter](#when-a-row-is-counted), not here.

### Table

Rows are states, columns are events. An empty cell means the event
does nothing in that state: the state, `purge_then` and every column
stay as they are.

| State | + | − | RP | RD | NF | NFx | GF | GO |
|---|---|---|---|---|---|---|---|---|
| `untracked` | `dead` if `record_state = deleted`, else **admit** | | | | | | | |
| `pending` | | purge→`untracked` | | purge→`dead` | purge→`missing` | | set `deferred_by`; purge→`deferred` | |
| `ready` | | `retained`; start grace | | purge→`dead` | | | | |
| `retained` | `ready`; clear grace | | | purge→`dead` | | | | |
| `unavailable` | | purge→`untracked` | | purge→`dead` | purge→`missing` | | set `deferred_by`; purge→`deferred` | |
| `purging` | if `purge_then ≠ dead`: `purge_then = untracked` | `purge_then = untracked` | if `purge_then = dead`: `purge_then = untracked` | `purge_then = dead` | | | | |
| `missing` | | `untracked` | **admit** if count > 0 | `dead` | stay; retry | `dead` | | |
| `dead` | | `untracked` | **admit (charged)** if count > 0 | | | | | |
| `deferred` | | `untracked` | | `dead` | | | | **admit** if count > 0 |

| State | OK | FT | OI | OA | DV | GE | PD |
|---|---|---|---|---|---|---|---|
| `untracked` | | | | | | | |
| `pending` | `ready`; promote | `unavailable` | `unavailable` | | **admit (charged)** | | |
| `ready` | refresh done ¹ | stay ¹ | stay ¹ | | charged purge ² | | |
| `retained` | refresh done ¹ | stay ¹ | stay ¹ | | charged purge ² | purge→`untracked` | |
| `unavailable` | `ready`; promote | stay; retry | stay | **admit** | **admit (charged)** | | |
| `purging` | | | | | | | clear `capped`; → `purge_then`; then, if that is `untracked` and count > 0: **admit** ³ |
| `missing` | | | stay; retry, not counted toward **NFx** | | | | |
| `dead` | | | | | | | |
| `deferred` | | | | | | | |

¹ The record check and the gates run only at admission; a fetched list
is not demoted by them, because later deletes arrive as events. A
`ready` or `retained` list is claimed only by a **refresh** run. Its
outcome never changes the state; a successful refresh that refused no
item clears `capped`.

² Divergence of the owner of a fetched list: the items came from the
discarded history. The re-admission is charged when **DV** fires. With
budget left: charge one, purge→`untracked`, and **PD** re-admits the
list (count > 0) uncharged for a fresh fetch. Over budget: set
`deferred_by = 5` and `next_retry_at`, purge→`deferred`. A list
nothing counts on (count = 0, which is what `retained` means) is not
re-admitted when its purge ends, so for it **DV** is purge→`untracked`
with no charge and no deferral, whatever the budget. **DV** is
fired *before* the owner's authored rows are purged, so the list is
already `purging` — treated like `pending` for coverage — and is never
served empty as `ready`.

³ A purge always finishes, since the items are partial. A list whose
purge ends in `purge_then = dead`, `missing` or `deferred` goes to
that state whatever the count.

"Stay; retry" leaves the state unchanged and tells the caller to
reschedule its retry.

### Invariant

Every **admit** requires `listblock_count > 0`. Hence: a list is
`pending`, `ready` or `unavailable` only while `listblock_count > 0`.
`retained` is the grace exception. While a list is `purging`,
`purge_then` is always set.

### Notes

- **Listitem apply.** Under author(O) and list(L) shared, read
  `track_state` (no row means `untracked`); apply the write if and
  only if the list is tracked. When the new version of an update is
  refused (the item moved to an untracked list, or a cap refused it)
  and a stored row for the key exists, the row is deleted and a
  refusal tombstone with rev `E − 1` is written.
- **Purge drains and history.** A drain batch records the items it
  deletes in `list_items_history` (cause `list_deleted`) only while
  L's `record_state` is `deleted` and its owner's status is not
  `deleted`. Every other drain records nothing: the list stopped being
  tracked, its members were not removed from it. See
  [history.md](history.md#the-extra-condition-for-list-memberships).
- **Account purge of an owner** fires **RD** for each of the owner's
  lists and cancels any running fetch run for the owner.
- **Coverage.** The effect of pending lists on a response is computed
  on read ([coverage.md](coverage.md)); nothing in the table maintains
  it.

## Why a refused listitem is never lost

Listitems for untracked lists are refused, not buffered. The question
is whether an item can fall between the refusal and the admission: a
listblock arrives just after the list's items were dropped, or the
reverse.

Let item I, by owner O in list L, be committed to O's repository.

- **I's event is processed before the admission commits.** It was
  refused. Its repository commit precedes its processing, which
  precedes the commit of the admission, which precedes the start of
  the list job's run: the run reads its stamp and claims L *after* the
  admission, and its cursor is never shared with an earlier job. The
  run lists O's repository as of the time of each page, so I is on its
  page unless it was deleted since, in which case the delete's
  tombstone beats the run's stamp. If I was *moved into* L by an
  update that was refused at rev `E` (L was untracked then), the
  refusal tombstone has rev `E − 1` and the run's stamp is ≥ `E`, so
  the listing applies I.
- **I's event is processed after the admission commits.** The list
  lock orders the write after the admission; it sees `pending` and is
  applied.
- **The reverse: a listitem after the last listblock went away.**
  From `ready` the list is `retained`, and the item is applied. From
  `pending` or `unavailable` the list is `purging`: the item is
  refused and the partial items are purged; a re-admission starts a
  fresh run.

No buffering of refused items is needed.

## Queries while a list is pending

The rule is **not yet indexed, never partial**. The items of a
pending list are an unordered subset of its members, and a consumer
that takes a subset for the answer under-enforces blocks.

- `getListMembers(L)` returns `state: "pending"`, no members, and
  coverage `partial` with `list_pending`.
- The list-derived answers (`getIncomingListBlocks`, `getListsNaming`,
  `checkBlocks`) leave pending lists out. Their `freshness` says so:
  the list endpoints lower `indexedAt` or the level, `checkBlocks`
  names the affected pairs in `partialFor`
  ([coverage.md](coverage.md)).

The window is normally short: the fetch pages through the owner's
listitem collection, so it grows with the number of items the owner
has. A list that stays `pending` longer than
`limits.pending_max_age` (3 h since admission, queue wait included)
becomes `unavailable` through **FT**. Fetching a `ready` list again never changes its state.

## List-size caps

| Cap | Key (default) | Enforced on |
|---|---|---|
| items per list | `limits.list_items_per_list` (1,000,000) | `lists.item_count` |
| items per owner | `limits.list_items_per_owner` (2,000,000) | `actors.owned_items` |

Both are enforced by a conditional update,

```sql
UPDATE lists SET item_count = item_count + 1
 WHERE id = $1 AND item_count < $cap
RETURNING …
```

and the same pattern on `actors.owned_items`. Zero rows returned means
the item is refused and `capped` is set on the list the refused item
belonged to. Refusals of items for tracked lists by a host cap or the
storage budget ([security.md](security.md#what-happens-to-a-refused-write)) also set `capped` on the
list.

Deletes decrement. Every path that deletes `list_items` rows (firehose
delete, changed or refused update, range or whole reconcile, purge
drain, account purge, divergence purge) uses one function. That
function also writes `list_items_history`: its caller names the cause
of the removal, and the account purge and the divergence purge name
none.

`capped` is cleared when a purge completes and by a refresh run that
refuses no item. A capped tracked list is counted in the exception
`cappedLists` and reported with the reason `list_capped`.

There is no minimum number of listblockers for admission: one counted
listblock admits a list, because a higher threshold would hide real
blocks.
