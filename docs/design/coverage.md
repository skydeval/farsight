# Freshness and coverage

The six read queries of the API (`getIncomingBlocks`,
`getIncomingListBlocks`, `getListsNaming`, `getListMembers`,
`checkBlocks` and `getStats`) carry a `freshness` object in every
response; `getBackfillStatus` and the admin procedures do not. It says
how far the index has followed the network (`indexedAt`), how well the
firehose is keeping up, and what Farsight can claim about the
completeness of this particular answer (`coverage`). The object exists
because an index that is still filling, or that lost part of the
firehose, returns answers that look the same as complete ones; a
consumer that enforces blocks needs to know which it has.

The object is defined in the lexicon
`app.nearhorizon.farsight.defs` (`#freshness`, `#coverage`,
`#exceptions`). The endpoints are described in
[api.md](api.md#read-queries).

```json
"freshness": {
  "asOf": "2026-09-30T16:00:00.412000Z",
  "indexedAt": "2026-09-30T16:00:00.101000Z",
  "firehoseAppliedThrough": "2026-09-30T16:00:00.101000Z",
  "firehoseLagSeconds": 0.311,
  "sourceLagSeconds": 1.2,
  "firehoseConnected": true,
  "coverage": {
    "level": "complete",
    "completeSince": "2026-09-14T03:12:00.000000Z",
    "reasons": [],
    "exceptions": {
      "unreachableRepos": 12, "pendingResyncs": 0,
      "cappedAuthors": 3, "refusedAuthors": 0,
      "unavailableLists": 1, "missingLists": 0,
      "deferredLists": 0, "cappedLists": 0,
      "excludedPendingLists": 4
    },
    "pendingLists": 5
  }
}
```

## Fields

| Field | Required | Meaning |
|---|---|---|
| `asOf` | yes | Server time the response was built. |
| `indexedAt` | no | Watermark of the response's scope, at or before `firehoseAppliedThrough`: everything witnessed at or before it is reflected for this scope, within the level. Absent before the first firehose batch. |
| `firehoseAppliedThrough` | no | Running maximum of the Jetstream witness time of applied events. Global. Absent before the first batch. |
| `firehoseLagSeconds` | no | `asOf` minus `firehoseAppliedThrough`. Includes the age of the coverage snapshot (at most 10 s). Present with `firehoseAppliedThrough`. |
| `sourceLagSeconds` | no | `asOf` minus the median rev commit time of the last 1000 applied events. Informational. |
| `firehoseConnected` | yes | Whether the firehose stream is connected. |
| `coverage.level` | yes | `complete`, `assisted` or `partial`. Open enum. |
| `coverage.completeSince` | no | Witness time since which the level holds. Absent when the level is `partial`. |
| `coverage.reasons` | yes | Reason codes (below). Open enum, possibly empty, sorted. |
| `coverage.exceptions` | yes | The counted exclusions (below). Always present, every count present. |
| `coverage.pendingLists` | yes | Global count of lists in state `pending`. |

Timestamps are RFC 3339 with microseconds, truncated, so a watermark
is never rounded up. The two lag fields are JSON numbers with three
decimals, never negative; the lexicon declares them `unknown` because
Lexicon has no floating-point type.

`checkBlocks` adds a required field beside `freshness`:
`partialFor`, an array of `{ "did", "reasons" }`, possibly empty (see
[`checkBlocks`](#checkblocks-per-pair-coverage)).

## Levels

| Level | Claim |
|---|---|
| `complete` | For the response's scope, every record Farsight is not excluding is reflected as of `indexedAt`. |
| `assisted` | The same claim for one subject, established by a discovery run for that subject while the network-wide claim cannot be made yet. `reasons` says why the network-wide claim fails. |
| `partial` | No completeness claim. The rows returned are real, but rows may be missing. `reasons` says why. |

The definition of `complete` is a stable contract and is quoted in the
lexicon: the **only** exclusions behind a `complete` level are the
ones counted in `exceptions`, and an exclusion stops being counted
only when the excluded data has been re-listed. For `checkBlocks` the
response-level level applies only to pairs **not** listed in
`partialFor`; each pair listed there is `partial` for the reasons
given.

The levels are ordered `partial` < `assisted` < `complete`. Where a
response combines several scopes it takes the lowest level, the union
of the reasons, the earliest `indexedAt` and the latest
`completeSince`.

## The witness clock

Every coverage timestamp is on one clock: the **witness clock**, the
Jetstream witness time of applied events. `firehoseAppliedThrough` is
a running maximum; re-applying older events after a failover rewind
does not lower it.

Work that happens at a server instant is placed on the witness clock
through the table `firehose_clock`. Each committed ingest batch
appends `(clock_timestamp() at commit, applied_through)`. `clock(t)`
is the `applied_through` of the latest row with server time ≤ `t`: it
rounds down. Every `t` given to `clock()` is read from the database in
the same round trip as the event it marks, so the clock skew of a
process never enters. Before the first batch `clock(t)` is undefined,
and work done then is not a coverage point.

A **coverage point** is taken at the start of a piece of work: the
instant after which every change is either captured by the work or
arrives through the firehose. It persists across a resume of the work.

| Work | Coverage point |
|---|---|
| full sweep or repair cycle | `S_C = clock(max(started_at, first_applied_at))` |
| repository job | `clock(job start)`: the job's first database round trip, before its stamp and `describeRepo` |
| list fetch run | `clock(run start)`, stored as `lists.fetched_witness` |
| subject discovery | `clock(started_at) − backfill.backlinks.lag_allowance` (5 min, for the lag of the backlink index), stored as `discovered_witness` |

### The gap predicate

`covered(t)` holds when `t` is defined and no unhealed gap overlaps
`[t, firehoseAppliedThrough]`. A gap is either recorded (a row of
`firehose_gaps`) or the **synthetic gap**
`[firehoseAppliedThrough, ∞)`, which exists while the stream is
disconnected, has applied nothing yet, or lags by more than
`firehose.tuning.synthetic_gap_lag` (5 min), and while the snapshot
that says so is stale (below). A recorded gap without an end overlaps
everything after its start.

When `covered(t)` fails, the reason reported is the first that
applies: `firehose_disconnected`, then `coverage_stale`, then
`firehose_lagging`, then `firehose_gap`.

## The global snapshot

The inputs that are the same for every response are held in memory as
one snapshot: the latest completed full sweep (its collections, `S_C`
and `completed_witness`), every gap, the firehose protocol and
connection state, whether a global storage refusal is active, the
exception counts, the effects of pending lists, and
`firehoseAppliedThrough` itself.

The snapshot is read in one `REPEATABLE READ, READ ONLY` transaction,
so its inputs are mutually consistent. It is refreshed on
`NOTIFY farsight_coverage`, and re-read in full every 10 s and on
every `LISTEN` reconnect, so a lost notification can delay a change
and never falsify a claim.

**A snapshot that cannot be read again goes stale.** When every read
has failed for 30 s (the database does not answer, or its pool is
exhausted), the snapshot still held describes a stream that may have
stopped since. From then on it counts as the synthetic gap: no scope
is `complete`, and the reason is `coverage_stale`, until a read
succeeds. `farsight_coverage_snapshot_age_seconds` is the time since
the snapshot in use was read. The notification is sent by every ingest
batch commit and by every change of tracking state from either
process: list transitions, purges, debt inserts and deletes, gap and
cycle changes.

A response uses the snapshot as a unit. `firehoseAppliedThrough` and
`firehoseLagSeconds` come from the same snapshot as the pending-list
effects and are never read live, so `indexedAt` cannot cover a list
admission the snapshot has not seen. The states of the lists a
response decides on (which lists naming X to include) are read live.
If a list that the response would otherwise include is, live,
`pending` or about to be re-admitted, and the snapshot did not know
it, the list is excluded and the response is `partial` with
`list_pending` (for `checkBlocks`, the affected pair goes into
`partialFor`). A live view and a snapshot view therefore never combine
into an over-claim.

"About to be re-admitted" means, here and below: `purging` with
`purge_then = untracked` and `listblock_count > 0`. Such a list is
treated as `pending` for coverage. A list purging towards any other
state hides nothing and is simply left out. The list states are
defined in [list-indexing.md](list-indexing.md).

## Reason codes

`reasons` explains a level below `complete`, lowers `indexedAt`, or
names what a `complete` scope leaves out. The last use matters: a
`complete` response may carry reasons.

| Reason | Raised when | Cleared when |
|---|---|---|
| `sweep_incomplete` | No completed full sweep covers the collection. In `checkBlocks`, also when the network-wide claim cannot be made and the actor has no clean listing that is covered. | The first full sweep completes; for the `checkBlocks` case, a listing of the actor ends clean. |
| `firehose_gap` | An unhealed recorded gap overlaps the interval from the scope's coverage point to `firehoseAppliedThrough`. | A repair cycle or a later full sweep heals the gap. |
| `firehose_disconnected` | The stream is not connected, or no batch has been applied yet. | The stream resumes. If events were lost, a gap is recorded and `firehose_gap` follows. |
| `coverage_stale` | The coverage snapshot is more than 30 s old and could not be read again, so what it says about the stream is not known to hold. | The snapshot is read again. |
| `firehose_lagging` | The stream is connected but `firehoseAppliedThrough` is more than `firehose.tuning.synthetic_gap_lag` behind. | The stream catches up. |
| `sync_events_unavailable` | The firehose runs on Jetstream v1, which carries no `#sync` events, or the interval spent on v1 is still open. | A v2 session takes over. The interval then becomes a closed gap and `firehose_gap` follows until it is repaired. |
| `storage_refusal` | The storage budget or the hard ceiling is refusing writes globally (an open row of `storage_refusals`). | The gate reopens. The authors refused meanwhile stay counted in `refusedAuthors` until re-listed. |
| `list_pending` | A list relevant to the response is `pending` or about to be re-admitted. Either the level is `partial`, or `indexedAt` is lowered to before the list's listblocks (see [Pending lists](#pending-lists)). | The list is promoted to `ready`, or leaves `pending` otherwise. |
| `list_pending_historical` | A pending list that takes effect has a listblock whose witness time is unknown. Level `partial`. | As `list_pending`. |
| `list_capped` | The stored item set of a list is cut by a cap. On `getListMembers` it accompanies the level; in `checkBlocks` it makes the actor side or the pair `partial`. | A refresh run after the cap cleared stores all items. |
| `list_unavailable` | A list is in state `unavailable`. On `getListMembers`, also a list whose owner is hidden. | A run promotes the list; the owner becomes active again. |
| `list_missing` | A list is in state `missing`; on `getListMembers`, also `dead`. | The record is found, or the listblocks on it go away. |
| `list_deferred` | A list is in state `deferred`. | The gate reopens and the list is admitted. |
| `list_not_tracked` | `getListMembers` on a list that no counted listblock targets. Farsight holds no items for it by design. | A listblock on the list admits it. |
| `discovery_truncated` | The network-wide claim cannot be made and the discovery run for the subject did not check every reference: it stopped at its reference cap, or a reference could not be read. Level `partial`. | A later discovery run completes, or the first full sweep completes. |
| `party_debt` | `checkBlocks`: the actor or one of the `others` has an open re-list debt. | A clean listing of that account. |

## Exceptions

`exceptions` counts what a `complete` level excludes. The counts are
global, the same in every response, and always present.

| Exception | Counts | Clears when |
|---|---|---|
| `unreachableRepos` | Actors with an `unreachable` debt: a repository job failed terminally, or its reconcile was skipped. | A clean run of the actor. |
| `pendingResyncs` | Actors with a `resync` debt: `#sync`, `desynchronized`, reactivation, divergence, a poisoned event. | A clean run. |
| `cappedAuthors` | Actors with a `capped` debt: a per-author cap, the admission rate or the intern rate refused a record or stored a listblock uncounted. | A clean run after that cap or rate no longer applies. |
| `refusedAuthors` | Actors with a `refused` debt: a host-bucket cap or the storage budget refused a write by this author. | A clean run after the gate reopened. |
| `unavailableLists` | Lists in `unavailable`. | A run promotes the list. |
| `missingLists` | Lists in `missing`. | The record is found, or the retries are exhausted (`dead`: nothing exists). |
| `deferredLists` | Lists in `deferred`. | The gate reopens. |
| `cappedLists` | Tracked lists with `capped` set. | A refresh run after the cap cleared stores all items. |
| `excludedPendingLists` | Pending lists without effect on coverage: no relevant listblocks, or beyond the per-owner-key bound. | The list is promoted, or starts taking effect. |

### Re-list debts

The actor-level exclusions are rows of one table,
`relist_debt(actor, reason, since_witness)`, with reasons
`unreachable` (1), `resync` (2), `capped` (3) and `refused` (4). Each
actor-level exception is the number of distinct actors holding a debt
of that reason.

- A debt is deleted only by a **clean** run of that actor whose
  coverage point is ≥ `since_witness`.
- Inserting a debt that already exists sets
  `since_witness = greatest(existing, new)`, so a run that started
  before the newer cause cannot clear it.
- A `resync` debt still open 7 days after its first cause is
  replaced by `unreachable`; an actor never counts twice for one
  cause.
- A feeder task turns debts into re-list jobs when, and only when,
  the re-list can clear them. It skips an actor that has a job
  running or waiting. Eligibility per reason:
  - `resync`: immediately, unless the run would be deletes-only (the
    hard ceiling is refusing, or the budget is refusing and the author
    is not on a large host) or one of the author's host buckets is
    closed; and only once per cause (not again after a run whose
    coverage point is ≥ `since_witness`).
  - `unreachable`: when the failed run's `next_attempt_at` has
    passed; without one, at most once an hour.
  - `capped` by a per-author cap: when the author is under 90% of the
    cap named by the debt's `cap_type`, the author's admission key has
    rate left in the current UTC day, the run would not be
    deletes-only, and the last run ended at least an hour ago.
  - `capped` by a daily rate (admission or intern): when no run of
    the author ended in the current UTC day, the key has rate left,
    and the run would not be deletes-only.
  - `refused`: when the gate named by `cap_type` is open (the budget,
    the ceiling, or the bucket's bit for that kind of record), and the
    last run ended at least an hour ago.

  A debt whose cause still applies waits, so there is no re-list loop.

List-level exclusions are list states, not debts. Jobs and run
outcomes (what makes a run clean) are described in
[backfill.md](backfill.md#outcomes).

On a live network `excludedPendingLists`, `missingLists` and
`unreachableRepos` are normally small and not zero. `getStats` and the
admin dashboard itemize the counts.

## Scopes

Coverage is computed per query. Which computation applies depends on
what the answer ranges over.

| Endpoint | Scope |
|---|---|
| `getIncomingBlocks` | network scope for `block`, else subject scope |
| `getIncomingListBlocks`, `getListsNaming` | composite scope for the subject |
| `getListMembers` | list scope |
| `checkBlocks` | per pair |
| `getStats` | network scope for `block` and `listblock`, with the pending-list table |

### Network scope

Network scope for a collection K is the claim about every account on
the network. It is `complete` if and only if all of these hold:

1. a completed full sweep covers K;
2. `covered(S_C)` for that sweep;
3. the firehose runs on Jetstream v2 and no v1 interval is open;
4. no global storage refusal is active.

Otherwise it is `partial` with every reason that applies
(`sweep_incomplete`, `firehose_disconnected` or `firehose_lagging`,
`firehose_gap`, `sync_events_unavailable`, `storage_refusal`).

`completeSince` is the later of the sweep's `completed_witness` and
the `healed_witness` of the last gap overlapping
`[S_C, firehoseAppliedThrough]`. `indexedAt` is
`firehoseAppliedThrough`.

### Subject scope

Subject scope applies to an actor X when network scope is not
`complete`. A discovery run asks a backlink index for the records that
name X and fetches them, so the answer for X can be whole long before
the first sweep has finished.

- If discovery for X completed untruncated for the kind of record in
  question (direct blocks, or the chain listitem → list → listblock)
  with point `D`, and `covered(D)`: level `assisted`,
  `completeSince = D`. The reasons of the failed network scope stay in
  `reasons`.
- If discovery was truncated: `partial` with `discovery_truncated`.
- Otherwise: the network scope's `partial` result.

### List scope

`getListMembers(L)` reports on one list.

| State of L | Level | Reasons |
|---|---|---|
| `ready`, `retained` | `complete` if `covered(fetched_witness)` and the firehose is on v2 with no open v1 interval; else `partial` | the failed check's reason; `list_capped` if the list is capped |
| `pending`, or about to be re-admitted | `partial` | `list_pending` |
| `unavailable` | `complete` | `list_unavailable` |
| `missing`, `dead` | `complete` | `list_missing` |
| `deferred` | `complete` | `list_deferred` |
| `untracked`, other `purging`, no row | that of network scope for `listblock` | `list_not_tracked`, plus the network scope's reasons |

For a `ready` or `retained` list `completeSince` is its
`fetched_witness`. The `complete` level of an `unavailable`,
`missing`, `dead` or `deferred` list is the contract at work: the list
is a counted exclusion (or, for `dead`, does not exist), the reason
names it, and no members are returned. An untracked list is `complete`
only when Farsight can be sure no listblock targets it, which is the
network claim for `listblock`.

### Composite scope

`getIncomingListBlocks` and `getListsNaming` answer for a subject X
across every list that names X. Their coverage is the minimum of:

- network scope for `listblock`, or subject scope for X over the list
  chain;
- `covered(fetched_witness)` for every `ready` or `retained` list
  naming X (a list without a `fetched_witness` gives `partial` with
  `list_pending`);
- the live rule of [the snapshot](#the-global-snapshot);
- at network scope: the [pending-list table](#pending-lists) over all
  pending lists;
- at subject scope: any list that discovery found naming X and that is
  pending, about to be re-admitted, `unavailable`, `deferred` or
  `missing` lowers the level to `partial` with the matching reason,
  because its blocks are known to be relevant. Other pending lists
  lower `indexedAt` to at most `D` with `list_pending`: they can name
  X only through a listitem witnessed after `D`.

### `checkBlocks`: per-pair coverage

`checkBlocks(X, others)` has a response-level `coverage` and a
per-pair list.

The **response-level** coverage covers what is common to every pair,
the X side:

- network scope, or subject scope for X, for `block` and for
  `listblock`;
- while network scope is not complete: X's last clean listing point
  `clean_witness` with `covered(clean_witness)`, else `partial` with
  `sweep_incomplete`;
- no re-list debt on X, else `partial` with `party_debt`;
- none of the lists X listblocks is pending, about to be re-admitted,
  `unavailable`, `deferred` or `missing` (`partial` with the matching
  `list_…` reason), or tracked and capped (`partial` with
  `list_capped`);
- `covered(fetched_witness)` for each `ready` or `retained` list X
  listblocks.

`partialFor` lists each of the `others` whose pair is not complete
beyond that, with its reasons:

- the other holds a re-list debt: `party_debt`;
- a list the other listblocks is tracked and capped: `list_capped`;
- a list the other listblocks is pending, about to be re-admitted,
  `unavailable`, `deferred` or `missing`: the matching `list_…`
  reason;
- a `ready` or `retained` list the other listblocks, and that names X,
  fails `covered(fetched_witness)`: the gap reason.

A list whose record is deleted, or whose owner is hidden (unless
`includeInactive` is set), does not bear on either side. Only lists on
which X or one of the `others` holds a listblock can hide a block in
this response, and those are known, so no global pending-list rule
applies here. An account that keeps its own lists pending can degrade
only the pairs involving its own accounts, and the X side when X
itself subscribes to a list it owns.

## Pending lists

A pending list L hides the blocks of L's listblockers against L's
not-yet-fetched members. How much that matters depends on which
listblocks exist on L. The effect is computed on read from current
rows, as part of the snapshot, for the pending lists and the lists
about to be re-admitted; no counter is maintained.

- A **covered author** is an author with no `resync` or `unreachable`
  debt.
- The **relevant listblocks** of L are the counted listblocks on L by
  covered authors.
- `list_blocks.witnessed_at` is the witness time of the firehose event
  that first stored the row. It is NULL if the row was first stored by
  a repository listing or by discovery, and it is sticky: a later
  re-list of the same record keeps it.

Effect of a pending list at network scope (the composite scope of the
list endpoints, and `getStats`):

| Relevant listblocks on L | Effect |
|---|---|
| none | counted in `excludedPendingLists`; no effect on level or `indexedAt` |
| all have `witnessed_at` | `indexedAt` ≤ `min(witnessed_at) − 1 µs`; reason `list_pending`; the level is unchanged |
| any has `witnessed_at` NULL | level `partial`; reason `list_pending_historical` |

The middle row is what keeps a busy network `complete`: a listblock
that arrives on the firehose and admits a new list moves the watermark
back to just before that listblock, for the seconds or minutes the
fetch takes, instead of lowering the level.

Two bounds limit how far pending lists can degrade the list endpoints:

- A list stops being `pending` after `limits.pending_max_age` (3 h) of
  wall-clock time since admission, queue wait included. It becomes
  `unavailable`, is counted in `unavailableLists`, and keeps retrying.
- Per **owner key**, at most `limits.pending_effects_per_owner_key`
  (5) pending lists take effect at a time, oldest admission first; the
  rest are counted in `excludedPendingLists`. The owner key is the cap
  bucket of the owner's host if the owner is resolved and the host is
  not a large host, else the owner's DID. Only lists with relevant
  listblocks occupy slots. Keys and slots are evaluated at each
  snapshot refresh.

What remains: an account holder who owns lists and controls covered
accounts that reveal listblocks without a witness time on them can
keep the list endpoints at `partial`, or their `indexedAt` lagged, in
windows of at most 3 h per list, five lists per owner key at a time.
Ordinary re-admissions produce the same short windows.

## Firehose gaps and repairs

A gap is an interval of witness time in which events may have been
lost. Gaps are rows of `firehose_gaps` with a cause:

| Cause | Code | Recorded when |
|---|---|---|
| `cursor_too_old` | 1 | A Jetstream refuses the cursor of a resume with `CursorTooOld`, or announces with `#info OutdatedCursor` that it resumed later than asked. |
| `heuristic` | 2 | A resume on an instance with its own cursor did not continue where it left off, as far as its first event shows: a timestamp resume whose first event is later than the stored cursor, or a `seq` resume answered with a lower `seq` or with a first event more than `firehose.tuning.gap_threshold` (300 s) after the stored cursor. |
| `failover` | 3 | A change of Jetstream instance that was clamped, or had no safe rewind. |
| `sync_unavailable` | 4 | An interval spent on a v1 Jetstream. |
| `seam_unrepaired` | 5 | The re-read of a seam window failed 5 times: the gap is the window. |
| `unreadable` | 6 | Three sessions in a row ended on a frame that could not be read at the same position, and the next one stepped past it: the gap runs from that position to the first event read after it. |

A disconnected or lagging stream needs no row: it is the synthetic gap
of the [gap predicate](#the-gap-predicate), and it ends by itself when
the stream resumes without loss. How the reader detects and records
gaps is in [firehose.md](firehose.md#how-a-gap-is-detected).

**The v1 interval.** Jetstream v1 carries no `#sync` events, so a
repository that diverged (a server restored from backup, a rebase)
can keep records in the index that no longer exist. Every interval
spent on v1 is therefore one gap of cause `sync_unavailable`, opened
when a v1 session starts and closed when a v2 session takes over.
While it is open the level is capped at `partial` with
`sync_events_unavailable`, and it cannot be repaired: a repair covers
closed gaps only. An instance that stays on v1 never reaches
`complete`. Once a v2 session takes over, the whole interval is one
closed gap and is repaired like any other.

**Healing.** A gap is healed by a **repair cycle** of the backfill
process:

1. One repair covers all closed, unhealed, unclaimed gaps at its
   start, from the earliest `from_at`. Gaps that open or close while
   it runs are taken by the next repair. An open gap waits.
2. The repair walks the relay's `com.atproto.sync.listRepos` and
   takes two groups of repositories. The first: every repository
   whose rev is at or after `from − backfill.repair_slack − lag`,
   where `repair_slack` is 1 h and `lag` is the firehose lag at the
   time of enumeration; an entry with no rev, or a rev that does not
   parse, is taken as changed. The second: every repository the relay
   reports active that Farsight holds with a status other than active
   or as inactive at its last listing. This group catches
   reactivations whose `account` event was lost; those accounts get a
   `resync` debt and their `unavailable` lists are re-admitted. Each
   member gets a repository job.
3. When every member's job has ended, the covered gaps get
   `healed_at` and `healed_witness`, and `firehose_gap` clears.
   `completeSince` moves to the `healed_witness`.
4. If the relay is unavailable when the repair starts, the repair
   re-lists the accounts Farsight already knows and heals nothing; the
   gaps wait for the next repair or full sweep.

A full sweep also heals every gap that closed before the sweep's
start point.

A repair re-reads every account whose repository changed during the
gap, and finds them by walking the whole of `listRepos`, so its
duration grows with the length of the gap; expect a repair after a
gap of days to run for days. An operator can hold or stop it:
`[backfill.repair]` has `auto_start` (default `true`; when `false` a
closed gap waits for `admin.startRepair`) and `paused` (default
`false`; a paused repair enumerates nothing new and keeps its place).
`admin.pauseRepair` sets `paused`. `admin.cancelRepair` switches
`auto_start` off, deletes the open repair cycle and releases its gaps
**unhealed**. While a gap is
unhealed, for whatever reason, the affected scopes stay `partial` with
`firehose_gap`. See [backfill.md](backfill.md#gap-repair) for the
cycle and its controls, [api.md](api.md#admin-procedures) for the
three procedures and [operations.md](operations.md#backfillrepair)
for the configuration keys.

## The first full sweep

Until a full sweep has completed, network scope is `partial` with
`sweep_incomplete` for every collection. The sweep does not start
before the firehose has committed its first batch, and its start point
is `S_C = clock(max(started_at, first_applied_at))`: from `S_C` on
the firehose supplies every change, and the sweep supplies what
existed before. That is why the first start has no gap by
construction, and why `covered(S_C)` is the condition for `complete`.

During the sweep an instance still answers usefully:

- rows already indexed are returned, at level `partial`;
- a subject that has been through discovery is answered at level
  `assisted`;
- accounts first seen after `S_C` are covered by the firehose from
  their first record on.

When the sweep completes, `completeSince` is its `completed_witness`.
Accounts the sweep could not list are not a reason for `partial`: they
are `unreachableRepos`. The sweep is described in
[backfill.md](backfill.md#the-sweep).

## What a consumer should do

- **`coverage.level`.** Treat `complete` and `assisted` as "the answer
  for this subject is whole, apart from the counted exclusions". Treat
  `partial`, **and any value you do not know**, as "rows may be
  missing": the rows returned are true, the absence of a row proves
  nothing. A consumer that enforces blocks can apply what it gets and
  ask again later; one that must not under-enforce can fall back to
  another source for that subject.
- **`indexedAt`.** The answer reflects the network up to here. To
  read your own write, wait until `indexedAt` is at or past the
  witness time of the event; do not compare it with your own clock.
  When `indexedAt` is absent the instance has applied nothing yet.
- **`completeSince`.** If you cache an answer taken at level
  `complete`, it has been continuously maintained since this instant.
  A value later than your cached copy's `indexedAt` means coverage was
  interrupted and restored in between; fetch again.
- **`reasons`.** Use them for diagnosis and for deciding how long to
  wait: `list_pending` normally clears quickly (and within
  `limits.pending_max_age` at the latest),
  `firehose_disconnected` and `firehose_lagging` when the stream
  recovers, `coverage_stale` when Farsight reaches its database again, `firehose_gap` after a repair, `sweep_incomplete` once per
  instance. Ignore codes you do not know; the enum is open. A
  `complete` response may carry reasons that name an exclusion
  (`list_unavailable`, `list_capped`, `list_not_tracked` and the like)
  or explain a lowered `indexedAt` (`list_pending`).
- **`exceptions`.** These are instance-wide counts, not facts about
  your query. If you need strictness, compare them with thresholds of
  your own; do not expect zeros.
- **`partialFor`** (`checkBlocks`). For each DID listed, treat that
  pair as `partial` whatever the response-level level says. A DID not
  listed has the response-level level.
- **`pendingLists`.** Informational: how many lists are being fetched.
- **`firehoseConnected`, `firehoseLagSeconds`, `sourceLagSeconds`.**
  Health indicators for monitoring. The level already accounts for
  them; do not derive completeness from them yourself.

Integration advice for an AppView is in
[../guide/appview.md](../guide/appview.md).
