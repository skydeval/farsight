# Cold start and background backfill

The firehose only tells Farsight what changes from now on. Everything
that already exists in the network, and everything missed while the
firehose was down, has to be read from the repositories themselves.
That is the work of a second process, `farsight-backfill`. This page
covers how it reads one repository, how it shares its capacity between
on-demand requests, newly seen authors and the systematic sweep, how
it enumerates the network, how it repairs firehose gaps, and the
endpoints that ask for a backfill.

Live ingestion is in [firehose.md](firehose.md). What a finished
backfill means for the `freshness` object of an API answer is in
[coverage.md](coverage.md).

## Processes

| Process | Role |
|---|---|
| `farsight` | Firehose ingestion, the API and the web UI. Serves from the first second; a fresh deployment reports `partial` coverage with the reason `sweep_incomplete`. |
| `farsight-backfill` | History: repository listings, list jobs, the sweep, gap repairs and subject discovery. |

`farsight-backfill` reads the same config file as the server (a
read-only mount is enough) and the same database. It holds no state of
its own that matters across a restart: every queue, lease, cursor and
checkpoint is a row in Postgres.

Work the scheduler has taken is never lost with the process. A queue
entry is claimed, not deleted, while its job runs. A cycle member
stays outstanding until a job settles it. A list fetch run stays open
until it is finished. When the process stops in an orderly way it
stops its jobs and gives back what they held. When it is killed, the
claims and leases it held run out within 10 minutes, and the feeder
takes the work up again ([what a stopped process
leaves](#what-a-stopped-process-leaves)).

Start-up and reload:

- With no config (setup not finished, or a config reset) the process
  idles and looks again every 5 seconds.
- With a config it connects, then waits until the server has migrated
  the schema to the version it was built for.
- It reloads the config on `NOTIFY farsight_config` and, as a
  fallback, every 60 seconds when the file's modification time has
  changed. An invalid edit keeps the running config. A change to
  `storage.database_url`, `[metrics]`, `[net]`,
  `backfill.concurrency`, `backfill.plc_url`, `limits.large_hosts` or
  `limits.cdn_ranges_extra` makes the process rebuild its tasks: the
  scheduler stops every running job at its next await, gives their
  queue entries and leases back, and new tasks start on the new
  config. Resumable state is already persisted, so the jobs go on
  from their cursors. Every other key applies in place; the request
  limits (`backfill.per_host_*`, `backfill.per_domain_*`,
  `backfill.plc_rps`) apply to the next request.
- One network layer serves the process across rebuilds. It keeps what
  it knows of every host (cooldowns, circuit breaker, requests in
  flight), and since the old jobs are stopped before the new ones
  start, a host is never asked by two sets of jobs at once.
- Its database pool is `backfill.concurrency + 8` connections,
  separate from the server's pools. A change of
  `backfill.concurrency` rebuilds the pool with the tasks.
- On shutdown the scheduler stops its jobs the same way and the
  process exits within 30 seconds.

The two processes never talk to each other directly. They coordinate
through these rows:

| Row or channel | Written by | Read by |
|---|---|---|
| `schema_version` | server (migrations) | backfill waits for it |
| `firehose_state.first_applied_at` | server (first ingest batch) | backfill: no cycle starts before it |
| `firehose_clock` | server (one row per ingest batch) | backfill: coverage points of jobs and cycles |
| `firehose_gaps` | server (records gaps); backfill (claims and heals them) | both |
| `backfill_queue` | server (`requestBackfill`, `#sync`, poisoned events, new firehose authors); backfill (retries, debts, claims) | backfill scheduler; server (`getBackfillStatus`) |
| `account_purges` | both (the transaction that records an account as `deleted`) | server (the purge task) |
| `list_jobs`, `lists` | server and backfill (list admissions) | backfill scheduler |
| `sweep_cycles` | backfill; server (`admin.startRepair`, `admin.cancelRepair`) | both |
| `job_leases` | backfill | both (`getBackfillStatus` reports `running`) |
| `NOTIFY farsight_config` | server, after it edits the config | backfill reloads |
| `NOTIFY farsight_coverage` | both, when coverage inputs change | server refreshes its coverage snapshot |

Each process measures the database and derives the storage gates for
its own writers once a minute (see [security.md](security.md)).

## The per-repo job

### Job kinds and the lease

| Kind | Collections listed | Promotes lists |
|---|---|---|
| `repo` | `app.bsky.graph.block`, `listblock`, `list`, `listitem` | no |
| `list_fetch` | `listitem` | yes, the lists its run claimed |
| `discovery` | none (backlink queries and `getRecord`) | no |

All kinds share one lease per DID in `job_leases(did, lease_owner,
lease_until)`, so at most one job runs for a DID at a time. The table
is keyed by the DID text, so a lease does not need an `actors` row. A
lease lasts 10 minutes and is renewed after every page; a job that
dies leaves a lease that expires. Each job holds its leases under its
own name (the process name and the job's number), so two jobs of one
process exclude each other like jobs of two processes, and a job
releases only its own lease.

The scheduler does not dispatch a queue entry whose DID has a live
lease. A job that finds the lease held all the same leaves its entry
in the queue for another 60 seconds. A sweep member that finds the
lease held is offered again on a later turn. A phase-1 check that
finds the owner's lease held is looked at again 30 seconds later.

### Steps of a `repo` job

For a DID `D`:

1. **Known hidden account.** If `D` is already stored with a hidden
   status (`deactivated`, `takendown`, `suspended`, `deleted`), the
   relay's `com.atproto.sync.getRepoStatus` is asked first. If it
   confirms, the job ends **inactive** without reading anything. If it
   says the account is active, the status is set back to active and
   the job goes on; if it says the account is inactive with a status
   that hides nothing (`throttled`, `desynchronized`), that status is
   stored and the job goes on. Only the relay's own answer lifts a
   hidden status. When the relay gives none (an error, a rate limit, a
   timeout, an answer without `active`), the account stays hidden,
   nothing is read, and the job **fails** and is retried like any
   other failure.
2. **Resume or start.** If `backfill_state.current_run_id` names a run
   whose cursor rows carry a stamp read less than 72 hours ago, the
   job re-attaches to that run: same stamp, same cursors. Otherwise it
   draws a new random run id. The coverage point of the job is the
   witness clock at job start.
3. **Resolve** `D` to its PDS through the outbound client that
   refuses private addresses (see [security.md](security.md)).
   `did:plc` goes to `backfill.plc_url` under the PLC limiter;
   `did:web` to `https://<host>/.well-known/did.json`. The result is
   cached in `actors` for 7 days; an `identity` event for a known DID
   clears the cache. A tombstoned DID sets the account `deleted` and
   ends the job **inactive**. A DID that does not resolve is a failure.
4. **Stamp early.** If `D` already holds stored rows, read
   `com.atproto.sync.getLatestCommit` and keep its rev as the stamp
   `R` (a rev whose time is more than 5 minutes ahead of the clock is
   refused and fails the job, since records stored with it would win
   over every later write), with `stamp_read_at` from the database clock, before anything
   else is read. Otherwise the stamp is deferred.
5. **`describeRepo`.** The present set `P` is the repo's collections
   among the four; `listitem` stays in `P` only if `D` owns a tracked
   list. The absent set `A` is the rest.
6. **Stamp late.** If `P` is not empty and no stamp was read yet, read
   it now and mark it *late*. If `P` is empty and `D` holds nothing,
   the job ends **clean** here: an empty repository costs one
   resolution and one `describeRepo`.
7. **Divergence check** (new runs only), below.
8. **Reconcile absent collections.** For each collection in `A`,
   delete `D`'s rows of that collection with `rev < R`. Listblock
   deletions go through the subscriber counter (see
   [list-indexing.md](list-indexing.md)). This step runs **only with
   an early stamp**. With a late stamp `D` held nothing at step 4, so
   anything present now was written by the firehose with its true rev
   and is current; deleting by `rev < R` could remove a record created
   between steps 5 and 6.
9. **List each collection in `P`** with
   `com.atproto.repo.listRecords`, `limit=100` and `reverse=true`
   (ascending record key), through this run's own cursor row in
   `backfill_cursors`, keyed `(actor, collection, job_kind, run_id)`.
   A job never adopts another run's cursor. Each page is one
   transaction under the author lock of `D` (plus list locks; see
   [storage.md](storage.md) for the lock order):
   - **Validate.** A page holds at most the 100 records asked for;
     one with more fails the job. The page's envelope is parsed, and
     each record on its own: a record that cannot be parsed (one
     nested deeper than the parser reads, for example) is dropped by
     itself and the rest of the page is used. The URI authority must
     be `D`, the collection the one asked for, and the record must be
     valid. An invalid record is dropped and counted against the host
     (`pds_hosts.errors_total`); its key still counts as listed.
   - **Apply** each record as an upsert with stamp `R` and no witness
     time. Last-write-wins decides against what is stored
     ([storage.md](storage.md)).
   - **Range reconcile.** Delete `D`'s rows of the collection with
     `rev < R` whose record key lies in `(prev_last, this_last]` and
     that are not on the page; on the last page the range is open
     upwards. Late stamps reconcile too: a row with `rev < R` that is
     absent from a page read *after* `R` is gone. A reconcile removes
     at most 5,000 rows and locks at most 250 lists in one
     transaction; one with more to remove goes on in further
     transactions before the page is done
     ([storage.md](storage.md#the-write-path)).
   - **Persist** `(cursor, prev_last, R, stamp_read_at)` and renew the
     lease.
   - Record keys must increase strictly, compared bytewise. On a
     violation the collection is restarted, once, with an in-memory
     set of the keys seen (a 64-bit hash of each, keyed anew for every
     listing). At the end the stored keys are read 2,000 at a time and
     each chunk is reconciled over its own range, keeping the keys
     that are in the set. If the set outgrows `backfill.seen_set_cap`
     (2,000,000, about 40 MB) the listing goes on to the end without
     it and without starting again, the reconcile is skipped for that
     collection and `D` gets an `unreachable` debt, which stays until
     a clean listing succeeds.
10. **Finish** with one of the outcomes below.

Bounds on every outbound request: a 30 second timeout, a 2 MB response
cap, no redirects followed.

Bounds on a job, so that no host can keep a worker:

| Bound | Value | When it is reached |
|---|---|---|
| Time of one attempt | `backfill.repo_job_max_duration` (1 hour) | the attempt **yields** |
| Pages of one attempt, over all collections | 25,000 | the attempt **yields** |
| A `listRecords` cursor the listing has already followed | none allowed | the job **fails** |
| Attempts of one run that yielded, in a row | 10 | the job **fails** |

A cursor is remembered by its hash for the length of a listing, so a
cursor that repeats and a cycle of cursors of any length are both
found. An account at every per-author cap is about 31,000 pages, so a
real repository can need more than one attempt.

A job that **yields** has stored what it listed, with its cursor. It
is queued again in its tier and goes on from the cursor: at once the
first two times, then after 30 seconds, doubling up to an hour. Each
yield is counted in `backfill_state.yields`. The eleventh in a row is
a failure like any other: it is retried on `backfill.retry_schedule`
with its cursor kept, and it becomes terminal after
`backfill.terminal_after`. A run that ends, in any outcome, starts
the count again.

An attempt that goes on with a run keeps that run's coverage point
and start time: the pages an earlier attempt read are no newer than
the first attempt's start. A job whose stamp has reached 72 hours does
not resume: its next attempt is a new run with a new stamp, listing
from the first page.

Cursor rows of runs that are no longer current are deleted once a day
by the server.

### Outcomes

Every `repo` job ends in exactly one outcome. "Clean" has this one
meaning everywhere in the design.

| Outcome | Meaning | Satisfies cycle membership | Clears debts | Sets `clean_witness` | Updates `backfill_rev` |
|---|---|---|---|---|---|
| **clean** | Every collection listed to the end, or confirmed absent, and reconciled; nothing refused; no uncounted listblock remains; account active. | yes | yes, those raised up to the job's coverage point | yes | yes |
| **complete-with-debts** | Listed and reconciled to the end, but some inserts were refused (a cap, a rate, the storage budget, a deletes-only run), a reconcile was skipped, or listblocks stay uncounted. Each shortfall is recorded as a debt. | yes | no | no | yes |
| **inactive** | The relay confirms the account inactive. Nothing is listed; its rows are hidden anyway. `inactive_at_listing = true`; a later reactivation adds a `resync` debt. | yes | no | no | no |
| **failed** | An error. Retried with backoff; terminal after `backfill.terminal_after`. | only when terminal | no | no | no |

Clean, complete-with-debts and inactive set `backfill_state.state` to
`done`, `backfilled_at = now()` and `backfilled_witness` to the job's
coverage point. Clean and complete-with-debts clear
`inactive_at_listing`; failed leaves it unchanged. A run that leaves
uncounted listblocks is never clean, so a `capped` debt persists until
the author is under the cap and a clean run re-evaluates the
uncounted rows.

`backfill_state` rows exist only for DIDs that have an `actors` row
(they hold data, or were requested) and for DIDs that need one: an
inactive or failed outcome creates the `actors` row.

Debts and coverage points are defined in [coverage.md](coverage.md).

### Divergence check

Revs of a repository must increase. The check compares the new stamp
`R` with `D`'s previous listing stamp `backfill_state.backfill_rev`,
never with revs applied from the firehose, which may legitimately
exceed `R` when a commit lands between the stamp and the apply.

If `R < backfill_rev` the repository went backwards (for example a
PDS restored from a backup). A host that came out of a cache proves
nothing here: an account that moved leaves an old copy behind, and
that copy's rev is behind too. So the job first resolves `D` again
at the directory and reads the stamp at the host it names. Only a
host confirmed this way starts the purge.

The job then, in this order:

1. adds a `resync` debt, so coverage reports the account for as long
   as its rows are going;
2. fires the divergence event on each of `D`'s tracked lists (see
   [list-indexing.md](list-indexing.md));
3. purges `D`'s authored rows through the counter path, in batches of
   at most 10,000 rows and 500 list locks. The purge takes rows,
   tombstones and list revs whose rev is below the stamp of the
   moment the divergence was found. What the firehose writes for `D`
   while the purge runs carries a later rev and stays;
4. lists from scratch.

### Repo-level errors, retries and terminal failure

A repo-level error is `RepoNotFound`, `RepoDeactivated`,
`RepoTakendown`, `RepoSuspended`, `RecordNotFound` in a record check,
or a refused connection. On one:

1. Re-resolve `D` bypassing the cache. If the PDS changed, run the
   job there.
2. Otherwise ask the relay's `getRepoStatus`. If the relay says the
   account is inactive with a hidden status, the status is stored and
   the outcome is **inactive**. `throttled` and `desynchronized`
   repositories are not hidden and count as a failure here.
3. Otherwise the outcome is **failed**.

An account's status is set only from the relay, Jetstream or the PLC
directory, never from what a PDS says about itself.

A failed job records `last_error`, increments `attempts`, and is put
back in the queue in its tier with a delay from
`backfill.retry_schedule` (default `1h`, `6h`, `24h`, `1d`; the last
step repeats). The delay is that of the retry alone: if an entry
already waits for the DID, such as a newer on-demand request, the
collapse rule keeps the earlier of the two times. When a DID has been
failing for
`backfill.terminal_after` (7 days, counted from its first failure in a
row) the failure is terminal: the DID gets an `unreachable` debt,
counted in `unreachableRepos`, its cycle membership is marked
`terminal`, and it is no longer retried by itself.

## Scheduler

One scheduler runs a pool of `backfill.concurrency` workers (default
32) and serves three tiers.

| Tier | Contents | Guaranteed share |
|---|---|---|
| 1, on-demand | `requestBackfill` repo jobs, discovery jobs, list jobs (requester `system:lists`), re-lists owed by debts, `#sync` and poisoned events (requester `system:resync`) | 60% |
| 2, active | authors first seen on the firehose (requester `system:firehose`) | 25% |
| 3, sweep and repair | members of sweep and repair cycles (`system:sweep`, `system:repair`) and their retries | 15% |

The shares are `backfill.tier_shares` (three percentages that sum to
100).

- **Shares are minimums.** Among the tiers that have work, the next
  free worker goes to the tier whose running count is lowest relative
  to its share. A share nobody uses flows to the others.
- **Workers kept for tier 1.** A lent worker comes back only when its
  job ends, so `backfill.on_demand_reserved` workers (default 4, and
  never more than `concurrency − 1`) are not lent: tiers 2 and 3
  together use at most `concurrency − on_demand_reserved`. An
  on-demand request finds a worker however long the sweep's jobs run.
- **Fairness inside tier 1** is cost-based deficit round-robin across
  requesters: each API token, the admin token, `system:lists`,
  `system:resync`. A requester is charged the outbound requests its
  jobs make; the least-charged requester with work goes next, and one
  that joins starts at the current minimum. A job is charged one
  request when it is dispatched, each further request as it makes it,
  and at its end whatever its cost says beyond that. So the jobs
  already running count, a long job included: several free workers
  are shared between the requesters that have work, not all given to
  the one that looked cheapest a moment ago.
- **Claims.** Dispatching a queue entry claims it
  (`backfill_queue.claimed_by`, `claimed_until`) for 10 minutes; the
  claim is renewed every minute while the job runs, and the entry is
  deleted when the job has ended. A job that did not run because the
  DID's lease was held gives its entry back to wait.
- **A job that panics** is logged and counted in
  `farsight_task_panics_total{task="backfill_job"}`, and recorded as
  a failed job of its kind: a repo job is retried on
  `backfill.retry_schedule` and becomes an `unreachable` debt if it
  keeps failing, a list fetch run counts a failed attempt, a phase-1
  check is put off by an hour. Its worker and the mark that its DID
  is being worked on are given back when its task ends, however it
  ends, so no failure of a job shrinks the pool.
- **Priority inside a requester**: `high` before `normal`, four to
  one. Within a priority, oldest first.
- **Caps.** At most 10,000 waiting entries per API requester
  (`QueueFull` beyond), of which at most 100 `high` (further ones are
  downgraded). Tier 2 stops taking entries when the queue holds
  about 1,000,000 (the planner's estimate of the table is read, the
  queue is not counted for every new author); overflow is dropped,
  because the sweep covers those repositories anyway.
- **System requesters** (`system:lists`, `system:resync`) keep at
  most `backfill.system_queue_cap` (50,000) queue entries. The durable
  source of truth is the debt table and the list state; a feeder, run
  every 10 seconds, enqueues from them as capacity frees and only when
  a re-list can clear the debt. Nothing is refused for queue space.
- **Collapse rule.** There is one *waiting* entry per `(actor, kind)`;
  the entry of a running job is claimed and apart from it. A new
  request for a DID that already has a waiting one upgrades it: the tier
  becomes the more urgent of the two, `not_before` the earlier (a new
  request means now), the priority the higher, and the requester that
  of the more urgent request, who is charged. So an on-demand request
  for a DID parked in a tier-3 backoff moves it to the front. A
  request for a DID whose job is *running* adds a waiting entry only
  with `force`; system requesters always add one, since their cause
  may postdate the running job's start.
- **Storage.** At 90% of the storage budget tier 3 is not dispatched.
  At 100% and above, tier-2, tier-3, API-key tier-1, resync and
  `list_fetch` jobs whose repository is on a host that is not a large
  host run **deletes-only**: the listing runs, deletions and
  reconciles apply, inserts are skipped and the author gets a
  `refused` debt. The outcome is complete-with-debts. A job that
  crosses the threshold in mid-run switches for its remaining pages.
  At the hard ceiling every job except an admin-requested one is
  deletes-only. Admin-requested jobs run normally under the budget.
  See [security.md](security.md) for the budget and the large-host
  rule.
- **Pacing of tier 3**: `backfill.sweep.max_repos_per_hour` (0, the
  default, leaves it bounded only by the hosts).

### What a stopped process leaves

A process that is stopped gives its work back: the scheduler stops
its jobs, releases the claims on their queue entries (they wait
again) and deletes their leases. A process that is killed cannot. What
it held is told by a claim or a lease that ran out, and the feeder
takes it up every 10 seconds:

| Left behind | Told by | What happens |
|---|---|---|
| A claimed queue entry | `claimed_until` has passed | The claim is taken off and the entry waits again. If a newer waiting entry exists for the same account and kind, that one stands for both. |
| A repo job marked `running` with no entry | no live lease, no queue entry, not an outstanding member of an open cycle | `backfill_state.state` becomes `queued` and a tier-2 `system:resync` entry is added. |
| A cycle member | its `cycle_outstanding` row, and no live lease | The scheduler dispatches it again in its turn. |
| A list fetch run | `finished_at` is NULL, no live lease on the owner, no `list_fetch` entry | The owner is queued, and its job resumes the run with the lists it had claimed. |

Until then `getBackfillStatus` reports the job as `queued`.

### Politeness per host

Every outbound request waits for a slot on its host:

| Limit | Key | Default |
|---|---|---|
| Token bucket, requests per second per host | `backfill.per_host_rps` | 10 |
| Concurrent requests per host | `backfill.per_host_concurrency` | 4 |
| Token bucket, requests per second over all hosts of one registrable domain | `backfill.per_domain_rps` | 20 |
| Concurrent requests over all hosts of one registrable domain | `backfill.per_domain_concurrency` | 8 |
| PLC directory, requests per second in total | `backfill.plc_rps` | 10 |

- A host is its name and port. The registrable domain is the one the
  cap buckets use (eTLD+1 by the public suffix list), so an operator
  who gives every account a host name of its own under one domain
  gets one domain's worth of requests, not one host's worth for each
  name. A domain is never held below what one of its hosts is
  allowed. The hosts in `limits.large_hosts` are exempt from the
  per-domain limits: each is limited by itself.

- A `429`, or a response with `RateLimit-Remaining: 0`, cools the host
  for the seconds its `Retry-After` names, at most 1 hour (60 seconds
  when it names none, or a date). Requests to a cooling host fail at
  once and the job is retried later.
- **Circuit breaker.** Five consecutive failures on a host trip it for
  1 minute; a second trip within an hour of the last lasts 1 hour.
- A request waits at most 2 minutes for a slot. A slot is given back
  when its request ends, also when the request is abandoned midway.
- The limiter remembers a host while it has a request in flight, a
  cooldown, a recent breaker trip, or was asked in the last 10
  minutes; beyond 10,000 hosts the others are forgotten.
- Half of `plc_rps` is reserved for DID resolution; enumeration of the
  PLC export uses the other half only.
- The User-Agent is `farsight/<version> (+https://<hostname>;
  <contact>)`.

These limits belong to the `farsight-backfill` process. The server
makes its own lookups for the public pages (handles, profile cards)
under its own budgets, and the two processes share no limiter state.

With the default rate, one large host is the practical bottleneck: a
host that serves most of the network is read at 10 requests a second
whatever `backfill.concurrency` is.

## The sweep

A sweep cycle enumerates a source of DIDs and gives each a `repo` job
in tier 3. The first cycle is the baseline that takes a fresh instance
from `sweep_incomplete` to complete coverage.

### Sources

`backfill.sweep.source`:

| Source | Enumerates | Notes |
|---|---|---|
| `relay_collections` (default) | `com.atproto.sync.listReposByCollection` on the relay, for `block`, `listblock` and `list` in turn | Only accounts that have one of those collections; as complete as the relay's collection directory |
| `relay_repos` | `com.atproto.sync.listRepos` on the relay | Every repository the relay knows; expect most to hold none of the four collections and end after one `describeRepo` |
| `plc` | the PLC directory's `/export`, in order | Every `did:plc` ever registered, and no `did:web` |

The relay is `backfill.relay_url` (default `https://bsky.network`).
Before every `relay_collections` cycle the process probes the relay
with a request for one repository. If the relay does not have
`listReposByCollection` (it answers `MethodNotImplemented`, or `404`,
`405` or `501`), the cycle uses `relay_repos` and a warning is logged;
a relay that has gained the method by the next cycle is used with it
again. A relay that is down or busy says nothing about the method:
the cycle starts on `relay_collections` and its pages are retried. If
a page of a cycle under way is answered with "no such method", that
cycle continues with `relay_repos` from the start of the listing; the
members it already enumerated stay. `sweep_cycles.source` records
what a cycle uses.

Every member has to be resolved before it can be read, so for a large
source expect resolution at `plc_rps` to set the pace of the first
sweep; the number of DIDs divided by `plc_rps` is its lower bound. A
PLC mirror with a
higher limit (`backfill.plc_url`, `backfill.plc_rps`) shortens that.
With the `plc` source, `backfill.plc_seed_from_export = true` takes
each account's PDS from the export itself. The export is a history:
the endpoint of an operation may have been replaced by a later one
that the cycle has not read yet. A seeded endpoint is used like any
cached resolution. A repo-level error at it, or a repository that
looks diverged there, makes the job resolve the account at the
directory before it acts. An account that moved and whose old host
still serves its old copy without error is listed from that copy
until its resolution is refreshed, so with this option the first
listing of such an account can be stale.

### Cycle start

- No cycle starts before the firehose has committed its first batch.
  A cycle's effective start is `max(started_at, first_applied_at)`,
  mapped to the witness clock and stored as
  `sweep_cycles.effective_start_witness`. This is what makes "sweep
  plus firehose" gapless: everything after the effective start is the
  firehose's to deliver.
- A full cycle starts when `backfill.sweep.enabled` is true, none is
  open, storage is under 90% of the budget, and either no full cycle
  ever ran or `backfill.sweep.full_every_days` days have passed since
  the last one started. With the default `0` there is one baseline
  cycle and no periodic one; later coverage comes from the firehose
  and from repairs.

### Enumeration, checkpoints and `cycle_outstanding`

```sql
CREATE TABLE cycle_outstanding (
  cycle_id BIGINT NOT NULL,
  did      TEXT COLLATE "C" NOT NULL,
  state    SMALLINT NOT NULL,   -- 1 outstanding, 2 terminal
  PRIMARY KEY (cycle_id, did)
);
```

Outstanding membership is persisted, keyed by DID text so that a
member needs no `actors` row.

- Every 5 seconds, each open cycle enumerates one page (at most 2,000
  entries) while its outstanding rows are fewer than
  `backfill.sweep.max_outstanding` (10,000). A page asks for no more
  than the room left, so the bound holds.
- An entry of the page that is not a valid DID is left out and
  logged: it names no repository. A member that is not a valid DID
  all the same is set to `terminal` when it is dispatched, so it does
  not keep its cycle open.
- The page's DIDs are inserted in the same transaction that advances
  `sweep_cycles.checkpoint`. A DID whose last job ended clean,
  complete-with-debts or inactive at or after the cycle's effective
  start is not inserted: it has already satisfied the cycle.
- The scheduler dispatches members straight from `cycle_outstanding`
  in DID order, skipping DIDs with a live lease or a queued repo job.
- A member's row is **deleted** when a job for the DID, in any tier,
  that *started* after the cycle's start ends clean,
  complete-with-debts or inactive. An on-demand job satisfies the
  sweep too, and so does the `describeRepo`-only job of an empty
  repository.
- A failed member goes to `backfill_queue` in tier 3 for its retries
  (which creates its `actors` row) and stays outstanding. The
  checkpoint advances past it.
- On terminal failure the row is set to `terminal` and the DID gets an
  `unreachable` debt.
- A failed enumeration page is retried at the next tick.

A full cycle pauses, enumeration and dispatch both, while
`backfill.sweep.enabled` is false (`admin.pauseSweep` sets it) or
storage is at 90% of the budget.

### Completion

A cycle completes when enumeration has finished and no row is still
outstanding; terminal rows do not hold it up, they stay counted in
`unreachableRepos`. Completion writes `completed_at` and
`completed_witness`, deletes the cycle's `cycle_outstanding` rows and
notifies the coverage snapshot. A completed full cycle also heals
every firehose gap that closed before its effective start.

Progress is published as
`farsight_backfill_sweep_progress_ratio{cycle_kind}`, measured over
what has been enumerated so far, and, once enumeration has finished,
`farsight_backfill_sweep_eta_seconds`: outstanding members divided by
the rate of the trailing hour.

## Gap repair

A deletion that happens while the firehose is not listening is lost:
nothing later says that the record went. When the firehose records a
gap (see [firehose.md](firehose.md)), coverage drops to `partial`
with the reason `firehose_gap` until a **repair cycle** has re-read
every repository that could have changed in it.

### What a repair covers

- One repair covers **all** gaps that are closed, unhealed and not
  yet claimed when it starts, from the earliest `from_at` among them
  (`sweep_cycles.repair_from`). It claims them by setting
  `firehose_gaps.repair_cycle_id`. Gaps that close while it runs wait
  for the next repair; there is never one repair per gap. The cause
  of a gap plays no part: a gap recorded because seam windows could
  not be read again (`seam_unrepaired`) is claimed and healed like
  one recorded at a reconnect.
- At most one repair cycle is open at a time (a unique index says
  so). The process and `admin.startRepair` start a repair under the
  same lock and look for an open one first, so whichever comes second
  finds the first one's cycle.
- An open gap, one whose stream is still down or still on a v1
  Jetstream, waits until it has closed.

### Candidates

A repair walks the relay's `com.atproto.sync.listRepos`, which carries
each repository's `rev` and `active` flag, whatever the sweep source
is. An entry becomes a member when either holds:

- **It changed since the gap.** The time inside its rev is at or
  after `repair_from − backfill.repair_slack − lag`, where
  `repair_slack` defaults to 1 hour and `lag` is the firehose's
  current distance behind the present. An entry with no readable rev
  is taken as changed.
- **It was reactivated unseen.** The relay reports it active while
  Farsight holds it with a status other than active, or with
  `inactive_at_listing`. A reactivation does not bump the rev, so this
  catches an `account` event lost in the gap. Such an account gets a
  `resync` debt at once and its `unavailable` lists are re-admitted.

Each member gets the ordinary `repo` job in tier 3 under the requester
`system:repair`, with the membership rules of the sweep. The range
reconcile of the job is what removes the rows whose deletion was lost.

When the cycle completes, the gaps it claimed get `healed_at` and
`healed_witness`.

### How long it takes

A repair has to walk the whole of the relay's `listRepos` to find its
candidates, under the same per-host rate as everything else, and then
re-read every candidate. The longer the gap, the larger the share of
accounts that changed during it: expect a repair after a long gap to
run for days. Coverage stays `partial` for all of it.

While a repair runs, the admin alert reads "being repaired" with the
cycle's id and the number of accounts re-read so far, and the
dashboard's "Catching up" block has a "Gap repair" line with the same
number; how many are left is not known until the walk of the relay's
list has finished. The Operations page shows whether the cycle is
running or paused.

To make one account current without waiting, request its backfill
with `force` (below).

### When the relay is unavailable

If `listRepos` cannot be reached when a repair starts, the repair
re-lists every DID in `backfill_state` plus the owners of tracked
lists instead (source `known_dids`). That refreshes what Farsight
already holds but cannot find accounts it has never seen, so it claims
no gap and heals none; the gaps wait for the next repair or the next
full cycle.

### Controls

```toml
[backfill.repair]
auto_start = true   # a repair starts by itself when a gap has closed
paused = false      # true holds repairs; one under way keeps its place
```

| Control | Effect |
|---|---|
| `backfill.repair.auto_start` (default `true`) | When true, the backfill process starts a repair by itself as soon as a closed, unclaimed, unhealed gap exists and no repair is open. When false, a closed gap waits for `admin.startRepair`. |
| `backfill.repair.paused` (default `false`) | Holds repairs. A paused repair enumerates nothing and its members are not dispatched; it keeps its checkpoint and continues from it when released. Retries already in the queue may still run. |
| `admin.startRepair` | Creates a repair cycle over every closed, unhealed gap, or returns the open one. The backfill process picks the row up, sets its effective start and claims the gaps. Returns `{ cycle, from, gaps }`; `gaps: 0` and no cycle when there is nothing to repair. |
| `admin.pauseRepair` | Input `{ "paused": bool }`. Sets `backfill.repair.paused`. |
| `admin.cancelRepair` | Cancels the repair under way. Returns `{ cycle, autoStart: false }`. |

`admin.cancelRepair` does this, in order:

1. It sets `backfill.repair.auto_start = false`. Without that the
   backfill process would start the same repair again at its next
   look. If the config cannot be written (one managed through the
   environment), the call fails and cancels nothing.
2. Under the same advisory lock that `admin.startRepair` takes, in one
   transaction, it deletes the queue entries of requester
   `system:repair`, the cycle's `cycle_outstanding` rows and the cycle
   itself, and releases its gaps (`repair_cycle_id = NULL`) unhealed.

Jobs already running finish, and what they wrote is kept. The gaps
remain open for coverage; a later `admin.startRepair`, or turning
`auto_start` back on, repairs them from the beginning.

All four are admin-token procedures outside the API stability
contract. The admin UI has a button for each; see the
[admin UI guide](../guide/admin-ui.md).

### What a repair cannot find

An account that was inactive when the sweep ran, is absent from a
sweep source that omits inactive repositories, and whose reactivation
event falls in a gap, is found by the repair's reactivation rule only
if Farsight already holds a row for it. Otherwise it is found when it
next writes one of the indexed collections, or by the next full
cycle (`backfill.sweep.full_every_days`).

## List jobs

A list's members are fetched only once somebody block-subscribes to
it. The state machine, its events (named here by their codes, such as
**NF** or **FT**) and the admission rules are in
[list-indexing.md](list-indexing.md#the-transition-function); this
section is the work the backfill process does for it. Bookkeeping
lives on the `lists` row. `list_jobs` is only the waiting queue for
phase 1, and phase 2 is a `backfill_queue` entry of kind `list_fetch`
for the list's **owner**.

### Phase 1: record check and gate

Phase 1 runs for a list that is `pending`, `unavailable` or `missing`;
a `list_jobs` row for a list in any other state is deleted. It holds
the owner's lease for the record check only, and is offered again
later if the lease is taken.

**The record check.** If the list record is already stored, the check
passes without a request. Otherwise:

1. Resolve the owner. A DID that does not exist is *not found*; a
   tombstoned DID is *owner inactive*; a resolution error is an
   *error*.
2. `getRecord` for the list at the owner's PDS. A record that comes
   back is applied as a write with stamp 0, so it never overrides a
   record the firehose delivered meanwhile. Applied: *present*.
   Refused by a cap: *refused*.
3. Anything else, a missing record or any error, is tried once more
   after resolving the owner again without the cache. Not-found is
   believed only from that second answer.
4. On the second attempt: a missing record is *not found*. On
   `RepoNotFound` the relay's `getRepoStatus` decides: not active is
   *owner inactive* (and the status is stored), active is *not found*,
   no answer is an *error*. On another repo-level error the relay is
   asked too: not active is *owner inactive*, anything else an
   *error*. Every other failure is an *error*.

**What each result does.**

| Result | Effect |
|---|---|
| present | The list is read again. If it was re-admitted meanwhile, or is no longer `pending` or `unavailable`, nothing more happens. Otherwise the host gate below, then the pass. |
| refused | **GF**: the list is `deferred`. |
| owner inactive | **OI**. The `list_jobs` row is deleted: only the owner becoming active again (**OA**, a new admission) revives the list. A `missing` list instead keeps its re-check schedule, and the attempt does not count. |
| not found | A list not yet `missing` fires **NF** and is re-checked after the first step of `backfill.missing_retry` (default `1h`, `24h`, `7d`). A `missing` list moves one step along the schedule; a not-found after the last step fires **NFx** and the list is `dead`. |
| error | Errors are not "not found". A `missing` list is re-checked at its current step again, weekly once the schedule is used up, and never moves toward `dead` on an error. Any other list retries on `backfill.phase1_retry` (default `5m`, `20m`, `1h`); after those, a `pending` list fires **FT** (`unavailable`, counted) and the check repeats weekly. An owner that errors only for Farsight therefore ends up counted, never silently dropped. |

**Host gate.** After a *present* result, if the owner is on a host
that is not a large host and one of its cap buckets is marked full
for list items, the list fires **GF** and is `deferred`. A bucket is
marked at 100% of a cap and unmarked below 95%; the server reopens
the lists waiting on it (see [security.md](security.md#cap-buckets)).

**The pass.** Under the list's lock, and only if the list has not
been re-admitted since the check began: `phase1_epoch = admit_epoch`,
the `list_jobs` row is deleted, and a tier-1 `list_fetch` entry for
the owner is enqueued under `system:lists`.

### Phase 2: the fetch run

One run serves every claimable list of an owner.

- **Cooldown.** Unless an unfinished run exists, an owner whose last
  run started less than `backfill.owner_fetch_cooldown` (10 minutes)
  ago is put back for the remainder. A held lease puts the entry back
  for 60 seconds.
- **Start.** Resolve the owner (tombstoned: the account is set
  `deleted` and the run ends; any other failure: retry in 5 minutes).
  Insert a `list_fetch_runs` row with the coverage point, the witness
  clock at that moment. Read the stamp `R` with `getLatestCommit`.
  Then, in one transaction holding the locks of the lists it takes,
  claim the claimable lists, at most 500 of them, lowest id first:

  ```sql
  UPDATE lists SET fetch_run_id = $run, fetch_run_epoch = admit_epoch
  WHERE id = ANY($claimable)
    AND ((track_state IN (pending, unavailable)
          AND phase1_epoch = admit_epoch)
      OR (track_state IN (ready, retained) AND refresh_requested))
  RETURNING id
  ```

  Lists beyond the 500 are claimed by the owner's next run. A run
  that claims nothing ends there. Every page is read after this
  commit, through the run's own cursor, so every claimed list saw
  every page.
- **Listing** is step 9 of the per-repo job for the `listitem`
  collection alone, with the run's id as the cursor key.
- **End.** For each list still carrying the run's claim, under the
  list's lock: if `fetch_run_id` is this run and `fetch_run_epoch`
  still equals `admit_epoch`, fire **OK** with the run's coverage
  point and whether any item was refused. What **OK** does to each
  state is in [list-indexing.md](list-indexing.md#table). Lists
  admitted after the claim are not promoted by this run. The claim is
  then released and the run's cursor rows deleted.
- **Deletes-only.** A run that listed any page under the deletes-only
  rule promotes nothing and releases its claim; the lists are claimed
  again by a later run.
- **Owner inactive.** On a repo-level error the relay is asked. If it
  says the account is not active, the status is stored, the claimed
  `pending` lists fire **OI**, and the run ends without promoting.
- **Failure.** Any other error, and a run that exceeds
  `backfill.list_fetch_max_duration` (1 hour), is a failed attempt:
  `fetch_attempts` is incremented on every claimed list, and a
  `pending` list that has reached `backfill.list_fetch_max_attempts`
  (2) fires **FT**. The claim is released. The owner is fetched again
  in 5 minutes, or in a week once every claimed list is out of
  attempts; `unavailable` lists stay claimable and are retried weekly
  without end. Waiting in the queue, the cooldown and a failure
  before the claim do not count as attempts.
- **Page bound.** A run that lists 25,000 pages without reaching the
  end is a failed attempt like one that runs out of time.
- **Any other error** (the database, the resolver) closes the run all
  the same: the claim is released, the run is finished as failed, and
  the owner is queued again in 5 minutes. No attempt is counted for
  it. No path leaves a run open with lists claimed.
- **Resume.** A run left unfinished because the process was killed is
  taken up by the feeder: it queues the owner again, and the run is
  resumed with its run id, cursor, claimed set and coverage point. It
  keeps its stamp if that was read less than 72 hours ago and
  otherwise reads a new one.

**Wall-clock bound.** Once a minute, every list `pending` for longer
than `limits.pending_max_age` (3 hours) since its admission fires
**FT** and becomes `unavailable`, which coverage counts. Its queue row
and lanes are untouched, so it keeps its place and is promoted when
its turn comes.

### Lanes

Inside the requester `system:lists`, phase-1 checks and fetch runs
share one deficit round-robin over **admission keys**
(`bucket:<host cap key>` for an author on an ordinary host,
`did:<did>` on a large host, `unresolved:<did>` for an author not yet
resolved; see
[security.md](security.md#admission-keys-and-daily-rates)).

- A phase-1 item is in the lane of every key that has a row for the
  list in `list_sched_keys`, plus one fallback lane that every item
  is in.
- A fetch run is in the lanes of the keys of its owner's `pending`
  and `unavailable` lists that passed phase 1, plus the fallback
  lane. A run that only refreshes is in the fallback lane alone.
- The least-charged lane that has a servable item serves its oldest
  admission. A lane new to the round starts at the current minimum.
  A lane is charged the outbound requests of what it served: one
  when the item is dispatched, the rest when it ends. At most 5,000
  waiting items of each kind are considered at a time, the oldest
  admissions first.
- An item is servable when it is not already running and its owner's
  host has a free slot and is not cooling down. A lane whose head is
  blocked serves its next servable item, or yields its turn.

The effect is that an account flooding listblocks only lengthens the
lanes of keys it controls. Every other subscriber's key, and the
fallback lane, still carry the list; and a slow host stalls only its
own items.

## On-demand backfill

Both endpoints need a bearer token with the `backfill` scope (or the
admin token). Full request and response shapes are in
[api.md](api.md).

### `app.nearhorizon.farsight.admin.requestBackfill`

Input `{ "actor": "did:…", "priority": "normal" | "high",
"force": false }`, as a JSON body or as query parameters. `high`
needs the `backfill:high` scope; without it, or beyond 100 waiting
`high` entries, the request is downgraded and the response says so.

It asks for a tier-1 `repo` job for the actor: the actor's outgoing
blocks, listblocks and lists, and the items of the actor's tracked
lists. The listblocks it finds may admit lists the actor subscribes
to. With a backlink source configured it also asks for a subject
discovery.

| State of the actor | Effect |
|---|---|
| A repo job is already waiting | No new work. The waiting entry's tier and priority are raised if the request is more urgent. |
| A repo job is running | No new work, unless `force`, which adds a waiting entry that runs after the current job. A `list_fetch` or `discovery` job holding the lease does not count as running. |
| `done` less than `backfill.request_fresh_window` ago (default 1 hour), no `force` | No new work. |
| Otherwise (never done, failed, stale, or `force`) | Enqueued. |

It answers `202` with the body of `getBackfillStatus` plus `enqueued`
and `downgraded`. `QueueFull` is returned when the requester already
holds 10,000 waiting entries, or when its daily allowance for
creating account rows is used up.

### `app.nearhorizon.farsight.query.getBackfillStatus`

Parameter `actor`. A pure read that never enqueues.

```json
{ "actor": "did:plc:…",
  "repo": { "state": "done", "lastBackfilledAt": "2026-01-01T00:00:00Z" },
  "discovery": { "state": "disabled", "truncated": false } }
```

`repo.state` is an open enum:

| State | Meaning |
|---|---|
| `never` | Nothing known and no baseline yet. |
| `queued` | A repo job waits; `position` estimates the entries of its tier served before it. Also reported for a job that a stopped process left behind, until it is taken up again. |
| `running` | The scheduler holds the job's queue entry, or (a cycle member) a repo job holds the lease. |
| `done` | The last job ended clean, complete-with-debts or inactive. |
| `failed` | The last job failed; `lastError` says why. |
| `covered_by_sweep` | No row exists for the DID, but a completed full cycle covers its repository. `lastBackfilledAt` is that cycle's completion. |

`discovery.state` takes `never`, `queued`, `running`, `done`, `failed`
and, with no backlink source configured, `disabled`.

## Subject discovery with a backlink index

A repo job makes Farsight current on what an account *does*. Who
blocks that account is spread over every other repository, and is as
complete as the sweep. Optionally, Farsight can ask a backlink index
for the records that point at the account and fetch those directly.

It is off by default. Set `backfill.backlinks.url` to an index that
serves `GET /links?target=&collection=&path=` and answers with
`linking_records` (each a `did`, `collection`, `rkey`) and a `cursor`.
Then `requestBackfill(X)` also runs a `discovery` job for `X`:

1. `app.bsky.graph.block` records with `.subject = X`.
2. `app.bsky.graph.listitem` records with `.subject = X`, in the
   repository of the list's owner.
3. For every list found in step 2, `app.bsky.graph.listblock` records
   with `.subject` equal to that list.

Nothing the index says is trusted. Every reference is fetched with
`getRecord` at its author's PDS and parsed; only a record that exists
is applied, with stamp 0, so it never overrides anything stored from
the firehose or a listing. Verified listblocks admit their lists the
normal way.

- The cost is charged to the requester.
- `backfill.backlinks.max_refs` (200,000) caps the references taken
  across the three steps. A run that hits it keeps its verified
  records and is marked `truncated`.
- A reference is settled when its record was read, or when there is
  an answer that it does not exist: the author's PDS says
  `RecordNotFound`, or the directory says the author does not exist
  or is tombstoned. A reference that could not be checked (the author
  did not resolve for a reason that may pass, or the PDS did not
  answer the read) may be real, so the run is marked `truncated` too.
- Every list found in step 2 is recorded in `subject_lists(X, L)`
  whatever its state; coverage uses it to know which pending lists
  could still add a block on `X`. An untruncated run replaces the
  set: lists an earlier run found and this one did not are removed.
- State is in `discovery_state`: `state`, `started_at`,
  `discovered_witness`, `completed_at`, `truncated`, `refs_found`,
  `source`, `last_error`.
- **Only an untruncated completion** writes `subject_coverage` for
  `X`, which lets `freshness` report incoming state as `assisted`.

For an AppView enrolling a member: without a backlink source,
`requestBackfill` makes Farsight current on the member's own actions,
and "who blocks the member" is as complete as the sweep, which
`freshness` states. With one, incoming state is filled in within
minutes, bounded by `max_refs`. See the
[AppView guide](../guide/appview.md).

## Configuration and metrics

Every `[backfill]` key, with its default, is listed in the
[configuration reference](operations.md#configuration-reference); the
keys this page relies on are named where they apply. The metrics of
the process, served on `metrics.backfill_bind`, are in
[operations.md](operations.md#metrics).
