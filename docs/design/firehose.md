# Firehose ingestion

Farsight follows the network live through Jetstream, the JSON
firehose. This page covers which Jetstream protocol it speaks and why
that matters for coverage, the pipeline from websocket to Postgres,
how it resumes after a disconnection or a failover without silently
losing events, how a loss it cannot avoid is recorded as a gap, and
what each kind of event does.

Everything that existed before the firehose was first connected, and
everything inside a recorded gap, is the backfill process's work; see
[backfill.md](backfill.md). Ingestion runs inside the `farsight`
server process.

## Source and protocol

Farsight subscribes to four collections and nothing else:
`app.bsky.graph.block`, `app.bsky.graph.listblock`,
`app.bsky.graph.list` and `app.bsky.graph.listitem`. `identity` and
`account` events arrive whatever the filter is. Frames are requested
zstd-compressed with the dictionary bundled in the binary; if an
instance has retired that dictionary, the session continues
uncompressed.

### v1 and v2

Jetstream has two protocols, and Farsight speaks both.

| | v1 | v2 |
|---|---|---|
| Endpoint | `/subscribe` | `/xrpc/network.bsky.jetstream.subscribeEvents`, websocket subprotocol `xrpc.v1.json` |
| Filter parameter | `wantedCollections` | `collections` |
| Cursor | `time_us`, microseconds | `seq`, a sequence number local to the instance; a timestamp is accepted too |
| Witness time of an event | `time_us` | `witnessedAt` (or `time` on older servers) |
| `#sync` events | no | yes |
| Cursor older than retention | clamped silently | refused with `CursorTooOld`, or announced with `#info OutdatedCursor` |

On every connect Farsight tries v2 first. An instance that answers the
v2 handshake with 404 does not offer it, and the session falls back to
v1.

**v2 is required for `complete` coverage.** A `#sync` event says that
a repository's commit chain was broken and its contents replaced: a
PDS restored from a backup, a rebase, a relay resync. After one,
records Farsight holds for that repository may no longer exist, and
no delete event will ever say so. On v2 Farsight hears the `#sync` and
re-reads the repository. On v1 it never hears it, so it cannot claim
to be complete; see [Running on v1](#running-on-v1).

Cursors are local to an instance on both protocols. A cursor from one
instance means nothing to another.

### Sources

`firehose.urls` is an ordered list of instances, in failover order.
The default is Bluesky's two public v2 instances:

```toml
[firehose]
urls = [
  "wss://jetstream.us-east.bsky.network",
  "wss://jetstream.us-west.bsky.network",
]
```

Bluesky's older `jetstream1` and `jetstream2` instances serve v1
only. An instance URL may be given with or without a trailing
`/subscribe`; the path is chosen by the protocol.

Jetstream does not forward commit signatures, so Farsight trusts the
instance it reads from. An operator who needs ingestion verified from
the source runs their own Jetstream and lists it here.

## Pipeline and backpressure

```text
reader ─► bounded channel (10,000 events) ─► single writer ─► Postgres
```

- **The reader** owns the websocket connection: protocol detection, resume,
  stall detection, gap detection, failover. It decodes and validates
  each event before queueing it. An event's position is validated
  with it: a witness time before the epoch or more than 24 hours ahead
  of the server's clock, or a `seq` that is negative or the largest
  value, is not an event. The frame is refused like one that cannot be
  decoded and the session ends, so a position an instance made up is
  never stored as the cursor or as `applied_through`. A websocket
  message, compressed or not, may be at most 16 MiB.
- **The channel** is bounded. When it is full the reader stops
  reading, and TCP pushes back on the instance. A connection the
  instance drops for that is resumed from the persisted cursor.
- **The writer** is a single task. It collects up to 500 events or
  250 ms, whichever comes first, and applies the batch in one
  transaction. There is one writer for ordering and atomicity;
  expect the four collections to be a small part of the network's
  traffic.
- Ingestion has its own database pool of 4 connections, separate from
  the API and from backfill.

The batch transaction also writes the new cursor (`seq` on v2,
`time_us` on v1), `firehose_state.applied_through` (the witness time
of the last applied event, kept as a running maximum) and one
`firehose_clock` row mapping the server's commit time to that witness
time. **The persisted cursor therefore never runs ahead of applied
data**, and a crash between any two statements loses nothing.

Session starts, gaps and disconnections travel through the same
channel as events. The writer flushes the events ahead of each, so
they are recorded in stream order.

**No event is dropped for rate reasons.** What an abusive account can
store is bounded by caps at the storage layer (see
[security.md](security.md)), not by discarding firehose traffic.

### Validation

A commit is dropped before it is applied, and counted in
`farsight_ingest_dropped_total{reason}`, when:

| Reason | Cause |
|---|---|
| `invalid` | malformed DID, a rev that is not a TID, a record that does not parse |
| `foreign_listitem` | a listitem naming a list in another repository; only the list's owner can add members |

Its position in the stream still counts, so the cursor moves past it.

### Poisoned events

A database error that is transient (a lost connection, a restarting
server, pool exhaustion, a serialisation failure, exhausted deadlock
retries) is retried without limit and is never held against an event.

For any other failure:

1. The batch is retried once.
2. If it fails again, its events are applied one by one.
3. An event that fails 3 times on its own is **poisoned**: it is
   logged to `op_errors`, its DID gets a `resync` debt and a tier-1
   re-list under the requester `system:resync`, and it is counted in
   `farsight_ingest_dropped_total{reason="poisoned"}` and in
   `pendingResyncs` until a clean re-list clears the debt. An hourly
   task turns a `resync` debt that is 7 days old into `unreachable`.
4. The batch's cursor and `applied_through` are then persisted in a
   transaction of their own.

So a single event that the code cannot apply never blocks the stream,
and what it would have changed is recovered by reading the repository.

### Storage calls outside a batch

The writer also writes what is not an event: the connected flag, gaps,
the purge of an account that became `deleted`, the record of a
poisoned event. These follow the same rule about errors. A transient
one is retried until it passes. Any other error is tried 3 times;
then the call is given up, logged, recorded in `op_errors` (with the
DID, when it concerns one account) and counted in
`farsight_ingest_storage_errors_total{op}`, and the writer goes on. No
single call that cannot succeed stops ingest.

What going on leaves behind:

| Call (`op`) | Left behind |
|---|---|
| `purge_account` | The account is `deleted` and its rows are still stored. They are withheld by the status; the purge is taken up again by the daily `account_purges` task and at the next start. |
| `record_poisoned` | The event has no `resync` debt. The operational error names its DID; `admin.requestBackfill` re-reads the repository. |
| `record_gap`, `open_sync_unavailable`, `close_sync_unavailable` | Nothing: these are not passed over. Coverage is claimed from the recorded gaps, so a gap that cannot be written stops the writer, which ends the process (see below); the next start resumes from the stored cursor and meets the gap again. |
| `mark_connected`, `set_connected`, `read_state` | The connected flag keeps its last value until the next session change. |

### A panic in the reader or the writer

The reader and the writer are one pipeline: a batch the writer holds
exists nowhere else. If either panics, the other ends with it, the
panic is logged and counted in `farsight_task_panics_total`, and the
server exits with an error. Its supervisor (the container's restart
policy) starts it again, and ingest resumes from the persisted cursor
like after any other stop.

## Cursors, reconnects and gaps

### Per-instance cursors

```sql
CREATE TABLE firehose_cursors (
  source_url           TEXT PRIMARY KEY,
  protocol             SMALLINT,      -- 1 v1, 2 v2
  cursor_seq           BIGINT,        -- v2, local to the instance
  cursor_us            BIGINT,        -- timestamp form
  last_connected_at    TIMESTAMPTZ,
  last_applied_through TIMESTAMPTZ
);
```

Each instance has its own row, written in the batch transaction and
kept with `GREATEST`, so a replay never lowers it. `firehose_state`
(one row) holds the current session's copy together with the global
`applied_through`, `first_applied_at` and the `connected` flag.

Failing over to another instance starts a new cursor space. The
previous instance's row stays, so a later return to it resumes from
that instance's own cursor, exact by `seq` on v2, rather than by
timestamp. An instance with no row is resumed as a failover.

### Resume plan

Before every connect the reader waits until the writer has flushed
everything already read, then reads the persisted state and decides:

| Situation | Cursor sent | Gap rule |
|---|---|---|
| First start, nothing applied yet | none (live tail) | none |
| Same instance, v2 after v2 | `seq + 1`, exact | none; if refused with `CursorTooOld`, connect at the live tail and record a gap |
| Same instance, v1 (or v2 after v1) | `cursor_us − 120 s` | gap if the cursor was clamped |
| Other instance, lag of the previous one known and at most `failover_max_lag` | `applied_through − max(failover_rewind_min, lag + 5 min)` | gap if the cursor was clamped |
| Other instance, lag unknown or too large | `applied_through − 30 min` | always a gap |

Replaying events that were already applied is harmless: every write
is last-write-wins by rev (see [storage.md](storage.md)), so a
replayed event is a stale no-op.

A first start has no gap by construction: the sweep takes its
starting point from the first committed batch (see
[backfill.md](backfill.md)).

### Reconnecting

- **Stall.** No message for `firehose.tuning.stall_timeout` (60 s)
  ends the session. An instance replaying a stretch with few wanted
  events can stay silent for longer than that, and a new session from
  the same cursor would meet the same silence. So every session that
  stalls without one event gives the next one twice as long, up to 16
  times the timeout; a session that delivers an event sets it back.
- A closed socket, a read error or a v2 error frame ends the session.
- **Backoff.** The reader waits before each reconnect: 0.5 s at
  first, doubled every time, up to 30 s. After a session that
  delivered events and lasted at least 60 s the wait starts again at
  0.5 s. So a process that has run for months reconnects as promptly
  as a new one, and an instance that drops every session within
  seconds is approached more and more slowly.
- **Failover.** A session **fails** when it cannot be opened, or
  when it ends without having delivered a single event: closed, an
  error, or silence for the stall timeout after the socket was
  accepted. After 3 failed sessions in a row on an instance the reader
  moves to the next URL in `firehose.urls`, wrapping around. With one
  URL it keeps retrying that one. The wait is kept across a failover,
  so with every instance down the attempts still slow down to one
  every 30 s.
- `admin.restartFirehose` drops the session; the reader reconnects at
  once from the persisted cursor.

### Failover rewind

A sequence number cannot be carried from instance A to instance B, so
B is resumed by timestamp. Two instances are not equally far behind
the network, so resuming B at A's last witness time could skip events
that B saw earlier than A did.

The reader keeps, for the current instance, the median of
`witness time − commit time` over its last 1,000 commit events, where
the commit time is the timestamp inside the commit's rev. That is the
instance's lag. On failover B is resumed at

```text
applied_through − max(firehose.tuning.failover_rewind_min, lag_A + 5 min)
```

with B's own lag taken as zero, which is always the safe side.
`failover_rewind_min` defaults to 10 minutes. If A's lag was never
measured, or exceeds `firehose.tuning.failover_max_lag` (30 minutes),
no rewind can be trusted: B is resumed 30 minutes back and a gap is
recorded regardless.

### How a gap is detected

A gap is an interval of witness time in which events may have been
missed. It is detected when the first event of a resumed session
arrives:

- **`CursorTooOld`** (v2, same instance). The gap is
  `[applied_through, first live event]`.
- **Clamp.** The instance replayed from later than asked. This is
  taken to have happened when the instance said so (`#info
  OutdatedCursor`) or when the first event's witness time is more
  than `firehose.tuning.gap_threshold` (300 s) after the requested
  cursor. A gap is recorded only when that first event lies **after**
  the gap's start; if the instance replayed from an older point than
  Farsight's position, nothing was skipped. On the same instance the
  gap starts at `applied_through`; on a failover it starts at
  `applied_through − 30 min`.
- **Failover without a safe rewind.** Always
  `[applied_through − 30 min, first event]`.

```sql
CREATE TABLE firehose_gaps (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_at         TIMESTAMPTZ NOT NULL,   -- witness clock
  to_at           TIMESTAMPTZ,            -- NULL while still open
  cause           SMALLINT NOT NULL,
  detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  healed_at       TIMESTAMPTZ,
  healed_witness  TIMESTAMPTZ,
  repair_cycle_id BIGINT
);
```

| `cause` | Name | Recorded when |
|---|---|---|
| 1 | cursor too old | a v2 instance refused the cursor |
| 2 | heuristic | a same-instance timestamp resume was clamped |
| 3 | failover | a cross-instance resume was clamped, or had no safe rewind |
| 4 | sync unavailable | an interval was spent on v1 (below) |

Gaps of causes 1 to 3 are recorded closed. Recording, closing or
healing a gap sends `NOTIFY farsight_coverage`.

### What a gap does

An unhealed gap makes network-wide and list coverage `partial` with
the reason `firehose_gap`. It is healed by a repair cycle that
re-reads every repository changed during it, or by a full sweep that
starts after it closed; healing sets `healed_at` and
`healed_witness`. Repairs, their duration and their controls are in
[backfill.md](backfill.md#gap-repair).

**Being disconnected is itself a gap for coverage**, without a row:
while `connected` is false, or `applied_through` is more than
`firehose.tuning.synthetic_gap_lag` (5 minutes) behind, answers
report `firehose_disconnected` or `firehose_lagging`. It ends when the
stream is resumed without loss. See [coverage.md](coverage.md).

### Seam repair

At the moment a resumed session passes from replay to the live tail,
an instance may drop events witnessed very close to that hand-over,
while a later replay of the same window returns them. Farsight
therefore re-reads the seam once after every resume from a prior
position: same instance on either protocol, failover, or recovery
from a clamp. It is skipped only on the first start ever.

- A session has **caught up** when an event's witness time first
  comes within `seam_repair_catchup_margin` of wall time. For an
  ordinary reconnect that is immediate; after a long replay it is
  correspondingly later than the connect.
- `seam_repair_delay` after catching up, a second connection to the
  same instance reads the window
  `[connect − seam_repair_before, caught up + seam_repair_after]` with
  a timestamp cursor and hands the events to the writer.
- Those events go through the normal apply path, where last-write-wins
  turns everything already applied into a no-op.
- A repair batch **never moves position state**. It writes no cursor,
  no `applied_through`, no `firehose_clock` row, and opens or closes
  no gap. The live session may still be replaying behind it, and a
  cursor moved past data not yet replayed would make the next
  reconnect skip it. The repair re-covers a window known to be lossy;
  it does not redefine where the stream is.

| `firehose.tuning` key | Default |
|---|---|
| `seam_repair_before` | `150s` |
| `seam_repair_after` | `30s` |
| `seam_repair_delay` | `60s` |
| `seam_repair_catchup_margin` | `5s` |

## Event handling

Every event is applied inside the batch transaction, under the
author lock of its repository and the list locks it needs (lock order
in [storage.md](storage.md)).

| Event | Action |
|---|---|
| commit create or update, `block` | Validate; last-write-wins upsert. |
| commit create or update, `listblock` | Validate; last-write-wins upsert; adjust the list's subscriber counter and run the list's state transition, which may admit the list for fetching ([list-indexing.md](list-indexing.md)). |
| commit create or update, `list` | Last-write-wins upsert under the list's exclusive lock; the list's record is now present; state transition. |
| commit create or update, `listitem` | Authority check (the item must be in the list owner's repository); stored only if the list is tracked. |
| commit delete, any collection | Last-write-wins delete plus a tombstone; a listblock delete adjusts the counter. |
| `#sync` (v2) | For **any** DID, known or not: a `resync` debt and a tier-1 re-list under `system:resync`, counted in `pendingResyncs`. |
| `identity` | For a DID Farsight holds: clear its cached PDS, so the next job resolves it afresh, and note the account in `handle_due` so its handle is checked again. |
| `account` | See below. |

A commit on any other collection, and an event of an unknown kind,
is ignored.

`#sync` is rare. Re-listing a DID Farsight has never seen costs one
`describeRepo` for most, and catches repositories that arrive with
records already in them.

The first indexed record that the firehose delivers for an author
creates that author's row, and the same batch enqueues a tier-2
`repo` job for the author (requester `system:firehose`), so that the
records the author wrote before Farsight was listening are read too.
Rows created by listings or by discovery do not enqueue it.

`createdAt` inside a record is the author's claim and is used for
display only. Ordering is by rev.

### `account` events

For a DID Farsight does **not** hold:

- Nothing is stored and no job is queued. An unknown DID becoming
  active is counted only, in `farsight_firehose_events_total` with
  the labels `collection="account"`, `op="activate"`,
  `outcome="applied"`. If the account later writes one of the four
  collections, the rule above creates its row and queues its job.

For a DID Farsight holds:

1. `account` events carry no rev, so they are ordered by witness
   time. An event older than the stored `status_at` is stale and
   ignored, which makes replays idempotent.
2. The status is stored: active, or the upstream status
   (`deactivated`, `takendown`, `suspended`, `deleted`, `throttled`,
   `desynchronized`; anything else is `unknown`). What a status hides
   is in [storage.md](storage.md).
3. Becoming `desynchronized` adds a `resync` debt.
4. Becoming **active** from a hidden status, or with
   `inactive_at_listing` set (the last backfill found the account
   inactive), adds a `resync` debt and re-admits the account's
   `unavailable` lists.
5. Becoming `deleted` purges the account's authored rows after the
   batch commits, in several transactions; an interrupted purge is
   finished at the next start.

## Running on v1

On v1 `#sync` never arrives, so a diverged repository can keep
records in Farsight that no longer exist. Farsight does not pretend
otherwise:

- **The whole time spent on v1 is one gap.** When the first batch of
  a v1 session is applied, a `firehose_gaps` row of cause "sync
  unavailable" is opened, starting at `applied_through`. At most one
  such row is open at a time; further v1 sessions extend it. It is
  closed, at the witness time of the first event, when a v2 session
  takes over.
- While it is open, and whenever the current protocol is not v2,
  coverage is capped at `partial` with the reason
  `sync_events_unavailable`.
- Once closed it is healed like any other gap: the repair cycle
  re-reads every repository that committed during the interval, which
  covers any resync that happened inside it. An interval of days on
  v1 therefore means a repair of days; see
  [backfill.md](backfill.md#gap-repair).
- A `desynchronized` account status still triggers a re-list on v1.
- The heuristic clamp detection applies to v1 resumes, since v1 never
  announces a clamp.

An operator who needs `complete` coverage uses instances that offer
v2. The setup wizard tests each URL and reports which protocol it
speaks; see the [setup guide](../guide/setup.md).

## Configuration and metrics

| Key | Default | Meaning |
|---|---|---|
| `firehose.urls` | the two instances above | Instances, in failover order |
| `firehose.tuning.stall_timeout` | `60s` | Silence that ends a session |
| `firehose.tuning.gap_threshold` | `300s` | Distance between requested cursor and first event that counts as a clamp |
| `firehose.tuning.failover_rewind_min` | `10m` | Smallest rewind on failover |
| `firehose.tuning.failover_max_lag` | `30m` | Largest instance lag for which a rewind is trusted |
| `firehose.tuning.synthetic_gap_lag` | `5m` | Lag beyond which coverage treats the stream as behind |
| `firehose.tuning.seam_repair_*` | see above | Seam repair window and timing |

| Metric | Meaning |
|---|---|
| `farsight_firehose_connected{protocol}` | 1 for the protocol of the current session |
| `farsight_firehose_lag_seconds` | now − `applied_through` |
| `farsight_firehose_source_lag_seconds` | now − median commit time of the last 1,000 commit events |
| `farsight_firehose_events_total{collection,op,outcome}` | events by outcome: `applied`, `stale`, `refused`, `dropped` |
| `farsight_firehose_reconnects_total{reason}` | `connect_error`, `stall`, `closed`, `error`, `server_error`, `kill`, `failover`, `cursor_too_old` |
| `farsight_firehose_open_gaps` | unhealed gaps |
| `farsight_firehose_seam_repairs_total{trigger}` | `resume`, `failover`, `clamp_recovery` |
| `farsight_firehose_seam_repair_events_total` | events re-read by seam repairs |
| `farsight_ingest_batch_seconds` | batch duration |
| `farsight_ingest_buffer_depth` | events waiting in the channel |
| `farsight_ingest_dropped_total{reason}` | `invalid`, `foreign_listitem`, `poisoned` |
| `farsight_ingest_storage_errors_total{op}` | storage calls outside a batch that failed permanently and were given up |

`getStats` reports the same state in its `firehose` object:
`connected`, `protocol`, `lagSeconds`, `sourceLagSeconds` and
`openGaps` (see [api.md](api.md#querygetstats)). The full
configuration reference and the metrics list are in
[operations.md](operations.md#configuration-reference).
