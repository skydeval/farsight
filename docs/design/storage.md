# Storage

Farsight keeps everything in one Postgres database: the indexed
records, the state of the firehose and the backfill, the coverage
bookkeeping, credentials, and a few tables that exist only for the web
UI. This page gives the schema as the migrations create it, the rules
every write follows (one write path, last-writer-wins on `rev`, a lock
per author, tombstones), what an account's status does to its rows, how
the counters are kept, and how large the database gets.

The operator's view of the same subject (disk, hardware, the storage
budget in the setup wizard) is in
[the storage guide](../guide/storage.md).

## Schema version and migrations

The schema is one migration file, `0001_initial.sql`, in
`crates/farsight-storage/migrations/`, embedded in the binaries. The
schema version is **1**.

- `schema_version` holds one row with one column, `version`. Each
  migration ends with `UPDATE schema_version SET version = <n>`, where
  `<n>` is its own number, so the stored version is the number of the
  last migration applied.
- Only `farsight-server` runs migrations, at start. After running them
  it reads `schema_version` and refuses to continue unless the value
  equals the version the build expects (`SCHEMA_VERSION = 1`).
- `farsight-backfill` never migrates. At start it polls
  `schema_version` every 5 seconds and begins work only when the value
  equals its own `SCHEMA_VERSION`.
- A build fails its tests if the number of migration files differs from
  `SCHEMA_VERSION`, or if a migration does not set its version.

Indexes that take long to build on a large table are not created by a
migration; see [the sort indexes](#the-sort-indexes).

### Conventions

- Every DID and every record key column is `TEXT COLLATE "C"`. Bytewise
  order is the order a PDS lists records in, which the range reconcile
  of the [backfill](backfill.md) relies on. The database's own
  collation is therefore irrelevant.
- Accounts are interned: a DID is stored once, in `actors`, and every
  other hot table refers to it by `actors.id` (`BIGINT`). This saves
  about 66 bytes per `blocks` row.
- `rev` columns hold a repository revision (a TID) decoded to a
  `BIGINT`. An event whose rev is not a TID is rejected.
- There are no foreign keys on the hot tables. Integrity is kept by the
  write path and checked by nightly recounts.
- Enumerations are `SMALLINT` codes; the codes are listed with each
  table and defined in `crates/farsight-storage/src/codes.rs`. Every
  column that stores a code has a `CHECK` constraint, named
  `<table>_<column>_code`, that allows the codes of its enumeration and
  nothing else; a nullable column may also be NULL. The same holds for
  the two text columns with a fixed set of values, `sweep_cycles.source`
  and `top_lists.kind`. The constraints are left out of the listings
  below. A build fails its tests if an enumeration and its constraint
  differ.
- Times named `…_witness`, `first_seen`, `last_seen` and `removed_at`
  are on the witness clock described in
  [coverage](coverage.md#the-witness-clock), not
  on the wall clock of the server.

## Tables

### Actors and hosts

```sql
CREATE TABLE schema_version (version INT NOT NULL);

CREATE TABLE actors (
  id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  did                 TEXT COLLATE "C" NOT NULL UNIQUE,
  status              SMALLINT NOT NULL DEFAULT 0,
  status_at           TIMESTAMPTZ,
  pds_host_id         INT,
  pds_resolved_at     TIMESTAMPTZ,
  authored_blocks     INT NOT NULL DEFAULT 0,
  authored_listblocks INT NOT NULL DEFAULT 0,
  authored_lists      INT NOT NULL DEFAULT 0,
  owned_items         INT NOT NULL DEFAULT 0,
  fetch_triggers      INT NOT NULL DEFAULT 0,
  admission_key       TEXT COLLATE "C",
  readmit_day         DATE,
  readmit_count       INT NOT NULL DEFAULT 0,
  resolve_failures    INT NOT NULL DEFAULT 0,
  flags               SMALLINT NOT NULL DEFAULT 0
);

CREATE TABLE pds_hosts (
  id              INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  host            TEXT COLLATE "C" NOT NULL UNIQUE,
  cap_key         TEXT COLLATE "C" NOT NULL,
  ip_bucket       TEXT,
  large           BOOLEAN NOT NULL DEFAULT false,
  rps_override    REAL,
  cooldown_until  TIMESTAMPTZ,
  requests_total  BIGINT NOT NULL DEFAULT 0,
  errors_total    BIGINT NOT NULL DEFAULT 0,
  latency_ewma_ms REAL,
  last_error      TEXT,
  last_error_at   TIMESTAMPTZ
);

CREATE TABLE host_usage (
  bucket            TEXT COLLATE "C" PRIMARY KEY,
  capped_mask       SMALLINT NOT NULL DEFAULT 0,
  stored_blocks     BIGINT NOT NULL DEFAULT 0,
  stored_items      BIGINT NOT NULL DEFAULT 0,
  stored_listblocks BIGINT NOT NULL DEFAULT 0,
  stored_lists      BIGINT NOT NULL DEFAULT 0,
  stored_interned   BIGINT NOT NULL DEFAULT 0
);
```

`actors` holds one row per DID Farsight has seen as an author, a
subject or a list owner. `status` and `status_at` are described under
[account status](#account-status). The `authored_*`, `owned_items` and
`fetch_triggers` columns are exact per-account counts used by the caps
in [security](security.md); `admission_key`, `readmit_day` and
`readmit_count` belong to list admission
([list indexing](list-indexing.md)). `flags` is display only.

**`actors` rows are never deleted.** An in-memory DID-to-id cache
relies on it, and the migration installs two triggers
(`actors_never_deleted`, `actors_never_truncated`) that raise an
exception on `DELETE` and on `TRUNCATE`.

`pds_hosts` holds one row per PDS host name: the keys its accounts are
capped under (`cap_key` is the registrable domain, `ip_bucket` the
resolved /24 or /48), whether it is one of `limits.large_hosts`, and
the request statistics the backfill keeps for it.

`host_usage` holds approximate stored-row counts per cap bucket and,
in `capped_mask`, one bit per kind of cap the bucket is currently over.
See [counters](#counters).

### Records

```sql
CREATE TABLE blocks (
  author_id  BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  subject_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ,
  rev        BIGINT NOT NULL,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (author_id, rkey)
);
CREATE INDEX blocks_by_subject ON blocks (subject_id, author_id, rkey);

CREATE TABLE list_blocks (
  author_id    BIGINT NOT NULL,
  rkey         TEXT COLLATE "C" NOT NULL,
  list_id      BIGINT NOT NULL,
  counted      BOOLEAN NOT NULL DEFAULT true,
  witnessed_at TIMESTAMPTZ,
  sched_key    TEXT COLLATE "C",
  created_at   TIMESTAMPTZ,
  rev          BIGINT NOT NULL,
  first_seen   TIMESTAMPTZ NOT NULL,
  last_seen    TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (author_id, rkey)
);
CREATE INDEX list_blocks_by_list ON list_blocks (list_id, author_id, rkey);
CREATE INDEX list_blocks_by_author_list ON list_blocks (author_id, list_id);

CREATE TABLE lists (
  id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id          BIGINT NOT NULL,
  rkey              TEXT COLLATE "C" NOT NULL,
  record_state      SMALLINT NOT NULL DEFAULT 0,
  deferred_by       SMALLINT,
  purpose           SMALLINT,
  name              TEXT,
  created_at        TIMESTAMPTZ,
  rev               BIGINT,
  listblock_count   INT NOT NULL DEFAULT 0,
  track_state       SMALLINT NOT NULL DEFAULT 0,
  capped            BOOLEAN NOT NULL DEFAULT false,
  item_count        INT NOT NULL DEFAULT 0,
  admitted_at       TIMESTAMPTZ,
  admit_epoch       INT NOT NULL DEFAULT 0,
  refresh_requested BOOLEAN NOT NULL DEFAULT false,
  purge_then        SMALLINT,
  phase1_epoch      INT NOT NULL DEFAULT 0,
  phase1_attempts   INT NOT NULL DEFAULT 0,
  retain_until      TIMESTAMPTZ,
  fetched_at        TIMESTAMPTZ,
  fetched_witness   TIMESTAMPTZ,
  fetch_run_id      BIGINT,
  fetch_run_epoch   INT,
  fetch_attempts    INT NOT NULL DEFAULT 0,
  next_retry_at     TIMESTAMPTZ,
  description       TEXT,
  avatar_cid        TEXT,
  UNIQUE (owner_id, rkey)
);
CREATE INDEX lists_by_state ON lists (track_state) WHERE track_state <> 0;

CREATE TABLE list_items (
  owner_id   BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  list_id    BIGINT NOT NULL,
  subject_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ,
  rev        BIGINT NOT NULL,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (owner_id, rkey)
);
CREATE INDEX list_items_by_subject ON list_items (subject_id, list_id);
CREATE INDEX list_items_by_list    ON list_items (list_id, subject_id, rkey);

CREATE TABLE tombstones (
  collection SMALLINT NOT NULL,
  author_id  BIGINT   NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  rev        BIGINT   NOT NULL,
  deleted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (collection, author_id, rkey)
);
CREATE INDEX tombstones_by_age ON tombstones (deleted_at);
```

- `blocks`: one row per `app.bsky.graph.block` record. The primary key
  serves "whom does this account block"; `blocks_by_subject` serves
  "who blocks this account".
- `list_blocks`: one row per `app.bsky.graph.listblock` record,
  pointing at a `lists` row. `counted` is false for a row whose author
  is over the trigger cap or the admission rate; only counted rows make
  a list tracked. `witnessed_at` is the witness time of the firehose
  event that first stored the row, NULL when a listing stored it.
  `sched_key` is the admission key charged when the row was inserted.
  `list_blocks_by_author_list` serves the outgoing direction of
  `checkBlocks`, which reads at most 1,000 lists per account through
  it, in list id order.
- `lists`: one row per list that anything refers to, whether or not its
  record has been seen. The record's fields are `record_state`
  (0 unknown, 1 present, 2 deleted), `purpose` (0 other, 1 moderation,
  2 curation, 3 reference), `name`, `created_at`, `rev`, `description`
  (the record's text, truncated to 300 characters) and `avatar_cid`
  (the CID of the record's avatar blob; the image is never stored).
  Everything else is tracking state: `track_state`
  (0 untracked, 1 pending, 2 ready, 3 retained, 4 unavailable,
  5 purging, 6 missing, 7 dead, 8 deferred), `deferred_by` (1 budget,
  2 ceiling, 3 host cap, 4 lists cap, 5 owner re-admissions), the two
  exact counts `listblock_count` and `item_count`, and the fetch
  bookkeeping. The states and their transitions are in
  [list indexing](list-indexing.md).
- `list_items`: one row per `app.bsky.graph.listitem` record of a
  tracked list. Items of untracked lists are not stored.
- `tombstones`: the rev of a recent delete per record key; `collection`
  is 1 block, 2 listblock, 3 list, 4 listitem. See
  [tombstones](#tombstones).

`first_seen` and `last_seen` on the three record tables are witness
bounds: when Farsight first stored the row and when it last applied a
write to it. They are display data, set by the write path as described
in [history](history.md).

### Firehose state and gaps

```sql
CREATE TABLE firehose_state (
  id               SMALLINT PRIMARY KEY CHECK (id = 1),
  source_url       TEXT,
  protocol         SMALLINT,
  cursor_seq       BIGINT,
  cursor_us        BIGINT,
  applied_through  TIMESTAMPTZ,
  first_applied_at TIMESTAMPTZ,
  connected        BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE firehose_cursors (
  source_url           TEXT PRIMARY KEY,
  protocol             SMALLINT,
  cursor_seq           BIGINT,
  cursor_us            BIGINT,
  last_connected_at    TIMESTAMPTZ,
  last_applied_through TIMESTAMPTZ
);

CREATE TABLE firehose_clock (
  server_at  TIMESTAMPTZ PRIMARY KEY,
  witness_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE firehose_gaps (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_at         TIMESTAMPTZ NOT NULL,
  to_at           TIMESTAMPTZ,
  cause           SMALLINT NOT NULL,
  detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  healed_at       TIMESTAMPTZ,
  healed_witness  TIMESTAMPTZ,
  repair_cycle_id BIGINT
);

CREATE TABLE firehose_seams (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_url  TEXT NOT NULL,
  protocol    SMALLINT NOT NULL,
  trigger     SMALLINT NOT NULL,
  from_at     TIMESTAMPTZ NOT NULL,
  to_at       TIMESTAMPTZ,
  due_at      TIMESTAMPTZ,
  attempts    INT NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- `firehose_state`: the single row of the current session: which
  Jetstream instance, which protocol (1 v1, 2 v2), its cursor in both
  forms, and `applied_through`, the witness time of the last applied
  event.
- `firehose_cursors`: one cursor per Jetstream instance, so that a
  failover back to an instance resumes from that instance's own cursor.
  Within one `source_url` a cursor only moves forward, except that the
  `seq` of an instance whose sequence started again is dropped.
- `firehose_clock`: pairs of server commit time and `applied_through`,
  one row per applied batch, thinned to one a minute after 24 hours and
  kept 30 days. It is how a wall-clock moment (the start of a listing)
  is turned into a point on the witness clock.
- `firehose_gaps`: intervals of the witness clock during which events
  may have been lost. `cause` is 1 `cursor_too_old`, 2 `heuristic`,
  3 `failover`, 4 `sync_unavailable`, 5 `seam_unrepaired`,
  6 `unreadable`, 7 `unapplied`; `to_at` is
  NULL while the gap is open; `healed_*` are set by the repair that
  covered it.
- `firehose_seams`: the seam windows that still have to be read again,
  one row per resumed session. `protocol` is 1 v1, 2 v2; `trigger` is
  1 `resume`, 2 `failover`, 3 `clamp_recovery`; `to_at` is NULL until
  the session has caught up or ended; `due_at` is the time before which
  the re-read is not started; `attempts` counts the re-reads that
  failed. A row is deleted when its re-read has been applied, or when
  it is given up and recorded as a gap.

The stream, its cursors and gap detection are described in
[firehose](firehose.md); gap repair in [backfill](backfill.md).

### Backfill queue, leases and cycles

```sql
CREATE TABLE backfill_state (
  actor_id            BIGINT PRIMARY KEY,
  state               SMALLINT NOT NULL,
  backfilled_at       TIMESTAMPTZ,
  backfilled_witness  TIMESTAMPTZ,
  clean_witness       TIMESTAMPTZ,
  last_outcome        SMALLINT,
  backfill_rev        BIGINT,
  attempts            INT NOT NULL DEFAULT 0,
  next_attempt_at     TIMESTAMPTZ,
  first_failed_at     TIMESTAMPTZ,
  last_error          TEXT,
  current_run_id      BIGINT,
  current_run_point   TIMESTAMPTZ,
  current_run_started_at TIMESTAMPTZ,
  inactive_at_listing BOOLEAN NOT NULL DEFAULT false,
  yields              INT NOT NULL DEFAULT 0
);
CREATE INDEX backfill_state_running ON backfill_state (actor_id) WHERE state = 2;
CREATE INDEX backfill_state_by_backfilled ON backfill_state (backfilled_at);

CREATE TABLE job_leases (
  did         TEXT COLLATE "C" PRIMARY KEY,
  lease_owner TEXT NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL
);

CREATE TABLE backfill_cursors (
  actor_id      BIGINT   NOT NULL,
  collection    SMALLINT NOT NULL,
  job_kind      SMALLINT NOT NULL,
  run_id        BIGINT   NOT NULL,
  stamp_rev     BIGINT   NOT NULL,
  stamp_read_at TIMESTAMPTZ NOT NULL,
  late_stamp    BOOLEAN  NOT NULL,
  cursor        TEXT,
  prev_last     TEXT COLLATE "C",
  unreconciled  BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (actor_id, collection, job_kind, run_id)
);

CREATE TABLE backfill_queue (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  actor_id    BIGINT   NOT NULL,
  kind        SMALLINT NOT NULL,
  tier        SMALLINT NOT NULL,
  priority    SMALLINT NOT NULL,
  requester   TEXT NOT NULL,
  cycle_id    BIGINT,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  not_before  TIMESTAMPTZ,
  claimed_by  TEXT,
  claimed_until TIMESTAMPTZ
);
CREATE UNIQUE INDEX backfill_queue_one_waiting ON backfill_queue (actor_id, kind)
  WHERE claimed_by IS NULL;
CREATE INDEX backfill_queue_pick
  ON backfill_queue (tier, requester, priority DESC, enqueued_at);
CREATE INDEX backfill_queue_by_requester ON backfill_queue (requester, priority);
CREATE INDEX backfill_queue_claimed ON backfill_queue (claimed_until)
  WHERE claimed_by IS NOT NULL;

CREATE TABLE list_jobs (
  list_id     BIGINT PRIMARY KEY,
  owner_id    BIGINT NOT NULL,
  admit_epoch INT NOT NULL,
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  not_before  TIMESTAMPTZ
);
CREATE INDEX list_jobs_by_owner ON list_jobs (owner_id);

CREATE TABLE list_fetch_runs (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id       BIGINT NOT NULL,
  started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  coverage_point TIMESTAMPTZ,
  finished_at    TIMESTAMPTZ,
  outcome        SMALLINT
);
CREATE INDEX list_fetch_runs_by_owner ON list_fetch_runs (owner_id, started_at);
CREATE INDEX list_fetch_runs_unfinished ON list_fetch_runs (owner_id)
  WHERE finished_at IS NULL;

CREATE TABLE discovery_state (
  actor_id           BIGINT PRIMARY KEY,
  state              SMALLINT NOT NULL,
  source             TEXT NOT NULL,
  started_at         TIMESTAMPTZ,
  discovered_witness TIMESTAMPTZ,
  completed_at       TIMESTAMPTZ,
  truncated          BOOLEAN NOT NULL DEFAULT false,
  refs_found         INT NOT NULL DEFAULT 0,
  last_error         TEXT
);

CREATE TABLE sweep_cycles (
  id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind                    SMALLINT NOT NULL,
  source                  TEXT NOT NULL,
  collections             SMALLINT[] NOT NULL,
  started_at              TIMESTAMPTZ NOT NULL,
  effective_start         TIMESTAMPTZ,
  effective_start_witness TIMESTAMPTZ,
  enumerated_at           TIMESTAMPTZ,
  completed_at            TIMESTAMPTZ,
  completed_witness       TIMESTAMPTZ,
  checkpoint              TEXT,
  total_est               BIGINT,
  done                    BIGINT NOT NULL DEFAULT 0,
  failed_terminal         BIGINT NOT NULL DEFAULT 0,
  repair_from             TIMESTAMPTZ
);
CREATE UNIQUE INDEX sweep_cycles_one_open ON sweep_cycles (kind)
  WHERE completed_at IS NULL;

CREATE TABLE cycle_outstanding (
  cycle_id BIGINT NOT NULL,
  did      TEXT COLLATE "C" NOT NULL,
  state    SMALLINT NOT NULL,
  PRIMARY KEY (cycle_id, did)
);
```

- `backfill_state`: one row per account whose repository has been
  queued for listing. `state` is 0 never, 1 queued, 2 running, 3 done,
  4 failed; `last_outcome` is 1 clean, 2 complete with debts,
  3 inactive, 4 failed. `backfilled_witness` and `clean_witness` are
  the coverage points of the last complete and the last clean run.
  `current_run_point` and `current_run_started_at` are the coverage
  point and the start of the run under way, set by its first attempt
  and kept by the attempts that go on with it. `yields` counts the
  attempts of that run that stopped at a bound, in a row. The partial
  index finds the rows marked `running`, among which the feeder looks
  for jobs a stopped process left behind.
- `job_leases`: which job holds a DID, and until when. `lease_owner`
  is the name of the process and the number of its job.
- `backfill_cursors`: the paging position and the listing stamp of one
  running job (`job_kind` 1 repository, 2 list fetch). A cursor belongs
  to its run and is never adopted by another. `unreconciled` says that
  an attempt which resumes from the row cannot tell which stored rows
  the repository no longer has (the listing was out of order, or past
  the seen-set cap).
- `backfill_queue`: jobs that wait or run. The scheduler claims an
  entry when it starts the job (`claimed_by` names the process,
  `claimed_until` the time the claim runs out unless it is renewed)
  and deletes it when the job has ended. An entry whose claim ran out
  waits again. `kind` is 1 repository, 2 list fetch, 3 discovery. The
  unique index allows one waiting entry per account and kind; a
  claimed entry is apart from it. `requester` names who
  asked: `token:<id>`, `admin`, or a system requester such as
  `system:sweep`, `system:repair`, `system:resync`, `system:firehose`,
  `system:lists`.
- `list_jobs`: lists waiting for their record to be read. The result is
  applied only if `admit_epoch` is still the list's.
- `list_fetch_runs`: one row per run that fetches an owner's list
  items. `outcome` is NULL while running, then 1 ok, 2 failed,
  3 owner inactive, 4 cancelled. A finished run is deleted after 7
  days.
- `discovery_state`: the per-account state of a discovery run (1
  queued, 2 running, 3 done, 4 failed) and its coverage point.
- `sweep_cycles`: one row per full sweep (`kind` 1) or gap repair
  (`kind` 2), with its enumeration checkpoint and progress. The unique
  index allows one open cycle of each kind.
- `cycle_outstanding`: the members of a cycle that are still in flight
  or being retried (`state` 1 outstanding, 2 terminal).

All of these are explained in [backfill](backfill.md).

### Coverage and rates

```sql
CREATE TABLE subject_coverage (
  actor_id     BIGINT   NOT NULL,
  scope        SMALLINT NOT NULL,
  confirmed_at TIMESTAMPTZ NOT NULL,
  refs_found   INT NOT NULL,
  discovered_witness TIMESTAMPTZ,
  PRIMARY KEY (actor_id, scope)
);

CREATE TABLE subject_lists (
  actor_id BIGINT NOT NULL,
  list_id  BIGINT NOT NULL,
  PRIMARY KEY (actor_id, list_id)
);
CREATE INDEX subject_lists_by_list ON subject_lists (list_id);

CREATE TABLE relist_debt (
  actor_id      BIGINT   NOT NULL,
  reason        SMALLINT NOT NULL,
  cap_type      SMALLINT,
  since_witness TIMESTAMPTZ NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (actor_id, reason)
);

CREATE TABLE storage_refusals (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_witness TIMESTAMPTZ NOT NULL,
  to_witness   TIMESTAMPTZ
);

CREATE TABLE admission_rate (
  key        TEXT COLLATE "C" NOT NULL,
  utc_day    DATE NOT NULL,
  admissions INT  NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

CREATE TABLE intern_rate (
  key     TEXT COLLATE "C" NOT NULL,
  utc_day DATE NOT NULL,
  n       BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

CREATE TABLE history_rate (
  key     TEXT COLLATE "C" NOT NULL,
  utc_day DATE NOT NULL,
  n       BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

CREATE TABLE list_sched_keys (
  list_id BIGINT NOT NULL,
  key     TEXT COLLATE "C" NOT NULL,
  n       INT  NOT NULL,
  PRIMARY KEY (list_id, key)
);
CREATE INDEX list_sched_keys_by_key ON list_sched_keys (key);
```

- `subject_coverage`: what a discovery run confirmed about an account
  as a subject (`scope` 1 direct blocks, 2 the listitem → list →
  listblock chain). `discovered_witness` is the coverage point of the
  run that confirmed it. A later run that fails or is truncated leaves
  the row as it is.
- `subject_lists`: every list a discovery run found naming the account.
  A run that checked every reference replaces the account's set, and
  rows whose list's record is deleted are removed once a day. The
  index serves the placeholder cleanup, which asks by list.
- `relist_debt`: the single source of per-account exceptions to
  coverage. `reason` is 1 unreachable, 2 resync, 3 capped, 4 refused;
  `cap_type` names the cap, rate or gate behind a capped or refused
  debt. See [coverage](coverage.md#re-list-debts).
- `storage_refusals`: the intervals during which the storage budget or
  the hard ceiling refused writes; see
  [the storage budget](#the-storage-budget).
- `admission_rate`, `intern_rate`, `history_rate`: per key and UTC day,
  how many lists a key admitted, how many `actors` rows it caused, and
  how many history rows it wrote. All three are exact and are updated
  inside the apply transaction. A nightly task deletes rows older than
  two days.
- `list_sched_keys`: for a list waiting to be fetched, the admission
  keys it is eligible under and the number of counted listblocks from
  each.

### Auth and operations

```sql
CREATE TABLE api_tokens (
  id           INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name         TEXT NOT NULL,
  sha256       BYTEA NOT NULL UNIQUE,
  scopes       TEXT[] NOT NULL,
  read_rps     REAL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ,
  revoked_at   TIMESTAMPTZ
);

CREATE TABLE admin_sessions (
  id_sha256  BYTEA PRIMARY KEY,
  csrf       BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
  ip         INET,
  user_agent TEXT
);

CREATE TABLE op_errors (
  id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  component TEXT NOT NULL,
  did       TEXT,
  host      TEXT,
  message   TEXT NOT NULL
);
CREATE INDEX op_errors_at ON op_errors (at);

CREATE TABLE account_purges (
  actor_id     BIGINT PRIMARY KEY,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  not_before   TIMESTAMPTZ
);

CREATE TABLE stats_counters (
  name       TEXT NOT NULL,
  shard      SMALLINT NOT NULL,
  value      BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (name, shard)
);
```

- `api_tokens`: API tokens by the SHA-256 of the token; the token
  itself is not stored.
- `admin_sessions`: admin sessions by a SHA-256 derived from the
  session cookie, with the session's CSRF secret.
- `op_errors`: the operational error log the dashboard shows. Rows
  older than 30 days are deleted once a day, and the oldest beyond
  100,000 rows.
- `account_purges`: the deleted accounts whose stored rows still have
  to be removed; see
  [the purge of a deleted account](#the-purge-of-a-deleted-account).
  `not_before` is set after a failed purge.
- `stats_counters`: the approximate totals behind `getStats` and the
  dashboard; see [counters](#counters).

Tokens are described in [API](api.md#tokens-and-scopes), admin sessions
in [web UI](web-ui.md#sessions).

### History

```sql
CREATE TABLE blocks_history (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  author_id   BIGINT NOT NULL,
  rkey        TEXT COLLATE "C" NOT NULL,
  subject_id  BIGINT NOT NULL,
  created_at  TIMESTAMPTZ,
  first_seen  TIMESTAMPTZ,
  last_seen   TIMESTAMPTZ,
  removed_at  TIMESTAMPTZ NOT NULL,
  removed_rev BIGINT,
  cause       SMALLINT NOT NULL
);
CREATE INDEX blocks_history_by_subject
  ON blocks_history (subject_id, removed_at DESC, id DESC);
CREATE INDEX blocks_history_by_author
  ON blocks_history (author_id, removed_at DESC, id DESC);

CREATE TABLE list_blocks_history (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  author_id     BIGINT NOT NULL,
  rkey          TEXT COLLATE "C" NOT NULL,
  list_owner_id BIGINT NOT NULL,
  list_rkey     TEXT COLLATE "C" NOT NULL,
  created_at    TIMESTAMPTZ,
  first_seen    TIMESTAMPTZ,
  last_seen     TIMESTAMPTZ,
  removed_at    TIMESTAMPTZ NOT NULL,
  removed_rev   BIGINT,
  cause         SMALLINT NOT NULL
);
CREATE INDEX list_blocks_history_by_list
  ON list_blocks_history (list_owner_id, list_rkey, removed_at DESC, id DESC);
CREATE INDEX list_blocks_history_by_author
  ON list_blocks_history (author_id, removed_at DESC, id DESC);

CREATE TABLE list_items_history (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id    BIGINT NOT NULL,
  rkey        TEXT COLLATE "C" NOT NULL,
  list_rkey   TEXT COLLATE "C" NOT NULL,
  subject_id  BIGINT NOT NULL,
  created_at  TIMESTAMPTZ,
  first_seen  TIMESTAMPTZ,
  last_seen   TIMESTAMPTZ,
  removed_at  TIMESTAMPTZ NOT NULL,
  removed_rev BIGINT,
  cause       SMALLINT NOT NULL
);
CREATE INDEX list_items_history_by_list
  ON list_items_history (owner_id, list_rkey, removed_at DESC, id DESC);
CREATE INDEX list_items_history_by_subject
  ON list_items_history (subject_id, removed_at DESC, id DESC);

CREATE TABLE history_windows (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  to_at   TIMESTAMPTZ
);
```

The three `…_history` tables hold one row per removed block, listblock
and list membership; `history_windows` holds the intervals during which
removals were recorded. They are display data and take no part in any
rule on this page. [History](history.md) describes them in full.

### Tables for the web UI

```sql
CREATE TABLE handle_cache (
  did         TEXT COLLATE "C" PRIMARY KEY,
  handle      TEXT NOT NULL,
  resolved_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE handle_due (
  did      TEXT COLLATE "C" PRIMARY KEY,
  asked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX handle_due_asked ON handle_due (asked_at);

CREATE TABLE avatar_cache (
  did        TEXT COLLATE "C" PRIMARY KEY,
  cid        TEXT NOT NULL,
  checked_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE block_recent (
  at         TIMESTAMPTZ NOT NULL,
  author_id  BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  subject_id BIGINT NOT NULL
);
CREATE INDEX block_recent_at ON block_recent (at);

CREATE TABLE top_lists (
  kind        TEXT COLLATE "C" PRIMARY KEY,
  computed_at TIMESTAMPTZ NOT NULL,
  rows        TEXT NOT NULL
);
```

- `handle_cache`: one row per DID whose handle has been checked: the
  handle that verified in both directions, and when. An empty `handle`
  means "checked, no handle to show". Which write may replace a
  verified handle depends on what the check established:
  - a successful verification replaces whatever is stored;
  - a check made while a page is viewed that finds nothing
    (`store_none`) writes an empty row or refreshes the time of an
    empty one, and **never replaces a verified handle**;
  - the handle pass replaces a stored handle with empty when the check
    proves the handle is no longer the account's (`store_gone`: the DID
    has no document, the document names no handle, or the handle
    resolves to another DID), and when the document names a handle
    that does not resolve back and differs from the stored one
    (`store_unresolved`; a stored handle equal to the named one is
    kept, since its host may only be unreachable);
  - a check that establishes nothing (`store_unknown`) leaves a stored
    handle alone, and dates a new or empty row six days back so that
    it is due again a day later.

  See [web UI](web-ui.md#handles).
- `handle_due`: accounts whose handle is to be checked ahead of the
  handle pass's walk over `actors`. The firehose writer adds an account
  that has an `actors` row when an identity event names it; a row is
  not served before `asked_at` and is removed once the check gives an
  answer.
- `avatar_cache`: the CID of the blob an account's profile uses as its
  avatar, never the image. An empty `cid` records that the profile has
  none. A row is used for a day, then read again.
- `block_recent`: a log of blocks as they are stored, for the home
  page's "last 24 hours" lists. The write path adds a row when it
  inserts a block whose own `createdAt` lies between a day before and
  five minutes after its arrival, so that old blocks read by the
  backfill do not count as recent. Rows older than 49 hours are
  deleted.
- `top_lists`: the four computed top lists (`kind` is `blockers_all`,
  `blocked_all`, `blockers_day`, `blocked_day`), each as JSON text in
  `rows`. A task computes them once a day; the home page reads the rows
  and never runs the counting queries.

All five can be emptied at any time: the UI fills them again. Their use
is described in [web UI](web-ui.md).

### The sort indexes

Four more indexes order the UI's row tables by a row's *shown time*:

```sql
-- <shown time> = COALESCE(LEAST(created_at, first_seen),
--                         '-infinity'::timestamptz)

CREATE INDEX CONCURRENTLY list_blocks_by_list_created ON list_blocks
  (list_id, (<shown time>) DESC, author_id DESC, rkey DESC);
CREATE INDEX CONCURRENTLY list_items_by_list_created ON list_items
  (list_id, (<shown time>) DESC, rkey DESC);
CREATE INDEX CONCURRENTLY blocks_by_subject_created ON blocks
  (subject_id, (<shown time>) DESC, author_id DESC, rkey DESC);
CREATE INDEX CONCURRENTLY blocks_by_author_created ON blocks
  (author_id, (<shown time>) DESC, rkey DESC);
```

The shown time is the record's stated `createdAt`, but never later than
the moment Farsight first stored the record, so a record dated in the
future sorts where it arrived.

These indexes are built by a task in the server, not by a migration: a
build inside the migration transaction would outlast a health check on
a large `blocks` table and be rolled back by the restart, on every
start. The task (`crates/farsight-server/src/sort_indexes.rs`) starts
when the server begins serving and works as follows.

1. It opens a connection of its own with no statement timeout and takes
   a session advisory lock, so that two server processes never build at
   once. A process that does not get the lock reads the indexes' state
   and looks again after 10 minutes.
2. For each index, in the order above (small tables first):
   - valid: done;
   - present but invalid (an interrupted build): `DROP INDEX
     CONCURRENTLY`, then as absent;
   - absent: the size is estimated as the table's `reltuples` × 62
     bytes. If `pg_database_size` plus the estimate exceeds
     `storage.budget_bytes`, nothing is built: the task logs a warning,
     the dashboard shows one, and the task looks again every 15
     minutes. Otherwise it runs `CREATE INDEX CONCURRENTLY`, which does
     not block writers.
3. A failed build is retried after 10 minutes and recorded in
   `op_errors` once per run of failures.

Until its index is valid, a UI table lists its rows in the order of
the index the API uses for it (by account, then record key); each table
switches by itself when its index becomes valid.
`farsight_ui_sort_indexes_ready` is the number of valid ones. The other
indexes serve the API's orders and cursors (which the sort indexes do
not change), `checkBlocks`, and the counts.

## The write path

Every write to the record tables, in both binaries, goes through one
function, `apply` in `crates/farsight-storage/src/apply.rs`, so the
firehose and the backfill cannot apply different rules. A *batch* is one
Postgres transaction at `READ COMMITTED` and has one origin:

| Origin | Stamp `W` of each write | Witness time `w` |
|---|---|---|
| Firehose events | the commit's rev | the event's witness time |
| A listing page (backfill) | `R`, the repository's latest rev read before the first page | the witness clock at the transaction's start |
| A discovery write | `0` | the witness clock at the transaction's start |

A batch carries writes (an upsert of a record, or a delete; deletes
come only from the firehose), reconciles (listing only), non-commit
firehose events (`identity`, `account`, `#sync`), and optionally the
firehose progress to persist. The transaction does, in this order:

1. **Author locks** for every author in the batch, ascending by key.
2. **Reads** of the stored rows of every listblock and listitem key in
   the batch and of every reconcile candidate. They are stable under
   the author locks, and they tell which lists a delete, a
   target-changing update or a reconcile will touch. The reconciles
   of one batch take at most 5,000 candidates and at most 250 list
   locks between them, lowest record keys first. A batch that leaves
   candidates says so, and its caller applies the same reconciles
   again until none is left.
3. **List locks**, ascending by key: exclusive for listblock and list
   writes, shared for listitem writes.
4. **Intern locks** for every DID the batch may create an `actors` row
   for, ascending. Without them two batches interning the same new DIDs
   in opposite orders deadlock on the unique index.
5. **Writes** in batch order, then **reconciles**, each under the
   [last-writer-wins rule](#last-writer-wins), with the caps of
   [security](security.md) and the list transitions of
   [list indexing](list-indexing.md); then the non-commit events.
6. **Firehose progress** (cursor, `applied_through`, a `firehose_clock`
   row) in the same transaction, so the persisted cursor never runs
   ahead of the effects of the events before it, and
   `NOTIFY farsight_coverage`.

The lock order is therefore global: authors, then lists, then interns,
each class ascending ([list indexing](list-indexing.md#locks) has the
lock set of each kind of write). All three are transaction-scoped Postgres
advisory locks (`pg_advisory_xact_lock`, shared variant for shared list
locks) on a 64-bit key: the first 8 bytes, big-endian, of the SHA-256
of `"a:" + did` for an author, `"l:" + owner_did + "/" + rkey` for a
list, `"i:" + did` for an intern. The hash is stable across processes
and builds, which the protocol requires, since both binaries must
derive the same key.

Each class of locks is taken with one statement, in ascending key
order.

**Lock bounds.** Postgres keeps every lock of every session in one
shared table of `max_locks_per_transaction × max_connections` entries
(6,400 with the defaults of 64 and 100), and a transaction that finds
it full fails with "out of shared memory". No transaction of Farsight
takes a number of advisory locks that grows with the data:

| Transaction | Locks it takes at most |
|---|---|
| A firehose batch (500 events) | one author lock per event, two list locks per listblock or listitem event, one intern lock per DID it names |
| A listing page (100 records) | 1 author lock, 2 list locks per record, and 250 list locks for its reconciles |
| One batch of an account purge or a divergence purge | 1 author lock and 500 list locks; a batch ends where its rows would need more |
| The re-evaluation of uncounted listblocks | 1 author lock and 500 list locks per transaction |
| The claim of a list fetch run | 500 list locks |
| The reactivations in one firehose batch (**OA** on the accounts' `unavailable` lists) | 500 list locks for the batch, given to the events in order; lists beyond them keep their own retry |
| One batch of the nightly recount of per-account counters | 500 author locks |

Work that needs more is split over several transactions. Keep
`max_locks_per_transaction` at 64 or above; the defaults leave room
for every backfill worker and the firehose writer at once.

A transaction aborted by a deadlock (`40P01`) is retried, up to 8
attempts, with an exponential backoff from 10 ms capped at 1 s plus
jitter. This holds for `apply` and for the transactions around it
that take the same locks: the purges, a list transition fired by a
job or a task, the promotion at the end of a fetch run. A deadlock
retry never counts toward poisoned-event handling.

In deletes-only mode (the storage gate) a listing page's upserts are
skipped with a `refused` debt. One case still writes: a skipped
version that would have pointed a stored row at another target (a
block's subject, a listblock's list, a listitem's list or subject)
removes that row, with a refusal tombstone, like any refused update.
The row would otherwise go on reporting a target its record no longer
names.

A listing batch whose stamp `R` was read 72 hours ago or more is
rejected (`StaleStamp`); the job reads a fresh stamp and starts again.
The [tombstone lifetime](#tombstones) depends on this bound.

## Last-writer-wins

### The author lock

Every write to the last-writer-wins columns of rows authored by a DID
D — its `blocks`, `list_blocks` and `list_items` rows, the record
fields of its `lists` rows, and its `tombstones` — happens in a
transaction that holds the author lock of D. The row and its tombstone
are read after the lock is granted, and every competing writer for the
same key holds the same lock, so the check and the write are atomic
with respect to every other writer in both processes. (The tracking and
counter columns of `lists` are written under the list lock instead.)

### The rule

For a key (collection, D, rkey) and an incoming write with stamp `W`:

- **Upsert.** Apply if and only if `W` is greater than the stored row's
  rev (or there is no row) **and** `W` is greater than the tombstone's
  rev (or there is no tombstone). An equal stamp is skipped.

  ```rust
  fn lww_upsert_wins(w: i64, row_rev: Option<i64>, tombstone_rev: Option<i64>) -> bool {
      row_rev.is_none_or(|r| w > r) && tombstone_rev.is_none_or(|t| w > t)
  }
  ```

  A winning upsert whose target differs from the stored row's — a block
  naming another subject, a listblock naming another list, a listitem
  naming another list or subject — is a removal of the old target
  followed by a fresh insert: the old target is recorded in
  [history](history.md) and the row's `first_seen` and `last_seen`
  start again. If the new version is then refused by a cap or a gate,
  the row is deleted and a [refusal tombstone](#tombstones) is written.

- **Delete** (firehose only). Delete the row if and only if its rev is
  less than `W`. In every case upsert the tombstone with
  `rev = GREATEST(existing, W)`. A delete that finds no row, or a row
  with rev ≥ `W`, removes nothing. A `lists` row is not deleted but
  marked: `record_state = 2`, its record fields cleared, because other
  authors' listblocks point at it.

- **Reconcile** (listing only). Given the author, a collection, the
  stamp `R`, a key range `(after, through]` and the keys present on the
  page: delete the author's rows in the range that are not on the page
  and have `rev < R`. No tombstone is written. For `lists` the row is
  marked deleted as above.

### Why the stamp read before the listing is correct

`R` is the repository's latest rev, read at wall time `t_R` before any
page is fetched. A page read later shows the repository as it is at
that later moment.

- Any change after `R` has a rev greater than `R`, so the firehose
  write for it beats the listing's `R` whichever arrives first.
- A record on a page may have been created after `R`. It is stored with
  the lower stamp `R`. This is harmless: its own create event (rev >
  `R`) applies the same content again, and any later update or delete
  has a rev greater than `R` and wins.
- A record deleted after `R` but before its page was read is absent
  from the page. The reconcile deletes the stored row (rev < `R`),
  which agrees with the firehose delete; and if the firehose delete was
  applied first, its tombstone (rev > `R`) keeps the listing from
  inserting the record again.

## Tombstones

A tombstone remembers the rev of a delete so that an older write
arriving later cannot bring the record back.

- A tombstone is written for every firehose delete, whether or not a
  row existed, with the delete's rev.
- A **refusal tombstone** is written when an update's new version is
  refused and the stored row is deleted. Its rev is **`E − 1`**, where
  `E` is the update's rev. It must block any listing stamped `R < E`,
  which may still show the old version: `R < E` is `R ≤ E − 1`, which
  the rule (`W` greater than the tombstone's rev) refuses. A listing
  stamped `R ≥ E` shows the refused but current version and must be
  able to apply once the cause of the refusal is gone; with rev `E` the
  equal-stamp rule would skip it for as long as the tombstone lived.
- Reconciles write no tombstone: the listing at `R` already reflects
  every delete with a rev below `R`.
- Tombstones live for `storage.tombstone_ttl` (default `"7d"`),
  measured from `deleted_at`, the time the delete was processed. An
  hourly task deletes the expired ones.

The lifetime is sufficient because of the 72-hour bound on a listing
stamp. A tombstone matters only against a write with a lower stamp. A
delete with rev `E > R` was committed after `R` was the latest rev,
that is after `t_R`; its tombstone is written when the delete is
processed, at or after `t_R`, and lives until at least `t_R + 7 d`,
well beyond the last moment the stamp may be applied, `t_R + 72 h`. How
old the records themselves are does not matter; only `t_R` does.
Discovery writes (`W = 0`) are applied immediately after the record was
verified with `getRecord`; a delete after that has a rev above 0 and a
live tombstone.

Deletes arrive at well under one a second, so seven days of tombstones
are fewer than 600,000 rows and less than 100 MB.

## Account status

`actors.status` holds one of eight codes (`ActorStatus` in
`crates/farsight-storage/src/codes.rs`):

| Code | Status | Upstream value | Hidden | Effect on the account's rows |
|---|---|---|---|---|
| 0 | active | `active`, or none | no | none |
| 1 | deactivated | `deactivated` | yes | kept; left out of results by default |
| 2 | takendown | `takendown` | yes | kept; left out of results by default |
| 3 | suspended | `suspended` | yes | kept; left out of results by default |
| 4 | deleted | `deleted` | yes | purged (below) |
| 5 | throttled | `throttled` | no | none |
| 6 | desynchronized | `desynchronized` | no | kept and shown; the repository is listed again |
| 7 | unknown | any other value | no | none |

A *hidden* status means that rows the account authored are excluded
from API results unless the caller asks for inactive accounts
([API](api.md)); the rows themselves stay in the tables, and come back
into results when the account becomes active again. The public pages
apply rules of their own on top ([web UI](web-ui.md)).

### Ordering

Account events carry no rev, so status is ordered by time: the `time`
of the upstream account event, which is the same on every Jetstream
instance, or the witness time for an event that has none or whose
`time` is after its witness time. A status
change is applied only if the event's time is at or after the stored
`status_at`; an older event is skipped, and applying an event with an
equal time again changes nothing. Replays of the stream can therefore
deliver older status events without undoing a newer status. An event
that is skipped has no further effect.

Status has three sources, and all three go through the same event path
and the same ordering:

- Jetstream `account` events;
- the relay's `com.atproto.sync.getRepoStatus`, which a repository job
  asks after a repo-level error from the PDS that re-resolving the DID
  did not cure. The answer is recorded only if the relay reports the
  account inactive with a hidden status;
- a DID that resolves as tombstoned, which is recorded as `deleted`.

An error response from a PDS never changes a status by itself: it leads
to the question to the relay, and only the relay's answer is recorded.
An `account` event from the firehose for a DID with no `actors` row
creates no row.

When an applied event

- sets `desynchronized`, the account gets a `resync` debt and its
  repository is listed again;
- makes an account active that was hidden, or that was inactive when
  last listed, the account gets a `resync` debt, and each of its lists
  in state `unavailable` is told that its owner is active again;
- sets `deleted`, a purge of the account is asked for in the same
  transaction (a row in `account_purges`).

### The purge of a deleted account

No writer purges. The transaction that records an account as
`deleted`, in either process, adds the account to `account_purges`,
and the server's `account_purges` task, which looks every 10 seconds,
does the work. The firehose writer and the backfill jobs go on at
once, however much the account stored. The task purges up to 20
accounts a pass, each for up to 25 batches, so one large account does
not hold up the others; an account with more to remove goes on in the
next pass.

A batch is at most 10,000 rows per table and at most 500 list locks,
under the account's author lock and the list locks it touches. It
marks at most 125 of the account's own lists deleted. The rows of a
batch are deleted with one statement per table, which also adjusts the
counters they were counted in:

- rows the account **authored** in `blocks`, `list_blocks` and
  `list_items` are deleted. Listblocks go through the same path as any
  other listblock delete, so the counts of the lists they pointed at
  are decremented and a list that loses its last counted listblock
  changes state.
- the account's `lists` rows are **kept**, marked `record_state = 2`
  with their record fields cleared, because other accounts' listblocks
  point at their ids. Each such list is told that its record was
  deleted, which drains its remaining items, and any fetch run under
  way for the owner is cancelled.
- rows that name the account as a **subject** (blocks of it, list
  memberships of it) stay.
- the purge writes no [history](history.md) rows, and after the live
  rows it deletes the history rows the account authored
  (`blocks_history` and `list_blocks_history` by `author_id`,
  `list_items_history` by `owner_id`). History rows that name the
  account as a subject, or as the owner of a listblocked list, stay.
- no tombstones are needed and the `actors` row is never deleted.

A batch reads the account's status under its author lock and removes
nothing unless it is `deleted`: a purge asked for an account that has
since been reactivated ends there, and the reactivation's `resync`
debt has the repository listed again. The debt is raised whenever the
status leaves `deleted`, whatever it becomes: an account that goes
from `deleted` to a status that is not hidden, and is activated
later, would otherwise come back without its rows and without a debt.

An account's row in `account_purges` is deleted when a batch finds
nothing left. The request survives a restart, so an interrupted purge
goes on where it was. A purge that fails is recorded among the
operational errors with the account's DID and tried again an hour
later; the other accounts do not wait for it.

An account counts as pending purge while it has status `deleted` and
still authors any live row, any list not yet marked deleted, or any
history row. Shortly after start and once a day the server looks for
such accounts that have no request (the `account_purge_scan` task)
and asks for their purge, so a history row written later by a replayed
event is removed within a day or when it ages out. Until an account
is purged its rows are never shown, because `deleted` is a hidden
status.

## Counters

Two kinds of count exist, and they are kept differently.

**Exact counts** live on rows the write already holds locked and are
updated inside the apply transaction: `actors.authored_blocks`,
`authored_listblocks`, `authored_lists`, `owned_items` and
`fetch_triggers` under the author lock; `lists.listblock_count` and
`lists.item_count` under the list lock. The caps that must be exact
read these. So are the three daily rate tables (`admission_rate`,
`intern_rate`, `history_rate`): they are keyed per bucket or DID, and
a row is locked from the batch's first charge to it until the batch
commits. A charge is conditional (it decides whether the write is
refused), so it is made where the write is applied, in batch order,
not in key order at commit. Two batches that charge the same two keys
in opposite orders can therefore deadlock; one of them is aborted and
[retried](#the-write-path). The keys are per bucket or DID, so this
needs two batches working on the same two hosts at the same moment.

**Approximate counts** are `stats_counters` (the totals `blocks`,
`list_blocks`, `lists` with a present record, `list_items`, `actors`)
and the `stored_*` columns of `host_usage`. An apply transaction never
touches these rows, because they would be the hottest rows in the
database. Each transaction collects its deltas; after it commits they
are merged into a per-process sink, which a background task flushes
every 5 seconds in a transaction of its own. `stats_counters` is
sharded by `(name, shard)` and summed on read.

The consequences:

- a cap enforced on `host_usage` can be overshot by at most 5 seconds
  of writes per process, a few thousand rows at backfill page rates,
  which is acceptable for a bound on abuse;
- a crash loses at most 5 seconds of deltas.

**Usage follows the account's host.** A row is counted in the buckets
its author has when it is stored, and taken out of the buckets the
author has when it is deleted. An author's buckets change when its
host becomes known (from `unresolved` to the host's domain and address
block), when it moves, and at its third failed resolution (from
`unresolved` to a bucket of its own). The resolver makes each of these
changes under the author lock and, in the same step, moves the
account's usage: its exact per-author counts are taken out of the old
buckets and added to the new. Without that the rows of every new
author would stay in `unresolved`, which only the nightly rebuild
would empty. The lifetime intern charge is not moved, which is why
`unresolved` has no lifetime bound on interning
([security.md](security.md#admission-keys-and-daily-rates)). A change to a
host's own facts (its address block, or whether it is a large host)
moves nothing; the nightly rebuild accounts for it.

### The nightly rebuild

A nightly task makes both tables exact again.

Counting takes minutes (about 150 million rows), and the writers go
on flushing deltas meanwhile. So the rebuild works from one snapshot
and keeps what is flushed after it:

1. In one read-only `REPEATABLE READ` transaction, which takes no lock
   a writer waits for, it reads the stored values of both tables,
   counts the five record tables, and reads `actors` in batches of
   100,000 by id, adding each account's exact counts to the account's
   current buckets.
2. `stats_counters`: in one short transaction under `LOCK TABLE
   stats_counters IN SHARE ROW EXCLUSIVE MODE`, the stored value of
   each name is read again, the rows of the name are deleted, and one
   row is inserted at shard 0 with the count plus what was flushed
   since the snapshot (the value now minus the value then).
3. `host_usage`: under `LOCK TABLE host_usage IN SHARE ROW EXCLUSIVE
   MODE`, each of the four columns `stored_blocks`, `stored_items`,
   `stored_listblocks` and `stored_lists` of each bucket is set to its
   computed value plus what was flushed since the snapshot.
   `stored_interned` is a lifetime charge with no per-row record and
   is left as it is.

A delta for a row that the snapshot already counted, flushed after
the snapshot, is counted twice: at most one flush interval (5 seconds)
of changes, until the next rebuild.

The table lock is required. The writers' flushes update these rows one
at a time, in their own order; replacing all the rows at once under row
locks deadlocks with a flush in progress. The table lock waits for
flushes under way and holds back new ones for the moment the rows are
replaced, and because the counting is done beforehand that moment is
short.

The same nightly run recounts `lists.listblock_count` and
`lists.item_count` in batches, each list under its exclusive list lock,
repairs any drift, and runs the list's transition where a repaired
count crosses zero.

## The storage budget

`[storage]` sets two sizes, both compared with
`pg_database_size(current_database())`. The server and the backfill
process each measure it and keep their own gate state; the server's
monitor runs every minute and is the one that records refusal intervals
and defers and releases lists:

```toml
[storage]
budget_bytes = 70_000_000_000   # the setup wizard sets 70% of the disk you enter
hard_ceiling_bytes = 0          # 0 = 115% of budget_bytes; at least 105% of the budget
tombstone_ttl = "7d"
```

Everything counts against the budget: the record tables, their indexes,
history, and the UI's tables.

| Database size | What happens |
|---|---|
| ≥ 90% of the budget | The full sweep pauses: no full cycle starts, an open one stops enumerating, and tier-3 jobs are not dispatched. Gap repairs are not paused by this. |
| ≥ 100% of the budget | **Budget refusal** begins: creates and updates are refused, except from accounts on a large host (`limits.large_hosts`) and from jobs the admin requested. Pending lists that no fetch run has claimed, of owners not on a large host, are deferred (`deferred_by` 1). Backfill jobs of the gated classes (tiers 2 and 3, token-requested jobs, `system:resync`, `system:lists`) for accounts not on a large host run deletes-only. It ends when the size falls below 95%. |
| ≥ 110% of the budget | The dashboard reports the state as critical. |
| ≥ the hard ceiling | **Ceiling refusal** begins: creates and updates are refused from every account, large hosts and admin-requested jobs included. Every unclaimed pending list is deferred (`deferred_by` 2), and every job except an admin-requested one runs deletes-only. It ends when the size falls below 105% of the budget. |

Under either refusal:

- **deletes and reconciles are always applied.** A refusal never keeps
  a row that upstream has removed.
- a refused write is not lost silently. The author gets a `refused`
  debt in `relist_debt`, with `cap_type` 12 (budget), 13 (ceiling) or
  14 (a deletes-only listing), and the repository is listed again once
  the gate is open. See [coverage](coverage.md#re-list-debts).
- an interval is opened in `storage_refusals` when the first refusal
  begins and closed when the last ends. While one is open, coverage of
  the whole network is reported as partial
  ([coverage](coverage.md#network-scope)).
- an update of a stored record whose target is unchanged is refused
  with the row kept; an update that changes the target is refused with
  the row deleted and a refusal tombstone written, as described above.
- a sort index that is not yet built is not built while the database
  plus its estimated size would exceed the budget.

The two thresholds for ending a refusal are lower than the ones for
beginning it, so the gates do not flap around a boundary. The budget
as a bound on abuse, and the per-host caps that sit below it, are
described in [security](security.md#aggregate-bounds).

## Sizing

The per-row sizes below are estimates from a model, not measurements:
a 24-byte tuple header plus aligned columns per heap row; about 16
bytes of overhead plus the keys per B-tree entry; about 90% fill. The
row counts are estimates of the network's size.

| Table | Rows | Per row, heap + indexes | Total |
|---|---|---|---|
| `blocks` | 50–150 million | ~96 + ~85 ≈ 181 B | 9–27 GB |
| `list_items` | ~50 million | ~101 + ~105 ≈ 206 B | ~10–11 GB |
| `actors` | ~20–30 million | ~100 + ~82 ≈ 180 B | 3.6–5.4 GB |
| `list_blocks` | ~2 million | ~80 + ~110 ≈ 190 B | ~0.4 GB |
| `lists` | ~1 million | ~130 + ~72 ≈ 200 B | ~0.2 GB |
| `backfill_state` | ~3–6 million | ~110 B | ~0.5 GB |
| the three history tables | one per removal | ~210–255 B | ~0.2 GB per million rows |
| tombstones, queues, the rest | | | < 0.5 GB |

By the same model the sort indexes cost about 53–62 bytes per entry
when freshly built:
`blocks_by_subject_created` ~3–9 GB, `blocks_by_author_created`
~2.7–8 GB, `list_items_by_list_created` ~3 GB,
`list_blocks_by_list_created` ~0.1 GB; together **~9–20 GB**. An index
filled by inserts in arrival order reaches about 64–90 bytes per entry,
so on a long-running instance these figures can be exceeded by up to
about half until a `REINDEX`. With them each block insert maintains
four indexes instead of two.

| Stage | Data and indexes | With overhead |
|---|---|---|
| Day one | < 100 MB | < 150 MB |
| 30 days, firehose only | ~2–8 GB | ~2.5–10 GB |
| First full sweep complete | ~24–45 GB | ~30–56 GB |
| Growth per year afterwards | ~15–16 GB | ~18–20 GB |

"With overhead" allows about a quarter for bloat and free space inside
the database; it is the figure to plan a disk by, and the one the
[operator's guide](../guide/storage.md) gives. The sort indexes come
on top of either column (~9–20 GB at a complete index), as does disk
for WAL, vacuum, reindexing and dumps.

- In the first 30 days a firehose-only instance stores about 5 million
  blocks (~0.9 GB). Most of the rest is list items: the first new
  listblock on each popular list admits it, so a large share of the
  list items can arrive in the first weeks, as far as the storage
  budget allows.
- Yearly growth is about 64 million blocks (~11.6 GB), items of tracked
  lists (~2–3 GB) and new accounts (~1 GB).
- History is extra and depends on how often records are removed: at
  most about 31 million rows, ~6.6–7.9 GB, per year of retention if
  removals arrived at one a second; deletes arrive well below that.

The hot set for queries is `blocks_by_subject` (~7 GB at 150 million
rows) and `list_items_by_subject` (~1.5 GB). With 8 GB of RAM and
`shared_buffers = 2GB` the upper levels of both stay cached, and a
lookup by subject costs one to three random leaf reads.

### Hardware

- **Recommended:** 4 vCPU, 8 GB RAM, 500 GB SSD or NVMe. The disk
  leaves room for vacuum, reindexing, `pg_dump` and growth in the
  number of blocks.
- **About 100 GB of disk** works with limits. Firehose-only, after the
  early arrival of list items, the database grows by under 20 GB a
  year (~18–20 GB with overhead), which is several years of room. With the sweep on, a budget of
  70 GB (ceiling ~80 GB) is the practical maximum: the ceiling must
  stay under the disk with about 20 GB left for WAL and for rewriting
  the largest table. A completed sweep (~30–56 GB with overhead) then
  leaves about 14–40 GB of budget, from under one to about two years
  of growth, before the budget gate engages. The setup wizard
  warns when the sweep is on and the disk is under 150 GB.

The Compose file keeps the database in the named volume
`farsight-pgdata` and tunes Postgres for an 8 GB host:
`shared_buffers=2GB`, `effective_cache_size=5GB`,
`maintenance_work_mem=512MB`, `wal_compression=on`,
`max_wal_size=4GB`, `random_page_cost=1.1`, and lowered autovacuum
scale factors. See [the storage guide](../guide/storage.md)
for choosing a disk and a budget.
