-- The Farsight schema. Every table is described in docs/design/storage.md.
--
-- Every DID and record key is TEXT COLLATE "C": bytewise order is the
-- order a PDS lists records in. Accounts are referred to by actors.id.
-- A rev is a repository revision (a TID) decoded to a BIGINT. Times
-- named *_witness, first_seen, last_seen and removed_at are on the
-- witness clock (docs/design/coverage.md).
--
-- A column that stores a code has a CHECK naming the codes it takes;
-- the codes are defined in crates/farsight-storage/src/codes.rs, and a
-- test there compares the two.

-- The schema version: one row. The server writes it by running the
-- migrations; the backfill waits for the version it was built for.
CREATE TABLE schema_version (version INT NOT NULL);

INSERT INTO schema_version (version) VALUES (0);

-- ---------------------------------------------------------------------
-- Actors and hosts
-- ---------------------------------------------------------------------

-- One row per DID seen as an author, a subject or a list owner.
CREATE TABLE actors (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  did             TEXT COLLATE "C" NOT NULL UNIQUE,
  status          SMALLINT NOT NULL DEFAULT 0,  -- 0 active 1 deactivated 2 takendown 3 suspended 4 deleted 5 throttled 6 desynchronized 7 unknown
  status_at       TIMESTAMPTZ,
  pds_host_id     INT,
  pds_resolved_at TIMESTAMPTZ,
  authored_blocks INT NOT NULL DEFAULT 0,       -- exact counts of the account's stored records, for the per-author caps
  authored_listblocks INT NOT NULL DEFAULT 0,
  authored_lists  INT NOT NULL DEFAULT 0,

  owned_items     INT NOT NULL DEFAULT 0,
  fetch_triggers  INT NOT NULL DEFAULT 0,
  admission_key   TEXT COLLATE "C",              -- the key list admissions are charged to; follows the account's host
  readmit_day     DATE,                          -- the owner's re-admission budget: the UTC day and the count within it
  readmit_count   INT NOT NULL DEFAULT 0,
  resolve_failures INT NOT NULL DEFAULT 0,
  flags           SMALLINT NOT NULL DEFAULT 0,  -- display only; relist_debt is the source of truth
  CONSTRAINT actors_status_code CHECK (status IN (0, 1, 2, 3, 4, 5, 6, 7))
);

-- Rows of actors are never deleted: an in-memory DID-to-id cache relies
-- on it. The storage crate issues no such statement, and these triggers
-- refuse one from anywhere else.
CREATE FUNCTION farsight_actors_never_deleted() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'farsight invariant: actors rows are never deleted';
END;
$$;
CREATE TRIGGER actors_never_deleted BEFORE DELETE ON actors
  FOR EACH ROW EXECUTE FUNCTION farsight_actors_never_deleted();
CREATE TRIGGER actors_never_truncated BEFORE TRUNCATE ON actors
  FOR EACH STATEMENT EXECUTE FUNCTION farsight_actors_never_deleted();

-- One row per PDS host name: the buckets its accounts are capped under
-- and the request statistics the backfill keeps for it.
CREATE TABLE pds_hosts (
  id              INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  host            TEXT COLLATE "C" NOT NULL UNIQUE,
  cap_key         TEXT COLLATE "C" NOT NULL,      -- the registrable domain (eTLD+1 by the public suffix list)
  ip_bucket       TEXT,                           -- the resolved /24 (IPv4) or /48 (IPv6)
  large           BOOLEAN NOT NULL DEFAULT false, -- the host is one of limits.large_hosts
  rps_override    REAL,
  cooldown_until  TIMESTAMPTZ,
  requests_total  BIGINT NOT NULL DEFAULT 0,
  errors_total    BIGINT NOT NULL DEFAULT 0,
  latency_ewma_ms REAL,
  last_error      TEXT,
  last_error_at   TIMESTAMPTZ
);

-- Approximate stored-row counts per cap bucket (a cap_key or an
-- ip_bucket).
CREATE TABLE host_usage (
  bucket        TEXT COLLATE "C" PRIMARY KEY,
  capped_mask   SMALLINT NOT NULL DEFAULT 0,   -- one bit per cap (blocks, items, listblocks, lists) the bucket is over; a bit clears below 95% of its cap
  stored_blocks BIGINT NOT NULL DEFAULT 0,
  stored_items  BIGINT NOT NULL DEFAULT 0,
  stored_listblocks BIGINT NOT NULL DEFAULT 0,   -- includes rows that point at a list whose record is unknown
  stored_lists  BIGINT NOT NULL DEFAULT 0,
  stored_interned BIGINT NOT NULL DEFAULT 0      -- actors rows the bucket caused, over its lifetime; bounded for buckets that are not large
);

-- ---------------------------------------------------------------------
-- Records
-- ---------------------------------------------------------------------

-- One row per app.bsky.graph.block record.
CREATE TABLE blocks (
  author_id  BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  subject_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ,            -- as the record states it
  rev        BIGINT NOT NULL,
  first_seen TIMESTAMPTZ NOT NULL,   -- when the row was stored
  last_seen  TIMESTAMPTZ NOT NULL,   -- when a write was last applied to it
  PRIMARY KEY (author_id, rkey)
);

CREATE INDEX blocks_by_subject ON blocks (subject_id, author_id, rkey);

-- One row per list that anything refers to, whether or not its record
-- has been seen: the record's fields, then the tracking state
-- (docs/design/list-indexing.md).
CREATE TABLE lists (
  id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id         BIGINT NOT NULL,
  rkey             TEXT COLLATE "C" NOT NULL,
  record_state     SMALLINT NOT NULL DEFAULT 0, -- 0 unknown 1 present 2 deleted
  deferred_by      SMALLINT,                    -- the gate that deferred the list: 1 budget 2 ceiling 3 host cap 4 lists cap 5 owner re-admissions
  purpose          SMALLINT,                    -- 0 other 1 moderation 2 curation 3 reference
  name             TEXT,
  created_at       TIMESTAMPTZ,
  rev              BIGINT,
  listblock_count  INT NOT NULL DEFAULT 0,      -- counted listblocks that point at the list
  track_state      SMALLINT NOT NULL DEFAULT 0, -- 0 untracked 1 pending 2 ready 3 retained 4 unavailable 5 purging 6 missing 7 dead 8 deferred
  capped           BOOLEAN NOT NULL DEFAULT false,
  item_count       INT NOT NULL DEFAULT 0,
  admitted_at      TIMESTAMPTZ,
  admit_epoch      INT NOT NULL DEFAULT 0,
  refresh_requested BOOLEAN NOT NULL DEFAULT false,
  purge_then       SMALLINT,                 -- the track_state the list takes when its purge ends
  phase1_epoch     INT NOT NULL DEFAULT 0,
  phase1_attempts  INT NOT NULL DEFAULT 0,
  retain_until     TIMESTAMPTZ,
  fetched_at       TIMESTAMPTZ,
  fetched_witness  TIMESTAMPTZ,
  fetch_run_id     BIGINT,
  fetch_run_epoch  INT,
  fetch_attempts   INT NOT NULL DEFAULT 0,
  next_retry_at    TIMESTAMPTZ,       -- when a missing or unavailable list is tried again
  description      TEXT,              -- the record's text, truncated
  avatar_cid       TEXT,              -- the CID of the record's avatar blob; the image is never stored
  UNIQUE (owner_id, rkey),
  CONSTRAINT lists_record_state_code CHECK (record_state IN (0, 1, 2)),
  CONSTRAINT lists_deferred_by_code CHECK (deferred_by IN (1, 2, 3, 4, 5)),
  CONSTRAINT lists_purpose_code CHECK (purpose IN (0, 1, 2, 3)),
  CONSTRAINT lists_track_state_code CHECK (track_state IN (0, 1, 2, 3, 4, 5, 6, 7, 8)),
  CONSTRAINT lists_purge_then_code CHECK (purge_then IN (0, 1, 2, 3, 4, 5, 6, 7, 8))
);

CREATE INDEX lists_by_state ON lists (track_state) WHERE track_state <> 0;

-- One row per app.bsky.graph.listblock record, pointing at a lists row.
CREATE TABLE list_blocks (
  author_id  BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  list_id    BIGINT NOT NULL,
  counted    BOOLEAN NOT NULL DEFAULT true, -- false: the author is over the trigger cap or the admission rate; only counted rows make a list tracked
  witnessed_at TIMESTAMPTZ,                  -- the witness time of the firehose event that stored the row; NULL when a listing stored it
  sched_key  TEXT COLLATE "C",               -- the admission key charged when the row was inserted; list_sched_keys counts by it
  created_at TIMESTAMPTZ,
  rev        BIGINT NOT NULL,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (author_id, rkey)
);

CREATE INDEX list_blocks_by_list ON list_blocks (list_id, author_id, rkey);

CREATE INDEX list_blocks_by_author_list ON list_blocks (author_id, list_id); -- the outgoing direction of checkBlocks

-- One row per app.bsky.graph.listitem record of a tracked list.
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

-- The rev of a recent delete per record key, so that an older write
-- arriving later does not bring the record back.
CREATE TABLE tombstones (
  collection SMALLINT NOT NULL,   -- 1 block 2 listblock 3 list 4 listitem
  author_id  BIGINT   NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  rev        BIGINT   NOT NULL,
  deleted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (collection, author_id, rkey),
  CONSTRAINT tombstones_collection_code CHECK (collection IN (1, 2, 3, 4))
);

CREATE INDEX tombstones_by_age ON tombstones (deleted_at);

-- ---------------------------------------------------------------------
-- Firehose state and gaps (docs/design/firehose.md)
-- ---------------------------------------------------------------------

-- The current session: a single row.
CREATE TABLE firehose_state (
  id               SMALLINT PRIMARY KEY CHECK (id = 1),
  source_url       TEXT,
  protocol         SMALLINT,          -- 1 v1 2 v2
  cursor_seq       BIGINT,            -- v2; local to the instance
  cursor_us        BIGINT,            -- v1, and the timestamp form a failover uses
  applied_through  TIMESTAMPTZ,       -- the witness time of the last applied event
  first_applied_at TIMESTAMPTZ,
  connected        BOOLEAN NOT NULL DEFAULT false,
  CONSTRAINT firehose_state_protocol_code CHECK (protocol IN (1, 2))
);

-- One cursor per Jetstream instance, so that a failover back to an
-- instance resumes from its own cursor. Within one source_url a cursor
-- only moves forward.
CREATE TABLE firehose_cursors (
  source_url           TEXT PRIMARY KEY,
  protocol             SMALLINT,      -- 1 v1 2 v2
  cursor_seq           BIGINT,        -- v2; local to the instance
  cursor_us            BIGINT,        -- v1, and the timestamp form
  last_connected_at    TIMESTAMPTZ,
  last_applied_through TIMESTAMPTZ,   -- the witness time of the last event applied from this instance
  CONSTRAINT firehose_cursors_protocol_code CHECK (protocol IN (1, 2))
);

-- Server commit time against applied_through, one row per applied
-- batch; thinned to one a minute after 24 hours and kept 30 days.
CREATE TABLE firehose_clock (
  server_at   TIMESTAMPTZ PRIMARY KEY,
  witness_at  TIMESTAMPTZ NOT NULL
);

-- Intervals of the witness clock during which events may have been
-- lost.
CREATE TABLE firehose_gaps (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_at      TIMESTAMPTZ NOT NULL,
  to_at        TIMESTAMPTZ,           -- NULL while the gap is open (an interval spent on v1)
  cause        SMALLINT NOT NULL,     -- 1 cursor_too_old 2 heuristic 3 failover 4 sync_unavailable
  detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  healed_at    TIMESTAMPTZ,
  healed_witness TIMESTAMPTZ,
  repair_cycle_id BIGINT,
  CONSTRAINT firehose_gaps_cause_code CHECK (cause IN (1, 2, 3, 4))
);

-- ---------------------------------------------------------------------
-- Backfill queue, leases and cycles (docs/design/backfill.md)
-- ---------------------------------------------------------------------

-- One row per account whose repository has been queued for listing.
CREATE TABLE backfill_state (
  actor_id        BIGINT PRIMARY KEY,
  state           SMALLINT NOT NULL,   -- 0 never 1 queued 2 running 3 done 4 failed
  backfilled_at   TIMESTAMPTZ,
  backfilled_witness TIMESTAMPTZ,
  clean_witness   TIMESTAMPTZ,          -- the coverage point of the last clean run
  last_outcome    SMALLINT,             -- 1 clean 2 complete_with_debts 3 inactive 4 failed
  backfill_rev    BIGINT,
  attempts        INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ,
  first_failed_at TIMESTAMPTZ,
  last_error      TEXT,
  current_run_id  BIGINT,
  current_run_point TIMESTAMPTZ,        -- the coverage point of the running job, stored when it starts
  inactive_at_listing BOOLEAN NOT NULL DEFAULT false,
  CONSTRAINT backfill_state_state_code CHECK (state IN (0, 1, 2, 3, 4)),
  CONSTRAINT backfill_state_last_outcome_code CHECK (last_outcome IN (1, 2, 3, 4))
);

-- Which worker holds the job for a DID, and until when.
CREATE TABLE job_leases (
  did         TEXT COLLATE "C" PRIMARY KEY,
  lease_owner TEXT NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL
);

-- The paging position and the listing stamp of one running job. A
-- cursor belongs to its run and is never adopted by another.
CREATE TABLE backfill_cursors (
  actor_id      BIGINT   NOT NULL,
  collection    SMALLINT NOT NULL,     -- 1 block 2 listblock 3 list 4 listitem
  job_kind      SMALLINT NOT NULL,     -- 1 repo 2 list_fetch
  run_id        BIGINT   NOT NULL,     -- repo: the job's id; list_fetch: list_fetch_runs.id
  stamp_rev     BIGINT   NOT NULL,
  stamp_read_at TIMESTAMPTZ NOT NULL,
  late_stamp    BOOLEAN  NOT NULL,
  cursor        TEXT,
  prev_last     TEXT COLLATE "C",
  PRIMARY KEY (actor_id, collection, job_kind, run_id),
  CONSTRAINT backfill_cursors_collection_code CHECK (collection IN (1, 2, 3, 4)),
  CONSTRAINT backfill_cursors_job_kind_code CHECK (job_kind IN (1, 2))
);

-- Waiting jobs only: a picked entry is deleted.
CREATE TABLE backfill_queue (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  actor_id     BIGINT   NOT NULL,
  kind         SMALLINT NOT NULL,    -- 1 repo 2 list_fetch 3 discovery
  tier         SMALLINT NOT NULL,    -- 1 on demand 2 active 3 sweep
  priority     SMALLINT NOT NULL,    -- 0 normal 1 high
  requester    TEXT NOT NULL,        -- 'token:<id>', 'admin', or a system requester such as 'system:sweep'
  cycle_id     BIGINT,
  enqueued_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  not_before   TIMESTAMPTZ,
  CONSTRAINT backfill_queue_kind_code CHECK (kind IN (1, 2, 3)),
  CONSTRAINT backfill_queue_tier_code CHECK (tier IN (1, 2, 3)),
  CONSTRAINT backfill_queue_priority_code CHECK (priority IN (0, 1))
);

CREATE UNIQUE INDEX backfill_queue_one_waiting ON backfill_queue (actor_id, kind); -- one waiting entry per account and kind

CREATE INDEX backfill_queue_pick ON backfill_queue (tier, requester, priority DESC, enqueued_at);

-- Lists waiting for their record to be read. Retry state is on lists.
CREATE TABLE list_jobs (
  list_id     BIGINT PRIMARY KEY,
  owner_id    BIGINT NOT NULL,
  admit_epoch INT NOT NULL,           -- the result is applied only if this is still the list's
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  not_before  TIMESTAMPTZ
);

CREATE INDEX list_jobs_by_owner ON list_jobs (owner_id);

-- One row per run that fetches an owner's list items.
CREATE TABLE list_fetch_runs (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id    BIGINT NOT NULL,
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  coverage_point TIMESTAMPTZ,         -- the witness time of the run's start, stored when it starts
  finished_at TIMESTAMPTZ,
  outcome     SMALLINT,               -- NULL while running; 1 ok 2 failed 3 owner_inactive 4 cancelled
  CONSTRAINT list_fetch_runs_outcome_code CHECK (outcome IN (1, 2, 3, 4))
);

-- Where an account's discovery run stands, and its coverage point.
CREATE TABLE discovery_state (
  actor_id          BIGINT PRIMARY KEY,
  state             SMALLINT NOT NULL,  -- 1 queued 2 running 3 done 4 failed
  source            TEXT NOT NULL,
  started_at        TIMESTAMPTZ,
  discovered_witness TIMESTAMPTZ,     -- the witness time of started_at: the coverage point
  completed_at      TIMESTAMPTZ,
  truncated         BOOLEAN NOT NULL DEFAULT false,
  refs_found        INT NOT NULL DEFAULT 0,
  last_error        TEXT,
  CONSTRAINT discovery_state_state_code CHECK (state IN (1, 2, 3, 4))
);

-- One row per full sweep or gap repair, with its enumeration checkpoint
-- and progress.
CREATE TABLE sweep_cycles (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind           SMALLINT NOT NULL,  -- 1 full 2 repair
  source         TEXT NOT NULL,      -- what the cycle enumerates
  collections    SMALLINT[] NOT NULL,
  started_at     TIMESTAMPTZ NOT NULL,
  effective_start TIMESTAMPTZ,
  effective_start_witness TIMESTAMPTZ,
  enumerated_at  TIMESTAMPTZ,
  completed_at   TIMESTAMPTZ,
  completed_witness TIMESTAMPTZ,
  checkpoint     TEXT,
  total_est      BIGINT,
  done           BIGINT NOT NULL DEFAULT 0,
  failed_terminal BIGINT NOT NULL DEFAULT 0,
  repair_from    TIMESTAMPTZ,
  CONSTRAINT sweep_cycles_kind_code CHECK (kind IN (1, 2)),
  CONSTRAINT sweep_cycles_source_code CHECK (source IN ('relay_collections', 'relay_repos', 'plc', 'known_dids')),
  CONSTRAINT sweep_cycles_collections_code CHECK (collections <@ ARRAY[1, 2, 3, 4]::SMALLINT[])
);

-- The members of a cycle that are in flight or being retried.
CREATE TABLE cycle_outstanding (
  cycle_id BIGINT NOT NULL,
  did      TEXT COLLATE "C" NOT NULL,
  state    SMALLINT NOT NULL,         -- 1 outstanding 2 terminal
  PRIMARY KEY (cycle_id, did),
  CONSTRAINT cycle_outstanding_state_code CHECK (state IN (1, 2))
);

-- ---------------------------------------------------------------------
-- Coverage and rates (docs/design/coverage.md)
-- ---------------------------------------------------------------------

-- What a discovery run confirmed about an account as a subject.
CREATE TABLE subject_coverage (
  actor_id     BIGINT   NOT NULL,
  scope        SMALLINT NOT NULL,    -- 1 block 2 list_chain
  confirmed_at TIMESTAMPTZ NOT NULL,
  refs_found   INT NOT NULL,
  PRIMARY KEY (actor_id, scope),
  CONSTRAINT subject_coverage_scope_code CHECK (scope IN (1, 2))
);

-- Every list a discovery run found naming the account.
CREATE TABLE subject_lists (
  actor_id BIGINT NOT NULL, list_id BIGINT NOT NULL,
  PRIMARY KEY (actor_id, list_id)
);

-- The single source of per-account exceptions to coverage.
CREATE TABLE relist_debt (
  actor_id      BIGINT   NOT NULL,
  reason        SMALLINT NOT NULL,    -- 1 unreachable 2 resync 3 capped 4 refused
  cap_type      SMALLINT,             -- for capped and refused: the cap, rate or gate behind the debt
  since_witness TIMESTAMPTZ NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (actor_id, reason),
  CONSTRAINT relist_debt_reason_code CHECK (reason IN (1, 2, 3, 4)),
  CONSTRAINT relist_debt_cap_type_code CHECK (cap_type IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
);

-- The intervals during which the storage budget or the hard ceiling
-- refused writes.
CREATE TABLE storage_refusals (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_witness TIMESTAMPTZ NOT NULL,
  to_witness   TIMESTAMPTZ
);

-- Lists admitted per admission key and UTC day.
CREATE TABLE admission_rate (
  key        TEXT COLLATE "C" NOT NULL,
  utc_day    DATE NOT NULL,
  admissions INT  NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

-- actors rows caused per key and UTC day.
CREATE TABLE intern_rate (
  key     TEXT COLLATE "C" NOT NULL,
  utc_day DATE NOT NULL,
  n       BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

-- History rows written per admission key and UTC day, over the three
-- history tables.
CREATE TABLE history_rate (
  key     TEXT COLLATE "C" NOT NULL,
  utc_day DATE NOT NULL,
  n       BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

-- For a list waiting to be fetched: the admission keys it is eligible
-- under.
CREATE TABLE list_sched_keys (
  list_id BIGINT NOT NULL,
  key     TEXT COLLATE "C" NOT NULL,
  n       INT  NOT NULL,              -- counted listblocks on the list from this key
  PRIMARY KEY (list_id, key)
);

CREATE INDEX list_sched_keys_by_key ON list_sched_keys (key);

-- ---------------------------------------------------------------------
-- Auth and operations
-- ---------------------------------------------------------------------

-- API tokens by the SHA-256 of the token; the token is not stored.
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

-- Admin sessions by a SHA-256 derived from the session cookie, with the
-- session's CSRF secret.
CREATE TABLE admin_sessions (
  id_sha256  BYTEA PRIMARY KEY,
  csrf       BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
  ip         INET,
  user_agent TEXT
);

-- The operational error log the dashboard shows.
CREATE TABLE op_errors (
  id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  component TEXT NOT NULL,
  did       TEXT,
  host      TEXT,
  message   TEXT NOT NULL
);

-- Approximate totals, sharded: a total is the sum of its rows.
CREATE TABLE stats_counters (
  name       TEXT NOT NULL,
  shard      SMALLINT NOT NULL,
  value      BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (name, shard)
);

-- ---------------------------------------------------------------------
-- History (docs/design/history.md)
-- ---------------------------------------------------------------------

-- Removed blocks.
CREATE TABLE blocks_history (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  author_id   BIGINT NOT NULL,
  rkey        TEXT COLLATE "C" NOT NULL,
  subject_id  BIGINT NOT NULL,
  created_at  TIMESTAMPTZ,           -- as the author stated it; display only
  first_seen  TIMESTAMPTZ,           -- copied from the live row
  last_seen   TIMESTAMPTZ,           -- copied from the live row
  removed_at  TIMESTAMPTZ NOT NULL,  -- when the removal was applied
  removed_rev BIGINT,                -- the rev of the firehose event that removed the record; NULL when a listing did
  cause       SMALLINT NOT NULL,     -- 1 delete 2 subject_change 3 refused_update 4 reconcile
  CONSTRAINT blocks_history_cause_code CHECK (cause IN (1, 2, 3, 4))
);

CREATE INDEX blocks_history_by_subject ON blocks_history (subject_id, removed_at DESC, id DESC);
CREATE INDEX blocks_history_by_author  ON blocks_history (author_id, removed_at DESC, id DESC);

-- Removed listblocks.
CREATE TABLE list_blocks_history (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  author_id     BIGINT NOT NULL,
  rkey          TEXT COLLATE "C" NOT NULL,
  list_owner_id BIGINT NOT NULL,     -- the list by owner and rkey, not by lists.id: a lists row can be deleted
  list_rkey     TEXT COLLATE "C" NOT NULL,
  created_at    TIMESTAMPTZ,
  first_seen    TIMESTAMPTZ,
  last_seen     TIMESTAMPTZ,
  removed_at    TIMESTAMPTZ NOT NULL,
  removed_rev   BIGINT,
  cause         SMALLINT NOT NULL,   -- 1 delete 2 subject_change 3 refused_update 4 reconcile
  CONSTRAINT list_blocks_history_cause_code CHECK (cause IN (1, 2, 3, 4))
);

CREATE INDEX list_blocks_history_by_list
  ON list_blocks_history (list_owner_id, list_rkey, removed_at DESC, id DESC);
CREATE INDEX list_blocks_history_by_author
  ON list_blocks_history (author_id, removed_at DESC, id DESC);

-- Removed list memberships.
CREATE TABLE list_items_history (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id    BIGINT NOT NULL,       -- the list's owner, who is also the listitem's author
  rkey        TEXT COLLATE "C" NOT NULL,  -- the listitem's rkey
  list_rkey   TEXT COLLATE "C" NOT NULL,  -- the list by owner and rkey, not by lists.id
  subject_id  BIGINT NOT NULL,
  created_at  TIMESTAMPTZ,           -- as the owner stated it; display only
  first_seen  TIMESTAMPTZ,           -- copied from the live row
  last_seen   TIMESTAMPTZ,           -- copied from the live row
  removed_at  TIMESTAMPTZ NOT NULL,  -- when the removal was applied
  removed_rev BIGINT,                -- the rev of the firehose event that removed the record; else NULL
  cause       SMALLINT NOT NULL,     -- 1 delete 2 subject_change 3 refused_update 4 reconcile 5 list_deleted
  CONSTRAINT list_items_history_cause_code CHECK (cause IN (1, 2, 3, 4, 5))
);

CREATE INDEX list_items_history_by_list
  ON list_items_history (owner_id, list_rkey, removed_at DESC, id DESC);
CREATE INDEX list_items_history_by_subject
  ON list_items_history (subject_id, removed_at DESC, id DESC);

-- The intervals during which removals were recorded; server time.
CREATE TABLE history_windows (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  to_at   TIMESTAMPTZ               -- NULL while recording
);

-- ---------------------------------------------------------------------
-- Tables for the web UI (docs/design/web-ui.md). Emptying any of them
-- is safe: the server fills it again.
-- ---------------------------------------------------------------------

-- One row per DID whose handle has been checked: the handle that
-- verified in both directions, or an empty one when there is none to
-- show.
CREATE TABLE handle_cache (
  did         TEXT COLLATE "C" PRIMARY KEY,
  handle      TEXT NOT NULL,
  resolved_at TIMESTAMPTZ NOT NULL   -- when the handle was last checked
);

-- Accounts whose handle is checked ahead of the handle pass's walk over
-- actors. The firehose writer adds an account when an identity event
-- for it arrives; the pass takes the oldest first and removes a row
-- once the check gave an answer.
CREATE TABLE handle_due (
  did      TEXT COLLATE "C" PRIMARY KEY,
  asked_at TIMESTAMPTZ NOT NULL DEFAULT now()   -- not served before this
);
CREATE INDEX handle_due_asked ON handle_due (asked_at);

-- Which image an account's profile uses as its avatar: the CID of the
-- blob, never the image. An empty cid records that the profile has no
-- avatar. A row is used for a day, then read again.
CREATE TABLE avatar_cache (
  did        TEXT COLLATE "C" PRIMARY KEY,
  cid        TEXT NOT NULL,
  checked_at TIMESTAMPTZ NOT NULL
);

-- A log of blocks as they are stored, kept for a day. A row is written
-- when a stored block's own date is within the day before it arrived,
-- so that history read by the backfill does not count as recent.
CREATE TABLE block_recent (
  at         TIMESTAMPTZ NOT NULL,
  author_id  BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  subject_id BIGINT NOT NULL
);

CREATE INDEX block_recent_at ON block_recent (at);

-- The home page's top lists as computed, one row per list; rows is JSON
-- text. The home page reads it and never runs the queries.
CREATE TABLE top_lists (
  kind        TEXT COLLATE "C" PRIMARY KEY,
  computed_at TIMESTAMPTZ NOT NULL,
  rows        TEXT NOT NULL,
  CONSTRAINT top_lists_kind_code CHECK (kind IN ('blockers_all', 'blocked_all', 'blockers_day', 'blocked_day'))
);

UPDATE schema_version SET version = 1;
