-- Design §7.1 schema, verbatim (r15). Group 4: backfill_state, job_leases, backfill_cursors, backfill_queue, list_jobs, list_fetch_runs, discovery_state, sweep_cycles, cycle_outstanding.

CREATE TABLE backfill_state (
  actor_id        BIGINT PRIMARY KEY,
  state           SMALLINT NOT NULL,   -- 0 never 1 queued 2 running 3 done 4 failed
  backfilled_at   TIMESTAMPTZ,
  backfilled_witness TIMESTAMPTZ,
  clean_witness   TIMESTAMPTZ,          -- point of the last clean run (§5.2.1)
  last_outcome    SMALLINT,             -- 1 clean 2 complete_with_debts 3 inactive 4 failed
  backfill_rev    BIGINT,
  attempts        INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ,
  first_failed_at TIMESTAMPTZ,
  last_error      TEXT,
  current_run_id  BIGINT,
  current_run_point TIMESTAMPTZ,        -- coverage point of the running job, persisted at start (§3.7.1)
  inactive_at_listing BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE job_leases (
  did         TEXT COLLATE "C" PRIMARY KEY,
  lease_owner TEXT NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL
);

CREATE TABLE backfill_cursors (      -- one job's own cursor; never adopted by another job
  actor_id      BIGINT   NOT NULL,
  collection    SMALLINT NOT NULL,
  job_kind      SMALLINT NOT NULL,     -- 1 repo 2 list_fetch
  run_id        BIGINT   NOT NULL,     -- repo: job id; list_fetch: list_fetch_runs.id
  stamp_rev     BIGINT   NOT NULL,
  stamp_read_at TIMESTAMPTZ NOT NULL,
  late_stamp    BOOLEAN  NOT NULL,
  cursor        TEXT,
  prev_last     TEXT COLLATE "C",
  PRIMARY KEY (actor_id, collection, job_kind, run_id)
);

CREATE TABLE backfill_queue (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  actor_id     BIGINT   NOT NULL,
  kind         SMALLINT NOT NULL,    -- 1 repo 2 list_fetch 3 discovery
  tier         SMALLINT NOT NULL,
  priority     SMALLINT NOT NULL,
  requester    TEXT NOT NULL,        -- 'token:<id>' | 'admin' | 'system:lists' | 'system:resync' | 'system:sweep'
  cycle_id     BIGINT,
  enqueued_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  not_before   TIMESTAMPTZ
);

CREATE UNIQUE INDEX backfill_queue_one_waiting ON backfill_queue (actor_id, kind); -- waiting entries only; picked entries are deleted

CREATE INDEX backfill_queue_pick ON backfill_queue (tier, requester, priority DESC, enqueued_at);

CREATE TABLE list_jobs (             -- phase-1 waiting queue only; retry state is on lists
  list_id     BIGINT PRIMARY KEY,
  owner_id    BIGINT NOT NULL,
  admit_epoch INT NOT NULL,           -- result applied only if still current
  enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  not_before  TIMESTAMPTZ
);

CREATE INDEX list_jobs_by_owner ON list_jobs (owner_id);

CREATE TABLE list_fetch_runs (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id    BIGINT NOT NULL,
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  coverage_point TIMESTAMPTZ,         -- clock(run start), persisted at start
  finished_at TIMESTAMPTZ,
  outcome     SMALLINT                -- NULL running, 1 ok, 2 failed, 3 owner_inactive, 4 cancelled
);

CREATE TABLE discovery_state (
  actor_id          BIGINT PRIMARY KEY,
  state             SMALLINT NOT NULL,  -- 1 queued 2 running 3 done 4 failed
  source            TEXT NOT NULL,
  started_at        TIMESTAMPTZ,
  discovered_witness TIMESTAMPTZ,     -- clock(started_at): the coverage point
  completed_at      TIMESTAMPTZ,
  truncated         BOOLEAN NOT NULL DEFAULT false,
  refs_found        INT NOT NULL DEFAULT 0,
  last_error        TEXT
);

CREATE TABLE sweep_cycles (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind           SMALLINT NOT NULL,  -- 1 full 2 repair
  source         TEXT NOT NULL,
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
  repair_from    TIMESTAMPTZ
);

CREATE TABLE cycle_outstanding (      -- in-flight / retrying cycle members only (§5.4)
  cycle_id BIGINT NOT NULL,
  did      TEXT COLLATE "C" NOT NULL,
  state    SMALLINT NOT NULL,         -- 1 outstanding 2 terminal
  PRIMARY KEY (cycle_id, did)
);

UPDATE schema_version SET version = 4;
