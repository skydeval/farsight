-- Design §7.1 schema, verbatim (r15). Group 3: firehose_state, firehose_clock, firehose_gaps.

CREATE TABLE firehose_state (
  id               SMALLINT PRIMARY KEY CHECK (id = 1),
  source_url       TEXT,
  protocol         SMALLINT,          -- 1 v1 2 v2
  cursor_seq       BIGINT,            -- v2, instance-local
  cursor_us        BIGINT,            -- v1 / failover timestamp form
  applied_through  TIMESTAMPTZ,       -- witness time of last applied event
  first_applied_at TIMESTAMPTZ,
  connected        BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE firehose_clock (       -- server commit time ↔ applied_through, one row per batch; 1-min granularity after 24 h; 30 d retention (§3.7)
  server_at   TIMESTAMPTZ PRIMARY KEY,
  witness_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE firehose_gaps (         -- times on the witness clock
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_at      TIMESTAMPTZ NOT NULL,
  to_at        TIMESTAMPTZ,           -- NULL while still open (v1 interval)
  cause        SMALLINT NOT NULL,     -- 1 cursor_too_old 2 heuristic 3 failover 4 sync_unavailable (v1 interval)
  detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  healed_at    TIMESTAMPTZ,
  healed_witness TIMESTAMPTZ,
  repair_cycle_id BIGINT
);

UPDATE schema_version SET version = 3;
