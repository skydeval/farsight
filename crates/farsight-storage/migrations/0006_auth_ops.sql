-- Design §7.1 schema, verbatim (r15). Group 6: api_tokens, admin_sessions, op_errors, stats_counters.

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

CREATE TABLE stats_counters (       -- sharded: (name, shard) rows, summed on read
  name       TEXT NOT NULL,
  shard      SMALLINT NOT NULL,
  value      BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (name, shard)
);

UPDATE schema_version SET version = 6;
