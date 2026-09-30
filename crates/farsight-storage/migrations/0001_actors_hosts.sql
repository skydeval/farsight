-- Design §7.1 schema, verbatim (r15). Group 1: schema_version, actors, pds_hosts, host_usage.

CREATE TABLE schema_version (version INT NOT NULL);

CREATE TABLE actors (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  did             TEXT COLLATE "C" NOT NULL UNIQUE,
  status          SMALLINT NOT NULL DEFAULT 0,  -- §7.4 mapping
  status_at       TIMESTAMPTZ,
  pds_host_id     INT,
  pds_resolved_at TIMESTAMPTZ,
  authored_blocks INT NOT NULL DEFAULT 0,       -- cap bookkeeping (§11)
  authored_listblocks INT NOT NULL DEFAULT 0,
  authored_lists  INT NOT NULL DEFAULT 0,

  owned_items     INT NOT NULL DEFAULT 0,
  fetch_triggers  INT NOT NULL DEFAULT 0,
  admission_key   TEXT COLLATE "C",              -- current key (§5.5); updated on resolution/migration
  readmit_day     DATE,                          -- owner re-admission budget (§4.4)
  readmit_count   INT NOT NULL DEFAULT 0,
  resolve_failures INT NOT NULL DEFAULT 0,
  flags           SMALLINT NOT NULL DEFAULT 0   -- display only; relist_debt is the source of truth
);

CREATE TABLE pds_hosts (
  id              INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  host            TEXT COLLATE "C" NOT NULL UNIQUE,
  cap_key         TEXT COLLATE "C" NOT NULL,      -- registrable domain (eTLD+1, PSL); §11.2
  ip_bucket       TEXT,                           -- resolved /24 (v4) or /48 (v6)
  large           BOOLEAN NOT NULL DEFAULT false, -- matches limits.large_hosts
  rps_override    REAL,
  cooldown_until  TIMESTAMPTZ,
  requests_total  BIGINT NOT NULL DEFAULT 0,
  errors_total    BIGINT NOT NULL DEFAULT 0,
  latency_ewma_ms REAL,
  last_error      TEXT,
  last_error_at   TIMESTAMPTZ
);

CREATE TABLE host_usage (           -- per cap bucket (cap_key or ip_bucket), approximate
  bucket        TEXT COLLATE "C" PRIMARY KEY,
  capped_mask   SMALLINT NOT NULL DEFAULT 0,   -- one bit per cap type (blocks, items, listblocks, lists); set/cleared per cap with 95% reopen
  stored_blocks BIGINT NOT NULL DEFAULT 0,
  stored_items  BIGINT NOT NULL DEFAULT 0,
  stored_listblocks BIGINT NOT NULL DEFAULT 0,   -- incl. placeholder rows
  stored_lists  BIGINT NOT NULL DEFAULT 0,
  stored_interned BIGINT NOT NULL DEFAULT 0      -- non-large buckets' lifetime intern bound (§11.2)
);

INSERT INTO schema_version (version) VALUES (0);

-- Design §11.2: `actors` rows are never deleted (an in-memory DID -> id
-- cache relies on it). Enforced at runtime here, in addition to the storage
-- crate never issuing such a DELETE.
CREATE FUNCTION farsight_actors_never_deleted() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'farsight invariant: actors rows are never deleted (design 11.2)';
END;
$$;
CREATE TRIGGER actors_never_deleted BEFORE DELETE ON actors
  FOR EACH ROW EXECUTE FUNCTION farsight_actors_never_deleted();
CREATE TRIGGER actors_never_truncated BEFORE TRUNCATE ON actors
  FOR EACH STATEMENT EXECUTE FUNCTION farsight_actors_never_deleted();

UPDATE schema_version SET version = 1;
