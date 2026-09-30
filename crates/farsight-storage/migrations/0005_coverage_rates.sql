-- Design §7.1 schema, verbatim (r15). Group 5: subject_coverage, subject_lists, admission_rate, intern_rate, list_sched_keys, relist_debt, storage_refusals.

CREATE TABLE subject_coverage (
  actor_id     BIGINT   NOT NULL,
  scope        SMALLINT NOT NULL,    -- 1 block 2 list_chain
  confirmed_at TIMESTAMPTZ NOT NULL,
  refs_found   INT NOT NULL,
  PRIMARY KEY (actor_id, scope)
);

CREATE TABLE subject_lists (         -- every list discovery found naming the actor
  actor_id BIGINT NOT NULL, list_id BIGINT NOT NULL,
  PRIMARY KEY (actor_id, list_id)
);

CREATE TABLE admission_rate (         -- per admission key per UTC day (§4.2, §11.1)
  key        TEXT COLLATE "C" NOT NULL,
  utc_day    DATE NOT NULL,
  admissions INT  NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

CREATE TABLE intern_rate (            -- daily intern charges per cause key (§11.2); janitor drops rows > 2 days old
  key     TEXT COLLATE "C" NOT NULL,
  utc_day DATE NOT NULL,
  n       BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

CREATE TABLE list_sched_keys (        -- lanes a waiting list is eligible in (§5.5); exact, no cap
  list_id BIGINT NOT NULL,
  key     TEXT COLLATE "C" NOT NULL,
  n       INT  NOT NULL,              -- counted listblocks on the list from this key
  PRIMARY KEY (list_id, key)
);

CREATE INDEX list_sched_keys_by_key ON list_sched_keys (key);

CREATE TABLE relist_debt (            -- §3.7.3: the single source of actor-level exceptions
  actor_id      BIGINT   NOT NULL,
  reason        SMALLINT NOT NULL,    -- 1 unreachable 2 resync 3 capped 4 refused
  cap_type      SMALLINT,             -- for capped/refused: which cap or rate (feeder eligibility waits on it)
  since_witness TIMESTAMPTZ NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (actor_id, reason)
);

CREATE TABLE storage_refusals (       -- global budget refusal intervals (§11.2)
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_witness TIMESTAMPTZ NOT NULL,
  to_witness   TIMESTAMPTZ
);

UPDATE schema_version SET version = 5;
