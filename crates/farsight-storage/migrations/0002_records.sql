-- Design §7.1 schema, verbatim (r15). Group 2: blocks, lists, list_blocks, list_items, tombstones.

CREATE TABLE blocks (
  author_id  BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  subject_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ,
  rev        BIGINT NOT NULL,
  PRIMARY KEY (author_id, rkey)
);

CREATE INDEX blocks_by_subject ON blocks (subject_id, author_id, rkey);

CREATE TABLE lists (
  id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id         BIGINT NOT NULL,
  rkey             TEXT COLLATE "C" NOT NULL,
  record_state     SMALLINT NOT NULL DEFAULT 0, -- 0 unknown 1 present 2 deleted
  deferred_by      SMALLINT,                    -- budget / ceiling / host cap / lists cap / owner re-admission budget
  purpose          SMALLINT,                    -- 1 mod 2 curate 3 reference 0 other
  name             TEXT,
  created_at       TIMESTAMPTZ,
  rev              BIGINT,
  listblock_count  INT NOT NULL DEFAULT 0,
  track_state      SMALLINT NOT NULL DEFAULT 0, -- §4.1 (incl. dead)
  capped           BOOLEAN NOT NULL DEFAULT false,
  item_count       INT NOT NULL DEFAULT 0,
  admitted_at      TIMESTAMPTZ,
  admit_epoch      INT NOT NULL DEFAULT 0,
  refresh_requested BOOLEAN NOT NULL DEFAULT false,
  purge_then       SMALLINT,                 -- target state after purge
  phase1_epoch     INT NOT NULL DEFAULT 0,
  phase1_attempts  INT NOT NULL DEFAULT 0,
  retain_until     TIMESTAMPTZ,
  fetched_at       TIMESTAMPTZ,
  fetched_witness  TIMESTAMPTZ,
  fetch_run_id     BIGINT,
  fetch_run_epoch  INT,
  fetch_attempts   INT NOT NULL DEFAULT 0,
  next_retry_at    TIMESTAMPTZ,       -- missing / unavailable retries
  UNIQUE (owner_id, rkey)
);

CREATE INDEX lists_by_state ON lists (track_state) WHERE track_state <> 0;

CREATE TABLE list_blocks (
  author_id  BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  list_id    BIGINT NOT NULL,
  counted    BOOLEAN NOT NULL DEFAULT true, -- false: author over trigger cap (§4.2); sticky
  witnessed_at TIMESTAMPTZ,                  -- first stored by firehose: its witness; NULL if by a listing (§3.7.4); sticky except on subject change
  sched_key  TEXT COLLATE "C",               -- admission key charged at insert; list_sched_keys decrements this, not the author's current key
  created_at TIMESTAMPTZ,
  rev        BIGINT NOT NULL,
  PRIMARY KEY (author_id, rkey)
);

CREATE INDEX list_blocks_by_list ON list_blocks (list_id, author_id, rkey);

CREATE INDEX list_blocks_by_author_list ON list_blocks (author_id, list_id); -- checkBlocks outgoing

CREATE TABLE list_items (
  owner_id   BIGINT NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  list_id    BIGINT NOT NULL,
  subject_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ,
  rev        BIGINT NOT NULL,
  PRIMARY KEY (owner_id, rkey)
);

CREATE INDEX list_items_by_subject ON list_items (subject_id, list_id);
CREATE INDEX list_items_by_list    ON list_items (list_id, subject_id, rkey);

CREATE TABLE tombstones (
  collection SMALLINT NOT NULL,   -- 1 block 2 listblock 3 list 4 listitem
  author_id  BIGINT   NOT NULL,
  rkey       TEXT COLLATE "C" NOT NULL,
  rev        BIGINT   NOT NULL,
  deleted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (collection, author_id, rkey)
);

CREATE INDEX tombstones_by_age ON tombstones (deleted_at);

UPDATE schema_version SET version = 2;
