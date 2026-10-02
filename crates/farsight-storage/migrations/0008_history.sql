-- Design §7.1 (r18.4, r19.2). Group 8: block and list-membership history (§7.7, §7.8).

-- Witness bounds on live rows. Nullable, no backfill: a metadata-only
-- change at any row count. NULL first_seen = stored before these columns.
ALTER TABLE blocks      ADD COLUMN first_seen TIMESTAMPTZ, ADD COLUMN last_seen TIMESTAMPTZ;
ALTER TABLE list_blocks ADD COLUMN first_seen TIMESTAMPTZ, ADD COLUMN last_seen TIMESTAMPTZ;
ALTER TABLE list_items  ADD COLUMN first_seen TIMESTAMPTZ, ADD COLUMN last_seen TIMESTAMPTZ;

CREATE TABLE blocks_history (        -- removed blocks (§7.7)
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  author_id   BIGINT NOT NULL,
  rkey        TEXT COLLATE "C" NOT NULL,
  subject_id  BIGINT NOT NULL,
  created_at  TIMESTAMPTZ,           -- author-claimed; display only
  first_seen  TIMESTAMPTZ,           -- copied from the live row; NULL: stored before the columns existed
  last_seen   TIMESTAMPTZ,           -- copied from the live row
  removed_at  TIMESTAMPTZ NOT NULL,  -- when Farsight applied the removal (§7.7)
  removed_rev BIGINT,                -- commit rev of the removing firehose event; NULL if learned from a listing
  cause       SMALLINT NOT NULL      -- 1 delete 2 subject_change 3 refused_update 4 reconcile
);

CREATE INDEX blocks_history_by_subject ON blocks_history (subject_id, removed_at DESC, id DESC);
CREATE INDEX blocks_history_by_author  ON blocks_history (author_id, removed_at DESC, id DESC);

CREATE TABLE list_blocks_history (   -- removed listblocks (§7.7)
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  author_id     BIGINT NOT NULL,
  rkey          TEXT COLLATE "C" NOT NULL,
  list_owner_id BIGINT NOT NULL,     -- the list by owner and rkey, not by lists.id: list rows can be deleted (§11.2)
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

CREATE TABLE list_items_history (    -- removed listitems (§7.8)
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id    BIGINT NOT NULL,       -- the list's owner, who is also the listitem's author (authority rule)
  rkey        TEXT COLLATE "C" NOT NULL,  -- the listitem's rkey
  list_rkey   TEXT COLLATE "C" NOT NULL,  -- the list by owner and rkey, not by lists.id (§11.2)
  subject_id  BIGINT NOT NULL,
  created_at  TIMESTAMPTZ,           -- owner-claimed; display only
  first_seen  TIMESTAMPTZ,           -- copied from the live row
  last_seen   TIMESTAMPTZ,           -- copied from the live row
  removed_at  TIMESTAMPTZ NOT NULL,  -- when Farsight applied the removal (§7.8)
  removed_rev BIGINT,                -- commit rev of the removing firehose event; else NULL
  cause       SMALLINT NOT NULL      -- 1 delete 2 subject_change 3 refused_update 4 reconcile 5 list_deleted
);

CREATE INDEX list_items_history_by_list
  ON list_items_history (owner_id, list_rkey, removed_at DESC, id DESC);
CREATE INDEX list_items_history_by_subject
  ON list_items_history (subject_id, removed_at DESC, id DESC);

CREATE TABLE history_windows (       -- intervals during which removals were recorded (§7.7, §7.8); server time
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  from_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  to_at   TIMESTAMPTZ               -- NULL while recording
);

CREATE TABLE history_rate (          -- daily history rows per admission key, all three history tables (§7.7, §7.8, §11.1)
  key     TEXT COLLATE "C" NOT NULL,
  utc_day DATE NOT NULL,
  n       BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (key, utc_day)
);

UPDATE schema_version SET version = 8;
