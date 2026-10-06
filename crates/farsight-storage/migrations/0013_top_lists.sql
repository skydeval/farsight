-- Group 13: the home page's top lists (§8.6): the accounts that block
-- the most and that are blocked the most, for the last 24 hours and for
-- all time.
--
-- `block_recent` is a log of blocks as they are stored, kept for a day:
-- a row is written when a stored block's own date is within the day
-- before it arrived, so that history read by the backfill does not
-- count as recent. The "last 24 hours" lists count its rows whose block
-- is still stored. `top_lists` holds the computed lists, one row per
-- list, as JSON text; the home page reads it and never runs the
-- queries. Emptying either table is safe: the lists are computed again.

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

UPDATE schema_version SET version = 13;
