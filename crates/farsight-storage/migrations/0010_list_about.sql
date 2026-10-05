-- Group 10: what a list says about itself, for its page (§8.6). The
-- description is the record's text, truncated; the image is the CID of
-- the record's avatar blob (the image itself is never stored or fetched
-- by the server). `about_read` is false on rows written before this
-- migration: their record has not been read for these two fields yet,
-- and the list page reads it once, on first view.

ALTER TABLE lists
  ADD COLUMN description TEXT,
  ADD COLUMN avatar_cid  TEXT,
  ADD COLUMN about_read  BOOLEAN NOT NULL DEFAULT false;

UPDATE schema_version SET version = 10;
