-- Lists the configured AppView no longer serves although their record is
-- present: taken down by the AppView's moderation
-- (`backfill.list_takedowns`, docs/design/list-indexing.md).
ALTER TABLE lists
  ADD COLUMN appview_absent_since TIMESTAMPTZ,  -- set: the AppView no longer serves the list
  ADD COLUMN appview_checked_at   TIMESTAMPTZ,  -- the last conclusive answer (the first miss while misses = 1)
  ADD COLUMN appview_misses       SMALLINT NOT NULL DEFAULT 0,
  ADD COLUMN appview_due_at       TIMESTAMPTZ;  -- when the list is checked next; NULL: not yet checked

-- The lists the checker looks at, next due first; lists never checked
-- before every other.
CREATE INDEX lists_appview_due ON lists (appview_due_at NULLS FIRST, id)
  WHERE listblock_count > 0 AND record_state = 1;

-- The lists marked taken down: counted and cleared through this.
CREATE INDEX lists_appview_absent ON lists (id) WHERE appview_absent_since IS NOT NULL;

-- The checker's last pass, for the admin dashboard. One row.
CREATE TABLE list_takedown_checks (
  id            SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  last_pass_at  TIMESTAMPTZ,
  last_checks   INT NOT NULL DEFAULT 0,
  last_errors   INT NOT NULL DEFAULT 0,
  last_error    TEXT,
  last_error_at TIMESTAMPTZ
);

UPDATE schema_version SET version = 3;
