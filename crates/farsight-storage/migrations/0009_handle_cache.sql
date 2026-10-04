-- Group 9: the handles the UI has verified, kept across restarts (§8.6).
-- One row per DID, the current handle only. A row is written when a
-- verification succeeds and never by a failure; emptying the table is
-- safe (the pages show DIDs until the handles are verified again).

CREATE TABLE handle_cache (
  did         TEXT COLLATE "C" PRIMARY KEY,
  handle      TEXT NOT NULL,
  resolved_at TIMESTAMPTZ NOT NULL   -- when the handle was last verified in both directions
);

UPDATE schema_version SET version = 9;
