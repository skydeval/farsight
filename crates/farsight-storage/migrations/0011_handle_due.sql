-- Group 11: accounts whose handle is to be checked ahead of the handle
-- pass's walk over `actors` (§8.6). The firehose writer adds an account
-- here when an identity event for it arrives; the pass takes the oldest
-- first and removes a row once the check gave an answer. Emptying the
-- table is safe: the walk reaches every account anyway.

CREATE TABLE handle_due (
  did      TEXT COLLATE "C" PRIMARY KEY,
  asked_at TIMESTAMPTZ NOT NULL DEFAULT now()   -- not served before this
);
CREATE INDEX handle_due_asked ON handle_due (asked_at);

UPDATE schema_version SET version = 11;
