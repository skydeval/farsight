-- Design r17.2 (§6.2, §7.1): one persisted cursor per Jetstream instance,
-- so a failover back to an instance resumes from its own cursor (seq on
-- v2) instead of by timestamp. firehose_state keeps the current session.

CREATE TABLE firehose_cursors (      -- per-instance cursor (§6.2); GREATEST within one source_url
  source_url           TEXT PRIMARY KEY,
  protocol             SMALLINT,      -- 1 v1 2 v2
  cursor_seq           BIGINT,        -- v2, instance-local
  cursor_us            BIGINT,        -- v1 / timestamp form
  last_connected_at    TIMESTAMPTZ,
  last_applied_through TIMESTAMPTZ    -- witness time of the last event applied from this instance
);

-- Seed from the current session's state.
INSERT INTO firehose_cursors (source_url, protocol, cursor_seq, cursor_us, last_applied_through)
SELECT source_url, protocol, cursor_seq, cursor_us, applied_through
FROM firehose_state WHERE id = 1 AND source_url IS NOT NULL;

UPDATE schema_version SET version = 7;
