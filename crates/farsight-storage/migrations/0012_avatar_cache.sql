-- Group 12: which image an account's profile uses as its avatar (§8.6):
-- the CID of the blob, never the image. Written when a profile card
-- reads the account's profile record; an empty CID records that the
-- profile has no avatar. A row is used for a day, then read again.
-- Emptying the table is safe: cards read the records again.

CREATE TABLE avatar_cache (
  did        TEXT COLLATE "C" PRIMARY KEY,
  cid        TEXT NOT NULL,
  checked_at TIMESTAMPTZ NOT NULL
);

UPDATE schema_version SET version = 12;
