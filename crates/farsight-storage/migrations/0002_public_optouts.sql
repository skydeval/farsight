-- Accounts that withheld their own public pages (docs/design/web-ui.md).
-- The DID only, canonical form; nothing else is kept.
CREATE TABLE public_optouts (
  did TEXT COLLATE "C" PRIMARY KEY
);

UPDATE schema_version SET version = 2;
