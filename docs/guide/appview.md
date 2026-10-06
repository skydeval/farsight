# AppView integration

```toml
[services.farsight]
url = "https://farsight.example"
token = "fsk_…"   # API key with read + backfill scopes
```

1. **When a member enrolls,** call `requestBackfill`, then poll
   `getBackfillStatus`.
2. **When hydrating a page,** call `checkBlocks` for the viewer
   against the authors on that page.
3. **Check coverage on every response.** Its `level` is `complete`,
   `assisted` or `partial`.
   - Treat any DID listed in `partialFor` as partial.
   - Treat any level value you don't recognise as partial.

`requestBackfill` indexes the account's own repo. Knowing who blocks
that account requires either a completed network sweep, or the
optional backlink-assisted discovery. The freshness watermark always
says which of the two applies.
