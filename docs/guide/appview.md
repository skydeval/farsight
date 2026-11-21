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
4. **A relation names at most 100 lists.** When `blocksActor` or
   `blockedByActor` carries `"listsTruncated": true`, more lists hold
   the block than `lists` shows. The block is there either way.
5. **Lists an AppView has taken down.** If the instance runs the
   taken-down list checks (`[backfill.list_takedowns]`), a relation can
   carry `takenDown`: the lists among `lists` that the AppView it asks
   no longer serves. They still count as blocks in Farsight's answer.
   To block as that AppView does, treat the relation as holding only if
   `direct` is true or a list in `lists` is not in `takenDown`. See
   [api.md](../design/api.md#lists-an-appview-has-taken-down).

`requestBackfill` indexes the account's own repo. Knowing who blocks
that account requires either a completed network sweep, or the
optional backlink-assisted discovery. The freshness watermark always
says which of the two applies.
