# History

The record tables hold live state: when a block, a listblock or a list
membership is removed, its row is gone. So that an administrator can
still see who blocked an account earlier, or who used to be on a list,
Farsight keeps a record of the rows it stored and later removed. This
page says what is recorded and when, how long it is kept, and what the
record can and cannot show.

History is **display data**. It takes no part in the last-writer-wins
rule, in list tracking or in coverage, nothing in the write path reads
it, and it is shown only on the admin pages.

## What is stored

Four tables; their columns and indexes are listed in
[storage](storage.md#history).

| Table | One row per |
|---|---|
| `blocks_history` | removed `blocks` row |
| `list_blocks_history` | removed `list_blocks` row |
| `list_items_history` | removed `list_items` row |
| `history_windows` | interval during which removals were recorded |

A history row carries what the live row held, plus how it ended:

| Column | Meaning |
|---|---|
| `author_id` / `owner_id`, `rkey` | The removed record's repository and key. |
| target | `subject_id` for a block; `list_owner_id` and `list_rkey` for a listblock; `list_rkey` and `subject_id` for a list membership. A list is stored by its owner and key, not by `lists.id`, because a `lists` row can be deleted. |
| `created_at` | The record's `createdAt`, as its author stated it. |
| `first_seen`, `last_seen` | Copied from the live row; see below. |
| `removed_at` | When Farsight applied the removal. |
| `removed_rev` | The rev of the commit that removed the record, when a firehose event did; otherwise NULL. |
| `cause` | Why the row was removed; see [causes](#causes). |

### Witness bounds on live rows

`blocks`, `list_blocks` and `list_items` each carry `first_seen` and
`last_seen`. Let `w` be the witness time of the write being applied:
the event's witness time for a firehose event; for a listing or a
discovery write, the witness clock at the start of the transaction, or
the database's `now()` while that clock is not yet defined. (The
witness clock is described in
[coverage](coverage.md#the-witness-clock).) A row's
*target* is the block's subject, the listblock's list, or the
listitem's (list, subject) pair.

- Insert: `first_seen = last_seen = w`.
- An upsert that wins last-writer-wins and is applied, with the same
  target: `last_seen = GREATEST(last_seen, w)`; `first_seen` is not
  touched.
- An upsert that wins and is applied, with a different target: as an
  insert. Both bounds start again.
- An upsert that loses (an equal or lower stamp: a listing of an
  unchanged repository, a replay), or that is refused with the row
  kept, writes nothing and so does not advance `last_seen`.

The bounds ride on statements the write path issues anyway and cost no
extra round trip. They are stamped whether or not history is enabled.
`last_seen` is therefore a lower bound on when the record was last
known to exist, not the time of the last listing that showed it.

Neither column is `list_blocks.witnessed_at`, which alone is used by
coverage.

## When a row is written

A history row is written when, and only when, a live row is removed by
one of the paths below. It is written in the transaction that removes
the live row, under the same author lock, and
`removed_at = GREATEST(w, last_seen)` with `w` as above for the
removing write.

### Causes

| `cause` | Name | Path | `removed_rev` |
|---|---|---|---|
| 1 | `delete` | A firehose delete that removed the row. | The delete's rev. |
| 2 | `subject_change` | A winning upsert that names a different target; the new version is stored in the row. | The update's rev if it came from the firehose, else NULL. |
| 3 | `refused_update` | A winning upsert whose new version is refused by a cap or a gate, so that the row is deleted. | The update's rev if it came from the firehose, else NULL. |
| 4 | `reconcile` | A range or whole-collection reconcile during a listing: the row was not in the repository any more. | NULL. |
| 5 | `list_deleted` | List memberships only: the list's record was deleted and its items are being drained. | NULL. |

Notes on the table:

- A firehose delete that finds no row, or a row with a rev at or above
  its own, removes nothing and records nothing.
- For a refused update `removed_rev` is the update's rev `E`, not the
  `E − 1` of the refusal tombstone. The tombstone's rev is a device of
  the last-writer-wins rule ([storage](storage.md#tombstones)); history
  records the event that superseded the row.
- A listing knows only its stamp, which is neither the rev of the
  removing commit nor a bound on it, so a reconcile records NULL.
- For listblocks, counted and uncounted rows are recorded alike.
- A later record for the same author and target creates a new live row
  as usual; the earlier history row stays, and a later removal adds
  another. A listing or an event that re-affirms a live row never
  writes history.

Two paths remove rows **without** writing history:

- the **purge of a deleted account** ([storage](storage.md#account-status)),
  which also deletes the history rows the account authored;
- the **divergence purge**, which discards everything stored from a
  repository whose history went backwards before it is listed afresh
  ([backfill](backfill.md#divergence-check)).

No other path removes these rows.

### The extra condition for list memberships

`list_items` holds members only of lists that are tracked, and a list's
items are dropped when it stops being tracked
([list indexing](list-indexing.md)). That is a change of *tracking*,
not of *membership*, and must not be recorded as one. A removed
`list_items` row of list L, owned by O, is therefore recorded only if,
at that moment,

1. O's status is not `deleted`, **and**
2. L is tracked (`track_state` is pending, ready, retained or
   unavailable), **or** L's `record_state` is deleted.

`purging` is the only untracked state in which a list still has items,
so condition 2 fails exactly while a list is being drained for a reason
other than its own deletion: its last counted listblock went away, its
owner's repository diverged, or its admission was abandoned. Neither
the drain, nor a firehose delete, nor a refused update, nor a reconcile
records anything for such a list.

When a list's record is deleted, the drain records its members with
cause `list_deleted`, so a deleted list keeps the record of the members
it had. Its `lists` row is kept as well. The one exception is the purge
of the owner's account.

Both values in condition 2 are columns of the `lists` row that the
delete path already updates for `item_count`; condition 1 is read with
the author's row. The check adds no query.

## Recording windows

`history_windows` holds the intervals during which the server recorded
history: `from_at`, and `to_at`, which is NULL while recording. At
start, and only then, the server opens a row if history is enabled and
none is open, and closes the open row if history is disabled.

The windows tell a reader which silence means "nothing was removed" and
which means "nothing was being recorded".

## Configuration

```toml
[storage]
block_history_enabled = true
block_history_retention = "365d"   # "0s" keeps removals forever

[limits]
history_per_did_per_day = 10000
history_per_bucket_per_day = 200000
```

The two `[storage]` keys govern all three history tables, list
memberships included.

### `storage.block_history_enabled`

With `false`, no history rows are written. Witness bounds are still
stamped. Rows recorded earlier are kept, are still pruned by retention,
are still deleted by an account purge, and are still readable on the
admin pages.

For the server the key takes effect at a restart: the firehose writer
reads it at start, when the recording window is opened or closed.
`farsight-backfill` takes it on its normal configuration reload.
Between an edit of the key and the server's restart the two processes
can therefore disagree, and removals found by a reconcile can be
missing inside a window, or present outside one, for that long.

### `storage.block_history_retention`

A daily task in the server walks each history table in `id` order from
the lowest, in batches of 10,000 rows, deletes the rows of the batch
with `removed_at` older than the retention, and stops at the first
batch that holds no expired row. (`id` order is removal order to within
the margin of a replay; a straggler is caught the next day.) It also
deletes `history_windows` rows that closed before the same horizon.
With `"0s"` the task does not run and nothing is deleted.

A page never shows a row older than the horizon, whether or not the
task has reached it yet.

To discard all history, run

```sql
TRUNCATE blocks_history, list_blocks_history, list_items_history,
         history_windows;
```

and restart the server, which opens a new window. The tables are never
dropped.

### Per-day limits

Each history row is charged, in the removing transaction, to the
*admission key* of the removed row's author (for a list membership, the
list's owner) in `history_rate (key, utc_day, n)`. The admission key is
the one list admission uses: the cap bucket of the author's PDS domain,
or the author's own DID on a large host and while a `did:plc` author's
PDS is not yet resolved
([security](security.md#admission-keys-and-daily-rates)).

| Key kind | Limit per UTC day | Config key |
|---|---|---|
| a DID | 10,000 | `limits.history_per_did_per_day` |
| a bucket | 200,000 | `limits.history_per_bucket_per_day` |

One daily budget per key covers all three tables. Over the limit the
removal is applied as always and the history row is **not** written. No
debt is raised, because history is outside the coverage contract; the
skip is counted. The limits are chosen, not derived: an account that
removes more than 10,000 records in a day loses the excess from
history, and the counter shows it.

A removal of many rows at once (a reconcile, a drain batch) is
charged once, for as many rows as the key's budget still allows; of
its rows, in record-key order, that many are recorded and the rest
are counted as skipped.

A drain deletes a list's items in batches of 10,000, each read
through the list's index (by member, then record key), so of a
deleted list larger than the owner's remaining budget the members of
the first batches are recorded and the rest are counted as skipped.

Every recorded membership removal is the owner's own act (a delete, an
edit, the deletion of the list) or is found in the owner's repository
(a reconcile). A third party who listblocks a list and removes the
listblock again causes drains that record nothing, and so cannot spend
an owner's budget.

### Metrics

| Metric | Labels | Counts |
|---|---|---|
| `farsight_block_history_written_total` | `table`, `cause` | history rows written |
| `farsight_block_history_skipped_total` | `table`, `reason="rate"` | removals not recorded because the daily limit was spent |
| `farsight_block_history_pruned_total` | `table` | rows deleted by retention |

`table` is the live table's name: `blocks`, `list_blocks` or
`list_items`. `cause` is a name from the table of causes. The full list
of metrics is in [operations](operations.md#metrics).

## Where history is shown

Only on the admin pages, which need an admin session:

- `/admin/did/{did}/history`, and the History tab of the admin DID
  lookup: the removed blocks that named the account (by
  `blocks_history.subject_id`) and the removed list memberships that
  named it (by `list_items_history.subject_id`);
- `/admin/list/{did}/{rkey}/history`: the removed listblocks of the
  list and its removed members.

**No API endpoint reads the history tables or returns `first_seen` or
`last_seen`, and no public page shows history or links to it.** Turning
the public UI on does not change this. Because
`storage.block_history_enabled = false` stops only the writing, an
operator who wants history unreadable as well denies the two history
routes at the reverse proxy, or discards the rows as above. The pages
are described in [the admin UI guide](../guide/admin-ui.md) and in
[web UI](web-ui.md#the-admin-ui).

Rules that bind every surface showing history:

- **Hidden accounts.** A row whose author is in a hidden status
  (deactivated, taken down, suspended, deleted) is not shown; for a
  listblock or a membership, neither is a row whose list owner is
  hidden, and the history page of a list with a hidden owner shows
  nothing. An admin session does not lift this. It is this filter, not
  the account purge, that guarantees a deleted account's history is
  never shown.
- **Order and paging.** Pages order by (`removed_at`, `id`) descending,
  and the cursor carries both. One reconcile or one drain batch can
  give thousands of rows the same `removed_at`; `id` tells them apart,
  and the history indexes end in `id` so that a page is an index range
  rather than a sort of the group. Cursors are opaque and unstable.
- **A row is a removed record, not a statement about the present.**
  Duplicate records exist, and a subject can be removed and added
  again. A row whose pair has a live row now is marked: "blocks this
  account again", "blocks this list again", "on this list now".
- **The removed record is shown as text.** It no longer exists, so a
  link to a record viewer would open "not found".
- **Lists may be gone.** The `lists` row is joined with an outer join:
  it can be missing (a placeholder that was cleaned up) and a deleted
  list has no name or purpose. The page then shows the list's at-uri,
  built from the owner's DID and `list_rkey`, and the list's state.
- **Limits are stated.** Each page prints the recording windows,
  clipped to the retention horizon (a window that closed before it is
  dropped, one that straddles it starts at it), the retention in force,
  and whether recording is on. It prints no coverage.

### Wording of causes

| `cause` | Block | Listblock | List membership |
|---|---|---|---|
| `delete` | Block deleted. | Listblock deleted. | Removed from the list. |
| `subject_change` | Record changed to block a different account. | Record changed to block a different list. | List entry changed to name another account or list. |
| `refused_update` | Record changed; the new version was not stored. | Record changed; the new version was not stored. | List entry changed; the new version was not stored. |
| `reconcile` | Found missing when the author's records were re-read. Removed some time between 'last seen' and this time. | the same | Found missing when the owner's records were re-read. Removed some time between 'last seen' and this time. |
| `list_deleted` | | | The list was deleted. |

Any other code is shown as "Removed."

## What history can and cannot show

History is a record of what this instance stored and then removed. It
can show that a block, a listblock or a membership existed at least
from `first_seen` to `last_seen`, that it was gone by `removed_at`, and
how Farsight learned of it. It makes no claim beyond that:

- **A record that began and ended before Farsight stored it is
  absent.** This is the dominant limit.
- A removal Farsight missed (a gap in the stream, an outage) appears
  only when a later listing reconciles the row, with `removed_at` the
  time of that reconcile and no rev. The true removal lies between
  `last_seen` and `removed_at`. While a reconcile is skipped (a
  collection too large for the job's set of seen keys;
  [backfill](backfill.md#steps-of-a-repo-job)), the stale row stays
  live and nothing is recorded yet.
- `removed_at`, and the bounds of rows stored by a listing, are on the
  witness clock, which stands still while the firehose is disconnected:
  a removal found during an outage is dated at its start.
- Removals outside a recording window, or over the daily limit, are not
  recorded.
- Removals by an account purge are not recorded, and the account's
  earlier history is deleted. A divergence purge records nothing: rows
  that the fresh listing creates again start their bounds again, and
  records that did not survive the divergence leave no trace.
- Listblock history and membership history are two records and are not
  joined. History can say that A stopped listblocking L, and that X was
  removed from L; it does not say that A's listblock covered X at a
  given time.

For list memberships in addition:

- Only tracked lists have items. A membership on a list nobody
  listblocked is never stored and never recorded.
- When a list stops being tracked its items are dropped without a
  record, and a member removed while it is untracked leaves no trace:
  after the list is admitted again that member is simply absent.
- `first_seen` is when Farsight stored the row. A list admitted again
  is fetched again, so the bound starts again: it means "seen on this
  list since", not "member since".
- A deleted list's members are recorded only up to the owner's daily
  limit. If the record is created again while the drain runs, the rest
  of that drain is not recorded; if a list is deleted while it is
  already being drained for another reason, only the batches after the
  deletion are.
- A capped list ([list indexing](list-indexing.md#list-size-caps))
  stores only part of its members; the others were never stored.

## Storage cost

A history row costs about 210 bytes with its indexes for a block, 225
for a listblock and 255 for a list membership: roughly 0.2 GB per
million rows. An upper bound for planning is one removal a second,
which is about 31 million rows, 6.6–7.9 GB, per year of retention;
deletes arrive well below that. Rows from edits, reconciles and drains
come on top and are bounded by the per-day limits. The two witness
columns add 16 bytes to each live row.

History counts against the storage budget like every other table
([storage](storage.md#the-storage-budget)).
