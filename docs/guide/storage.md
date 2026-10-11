# Storage and hardware

Farsight is a real infrastructure component. Direct blocks take up
most of the storage.

| Stage | Database size |
|---|---|
| Day one | < 150 MB |
| First full backfill sweep complete | about 95 GB |
| Growth afterwards | not measured yet |

**The sweep's figure is a measurement**, taken when a first sweep
completed in October 2026: 94.5 GB (decimal, as the dashboard and the
storage budget count) for 212 million blocks made by 3.1 million
accounts, 16.5 million list members and 12.8 million accounts in all.
A block takes about 380 bytes, the four indexes on it included. A few
accounts hold a large share: half of the accounts that block anyone
have 5 blocks or fewer, one in a hundred has over 900, and the
thousand largest hold a fifth of all blocks. 0.6.3 gave about 180 GB
here, scaled up from the first third of that sweep; those were the
network's oldest accounts, which block far more than the rest, and
the figure was twice the truth. Versions before it gave 30–56 GB,
from a guess. Growth per year has not been measured yet.

The figure includes the two indexes that sort the UI's tables of
blocks by creation time, about a third of it. They are built in the
background, and only while the storage budget has room for them.
`limits.blocks_per_author` (1,000,000) is the most one account's
blocks may take; lowering it is the one setting that makes the index
much smaller, at the price of not listing an account's blocks past
it. Removed records are extra: about 0.2 GB per million kept,
inside the same storage budget. `storage.block_history_retention` bounds them
(`"0s"` keeps them forever); `storage.block_history_enabled = false`
stops recording.

- **Recommended:** 4 vCPU, 8 GB RAM, 500 GB SSD.
- **A smaller disk:** supported. Choose one of these:
  - run firehose-only;
  - run the sweep within the size you give the wizard. That size is
    the hard ceiling, where Farsight stores nothing new, and the
    storage budget is set just under it. The sweep pauses before it is
    reached, and every refusal is reported. Leave room on the disk
    beyond it for Postgres' write-ahead log (`max_wal_size`) and for
    maintenance.

  A sweep that pauses goes on where it stopped once the budget is
  raised.
- The Postgres data lives in the `farsight-pgdata` volume. That volume
  needs the disk space.
- **The first sweep writes heavily.** While it runs, Postgres can write
  tens of megabytes a second for days. On a disk shared with other
  services this slows them down, sometimes badly. Give Farsight a disk
  of its own if you can. Otherwise lower `backfill.concurrency`, or
  turn the sweep off while the other services need the disk
  (`backfill.sweep.enabled`; it resumes where it stopped).
- **Postgres settings.** The compose file's Postgres needs no change.
  With a Postgres of your own, keep `max_locks_per_transaction` at its
  default of 64 or above, and `max_connections` at 100 or above for
  the default pools.
- **Container logs.** The compose file keeps three files of 50 MB for
  each container. The backfill writes a line for every job it
  finishes, tens of megabytes a day during a sweep.

## A host with more memory

The compose file sizes Postgres for the recommended 8 GB. On a larger
host the memory it leaves unused is not wasted: Linux keeps the
database's files in it, and `free` shows nearly all of it as
`buff/cache`. The figure to watch is `available`.

Postgres can still be told about the larger host. Put a
`compose.override.yml` beside `compose.yml`; Compose reads both. A
`command` list replaces the one in `compose.yml` whole, so it repeats
every flag, and it is worth comparing with `compose.yml` after an
upgrade. For 48 GB:

```yaml
services:
  postgres:
    command:
      - postgres
      - -c
      - shared_buffers=8GB
      - -c
      - effective_cache_size=36GB
      - -c
      - maintenance_work_mem=512MB
      - -c
      - wal_compression=on
      - -c
      - max_wal_size=16GB
      - -c
      - checkpoint_timeout=15min
      - -c
      - max_connections=200
      - -c
      - random_page_cost=1.1
      - -c
      - autovacuum_vacuum_scale_factor=0.05
      - -c
      - autovacuum_analyze_scale_factor=0.02
```

`shared_buffers` at about a sixth of the memory and
`effective_cache_size` at about three quarters are sound starting
points. During a sweep the larger `max_wal_size` turns checkpoints
forced every few minutes into one every quarter of an hour.
`max_connections` matters only if you raise `backfill.concurrency` or
`rate_limit.query_concurrency`: the two processes open up to 89
connections with the defaults, and each step of either setting adds
one. `docker compose up -d postgres` applies the file; Postgres is
away for a few seconds and both processes wait for it.
