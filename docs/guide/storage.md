# Storage and hardware

Farsight is a real infrastructure component. Direct blocks take up
most of the storage.

| Stage | Database size |
|---|---|
| Day one | < 150 MB |
| First full backfill sweep complete | about 180 GB |
| Growth afterwards | not measured yet |

**The sweep's figure is a measurement, taken in October 2026** on a
sweep 37% done and scaled to the whole: 67 GB for 158 million blocks
of about 1.35 million accounts, out of 3.7 million accounts that hold
a block or a list. That is roughly 430 million blocks at about 360
bytes each, the four indexes on them included. A few accounts hold
most of them: the median account that blocks anyone has 7 blocks, and
the thousand largest held a quarter of all that was stored. Earlier
versions of this guide gave 30–56 GB, from a guess at the number of
blocks that was about a third of what is there. Growth per year was
derived from the same guess and is left out until it has been
measured.

The figure includes the four indexes that sort the UI's tables by
creation time, about a third of it. They are built in the
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
