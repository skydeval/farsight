# Storage and hardware

Farsight is a real infrastructure component. Direct blocks take up
most of the storage.

| Stage | Approximate size, including overhead |
|---|---|
| Day one | < 150 MB |
| 30 days, firehose only | 2.5–10 GB |
| First full backfill sweep complete | 30–56 GB |
| Growth afterwards | 18–20 GB per year |

On top of these come the four indexes that sort the UI's tables by
creation time: 9–20 GB for a complete index, roughly a third more. They
are built in the background, and only while the storage budget has room
for them. Removed records are extra: about 0.2 GB per million kept,
inside the same storage budget. `storage.block_history_retention` bounds them
(`"0s"` keeps them forever); `storage.block_history_enabled = false`
stops recording.

- **Recommended:** 4 vCPU, 8 GB RAM, 500 GB SSD.
- **Starter (~100 GB disk):** supported. Choose one of these:
  - run firehose-only;
  - run the sweep with the storage budget the wizard sets (70% of the
    disk you enter). The sweep pauses before the disk fills, and every
    refusal is reported.

  Expect from under one to about two years of runway on a 100 GB
  disk.
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
