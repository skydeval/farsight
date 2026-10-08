# Changelog

All notable changes to Farsight are recorded in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Before 1.0, a minor version may change addresses, settings or the
database schema; each entry says so where it does.

## [Unreleased]

### Added

- For contributors: the integration harnesses for stages 1 and 3 to 9
  now run every night on GitHub Actions and can be started by hand
  (`.github/workflows/harness.yml`). Each stage is a job of its own and
  keeps its log.
- For contributors: every push and pull request is also built with
  `--locked`, checked with `cargo deny` against the new `deny.toml`
  (advisories, licences, crate sources) and measured for unit test
  coverage, which the job prints in its summary.

### Changed

- For contributors: stored codes, row ids and record stamps are typed
  throughout the Rust code (`farsight-storage/src/codes.rs`, `ids.rs`),
  and failures in the web UI, the backfill jobs and the server tasks
  are error enums. Nothing an operator sees changes.
- Public home page: left open, it now brings its totals and its "Last
  updated" line up to date together, once a minute while the tab is
  visible.
- The DNS resolver (`hickory-resolver`) is at 0.26, which clears two
  advisories against 0.25; `deny.toml` no longer ignores any. Building
  from source needs Rust 1.88 or later.
- The database schema is one initial migration, and the database checks
  every stored code: a column that holds a code accepts only the codes
  its enumeration defines. Schema version 1.
- The metrics listeners (`metrics.bind`, `metrics.backfill_bind`)
  default to loopback, `127.0.0.1:9464` and `127.0.0.1:9465`. The
  compose file sets them to every interface of the container, where the
  ports are not published. Outside compose, set them, and set
  `metrics.bearer_token_sha256` with a bind another host can reach.
- The compose file gives both containers 45 seconds to stop
  (`stop_grace_period`); they take up to 30.
- New settings: `backfill.repo_job_max_duration` (`"1h"`),
  `backfill.per_domain_rps` (20), `backfill.per_domain_concurrency`
  (8) and `backfill.on_demand_reserved` (4). They are described under
  Fixed.
- The config is checked more strictly, and a file that fails does not
  load: `backfill.concurrency` is 1 to 512; no step of
  `backfill.retry_schedule`, `missing_retry` or `phase1_retry` is
  `0s`; `backfill.system_queue_cap`, the request limits and
  `list_fetch_max_attempts` are positive; `backfill.relay_url`,
  `backfill.plc_url` and `backfill.backlinks.url` are `http` or `https`
  URLs; `storage.tombstone_ttl` is at least `72h`, the time a listing
  may still be applied after it was read.
- Backfill: a change of `backfill.per_host_rps`,
  `per_host_concurrency` or `plc_rps` applies to the next request and
  no longer rebuilds the process. A change of `backfill.concurrency`
  now rebuilds its tasks, so the database pool always has a connection
  for every worker.
- The `account_purges` task runs every 10 seconds, not once a day; the
  new `account_purge_scan` and `table_pruning` tasks run daily. The
  series `farsight_ingest_storage_errors_total{op="purge_account"}` is
  gone, because the firehose writer no longer purges.
- Schema: `backfill_queue.claimed_by` and `claimed_until`,
  `backfill_state.yields` and `current_run_started_at`, the table
  `account_purges`, and indexes on `backfill_queue`, `backfill_state`,
  `list_fetch_runs`, `subject_lists`, `op_errors` and `sweep_cycles`.
  Schema version 1.
- Admin UI: some changes now ask you to **sign in again** if you
  signed in more than 10 minutes ago. They are: a settings save that
  changes `auth.*`, `net.*`, `backfill.plc_url` or
  `backfill.relay_url`, or that opens access further (`access.reads`
  towards `public`, `access.cors` or the public UI switched on);
  rotating the admin token; and creating an API key. The change is not
  made: the sign-in page opens with "Sign in again to change this
  setting", and after signing in you are back on Settings or
  Operations, where you make the change again. What was typed into
  the form is not kept. Everything else, and every change that closes
  access, works as before. The reason is that each of these changes
  would let whoever held a stolen session keep control after the
  session ended.
- Admin sign-in resolves the admin DID through the PLC directory that
  `backfill.plc_url` named when the server started. A changed
  `plc_url` still applies to handles and backfill at once, and to the
  sign-in after a restart.
- A change of the admin token signs every admin session out however it
  was made, also when `auth.admin_token_sha256` is written in the
  `config.toml` editor.
- The admin session cookie is named `__Host-farsight_admin` when the
  request arrived over HTTPS (as a trusted proxy reports it), and
  `farsight_admin` over plain HTTP.
- The `config.toml` editor refuses a save when the file has changed
  since the page was loaded (another tab, an Operations control), and
  asks you to reload. The same holds on the public UI's confirmation
  page.
- Setup: a wizard session ends when the setup token is replaced, also
  by `farsight setup-token --rotate`, and 12 hours after it was
  opened.
- API: one caller holds only part of the query slots. A quarter of
  `rate_limit.query_concurrency` is kept from anonymous callers; one
  anonymous address (IPv6: one `/48`) has at most a quarter of the
  rest in flight, one API key at most half of all. A request beyond
  that waits for one of its caller's own places and, like any request
  that finds no slot within 2 seconds, gets `503 Overloaded`. With the
  defaults that is 6 requests at a time for an anonymous address and
  16 for a key.
- Rate limits: an anonymous IPv6 caller is limited by its `/64` as
  before and now also by its `/48`, at 4 times the limit, and its
  `/32`, at 16 times. At most 100,000 rate-limit buckets are kept.
- API: a `did:web` is stored and returned with its hostname in lower
  case, and every spelling of it is the same account in a request, in
  `public_ui.excluded_dids` and in the index.
- API: a cursor that Farsight did not hand out is `InvalidRequest`;
  one holding a NUL byte was an `InternalError` before. The lexicons
  list `InternalError` among every method's errors, and
  `admin.createApiKey` counts a name's 200 characters as characters,
  not bytes.
- Public UI: a table's count is read at most once in 30 seconds for
  the same table and filters, whatever address the page is asked for
  under, and one address (IPv6: one `/48`) renders at most half of
  `public_ui.query_concurrency` pages at once; a request beyond that
  gets the "Too many requests" page.
- Public and admin pages show names without invisible characters:
  zero-width spaces, word joiners and byte-order marks are left out
  of list names, descriptions and handles, and a zero-width joiner or
  non-joiner stays only where an emoji sequence or a script needs it.
- Firehose: on a failover with a measured lag the new instance is
  asked for `firehose.tuning.gap_threshold` more than the rewind, and
  a first event later than the rewind itself records a gap. Gaps have
  a new cause, `Unreadable` (see Fixed), and
  `farsight_ingest_dropped_total` a new `reason`, `unreadable`.
- `server.hostname` and `server.contact` must be one line of text; a
  control character in either is refused when the config is loaded or
  saved.
- The compose file names the image by version
  (`ghcr.io/skydeval/farsight:0.6.0`), not `latest`. The Dockerfile
  names its base images by digest. The server logs a warning at
  start while the database is reached with the compose file's default
  password.
- For contributors: the GitHub Actions workflows name every action by
  commit, and the stage 3 harness names its `busybox` image by digest.
- Schema: `firehose_gaps.cause` accepts the new code 6. Schema
  version 1.

### Fixed

- Backfill: a host answering `429` with an enormous `Retry-After`
  crashed the job that asked and took its worker out of the pool for
  good. The header is now honoured for at most an hour, and a job that
  fails in any way gives its worker back.
- A panic in a background task no longer silently stops that part of
  Farsight. Tasks are started again and the panic is logged and counted
  in the new metric `farsight_task_panics_total{task}`. A panic in the
  firehose reader or writer makes the server exit, so that its
  supervisor starts it again and ingest resumes from the stored cursor.
- Sweep: with a relay that has no `listReposByCollection`, the sweep
  never fell back to `listRepos` as documented and retried without
  end. It now falls back, also in the middle of a cycle, and asks the
  relay again before every full cycle.
- Sweep: a cycle's "failed for good" and "done" figures could come out
  one short for an account whose job was cut off at shutdown. The
  account and the figure now change together.
- Backfill: an account held as taken down, suspended, deactivated or
  deleted was made active again, and its rows shown, whenever the relay
  failed to answer `getRepoStatus` (an error, a rate limit, a timeout).
  Only the relay's own answer that the account is active lifts the
  status now; without one the account stays hidden and the job fails
  and is retried.
- Firehose: one record nested deeper than the JSON parser allows made
  its whole frame undecodable, and ingest stood still on it on every
  instance. A frame is now read one level at a time: a record that
  cannot be parsed is a rejected commit, counted in
  `farsight_ingest_dropped_total{reason="invalid"}`, and the stream
  moves on.
- Firehose: a v2 instance that announced with `#info OutdatedCursor`
  that it resumed later than the stored `seq` lost the stretch in
  between without a gap. An announced clamp now records a gap on every
  kind of resume.
- Firehose: an instance whose `seq` numbering started again was asked
  for a `seq` it no longer had on every reconnect. A first event with a
  lower `seq` than asked for now records a gap and drops the stored
  `seq`; a first event more than `gap_threshold` after the stored
  cursor records a gap too.
- Firehose: a v1 resume that the instance clamped by less than 5
  minutes lost up to 3 minutes without a gap. A resume by timestamp on
  an instance with its own cursor now records a gap whenever the first
  event is later than the stored cursor.
- Firehose: a witness time up to 24 hours ahead of the clock was
  accepted, which could put a failover cursor in the future and make
  its gap empty. The allowance is 5 minutes, a failover is planned from
  the clock when the position is ahead of it, and a gap that has to be
  recorded is never empty.
- Firehose: a refused cursor (`CursorTooOld`) on a failover, or on a
  return to an instance while the position came from another, started
  its gap at `applied_through`. It starts 30 minutes earlier, like
  every other gap between two instances.
- Firehose: on v1, an instance that changed its compression dictionary
  was reconnected to without end. After the first frame that cannot be
  decompressed the sessions are opened uncompressed.
- Firehose: a seam repair that could not connect, broke off, went
  silent or was lost in a restart was passed over as if it had run.
  Seam windows are now stored (`firehose_seams`) from the first event
  of the resumed session and removed only when their re-read reached
  the end of the window and was applied. A re-read that fails is tried
  again; after 5 failures the window is recorded as a gap of the new
  cause `SeamUnrepaired`. A session on an instance that stays more than
  5 seconds behind the clock now gets its repair too. The new gauge
  `farsight_firehose_pending_seams` shows the windows waiting.
- Firehose: a seam repair held up to 500,000 events in memory and
  handed them over at once, and reconnects in quick succession each
  read their own overlapping window. Events are handed over in batches
  of 500 as they are read, and the due windows of one instance are read
  in one pass.
- Firehose: account events were ordered by the time an instance
  witnessed them, so after a failover an older status could replace a
  newer one, or a newer one be dropped, and an account stay hidden or
  shown wrongly. They are ordered by the upstream event's own time.
- Firehose: an update that turned a valid block, listblock or listitem
  into one Farsight rejects (a listitem moved to another account's
  list, a subject that is not a DID) left the previous version in the
  index. The rejected create or update now removes the version stored
  under its key.
- Firehose and backfill: a commit rev or a repository's latest rev from
  the future was accepted, which made the record undeletable and every
  later listing look diverged. A rev more than 5 minutes ahead of the
  clock is refused: the commit is dropped as `invalid`, and a listing
  with such a stamp fails.
- Firehose: rows of a deleted and purged account could be stored again
  by a replay of the stream and stay until the daily purge. A commit of
  a deleted account witnessed no later than its deletion stores
  nothing.
- Backfill: work under way was lost when the process was killed or
  updated. A queue entry was deleted when its job started, so the job
  was gone with the process and `getBackfillStatus` said `running`
  from then on. An entry is now claimed while its job runs and deleted
  when the job has ended; a claim that is not renewed runs out within
  10 minutes and the entry waits again. A job left marked running with
  nothing running it is queued again, and is reported as `queued`
  until then.
- Backfill: a list fetch run that was cut off by a restart, or that
  ended in a database or resolver error, stayed open for good with its
  lists claimed, and that owner's lists were never fetched or refreshed
  again. Every error now closes the run and queues the owner again, and
  a run left open by a killed process is taken up and resumed.
- Backfill: jobs kept running after a shutdown signal and after a
  config change that rebuilds the process, where the old jobs went on
  beside the new ones with the old limits. The scheduler now stops its
  jobs, gives their queue entries and leases back, and one network
  layer with one set of host limits, cooldowns and breaker state
  serves the process across rebuilds.
- Backfill: a PDS could keep a worker for days by answering every page
  slowly with a new cursor, and enough such hosts kept every worker. An
  attempt of a repository job now runs for at most
  `backfill.repo_job_max_duration` and 25,000 pages, then stops and is
  queued to go on from its cursor, with a growing wait from the third
  time. A run that stops this way more than 10 times in a row is a
  failure, retried on the failure schedule and terminal after
  `backfill.terminal_after`. A cursor that comes back, also through a
  cycle of several cursors, fails the job.
- Backfill: the per-host request limits were per host name, so an
  operator with one name per account under one domain was limited per
  account. All hosts of one registrable domain now also share
  `backfill.per_domain_rps` and `backfill.per_domain_concurrency`. The
  hosts in `limits.large_hosts` are exempt.
- Backfill: the on-demand tier's share was only an order of dispatch,
  and long-running sweep jobs could hold every worker.
  `backfill.on_demand_reserved` workers are now never used by the sweep
  or by the firehose's new authors. A requester is charged for its
  jobs' requests as they are made, not when the jobs end, so a
  requester with long jobs no longer keeps looking cheapest.
- Backfill: a resumed repository job took a new coverage point for
  pages an earlier attempt had read before it. A run keeps the coverage
  point and start of its first attempt.
- Backfill: a job that panicked was neither retried on a schedule nor
  recorded. It is now recorded as a failed job: a repository job is
  retried with backoff and becomes an `unreachable` debt if it keeps
  failing.
- Backfill: one page of `listRecords` with a record nested deeper than
  the JSON parser allows made the whole page, and so the repository,
  unreadable. Each record is parsed on its own, and one that cannot be
  parsed is dropped by itself. A page with more than the 100 records
  asked for fails the job.
- Backfill: a PDS that listed records out of order could make a job
  hold a gigabyte of keys, and one that listed more keys than
  `backfill.seen_set_cap` made the job start the collection again
  without end. Keys are remembered as 8-byte hashes, and past the cap
  the listing goes on to its end without reconcile, as documented.
- Sweep: one entry in the relay's listing that was not a valid DID kept
  its cycle open for good, and coverage at `sweep_incomplete`. Such
  entries are left out, and one that is already a member is settled.
- Backfill: the retry of a failed job set its backoff on the waiting
  entry for the account even when that entry was a newer on-demand
  request, which then waited an hour to a day. A waiting entry keeps
  the earlier time.
- Discovery: a reference that could not be checked (its author did not
  resolve, or the PDS did not answer) was skipped, and the run still
  confirmed subject coverage. Such a run is now marked `truncated`,
  like one that hit its reference cap, and confirms nothing.
- Backfill: with `backfill.plc_seed_from_export`, or a cached
  resolution, a repository read from a host the account had left could
  look diverged and have its stored rows purged. The account is now
  resolved at the directory before a divergence is acted on.
- Gap repair: the backfill process and `admin.startRepair` could each
  start a repair cycle at the same moment. Both start one under the
  same lock, and the database allows one open cycle of a kind.
- A repository that went backwards was reported as fully covered while
  its rows were being purged, and what the firehose wrote for it during
  the purge could be removed with the rest. The `resync` debt is
  recorded first, and the purge takes only what was stored before the
  divergence was found.
- Purging a deleted account with many rows paused the firehose for the
  length of the purge. The deletion now asks the server's purge task
  for it, and neither the firehose writer nor a backfill job waits.
- A purge, a re-evaluation of uncounted listblocks or a reconcile of a
  whole collection could take one advisory lock per list in a single
  transaction, tens of thousands of them, and fail with "out of shared
  memory" on the same account every time. Every transaction now takes a
  bounded number (at most 500 list locks) in one statement, and larger
  work is split over several transactions.
- Every new author on the firehose made the batch count up to a million
  queue rows while it held its locks. The tier-2 cap is read from the
  planner's estimate of the table, and the per-requester caps have an
  index.
- Per-host usage drifted: a new author's rows were counted under
  `unresolved` and their deletes taken from its host's bucket once the
  host was known, so `unresolved` only grew until the nightly rebuild,
  and at its cap every unresolved author was refused. An account's
  usage now moves with it when its host becomes known or changes.
- The nightly counter rebuild lost or doubled the changes written while
  it counted, a minute or more of them. It counts in one snapshot and
  adds what was flushed since.
- `list_fetch_runs` and `op_errors` grew without end, and
  `subject_lists` kept references to lists that no longer name the
  account. Finished runs are deleted after 7 days, logged errors after
  30 days, and a discovery run that checked every reference replaces
  the account's set of lists.
- Storage gate: under deletes-only, a stored block, listblock or
  listitem whose record had come to name another subject or list was
  kept with the old one. It is removed, like any refused update.
- Two lists of one owner re-admitted at the same moment could both take
  the last of the owner's daily re-admission budget, and a divergence
  charged the budget for a `retained` list that nothing would re-admit.
  The charge is one conditional update, and a list nothing counts on is
  purged without a charge.
- The nightly recount stopped at the first list that had been deleted
  since it was picked. It goes on.
- A deadlock in a purge or in a list transition fired by a job or a
  task ended the work with an error. It is retried, as in the write
  path.
- Backfill: a record check for a list released the lease of another job
  that was reading the same owner's repository. Each job holds leases
  under its own name; a check that finds the owner busy waits 30
  seconds.
- Backfill: the resolver's memory of DIDs that do not exist was never
  emptied, and the host limiter remembered every host it ever asked.
  Both are bounded.
- API: `query.getListMembers` answered `ready`, no members and
  `complete` for a list whose owner is deactivated, taken down,
  suspended or deleted, with the list's name and purpose. Such a list
  is now reported as `unavailable` with the reason `list_unavailable`,
  without `name` and `purpose` and with `listblockCount` 0.
- API: when Farsight could not read its coverage state from the
  database, answers kept the last one indefinitely, `complete` and
  `firehoseConnected: true` included. A coverage snapshot older than 30
  seconds now makes every answer `partial` with the new reason
  `coverage_stale`; its age is the new gauge
  `farsight_coverage_snapshot_age_seconds`.
- `/health` stayed `200` while the firehose was connected but nothing
  was being applied. It is `503` once `applied_through` is more than
  `firehose.tuning.synthetic_gap_lag` behind, and
  `farsight_firehose_lag_seconds` keeps growing in that state instead
  of holding its last value.
- Firehose: an instance that replayed a stretch too slowly to send an
  event within `stall_timeout` was dropped and resumed at the same
  point without end, and ingest stood still. Each session that stalls
  without an event now gives the next one twice as long, up to sixteen
  times the timeout.
- Firehose: after about seven disconnects in the life of a process
  every reconnect waited 30 seconds. The wait now starts again at
  half a second after a session that ran well for a minute.
- Firehose: an instance that accepted the connection and then sent
  nothing was never failed over. A session that delivers no event now
  counts as failed, and three in a row move on to the next instance.
- Firehose: an event with a time far in the future or outside the
  representable range could be stored as the stream position. Such a
  frame now ends the session and is not stored. Websocket messages are
  limited to 16 MiB.
- Ingest: one database call that could never succeed, such as a broken
  purge of a deleted account, stopped ingest or kept the server from
  starting. Such a call is now tried three times, recorded as an
  operational error with the account's DID and counted in
  `farsight_ingest_storage_errors_total{op}`, and ingest goes on. A
  daily task, `account_purges`, finishes purges that were left over.
- Backfill: a requester could take every free worker at once, because
  jobs were charged only when they ended. They are now charged when
  they start.
- Admin sign-in on `127.0.0.1` was offered to any client that sent
  that `Host` header. It is now offered only to a client whose address
  is loopback or private.
- `POST /admin/logout` did not check the form token, so another site
  could sign the admin out. It now does, like every other form.
- Outbound requests used a proxy named in `HTTPS_PROXY`, `HTTP_PROXY`
  or `ALL_PROXY`, which bypassed the check that refuses private
  addresses. They are now always made directly. An instance that
  reached the network only through such a proxy stops working.
- Outbound requests are now also refused to 6to4 (`2002::/16`), Teredo
  (`2001::/32`) and IPv4-compatible IPv6 addresses, to
  `198.18.0.0/15`, and to a few other ranges that are not public.
- Purging a list or a deleted account deleted its rows one at a time,
  several database round trips each. A batch of 10,000 rows is now a
  few statements, and so is a reconcile.
- A list of a reactivated account that had timed out at the same
  moment could be re-admitted without its lock being held.
- Public UI: a handle that had passed to another account stayed on
  the first account's rows until the handle pass looked at it, and for
  good with the pass off. A check that finds the handle now resolves
  to another account, or that the account's document names another
  handle or none, removes the handle at once, from memory and from
  `handle_cache`. A check that cannot reach the hosts still keeps it.
- Public UI: an account or list page whose own handle could not be
  checked (no budget left, or no answer in 2 seconds) was cached for
  30 seconds without its handle. Such a page is `no-store`.
- Public UI: pressing Enter in a table's filter box with a whole
  handle drew on the instance's handle budget at the rate of page
  views. The lookup is charged to the visitor's lookup class
  (`rate_limit.ui_lookup_rps`) first.
- Public UI: a list page built its image's address from whatever host
  the owner's identity named, a private address or a local name
  included, so a visitor's browser could be made to request a host on
  the visitor's own network. An image is named only on an `https`
  server with a public domain name; this holds for avatars too.
- API: `getListsNaming` read the subject's list items from the start
  for every page. A later page now starts where the last one ended.
- API: a revoked API key could work again for up to 30 seconds when a
  refresh of the key table that had started before the revocation
  finished after it. A revoked key stops at once and stays out.
- A proxy that writes `X-Forwarded-For` entries with a port
  (`203.0.113.7:4711`) made Farsight skip its entry and take the
  client's own claim as the client address. Such an entry is read; an
  entry that is not an address at all ends the walk, and the nearest
  proxy that was read stands in.
- The periodic sweep of rate-limit buckets dropped buckets that were
  not full, which gave a caller of a class that does not refill a new
  allowance. It drops only buckets that are full again.
- With `proxy.cloudflare_refresh`, a fetched range list was trusted as
  it came. A list with a range broader than `/8` (IPv4) or `/24`
  (IPv6), a private range or more than 500 ranges is refused whole
  and the previous set stays in force.
- A line break or another control character in `server.contact` or
  `server.hostname` made the server exit at every start.
- Settings and setup showed a database password in clear when it was
  given as a URL parameter (`?password=`) or in the keyword form
  (`password=…`). Both are shown redacted.
- Setup: the wizard could finish with a connection string whose test
  had failed, if the storage step had passed earlier with another
  one. A failed test, or a test of another string, takes the step
  back.
- Setup: the count of wrong setup tokens per client grew without
  bound and gave every IPv6 address a count of its own. It counts by
  `/64` and holds at most 10,000 clients.
- Setup: the Jetstream test's 10-second limit did not cover
  connecting.
- Firehose: a frame that could not be read ended the session, and one
  that was unreadable every time came first again on every resume, on
  every instance. After three sessions in a row have ended at the
  same position, the next one steps past the frames it cannot read
  there (at most 64) and records a gap of the new cause `Unreadable`,
  which a repair then covers.
- Firehose: on a failover, a new instance that started up to
  `gap_threshold` later than asked took that time out of the rewind
  without a gap.
- Firehose: the zstd dictionary was prepared again for every message.
  It is prepared once. A frame may ask for a window of at most 16
  MiB, the size a frame may expand to.
- Admin UI: the list lookup showed "Complete" coverage above the
  members of a list whose owner is hidden, where the API reports the
  list as unavailable. It shows the same coverage as the API.
- Backfill: a discovery run that found a list it could not record,
  because a cap refused the list's placeholder row, still confirmed
  subject coverage. Such a run is marked `truncated`.
- Backfill: a record on a `listRecords` page that was not valid left
  an older stored version of it in place. The stored version is
  removed by the page's reconcile, as on the firehose.
- Backfill: a job's 10-minute lease on its account was renewed only
  when it read a page, so a job that waited longer between two pages
  could lose it to a second job. The scheduler renews the leases of
  running jobs every minute.
- A purge of a list's items that met a deadlock with the firehose
  writer failed the whole purge pass. It is tried again like every
  other transaction that takes those locks.
- Backfill: the table of request counts per host, from which the
  `host` metric label's 50 busiest hosts are chosen, grew to 100,000
  hosts before it was cut. It holds at most 4,096.

## [0.6.0] - 2026-10-06

### Added

- Dashboard: an "API usage" block lists every API endpoint with the
  requests it has answered since the server started, its errors, its
  rate-limited requests and when it was last called. Endpoints nobody
  has called are set back.
- Gap repairs can be paused, cancelled and kept from starting by
  themselves. Operations has "Pause repair", "Cancel repair" and a
  switch for automatic repairs; the API has `admin.pauseRepair` and
  `admin.cancelRepair`; the settings are `backfill.repair.paused` and
  `backfill.repair.auto_start` (on by default, as before). Cancelling
  turns automatic repairs off so the repair does not start again.
- Public home page: "Top blockers" and "Most blocked", side by side,
  on two tabs: "Last 24H" and "All Time". Each lists the 20 accounts
  with the most blocks made or received: ten rows, and ten more behind
  "Show more". The lists are counted once a day, at 05:00
  EST, in the background; the page reads the stored result. Both are
  off by default and switched on separately in Settings → Public UI
  (`show_top_blockers`, `show_top_blocked`). Schema version 13.
- Admin dashboard: a "Catching up" block at the top says how far the
  history sweep, the handle pass and the reading of list descriptions
  have got and about how long each has left. A line goes away when its
  work is done.
- The admin DID lookup page has the account header of the public page
  (avatar, handle, DID, creation date and age, host) with the backfill
  state on its right, its three tables behind tabs, numbered page
  controls, a filter box on the blocks and lists tables, and a "Copy
  at:// URL" button for each block record.

- A tab for the lists an account subscribes to as block lists (its own
  listblock records). On the public account page it is "Blocking
  Lists" (`?tab=blockinglists`) and appears with `show_outgoing_blocks`,
  like "Blocking"; on the admin DID lookup it is always there and also
  lists subscriptions to lists Farsight has no record of.
- A History tab on the admin DID lookup page, at the right end of the
  tabs: the account's handle history and PDS history from the PLC
  directory, and the removed blocks and removed list entries that used
  to be on a separate page. It is read only when the tab is opened.

### Changed

- The admin pages, the setup wizard and the sign-in page now send a
  `Content-Security-Policy` like the public pages': no inline script or
  style, nothing from another origin except avatar images over https,
  no framing. Their inline style attributes moved into the stylesheet.
- Home and search pages on a narrow screen: the search box no longer
  shows the magnifier or the "/" key hint, which each took a line.
- Admin pages: every count has thousands separators (the dashboard's
  queues, exceptions, lists by state and host buckets; the list
  lookup's listblocks and stored items). In the Index block a
  figure too long to sit beside its name goes to a line of its own, at
  the right.
- The default Jetstream is Bluesky's two public v2 instances,
  `wss://jetstream.us-east.bsky.network` and, as its failover,
  `wss://jetstream.us-west.bsky.network`, in place of
  `jetstream2.us-east`, which serves v1 only. The setup wizard's note says which public instances
  offer v2. An existing config keeps the address it has.
- The public list page's tables, the list lookup's facts and
  Operations' API keys are frameless like the other tables.
- Dashboard: the Index counts have thousands separators.
- Public pages: the "taken down" tag is darker in the light theme
  (contrast 5.7:1, from 4.3:1), and the History tab's "current" tag
  stays beside its handle or host.
- Admin pages: the dashboard's warnings and its coverage sentence moved
  into an "Alerts" drop-down in the bar, next to the theme selector, on
  every admin page. A number on it counts the warnings.
- Admin pages: every remaining time that read in UTC (coverage
  sentences, firehose gaps, Operations' recent errors and API keys, the
  removed-records pages, "Last backfilled") now reads in the browser's
  timezone with its short name. Without script they still read in UTC.
- Admin dashboard: Firehose, Backfill and Storage are rows that fill
  their block, as Index is, in place of an inner table that squeezed
  values onto several lines. In all four blocks the name is at the
  left and the value at the right, and the Index figures are smaller.
- Admin list lookup: Members and Subscribers (the former "Inbound
  listblocks") sit behind tabs (`?tab=subscribers`) and turn by page
  number with counts; the record's at-uri is copied with a button
  rather than printed; dates carry no zone and say how long ago on a
  second line; the "First seen" column is gone; the tables stand in
  their block without a frame; and the list's facts include its
  description. Old cursor addresses (`mc`, `bc`) are ignored.
- Tables that stand in their block without a frame, a header band or a
  fill, with a thin line between rows: the admin DID lookup's (where
  the headers of the narrow columns are centred), the "Recent errors"
  table on Operations, and every table of the public account page.
- Admin dashboard: Exceptions and Lists by state are rows like the
  blocks above them, and the host-bucket table has no frame.
- On the History tab, public and admin, the handle history and the
  host history stand side by side, and one under the other on a narrow
  screen.
- The DID lookup page's "View history" link is replaced by the History
  tab. The account's separate history page, with its "What this page
  covers" section, still exists at its address; the tab leaves that
  section out.
- On the admin DID lookup page: dates carry no time zone (each table
  states it once) and say how long ago on a second line; the "First
  seen" column is gone; the record's at-uri is copied with a button
  rather than printed, with a "View" link beside it when a record
  viewer is configured. Old cursor addresses (`bc`, `lc`, `nc`) are
  ignored. The list lookup page is unchanged.
- The admin pages have their own script, `/static/admin.js`, and their
  own copies of the styles they share a look with: the admin and the
  public pages no longer share a script.

- The dark theme uses the Carbon palette: background `#161616`, text
  `#f4f4f4`, accent `#78a9ff`, on a flat background. The gradient rule
  on the header card is a solid 1px line, coloured glows are gone,
  hovers shift tone, and the status dot no longer pulses. The light
  theme is unchanged.
- The footer reads "An independent index of public block records.
  Farsight is not affiliated with Bluesky."

### Fixed

- The admin DID lookup no longer times out on an account that is on
  hundreds of lists. Its "Incoming listblocks" table read every
  listblock's author before cutting a page; it now cuts the page first.
  A listblock of a deactivated or deleted account is left off its page
  and still counted in the heading.
- Home page top lists: "Show fewer" sits under the extra rows, not
  between them and the first ten.
- While a repair cycle is running, the gap alert says so, with how
  many accounts it has re-read, where it used to keep telling you to
  start one. Operations says a repair is already running when asked
  again, and the dashboard's "Catching up" has a line for it (the
  "History" line no longer shows the repair's figures as the sweep's).
- The nightly rebuild of the approximate counters failed with
  "deadlock detected" on a busy instance, every time it ran: it
  rewrote every row of the host buckets while the writers were
  updating them. It now takes a brief table lock, so the host buckets'
  "Stored" figures and the totals are corrected nightly again.
- Start-up no longer waits minutes on a large index. The check for
  deleted accounts whose purge was interrupted walked every account
  through the primary key; it now reads the deleted accounts in one
  pass (13 seconds where it had not finished after six minutes, with
  11.7 million accounts).
- Admin alerts no longer tell you to start a repair for a gap that
  cannot be repaired. The interval of a v1 firehose is one open gap
  until a v2 Jetstream takes over; it is now part of the v1 alert, and
  the gap alert counts only gaps a repair cycle can heal. The
  dashboard's Firehose block shows the two counts separately.
- Turning a page of a table no longer makes the page jump. A page
  number or arrow used to load the whole page, after which the browser
  scrolled to the table; now the table is replaced where it stands and
  the window does not move, on the public pages and on the admin DID
  lookup. The address still names the page, and Back returns to the
  one before. Without the page's script the controls are the links
  they were.

### Removed

- The redirects from addresses the pages had before any release
  (`/public/…`, and the admin pages at `/lookup/…`, `/ops`, `/settings`
  and `/reset`). Those paths are unknown paths now.
- What was kept for configs and links from before any release: the
  password sign-in and its migration page (`auth.admin_password_bcrypt`,
  `rate_limit.bcrypt_concurrency`), the keys `access.ui` and
  `public_ui.show_history`, and the redirect of `?bc=…`-style cursor
  parameters. A config file with one of these keys does not load.
- The dashboard's "Oldest pending lists" block. The number of pending
  lists is still under Exceptions.
- Code that only a database from before any release could reach: the
  background read of list descriptions for lists stored without one
  (and the "List descriptions" line under "Catching up"), and the
  wording for rows without a "first seen" time.
- Unused code: cursor paging of the row tables, which all turn by page
  number, history queries no page showed, and style rules and script
  functions no page used.

## [0.5.0] - 2026-10-04

Database schema version 12.

### Added

- `public_ui.avatar_thumbnails` (off by default): avatars and list
  images load as thumbnails of a few kB from Bluesky's image service
  instead of the original upload, about 300 kB on average, from each
  account's own server.
- Farsight remembers which image an account's profile uses (new table
  `avatar_cache`, a CID, never the image) for a day, so opening a
  profile card no longer reads the profile record every time.

- The handle pass (`public_ui.handle_pass_rps`, off by default): a
  background worker that checks the handle of every account Farsight
  holds, so that a page's rows have their handles before anyone opens
  it. It keeps its own pace, waits by itself when checks fail, and
  reports its progress as metrics.
- Identity events from the firehose queue the account for a handle
  check ahead of everything else (new table `handle_due`), so a changed
  handle is picked up without anyone viewing the account.
- While the pass is on, lists stored by an earlier version have their
  description and image read in the background, not only when their
  page is opened.

- List pages show the list's description (plain text, no links, at
  most 300 characters) and its image. Farsight stores the text and
  which image it is (three new columns on `lists`); the image itself is
  fetched by the visitor's browser from the owner's server, and only
  with `show_avatars`. A list stored by an earlier version has its
  record read once, the first time its page is opened.
- The account page's header gives the account's age after its creation
  date, in its two largest units ("1 year, 11 months ago"). Profile
  cards state the age the same way.
- The home page shows three totals under the search box: blocks
  indexed, lists tracked and accounts seen.
- A guide to the tabs and the account tags, "How to read a page". It is
  collapsed under its heading on the home page, and behind a button in
  the bar on every other public page.

### Changed

- "Banned" is now "taken down" everywhere: the row tag, the switch
  ("Show taken down accounts"), the heading, and the address, which is
  `?takendown=1`. **`?banned=1` is no longer read.**
- A list page's second tab is "Subscribers" instead of "Blocked By",
  and says that mute subscriptions are private and not shown. Its
  address is `?tab=subscribers` and its page parameter `subscribers`;
  `?tab=listblockers` redirects to the list's first tab.
- Avatars are rounded squares (squircles where the browser draws them)
  instead of circles. The header avatar is larger: 88px, 64px on a
  phone.
- Every page's browser tab reads "Farsight" and shows Farsight's icon,
  whatever the page is: a tab or a task switcher no longer shows which
  account or list is open. Link previews (`og:title`) still name the
  page.
- The bar shows Farsight's icon without the name, and the home page's
  heading is "Farsight" without the hostname.
- The home page's bar has no search box and no guide button: the page
  has both. "/" focuses the page's own search box where it has one.
- The default description on the home page is shorter.
- "Last updated" moved into the footer of every page that has one,
  where the "ATProto Block Graph Index" label was.
- The workspace version is 0.5.0 (it was 0.1.0 since the first commit).
- A handle check by the pass that shows the account's document no
  longer names the stored handle, or that the handle now belongs to
  another account, removes the stored handle. An unreachable host still
  leaves it in place.

### Fixed

- The handle pass does not wait when the checks that failed are mostly
  handles under one domain: a single host that is down no longer holds
  up every other account.
- Outbound connections are closed ten seconds after their last request
  instead of ninety. Checking handles at a steady rate held one open
  connection per account checked, which reached the process's limit of
  open files about every two minutes; every outbound request then
  failed for a minute.

### Removed

- The "ATProto Block Graph Index" label above the home page's name and
  in the footer.
- The contact line on the home page. `public_ui.contact` remains a
  setting; no public page shows it.
- The line under the home page's search box that repeated the text
  inside it.
- Unused script and styles left from the copy buttons, the section
  links and the stat tiles.

## [0.4.0] - 2026-10-04

Database schema version 9.

### Added

- Verified handles are stored (`handle_cache` table) and survive a
  restart.
- `public_ui.handle_rps` (default 20): how many handle verifications a
  second the public pages may cause.
- Numbered page controls on every public table, 50 rows a page
  (`?page=N`), above and below the table, with as many page numbers as
  fit the row.
- Tabs on account and list pages (`?tab=…`): one table in view at a
  time.
- A History tab on the account page: the handles and hosts the account
  has had, read from the PLC directory when the tab is opened.
- A filter box on each table of the account page (`?find=…`): a DID, or
  part of a handle.
- A switch on each table that includes accounts their host has taken
  down; accounts their host has suspended are shown with a tag.
- The account's avatar, creation date and host in the account page's
  header.
- A phone layout for the public pages.

### Changed

- A public row appears only once Farsight has verified that the handle
  belongs to the account; rows still being verified are counted on the
  page and filled in as they settle.
- Table headings are the count itself, exact up to five million rows,
  with a description under it. "Blocked By Lists" adds up the
  subscriptions to the lists that name the account.
- Rows are more compact (28px), with the date column right-aligned and
  as narrow as its content.
- Row times carry no time zone; each table states the zone once.
- Old cursor addresses (`?bc=…` and the like) redirect to the first
  page of their table. A page number past a table's end is answered
  with its last page.
- The hero tiles and the section links are gone from account and list
  pages.

### Removed

- Every "copy" button on public pages. One-click copying of identifiers
  makes targeted harassment easier.
- "Load more" on public tables, replaced by the page controls.

### Fixed

- Pages name stylesheets and scripts with a fingerprint of the build,
  so a browser never shows a new page with an old stylesheet.
- Profile cards opened from rows near the bottom of a table are no
  longer cut off.

## [0.3.0] - 2026-10-03

### Added

- Public and admin tables are sorted by the time the block was created.
  The four indexes this needs are built by the server after start, in
  the background.
- Handle warming: handles of the accounts on a page are verified in the
  background and filled in.
- `access.admin_ui`: the admin UI has its own switch.
- Profile cards and a handle column on the admin lookup pages.
- A new visual design for the public pages, in light and dark.

### Changed

- **The public UI is served at the root** (`/did/…`, `/list/…`) and the
  admin UI under `/admin`. The old addresses under `/public` redirect.
- `access.ui` is replaced by `access.public_ui` and `access.admin_ui`.

### Fixed

- A profile card no longer opens underneath the sticky bar.
- Public pages carry no inline style, as their content security policy
  requires.

## [0.2.0] - 2026-10-02

### Added

- The public UI: who blocks an account, which listblocked lists name
  it, optionally whom it blocks, and the members and subscribers of a
  list. Off by default (`access.public_ui`), with a confirmation page
  that states what becomes public.
- A record of removed blocks, listblocks and list memberships, shown on
  the admin history pages.
- Profile cards on public pages: avatar, verified handle, DID and
  creation date. The avatar is fetched by the visitor's browser from
  the account's own server, never by Farsight.
- A bar on every public page with the search form and a light, dark or
  system theme choice.
- Links from records to a record viewer on the admin pages
  (`public_ui.record_viewer_url`).
- Admin sign-in with ATProto OAuth, and the `set-admin-did` and
  `admin-did` commands.
- Rate classes for the public pages and the profile cards.

### Changed

- **The admin signs in at `/enter` with their ATProto account.** The
  admin password is gone; the README describes the upgrade.
- Relative times on public pages are written by the page's script and
  kept current; the server renders absolute times only.

## [0.1.0] - 2026-10-01

### Added

- Core types (DID, AT-URI, TID, NSID), record parsing, configuration,
  and an outbound HTTP client that refuses private addresses.
- Storage on PostgreSQL: blocks, listblocks, lists and list items, with
  last-writer-wins apply, list tracking states, per-author and per-host
  caps, counters, a janitor and coverage accounting.
- Firehose ingest from Jetstream, with per-instance cursors and seam
  repair after a reconnect.
- The XRPC API: the stable queries (`getIncomingBlocks`,
  `getIncomingListBlocks`, `getListsNaming`, `getListMembers`,
  `checkBlocks`, `getBackfillStatus`, `getStats`), `requestBackfill`,
  API keys, rate limits and admin procedures, with their lexicons.
- The setup wizard and the admin pages: dashboard, lookup and settings.
- The backfill service: the repository sweep, list fetches, on-demand
  backfill and repair.
- A Docker image and a compose file that runs the server, the backfill
  service and PostgreSQL.

[Unreleased]: https://github.com/skydeval/farsight/compare/08be68d...HEAD
[0.6.0]: https://github.com/skydeval/farsight/compare/dd11be4...08be68d
[0.5.0]: https://github.com/skydeval/farsight/compare/380a503...dd11be4
[0.4.0]: https://github.com/skydeval/farsight/compare/d69003d...380a503
[0.3.0]: https://github.com/skydeval/farsight/compare/11408c3...d69003d
[0.2.0]: https://github.com/skydeval/farsight/compare/0207b49...11408c3
[0.1.0]: https://github.com/skydeval/farsight/compare/8f63563...0207b49
