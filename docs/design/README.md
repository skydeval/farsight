# Farsight design

Farsight is a standalone, self-hostable block-graph index for ATProto.
It answers, per DID, "who blocks this account?" - directly and through
moderation lists - for any AppView that asks over HTTP. These pages
describe how it is built: what it indexes, the processes and the
database behind it, and the rules each part follows. This page is the
overview and the index of the others.

Version 0.6.2; database schema version 1; NSID prefix
`app.nearhorizon.farsight`.

## What Farsight is

An AppView has to enforce blocks, but the blocks that concern one of
its users live in *other* accounts' repositories, most of which the
AppView never indexes. Farsight indexes four record collections for
the whole network and serves the inbound view.

| Collection | What is stored |
|---|---|
| `app.bsky.graph.block` | Every record whose `subject` is a syntactically valid DID. |
| `app.bsky.graph.listblock` | Every record whose `subject` is a valid `at://<did>/app.bsky.graph.list/<rkey>` URI. |
| `app.bsky.graph.list` | Every record: purpose, `name` truncated to 128 characters, `description` truncated to 300 characters, and the CID of the list image (never the image). |
| `app.bsky.graph.listitem` | Only for **tracked lists**, and only when the listitem's author is the list URI's authority. |

The **authority rule**: list membership is defined by listitem records
in the list owner's repository. A listitem in repository A that names
list `at://B/...` means nothing and is dropped (counted in
`farsight_ingest_dropped_total{reason="foreign_listitem"}`).

Every read response carries a `freshness` object that states how
current the answer is and whether Farsight can claim it is complete.
That claim is the centre of the design; see [coverage.md](coverage.md).

## What Farsight is not

- Not a general firehose index. No collection beyond the four.
- No feed generation, timelines or hydrated `app.bsky.*` views.
- No outgoing-block *listing* in the API. An AppView reads its own
  users' repositories. Outgoing data is stored all the same, because
  the incoming blocks of one account are the outgoing blocks of
  another; `checkBlocks` reports both directions for a pair because
  that costs nothing extra.
- No private or member-only data. Mute lists' subscribers, for one, are
  not public records and are not indexed.
- No user-account system. The API authenticates with bearer tokens;
  the admin UI admits one ATProto account, which signs in with OAuth.
- No block *policy*. Farsight reports a list's `purpose`; the consumer
  decides, for example, whether a listblock on a curation list is
  honored.

## Who it is for

AppView operators who need inbound block state without depending on a
third-party index, and operators who want to run a public instance
with a web interface for looking accounts and lists up.

## Terms

| Term | Meaning |
|---|---|
| **author** | The DID whose repository contains a record. |
| **subject** | The DID a `block` or `listitem` points at. |
| **list owner** | The DID whose repository contains an `app.bsky.graph.list` record. |
| **tracked list** | A list whose members Farsight stores: one in state `pending`, `ready`, `retained` or `unavailable`. A list becomes tracked when it gains its first counted `listblock`. Only tracked lists accept listitem writes. See [list-indexing.md](list-indexing.md). |
| **rev** | The repository commit revision (a TID) that produced a record's state. TIDs sort by time; stored decoded as `BIGINT`. |
| **stamp** | The rev Farsight attaches to a written row. For a firehose row it is the event's commit rev; for a backfilled row it is the repository's latest rev, read *before* the listing began. Two writes of one record are ordered by their stamps (last writer wins). |
| **witness time** | The time Jetstream attached to an event. The firehose watermark and every coverage statement are on this clock. |
| **author lock** | A transaction-scoped Postgres advisory lock keyed on an author DID, held by every write to rows that DID authors. |
| **list lock** | A transaction-scoped advisory lock keyed on a list URI: exclusive for tracking-state changes, shared for listitem writes. |
| **sweep** | The systematic backfill, which reads every repository that may hold one of the four collections. A **sweep cycle** is one complete run through its enumeration source. |
| **gap** | An interval of witness time in which firehose events may have been missed. A **repair** re-reads the accounts whose repositories changed during a gap. |
| **coverage** | What Farsight can claim about the completeness of an answer: `complete`, `assisted` or `partial`, with reasons and counted exceptions. |
| **hidden status** | An account status of `deactivated`, `takendown`, `suspended` or `deleted`. Rows authored by such accounts are left out of API answers unless the caller asks for them. |

## Architecture

```
                 +----------------- farsight (server) -------------------+
 Jetstream --ws->| ingest -> single writer --+        +-- XRPC API  <----|-- AppViews,
 (4 collections, |   gap and lag tracking    |        |   /xrpc/...      |   scripts
  identity and   |                           v        |                  |
  account events,|                     +----------+   +-- web UI    <----|-- browsers
  #sync on v2)   |  periodic tasks --> | Postgres |-->|   public, admin, |
                 |                     +----------+   |   setup wizard   |
                 +--------------------------^----------------------------+
                                            |  queue, state,
                                            |  LISTEN/NOTIFY
                 +------------ farsight-backfill ------------------------+
 relay or PLC -->| sweep producer -> scheduler -> workers -> same        |
 (enumeration)   |   tiers, per-requester fairness,          apply path  |
 PDS hosts <-----|   per-host rate limits                                |
 (listRecords)   |                                                       |
 backlink index  | subject discovery (optional)                          |
 (optional) ---->|                                                       |
                 +-------------------------------------------------------+
```

There are two processes and one database.

**`farsight`, the server.** It holds the Jetstream connection and
applies its events through a single writer; serves the XRPC API under
`/xrpc/`, the web UI, `/health` and `/livez`; runs the migrations; and
runs the periodic maintenance tasks. On first start, with no
configuration, it serves a setup wizard instead and switches to normal
operation in-process when the wizard finishes.

**`farsight-backfill`, the backfill process.** It enumerates
repositories from a relay (or from the PLC directory), reads the four
collections from each account's PDS with `listRecords`, fetches the
members of lists that become tracked, re-reads accounts after a
firehose gap, and serves on-demand requests made through the API. It
has no public listener.

**Postgres.** The index, the backfill queue, the firehose cursor, the
coverage state and the admin sessions all live in one database.

Three rules hold the parts together:

- **Postgres is the only coordination point.** Every correctness
  decision - which of two writes wins, whether a list is tracked - is
  made inside a Postgres transaction under the author and list locks,
  never on in-memory state. The two processes talk to each other only
  through tables and `LISTEN`/`NOTIFY` (channels `farsight_config` and
  `farsight_coverage`).
- **One apply path.** Both processes write through the same library
  code (`farsight-storage::apply`), so the firehose and the backfill
  cannot disagree on what is admitted.
- **Only the server runs migrations.** `farsight-backfill` does no
  work until `schema_version` equals the version it was built for; it
  polls every 5 seconds.

### Data flow

1. *Live.* The server subscribes to Jetstream for the four collections
   plus identity and account events, preferring the v2 endpoint, whose
   `#sync` events and cursor errors make honest coverage possible. The
   writer applies events in batches, advances a watermark, and records
   a gap whenever events may have been missed.
   See [firehose.md](firehose.md).
2. *History.* The backfill process sweeps the network once, repository
   by repository, at a polite per-host rate. Until the first sweep
   cycle completes, answers about incoming blocks are `partial`.
   See [backfill.md](backfill.md).
3. *Lists.* The first counted listblock on a list makes it tracked; a
   list job then fetches its members from the owner's repository. A
   list is served only once it has been fetched completely.
   See [list-indexing.md](list-indexing.md).
4. *Out.* Read queries run against the index and attach `freshness`
   from a coverage snapshot that is at most 10 seconds old. Responses
   do not depend on the caller, so an edge cache may hold them. The web
   UI renders the same data as HTML. See [api.md](api.md),
   [coverage.md](coverage.md) and [web-ui.md](web-ui.md).

### Crates

| Crate | Kind | Responsibility |
|---|---|---|
| `farsight-core` | lib | DID, AT-URI, TID and NSID types; record parsing and validation; the configuration schema and its loading (TOML and environment); the safe outbound HTTP client. |
| `farsight-storage` | lib | Migrations; `apply` (locks, last-writer-wins, list tracking); queries; coverage computation; the janitor and recount routines the periodic tasks call. |
| `farsight-ingest` | lib | Jetstream client (v2 and v1), cursor persistence, gap and lag tracking, the batching writer. |
| `farsight-backfill` | lib + bin | Sweep sources, scheduler, per-host limiter, repository lister, list jobs, backlink discovery. Binary `farsight-backfill`. |
| `farsight-api` | lib | XRPC handlers, lexicons, authentication, rate limiting, client-IP resolution, cache headers, the live configuration store. |
| `farsight-web` | lib | The server-rendered web UI (askama templates, vendored htmx): public pages, admin pages, setup wizard, OAuth sign-in and admin sessions, handle verification. |
| `farsight-server` | bin | Composition root: setup or normal mode, listeners, the ingest task, the periodic tasks. Binary `farsight`. |

The stack is tokio, axum, sqlx, tokio-tungstenite, reqwest with
rustls, serde, toml, askama, `metrics` with the Prometheus exporter,
and tracing.

## Deployment shape

Farsight ships as one container image with both binaries and a
`compose.yml` with three services.

| Service | Container | Command | Volumes | Ports |
|---|---|---|---|---|
| `farsight` | `farsight` | `farsight` | `farsight-config` at `/etc/farsight` | `8080` published (API and web UI); `9464` metrics, inside the compose network only (the compose file sets `metrics.bind`) |
| `farsight-backfill` | `farsight-backfill` | `farsight-backfill` | `farsight-config` at `/etc/farsight`, read-only | `9465` metrics, inside the compose network only (the compose file sets `metrics.backfill_bind`) |
| `postgres` | `farsight-postgres` | `postgres:16` with tuned settings | `farsight-pgdata` | `5432`, inside the compose network only |

- Only the web port is published. `FARSIGHT_PORT` changes its host
  side, for example `127.0.0.1:8080`.
- Both Farsight containers run the same image as the same user
  (`10001:10001`), so that the configuration file, mode 0600 in a 0700
  directory, is readable by both. They also get the same
  `FARSIGHT__*` environment (an optional `farsight.env` file).
- The backfill container idles until a configuration exists and the
  server has migrated the schema.
- The compose health check calls `/health` and is status only; an
  orchestrator that restarts containers should probe `/livez`.
- Both processes write block and list-membership history: the server
  on firehose removals and when a deleted list is drained, the backfill
  process when a re-read finds a record gone. The server runs the
  retention task. See [history.md](history.md).

Quick start: put `POSTGRES_PASSWORD` in `.env`, run
`docker compose up -d`, open `http://<host>:8080`, read the setup
token with `docker compose logs farsight`, and follow the wizard.
The step-by-step guide is [../guide/setup.md](../guide/setup.md); disk
and hardware sizing is in [../guide/storage.md](../guide/storage.md).

### Running a public instance

- Read responses are caller-independent and marked cacheable, so a CDN
  in front absorbs repeated queries
  ([../guide/cloudflare.md](../guide/cloudflare.md)).
- `access.reads = "api_key"` gates reads behind keys. Alternatively
  keep `"public"` with low anonymous limits and issue keys with a
  higher `readRps` and, where wanted, the `backfill` scope.
- A global query semaphore sheds load as `503 Overloaded` with
  `Retry-After`.
- `requestBackfill` is never anonymous, and the scheduler is fair
  between requesters by cost ([backfill.md](backfill.md#scheduler)).
- Per-host caps and the storage budget bound what a hostile PDS or a
  flood of records can make Farsight store
  ([security.md](security.md#aggregate-bounds)).

## Pages

| Page | Covers |
|---|---|
| [api.md](api.md) | The HTTP API: conventions and errors, the sixteen endpoints with their parameters, responses and errors, tokens and scopes, access modes, rate limits and their headers, caching and CORS, the stability contract, and how an AppView uses it. |
| [coverage.md](coverage.md) | The `freshness` object: its fields, the three levels, the witness clock and the gap predicate, the global snapshot, reason codes, exceptions and re-list debts, the scopes (network, subject, list, composite) and per-pair coverage in `checkBlocks`, pending lists, gaps and repairs, the first full sweep, and what a consumer should do. |
| [list-indexing.md](list-indexing.md) | Two-stage indexing of lists: the nine list states, the listblock counter and when a row is counted, the lock order, the transition function (events, actions, table, invariant), why a refused listitem is never lost, queries while a list is pending, and the list-size caps. |
| [backfill.md](backfill.md) | The backfill process: the per-repository job with its lease, outcomes and retries, the three-tier scheduler and per-host politeness, the sweep (sources, cycles, checkpoints), gap repair and its controls, list jobs and their lanes, on-demand backfill, and subject discovery with a backlink index. |
| [firehose.md](firehose.md) | Jetstream ingestion: v1 and v2, the pipeline and backpressure, validation and poisoned events, per-instance cursors, the resume plan, failover rewind, how a gap is detected and what it does, seam repair, the handling of each event kind, and running on v1. |
| [storage.md](storage.md) | The Postgres schema table by table, migrations, the write path, last-writer-wins and the author lock, tombstones, account status and the purge of a deleted account, counters and their rebuild, the storage budget, and sizing. |
| [history.md](history.md) | Removed blocks, listblocks and list memberships: what is stored, when a row is written and with which cause, recording windows, retention and per-day limits, where history is shown, and what it can and cannot show. |
| [web-ui.md](web-ui.md) | The three browser surfaces: server modes, the first run (setup token and wizard), admin sign-in by OAuth and sessions, the admin UI page by page, the public UI (routes, what it shows and never shows, tables, caching), handle verification with the warming worker and the handle pass, profile cards and avatars, times and themes. |
| [security.md](security.md) | The threat model, cap buckets and limits, admission keys and daily rates, what happens to a refused write, the storage budget and hard ceiling, the safe outbound client, the network edge (client address, proxy trust, cache headers), tokens and rate-limit classes, the protections of both web interfaces, and the residual risks. |
| [operations.md](operations.md) | Running an instance: how both processes start, the command line, configuration sources and environment variables, what applies without a restart, every configuration key with its default, the periodic and long-running tasks, health endpoints, metrics and logs. |

Operator how-to pages are in [`../guide/`](../guide/):
[setup](../guide/setup.md), [admin UI](../guide/admin-ui.md),
[public UI](../guide/public-ui.md), [storage and
hardware](../guide/storage.md), [Cloudflare](../guide/cloudflare.md)
and [AppView integration](../guide/appview.md).
