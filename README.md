# Farsight

Farsight is a self-hostable block-graph index for ATProto. For any
account, it answers **"who blocks this account?"**, covering both
direct blocks and blocks through moderation lists. Any AppView can
query it over HTTP.

An AppView has to enforce blocks in feeds, threads, notifications and
mentions. On its own it only sees blocks from accounts it already
indexes, so it misses everyone else. Farsight closes that gap.

## What Farsight is

- An index of four record types, network-wide:
  - `app.bsky.graph.block`
  - `app.bsky.graph.listblock`
  - `app.bsky.graph.list`
  - `app.bsky.graph.listitem`, stored only for lists that at least one
    listblock targets.
- Live ingestion from Jetstream, plus a background worker that
  backfills history at a polite rate. It can also backfill a specific
  account on request.
- An XRPC API under `app.nearhorizon.farsight.*`:
  - `query.getIncomingBlocks`: who directly blocks an account.
  - `query.getIncomingListBlocks`: who blocks an account through a
    list that names it.
  - `query.getListsNaming`: which listblocked lists name an account.
  - `query.getListMembers`: the members of a list.
  - `query.checkBlocks`: block relations between one account and up to
    100 others, in both directions. Use this when hydrating a page.
  - `query.getStats`: counts, firehose status and backfill progress.
  - `query.getBackfillStatus` and `admin.requestBackfill`: the
    on-demand backfill hook. Both need a token.
  - `admin.listErrors`, `admin.restartFirehose`, `admin.pauseSweep`,
    `admin.startRepair`, `admin.pauseRepair`, `admin.cancelRepair`,
    `admin.createApiKey` and `admin.revokeApiKey`: what the Operations
    page does, for an instance run without the admin UI.
- A freshness watermark on every response. It reports how current the
  answer is and whether Farsight can claim it is complete.
- A first-run setup wizard, and an optional server-rendered **admin
  UI** under `/admin`: dashboard, lookups, operations and settings,
  after sign-in.
- An optional **public UI** at the root of the hostname, off by
  default: a lookup site where anyone can see who blocks an account or
  a list. Coverage
  is stated per section in the admin UI; the public UI shows a single
  "Last updated" line per page. See the [public UI guide](docs/guide/public-ui.md).
- A record of the blocks, listblocks and list memberships it stored
  and later removed, kept for a year by default
  (`storage.block_history_retention`) and shown only in the admin UI.
  No API endpoint returns it.

## What Farsight is not

- It is not an AppView. It serves no `app.bsky.*` views, feeds or
  timelines.
- It is not a general firehose indexer. It stores only the four record
  types above.
- It holds nothing private. Everything it stores comes from public
  records.
- It has no user accounts. The API uses bearer tokens; the web UI
  admits one admin, who signs in with an ATProto account.
- It does not decide block policy. For example, it reports each list's
  purpose, and the consumer decides how to treat it.

What the API leaves out is deliberate; see
[the API's design](docs/design/api.md#what-the-api-leaves-out-and-why).
A fork that changes the API should serve it under its own namespace,
not under `app.nearhorizon.farsight.*`.

## Quick start

```sh
export POSTGRES_PASSWORD='choose-a-password'
docker compose up -d          # builds the image on first run
docker logs farsight          # copy the setup token printed at startup
# open http://<host>:8080 and walk the setup wizard
```

The wizard asks for the public hostname, the Jetstream source, backfill
preferences, who may read the API and which web interfaces to serve,
then writes the config and starts ingesting. The default Jetstream
source is Bluesky's two public v2 instances.

Hardware: 4 vCPU, 8 GB RAM and a 500 GB SSD are recommended; a 100 GB
disk is supported with limits.

## Guide

| Page | What it covers |
|---|---|
| [Setting up](docs/guide/setup.md) | the setup token, the wizard's steps, the backfill container, unattended deployment, health checks |
| [Admin UI and sign-in](docs/guide/admin-ui.md) | the admin pages, signing in with an ATProto account, changing the admin |
| [Public UI](docs/guide/public-ui.md) | the optional public lookup site: what it shows, what it fetches, its settings |
| [Storage and hardware](docs/guide/storage.md) | sizes over time, the storage budget, small disks |
| [Running behind Cloudflare](docs/guide/cloudflare.md) | DNS, cache rules, locking the origin |
| [AppView integration](docs/guide/appview.md) | which endpoints to call and how to read coverage |

How Farsight works inside (the data model, coverage, the list state
machine, backfill, the firehose, limits) is in the
[design documentation](docs/design/README.md). Changes are in the
[changelog](CHANGELOG.md).

## Workspace

| Crate | Role |
|---|---|
| `farsight-core` | shared types, config, safe outbound HTTP |
| `farsight-storage` | Postgres schema, apply path, queries, coverage |
| `farsight-ingest` | Jetstream ingestion |
| `farsight-backfill` | backfill library and the `farsight-backfill` binary |
| `farsight-api` | XRPC handlers, auth, rate limits |
| `farsight-web` | web UI, setup wizard and public UI |
| `farsight-server` | the `farsight` binary |

## Prior art and compatible services

- **Clearsky**, the comparable public backlink index for blocks and
  block lists. Farsight is a self-hosted equivalent of what it
  provided, and its data model matches Clearsky's.
- **Jetstream**, Bluesky's JSON firehose. It is Farsight's live
  source. Bluesky's public instances at
  `jetstream.us-{east,west}.bsky.network` serve the v2 protocol, which
  Farsight needs to report complete coverage; the older
  `jetstream{1,2}.us-{east,west}.bsky.network` serve only v1, which
  carries no `#sync` events.
- **Constellation** and **Slingshot** from the microcosm project, a
  backlink index and a record/identity cache. Farsight can use a
  Constellation-compatible backlink API for optional subject
  discovery, and a Slingshot-style resolver. Both are off by default.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this work, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms
or conditions.
