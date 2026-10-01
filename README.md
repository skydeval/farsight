# Farsight

Farsight is a self-hostable block-graph index for ATProto. For any
account, it answers **"who blocks this account?"**, covering both
direct blocks and blocks through moderation lists. Any AppView can
query it over HTTP.

An AppView has to enforce blocks in feeds, threads, notifications and
mentions. On its own it only sees blocks from accounts it already
indexes, so it misses everyone else. Farsight closes that gap without
depending on a third-party index.

> **Status: in development.** The design is locked and the storage
> layer is built; ingest is in progress. The commands below describe
> the intended deployment. No image is published yet.

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
- A freshness watermark on every response. It reports how current the
  answer is and whether Farsight can claim it is complete.
- A small server-rendered web UI with a first-run setup wizard,
  dashboard, lookups, operations and settings.

## What Farsight is not

- It is not an AppView. It serves no `app.bsky.*` views, feeds or
  timelines.
- It is not a general firehose indexer. It stores only the four record
  types above.
- It holds nothing private. Everything it stores comes from public
  records.
- It has no user accounts. Auth is bearer tokens plus one admin
  password.
- It does not decide block policy. For example, it reports each list's
  purpose, and the consumer decides how to treat it.

## Quick start (intended)

```sh
docker compose up -d
docker logs farsight          # copy the setup token printed at startup
# open http://<host>:8080 and walk the setup wizard
```

The wizard asks for:

- the setup token;
- the public hostname and an admin contact;
- the Jetstream source (it tests the connection and checks for v2
  support);
- backfill preferences and the disk space available to Postgres;
- access modes and an admin password, after which it shows the admin
  token once;
- reverse-proxy trust (it has a preset for Cloudflare);
- the Postgres connection string.

It then writes `/etc/farsight/config.toml` and switches Farsight to
normal mode.

For automated deployments, set `FARSIGHT_SKIP_WIZARD=1` and supply the
config as a file, or entirely through environment variables. Nested
config keys use one double underscore per level, for example
`FARSIGHT__BACKFILL__SWEEP__ENABLED=true`.

Set `POSTGRES_PASSWORD` in the environment before the first start.

## Storage and hardware

Farsight is a real infrastructure component. Direct blocks take up
most of the storage.

| Stage | Approximate size, including overhead |
|---|---|
| Day one | < 150 MB |
| 30 days, firehose only | 2.5–10 GB |
| First full backfill sweep complete | 28–52 GB |
| Growth afterwards | 17–19 GB per year |

- **Recommended:** 4 vCPU, 8 GB RAM, 500 GB SSD.
- **Starter (~100 GB disk):** supported. Choose one of these:
  - run firehose-only;
  - run the sweep with the storage budget the wizard sets (70% of the
    disk you enter). The sweep pauses before the disk fills, and every
    refusal is reported.

  Expect roughly 1–2.5 years of runway on a 100 GB disk.
- The Postgres data lives in the `farsight-pgdata` volume. That volume
  needs the disk space.

## Running behind Cloudflare

1. Create the DNS record as **proxied**.
2. Set SSL/TLS to **Full (strict)**, or use Cloudflare Tunnel.
3. Add cache rules:
   - Make `/xrpc/app.nearhorizon.farsight.query.*` eligible for cache
     and respect origin headers, except
     `query.getBackfillStatus`.
   - Bypass the cache for `/admin*`, `/setup*`,
     `/xrpc/app.nearhorizon.farsight.admin.*`, `/health` and `/livez`.
4. Lock the origin with Cloudflare Tunnel or Authenticated Origin
   Pulls. Firewalling the origin to Cloudflare's IP ranges alone is not
   enough.
5. In the wizard's reverse-proxy step, choose **Cloudflare**. Farsight
   then trusts `CF-Connecting-IP` only from Cloudflare addresses.

## AppView integration

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

## Workspace

| Crate | Role |
|---|---|
| `farsight-core` | shared types, config, safe outbound HTTP |
| `farsight-storage` | Postgres schema, apply path, queries, coverage |
| `farsight-ingest` | Jetstream ingestion |
| `farsight-backfill` | backfill library and the `farsight-backfill` binary |
| `farsight-api` | XRPC handlers, auth, rate limits |
| `farsight-web` | web UI and setup wizard |
| `farsight-server` | the `farsight` binary |

## Prior art and compatible services

- **Clearsky**, the public block-graph service. Farsight is a
  self-hosted equivalent of what it provided, and its data model
  matches Clearsky's.
- **Jetstream**, Bluesky's JSON firehose. It is Farsight's live
  source.
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
