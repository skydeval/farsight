# Farsight

Farsight is a self-hostable block-graph index for ATProto. For any
account, it answers **"who blocks this account?"**, covering both
direct blocks and blocks through moderation lists. Any AppView can
query it over HTTP.

An AppView has to enforce blocks in feeds, threads, notifications and
mentions. On its own it only sees blocks from accounts it already
indexes, so it misses everyone else. Farsight closes that gap without
depending on a third-party index.

> **Status: v1 feature-complete.** Storage, ingest, backfill, the API,
> the web UI, the setup wizard and the optional public lookup site are
> built. No image is published yet: `docker compose` builds it from this
> checkout.

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
- An optional **public UI** under `/public`, off by default: a lookup
  site where anyone can see who blocks an account or a list, with
  coverage stated per section. See [Public UI](#public-ui).
- A record of the blocks, listblocks and list memberships it stored
  and later removed, kept for a year by default
  (`storage.block_history_retention`) and shown only on the public UI's
  history pages. No API endpoint returns it.

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

## Quick start

```sh
export POSTGRES_PASSWORD='choose-a-password'
docker compose up -d          # builds the image on first run
docker logs farsight          # copy the setup token printed at startup
# open http://<host>:8080 and walk the setup wizard
```

The setup token is re-printed every 10 minutes, and
`docker exec farsight farsight setup-token` prints it on demand
(`--rotate` replaces it). To keep the wizard off the network until it is
done, publish the port on loopback only and use an SSH tunnel:

```sh
FARSIGHT_PORT=127.0.0.1:8080 docker compose up -d
ssh -L 8080:127.0.0.1:8080 your-server   # then open http://127.0.0.1:8080
```

The wizard asks for:

- the setup token;
- the public hostname and an admin contact;
- the Jetstream source (it tests the connection and checks for v2
  support). The default is Bluesky's public instance
  `wss://jetstream2.us-east.bsky.network`. The public instances serve
  v1 only, so coverage stays `partial` until you point Farsight at a
  self-hosted v2 Jetstream;
- backfill preferences and the disk space available to Postgres;
- access modes and an admin password, after which it shows the admin
  token once;
- reverse-proxy trust (it has a preset for Cloudflare);
- the Postgres connection string.

It then writes `/etc/farsight/config.toml` and switches Farsight to
normal mode in-process: it runs migrations, connects to the firehose and
starts serving the API. A config reset (Settings → Reset) returns it to
the wizard; the database is kept.

The `farsight-backfill` container starts with the others. It idles
until the wizard has written the config and the server has migrated the
database, then works through on-demand requests, recently active
accounts, list fetches and (if enabled in the wizard) the systematic
sweep. It talks to PDS hosts at a polite per-host rate, to the PLC
directory and to the relay set in `backfill.relay_url`; progress, ETA
and queue depths are on the dashboard and on its metrics port (9465,
not published).

For automated deployments, set `FARSIGHT_SKIP_WIZARD=1` and supply the
config as a file, or entirely through environment variables. Nested
config keys use one double underscore per level, for example
`FARSIGHT__BACKFILL__SWEEP__ENABLED=true`.

Set `POSTGRES_PASSWORD` in the environment before the first start; the
compose file passes the matching connection string to Farsight, and the
wizard's storage step is prefilled with it.

`/health` returns 200 when the firehose is connected and the database
answers within a second (compose uses it as a status check); `/livez`
returns 200 while the process serves HTTP and is the right probe for
orchestrators that restart unhealthy containers.

## Public UI

Farsight can serve a public lookup site from the same binary: search by
handle, DID, `at://` URI or bsky.app link, then a page per account (who
blocks it, which listblocked lists name it) and per list (members, who
blocks it). It is an independent equivalent of Clearsky's lookup pages.

It is **off by default**. Turn it on in Settings → Public UI; Farsight
first shows what becomes reachable without login and asks you to
confirm. It needs `access.reads = "public"` and `access.ui =
"public_read"`: a public site in front of a gated API is not a
supported combination.

- **Coverage is stated honestly.** Every section prints the coverage of
  the data it was built from (`Complete`, `Best effort` or `Partial`,
  with the reasons), and the page summary is the lowest of its sections.
  Times are absolute UTC.
- **Removed records** (`show_history`, on by default): blocks and list
  memberships Farsight stored and later removed, with what the record
  can and cannot show spelled out on the page. This data has no other
  public source; turn it off if you do not want to publish it.
- **Outgoing blocks** (`show_outgoing_blocks`, off by default): the
  blocks an account has made.
- **Accounts that are not shown.** Deactivated, suspended, taken-down
  and deleted accounts have no page and appear in no row. You can
  withhold more accounts with `excluded_dids`; they get the same
  neutral notice. Exclusion changes what the public pages show, nothing
  else: the API still returns the data.
- **Search engines** are asked to stay out unless you set `crawlable`.
  History, search and error pages are never offered.
- **Link previews** carry a title, a fixed description and one static
  image, never data: a count in a preview is a stale claim with no date.
- Pages are safe to cache at the edge (they do not depend on the
  visitor), carry a strict Content-Security-Policy, and need no
  JavaScript to read.

Every `[public_ui]` key applies on save, without a restart:

```toml
[access]
public_ui = false                 # the toggle

[public_ui]
instance_description = ""         # plain text on the home page
contact = ""                      # "" = server.contact
show_outgoing_blocks = false
show_history = true
show_opengraph_image = true
dark_mode_default = "system"      # "light" | "dark" | "system"
crawlable = false
rate_limit_rps = 5                # page views per second per address
rate_limit_burst = 20
query_concurrency = 8             # concurrent page renders
handle_cache_ttl = "1h"
excluded_dids = []                # at most 10,000
```

Search and history pages share the lookup rate
(`rate_limit.ui_lookup_rps`, 1 per second per address).

## Storage and hardware

Farsight is a real infrastructure component. Direct blocks take up
most of the storage.

| Stage | Approximate size, including overhead |
|---|---|
| Day one | < 150 MB |
| 30 days, firehose only | 2.5–10 GB |
| First full backfill sweep complete | 28–52 GB |
| Growth afterwards | 17–19 GB per year |

Removed records are extra: about 0.2 GB per million kept, inside the
same storage budget. `storage.block_history_retention` bounds them
(`"0s"` keeps them forever); `storage.block_history_enabled = false`
stops recording.

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
   - If the public UI is on, make `/public/*` eligible for cache and
     respect origin headers.
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
| `farsight-web` | web UI, setup wizard and public UI |
| `farsight-server` | the `farsight` binary |

## Prior art and compatible services

- **Clearsky**, the comparable public backlink index for blocks and
  block lists. Farsight is a self-hosted equivalent of what it
  provided, and its data model matches Clearsky's.
- **Jetstream**, Bluesky's JSON firehose. It is Farsight's live
  source. Bluesky runs public instances at
  `jetstream{1,2}.us-{east,west}.bsky.network`; they currently serve
  only the v1 protocol, which carries no `#sync` events.
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
