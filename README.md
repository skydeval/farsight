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
  site where anyone can see who blocks an account or a list. Coverage
  is stated per section in the admin UI; the public UI shows a single
  "Last updated" line per page. See [Public UI](#public-ui).
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
- access modes and the DID of the ATProto account that will
  administer the instance, after which it shows the admin token once;
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

## Admin sign-in

The web UI's admin is one ATProto account, named by its DID in
`access.admin_did`. Signing in at `/enter` sends you to that account's
own server (its PDS or entryway) to authenticate, through ATProto
OAuth; Farsight asks for the `atproto` scope only, which proves who you
are and grants no access to the account. It keeps no token: once the
account is confirmed, Farsight issues its own session (12 hours idle,
7 days at most) and forgets the rest. There is no password.

Farsight can be an OAuth client in two ways:

- **On its hostname.** When `server.hostname` is a public domain name
  without a port, reachable over HTTPS, open `https://<hostname>/enter`.
  The account's server fetches Farsight's client description from
  `/.well-known/atproto-oauth-client-metadata`. The reverse proxy must
  pass the original `Host` header.
- **On 127.0.0.1.** Anywhere else — an IP address, a port, a private
  network — sign in on the machine that runs Farsight, or through an SSH
  tunnel to it, at `http://127.0.0.1:<port>/enter`
  (`ssh -L 8080:127.0.0.1:8080 your-host`). Use `127.0.0.1`, not
  `localhost`. Browsers share cookies across ports of `127.0.0.1`, so two
  instances tunnelled at once sign each other out.

The account's server must be reachable from Farsight at a public
address: like every request Farsight makes to an address it learned
from the network, sign-in refuses private, loopback and link-local
addresses.

Things to know:

- **Whoever controls the account controls the admin UI.** That
  includes the account's server, its password there, and its PLC
  rotation keys or `did:web` domain. An account on a server you run is
  the safer choice.
- **Signing out of Farsight does not sign you out of the account's
  server.** If that server remembers you, signing in again may take one
  click.
- **Changing the admin account, or recovering from a lost one,** is done
  in the container: `docker exec farsight farsight set-admin-did <did>`,
  then `docker restart farsight`. The running server does not re-read
  `config.toml`; the change applies at the restart, and every session of
  the previous account ends then. `farsight admin-did` prints the
  current value. With `FARSIGHT_SKIP_WIZARD=1`, set
  `FARSIGHT__ACCESS__ADMIN_DID` instead. Settings shows the admin DID
  but does not change it.
- Neither the wizard nor the CLI can prove that the DID you enter is
  yours; they show what it resolves to. A wrong DID is fixed with the
  CLI.
- **`access.ui = "auth_all"`** answers every admin page with a plain
  404 to anyone not signed in. `/enter`, its callback and the client
  description stay reachable — the sign-in is hidden from nobody, the
  admin data is. After a session expires, go to `/enter`. Under
  `"public_read"` (the default) the dashboard and lookups stay public.

## Public UI

Farsight can serve a public lookup site from the same binary: search by
handle, DID, `at://` URI or bsky.app link, then a page per account (who
blocks it, which listblocked lists name it) and per list (members, who
blocks it). It is an independent equivalent of Clearsky's lookup pages.

It is **off by default**. Turn it on in Settings → Public UI; Farsight
first shows what becomes reachable without login and asks you to
confirm. It needs `access.reads = "public"`: a public site in front of
a gated API is not a supported combination. It works with every
`access.ui` mode; with `"auth_all"` the public pages are up and the
admin pages are hidden.

- **A bar on every page** with search and a light / dark / system
  theme toggle. Public pages link only to other public pages: no login
  link, no admin route.
- **Newest first.** Tables that show a creation time are sorted by it.
  The time is the author's own claim, so the order uses the earlier of
  that and the moment Farsight first stored the record: a record dated
  in the future sits where it arrived, not at the top of the page.
- **Handles.** A row shows `@handle` once Farsight has verified it in
  both directions, the DID until then. A background worker verifies
  the accounts that pages had to show as DIDs (`handle_warming_enabled`,
  on by default), at no more than the two lookups per second the
  instance already allowed itself: the first view of a page nobody has
  opened shows DIDs, a later one shows handles. Handles are kept in
  memory only; after a restart they fill in again.
- **No coverage detail.** A public page prints no coverage level. It
  says "None on record at this instance" for an empty section, says so
  when a list is not indexed, and ends with one "Last updated" line.
  The dashboard and the lookup pages state coverage in full.
- **Profile cards.** Resting the pointer on an account in a row (or
  focusing it with the keyboard) opens a card with its avatar, verified
  handle, DID and the date the DID was created. Farsight fetches that
  from the PLC directory and the account's own server when the card is
  asked for, and stores none of it. Touch devices get no cards.
- **Avatars come from the account's own server** (`show_avatars`, on by
  default): the visitor's browser fetches the image there, so that
  server's operator sees the visitor's address. Set it to `false` and
  visitors' browsers talk only to your instance; cards keep everything
  else.
- **No record addresses.** Public tables do not show `at://` record
  URIs. They are on the admin lookup pages, as links if you set
  `record_viewer_url` (a URL template with `{authority}`,
  `{collection}` and `{rkey}`).
- **Times** are sent as absolute UTC and shown in the visitor's own
  timezone by the page's script.
- **Removed records are not public.** Blocks and list memberships
  Farsight stored and later removed are on admin pages, reached from
  the DID and list lookups after login.
- **Outgoing blocks** (`show_outgoing_blocks`, off by default): the
  blocks an account has made.
- **Accounts that are not shown.** Deactivated, suspended, taken-down
  and deleted accounts have no page and appear in no row. You can
  withhold more accounts with `excluded_dids`; they get the same
  neutral notice. Exclusion changes what the public pages show, nothing
  else: the API still returns the data.
- **Search engines** are asked to stay out unless you set `crawlable`.
  Search, cards and error pages are never offered.
- **Link previews** carry a title, a fixed description and one static
  image, never data: a count in a preview is a stale claim with no date.
- Pages are safe to cache at the edge (they do not depend on the
  visitor), carry a strict Content-Security-Policy with no inline
  script, and need no JavaScript to read.

Every `[public_ui]` key applies on save, without a restart:

```toml
[access]
public_ui = false                 # the toggle

[public_ui]
instance_description = ""         # plain text on the home page
contact = ""                      # "" = server.contact
show_outgoing_blocks = false
record_viewer_url = ""            # admin lookup pages; "" = records are not links
show_avatars = true               # false = cards carry no image
card_rps = 4                      # cards fetched per second, all visitors
card_burst = 8
show_opengraph_image = true
dark_mode_default = "system"      # "light" | "dark" | "system"
crawlable = false
rate_limit_rps = 5                # page views per second per address
rate_limit_burst = 20
query_concurrency = 8             # concurrent page renders
handle_cache_ttl = "1h"
handle_warming_enabled = true     # verify handles of shown accounts in the background
excluded_dids = []                # at most 10,000
```

`handle_warming_enabled` and `record_viewer_url` also apply to the admin
lookup and history pages, whether or not the public UI is on. There a
signed-in admin additionally gets profile cards on account links and a
"First seen" column (when Farsight first stored the record).

Search shares the lookup rate (`rate_limit.ui_lookup_rps`, 1 per second
per address). Cards have their own: 2 per second per address, and
`card_rps` / `card_burst` for the whole instance. Each card makes
Farsight send at most three requests (the PLC directory, the handle's
host, the account's server); these are not counted in
`backfill.plc_rps`, so lower one of the two if your PLC source has a
tight limit. With the budget used up, cards show the DID only. A PLC
mirror must serve `/{did}/log/audit` for cards to show a creation date.

`show_history` is retired. A config that still has it loads, with a
warning; the value does nothing, and the key is removed the next time
you save the Public UI settings.

### Upgrading from an earlier version

From a version whose tables were not sorted by creation time:

- **Disk.** Four new indexes, roughly a third on top of the current
  database size. Check the headroom under `storage.budget_bytes` first:
  an index that does not fit is not built, the dashboard says so, and
  its table keeps its previous order until you raise the budget.
- **Time.** The indexes are built in the background after the new
  version starts; the instance serves and ingests meanwhile. Each table
  switches to the new order when its index is ready.
- **Paging links** into those tables stop working once, when a table
  switches; they lead to a page that links to the first page.
- **Public pages no longer show record URIs.** `record_viewer_url` now
  affects the admin lookup pages only.
- **New key** `public_ui.handle_warming_enabled` (on by default). An
  older binary refuses a config that holds it: delete the line before
  rolling back.
- **Rolling back.** An older version ignores the four indexes; they
  cost disk and write time until you drop them
  (`DROP INDEX CONCURRENTLY blocks_by_subject_created,
  blocks_by_author_created, list_blocks_by_list_created,
  list_items_by_list_created`, one per statement).

From a version with a password login:

- **Admin sign-in uses an ATProto account instead of the password.**
  After the upgrade, `/enter` shows a one-time page: enter the existing
  admin password and the DID of the account that will administer the
  instance. Farsight then writes `access.admin_did`, removes the
  password hash, ends every password session, and you sign in with the
  account. `farsight set-admin-did <did>` in the container does the same
  (then restart). Until you do either, the instance runs as before and
  the old password still opens that page — and only that page.
- The config as it was is kept next to it as `config.toml.pre-oauth`
  (0600). It holds the old password hash: delete it once sign-in works.
  To roll back, stop Farsight, copy it over `config.toml` and start the
  older image; an older binary refuses a config that has
  `access.admin_did`.
- With `FARSIGHT_SKIP_WIZARD=1` there is no file to write: set
  `FARSIGHT__ACCESS__ADMIN_DID` and restart. Until then the UI has no
  sign-in; ingest and the API run.
- `access.public_ui` no longer requires `access.ui = "public_read"`.
- Add `/enter/callback` to anything that names `/enter` (cache bypass,
  firewall rules); see "Running behind Cloudflare".

From earlier versions:

- **The admin login is at `/enter`.** `/login` is a 404. Update
  bookmarks, and any proxy or firewall rule that names `/login`.
- **`/public/about` and the public history pages are gone** (404).
  History is under `/admin/…/history`, after login.
- With avatars on, the `Content-Security-Policy` of public pages allows
  images from `https:` origins. A proxy that sets its own policy for
  `/public/*` must allow that, or set `show_avatars = false`.
- An older binary refuses a config that holds `record_viewer_url`,
  `show_avatars`, `card_rps` or `card_burst`. Delete those lines before
  rolling back.

## Storage and hardware

Farsight is a real infrastructure component. Direct blocks take up
most of the storage.

| Stage | Approximate size, including overhead |
|---|---|
| Day one | < 150 MB |
| 30 days, firehose only | 2.5–10 GB |
| First full backfill sweep complete | 28–52 GB |
| Growth afterwards | 17–19 GB per year |

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
   - Bypass the cache for `/admin*`, `/enter*` (the sign-in and its
     callback), `/setup*`, `/xrpc/app.nearhorizon.farsight.admin.*`,
     `/health` and `/livez`. The admin pages outside `/admin*` (`/`,
     `/dashboard/fragment`, `/lookup/*`, `/ops`, `/settings`, `/reset`)
     send `no-store` to a signed-in admin; bypass them too if the
     dashboard is not public.
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
