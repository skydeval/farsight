# Operations

This page describes what a running Farsight instance does by itself
and what an operator can set: how the two processes start, the command
line, every configuration key with its default, the environment
variables, which settings apply without a restart, the periodic tasks,
the health endpoints, metrics and logs. It is the reference; the
step-by-step guides are in [`../guide/`](../guide/), starting with
[setup.md](../guide/setup.md).

## Processes and start-up

An instance is two processes from one image, `farsight` (the server)
and `farsight-backfill`, and one Postgres database. The overview is in
[README.md](README.md).

### The server

1. **Configuration.** The server looks for the configuration file
   (`/etc/farsight/config.toml`, or the path in `FARSIGHT_CONFIG`).
   - The file exists: it is loaded, environment overrides are applied,
     and the result is validated.
   - No file, and `FARSIGHT_SKIP_WIZARD` is set: the configuration is
     built from the environment alone.
   - No file otherwise: **setup mode**. The server serves the setup
     wizard, every `/xrpc/` path answers `503 SetupRequired`, and
     `/health` answers `503 {"status":"setup"}`. When the wizard
     finishes, the server switches to normal mode in the same process.
   - An unparseable or invalid configuration makes the process exit
     with status 1. It is never rewritten and never leads to setup
     mode.
2. **Database.** The server connects, retrying with a delay that
   doubles from 0.5 to 10 seconds, and runs the migrations. Only the
   server migrates.
3. **History window.** `storage.block_history_enabled` is read here
   and only here; the recording window opens or closes accordingly
   ([history.md](history.md)).
4. **Ingest.** Before the Jetstream reader and its writer start,
   ingest looks for accounts with status `deleted` that still
   have rows (a purge interrupted by a crash) and purges them, up to
   1,000 accounts per start. The check first reads the deleted
   accounts in a single scan of `actors` and only then looks for their
   rows, so that it does not walk the whole table through its primary
   key before the server serves.
5. **Serving.** The first coverage snapshot is read, the API keys are
   loaded, and the listener on `server.bind` opens.
6. **Background work** starts once the server is serving, so that none
   of it stands between a start and a healthy instance: the periodic
   tasks, the handle workers, the top-list task and the builder of the
   UI sort indexes (see [Background tasks](#background-tasks)).

On `SIGTERM` or `SIGINT` the server stops accepting requests, stops
its tasks, shuts ingest down, flushes its counters and exits 0.

Database connections, for sizing Postgres `max_connections`:

| Pool | Size |
|---|---|
| Server, API and UI | `rate_limit.query_concurrency` + 8 (40) |
| Server, ingest | 4 |
| Server, periodic tasks | 4 |
| Server, sort-index builder | 1, of its own, while it checks or builds |
| Backfill | `backfill.concurrency` + 8 (40) |

With the defaults that is at most 89 connections, under Postgres'
default `max_connections` of 100. Raising `query_concurrency` or
`backfill.concurrency` raises the total by the same amount.

### The backfill process

`farsight-backfill` uses the same file, the same `FARSIGHT__*`
environment and the same loading rules. Without a configuration it
idles and looks again every 5 seconds, which is how it waits out the
setup wizard. With one, it waits until the database's `schema_version`
equals the version it was built for (it polls every 5 seconds), then
runs its scheduler, the sweep, the repair logic and a storage-budget
monitor of its own. What it does is described in
[backfill.md](backfill.md).

It follows configuration changes by itself: it reloads on a
`NOTIFY farsight_config` from the server, and every 60 seconds when
the file's modification time has changed. An invalid file keeps the
running configuration. If the file disappears (a configuration reset)
it goes back to idling.

## Command line

```
farsight [setup-token [--rotate] | set-admin-did <did> [--force] | admin-did]
farsight --version
farsight-backfill
farsight-backfill --version
```

| Command | Effect |
|---|---|
| `farsight` | Runs the server. |
| `farsight setup-token` | Prints the setup token, creating one if none exists. Refuses (exit 1) when a configuration file exists: a configured instance has no setup token. |
| `farsight setup-token --rotate` | Replaces the setup token and prints the new one. |
| `farsight admin-did` | Prints the admin DID in force and where it comes from: the environment (`FARSIGHT__ACCESS__ADMIN_DID`) or the file. Makes no network request. |
| `farsight set-admin-did <did>` | Sets `access.admin_did` in the configuration file. This is the way to change the admin account, and the recovery when the account is lost or was mistyped. |
| `farsight --version`, `-V` | Prints the version. |

`set-admin-did` in detail:

- It takes a DID, not a handle: `did:plc:` followed by 24 characters
  of `a-z` and `2-7`, or `did:web:` followed by a hostname.
- It refuses when there is no configuration file, and when
  `FARSIGHT__ACCESS__ADMIN_DID` is set, since the environment would
  override the file. Change the variable instead.
- It resolves the DID before writing and prints the verified handle. A
  DID that does not resolve is refused; `--force` sets it anyway, for
  a recovery while a directory is down.
- The edited file must load; otherwise nothing is changed.
- It edits the file only. **The running server does not re-read the
  file**: restart `farsight` to apply.

In the compose deployment the commands run inside the container, for
example `docker exec farsight farsight admin-did`. Exit status: 0 on
success, 1 on failure, 2 on a usage error.

## Configuration

### Sources

In increasing precedence:

1. Built-in defaults.
2. The file: `/etc/farsight/config.toml` on the `farsight-config`
   volume, mode 0600. `FARSIGHT_CONFIG` names another path.
3. Environment variables of the form `FARSIGHT__<SECTION>__<KEY>`,
   with one `__` per nesting level:
   `FARSIGHT__BACKFILL__SWEEP__ENABLED=false`,
   `FARSIGHT__FIREHOSE__TUNING__STALL_TIMEOUT=90s`.

Every table refuses unknown keys, in the file and in the environment
alike: a misspelled key stops the process rather than being silently
ignored. An environment value is typed by the default at the same
path:

| Type | Accepted |
|---|---|
| boolean | `true`, `1`, `yes`, `on`; `false`, `0`, `no`, `off` |
| integer | Digits, with optional `_` separators |
| string, duration | As written |
| list | Comma-separated; an empty value is the empty list |

Durations are a number and a unit: `"30s"`, `"5m"`, `"1h"`, `"7d"`.

Four keys have no default and are required: `server.hostname`,
`server.contact`, `storage.database_url` and
`auth.admin_token_sha256`. The wizard writes them; an unattended
deployment supplies them in the environment.

### Other environment variables

| Variable | Read by | Meaning |
|---|---|---|
| `FARSIGHT_SKIP_WIZARD` | both processes | Set to anything but empty, `0` or `false`: with no file, build the configuration from `FARSIGHT__*` alone and never enter setup mode. Such an instance is managed from outside: no setting can be changed from the admin UI or through the API. |
| `FARSIGHT_CONFIG` | both processes | Path of the configuration file. |
| `FARSIGHT_SETUP_BIND` | server | Restricts the listener while in setup mode, for example `127.0.0.1` or `127.0.0.1:8080`. |
| `RUST_LOG` | both processes | Log filter; the default is `info,sqlx=warn,hyper=warn`. |
| `FARSIGHT_PORT` | `compose.yml` | Host side of the published web port (default `8080`). |
| `POSTGRES_PASSWORD` | `compose.yml` | Password of the bundled Postgres, also placed in `FARSIGHT__STORAGE__DATABASE_URL`. |

A key set from the environment is **locked**: the admin UI shows it
but cannot change it, and an API procedure that would write it answers
`InvalidRequest`.

### What applies without a restart

The server reads its configuration at start. It does not watch the
file. Changes reach a running server only through its own edits: the
Settings and Operations pages of the admin UI, and the admin
procedures `pauseSweep`, `pauseRepair` and `cancelRepair`. Such an
edit is validated with the same loader as at start, written to the
file atomically, and announced to the backfill process. The save
reports which of the changed keys still need a restart.

| Keys | Server |
|---|---|
| `[public_ui]`, all keys | apply at once |
| `[access]`: `reads`, `cors`, `public_ui` | apply at once |
| `[auth]`, `[proxy]` | apply at once |
| `server.contact` | applies at once |
| `[rate_limit]`: `anon_rps`, `anon_burst`, `key_rps`, `key_burst`, `admin_backfill_rps`, `key_backfill_rps`, `ui_lookup_rps`, `query_timeout` | apply at once |
| `[backfill]` and its sub-tables | nothing to apply in the server; the backfill process reloads them |
| `access.admin_ui` | at start only; an edit from the running server that would change it is refused |
| `access.admin_did` | at start only; set with `farsight set-admin-did`, then restart |
| `server.bind`, `server.hostname`, `[storage]`, `[firehose]`, `[limits]`, `[net]`, `[metrics]`, `rate_limit.query_concurrency` | restart `farsight` |

`access.admin_ui` is start-only for two reasons: an edit made in the
admin UI would remove the page it was made from, and a change waiting
in a hand-edited file must not go live as a side effect of another
save. While the file and the running server disagree on it, no setting
can be saved from the running server; restart, or undo the hand edit.

A file edited by hand reaches the server at its next restart. The
backfill process picks it up within a minute.

The backfill process applies every change it can in place. For a
change of `storage.database_url`, `[metrics]`, `[net]`,
`backfill.per_host_rps`, `backfill.per_host_concurrency`,
`backfill.plc_rps`, `backfill.plc_url`, `limits.large_hosts` or
`limits.cdn_ranges_extra` it rebuilds itself in-process; the container
is not restarted.

`storage.block_history_enabled` is read by both processes, and the
server applies a change at restart only.

## Configuration reference

Defaults are the values a key takes when it is absent.

### `[server]`

| Key | Default | Meaning |
|---|---|---|
| `bind` | `"0.0.0.0:8080"` | Listen address of the API and the web UI. |
| `hostname` | required | Public hostname: absolute links, the OAuth client identity, the outbound `User-Agent`. |
| `contact` | required | Operator contact, shown in `getStats` and sent in the outbound `User-Agent`. |

### `[storage]`

| Key | Default | Meaning |
|---|---|---|
| `database_url` | required | Postgres connection string. |
| `budget_bytes` | `70_000_000_000` | Storage budget, compared with `pg_database_size`. At the budget the sweep and new list admissions are held back. Must be positive. See [security.md](security.md). |
| `hard_ceiling_bytes` | `0` | Hard ceiling; `0` means 115% of `budget_bytes`. Must exceed the budget. |
| `tombstone_ttl` | `"7d"` | How long a deletion's tombstone is kept ([storage.md](storage.md)). |
| `block_history_enabled` | `true` | Record removed blocks, listblocks and list memberships ([history.md](history.md)). |
| `block_history_retention` | `"365d"` | How long history rows are kept, for all three history tables; `"0s"` keeps them forever. |

Sizing advice is in [../guide/storage.md](../guide/storage.md).

### `[firehose]`

| Key | Default | Meaning |
|---|---|---|
| `urls` | `["wss://jetstream.us-east.bsky.network", "wss://jetstream.us-west.bsky.network"]` | Jetstream instances, in failover order. At least one; each a `ws://` or `wss://` URL. |

The defaults are Bluesky's two public instances that offer the v2
endpoint. An instance that offers only v1 works, but caps coverage at
`partial` with the reason `sync_events_unavailable`; see
[firehose.md](firehose.md).

### `[firehose.tuning]`

| Key | Default | Meaning |
|---|---|---|
| `gap_threshold` | `"300s"` | On a resume, a first event later than the requested cursor by more than this counts as a clamp (the instance no longer had the position) and opens a gap. |
| `failover_rewind_min` | `"10m"` | Smallest rewind on failover: the new instance is asked for the applied position minus the larger of this and its lag plus 5 minutes. |
| `failover_max_lag` | `"30m"` | Largest lag of the new instance for which such a rewind is trusted. |
| `synthetic_gap_lag` | `"5m"` | Applied-through lag beyond which coverage treats the stream as behind, as it does while disconnected. |
| `stall_timeout` | `"60s"` | Silence that ends a session. |
| `seam_repair_before` | `"150s"` | A seam repair's window starts this long before the session's connect. |
| `seam_repair_after` | `"30s"` | And ends this long after the session caught up. |
| `seam_repair_delay` | `"60s"` | Wait between catching up and the seam repair. |
| `seam_repair_catchup_margin` | `"5s"` | A session has caught up once an event's witness time is within this of wall time. |

The resume plan, failover and seam repair are explained in
[firehose.md](firehose.md#cursors-reconnects-and-gaps).

### `[backfill]`

| Key | Default | Meaning |
|---|---|---|
| `concurrency` | `32` | Worker pool size. Must be positive. |
| `per_host_rps` | `10` | Request rate towards any one PDS host. |
| `per_host_concurrency` | `4` | Concurrent requests towards any one host. |
| `plc_url` | `"https://plc.directory"` | PLC directory. |
| `plc_rps` | `10` | Request rate towards the PLC directory. |
| `plc_seed_from_export` | `false` | Seed PDS resolution from the PLC export. |
| `relay_url` | `"https://bsky.network"` | Relay used to enumerate repositories. |
| `request_fresh_window` | `"1h"` | A `requestBackfill` for an account done more recently than this adds no work unless forced ([api.md](api.md)). |
| `owner_fetch_cooldown` | `"10m"` | Least interval between list fetch runs for one owner. |
| `retry_schedule` | `["1h", "6h", "24h", "1d"]` | Backoff of a failing repository job; the last step repeats. Not empty. |
| `terminal_after` | `"7d"` | A repository job failing for this long becomes terminal. |
| `missing_retry` | `["1h", "24h", "7d"]` | Re-check schedule of a list whose record is missing. Not empty. |
| `list_fetch_max_attempts` | `2` | Failed fetch attempts before a list fetch gives up. |
| `phase1_retry` | `["5m", "20m", "1h"]` | Retry schedule for errors in the first phase of a list fetch. Not empty. |
| `list_fetch_max_duration` | `"1h"` | Wall-clock cap on one list fetch run. |
| `repair_slack` | `"1h"` | Margin added around a gap when choosing the accounts to re-read. |
| `seen_set_cap` | `2_000_000` | Cap of the in-memory set that tolerates out-of-order listings. |
| `system_queue_cap` | `50_000` | Queue entries per system requester. |
| `tier_shares` | `[60, 25, 15]` | Guaranteed shares of the three scheduler tiers, in percent. Three values summing to 100. |

### `[backfill.sweep]`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Whether the systematic sweep runs. `admin.pauseSweep` and the Operations page set this. |
| `source` | `"relay_collections"` | Enumeration source: `relay_collections` (the relay's per-collection repository listing), `relay_repos` (the relay's `listRepos`) or `plc` (the PLC export, exhaustive and slow). |
| `max_repos_per_hour` | `0` | Pacing cap; `0` means bounded only by the per-host limits. |
| `full_every_days` | `0` | Interval of a periodic full cycle; `0` means never. |
| `max_outstanding` | `10_000` | Bound on cycle members handed out and not yet finished. |

### `[backfill.repair]`

| Key | Default | Meaning |
|---|---|---|
| `auto_start` | `true` | A repair starts by itself once a firehose gap has closed. When false, a closed gap waits for `admin.startRepair`. `admin.cancelRepair` sets this to false. |
| `paused` | `false` | Holds repairs: a repair under way reads nothing new and keeps its place. `admin.pauseRepair` sets this. |

A repair re-reads every account whose repository changed during the
gap, found by walking the relay's whole repository listing. Expect a
repair after a gap of days to run for days. The two keys exist so
that such a repair can be held, or kept from starting, while the
instance does something more urgent. The design of repairs and their
controls is in [backfill.md](backfill.md#gap-repair); the operator's
view is in [../guide/admin-ui.md](../guide/admin-ui.md).

### `[backfill.backlinks]`

| Key | Default | Meaning |
|---|---|---|
| `url` | `""` | A backlink index to ask "who references this DID". Empty disables subject discovery; Farsight then depends on no third-party index. |
| `max_refs` | `200_000` | Reference cap of one discovery; a discovery that hits it is reported as truncated. |
| `lag_allowance` | `"5m"` | Allowance for the backlink index's own lag. |

### `[access]`

| Key | Default | Meaning |
|---|---|---|
| `reads` | `"public"` | Who may call the read queries: `public`, `api_key` or `disabled` ([api.md](api.md)). |
| `cors` | `true` | Send `Access-Control-Allow-Origin: *` on read queries. |
| `public_ui` | `false` | Serve the public site at `/`. Requires `reads = "public"`. |
| `admin_ui` | `true` | Serve the admin UI under `/admin`, with its sign-in at `/enter`. Read at start only. |
| `admin_did` | unset | The DID of the one account that may sign in to the admin UI. Never required: without it the instance runs and nobody can sign in. A malformed value fails the load. |

The two UI switches combine freely:

| | `admin_ui = true` | `admin_ui = false` |
|---|---|---|
| **`public_ui = true`** | Public site at `/`, admin UI at `/admin`. | Public site at `/`; administration by admin token and configuration file. |
| **`public_ui = false`** | Admin UI at `/admin`; `/` redirects there. | API only; `/` is a short text page. |

Every admin page needs a session, in every combination. See
[web-ui.md](web-ui.md).

### `[public_ui]`

Every key applies without a restart. What the pages show is described
in [web-ui.md](web-ui.md) and
[../guide/public-ui.md](../guide/public-ui.md).

| Key | Default | Meaning |
|---|---|---|
| `instance_description` | `""` | Plain text for the home page; empty shows a default text. At most 2,000 characters. |
| `contact` | `""` | Contact for the public pages; empty means `server.contact`. At most 200 characters. |
| `show_outgoing_blocks` | `false` | Account pages also show whom the account blocks and which block lists it subscribes to. |
| `show_top_blockers` | `false` | The home page lists the accounts that block the most. |
| `show_top_blocked` | `false` | The home page lists the accounts that are blocked the most. |
| `show_opengraph_image` | `true` | Emit an `og:image` tag for the one static image. |
| `dark_mode_default` | `"system"` | Theme before the visitor chooses: `light`, `dark` or `system`. |
| `crawlable` | `false` | Let crawlers index the public pages; when false they are served with `X-Robots-Tag: noindex`. |
| `rate_limit_rps` | `5` | Public page views per second per client address. Positive. |
| `rate_limit_burst` | `20` | Burst of the same. Positive. |
| `query_concurrency` | `8` | Concurrent public page renders. Positive, and at most `rate_limit.query_concurrency` while the public UI is on. |
| `handle_cache_ttl` | `"1h"` | How long a verified handle stays in the memory cache. The stored copy outlives it and refills it. |
| `excluded_dids` | `[]` | DIDs the public pages withhold. At most 10,000 valid DIDs. |
| `record_viewer_url` | `""` | URL template of a record viewer for the admin lookup pages; empty means records are not links. See below. |
| `show_avatars` | `true` | Profile cards and headers carry the account's avatar, which the visitor's browser fetches. |
| `avatar_thumbnails` | `false` | Name avatars and list images on Bluesky's image service (`cdn.bsky.app`) as small thumbnails, instead of the original on the account's own server. Only with `show_avatars`. Farsight itself stores no image either way. |
| `card_rps` | `4` | Profile cards the process fetches per second, for all visitors together. Positive. |
| `card_burst` | `8` | Burst of the same. Positive; a value below `card_rps` is raised to it, with a warning. |
| `handle_warming_enabled` | `true` | A background worker verifies the handles of accounts that pages had to show as bare DIDs. It governs the admin pages too and works with the public UI off. |
| `handle_rps` | `20` | Handle verifications started per second, for pages, cards and the warming worker together. 1 to 200. Each is up to two outbound requests. |
| `handle_pass_rps` | `0` | Rate of the handle pass, which works through every account Farsight holds; `0` is off, at most 200. It has its own pace and does not draw on `handle_rps`. |

`record_viewer_url`, when not empty, must be an absolute `http` or
`https` URL with a host and no credentials, at most 500 characters,
without spaces, containing each of `{authority}`, `{collection}` and
`{rkey}` and no other braces. The scheme, host and port must be fixed
text: placeholders belong after the first `/` of the path.

### `[auth]`

| Key | Default | Meaning |
|---|---|---|
| `admin_token_sha256` | required | SHA-256 of the admin token, as 64 hex characters. The token itself is shown once, by the wizard. |

### `[proxy]`

| Key | Default | Meaning |
|---|---|---|
| `mode` | `"none"` | `none`: the TCP peer is the client. `cloudflare`: `CF-Connecting-IP` from trusted peers. `forwarded`: `X-Forwarded-For` from trusted peers. |
| `trusted` | `[]` | CIDR ranges of the proxies to believe. `0.0.0.0/0`, `::/0` and anything broader than /8 (IPv4) or /24 (IPv6) is refused. |
| `cloudflare_refresh` | `false` | Fetch Cloudflare's published ranges at start and once a day. |

A fresh instance trusts no proxy. A `mode` other than `none` with an
empty `trusted` list is accepted with a warning, and forwarding
headers are then ignored. A public range outside Cloudflare's also
draws a warning. See [security.md](security.md) and
[../guide/cloudflare.md](../guide/cloudflare.md).

### `[limits]`

Caps that bound what any one account, host or list can make Farsight
store or do. A cap that is hit is recorded, counted in
`farsight_abuse_capped_total` and, where a query can observe it,
reported in coverage. Buckets, admission keys and what happens to a
refused write are explained in
[security.md](security.md#aggregate-bounds); the list caps in
[list-indexing.md](list-indexing.md#list-size-caps).

| Key | Default | Meaning |
|---|---|---|
| `list_grace` | `"7d"` | How long a list stays `retained` after its last listblock is gone. |
| `owner_readmissions_per_day` | `4` | Re-admissions of its lists that one owner can cause per UTC day; beyond it a list is `deferred` until the next day. |
| `pending_max_age` | `"3h"` | Longest a list stays `pending`, counted from its admission, queue wait included; after it the list becomes `unavailable` and keeps retrying. |
| `pending_effects_per_owner_key` | `5` | Pending lists per owner key that take effect in coverage at a time, oldest admission first; the rest are counted as excluded ([coverage.md](coverage.md#pending-lists)). |
| `list_items_per_list` | `1_000_000` | Stored items per list. |
| `list_items_per_owner` | `2_000_000` | Stored items per list owner. |
| `blocks_per_author` | `1_000_000` | Stored blocks per author. |
| `listblocks_per_author` | `100_000` | Stored listblocks per author. |
| `lists_per_author` | `10_000` | Stored list records per author. |
| `listblock_fetch_triggers_per_author` | `5_000` | Counted listblocks per author; beyond it a listblock is stored uncounted and admits no list. |
| `large_hosts` | `["*.host.bsky.network"]` | Hosts exempt from the bucket caps. Exact names, or `*.suffix` for any subdomain. Per-author caps and daily rates still apply. |
| `host_blocks` | `20_000_000` | Blocks per domain or address bucket. |
| `host_list_items` | `5_000_000` | List items per bucket. |
| `host_listblocks` | `2_000_000` | Listblocks per bucket. |
| `host_lists` | `200_000` | List records per bucket. |
| `host_interned_lifetime` | `5_000_000` | Rows (accounts and placeholder lists) a bucket may ever cause to be created. |
| `unresolved_blocks` | `1_000_000` | Blocks in the `unresolved` bucket: authors whose DID has not been resolved yet. |
| `unresolved_list_items` | `500_000` | List items in that bucket. |
| `unresolved_listblocks` | `200_000` | Listblocks in that bucket. |
| `unresolved_lists` | `20_000` | Lists in that bucket. |
| `bucket_admissions_per_day` | `20_000` | List admissions per UTC day for an admission key that starts with `bucket:` (authors on an ordinary host share their domain's key). Over it a listblock is stored uncounted. |
| `did_admissions_per_day` | `200` | List admissions per UTC day for every other admission key: an account on a large host, or an unresolved one. |
| `intern_per_did_per_day` | `1_000_000` | Rows created per UTC day on behalf of one DID key or one API requester (`token:<id>`, `admin`). |
| `intern_per_bucket_per_day` | `5_000_000` | The same for a `bucket:` key. |
| `cdn_ranges_extra` | `[]` | Further shared CDN or anycast ranges not to be used as address buckets. |
| `history_per_did_per_day` | `10_000` | History rows per UTC day per DID admission key, all three history tables together. |
| `history_per_bucket_per_day` | `200_000` | The same for a `bucket:` key. |

### `[net]`

| Key | Default | Meaning |
|---|---|---|
| `allow_http_hosts` | `[]` | Hosts the outbound client may reach over plain `http`. For development only. |

### `[rate_limit]`

| Key | Default | Meaning |
|---|---|---|
| `anon_rps` | `10` | Anonymous read queries per second per client address. |
| `anon_burst` | `50` | Burst of the same. |
| `key_rps` | `100` | Read queries per second per API key, unless the key has its own rate. |
| `key_burst` | `500` | Burst of the same. |
| `admin_backfill_rps` | `20` | `requestBackfill` per second with the admin token (burst 100). |
| `key_backfill_rps` | `5` | `requestBackfill` per second per API key (burst 20). |
| `ui_lookup_rps` | `1` | Public UI searches per second per client address (burst 5). |
| `query_concurrency` | `32` | Concurrent API requests; also sizes the API connection pool. |
| `query_timeout` | `"5s"` | `statement_timeout` of a read query. |

### `[metrics]`

| Key | Default | Meaning |
|---|---|---|
| `bind` | `"0.0.0.0:9464"` | The server's metrics listener. |
| `backfill_bind` | `"0.0.0.0:9465"` | The backfill process's metrics listener. |
| `bearer_token_sha256` | `""` | SHA-256 (64 hex characters) of a bearer token required on `/metrics`; empty means no authentication. |

### An example

A small file; every key not written takes its default:

```toml
[server]
hostname = "farsight.example"
contact = "mailto:ops@farsight.example"

[storage]
database_url = "postgres://farsight:secret@postgres:5432/farsight"
budget_bytes = 200_000_000_000

[access]
reads = "public"
public_ui = true
admin_did = "did:plc:abcdefghijklmnopqrstuvwx"

[auth]
admin_token_sha256 = "<64 hex characters>"

[proxy]
mode = "cloudflare"
trusted = ["173.245.48.0/20", "103.21.244.0/22"]

[backfill.repair]
auto_start = false
```

## Background tasks

### Periodic tasks of the server

One scheduler in the server runs these. Each task runs in its own
task and never overlaps itself, so a slow daily job does not delay the
budget monitor. Intervals are measured from the process's start, not
from the clock: the first run is the stated delay after start plus up
to a quarter of it, and each later run follows after the period plus
up to 5%. A task that fails is logged and recorded among the
operational errors as `task:<name>` (`admin.listErrors`, and the
admin UI).

| Task | Every | First run after | What it does |
|---|---|---|---|
| `budget_monitor` | 1 min | at once | Measures `pg_database_size`, drives the storage gates and publishes them to every writer, records refusal intervals for coverage, defers unclaimed `pending` lists while a gate refuses, and re-admits up to 200 deferred lists per run once it reopens. Sets `farsight_storage_db_bytes` and `farsight_storage_budget_ratio`. Warns when the last day's growth exceeds twice the trailing average (needs three days of uptime). |
| `purges` | 10 s | 5 s | Deletes, in batches, the items of lists that are being purged, and finishes the purge. |
| `grace_expiry` | 1 h | 2 min | Ends the grace period of `retained` lists older than `limits.list_grace`. |
| `tombstones` | 1 h | 3 min | Deletes tombstones older than `storage.tombstone_ttl`. |
| `firehose_clock` | 1 h | 4 min | Thins `firehose_clock` rows older than 24 hours to one per minute and deletes those older than 30 days. |
| `resync_expiry` | 1 h | 5 min | Marks resync debts older than 7 days as unreachable, so that they are counted as exceptions instead of waited for. |
| `admin_sessions` | 1 h | 6 min | Deletes admin sessions past their idle or absolute lifetime. |
| `storage_metrics` | 5 min | 30 s | Publishes table sizes (`farsight_storage_table_bytes`) and record counts (`farsight_records`). |
| `deferred_retry` | 1 day | 10 min | Re-admits `deferred` lists whose retry time has come (those deferred by the owner re-admission allowance). Lists deferred by a storage gate are re-admitted by the budget monitor. |
| `placeholder_lists` | 1 day | 15 min | Deletes placeholder `lists` rows (record never seen, untracked) that nothing refers to any more, each under its list lock. |
| `rate_tables` | 1 day | 16 min | Deletes `admission_rate`, `intern_rate` and `history_rate` rows older than two days. |
| `orphaned_cursors` | 1 day | 17 min | Deletes listing-cursor rows of runs that are no longer current. |
| `history_retention` | 1 day | 18 min | Deletes history rows older than `storage.block_history_retention`. Does not run with `"0s"`. |
| `counter_recount` | 1 day | 20 min | Recounts the per-list and per-account counters exactly, in batches of 1,000, each list under its list lock. Drift is repaired; where a repair takes a list's listblock count across zero the tracking transition is run. Any drift is recorded as an operational error. |
| `counter_rebuild` | 1 day | 30 min | Rebuilds the approximate counters behind `getStats` and the per-host usage. |

The **counter rebuild** deserves a note, because it touches rows every
writer touches. It first counts the tables, outside any lock; expect
that to take a minute or more on a full index. Only then does it open a
transaction, take `LOCK TABLE stats_counters IN SHARE ROW EXCLUSIVE
MODE` and replace the rows; the same for `host_usage`. The table lock
waits for counter flushes under way and holds new ones back for the
moment the rows are replaced. Replacing the rows under row locks
inside the counting transaction would deadlock with the writers'
flushes on a busy instance.

### Other long-running work in the server

| Task | Behavior |
|---|---|
| Ingest | The Jetstream reader and the single writer ([firehose.md](firehose.md)). |
| Coverage snapshot | Refreshed on `NOTIFY farsight_coverage` and on a timer; an answer's `freshness` is from a snapshot at most 10 seconds old ([coverage.md](coverage.md)). |
| Housekeeping, every 30 s | Reloads the API keys (so a key created or revoked elsewhere takes effect), writes keys' last-used times every minute, drops rate-limit buckets idle for 10 minutes, drops expired sign-in flows, and refreshes the Cloudflare ranges at start and daily when `proxy.cloudflare_refresh` is on. |
| Counter flush | Writes the server's own counter deltas. |
| Handle warming | Verifies handles for accounts that pages showed as bare DIDs, within `public_ui.handle_rps` and always leaving a reserve for page requests. Off with `handle_warming_enabled = false`. |
| Handle pass | With `public_ui.handle_pass_rps` above 0: first the accounts named by a recent identity event, then a walk over every account without a stored answer. After the last account it rests 10 minutes. When more than half of a batch establishes nothing it backs off, from one minute up to 30. |
| Top lists | Looks once a minute. Once a day, for the day that ended at the last 10:00 UTC, counts the lists that `show_top_blockers` and `show_top_blocked` ask for; a switch turned on later gets its lists at the next look. A list's query may run for up to 20 minutes; the all-time "most blocked" list reads all of `blocks`. Trims the log of recent blocks hourly, at 49 hours. Pages read the stored result and never count. |
| Sort-index builder | Builds four indexes that order the UI's tables by shown time, with `CREATE INDEX CONCURRENTLY`, after the server is serving and never in a migration. It does not build while the database plus the index's estimated size would exceed `storage.budget_bytes`, and looks again after 15 minutes; a failed build is retried after 10. Until an index is valid its table lists its rows by account. `farsight_ui_sort_indexes_ready` counts the valid ones. |

Handle verification and the UI tasks are described in
[web-ui.md](web-ui.md).

### Outbound connections

Both processes make many short requests to many hosts. Idle outbound
connections are closed after 10 seconds and at most two are kept per
host, so that a walk over thousands of PDS hosts does not exhaust the
process's open-file limit.

## Health endpoints

| Path | Answer |
|---|---|
| `/health` | `200` if and only if the firehose is connected and `SELECT 1` answers within 1 second; otherwise `503`. In setup mode `503 {"status":"setup"}`. |
| `/livez` | `200 {"status":"ok"}` whenever the process serves HTTP. |

```json
{ "status": "ok",
  "firehose": { "connected": true, "lagSeconds": 1.4 },
  "db": "ok" }
```

`status` is `ok` or `unhealthy`; `db` is `ok` or `error`; `lagSeconds`
is null before the first applied event. Both answers are
`Cache-Control: no-store`.

Use `/health` to *report* and `/livez` to *restart*. A Jetstream
outage makes `/health` fail while the server still serves correct,
if aging, answers, each of them saying so in its `freshness`. An
orchestrator that restarted the server on `/health` would turn an
upstream outage into downtime. The compose file's health check uses
`/health` for status only and restarts nothing.

## Metrics

Each process serves Prometheus text at `/metrics` on its own listener:
the server on `metrics.bind` (port 9464), the backfill process on
`metrics.backfill_bind` (port 9465). In the compose deployment neither
port is published; a scraper joins the compose network. With
`metrics.bearer_token_sha256` set, both require
`Authorization: Bearer <token>`.

Metric names are kept stable on a best-effort basis and are not part
of the API contract.

### Firehose and ingest (server)

| Metric | Type | Labels |
|---|---|---|
| `farsight_firehose_connected` | gauge | `protocol` (`v1`, `v2`) |
| `farsight_firehose_lag_seconds` | gauge | |
| `farsight_firehose_source_lag_seconds` | gauge | |
| `farsight_firehose_events_total` | counter | `collection` (the four NSIDs, and `account`), `op` (`create`, `update`, `delete`; `activate` for `account`; `unknown`), `outcome` (`applied`, `stale`, `refused`, `dropped`) |
| `farsight_firehose_reconnects_total` | counter | `reason` (`connect_error`, `stall`, `closed`, `error`, `server_error`, `kill`, `failover`, `cursor_too_old`) |
| `farsight_firehose_seam_repairs_total` | counter | `trigger` (`resume`, `failover`, `clamp_recovery`) |
| `farsight_firehose_seam_repair_events_total` | counter | |
| `farsight_firehose_open_gaps` | gauge | |
| `farsight_ingest_batch_seconds` | histogram | |
| `farsight_ingest_buffer_depth` | gauge | |
| `farsight_ingest_dropped_total` | counter | `reason` (`invalid`, `foreign_listitem`, `poisoned`) |

### API (server)

| Metric | Type | Labels |
|---|---|---|
| `farsight_query_requests_total` | counter | `endpoint` (the method name without its group, such as `checkBlocks`), `status` |
| `farsight_query_duration_seconds` | histogram | `endpoint` |
| `farsight_rate_limited_total` | counter | `class` (`anon_read`, `key_read`, `admin_backfill`, `key_backfill`, `ui_lookup`, `ui_login`, `ui_login_start`, `public_ui`, `public_ui_handle`, `public_ui_card`, `public_ui_card_budget`) |
| `farsight_coverage_exceptions` | gauge | `kind` (the nine counters of `freshness.coverage.exceptions`, by their field names, such as `unreachableRepos`) |
| `farsight_lists` | gauge | `state` (the nine tracking states of [list-indexing.md](list-indexing.md#list-state)) |

The admin dashboard also shows, per endpoint and since the process
started, the requests, the errors (status 400 and above other than
429), the rate-limited requests (429) and the time of the last
request. These counts are in memory only.

### Storage and history

| Metric | Type | Labels |
|---|---|---|
| `farsight_storage_db_bytes` | gauge | |
| `farsight_storage_budget_ratio` | gauge | |
| `farsight_storage_table_bytes` | gauge | `table`; refreshed every 5 minutes |
| `farsight_records` | gauge | `collection` (`block`, `listblock`, `list`, `listitem`) |
| `farsight_abuse_capped_total` | counter | `kind`: the `[limits]` key of the cap (`blocks_per_author`, `listblocks_per_author`, `lists_per_author`, `listblock_fetch_triggers_per_author`, `host_blocks`, `host_list_items`, `host_listblocks`, `host_lists`, `host_interned_lifetime`), or `admission_rate`, `intern_rate`, `budget`, `ceiling`, `deletes_only`. From both processes. |
| `farsight_block_history_written_total` | counter | `table` (`blocks`, `list_blocks`, `list_items`), `cause` (`delete`, `subject_change`, `refused_update`, `reconcile`, `list_deleted`) |
| `farsight_block_history_skipped_total` | counter | `table`, `reason` (`rate`) |
| `farsight_block_history_pruned_total` | counter | `table` |

### Backfill (backfill process)

| Metric | Type | Labels |
|---|---|---|
| `farsight_backfill_queue_depth` | gauge | `tier` (`1`, `2`, `3`) |
| `farsight_backfill_repos_total` | counter | `tier`, `outcome` (`clean`, `complete_with_debts`, `inactive`, `failed`) |
| `farsight_backfill_repos_per_hour` | gauge | |
| `farsight_backfill_sweep_progress_ratio` | gauge | `cycle_kind` (`full`, `repair`) |
| `farsight_backfill_sweep_eta_seconds` | gauge | |
| `farsight_backfill_pds_request_seconds` | histogram | `host`, `method` (the XRPC method's short name, such as `listRecords`) |
| `farsight_backfill_pds_errors_total` | counter | `host`, `kind` (`cooling`, `rate_limited`, `server`, `client`, `transport`, `decode`) |
| `farsight_backfill_plc_request_seconds` | histogram | |

The `host` label names the 50 hosts with the most requests and puts
the rest under `other`, so it has at most 51 values. Series not
updated for 15 minutes are dropped. Full per-host figures are on the
admin dashboard.

### Web UI (server)

| Metric | Type | Labels |
|---|---|---|
| `farsight_public_ui_requests_total` | counter | `page` (`home`, `search`, `did`, `list`, `card`, `robots`), `status` (`2xx`, `3xx`, `4xx`, `5xx`) |
| `farsight_public_ui_duration_seconds` | histogram | `page` |
| `farsight_public_ui_handle_resolutions_total` | counter | `outcome` (`cached`, `resolved`, `unverified`, `failed`, `skipped`) |
| `farsight_public_ui_withheld_total` | counter | `reason` (`hidden_status`, `operator_excluded`) |
| `farsight_public_ui_cards_total` | counter | `outcome` (`served`, `rate_limited`, `plc_timeout`, `pds_failed`, `avatars_disabled`); admin cards are counted here too |
| `farsight_handle_warming_total` | counter | `outcome` (`resolved`, `unverified`, `failed`, `cached`, `dropped`) |
| `farsight_handle_warming_queue` | gauge | |
| `farsight_handle_pass_total` | counter | `outcome` (`handle`, `gone`, `unresolved`, `unknown`) |
| `farsight_handle_pass_position` | gauge | |
| `farsight_handle_pass_laps_total` | counter | |
| `farsight_ui_sort_indexes_ready` | gauge | 0 to 4 |

## Logs

Both processes write structured JSON to standard output, one object
per line. The default filter is `info,sqlx=warn,hyper=warn`;
`RUST_LOG` replaces it. Requests, ingest batches and backfill jobs
are not logged one by one. At `info` the lines are:

| Process | Lines at `info` |
|---|---|
| Server | Migrations applied; serving; each Jetstream session start; a seam repair scheduled and replayed; one line per periodic task run that did something, with its duration; the top lists counted; shutdown. |
| Backfill | Idle without a configuration; waiting for the schema; running; a sweep or repair cycle started, adopted after a restart, and completed; storage gates changed; configuration reloaded. |

Warnings carry what needs attention: a failed periodic task, a
storage budget ratio of 80% or more when the gates change, counter
drift, a failed configuration reload. Configuration warnings (a
public range in `proxy.trusted`, an admin sign-in that is not
configured) are logged at start, one line each.

Failures worth an operator's attention are also stored, and can be
read with `admin.listErrors` or on the admin UI: failed periodic
tasks, backfill failures with their account and host, counter drift.
