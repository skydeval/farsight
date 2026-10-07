# Web interfaces and first-run setup

The `farsight` server carries three browser surfaces in one binary: a
setup wizard that writes the first configuration, an admin UI for the
one operator account, and an optional public lookup site. This page
describes how they are built, what each one may show, and the rules
that keep them apart. It is a design description; the operator's
how-to is in [Setting up](../guide/setup.md),
[Admin UI and sign-in](../guide/admin-ui.md) and
[Public UI](../guide/public-ui.md).

## Stack

- **Server-rendered HTML.** Pages are askama templates compiled into
  the binary and served by axum handlers. There is no build step, no
  bundler and no CDN: the stylesheets, the scripts, the icon and the
  link-preview image are embedded in the binary and served from
  `/static/`.
- **Two stylesheets, two scripts.** `farsight.css` and `admin.js` belong
  to the admin pages, the sign-in page and the wizard; `public.css` and
  `public.js` belong to the public pages. A vendored `htmx.min.js`
  drives the two polled fragments of the admin UI (the dashboard and
  the alerts). Public pages use none of its attributes: their page
  turns and re-reads are done by `public.js`.
- **No inline script or style.** No page carries an inline script, a
  style attribute or a style element: every script and stylesheet is a
  static file, and htmx is configured not to inject its own styles or
  evaluate scripts. Every page is sent with a
  `Content-Security-Policy` that enforces this. The public pages'
  policy:

  ```
  default-src 'none'; style-src 'self'; script-src 'self';
  img-src 'self'; connect-src 'self'; base-uri 'none';
  form-action 'self'; frame-ancestors 'none'
  ```

  With `public_ui.show_avatars` on, `img-src` is `'self' https:`,
  because avatars are loaded by the visitor's browser from other
  origins (see [Profile cards and avatars](#profile-cards-and-avatars)).
  Public responses also carry `X-Content-Type-Options: nosniff` and
  `Referrer-Policy: same-origin`.

  The admin pages, the setup wizard and the sign-in page are sent with
  the same policy with `img-src 'self' https:` always (their profile
  cards show avatars whatever the public setting). The sign-in page's
  differs in one directive, `form-action 'self' https:` (plus plain
  `http` to the hosts in `net.allow_http_hosts`): its form is
  answered with a redirect to the account's own authorization server,
  and browsers apply `form-action` to that redirect.
- **Pages work without JavaScript.** Tabs, page controls, filter boxes
  and switches are links and forms. Script adds the theme toggle, local
  times, profile cards, in-place page turns and the re-read of a page
  that is waiting for handles.
- **Assets are fingerprinted.** Every page names its assets as
  `/static/<file>?v=<fingerprint>`, a short hash of the embedded files.
  Assets are `public, max-age=3600`; a new build's pages ask for them
  under a new address, so a change shows on the next page view. The
  bare path serves the same file. Assets have no gate and no rate
  class in any configuration.

## Modes of the server

The process is in one of two modes, chosen at start from the config
file (`/etc/farsight/config.toml`) and the environment.

| Condition at start | Result |
|---|---|
| No config file, `FARSIGHT_SKIP_WIZARD` unset | Setup mode |
| Config file present and valid | Normal mode |
| Config file present but unparseable or invalid | Exit non-zero with the error. The file is never rewritten and setup mode is never entered. |
| `FARSIGHT_SKIP_WIZARD=1`, no config file | Normal mode with a config built from `FARSIGHT__<SECTION>__<KEY>` variables; exit non-zero, naming the missing keys, if a required key is absent. |

**Setup mode** serves `/setup/*`, the static assets, `/livez`, and
`/health` (503 with `{"status":"setup"}`). `/` redirects to `/setup`.
Every `/xrpc/*` request answers `503 SetupRequired`. There is no
firehose connection and no database connection, except the storage
step's test.

**Normal mode** serves the API, the health endpoints and the web
router. The web router is unconditional: every route is always mounted
and each handler reads the config in force for the request. A surface
that is switched off answers the bare `404` of an unknown path
(`no-store`), so a disabled feature's response does not reveal what
the instance could serve if configured otherwise. `/setup/*` is an
unknown path in normal mode.

What `/` is depends on the two switches, decided per request:

| `access.public_ui` | `access.admin_ui` | `GET /` |
|---|---|---|
| on | any | The public home page |
| off | on | `303` to `/admin`, `no-store` (not permanent: turning the public UI on changes what `/` is) |
| off | off | A short `text/plain` page naming Farsight, so that an API-only instance does not look broken |

The two transitions between modes happen **in-process**, independent
of any container restart policy: when the wizard finishes, the setup
listener is shut down and the process loads the new config, runs
migrations and starts ingest; a config reset from the admin UI does
the reverse.

## First run

### The setup token

The wizard is reachable by anyone who can reach the port, so it is
gated by a token that only someone with access to the container's log
or shell can read.

- **Form.** 128 bits from the OS CSPRNG, shown as
  `fst-XXXXX-XXXXX-XXXXX-XXXXX-XXXXXX` in Crockford base32. Comparison
  ignores case, hyphens, whitespace and the prefix, and maps the
  look-alikes `O`→`0` and `I`/`L`→`1`.
- **Storage.** `.setup-token` beside the config file, mode 0600: the
  token and its creation time. It is plaintext so that it can be
  printed again; whoever can read the config volume already controls
  the deployment.
- **Printing.** Logged at WARN on every setup-mode start and every 10
  minutes while in setup mode. It is the only secret Farsight logs.
  `farsight setup-token` prints it on demand and `--rotate` replaces
  it.
- **Lifetime.** 24 hours. It survives a restart while unexpired. On
  expiry (checked at start and every minute) a new token is generated,
  written and printed, and every setup session ends. Rotation is
  postponed while a verified setup session was active in the last
  hour, up to 72 hours after creation at most. The wizard warns one
  hour before expiry.
- **Use.** A correct token creates a setup session: a 256-bit id in
  the cookie `farsight_setup` (`Path=/setup`, `HttpOnly`,
  `SameSite=Strict`, host-only), stored server-side by its SHA-256.
  The token stays valid until setup completes, so a lost browser
  session can enter again. On completion the token file is deleted.
- **Guessing.** The submitted token is always checked first, in
  constant time, and **a correct token is never refused by any
  limiter**. Limits apply to failures only: after 5 failures a minute
  from one client address, that client's further failures are answered
  after a 2-second delay and logged by sample; at most 64 delayed
  responses are held at once, and beyond that a failure gets an
  immediate `429`, so the delay cannot exhaust sockets. There is no
  global lockout. With 128 bits, guessing is infeasible; the limits
  only keep the log quiet. Behind a proxy that is not configured yet,
  all clients share one address, which costs the operator nothing
  because a correct token bypasses the limiter.
- **Binding.** `FARSIGHT_SETUP_BIND` restricts the setup listener to
  an address, for setup through an SSH tunnel.

### The wizard

The wizard's answers are held server-side, per setup session, until
the final write. Each step validates on submit; a step can be opened
only when every step before it is done, and done steps can be
revisited until the end. Every form carries the session's CSRF token.

| Step | What it collects | What it validates |
|---|---|---|
| Setup token | The token | Nothing else renders until it verifies |
| Welcome | — | — |
| Public identity | `server.hostname`, `server.contact` | A hostname without scheme or path; a non-empty contact of at most 300 characters |
| Firehose source | `firehose.urls`, one per line | At least one `ws://` or `wss://` URL. "Test connection" subscribes for at most 10 seconds in total across the instances and reports events, lag and whether v2 is offered; with no v2 instance the step warns that coverage stays `partial` |
| Backfill | Sweep on or off and its source, `backfill.per_host_rps`, `backfill.concurrency`, the repos-per-hour cap, `backfill.plc_url` and whether to seed from its export, the disk available to Postgres in GB, an optional backlink source URL | Ranges and URL schemes. `storage.budget_bytes` is set to 70% of the disk entered. With the sweep on and less than 150 GB, a warning states the budget, where the sweep pauses and the projected runway |
| Access | `access.reads`; two boxes, "Enable public UI" and "Enable admin UI", **both unticked**; the admin DID; the admin token | See below |
| Reverse proxy | None, Cloudflare (bundled ranges), a tunnel or local proxy (its CIDR), or custom CIDRs | CIDR syntax. Trusting public address space outside the bundled Cloudflare ranges needs an explicit acknowledgement. A preview shows the current request's peer, its forwarding headers and the client address that would be resolved |
| Storage | `storage.database_url`, prefilled from the environment | "Test" must pass: connect within 10 seconds, PostgreSQL 15 or newer, and the database either empty or holding a Farsight schema no newer than this build |
| Review | — | Shows the whole config with secrets redacted |
| Done | — | Writes the config |

The default firehose source is Bluesky's two public v2 Jetstream
instances, `wss://jetstream.us-east.bsky.network` then
`wss://jetstream.us-west.bsky.network`, in failover order.

**The Access step** carries three decisions.

- *The admin token* is generated once per setup session and shown
  until the operator ticks "I have saved this"; the step does not
  advance without it. Only its SHA-256 is written to the config.
- *The admin DID* is asked for when the admin box is ticked. It must
  be a `did:plc:` or `did:web:` DID, not a handle. The first submit
  resolves it through the safe outbound client and shows its handle
  and host; a second submit confirms it. A DID that does not resolve
  advances only with "Use this DID anyway". The wizard shows what the
  DID resolves to; it cannot prove the operator controls it. With the
  box unticked the field is hidden by a stylesheet rule (no script)
  and whatever it holds is discarded by the server.
- *The public UI* needs `access.reads = "public"`; ticking it with
  another read mode is an error on the step. When ticked, the step
  answers with a confirmation page that lists what becomes reachable
  without login, and the step is not done until the operator confirms
  with a second request. Without that confirmation the config is
  written with the public UI off.

### Writing the config

"Done" requires every step to be validated. The config is serialized,
loaded once more through the normal loader (a config the server would
refuse at start is refused here), and written with mode 0600 by a
create-if-absent operation, so **the first writer wins**: two setup
sessions racing to finish cannot both succeed, and the loser is told
that another session completed setup.

On success the token file is deleted, all setup sessions are dropped,
the setup cookie is cleared and the process switches to normal mode.
The final page says what exists now: with the admin UI, `/admin`, the
sign-in at `/enter`, the admin account, and the loopback instructions
when the hostname cannot be an OAuth client; with the public UI only,
`/`; with neither, that the instance is API-only.

A **config reset** (`/admin/reset`, confirmed by typing the hostname)
is the way back: it deletes all admin sessions, revokes all API keys,
deletes the config file (and with it the admin token), writes and
prints a new setup token, notifies the backfill process and switches
to setup mode. Database contents are kept, and the wizard's storage
step recognizes them. Reset is unavailable when the config comes from
the environment.

## Admin sign-in

### One admin, one DID

The admin is one ATProto account, named by `access.admin_did`. There is
no user table and no credential of Farsight's own. Whoever controls that
account (its server, its credentials there, its PLC rotation keys or
`did:web` domain) controls the admin UI.

### The OAuth flow

`/enter` signs the admin in with ATProto OAuth: discovery of the
account's authorization server from its DID document, a pushed
authorization request, PKCE (S256) and DPoP (ES256, one P-256 key per
flow, never stored). Farsight asks for the scope `atproto` only, which
authenticates the account and grants no access to its repository.

1. `POST /enter` checks same-origin, rate limits (5 starts a minute
   per address; one process-wide bucket for addresses that have not
   signed in recently, so that anonymous callers cannot make Farsight
   send unbounded requests and cannot keep the admin out), starts the
   flow, and redirects to the authorization server. The flow is kept
   in memory for 10 minutes; at most 256 flows are held, the oldest
   evicted.
2. `GET /enter/callback` runs its checks in a fixed order: rate limit;
   flow cookie and `state` present; the flow exists and is young
   enough; the cookie is the one the flow was started with (otherwise
   the flow is kept for its real owner); the flow is then removed, so
   its `state` is spent whatever follows; `iss` equals the flow's
   issuer; `code` present and no `error`; the token request; `sub`.
   Every refusal looks the same to the browser.
3. The `sub` of the token response must be the DID the flow was
   started for, and that must still be the configured admin DID. The
   tokens are discarded as soon as `sub` is read: Farsight never calls
   the account's server on the admin's behalf.
4. The callback answers `200` with a page that starts a same-site
   navigation to `/admin`, not a redirect: it ends a cross-site
   redirect chain, and a `SameSite=Strict` cookie set on it would not
   be sent on a redirect that continued the chain.

All outbound requests of the flow go through the safe client, which
refuses private, loopback and link-local addresses; see
[Security](security.md#the-safe-outbound-client).

### Two client modes

The OAuth client identity is chosen per request, from the `Host`
header and, for loopback mode, the client's address.

| Mode | When | `client_id` | `redirect_uri` |
|---|---|---|---|
| Hostname | `Host` names `server.hostname`, and that is a domain name (no port, no IP address, no single label) | `https://<hostname>/.well-known/atproto-oauth-client-metadata` | `https://<hostname>/enter/callback` |
| Loopback | `Host` is `127.0.0.1` or `[::1]`, any port, **and** the client is local | `http://localhost?redirect_uri=…&scope=atproto` | `http://<host>/enter/callback` |

A client is **local** when its address is loopback or private
(RFC 1918, link-local, unique local). The address is the resolved
client address that rate limits use
([security.md](security.md#client-address)): the TCP peer, or, when
the peer is a trusted proxy, the address the proxy forwarded. Private
addresses count because a container runtime delivers a connection
made to a published port on the host, which is what a browser on the
machine and an SSH tunnel make, from its bridge's gateway and not from
`127.0.0.1`. A request with `Host: 127.0.0.1` from a public address
gets the page that says where to sign in, and `POST /enter` answers
it `400` before any rate limit is charged or any request is sent.

Two deployments make every client look local, and there the rule adds
nothing: a reverse proxy on the same machine that is not listed in
`proxy.trusted` (or that sends no client address), and a container
runtime that hides peer addresses (rootless Docker, Docker Desktop).
In the first case list the proxy in `proxy.trusted`; in the second
publish the port on loopback (`FARSIGHT_PORT=127.0.0.1:8080`) or put a
trusted proxy in front.

In hostname mode the authorization server fetches the client metadata
document, which Farsight serves only on the hostname it describes
(`404` otherwise, and with the admin UI off). Loopback mode needs
nothing reachable from outside and is how an instance on an IP
address, a port or a private network is administered: on the machine
itself or through an SSH tunnel. `localhost` is not accepted as a
`Host`, because the callback arrives on the address, which is a
different cookie host. Any other `Host`, and a loopback `Host` from a
client that is not local, gets the sign-in page with a `400` and no
flow.

### Sessions

A successful sign-in creates a row in `admin_sessions` and sets the
cookie `farsight_admin` (`Path=/`, `HttpOnly`, `SameSite=Strict`,
host-only, 256 random bits). A session ends after **12 hours idle** or
**7 days** in total, whichever comes first.

The stored key of a session is `SHA-256(cookie ‖ 0x00 ‖ admin DID)`.
A session is therefore found only while the DID it was created for is
the configured one: changing `access.admin_did` ends every session of
the previous account without touching the table. Rotating the admin
token deletes all sessions, as does a config reset.

Sign-in has three states:

| State | `/enter` |
|---|---|
| Admin UI off (`access.admin_ui = false`) | `404`; nothing under `/admin` or `/enter` exists and no session is looked up |
| DID configured | The sign-in page; a signed-in admin is sent to `/admin` |
| No DID | A page saying that no admin account is set; the process runs and the API serves |

### Access rule

Every `/admin…` route needs a valid session. Without one:

- a navigation gets `303` to `/enter`;
- a request sent by htmx (`HX-Request`: the dashboard poll, the alerts
  poll, the history tables' "Next") gets the bare `404`, which leaves
  the page it came from as it is, because htmx would follow a redirect
  and swap the sign-in page into the fragment;
- `/admin/card/{did}` gets the bare `404` for the same reason.

Every admin response is `no-store, private`.

### CSRF

Every state-changing request must pass two checks:

1. **Same origin.** When the browser sends `Sec-Fetch-Site`, it must
   be `same-origin`; when it sends `Origin`, its host and port must
   equal `Host`. A request carrying neither header passes this check.
2. **The session's token.** Each session (setup or admin) has its own
   random CSRF token, embedded in every form and compared in constant
   time.

A failure is `403`.

Two requests have no session yet and pass the first check alone:
`POST /enter` (it starts a sign-in) and `POST /setup` (it presents
the setup token and creates the setup session). Every other `POST`
passes both, `POST /admin/logout` included: a logout without the
token is refused, the session goes on and its cookie is left alone.
Without a session, `POST /admin/logout` answers with the redirect to
`/enter` and changes nothing.

### Changing the admin DID

The admin DID is changed from the command line, never from the UI:

```sh
farsight admin-did                 # prints the effective DID and its source
farsight set-admin-did <did> [--force]
```

`set-admin-did` validates the DID's syntax, checks that the edited
file still loads, and resolves the DID before writing (a typo is
cheaper to catch there); `--force` writes a DID that does not resolve,
for a recovery while a directory is down. It refuses when
`FARSIGHT__ACCESS__ADMIN_DID` is set, since the environment overrides
the file. It edits `config.toml` only; the change applies at the next
start.

Two config keys are **applied at start only**: `access.admin_did` and
`access.admin_ui`. Every in-process config edit (the Settings editor,
the Public UI form, the admin API methods that edit the config) reads
the file, changes it and installs the result, and an edit whose result
differs from the running config in either key is refused and nothing
is written. So Settings cannot remove the page it is on, and a hand
edit waiting for a restart does not go live through an unrelated save,
at the cost that no in-process edit succeeds until that restart.

### Proxy-safety rules

Farsight is expected to run behind a reverse proxy it does not
control. These rules hold in every mode.

- **Relative redirects only.** A `Location` is a path on this host,
  never an absolute URL, and Farsight never redirects HTTP to HTTPS
  itself. A `Location` is never built from the request's raw path,
  which could name another host.
- **Cookies** are host-only (no `Domain`), `Path`-scoped, `HttpOnly`
  and `SameSite=Strict`. The one exception is `farsight_flow`
  (`Path=/enter`, `Max-Age=600`), which is `SameSite=Lax` because the
  OAuth callback is a cross-site top-level navigation and must carry
  it. It authorizes nothing by itself: it ties a callback to the
  browser that started the flow.
- **`Secure`** is set on a cookie when the request arrived with
  `X-Forwarded-Proto: https` from a trusted proxy. Farsight does not
  terminate TLS itself, so that header is its only evidence of HTTPS.
  In setup mode, where no proxy is trusted yet, the header is
  honoured from any peer for this flag only (a forged value affects
  only the forger's own cookie) and never for the client address.
- **Absolute URLs come from the config.** The OAuth client id, the
  redirect URI and the link-preview tags are built from
  `server.hostname`, never from a request header.
- **Public responses do not depend on the caller.** A public page is a
  function of the path, the query string and the instance's state: no
  cookie is read or set and no request header changes the body. That
  is what makes `Cache-Control: public` safe.

A proxy may restrict who reaches `/admin*` and `/enter*`; it cannot
rename them.

## The admin UI

### Separate from the public surface

The admin pages and the public pages are **separate copies of their
surface**: templates, stylesheet and script are each side's own, so
that either can change without the other, and so that the public
surface can be frozen while the admin one moves. An admin page may
look like its public counterpart; it shares none of its markup. What
the two share is below the surface: the row queries, the page
arithmetic, the handle cache and warming queue, and the profile-card
fetch.

The admin tables do not apply the public display rules. They show
accounts in every status, record addresses, coverage in full, and
removed records.

### Routes

| Route | Serves |
|---|---|
| `GET /admin` | Dashboard |
| `GET /admin/dashboard/fragment` | The dashboard's poll, every 10 s |
| `GET /admin/alerts` | The alerts drop-down's content, every 30 s |
| `GET /admin/lookup/did`, `/admin/lookup/list` | Lookups |
| `GET /admin/did/{did}/history`, `/admin/list/{did}/{rkey}/history` | Removed records ([History](history.md#where-history-is-shown)) |
| `GET /admin/ops`, `POST /admin/ops/{action}` | Operations |
| `GET, POST /admin/settings`; `POST /admin/settings/public-ui`, `…/public-ui/confirm`, `…/token` | Settings |
| `GET, POST /admin/reset` | Config reset |
| `GET /admin/card/{did}` | Profile-card fragment for admin tables |
| `POST /admin/logout` | Ends the session (same-origin and the form token, like every other `POST`) |
| `GET, POST /enter`, `GET /enter/callback`, `GET /.well-known/atproto-oauth-client-metadata` | Sign-in |

### Dashboard

The dashboard is blocks of label-and-value rows, replaced as a whole
by the 10-second poll.

- **Catching up**, at the top, lists background work that has an end,
  each with its share done and, where a rate is known, the time left:
  the history sweep; the handle pass (the walk's position over the
  newest account id, at the configured rate, which is an upper bound
  because answered accounts are passed over); and a gap repair under
  way (accounts re-read so far). A line disappears when its work is done,
  and the block disappears with its last line.
- **Index**: blocks, listblocks, lists, tracked lists, list items,
  accounts known.
- **Firehose**: connected, protocol, lag, source lag, gaps to repair,
  gaps still open, and the unhealed gaps themselves.
- **Backfill**: the sweep's cycle, source, state, progress and ETA;
  queue depth per tier (on-demand, active, sweep); repos per hour.
- **Storage**: database size, budget, share of budget, hard ceiling,
  and the size of the history tables.
- **Exceptions**: the coverage exception counts, as `getStats` reports
  them.
- **Lists by state**: one count per list state
  ([List indexing](list-indexing.md#list-state)).
- **API usage**: one row per API endpoint with requests, errors
  (status 400 and above other than 429), rate-limited (429) and the
  time of the last call, counted in memory since the server started.
  Every endpoint is listed; one never called is set back visually. The
  admin pages' own reads are not counted.
- **Host buckets**: the ten host buckets with the most lifetime
  interning, with what each has stored and whether it is capped
  ([Security](security.md#cap-buckets)).

All counts carry thousands separators.

### Alerts

The warnings and the coverage sentence are not on the dashboard but in
a drop-down in the bar of **every** admin page, loaded on page load
and every 30 seconds, with the number of warnings on its button. An
operator on the lookup pages sees a full storage budget as soon as
one on the dashboard does.

Warnings cover: the storage budget (approaching, sweep paused,
refusing, critical) and sustained growth; Cloudflare traffic while
Cloudflare is not trusted; a disconnected firehose; a v1 firehose;
unhealed gaps; sort indexes not yet built or held for lack of budget.

Unhealed firehose gaps are of **three kinds**, and the alerts word
each differently because each calls for a different action:

| Kind | How it is recognized | What the alert says |
|---|---|---|
| The open interval of a v1 firehose | Open, cause `SyncUnavailable` | Part of the v1 warning: the time on v1 is one gap that stays open, and cannot be repaired, until a v2 Jetstream takes over |
| Open because the firehose is down | Open, any other cause | "Still open": it closes when the firehose reconnects and can be repaired after that |
| Closed | Has an end | "Can be repaired" until a repair cycle is under way (and whether one starts by itself, per `backfill.repair.auto_start`); then "being repaired" with the cycle's id and the accounts re-read so far; or "paused" when `backfill.repair.paused` is set |

### DID lookup

Input is a DID or a handle; a handle is resolved through the safe
client. The page has the account's header (avatar, handle, DID,
creation date and host from the profile card) with the account's
backfill state at the right, then tabs:

| Tab | Rows |
|---|---|
| Incoming blocks | Blocker, record, created |
| Incoming listblocks | List, purpose, blocker, created |
| Lists | The listblocked lists naming the account: list, purpose, owner, listblock count, added |
| Blocking lists | The lists the account subscribes to as block lists, among lists this instance serves, with the listblock record. Always shown here, whatever `public_ui.show_outgoing_blocks` says |
| History | Handle history and host history from the PLC audit log; removed blocks and removed list memberships with created, first and last seen, removed, and how each ended. Read only when the tab is asked for |

Tables hold 50 rows with numbered page controls and exact count
headings; the blocks, lists and blocking-lists tables have a filter box.
A table states its coverage in words. Each record has a button that
copies its `at://` address, and a link when
`public_ui.record_viewer_url` is set (never for a removed record, which
no viewer can show).

### List lookup

Input is an AT-URI or a bsky.app list URL. The page shows the list's
facts (owner, purpose, name, state, description), then the tabs
"Members" and "Subscribers", built like the DID lookup's tables, and a
link to the list's removed-membership history.

### Operations

Forms that call the same code paths as the admin API
([API](api.md#admin-procedures)): enqueue a backfill for an account
(priority, force); restart the firehose; pause or resume the sweep;
create and revoke API keys (a new key is shown once); and the recent
error log.

Gap repair has four controls, backed by `[backfill.repair]`
(`auto_start`, default `true`; `paused`, default `false`):

| Control | Effect |
|---|---|
| Start repair | Starts a repair cycle for every closed, unclaimed gap. Asking while one runs says so and joins it |
| Pause / Resume repair | Sets `paused`. A paused repair enumerates nothing and dispatches none of its members; it keeps its place |
| Cancel repair | First sets `auto_start = false` (or the same repair would begin again), then deletes the open repair cycle and releases its gaps unhealed. With a config it cannot write, it cancels nothing |
| Turn automatic repairs off / on | Sets `auto_start` |

With a config managed through the environment, the three controls that
write the config are replaced by a note naming the variables. The repair
mechanism itself is described in [Backfill](backfill.md#gap-repair).

### Settings

- **The config editor** shows `config.toml` with secrets redacted; a
  redacted value left in place is kept. Keys set by environment
  variables are listed as locked. A save is validated by the loader,
  written atomically and announced with `NOTIFY farsight_config`, on
  which the backfill process reloads. These keys apply immediately:
  everything under `access`, `public_ui`, `auth`, `proxy` and
  `backfill`; `server.contact`; and of `rate_limit` the keys
  `anon_rps`, `anon_burst`, `key_rps`, `key_burst`,
  `admin_backfill_rps`, `key_backfill_rps`, `ui_lookup_rps` and
  `query_timeout`. Any other changed key is named on the page as
  needing a restart. (`access.admin_did` and `access.admin_ui` are
  refused before that, as described above.)
- **The Public UI form** exposes the public toggle and every
  `[public_ui]` key; all apply on save. Turning the toggle on answers
  with the confirmation page (below).
- **Admin token rotation** shows the new token once and signs every
  session out.
- The admin DID is shown read-only, and there is no control for
  `access.admin_ui`; the page says where each is changed.

## The public UI

### Off by default, confirmed before it is on

`access.public_ui` is `false` unless the operator turns it on, and it
requires `access.reads = "public"`: a public site in front of a gated
API is not a supported combination. It does not depend on the admin
UI.

Turning it on, in the wizard or in Settings, is never a single
request. The first request renders a page listing what becomes
reachable without login for this configuration: the hostname; when
the index was last updated; incoming blocks for any account; the
lists naming any account; list memberships; outgoing blocks if
`show_outgoing_blocks` is on; profile cards and where visitors'
browsers will fetch avatars from; the top lists if their switches are
on. It also states what stays private: removed records. Only a second
request writes the change. In Settings that request must carry the
confirmation token of the first and the session's CSRF token, within
10 minutes.

### Routes

| Route | Serves | Rate class |
|---|---|---|
| `GET /` | Home | `public_ui` |
| `GET /search?q=` | Resolves a handle, DID, `at://` URI or bsky.app link and redirects to its page | `ui_lookup` |
| `GET /did/{did}` | Account page | `public_ui` |
| `GET /list/{did}/{rkey}` | List page | `public_ui` |
| `GET /card/{did}` | Profile-card fragment, fetched by script | `public_ui_card` |
| `GET /robots.txt` | Served in every normal-mode configuration | — |

Page views are limited per client address
(`public_ui.rate_limit_rps` / `rate_limit_burst`) and renders are
bounded process-wide (`public_ui.query_concurrency`; a render that
cannot get a slot within 2 seconds is a `503` with `Retry-After`).

An unknown root path is the bare `404` also while the public UI is on;
the public not-found page is for public routes whose subject does not
exist. `robots.txt` disallows everything unless the public UI is on
and `public_ui.crawlable` is set; then it closes `/admin`, `/enter`,
`/setup`, `/xrpc/`, `/search`, `/card/` and the health endpoints and
allows the rest. Pages that are not offered to crawlers also carry
`X-Robots-Tag: noindex, nofollow`; search, cards and every error page
always do.

### Rules every public handler keeps

- **Off means absent.** With the toggle off, a public route answers
  like any unknown route.
- **No way out.** A public page links only to public pages. It carries
  no login link and names no admin route.
- **No writes.** A public page view never interns a row and never
  enqueues backfill work. Its only side effects are handle and avatar
  cache entries and requests to the warming queue.
- **One withheld rule.** An account in a hidden status, or in
  `public_ui.excluded_dids`, has no page, and the notice is the same
  for both; the page never says which.

### What a page shows and never shows

- **No coverage detail.** A public page prints no coverage level,
  reason or exception. It says "None on record at this instance" for
  an empty section, says when a list is not indexed, and states "Last
  updated" once, in its footer. Coverage is a contract between the API
  and an integrator ([Coverage](coverage.md)); a visitor cannot act on
  it, and a partial statement beside a number reads as a claim about
  the account.
- **No record addresses.** Public tables have no record column and
  show no `at://` URI of a block, listblock or list item.
- **No one-click copy of identifiers.** No button on a public page
  copies a DID, a handle or a URI.
- **Removed records are never public.** Blocks and list memberships that
  were stored and later removed are admin pages only
  ([History](history.md#where-history-is-shown)), and the public toggle
  does not change that.
- **No data in link previews.** OpenGraph tags carry a title, a fixed
  description and one static image. Platforms keep previews for days,
  and a count in a preview is a stale claim with no date.
- **No account in the browser tab.** Every page is titled "Farsight"
  with one icon; the title names no account or list.
- **Mute subscriptions** to a list are not public on the network and
  are never shown.
- **`public_ui.contact`** is shown on no public page.

### The display rule for inactive accounts

Rows of every public table, the top lists included, follow one rule
by the status of the account in the row:

| Status | In a row | Own page |
|---|---|---|
| Active | Shown | Shown |
| Suspended by its host | Shown, tagged "suspended" | Withheld |
| Taken down by its host | Left out unless the table's switch is on (`?takendown=1`); then shown, tagged "taken down" | Withheld |
| Deactivated, deleted | Never shown | Withheld |
| In `excluded_dids` | Never shown | Withheld |

Where the switch exists, the heading states both counts ("4,114
(6,234 counting taken down accounts)"). The rule is a property of the
public pages only: the API's hidden set and the admin tables are
unchanged by it.

### Tables

- **Tabs.** A page shows one table at a time, chosen by `?tab=`. An
  account page has "Blocked By", "Blocked By Lists", then "Blocking"
  and "Blocking Lists" with `show_outgoing_blocks`, then "History". A
  list page has "Members" (when the list's state has members to show)
  and "Subscribers", the accounts that subscribe to it as a block
  list. The first tab has no parameter.
- **Pages by number.** Every table holds 50 rows a page; there is no
  `limit` parameter. The page is a query parameter of its table, so
  the tables of a page turn independently: `page`, `lists`, `out`,
  `outlists` on an account page; `page`, `subscribers` on a list page.
  Page 1 is the address without the parameter. A value that is not an
  integer from 1 to 1,000,000 gets a `400` page with a first-page
  link; a page past a table's end is answered with its last page.
  Controls sit above and below the table and are plain links; the
  script replaces the table in place and updates the address, and
  hides the outermost page numbers until the row fits the screen.
- **Count headings.** A table's heading is its count, with the page's
  filters applied, exact up to 5,000,000 (`COUNT_CAP`); beyond that
  it reads "more than 5,000,000" and the controls end in a gap and a
  next arrow, which works for as long as rows follow, so every row
  stays reachable. If a count cannot be read in time, the heading
  shows a dash and the controls show only what the rows prove.
- **Order.** Rows are newest first by the shown time, once the
  table's sort index is built
  ([Storage](storage.md#the-sort-indexes)).
- **Filter box.** Each table of an account page has a box
  (`?find=`, at most 100 characters) that filters it across all its
  pages by DID, or by part of a handle among the handles this
  instance has verified. Submitting a whole handle resolves it, from
  the handle budget, and finds the account either way.
- **Layout.** Tables are frameless, with a hairline between rows; a
  row is one line.

### The History tab

The History tab of an account page lists the handles and the hosts the
account has had, in two tables side by side, as the PLC directory's
audit log records them. The handles are what the account claimed at
the time and are not verified. The log is read only when the tab is
opened: one request to the PLC directory, under the profile-card
budget. It is unrelated to the removed-records history, which is not
public.

### The page header

An account page's header shows the verified handle and the DID from the
server, and leaves a slot that the script fills from the account's
profile card: the avatar, the creation date with the account's age, and
its host. A list page's header shows the list's name, its `at://`
address, purpose, state, owner, its description (plain text, at most 300
characters, no links) and its image, under the same avatar rules.

`lists.description` and `lists.avatar_cid` are written when the list
record is applied and cleared when it is deleted. The page shows what
is stored and reads no record.

### The home page

The home page has the heading "Farsight", the operator's
`public_ui.instance_description` (plain text; a default when empty),
the search box, and three totals from `getStats`: blocks indexed,
lists tracked, accounts seen. A guide, "How to read a page", is
collapsed under its heading; the other public pages have the same
guide behind a button in the bar, which otherwise holds the icon, the
search form and the theme toggle.

A home page left open keeps itself current. Once a minute, while the
tab is visible, the script reads the page again and replaces the
totals and the "Last updated" line together, so the two never
disagree. Without JavaScript the page changes on a reload.

**Top lists** are optional. Two switches, each off by default, add
them:

| Key | List | Counts |
|---|---|---|
| `public_ui.show_top_blockers` | "Top blockers" | Blocks an account has made |
| `public_ui.show_top_blocked` | "Most blocked" | Accounts that block an account directly; blocks through lists are not counted |

They appear on two tabs, "Last 24H" and "All Time"
(`/?tab=alltime`), and on each tab "Top blockers" sits beside "Most
blocked". A list shows 20 accounts: ten, and ten more behind "Show
more" (a checkbox and a label, so it works without script).

None of the four lists can be counted while a page is served: ranking
every subject of `blocks` reads the whole table. So the page never
counts.

- A **background task** in the server counts the lists whose switch is
  on **once a day, for the day that ended at the last 10:00 UTC**, and
  stores each as one row of `top_lists` (`kind`, `computed_at`, the
  rows as JSON). The task looks once a minute; a list whose stored
  day is not the current one is counted, so a switch turned on during
  the day gets its lists at the next look. Each count runs with a
  20-minute statement timeout. The all-time "Most blocked" reads all
  of `blocks`; expect it to take minutes on an index of more than a
  hundred million blocks.
- The **day's lists** are counted from `block_recent`, a log the apply
  path appends to when it inserts a block whose own creation date lies
  less than 24 hours before, or at most five minutes after, its
  arrival (a block without a creation date is not logged). History
  read by the backfill is therefore not recent. A log row counts only
  while its block is still stored, so a block made and removed again
  does not. The log is trimmed at 49 hours.
- Each list is stored with 60 accounts, so that the page can apply
  its display rule (leaving out excluded, deactivated, deleted and
  taken-down accounts) and still show 20.
- An account in a top list whose handle has not been checked yet is
  shown as its DID; rows of a ranking are not held back.

Emptying either table is safe: the lists are counted again.

### Caching

Public pages are safe to cache at an edge because they do not depend
on the caller.

| Response | `Cache-Control` |
|---|---|
| Account or list page, complete | `public, max-age=30` |
| Home page, complete | `public, max-age=60` |
| A page that held rows back, or a top list still waiting for handles | `no-store` |
| A complete profile card | `public, max-age=300` |
| Every 4xx and 5xx, every redirect to a public page | `no-store` |
| `robots.txt`, the API-only text at `/` | `public, max-age=300` |
| Assets | `public, max-age=3600` |

A page that is waiting for handles is `no-store` because it is about
to change: its script re-reads the same address until the missing
rows are in, and a cached copy, in the browser or at an edge, would
hand the same incomplete page back to it and to every other visitor
for the cache's lifetime.

## Handles

### Verification

A handle is shown only when it has been verified **in both
directions**:

1. read the DID document (`did:plc` from `backfill.plc_url`, `did:web`
   from its host) through the safe client;
2. take the first `at://` entry of `alsoKnownAs`;
3. resolve that handle forward (DNS TXT `_atproto.<handle>`, then
   `https://<handle>/.well-known/atproto-did`) and require the result
   to equal the DID.

A handle that fails step 3 is not shown, on any page. Rendering a row
never waits for an outbound request. A page verifies at most one
handle inline (its own subject, or a list's owner), waiting at most 2
seconds.

### `handle_cache`

The answer of a check is kept in two layers.

- **Stored:** table `handle_cache`, one row per DID.

  ```sql
  CREATE TABLE handle_cache (
    did         TEXT COLLATE "C" PRIMARY KEY,
    handle      TEXT NOT NULL,          -- '' = checked, no handle to show
    resolved_at TIMESTAMPTZ NOT NULL
  );
  ```

  There is no history of earlier handles and no size limit.
  `DELETE FROM handle_cache` is safe: handles are verified again.
- **In memory:** a bounded LRU in front of it. An entry lives
  `public_ui.handle_cache_ttl` (default 1 hour; a negative answer 10
  minutes) and is refilled from the table, not from the network.

The read path is the same for public and admin pages: memory; on a
miss the table, read once per page for all its accounts; on a miss in
both, the account has no answer and is queued for warming. A stored
verified handle older than 7 days is shown as it is and queued for
one background verification; a stored "no handle" is re-checked after
1 hour. A re-check on view that fails keeps a stored verified handle.

### Hold until verified

A public row is shown only once its account's handle check **has an
answer**: a verified handle, or "none", in which case the row shows
the DID. A row without an answer is left out of the table. The page
states how many rows it held back in a line marked `data-pending`,
and its script re-reads the page until they are in (at most 20
times), swapping in each table as it completes.

The reason is that a DID that turns into a handle a moment later
reads as two different accounts, and a table that reorders or relabels
itself under the reader cannot be trusted; an account that appears a
few seconds late can.

Exceptions: a taken-down account's row waits for nothing, since its
host no longer answers for it; the top lists show unchecked accounts
as DIDs; and with `public_ui.handle_warming_enabled = false` nothing
is held back, because nothing would ever supply the answer. Admin
tables never hold rows back: an unchecked account shows as its DID.

### The warming worker

A page that meets an account with no answer, or with a stale one,
puts its DID on an in-memory queue. One worker in the server verifies
them.

- The queue holds at most 2,000 DIDs without duplicates. The newest
  request is served first, because the page someone is looking at now
  matters more than one from ten minutes ago; a DID asked for again
  moves to the front; within one page the first row is served first;
  when the queue is full the oldest entry is dropped.
- Each verification has a 5-second deadline, and at most 64 are in
  flight; the budget sets the pace and that bound limits what slow
  hosts can pile up.
- **Budget.** `public_ui.handle_rps` (default 20, 1 to 200) is one
  process-wide budget for the handle checks of page subjects, search,
  the filter box and warming together. The worker takes a token only
  while the bucket holds more than a reserve of 5, so a visitor's own
  request always finds some. Each check is up to two outbound HTTP
  requests (the DID document, the handle's host) and a DNS lookup.
  Warming cannot raise the server's outbound rate above the
  budget.
- The worker reads nothing from the database and scans no table; it
  writes one `handle_cache` row per answer.
- The queue is memory: a restart empties it and the memory layer, not
  the table.

`handle_warming_enabled` governs the admin pages too, and works with
the public UI off. With it off, nothing re-verifies a stored handle
and an account not seen before shows as a DID unless its own page or
card is opened.

Metrics: `farsight_handle_warming_total{outcome}`,
`farsight_handle_warming_queue`,
`farsight_public_ui_handle_resolutions_total`.

### The handle pass

Warming answers for accounts a page is about to show. The handle pass
answers for **every account the instance holds**, ahead of any view,
so that pages are complete on first render. It is off by default:
`public_ui.handle_pass_rps` is its rate in checks a second (0 = off,
at most 200), a pace of its own that does not draw on `handle_rps`.

1. **Identity changes first.** When an identity event names an
   account that has an `actors` row, the firehose writer upserts a
   row in `handle_due` in the same statement that clears the
   account's cached host; a row already there has its `asked_at` set
   to now. An account Farsight does not hold is not noted.

   ```sql
   CREATE TABLE handle_due (
     did      TEXT COLLATE "C" PRIMARY KEY,
     asked_at TIMESTAMPTZ NOT NULL DEFAULT now()  -- not served before this
   );
   ```

   The pass serves these before anything else, oldest first, 50 at a
   time. A row is removed when its check gives an answer and deferred
   an hour when the check establishes nothing. Emptying the table is
   safe: the walk reaches every account anyway.
2. **Then the walk.** `actors` in id order, a window of 20,000 ids a
   query, taking accounts whose status is not deactivated or deleted
   and that have no `handle_cache` row, or an empty one older than 7
   days. A verified handle is not re-checked by the walk; identity
   events and page views do that. The position is memory: a restart
   begins at the first account and passes over answered ones without
   a request. After the last account the walk rests 10 minutes.
3. **Answers.** A check has an 8-second deadline and one of four
   outcomes:

   | Outcome | Condition | Stored |
   |---|---|---|
   | `handle` | Verified in both directions | The handle, dated now |
   | `gone` | The DID has no document, the document names no handle, or the handle resolves to another DID | Empty, **replacing** a stored handle |
   | `unresolved` | The document names a handle whose host answered, but not with this DID | A stored handle equal to the named one is kept; any other is replaced by empty |
   | `unknown` | Nothing established: no answer, a refusal, a timeout | A stored handle is kept; otherwise an empty row dated to be due again in a day |

   An `unknown` or `unresolved` result stores nothing the first time:
   the account is checked once more after its batch (or after a
   back-off) and the second result is stored. A failure that proves
   nothing never removes a handle; a failure that proves the handle
   is no longer the account's does.
4. **Back-off.** When more than half of a batch of 20 or more
   established nothing, the pass waits a minute, doubling up to 30
   minutes. Not when those failures are mostly handles under one
   domain: one host that is down says nothing about the others.
5. **The memory cache** is written only where it already holds the
   account, so the pass does not push the accounts pages are showing
   out of it.

Metrics: `farsight_handle_pass_total{outcome}`,
`farsight_handle_pass_position`, `farsight_handle_pass_laps_total`.
Progress is on the dashboard's "Catching up".

## Profile cards and avatars

A profile card is an HTML fragment (`GET /card/{did}`) that the
script shows when the pointer rests on an account in a row or the
link takes keyboard focus. Touch devices get no cards. The same fetch
fills the header of an account page.

A card shows what the network publishes about the account: the
avatar, the verified handle, the DID, and when the DID was created.
The request is handled in this order:

1. the per-address class `public_ui_card`; over it, `429`;
2. one status lookup: an account this instance does not hold, or does
   not show, gets the same `404` and nothing is fetched for it;
3. the process-wide budget (`public_ui.card_rps`, default 4, and
   `card_burst`, default 8): with none left a short card (the DID and
   what is cached) is returned and nothing is fetched;
4. the fetches, **by the server**, through the safe client, with one
   3-second deadline for the whole card and no retry: the PLC audit
   log (or the `did:web` document) for the creation date and host,
   then the handle's forward resolution and the profile record side
   by side;
5. the fragment. A part whose fetch failed is left out.

A card makes at most three outbound HTTP requests (the audit log or
document, the handle's host, the profile record) and one DNS lookup.
With a fresh `avatar_cache` row the profile record is not read. The
card budget is their only limit: they do not pass through the
backfill's PLC rate limit.

**The server never fetches and never stores an image.** The card names
an image address, and the visitor's browser loads it, without a
referrer:

| Setting | Where the browser loads the avatar from | Who sees the visitor's address |
|---|---|---|
| `show_avatars = false` | Nowhere; the card has no image, and the CSP allows none | Only this instance |
| `show_avatars = true` (default), `avatar_thumbnails = false` (default) | The original blob, `com.atproto.sync.getBlob` on the account's own server; only for an `https` endpoint the safe client has just read the profile from | That server's operator |
| `show_avatars = true`, `avatar_thumbnails = true` | A thumbnail of a few kB from Bluesky's image service (`cdn.bsky.app`) | That service; an image appears only if the service has it |

The first default is a privacy trade the operator should weigh: on a
page listing who blocks an account, a blocker who runs their own
server can log who looks. That is why the choice is stated on the
confirmation page.

What the server does store is **which image** an account uses:

```sql
CREATE TABLE avatar_cache (
  did        TEXT COLLATE "C" PRIMARY KEY,
  cid        TEXT NOT NULL,            -- '' = the profile has no avatar
  checked_at TIMESTAMPTZ NOT NULL
);
```

A row is written when a card reads the account's profile record and
is used for a day, so that a card does not read the record on every
view. Emptying the table is safe. A list's image follows the same
rule with `lists.avatar_cid`: the address is named only with
`show_avatars`, and the browser loads it.

`GET /admin/card/{did}` serves the same fragment to admin tables. It
differs in three ways: without a valid session it is the bare `404`
and never a redirect; the withheld rule is not applied, since the
operator's tables show those accounts; and every answer is `no-store,
private`. It draws on the same per-address class and the same
process-wide budget.

## Times

The server renders every time as UTC, inside a `<time>` element whose
`datetime` attribute is the instant. The page's script rewrites the
text in the browser's timezone. A page read without script is
therefore correct and complete; it is in UTC and says so.

- On public tables the row shows the instant without a zone, and the
  zone is stated once per table ("All times are in UTC", the zone
  name replaced by the script along with the times).
- On admin pages the same rewriting applies to every time, including
  times inside a sentence of an alert or a coverage statement: the
  server finds `YYYY-MM-DD HH:MM[:SS] UTC` stamps in such text and
  wraps them as `<time data-plain>`, which the script rewrites in
  place.

The server never guesses a visitor's timezone and never varies a
response by it, which is what keeps public pages cacheable.

## Themes

Both surfaces have a light and a dark theme and a three-way toggle:
light, dark, system. The colours are custom properties on `:root`,
redefined under `prefers-color-scheme: dark` and under an explicit
`data-theme` on `<html>`; effects that exist in one theme only are
tokens too, so a component's rules do not test the theme.

- Without script the page follows the system preference, or the
  operator's default: `public_ui.dark_mode_default` (`light`, `dark`
  or `system`, default `system`) is emitted as `data-theme` on public
  pages.
- The toggle is hidden in the markup and revealed by the script,
  which stores the visitor's choice in `localStorage` (never in a
  cookie, so the server's response is still the same for everyone).
  On public pages the script is loaded in the head, without `defer`,
  so the stored choice is applied before the first paint.

The icon in the browser tab is one SVG that follows the palette.
