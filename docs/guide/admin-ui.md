# Admin UI and sign-in

The admin UI lives under `/admin` and is served while
`access.admin_ui = true` (the default for a config that does not say).
Every page of it needs a session: without one, a browser is sent to
`/enter`. With `access.admin_ui = false` neither `/admin` nor `/enter`
exists, and the instance is administered with the admin token over the
API and by editing its config. The switch is read at start only: change
it in `config.toml` (or `FARSIGHT__ACCESS__ADMIN_UI`) and restart.
Settings refuses a save that would change it, and while `config.toml`
holds a value that differs from the running one, no setting can be
saved from the running server (pausing the sweep over the API
included) until the restart.

## The pages

- **Dashboard.** Index counts, firehose and backfill state, storage,
  exceptions, lists by state and the largest host buckets, refreshed
  every 10 seconds. A "Catching up" block at the top says how far the
  history sweep and the handle pass have got and about how long each
  has left; a line goes away when its work is done.
- **Alerts**, a drop-down in the bar of every admin page: the warnings
  (such as a full storage budget, a v1 firehose, unrepaired gaps or
  sort indexes not yet built) and the coverage sentence, with the
  number of warnings on its button.
- **DID lookup.** The account's header with its backfill state, then
  tabs: incoming blocks, incoming listblocks, the lists that name it,
  the lists it subscribes to as block lists, and History (handle and
  host history, and the blocks and list memberships this instance
  stored and later removed). Each record has a button that copies its
  `at://` address.
- **List lookup.** The list's facts and description, then "Members"
  and "Subscribers".
- **Operations.** Queue a backfill, restart the firehose, pause the
  sweep; API keys; recent errors. And gap repair: see below.
- **Settings.**

Times on every admin page are shown in the browser's timezone.

## Gap repair

When the firehose loses its place (a disconnection, or time spent on a
v1 Jetstream), Farsight records a gap. Once the gap has closed, a
repair re-reads every account whose records changed during it, found
by walking the relay's account list. After a long gap that is a great
many accounts: a three-day gap is days to weeks of work at the default
per-host rate. Coverage stays `partial` until the repair finishes.

Operations has the controls, and `[backfill.repair]` the settings:

```toml
[backfill.repair]
auto_start = true   # a repair starts by itself when a gap has closed
paused = false      # true holds repairs; one under way keeps its place
```

- **Start repair** starts one for every closed gap. With `auto_start`
  on you never need it.
- **Pause repair / Resume repair** sets `paused`. A paused repair reads
  nothing new and continues where it stopped when resumed.
- **Cancel repair** drops the repair under way and leaves its gaps
  unrepaired. It also turns `auto_start` off, since otherwise the same
  repair would begin again at once.
- **Turn automatic repairs off / on** sets `auto_start`.

The same over the API: `admin.startRepair`, `admin.pauseRepair`,
`admin.cancelRepair`. With a config managed through the environment,
set `FARSIGHT__BACKFILL__REPAIR__PAUSED` and
`FARSIGHT__BACKFILL__REPAIR__AUTO_START` instead.

## Signing in

The admin is one ATProto account, named by its DID in
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
  then `docker restart farsight`. The change applies at the restart
  (the running server refuses to pick it up in between), and every session of
  the previous account ends then. `farsight admin-did` prints the
  current value. With `FARSIGHT_SKIP_WIZARD=1`, set
  `FARSIGHT__ACCESS__ADMIN_DID` instead. Settings shows the admin DID
  but does not change it.
- Neither the wizard nor the CLI can prove that the DID you enter is
  yours; they show what it resolves to. A wrong DID is fixed with the
  CLI.
- **A reverse proxy may restrict who reaches `/admin*` and `/enter*`**
  (an address allowlist, its own authentication). It cannot rename
  them: every link and redirect is an absolute path, and the sign-in
  returns to `https://<hostname>/enter/callback`. For sign-in on the
  hostname, the account's server must be able to fetch
  `/.well-known/atproto-oauth-client-metadata` from the internet; an
  instance whose `/enter` is closed to the outside signs in on
  `127.0.0.1`.
