# Lodestar

[Lodestar](https://github.com/skydeval/lodestar) is a read-only mirror
of the PLC directory, kept seconds behind it, that also verifies every
handle an identity claims, in both directions, and re-checks each one
weekly. skydeval runs a public instance at
`https://lodestar.nearhorizon.app`, with no authentication; anyone can
run their own from the repository. Its API is described in
[its docs](https://github.com/skydeval/lodestar/blob/main/docs/design/api.md).

Farsight can read identities from a lodestar instance instead of making
every lookup itself. It is optional: with `backfill.lodestar_url` empty,
the default, nothing here applies and Farsight behaves as it always
has.

## What Farsight reads from it

| Read | Without lodestar | With lodestar |
|---|---|---|
| DID → PDS in the backfill ([backfill.md](backfill.md#steps-of-a-repo-job)) | The DID document from `backfill.plc_url` | `GET /account/{did}`: the PDS, and whether the DID exists or is tombstoned |
| A page of DIDs the backfill is about to resolve | One request each | `POST /accounts`, 1,000 DIDs a request, kept in the resolver's memory |
| A handle shown on a page, a card, or by the warming worker ([web-ui.md](web-ui.md#handles)) | The DID document, then DNS and the handle's own host | `GET /account/{did}`: lodestar's own verification |
| The card's "DID created" and its host | The PLC audit log | `GET /account/{did}` (`created_at`, `pds`) |
| The History tab's handles and hosts | The PLC audit log | `GET /account/{did}/history`, up to five pages of 200 |
| The stored handles of every account ([below](#the-handle-sync)) | Re-checked one by one, only by the handle pass or a page view | `POST /accounts`, 1,000 accounts a request |

Two things always stay with the PLC directory:

- **The `plc` sweep source.** It lists every identity from the
  directory's `/export`; lodestar keeps no operation log to list from.
- **Admin sign-in.** OAuth reads the admin's keys and services from the
  source, at the address the process started with
  ([web-ui.md](web-ui.md#admin-sign-in)).

`did:web` accounts are resolved in the backfill as before, from their
own host. Their handles go through lodestar like any other, since it
fetches and verifies `did:web` documents too.

## When Farsight asks the PLC directory instead

Every read falls back on its own, for that read only, when lodestar
gives nothing to go by:

- it answers `503` (`syncing`: it is not at the directory's tip yet;
  `unavailable`: a live resolution failed or its verification is
  paused), or any other error;
- it does not answer, or not within the wait: 1.5 seconds for a page,
  a card or the History tab, the backfill's usual timeout otherwise;
- its answer says it is not fresh (`source.plc.fresh` is not `true`);
- for a handle: it has not checked the claim yet (`pending`), or the
  identity is a `did:web` it could not read for two weeks
  (`unresolvable`).

The PLC directory and Farsight's own checks then answer as they do
without lodestar.

What lodestar does say is taken as the answer. A DID it reports as
`not_found` (it says so only while at the directory's tip) is a DID
that does not exist, and a handle it reports as `mismatch`, `unresolved`
or `none` is not shown. An `unreachable` handle (its domain has not
answered for at least a day) is treated as Farsight treats a host that
does not answer: a stored handle equal to the claim is kept for two
weeks after its last verification. Asking the PLC directory again for
every such answer would bring back the lookups lodestar exists to
save.

## The handle sync

Lodestar re-checks every handle each week. The handle sync makes the
handles Farsight has stored follow those checks without waiting for
someone to open a page. It runs in the `farsight` process while
`backfill.lodestar_url` is set and `public_ui.lodestar_sync_rps` (one
request a second by default, at most 20, `0` = off) is above 0.

1. **Identity changes first.** Every 30 seconds, the accounts an
   identity event named (`handle_due`) between 10 seconds and 5 minutes
   ago are asked for in one request. An answer settles an account only
   if lodestar checked the handle after the event, or the DID is gone;
   the account is then stored and leaves the queue. What lodestar has
   not settled after 5 minutes is left to the handle pass, which checks
   it itself, or, with the pass off, to the next page that shows the
   account.
2. **Then the walk.** `actors` in id order, 1,000 accounts a request,
   deactivated and deleted accounts left out. For every account
   lodestar can say something about, the stored answer is replaced
   when it differs from lodestar's, or when lodestar has checked the
   handle since it was stored. A verified handle is stored dated by
   lodestar's check, so a stored handle goes stale on lodestar's weekly
   cycle and not on the day Farsight happened to ask. Accounts it cannot
   say anything about are left as they are. After the last account the
   walk rests an hour.
3. **Back-off.** A request lodestar cannot answer is made again after a
   minute, doubling up to half an hour.

The walk's position is memory: a restart starts again from the first
account. Nothing is requested from any server but lodestar.

The handle pass (`public_ui.handle_pass_rps`) still works with
lodestar: its walk asks lodestar first, and checks for itself only
what lodestar cannot answer. Identity changes it takes only once the
sync has given up on them, and those it checks itself, since
lodestar's answer is older than the change.

## Limits

- **Backfill:** `backfill.lodestar_rps` (default 50) requests a second
  to the lodestar instance, a budget of its own: lodestar's requests do
  not use `backfill.plc_rps`, and the PLC directory's requests do not
  use this one. A batch of 1,000 DIDs is one request.
- **Server:** reads for pages and cards draw on the same budgets as
  before (`public_ui.handle_rps`, `card_rps`), and the handle sync on
  `public_ui.lodestar_sync_rps`.

## Trust

Lodestar decides which handles Farsight shows as verified, so the
instance named is trusted as the PLC directory is. A change of
`backfill.lodestar_url` from the admin UI needs a sign-in within the
last ten minutes ([security.md](security.md)). Requests to it go
through the safe client like every other outbound request. The
backfill process rebuilds its tasks in place to take a new address;
the server uses it from the next request.

## Settings

| Key | Default | Meaning |
|---|---|---|
| `backfill.lodestar_url` | `""` | The lodestar instance; empty: not used. Set in the admin UI under Settings → Identity source, or in `config.toml`. |
| `backfill.lodestar_rps` | `50` | Requests a second to it from the backfill process. |
| `public_ui.lodestar_sync_rps` | `1` | Requests a second of the handle sync; `0` = off, at most 20. |

## Metrics

| Name | Type | Labels |
|---|---|---|
| `farsight_backfill_lodestar_reads_total` | counter | `outcome` (`answered`, `fallback`) |
| `farsight_backfill_lodestar_request_seconds` | histogram | |
| `farsight_public_ui_lodestar_reads_total` | counter | `outcome` (`answered`, `fallback`) |
| `farsight_handle_sync_total` | counter | `outcome` (`written`, `unchanged`, `unknown`) |
| `farsight_handle_sync_position` | gauge | |
| `farsight_handle_sync_laps_total` | counter | |
