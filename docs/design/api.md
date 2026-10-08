# HTTP API

Farsight's API is a set of XRPC methods under the NSID prefix
`app.nearhorizon.farsight`. Six read queries answer questions about
the block graph, two methods let a token holder request and watch the
backfill of one account, and eight admin methods do what the admin
UI's Operations page does. This page gives the conventions, every
endpoint with its parameters, response and errors, and the rules for
tokens, access, rate limits and caching.

The lexicon files in `lexicons/app/nearhorizon/farsight/` are the
contract. Where this page and a lexicon disagree, the lexicon is
right.

## Conventions

- **Transport.** Queries are `GET /xrpc/<nsid>` with query parameters.
  Procedures are `POST /xrpc/<nsid>` with a JSON body. A method called
  with the other verb answers `400 InvalidRequest`; an unknown NSID
  answers `404` with the error name `InvalidRequest`.
- **DIDs only.** `actor` and `others` accept DIDs. A handle is
  `InvalidRequest`.
- **Pagination.** `limit` is 1 to 1000, default 100. `cursor` is an
  opaque keyset cursor from the previous page; a response without
  `cursor` is the last page. A cursor stays valid across writes: items
  inserted behind it are not returned, items deleted ahead of it are
  skipped. Cursor contents are not part of the contract. A string that
  does not have the shape of a cursor of the method is
  `InvalidRequest`. Cursors are not signed: a well-formed one that
  Farsight did not hand out is read as a position like any other, and
  returns the rows after it.
- **DIDs are compared in canonical form.** A `did:web` is the same
  account in any case of its hostname; requests accept any spelling
  and responses carry the lower-case one.
- **Repeated parameters.** An array parameter is given by repeating
  it: `others=did:plc:a&others=did:plc:b`.
- **Booleans** are `true` or `false`; absent means false.
- **Hidden accounts.** Rows authored by an account whose status is
  `deactivated`, `takendown`, `suspended` or `deleted` are left out
  unless `includeInactive=true`. Any other status, including unknown
  values, `throttled` and `desynchronized`, is shown. List membership
  is authored by the list's owner, so a hidden owner hides its lists
  in `getListsNaming`, `getIncomingListBlocks` and `checkBlocks`, and
  `getListMembers` reports such a list as `unavailable`.
- **Freshness.** Every read response carries a `freshness` object
  saying how current the answer is and what Farsight can claim about
  its completeness. It is described in [coverage.md](coverage.md).
- **Timestamps** named `createdAt` and `addedAt` are the record
  author's own claim and are for display only.
- **Open enums.** Fields declared with `knownValues` may gain values.
  Ignore unknown fields; treat an unknown coverage `level` as
  `partial` and an unknown list purpose as `other`.

### Errors

An error response is `{ "error": "<name>", "message": "<text>" }` with
`Cache-Control: no-store`.

| Name | Status | When |
|---|---|---|
| `InvalidRequest` | 400 | A missing or malformed parameter or body, an unknown body field, the wrong HTTP verb. Also an admin procedure that cannot write the configuration. |
| `AuthRequired` | 401 | No token where one is needed, a malformed `Authorization` header, or an invalid or revoked token. Sent with `WWW-Authenticate: Bearer`. |
| `Forbidden` | 403 | A valid API key that lacks the scope, or any API key on an admin method, or an API key while reads are disabled. |
| `RateLimitExceeded` | 429 | The caller's bucket is empty. Sent with `Retry-After`. |
| `QueueFull` | 429 | `requestBackfill` only: the requester's queue or daily allowance is exhausted. Sent with `Retry-After: 60`. |
| `Overloaded` | 503 | No query slot within 2 seconds, or the query ran into its time limit. Sent with `Retry-After: 1`. |
| `SetupRequired` | 503 | The instance has not been set up; every `/xrpc/` path answers this until the wizard has finished. |
| `InternalError` | 500 | Anything else. The detail is logged, not returned. |

A token that is presented but not valid is always `401`, also on an
endpoint that would have answered an anonymous caller. The routes are
not hidden behind `404`: the lexicons are public, so hiding them would
gain nothing.

## Endpoints

| NSID suffix | Verb | Class | Needs |
|---|---|---|---|
| `query.getIncomingBlocks` | GET | read | per `access.reads` |
| `query.getIncomingListBlocks` | GET | read | per `access.reads` |
| `query.getListsNaming` | GET | read | per `access.reads` |
| `query.getListMembers` | GET | read | per `access.reads` |
| `query.checkBlocks` | GET | read | per `access.reads` |
| `query.getStats` | GET | read | per `access.reads` |
| `query.getBackfillStatus` | GET | backfill | `backfill` scope or admin token |
| `admin.requestBackfill` | POST | backfill | `backfill` scope or admin token |
| `admin.listErrors` | GET | admin | admin token |
| `admin.restartFirehose` | POST | admin | admin token |
| `admin.pauseSweep` | POST | admin | admin token |
| `admin.startRepair` | POST | admin | admin token |
| `admin.pauseRepair` | POST | admin | admin token |
| `admin.cancelRepair` | POST | admin | admin token |
| `admin.createApiKey` | POST | admin | admin token |
| `admin.revokeApiKey` | POST | admin | admin token |

Every endpoint may answer `InvalidRequest`, `AuthRequired`,
`Forbidden`, `RateLimitExceeded`, `Overloaded`, `SetupRequired` and
`InternalError`. Only `admin.requestBackfill` adds `QueueFull`. The
sections below name the cases particular to each endpoint.

## Read queries

### `query.getIncomingBlocks`

The direct blocks that name an account.

| Parameter | Type | |
|---|---|---|
| `actor` | DID, required | The blocked account. |
| `limit` | integer 1-1000, default 100 | Page size. |
| `cursor` | string | From the previous page. |
| `includeInactive` | boolean, default false | Include blocks by hidden accounts. |

```json
{ "actor": "did:plc:subject",
  "blocks": [ { "did": "did:plc:blocker",
                "uri": "at://did:plc:blocker/app.bsky.graph.block/3l2x...",
                "createdAt": "2026-03-01T12:00:00.000Z" } ],
  "cursor": "...",
  "freshness": { } }
```

`actor`, `blocks` and `freshness` are always present; in a block, `did`
and `uri` are. Order: blocker (by Farsight's own actor id), then record
key. An account that holds several block records for the same subject
appears once per record; dedupe on `did`.

### `query.getIncomingListBlocks`

Who blocks an account through a list that names it. For each **ready
or retained** list naming `actor`, every listblock on the list,
flattened to (list, blocker) pairs.

| Parameter | Type | |
|---|---|---|
| `actor` | DID, required | The listed account. |
| `limit`, `cursor` | | As above. |
| `includeInactive` | boolean, default false | Include hidden blockers and lists of hidden owners. |
| `purpose` | `modlist`, `curatelist`, `referencelist` or `other` | Only lists with this purpose. Any other value is `InvalidRequest`. |

```json
{ "actor": "did:plc:subject",
  "items": [ { "list": "at://did:plc:owner/app.bsky.graph.list/3k...",
               "listPurpose": "modlist",
               "listName": "...",
               "blocker": "did:plc:blocker",
               "listblockUri": "at://did:plc:blocker/app.bsky.graph.listblock/3m...",
               "createdAt": "..." } ],
  "cursor": "...",
  "freshness": { } }
```

`list`, `listPurpose`, `blocker` and `listblockUri` are always present
in an item. Order: list (by Farsight's own list id), then blocker, then
listblock record key. A subject named twice in one list yields one pair
per listblock, not one per listitem. Every stored listblock on such a
list is returned, including one that does not count towards the list's
`listblockCount`: it is still a real block. Which listblocks are
counted is defined in
[list-indexing.md](list-indexing.md#the-listblock-counter).

Left out, and accounted for in `freshness`: lists whose record is
deleted (a listblock on a deleted list blocks nothing), lists whose
record has not been seen, and lists that are not `ready` or
`retained`.

### `query.getListsNaming`

Which **listblocked** lists name an account. Only tracked lists have
stored members, and a list is tracked only while at least one account
listblocks it; a list nobody listblocks never appears here.

| Parameter | Type | |
|---|---|---|
| `actor` | DID, required | The listed account. |
| `limit`, `cursor` | | As above. |
| `purpose` | as above | Only lists with this purpose. |
| `includeInactive` | boolean, default false | Include lists of hidden owners. |

```json
{ "actor": "did:plc:subject",
  "lists": [ { "uri": "at://did:plc:owner/app.bsky.graph.list/3k...",
               "purpose": "modlist",
               "name": "...",
               "listblockCount": 1234,
               "addedAt": "...",
               "itemUri": "at://did:plc:owner/app.bsky.graph.listitem/3n..." } ],
  "cursor": "...",
  "freshness": { } }
```

`uri`, `purpose`, `listblockCount` and `itemUri` are always present in a
list. Order: list (by Farsight's own list id). A subject named twice in
one list appears once; `itemUri` is then the listitem with the lowest
record key, and `addedAt` belongs to it. `listblockCount` is the number
of *counted* listblocks on the list.

### `query.getListMembers`

The members of one list.

| Parameter | Type | |
|---|---|---|
| `list` | AT-URI, required | An `app.bsky.graph.list` record. Any other collection is `InvalidRequest`. |
| `limit`, `cursor` | | As above. |

```json
{ "list": "at://did:plc:owner/app.bsky.graph.list/3k...",
  "state": "ready",
  "capped": false,
  "purpose": "modlist",
  "name": "...",
  "listblockCount": 1234,
  "members": [ { "did": "did:plc:...",
                 "itemUri": "at://did:plc:owner/app.bsky.graph.listitem/3n...",
                 "addedAt": "..." } ],
  "cursor": "...",
  "freshness": { } }
```

`list`, `state`, `capped`, `listblockCount`, `members` and `freshness`
are always present; in a member, `did` and `itemUri` are. Order:
member (by Farsight's own actor id), then listitem record key.

`state` is an open enum:

| `state` | Meaning | Members returned |
|---|---|---|
| `ready` | Admitted, and a fetch of the owner's repository has completed. | yes |
| `retained` | Was `ready` and lost its last counted listblock; kept for a grace period (`limits.list_grace`). | yes |
| `pending` | Admitted; the fetch has not completed yet. | no |
| `unavailable` | The fetch failed through its retries, the list stayed `pending` longer than `limits.pending_max_age`, or the owner is inactive or hidden. Farsight keeps retrying. | no |
| `deferred` | Counted listblocks exist, but a gate refused the admission: the storage budget or ceiling, a host or author cap, or the owner's daily re-admission allowance. | no |
| `untracked` | No counted listblock points at it. Also reported for a list Farsight has never seen. | no |
| `missing` | Counted listblocks exist, but the list record was not found; Farsight keeps retrying. | no |
| `dead` | The list record is known to be deleted, or was not found through all retries. | no |

A list whose owner is hidden is reported as `unavailable` whatever
state Farsight holds it in, with the reason `list_unavailable`, an
empty `members` array, no `purpose` and no `name`, `capped` false and
`listblockCount` 0. It is never reported as `ready` with no members,
which would read as a list known to be empty. The stored state is
reported again when the owner becomes active.

A list whose items are being purged is reported as `pending` if the
purge will end in a re-admission, else as `untracked`. The states and
the moves between them are defined in
[list-indexing.md](list-indexing.md#list-state).

A `pending` list returns an empty `members` array and partial
coverage. The rule is **not yet indexed, never partial**: a list is
served only when Farsight holds all of it, because a prefix of a block
list presented as the list would be a wrong answer. The one exception
is stated in the response: `capped: true` means a storage cap refused
further items, and the stored set of a `ready` or `retained` list is a
prefix.

### `query.checkBlocks`

The hydration endpoint. For each DID in `others`, whether a block
exists between it and `actor`, in either direction, directly or
through a ready or retained list. An AppView checks a viewer against
the authors on one page in a single indexed call, instead of paging
through the viewer's whole incoming set.

| Parameter | Type | |
|---|---|---|
| `actor` | DID, required | The viewer. |
| `others` | DID, repeated, 1 to 100, required | The other accounts. None, or more than 100, is `InvalidRequest`. |
| `includeInactive` | boolean, default false | Include blocks by hidden accounts. |

```json
{ "actor": "did:plc:viewer",
  "results": [ { "did": "did:plc:author",
                 "blocksActor":    { "direct": true,  "lists": ["at://..."] },
                 "blockedByActor": { "direct": false, "lists": [] } } ],
  "partialFor": [ { "did": "did:plc:other", "reasons": ["list_pending"] } ],
  "freshness": { } }
```

- `results` lists only the DIDs with at least one relation. A DID
  absent from `results` has no known block with `actor`.
- `blocksActor` are blocks held by `did` against `actor`;
  `blockedByActor` are blocks held by `actor` against `did`. `direct`
  says an `app.bsky.graph.block` record exists; `lists` are the lists
  that name the blocked party and that the blocking party listblocks,
  in URI order.
- `lists` holds at most 100 URIs. When more lists carry the relation,
  the object also has `"listsTruncated": true` and `lists` holds the
  first 100. The field is absent otherwise. The relation itself is
  reported either way.
- One call weighs at most 1,000 lists per account (the actor and each
  of the `others`). An account that holds listblocks on more is
  `partial` with the reason `listblocks_truncated`: at response level
  for the actor, in `partialFor` for one of the `others`
  ([coverage.md](coverage.md#checkblocks-per-pair-coverage)).
- `partialFor` is always present and may be empty. It names each of
  the `others` whose pair is not fully covered, with reasons from the
  same open enum as coverage reasons. The response-level
  `freshness.coverage` applies only to pairs **not** listed in
  `partialFor`; every pair listed there is `partial`.

There is no cursor: the answer is one page by construction.

### `query.getStats`

Counts, firehose status, backfill progress and the instance-wide
freshness. No parameters.

```json
{ "service": { "version": "0.6.2",
               "hostname": "farsight.example",
               "contact": "mailto:ops@example" },
  "counts": { "blocks": 0, "listBlocks": 0, "lists": 0,
              "trackedLists": 0, "listItems": 0, "actors": 0 },
  "firehose": { "connected": true, "protocol": "v2",
                "lagSeconds": 1.4, "sourceLagSeconds": 2.0,
                "openGaps": 0 },
  "backfill": { "sweep": { "cycle": 1, "source": "relay_collections",
                           "state": "running", "progress": 0.42,
                           "etaSeconds": 612000 },
                "queue": { "onDemand": 3, "active": 1201 },
                "reposPerHour": 18500 },
  "freshness": { },
  "detail": { } }
```

- `counts` come from maintained counters, not from counting the
  tables; they are rebuilt exactly once a day
  ([operations.md](operations.md)).
- `firehose.lagSeconds` is now minus the applied-through watermark;
  `sourceLagSeconds` is now minus the median commit time of recent
  events. Both are JSON numbers with a fractional part. The lexicon
  declares them `unknown` because Lexicon has no floating-point type;
  the same holds for `sweep.progress`.
- `backfill.sweep` is absent before the first cycle exists. Its
  `state` is `running`, `completed` or `paused` (open enum);
  `completedAt` appears once the cycle is complete.
  `queue.onDemand` counts the waiting entries of the scheduler's
  first tier: `requestBackfill` jobs, discoveries, list jobs and
  re-reads owed after a `#sync` or a refusal. `queue.active` counts
  those of the second tier: accounts first seen authoring an indexed
  record on the firehose, queued so that their earlier records are
  read too. The third tier, sweep and repair members, is not reported
  in `queue`. See [backfill.md](backfill.md#scheduler).
- `detail` is unstable diagnostic data (itemized exceptions, queue
  depth by tier, unhealed gaps). Do not build on it.

## Backfill endpoints

Both need a bearer token: an API key with the `backfill` scope, or the
admin token. They are never anonymous, whatever `access.reads` says.

Requesting the backfill of account X indexes X's **own** repository.
It cannot reveal who blocks X: those records live in other accounts'
repositories. Incoming coverage for X is complete only after a sweep
cycle, or sooner at level `assisted` through the optional backlink
discovery. See [coverage.md](coverage.md) and
[backfill.md](backfill.md).

### `admin.requestBackfill`

Input, as a JSON body:

```json
{ "actor": "did:plc:...", "priority": "normal", "force": false }
```

| Field | Type | |
|---|---|---|
| `actor` | DID, required | The account whose repository to read. |
| `priority` | `normal` (default) or `high` | `high` needs the `backfill:high` scope or the admin token. Without it the request runs as `normal` and the response says so. Another value is `InvalidRequest`. |
| `force` | boolean, default false | Run even if recently done or running. |

The query-parameter form is also accepted, still as a `POST` and with
an empty body: `?actor=...&priority=...&force=...`. A JSON body with a
field other than these three is `InvalidRequest`.

Effect: a first-tier **repository job** for `actor` and, if a backlink
source is configured (`backfill.backlinks.url`), a **subject
discovery**. The rules:

| State of the account | Without `force` | With `force` |
|---|---|---|
| Already queued | No new work; the entry's priority is raised if the request's is higher. | Same. |
| A repository job is running | No new work. | A waiting entry is added and runs after the current job. |
| Done less than `backfill.request_fresh_window` ago (default 1 hour) | No new work. | Enqueued. |
| Never done, failed, or done longer ago | Enqueued. | Enqueued. |

Only a repository job counts as "running". If a list fetch or a
discovery for the same account is holding the account's lease, a new
repository job is enqueued and waits for it.

Response: `202` with the body of `getBackfillStatus` plus two fields.

```json
{ "actor": "did:plc:...",
  "repo": { "state": "queued", "position": 4 },
  "discovery": { "state": "disabled", "truncated": false },
  "enqueued": true,
  "downgraded": false }
```

`enqueued` says new work was added. `downgraded` says `high` was
lowered to `normal`.

`QueueFull` (429, `Retry-After: 60`) is returned when the requester
already holds 10,000 waiting entries, or when the requester's daily
allowance for introducing accounts Farsight has never seen
(`limits.intern_per_did_per_day`) is used up.

### `query.getBackfillStatus`

| Parameter | Type | |
|---|---|---|
| `actor` | DID, required | |

A pure read; it never enqueues, so polling it is safe.

```json
{ "actor": "did:plc:...",
  "repo": { "state": "done",
            "position": null,
            "lastBackfilledAt": "2026-03-01T12:00:00.000Z",
            "lastError": null },
  "discovery": { "state": "done",
                 "completedAt": "2026-03-01T12:00:05.000Z",
                 "truncated": false,
                 "source": "https://backlinks.example" } }
```

Fields shown as `null` here are omitted from a real response when they
have no value.

| `repo.state` | Meaning |
|---|---|
| `never` | Nothing known and no baseline covers the account. |
| `queued` | Waiting; `position` counts the waiting entries of the same tier ahead of it. |
| `running` | A repository job is reading the account. |
| `done` | Read completely; `lastBackfilledAt` says when. |
| `failed` | The last attempt failed; `lastError` says why. |
| `covered_by_sweep` | Farsight keeps no per-account record (it keeps one only for accounts it holds data for or that were requested), but a completed sweep cycle covers the repository. `lastBackfilledAt` is the cycle's completion time. |

`discovery.state` is `disabled` when no backlink source is configured,
else `never`, `queued`, `running`, `done` or `failed`.
`discovery.truncated` means the discovery did not check every
reference: it stopped at its reference cap
(`backfill.backlinks.max_refs`), or a reference could not be read
(its author did not resolve, or the PDS did not answer). Incoming
coverage for the account stays `partial`. Both enums are open.

The response is `Cache-Control: no-store, private`.

## Admin procedures

These need the admin token; an API key gets `403 Forbidden`. They are
**unstable**: outside the stability contract, and free to change
between releases. The admin UI's Operations page calls the same code;
see [../guide/admin-ui.md](../guide/admin-ui.md).

Procedures with an empty input accept an empty body. A JSON body with
an unknown field is `InvalidRequest`.

### `admin.listErrors`

A query (`GET`). Recent operational errors, newest first: failed
periodic tasks, backfill failures, counter drift.

| Parameter | Type |
|---|---|
| `limit` | integer 1-1000, default 100 |
| `cursor` | string |

```json
{ "errors": [ { "id": 812, "at": "2026-03-01T12:00:00.000Z",
                "component": "task:counter_rebuild",
                "did": "did:plc:...", "host": "pds.example",
                "message": "..." } ],
  "cursor": "..." }
```

`id`, `at`, `component` and `message` are always present; `did` and
`host` only when the error concerns one.

### `admin.restartFirehose`

Input `{}`. Drops the Jetstream session; the reader reconnects from
the persisted cursor. Output `{ "restarted": true }`. It does not wait
for a query slot, so it works on an overloaded instance.

### `admin.pauseSweep`

Input `{ "paused": true }` to pause, `false` to resume. Sets
`backfill.sweep.enabled` in the configuration file and notifies the
backfill process, which reloads it. Output `{ "paused": true }`.

### `admin.startRepair`

Input `{}`. Requests one repair cycle covering every closed, unhealed
firehose gap, starting from the earliest of them; the backfill process
runs it. A gap that is still open waits. If a repair cycle is already
under way it is reused, not doubled.

```json
{ "cycle": 7, "from": "2026-03-01T10:00:00.000Z", "gaps": 2 }
```

`gaps` is always present. With no closed, unhealed gap the output is
`{ "gaps": 0 }` and nothing is started.

### `admin.pauseRepair`

Input `{ "paused": true }` or `false`. Sets `backfill.repair.paused`.
A paused repair enumerates nothing new and its members are not
dispatched, and it keeps its place; retries that were already queued
may still run. Output `{ "paused": true }`.

### `admin.cancelRepair`

Input `{}`. Cancels the repair cycle under way, in this order:

1. Sets `backfill.repair.auto_start = false`. Without this the backfill
   process would start the same repair again at its next look. If the
   configuration cannot be written, nothing is cancelled.
2. Under the same advisory lock `admin.startRepair` takes, deletes the
   open repair cycle, its outstanding members and its queue entries,
   and releases its gaps, which remain unhealed.

Jobs already running finish, and what they wrote is kept.

```json
{ "cycle": 7, "autoStart": false }
```

`autoStart` is always present and always false; `cycle` is absent when
no repair was under way. `admin.startRepair` still starts a repair by
hand afterwards; to have repairs start by themselves again, set
`backfill.repair.auto_start` back to true.

### `admin.createApiKey`

```json
{ "name": "my-appview", "scopes": ["read", "backfill"], "readRps": 200 }
```

| Field | Type | |
|---|---|---|
| `name` | string, 1 to 200 characters (counted as characters, not bytes), required | A label for the key. |
| `scopes` | array, at least one, required | Of `read`, `backfill`, `backfill:high`. An unknown scope is `InvalidRequest`. |
| `readRps` | positive integer | Per-key read rate, replacing `rate_limit.key_rps`. |

Output `{ "id": 3, "token": "fsk_..." }`. The token is returned this
once and stored only as a hash.

### `admin.revokeApiKey`

Input `{ "id": 3 }`. Output `{ "revoked": true }`; false when no live
key has that id. A revoked key stops working at once.

### When the configuration cannot be written

`admin.pauseSweep`, `admin.pauseRepair` and `admin.cancelRepair` edit
the configuration file. They answer `InvalidRequest`, with the reason
in `message`, when the key they set is fixed by an environment
variable, or when the instance has no file at all and takes its whole
configuration from the environment. See
[operations.md](operations.md).

## Tokens and scopes

| Token | Shape | Stored as |
|---|---|---|
| Admin token | `fsa_` + 43 base64url characters (256 bits) | SHA-256, hex, in the configuration (`auth.admin_token_sha256`) |
| API key | `fsk_` + 43 base64url characters (256 bits) | SHA-256 in the `api_tokens` table |

Tokens are sent as `Authorization: Bearer <token>`. They are random
256-bit values, so a fast hash is enough; hashes are compared in
constant time.

| Scope | Allows |
|---|---|
| `read` | The six read queries. Needed only when `access.reads = "api_key"`; it also moves the caller from the anonymous limits to the per-key limits. |
| `backfill` | `admin.requestBackfill` and `query.getBackfillStatus`. |
| `backfill:high` | `priority: "high"` on `requestBackfill`. Does not include `backfill`. |

The admin token allows everything. API keys are created with
`admin.createApiKey` or on the admin UI's keys page.

## Access modes

`access.reads` in the configuration decides who may call the six read
queries.

| Caller | `public` (default) | `api_key` | `disabled` |
|---|---|---|---|
| Anonymous | allowed | `401 AuthRequired` | `401 AuthRequired` |
| API key with `read` | allowed | allowed | `403 Forbidden` |
| API key without `read` | `403 Forbidden` | `403 Forbidden` | `403 Forbidden` |
| Admin token | allowed | allowed | allowed |

With `disabled`, the admin token and the signed-in admin UI still
read, so that the operator can diagnose the instance.

The backfill and admin endpoints do not depend on `access.reads`:

| Caller | Backfill endpoints | Admin endpoints |
|---|---|---|
| Anonymous | `401 AuthRequired` | `401 AuthRequired` |
| API key with `backfill` | allowed | `403 Forbidden` |
| API key without `backfill` | `403 Forbidden` | `403 Forbidden` |
| Admin token | allowed | allowed |

The public web UI requires `access.reads = "public"`; a configuration
that combines `access.public_ui = true` with another mode is refused.
See
[web-ui.md](web-ui.md#off-by-default-confirmed-before-it-is-on).

## Rate limits

Limits are token buckets held in memory. An anonymous caller's bucket
is keyed by its resolved client address (an IPv6 address by its /64);
an authenticated caller's by its token. How the client address is
resolved behind a proxy is in
[security.md](security.md#client-address).

| Class | Applies to | Sustained | Burst |
|---|---|---|---|
| `anon_read` | Anonymous read queries, per address | `rate_limit.anon_rps` (10/s) | `rate_limit.anon_burst` (50) |
| `key_read` | Read queries and `getBackfillStatus` with an API key, per key | the key's `readRps`, else `rate_limit.key_rps` (100/s) | `rate_limit.key_burst` (500); for a key with its own rate, scaled to keep the same ratio |
| - | Reads and admin procedures with the admin token | unlimited | - |
| `admin_backfill` | `requestBackfill` with the admin token | `rate_limit.admin_backfill_rps` (20/s) | 100 |
| `key_backfill` | `requestBackfill` with an API key, per key | `rate_limit.key_backfill_rps` (5/s) | 20 |

The web UI has classes of its own (page views, lookups, profile cards,
sign-in); the full table is in
[security.md](security.md#rate-limit-classes).

An anonymous IPv6 caller draws on three buckets at once: its `/64` at
the limit above, its `/48` at 4 times and its `/32` at 16 times, and a
request needs a token in each. `RateLimit` reports the `/64`; a
refusal reports the bucket that refused.

Two further bounds apply to every caller, the admin token included:

- **Concurrency.** A global semaphore admits
  `rate_limit.query_concurrency` (32) requests at a time. A request
  that waits more than 2 seconds for a slot gets `503 Overloaded` with
  `Retry-After: 1`. A quarter of the slots (8) is never given to
  anonymous callers. One caller holds only part of them: an anonymous
  address (IPv6: a `/48`) a quarter of the anonymous slots (6), an API
  key half of all slots (16). A request over that waits for one of
  its caller's own places, and after 2 seconds gets `503 Overloaded`
  like any request that found no slot; the admin token has no such
  bound.
- **Time.** Read queries run in a read-only transaction with
  `statement_timeout` set to `rate_limit.query_timeout` (5 s). A query
  that hits it gets `503 Overloaded`.

### Headers

`RateLimit-Policy` and `RateLimit` follow the IETF draft:

```
RateLimit-Policy: "anon_read";q=50;w=5
RateLimit: "anon_read";r=49;t=1
```

`q` is the bucket size, `w` the seconds a whole bucket takes to
refill, `r` the requests remaining, `t` the seconds until the bucket
is full. A `429` adds `Retry-After`.

These headers describe one caller, so they are sent only on responses
a shared cache never replays: errors, and successful responses that
are `private` or `no-store`. A `public` response omits them.

## Caching and CORS

Read responses do not depend on who asks. That makes them safe to
cache at an edge, and the headers say so.

| Response | `Cache-Control` |
|---|---|
| `getStats`, reads public | `public, max-age=60` |
| The other five read queries, reads public | `public, max-age=30` |
| Any read query when `access.reads` is `api_key` or `disabled` | `private, max-age=30` |
| `getBackfillStatus`, `requestBackfill`, admin procedures | `no-store, private` |
| Any error | `no-store` |

With `access.cors = true` (the default) the six read queries send
`Access-Control-Allow-Origin: *` and answer an `OPTIONS` preflight
with `204`. The backfill and admin endpoints never send CORS headers.

How to put a CDN in front is in
[../guide/cloudflare.md](../guide/cloudflare.md).

## Stability and versioning

### What is stable

- The seven queries `query.getIncomingBlocks`,
  `query.getIncomingListBlocks`, `query.getListsNaming`,
  `query.getListMembers`, `query.checkBlocks`, `query.getStats`
  (without `detail`) and `query.getBackfillStatus`, and the procedure
  `admin.requestBackfill`.
- The `freshness` object, including the definition of `complete`
  ([coverage.md](coverage.md)).
- `checkBlocks.partialFor`: required, and part of the definition of
  `complete`.
- The error names.
- The meaning of `/health` ([operations.md](operations.md)).

### What is not

The other `admin.*` methods, `getStats.detail`, cursor contents, the
web UI (routes, markup, and its configuration keys), block and
list-membership history, and metric names (kept on a best-effort
basis).

### Rules

- **The NSID is the version.** A breaking change ships under a new
  NSID, such as `query.getIncomingBlocksV2`; the old one keeps working
  unchanged.
- **Non-breaking:** new optional parameters; new output fields; new
  values in open enums (coverage `level` and `reasons`, list `state`,
  backfill `state`, `purpose`); new `exceptions` counters; new
  endpoints.
- **Breaking, so a new NSID:** removing or renaming a field; changing
  a type or a meaning, the definition of `complete` included; making a
  parameter required; changing default filtering, such as which
  statuses are hidden; changing an ordering guarantee; changing an
  error name.
- **Deprecation.** No NSID has been superseded so far. The commitment
  for when one is: the old NSID is served for at least six months
  after its successor ships, is announced as deprecated for that
  time, and is removed only in a major release.
- The binary's version follows the lexicon set: a major version means
  an NSID was removed.

## What the API leaves out, and why

The API answers what an AppView needs in order to enforce blocks:
whether a block stands between two accounts, and who blocks a given
account. It is kept to that on purpose. These are design decisions,
not gaps waiting to be filled:

- **No listing of an account's outgoing blocks.** `checkBlocks`
  reports both directions for a named pair; nothing returns "everyone
  this account blocks". An AppView reads that from its own users'
  repositories.
- **No removed records.** Blocks and list memberships that were stored
  and later removed are kept for the operator and shown only on the
  admin pages (see [history.md](history.md)). No endpoint returns them.
- **No rankings.** Nothing returns the most-blocked or most-blocking
  accounts.
- **No profiles.** No handles, display names, avatars or other account
  data: Farsight indexes four record collections and nothing else.

### Changing the API

The method names under `app.nearhorizon.farsight.*` belong to this
project. A fork that adds, removes or changes methods should serve its
API under a namespace of its own, so that a client can tell which API
it is talking to and what that API promises.

## Using the API from an AppView

Farsight knows nothing about any AppView; the integration is four
calls. The how-to is [../guide/appview.md](../guide/appview.md).

1. **Enrollment.** Call `requestBackfill` for the new member, then
   poll `getBackfillStatus` (every 10 seconds, backing off to 60)
   until `repo.state` is `done` or `failed`, and `discovery.state`
   likewise where discovery is enabled.
2. **Hydration.** Call `checkBlocks` with the viewer as `actor` and
   the authors on the page as `others`: one call per page of up to
   100 authors, cacheable for 30 seconds. For a viewer-wide screen
   ("who blocks me") page through `getIncomingBlocks` and
   `getIncomingListBlocks`.
3. **Coverage.** `complete`: enforce the answer as it is. `assisted`:
   enforce it, as best effort. `partial`: enforce what is there, and
   consider stricter treatment of strangers. An unknown level is
   `partial`. In `checkBlocks`, every DID in `partialFor` is `partial`
   whatever the response-level level says.
4. **Suspected staleness** for one account: `requestBackfill` with
   `force: true`.

The key for this needs the `read` scope (on an instance that gates
reads) and the `backfill` scope.
