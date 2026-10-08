# Security: hostile actors, bounds and the network edge

Farsight stores records that anyone on the network can write, fetches
from hosts that anyone can name, and answers queries from anyone who
can reach it. This page states what an attacker can do with each of
those, the bound Farsight puts on it, and what remains possible inside
the bounds. It also covers the network edge: how the client address is
found behind a proxy, what is cached, and how the API and the two web
interfaces are protected.

The mechanics of authentication and rate limiting are in
[api.md](api.md); the way a refusal shows up in query results is in
[coverage.md](coverage.md).

## Threat model

An ATProto account costs nothing, a PDS can be self-hosted, and a
repository can hold any number of block, listblock, list and list-item
records. Farsight therefore assumes that:

- any number of accounts, on any number of hosts, write records with
  the purpose of filling the index, or of making it fetch;
- a PDS, a DID document, a `did:web` host or a handle domain may be
  operated by the attacker and may answer anything, slowly, or with a
  redirect to somewhere else;
- an optional backlink index used for discovery may return references
  that do not exist;
- API callers and page visitors send as many requests as they are let.

The design principle is that **a cap is a deliberate refusal to store
data, and every refusal is counted and reported**. An attacker can make
Farsight decline to store the attacker's own records. They cannot make
it silently lose someone else's, and they cannot grow the database
beyond the operator's budget.

### Attacks and their bounds

| Attack | Bound |
|---|---|
| Listblocks that name lists which do not exist | The list record is checked first: one `getRecord`, then the list is `missing` (an authoritative not-found) or `unavailable` (the claimed owner's host errors; retried). No items are fetched either way. Past `limits.listblock_fetch_triggers_per_author` an author's listblocks are stored uncounted: they cannot admit a list or change a list's state. DIDs that do not exist fail at resolution and are remembered as such for 24 hours. |
| Listblocks on a third party's real lists, to make Farsight hammer that party's PDS | Fetches are coalesced per list owner: at most one run per owner per `backfill.owner_fetch_cooldown` (`10m`). Every outbound host has its own pace. |
| Spam lists that the attacker listblocks themselves | Caps on items per list and per owner; the per-host buckets and the storage budget. |
| List items for lists nobody blocks | Not stored. Costs nothing. |
| List items that name someone else's list | Dropped: only the owner's repository has authority over a list's members. |
| Block spam from one repository | `limits.blocks_per_author`, exact. |
| Listblock or list record spam from one repository or host | `limits.listblocks_per_author`, `limits.lists_per_author`; the buckets' `host_listblocks` and `host_lists` caps. A placeholder list row is charged to the bucket of the listblock's author. |
| Many cheap accounts on one self-hosted PDS | The per-host buckets and the storage budget. |
| Repositories full of old listblocks, to force list coverage to `partial` everywhere | A daily admission rate per admission key; at most `limits.pending_effects_per_owner_key` pending lists per owner key take effect; a list is `pending` for at most `limits.pending_max_age`. `checkBlocks` reports coverage per result, which confines the effect to the attacker's own pairs. |
| Block and unblock (or list add and remove) churn, to grow the history tables | A daily rate of history rows per admission key, one budget across the three history tables. Beyond it a removal is applied and not recorded. Retention bounds the total. See [history.md](history.md). |
| A hostile PDS during backfill | Size and time bounds on every response; records whose authority is another repository are discarded. See [backfill.md](backfill.md). |
| A malicious backlink index | Every reference is verified at its author's PDS. Waste is bounded by `backfill.backlinks.max_refs` (200,000) and charged to the requester. |
| SSRF through DID documents, `did:web` hosts and handles | The safe outbound client, below. |
| Expensive queries | Rate limits, a concurrency bound, a statement timeout, cacheable responses. |
| Abuse of the admin sign-in | Per-address and process-wide limits on sign-in attempts, a capped store of sign-ins in progress, and the checks of the OAuth flow. |
| Forged client addresses, to escape rate limits | Forwarding headers are read only from trusted proxies. |

### What Farsight does not defend against

- **Its firehose source.** Whoever operates the Jetstream instance
  Farsight reads decides which events it sees and when it saw them.
  The times that order rows are the source's witness times, so the
  source can set them; an account's own PDS cannot.
- **The PLC directory and DNS**, as far as identity goes: a DID
  resolves to what they say.
- **A large host that does not police its own accounts.** Hosts in
  `limits.large_hosts` are exempt from the per-host buckets; see
  "Residuals".
- **Authenticity of record content.** Farsight indexes what
  repositories contain. That an account blocks another is a fact about
  the repository, not a judgement.
- **A compromised operator account or host.** The admin token and the
  admin account have full control; the database is trusted.

## Aggregate bounds

The **storage budget** is the aggregate bound: nothing an attacker
does on non-large hosts makes the database grow past it. The per-host
caps exist for fairness between hosts, so that one hostile host cannot
use up the whole budget; the per-author caps do the same between
accounts.

### Cap buckets

Every author is charged to one or more rows of `host_usage`, keyed by
`bucket`:

| Bucket | Who is charged to it |
|---|---|
| `d:<registrable domain>` | Authors whose PDS host is under that domain (eTLD+1 by the Public Suffix List). A `did:web` author is charged to its DID's domain before it is resolved; that needs no I/O. A host that is an address and not a name has no domain: its bucket is `d:` followed by its address block, the same /24 or /48 as below, so the addresses of one block share one bucket, one admission key and one outbound pace. |
| `ip:<a.b.c.0/24>`, `ip:<x:y:z::/48>` | Authors whose PDS host resolves into that address block. Omitted when the address is in a shared CDN range: the bundled Cloudflare ranges and `limits.cdn_ranges_extra`. Only the domain bucket applies there. |
| `unresolved` | `did:plc` authors whose DID has not been resolved yet. It has the four record caps and no lifetime bound on interning (see [Admission keys and daily rates](#admission-keys-and-daily-rates)). |
| `did:<did>` | A `did:plc` author whose resolution has failed three times with an error other than not-found (`actors.resolve_failures`). It gets a bucket of its own, with the per-author caps, so that one unresolvable account cannot fill `unresolved` for everyone. |
| (none) | Authors on a host matching `limits.large_hosts`. Exempt. |

A resolved author on an ordinary host is charged to both its domain
bucket and its address bucket, and is refused when either is closed.
Holding many domains on one address block, or many addresses under one
domain, gains nothing.

`host_usage` is approximate: writers accumulate deltas and flush them.
A daily rebuild recounts the four `stored_*` record columns from the
per-author counters, grouped by each author's current buckets; it
leaves `stored_interned` as it is. Usage that was charged to
`unresolved` moves to the author's real buckets at the moment the
author is resolved: the resolver takes the author's exact counts out of
the old buckets and adds them to the new ones
([storage.md](storage.md#counters)). The rebuild corrects what is left:
flushes lost in a crash, and a host whose own address block or
large-host standing changed.

Each row has a `capped_mask` with one bit per cap (blocks 1, list
items 2, listblocks 4, lists 8, lifetime interning 16). A bit is set
when usage reaches the cap and cleared when usage falls below 95% of
it.

The large-host exemption trusts the PDS hostname in the DID document.
That is sound because a relay only relays an account's events from the
host its DID document names. Per-author caps and the daily rates still
apply on large hosts.

### Limits

All keys are in `[limits]`.

| Key | Default | Scope |
|---|---|---|
| `blocks_per_author` | 1,000,000 | Stored blocks per author. Exact. |
| `listblocks_per_author` | 100,000 | Stored listblocks per author. Exact. |
| `lists_per_author` | 10,000 | Stored list records per author. Exact. |
| `listblock_fetch_triggers_per_author` | 5,000 | Counted listblocks per author; beyond it a listblock is stored uncounted. |
| `list_items_per_list` | 1,000,000 | Items per list. |
| `list_items_per_owner` | 2,000,000 | Items per list owner. |
| `host_blocks` | 20,000,000 | Blocks per `d:` or `ip:` bucket. |
| `host_list_items` | 5,000,000 | List items per bucket. |
| `host_listblocks` | 2,000,000 | Listblocks per bucket. A placeholder list row counts as one from the write that creates it until the next daily rebuild, which counts stored listblocks only. |
| `host_lists` | 200,000 | List records per bucket. |
| `host_interned_lifetime` | 5,000,000 | Rows a `d:`, `ip:` or `did:` bucket has ever caused to be interned. |
| `unresolved_blocks` | 1,000,000 | Blocks in the `unresolved` bucket. |
| `unresolved_list_items` | 500,000 | List items in `unresolved`. |
| `unresolved_listblocks` | 200,000 | Listblocks in `unresolved`. |
| `unresolved_lists` | 20,000 | List records in `unresolved`. |
| `bucket_admissions_per_day` | 20,000 | List admissions per `bucket:` admission key, per UTC day. |
| `did_admissions_per_day` | 200 | List admissions per DID admission key, per UTC day. |
| `intern_per_bucket_per_day` | 5,000,000 | Rows interned per `bucket:` cause key, per UTC day. |
| `intern_per_did_per_day` | 1,000,000 | Rows interned per DID or requester cause key, per UTC day. |
| `history_per_bucket_per_day` | 200,000 | History rows per `bucket:` admission key, per UTC day. |
| `history_per_did_per_day` | 10,000 | History rows per DID admission key, per UTC day. |
| `pending_effects_per_owner_key` | 5 | Pending lists per owner key that take effect in queries. |
| `pending_max_age` | `3h` | Longest a list stays `pending`. |
| `large_hosts` | `["*.host.bsky.network"]` | Hosts exempt from buckets. An entry is an exact host, or `*.suffix` for any subdomain of `suffix`. |
| `cdn_ranges_extra` | `[]` | More shared ranges that are not used as address buckets. |

A per-DID bucket (`did:<did>`) uses `blocks_per_author`,
`list_items_per_owner`, `listblocks_per_author` and `lists_per_author`
as its four record caps. Every bucket, of any kind, uses
`host_interned_lifetime`.

The caps apply to every bucket of a kind alike; there is no setting
for one named bucket.

### Admission keys and daily rates

Three daily rates are keyed by the author's **admission key**, which is
derived as follows (first match):

1. the key the resolver stored for the author, if any;
2. resolved, on a large host: `did:<did>`;
3. resolved, on another host: `bucket:<registrable domain>`;
4. not resolved, `did:web`: `bucket:<registrable domain of the DID>`;
5. not resolved, `did:plc`: `unresolved:<did>`.

A key that starts with `bucket:` has the per-bucket rate; every other
key has the per-DID rate. Days are UTC days.

- **Admissions.** Only a listblock insert (or a change of its subject)
  that would admit a list spends admission rate. Over the limit the
  listblock is stored uncounted and its author gets a `capped` debt.
  A self-hosted PDS with thousands of accounts shares one
  `bucket:` key; an account on a large host has 200 a day of its own.
- **Interning.** Every newly created `actors` row (a subject, member
  or owner seen for the first time) and every placeholder `lists` row
  is charged, in the transaction that creates it, to a **cause key**:
  the writing author's admission key, or, for rows created by work
  with no record author behind it, the requester (`token:<id>` for an
  API key, `admin`). Over the rate the write is refused with a `capped`
  debt; for a list item, the list is marked `capped` and flagged for a
  refresh. The write is eligible again the next UTC day. Charges are
  never refunded. An author's **own** `actors` row is always created
  (a debt needs it to exist). Rows created for DIDs enumerated by the
  network sweep are charged to `system:sweep`, which has no rate and
  is bounded by the size of the source.
- **History.** See [history.md](history.md).

`actors` rows are never deleted, so a daily rate alone would let one
hostile host grow the table for months. That is what
`host_interned_lifetime` bounds: once a bucket has caused that many
rows, further interning by it is refused until the limit is raised.

The bound applies to the buckets of a host (`d:`, `ip:`) and to a
`did:` bucket. The shared `unresolved` bucket has none. Every
`did:plc` author starts there, the count of a bucket never goes down,
and it is not moved when an author resolves, so a bound on it would be
reached once by ordinary traffic and would then refuse every new
author for good. An unresolved author is bounded by its own daily
rate, by the short time it stays unresolved, and, if it never
resolves, by the bucket of its own that it gets at the third failure.

**A record found by discovery is its author's.** Discovery, and the
record check of a list, write records with no firehose event behind
them. What such a write interns is charged to the requester. The
record itself is stored in its author's buckets and passes its
author's gates like any other write: the budget gate, the hard
ceiling and the host caps. So a job requested with an API key stores
nothing for an author on an ordinary host while the budget gate is
closed, and a list record found by the record check counts against
`host_lists`.

Placeholder `lists` rows, unlike `actors` rows, are removed by a
nightly cleanup once nothing references them and their record is still
unknown; see [storage.md](storage.md).

### What happens to a refused write

A refused write is not stored, and it is not forgotten:

1. A row is written to `relist_debt` for the **author**, with reason
   `capped` (a per-author cap or a daily rate) or `refused` (a bucket
   cap, the lifetime interning bound, the budget or the ceiling), the
   cap type, and the witness time of the refusal. A refused list item
   for a tracked list also marks the list `capped`.
2. The refusal is counted in `farsight_abuse_capped_total{kind}`, shown
   on the admin dashboard, and reported in `coverage.exceptions` of
   every query it can affect.
3. The backfill feeder re-lists the author's repository when the cause
   has gone:

   | Debt | Eligible when |
   |---|---|
   | `refused`, bucket cap | the bucket's `capped_mask` bit is clear |
   | `refused`, budget | the budget gate is open (large hosts: always) |
   | `refused`, ceiling | the ceiling gate is open |
   | `capped`, daily rate | a later UTC day, with rate left for the key |
   | `capped`, per-author cap | the author's count is under 90% of the cap |

   A debt is not fed again within an hour of the author's last run.
4. The debt is deleted by a clean full run of the repository whose
   coverage point is at or after the debt's witness time.

A debt needs no record-level bookkeeping: the author is known at the
moment of refusal, and re-reading the repository finds whatever was
refused. Only the owner bucket's `host_list_items` cap defers the
admission of a list as a whole; see [list-indexing.md](list-indexing.md).

### The storage budget

`storage.budget_bytes` (default 70,000,000,000) is compared once a
minute with `pg_database_size` of the database.

| Usage | Effect |
|---|---|
| ≥ 90% of budget | The network sweep pauses. |
| ≥ 100% | The **budget gate** closes. Creates and updates from authors that are not on a large host (unresolved authors included) are refused with `refused` debts. For those repositories the gated classes of backfill job — tier 2 and tier 3 work, jobs requested with an API key, resyncs and list fetches — apply deletes only. `pending` lists that no fetch run has claimed, and whose owner is not on a large host, are deferred. Deletes, admin-requested jobs and writes from large hosts continue. |
| ≥ 110% | The dashboard shows a critical state. |
| < 95% | The gate reopens (hysteresis). Deferred lists are released oldest first, at a limited rate, and the feeder re-lists authors with `refused` debts. |

While the gate is closed a `storage_refusals` interval is open and the
coverage of the network as a whole is `partial` with reason
`storage_refusal`, because any non-large author may be affected.

The history tables are inside `pg_database_size` and so inside the
budget. A history row is written by a removal, which no gate refuses;
but a removal needs a live row, and creating one is gated, so past the
gate history can grow only by the live rows that already exist.

`pg_database_size` does not shrink after deletes without
`VACUUM FULL` or `pg_repack`. Raising the budget after adding disk is
the expected way out; see [../guide/storage.md](../guide/storage.md).

### The hard ceiling

`storage.hard_ceiling_bytes` (default `0`, meaning 115% of
`budget_bytes`; it must be at least 105% of the budget) is the disk
backstop. At or
above it **all** creates and updates are refused, large hosts
included. Every job the admin did not request applies deletes only;
an admin-requested job still runs, but its creates and updates are
refused like any other. `pending` lists not yet claimed are deferred,
those of large-host owners too. The ceiling gate reopens when usage
falls below 105% of the budget. That is why a ceiling under 105% is
refused at load: its gate would close at the ceiling and reopen on
the next measurement.

The ceiling fires only if growth from large hosts outruns the budget.
It never changes the budget's order: non-large authors are refused
first.

The dashboard and `farsight_storage_budget_ratio` warn at 80% of the
budget, and when a day's growth is more than twice the trailing
average.

**Worst-case storage is the budget plus the growth of large hosts**,
whatever number of accounts, domains or address blocks an attacker
holds elsewhere.

## The safe outbound client

Every request to an address learned from the network — a DID
document's PDS endpoint, a `did:web` host, a handle's domain, a result
from a backlink index — goes through one client
(`farsight_core::net::SafeClient`). It:

- accepts only `https` URLs, with no credentials in them. Plain `http`
  is allowed for the hosts in `net.allow_http_hosts`, which exists for
  development;
- refuses addresses that are not public. In IPv4: unspecified and
  `0.0.0.0/8`, loopback, private (RFC 1918), link-local (which
  includes the cloud metadata address `169.254.169.254`), CGNAT
  (`100.64.0.0/10`), protocol assignments (`192.0.0.0/24`), the 6to4
  relay anycast range (`192.88.99.0/24`), benchmarking
  (`198.18.0.0/15`), multicast, broadcast and `240.0.0.0/4`. In IPv6:
  unspecified, loopback, unique local (`fc00::/7`), link-local,
  site-local (`fec0::/10`), multicast, discard-only (`100::/64`),
  benchmarking (`2001:2::/48`) and local-use NAT64
  (`64:ff9b:1::/48`). An IPv4 address embedded in IPv6 — IPv4-mapped,
  or under the NAT64 prefix `64:ff9b::/96` — is judged as the IPv4
  address it carries. The forms that tunnel to an IPv4 address are
  refused whatever address they carry: 6to4 (`2002::/16`), Teredo
  (`2001::/32`), IPv4-compatible addresses (`::/96`) and
  IPv4-translated addresses (`::ffff:0:0:0/96`). A host in
  `net.allow_http_hosts` is not exempt from any of this;
- connects directly. A proxy named in the environment (`HTTPS_PROXY`,
  `HTTP_PROXY`, `ALL_PROXY`) is not used: a proxy would resolve and
  contact the target itself, past the address check;
- follows at most 3 redirects, and applies every check again to each
  hop. A form `POST` (used by the admin sign-in) follows none: a 3xx is
  returned as it is, so a form is never re-sent to another host. The
  backfill process follows none either: its requests are counted
  against the limits of the host they were made to, and a redirect
  would take one to a host whose limits it never touched;
- bounds time and size: 10 s to connect, 30 s for the whole request, a
  response body of at most 2 MiB (checked against `Content-Length` and
  again while reading);
- closes a connection 10 s after its last request and keeps at most
  two idle connections per host. Handle checks contact a different
  host each time, and connections kept for longer exhaust the
  process's open-file limit;
- identifies itself:
  `farsight/<version> (+https://<server.hostname>; <server.contact>)`.
  Both values must be one line of text; a config with a control
  character in either does not load.

The setup wizard's two connection tests are the exception: they
connect to the Jetstream and Postgres addresses the operator types,
private ones included, and are open only to a setup session
([web-ui.md](web-ui.md#the-wizard)).

### DNS rebinding

A check of a hostname followed by a second, independent resolution at
connect time would let a hostile DNS server answer with a public
address first and a private one second. The client has no such gap:
its HTTP stack resolves names through the client's own resolver, which
drops every forbidden address from the answer and fails the request
when none is left. The address that was checked is the address that is
connected to. A literal IP address in a URL is checked before any
request is made. A new connection resolves, and is checked, again.

Two lookups contact nothing and are not filtered: TXT records for
handle verification (`_atproto.<handle>`), and the address lookup that
assigns a PDS host its address bucket. A host that changes addresses
can therefore move between address buckets; its domain bucket stays
the same.

On top of the client, backfill keeps a pace and a concurrency limit
per host, honours `429` and `Retry-After`, and has a circuit breaker
per host; see [backfill.md](backfill.md).

## The network edge

### Client address

Rate limits for anonymous callers are keyed by client address, so the
address must be one the client cannot choose. Two settings decide it:
`proxy.mode` (`none`, the default; `cloudflare`; `forwarded`) and
`proxy.trusted`, a list of CIDRs.

```
peer := TCP peer address
if peer not in trusted:        client := peer    # every forwarding header ignored
elif mode == cloudflare and CF-Connecting-IP parses:
                               client := CF-Connecting-IP
elif X-Forwarded-For present:  walk right to left, skip entries in trusted;
                               client := first entry not in trusted
                               (if all are trusted: the leftmost)
                               an entry that is not an address ends the walk:
                               client := the entry to its right, or the peer
else:                          client := peer
```

- An `X-Forwarded-For` entry may carry a port (`203.0.113.7:4711`,
  `[2001:db8::1]:4711`), as some proxies write it. An entry that is
  not an address at all is never skipped: everything to its right was
  written by trusted proxies, everything to its left may have been
  written by the client, so stepping over it would let the client name
  its own address. The walk stops there, and the nearest proxy that
  was read stands in; the clients behind it share one rate-limit key.

- An IPv4-mapped IPv6 peer is treated as its IPv4 address.
- From a peer that is not trusted, the headers `Forwarded`,
  `X-Forwarded-For`, `X-Forwarded-Proto`, `X-Forwarded-Host`,
  `X-Real-IP`, `CF-Connecting-IP`, `True-Client-IP` and `CF-Visitor`
  are removed from the request before any handler or log sees them. A
  forged `CF-Connecting-IP` sent straight to the origin is therefore
  ignored, and its sender is rate-limited as themselves.
- `X-Forwarded-Proto` is honoured only from a trusted peer. It decides
  whether cookies get the `Secure` attribute. (The setup wizard, which
  runs before any proxy is configured, honours it for that attribute
  alone and never for the client address.)
- In `forwarded` mode `CF-Connecting-IP` is not read.
- With Cloudflare Tunnel the peer is the local `cloudflared`: trust its
  address, with `mode = "cloudflare"`.

The Cloudflare preset of the setup wizard sets `mode = "cloudflare"`
and trusts Cloudflare's published edge ranges, which are bundled with
a date. `proxy.cloudflare_refresh = true` adds a daily refresh from
Cloudflare's published lists. A refreshed list is held to the bounds
of `proxy.trusted` below, and to public addresses: a list that does
not parse, holds more than 500 ranges, or holds a range broader than
`/8` (IPv4) or `/24` (IPv6) or a private, loopback or link-local one
is refused whole and leaves the previous set in force. A fetched list
that read `0.0.0.0/0` would otherwise make every peer a trusted
proxy.

### Validation of `proxy.trusted`

- Refused at load: any IPv4 prefix shorter than `/8` and any IPv6
  prefix shorter than `/24` (so `0.0.0.0/0` and `::/0` too). Trusting
  so much would let clients forge their address.
- Warned about: a public range that is not inside the bundled
  Cloudflare set ("confirm it belongs to your proxy"), and a
  `proxy.mode` other than `none` with an empty `proxy.trusted`
  (forwarding headers are then ignored).

### Failure modes

| Misconfiguration | Effect | Detection |
|---|---|---|
| Behind Cloudflare, Cloudflare not trusted | Every client appears as a Cloudflare edge; all share a few rate-limit buckets; false `429`s. | When more than half of the requests of a five-minute window (of at least 20 requests) arrive from Cloudflare ranges that are not trusted, the dashboard shows a warning and it is logged. |
| Trust that is too broad | Clients can forge their address. | Refused or warned about at load, as above. |
| Bundled Cloudflare ranges out of date | New edges are not trusted; their clients share a bucket. | The bundle is dated; `proxy.cloudflare_refresh` keeps it current. |
| Behind any proxy that is not in `proxy.trusted` | All callers share the proxy's address: one anonymous bucket and one sign-in bucket. A proxy with a public address also holds at most 128 connections to Farsight at once ([operations.md](operations.md#inbound-connections)). | None in general (the Cloudflare case above is detected). Set `proxy.mode` and `proxy.trusted`. |

Operator steps for Cloudflare — DNS, TLS mode, cache rules, locking
the origin — are in [../guide/cloudflare.md](../guide/cloudflare.md).
Note that a firewall admitting only Cloudflare's ranges still admits
other Cloudflare customers who point a zone at your origin; a Tunnel
or Authenticated Origin Pulls closes that.

### Cache headers

Read responses do not depend on who asks, which is what makes shared
caching safe. Public pages read no cookie and set none, and no request
header changes their body.

| Response | `Cache-Control` |
|---|---|
| `query.getStats` | `public, max-age=60` |
| Other read queries (per DID, per list, `checkBlocks`) | `public, max-age=30` |
| Any read query while `access.reads` is `api_key` or `disabled` | `private, max-age=30` |
| `getBackfillStatus`, `requestBackfill`, admin endpoints | `no-store, private` |
| API errors | `no-store` |
| `/health`, `/livez` | `no-store` |
| `/` as the public home | `public, max-age=60`; `no-store` while rows are held back |
| `/` as the redirect to the admin UI | `no-store, private` |
| `/` as the text page of an API-only instance | `public, max-age=300` |
| `/did/…`, `/list/…` | `public, max-age=30`; `no-store` while rows are held back for a handle check |
| `/card/{did}` | `public, max-age=300` for a complete card: one whose every fetch answered (the account's identity, its handle's check, its profile record), whether or not the account has a handle, an avatar or a creation date to show. `no-store` when a fetch failed, timed out or was not made for want of budget. An admin card is always `no-store, private` |
| `/search`, public error pages | `no-store` |
| `/robots.txt` | `public, max-age=300` |
| Static assets under `/static/` | `public, max-age=3600`, with `X-Content-Type-Options: nosniff`; served in every configuration |
| `/.well-known/atproto-oauth-client-metadata` | `public, max-age=3600` |
| Everything under `/admin`, `/enter`, `/setup` | `no-store, private`; the sign-in callback also sends `Referrer-Policy: no-referrer` |

`RateLimit` and `RateLimit-Policy` headers are sent only on responses
that a shared cache will not replay (errors, `private` and `no-store`
responses). A `public` response carries no per-caller header.

Read endpoints send `Access-Control-Allow-Origin: *` unless
`access.cors` is turned off.

## Access and abuse

### Tokens

- The **admin token** is `fsa_` followed by 43 base64url characters
  (256 bits). Its SHA-256 is stored in the config as
  `auth.admin_token_sha256`. Admin and backfill-control endpoints
  always require a bearer token.
- An **API key** is `fsk_` followed by 43 characters, stored as
  SHA-256 in `api_tokens` with its scopes: `read`, `backfill`,
  `backfill:high`. A key may have its own read rate.
- Tokens are high-entropy, so a fast hash is sufficient; hashes are
  compared in constant time.
- A missing or invalid token gets `401 AuthRequired` with
  `WWW-Authenticate: Bearer`. The routes are not hidden: the lexicons
  are public, and hiding them buys nothing.
- `access.reads` is `public` (default), `api_key` or `disabled`.
  `disabled` turns read queries off for anonymous and API-key callers;
  the admin token and the admin UI still read.
- A change of the admin token's hash signs out every admin session,
  whether it was rotated or written in the config editor.
- A revoked API key stops working at once. The table of live keys is
  read again every 30 seconds, and a read that began before the
  revocation cannot put the key back.
- The metrics listeners are separate ports and take an optional bearer
  token (`metrics.bearer_token_sha256`); empty means no authentication,
  so keep them off the public network.

### Rate-limit classes

Token buckets, in memory. An anonymous caller is keyed by resolved
client address; an authenticated caller by token.

An IPv6 caller draws on three buckets at once: its `/64` at the
class's limit, its `/48` at 4 times that, and its `/32` at 16 times.
A request is admitted only if each has a token. A `/64` is one
subscriber, but a routed `/48` holds 65,536 of them and is free to
have: keyed by `/64` alone, it would be that many callers. The
multiples leave room for the real subscribers of one site or one
provider.

At most 100,000 buckets are held. Beyond that, buckets that are full
again are dropped first, since they hold no state, and then the
quarter used longest ago. The periodic sweep drops only buckets that
are full: a spent bucket of a class that does not refill is kept, so
dropping it never hands its caller a new allowance.

| Class | Sustained | Burst | Keyed by |
|---|---|---|---|
| Anonymous read | `rate_limit.anon_rps` (10/s) | `anon_burst` (50) | address |
| API-key read | `rate_limit.key_rps` (100/s), or the key's own rate | `key_burst` (500), scaled with a key's own rate | key |
| Admin token read | unlimited | | |
| `requestBackfill`, admin | `rate_limit.admin_backfill_rps` (20/s) | 100 | |
| `requestBackfill`, API key | `rate_limit.key_backfill_rps` (5/s) | 20 | key |
| Public search (`ui_lookup`) | `rate_limit.ui_lookup_rps` (1/s) | 5 | address |
| Public page view (`public_ui`) | `public_ui.rate_limit_rps` (5/s) | `public_ui.rate_limit_burst` (20) | address |
| Profile card request (`public_ui_card`) | 2/s | 20 | address |
| Profile card fetches (`public_ui_card_budget`) | `public_ui.card_rps` (4/s) | `public_ui.card_burst` (8) | whole process |
| Handle verification (`public_ui_handle`) | `public_ui.handle_rps` (20/s, at most 200) | the rate, at least 10 | whole process |
| Sign-in attempts (`ui_login`) | 5 a minute | 5 | address |
| Sign-in starts (`ui_login_start`) | 1/s | 10 | whole process; an address with a successful sign-in in the last 7 days is not charged |

A `429` carries `Retry-After`.

The process-wide classes bound what Farsight itself sends out on
behalf of visitors, whatever their number. A card request is charged
to the visitor's own class first and then to the shared card budget,
which public and admin cards both draw on. The handle budget serves
page subjects, search, cards and the background warming worker
together; the worker draws on it only while more than 5 tokens are
left, so requests are never starved by it. The handle pass
(`public_ui.handle_pass_rps`, off by default) has a pace of its own.

### Query bounds

- At most `rate_limit.query_concurrency` (32) read queries run at
  once. A request that waits more than 2 s for a slot gets
  `503 Overloaded` with `Retry-After: 1`.
- **No caller holds every slot.** A quarter of the slots (8 of 32; at
  least one) is never given to anonymous callers, so callers with a
  token always find them free of anonymous load. One anonymous
  address (IPv6: one `/48`) has at most a quarter of the anonymous
  slots in flight (6 of 24), one API key at most half of all slots
  (16); the admin token has no such bound. A request over its
  caller's bound waits for one of that caller's own places, up to the
  same 2 s, so a burst of quick requests from one caller is served a
  few at a time and a caller with slow ones holds only its share.
- Every read runs with `statement_timeout` =
  `rate_limit.query_timeout` (`5s`); a timeout is `503 Overloaded`.
- Public pages have their own render gate,
  `public_ui.query_concurrency` (8, never more than
  `rate_limit.query_concurrency`), with the same 2 s wait. A page's
  queries take their slot as an anonymous API call does, from the
  slots open to anonymous callers, so public pages and anonymous API
  calls together never hold a slot kept for callers with a token. One
  address (IPv6: one `/48`) renders at most half of the render slots
  at once.
- One `checkBlocks` call weighs at most 1,000 lists per account and
  names at most 100 lists per relation
  ([api.md](api.md#querycheckblocks)), so what it reads and builds is
  in proportion to its 101 accounts, whatever they subscribe to.
- A table's count is read at most once in 30 seconds for the same
  table and filters. It is remembered by what was counted, not by the
  address, so adding query parameters the page does not read does not
  make Farsight count again. Such an address is still a page of its
  own to a cache in front of Farsight, and is rendered for it; the
  rate limit and the bound on renders per address are what limit
  that.
- Public tables are 50 rows a page with no `limit` parameter. A
  section's count is exact up to 5,000,000 and stated as "more than"
  beyond, so that a count cannot run to the timeout.

### The public UI

- **Off means absent.** With `access.public_ui = false` a public route
  answers like any unknown route. `public_ui = true` requires
  `reads = "public"`; a config that says otherwise is refused.
- **No way out.** A public page links only to public pages and, if the
  operator configured one, a record viewer. It has no sign-in link and
  names no admin route.
- **No writes.** A public page view never interns a row and never
  queues backfill work.
- **Content Security Policy** on every public response:

  ```
  default-src 'none'; style-src 'self'; script-src 'self';
  img-src 'self'; connect-src 'self'; base-uri 'none';
  form-action 'self'; frame-ancestors 'none'
  ```

  With `public_ui.show_avatars` on, `img-src` is `'self' https:`. There
  is no inline script or style and nothing is loaded from another
  origin except those images. Responses also carry
  `X-Content-Type-Options: nosniff` and
  `Referrer-Policy: same-origin`, and `X-Robots-Tag: noindex, nofollow`
  unless `public_ui.crawlable` is set.
- Text that comes from the network (handles, list names and
  descriptions) is rendered as plain text, without control
  characters, bidirectional overrides or invisible characters
  (zero-width space, word joiner, byte-order mark; a zero-width
  joiner or non-joiner stays only inside a run of a script or an
  emoji sequence that needs it). Two names that differ only by
  characters nobody can see therefore look the same because they are
  shown the same. A handle is shown only once it has been verified in
  both directions, and is removed as soon as a check shows it is no
  longer the account's.
- **Images are named only on public hosts.** The address of an avatar
  or of a list's image is built from the account's own server, which
  the account names itself. It is used only if it is `https` with a
  domain name of two or more labels that is not an IP address and not
  under a private-network suffix; a card for any other server carries
  no image and no host. A page therefore never makes a visitor's
  browser request an address inside the visitor's own network.

### The admin UI

- Every page under `/admin` needs a session; there is no anonymous
  dashboard or lookup. With `access.admin_ui = false` the pages do not
  exist. `admin_ui` is read at start and cannot be changed by an edit
  from within the running process.
- **Sign-in** is ATProto OAuth as the one account named by
  `access.admin_did`. The flow uses PKCE (S256) and DPoP with a key
  made for that one flow. The callback needs both the `state` and the
  flow cookie whose hash was stored with it; the flow is spent on
  first use and expires after 10 minutes. The `iss` of the answer must
  be the issuer the flow started with, the token must be DPoP-bound,
  and its `sub` must be the admin DID. An authorization server whose
  metadata names another issuer than the one it was fetched from is
  refused. At most 256 sign-ins are held in progress; a newer one
  displaces the oldest.
- **A fresh sign-in for what outlasts a session.** A session cookie
  that is stolen is good for up to seven days, and some things would
  let its holder keep control, or keep the damage, afterwards: a
  database, PLC directory, relay, firehose or backlink source of their
  own (`storage.database_url`, `backfill.plc_url`,
  `backfill.relay_url`, `firehose.urls`, `backfill.backlinks.url`), a
  hostname of their own (`server.hostname`), proxy trust that lets
  them forge client addresses (`proxy.*`), a host the outbound client
  may newly reach (`net.*`), a token they know (`auth.*`, `metrics.*`,
  the rotate button), an API key they made or one of the operator's
  they revoked, and a reset of the instance. Each of these, and any
  change that opens `access.*` further or shows more on the public
  pages, is carried out only in a session that signed in at most 10
  minutes ago; an older one is sent through the sign-in again and
  nothing is changed until it returns. See
  [web-ui.md](web-ui.md#a-fresh-sign-in-for-sensitive-actions). Two
  rules close what that leaves. The admin DID is resolved through the
  PLC directory named when the process started, so a changed
  `plc_url` cannot redirect the sign-in itself before a restart. And
  a change of the admin token's hash ends every session, however it
  was made.
- **Cookies** are host-only, `HttpOnly`, and `Secure` whenever the
  request arrived over HTTPS. Whether it did is what a trusted proxy
  says (`X-Forwarded-Proto`): behind a proxy that is not in
  `proxy.trusted`, cookies are set without `Secure`. The session
  cookie is `SameSite=Strict`; it is named `__Host-farsight_admin`
  over HTTPS, a name a browser accepts only with `Secure`, `Path=/`
  and no `Domain`, and `farsight_admin` over plain HTTP (the loopback
  sign-in). The flow cookie is `SameSite=Lax` with
  `Path=/enter`, because the return from the authorization server is a
  cross-site navigation. The stored session key is a SHA-256 over the
  cookie value and the admin DID, so a session does not survive a
  change of `admin_did`. A session ends after 12 hours idle or 7 days.
- **CSRF.** Every state-changing request must satisfy a same-origin check
  (`Sec-Fetch-Site: same-origin` when the browser sends that header;
  an `Origin` whose host equals `Host` when it sends that one) and
  carry the session's form token, compared in constant time. That
  includes `POST /admin/logout`. The two requests that create a
  session have none to carry a token of and pass the same-origin
  check alone: `POST /enter`, which starts a sign-in, and
  `POST /setup`, which presents the setup token.
- **Loopback sign-in is for local clients.** The loopback OAuth
  client is used only when the request's `Host` is `127.0.0.1` or
  `[::1]` **and** its client address is loopback or private. The
  header alone would let any remote client start sign-ins (it could
  not complete one: the account's server decides that). See
  [web-ui.md](web-ui.md#two-client-modes).
- **Content-Security-Policy.** Every admin page, the setup wizard and
  the sign-in page are sent with `default-src 'none'; style-src
  'self'; script-src 'self'; img-src 'self' https:; connect-src
  'self'; base-uri 'none'; form-action 'self'; frame-ancestors
  'none'`. The pages carry no inline script or style, so injected
  markup cannot run or restyle, and they cannot be framed. Images over
  `https` are allowed for the avatars on profile cards. The sign-in
  page alone has `form-action 'self' https:` (plus plain `http` to the
  hosts in `net.allow_http_hosts`, if any), because its form is
  answered with a redirect to the account's authorization server.

### Privacy positions of the public UI

The data is public on the network, but an index makes it easy to find,
so the public pages are deliberately narrower than the API:

- **No record addresses.** Public tables show no `at://` record URI.
- **No copy buttons.** No control on a public page copies an
  identifier.
- **Removed records are not public.** Blocks and list memberships that
  were stored and later removed are admin pages.
- **Mute subscriptions** to a list are private and are not shown; a
  list's "Subscribers" are its listblocks.
- **Inactive accounts.** Deactivated and deleted accounts are in no
  public table and have no page. Suspended and taken-down accounts
  have no page of their own either: the four statuses are withheld
  alike.
- **`public_ui.excluded_dids`** (at most 10,000) withholds accounts:
  no page, no row, and the same neutral notice as for an account that
  is hidden for any other reason, so that exclusion cannot be told
  apart. It changes the public pages only; the API still returns the
  data.
- **No stored images.** Farsight stores which image an account's
  profile uses (`avatar_cache`) and never the image.
- **Visitors and third parties.** With `show_avatars` on, a visitor's
  browser fetches each avatar from the account's own server, whose
  operator then sees the visitor's address; with
  `public_ui.avatar_thumbnails` it is Bluesky's image service that
  sees it. With `show_avatars = false` a visitor's browser talks to
  your instance alone.
- **Link previews and titles** carry no data: every page is titled
  "Farsight", and the preview image is one static file.

### What is off until an operator turns it on

An index of blocks can be used to look people up as well as to enforce
their blocks. The design draws the line in three places:

- **The API never offers it.** Nothing returns everyone an account
  blocks, the records that were removed, or a ranking of accounts
  (see [api.md](api.md#what-the-api-leaves-out-and-why)).
- **The public pages offer it only by a deliberate choice.** The
  public UI is off by default, and turning it on shows what becomes
  public and asks for confirmation. On top of that, three things have
  switches of their own, each off by default:
  `public_ui.show_outgoing_blocks` (the blocks an account has made and
  the lists it subscribes to), `public_ui.show_top_blockers` and
  `public_ui.show_top_blocked` (the home page's rankings; the second
  names the most-blocked accounts on the front page, and its setting
  says so).
- **Some things have no switch.** Removed records, record addresses
  and one-click copying of identifiers are never on a public page.

An instance installed and left alone therefore exposes what an AppView
needs and nothing aimed at finding people. Anything further is a
decision an operator makes for their own instance, with its effect
stated where the decision is made.

The operator's view of all this is in
[../guide/public-ui.md](../guide/public-ui.md).

## Residuals

These remain possible inside the bounds. Each is bounded, and each is
counted where a query or the dashboard can see it.

- **A handful of cheap hosts can fill the budget.** Each host with its
  own domain and address block has its own buckets, and so its own
  20,000,000 blocks, 5,000,000 list items, 2,000,000 listblocks and
  5,000,000 interned rows. A few such hosts hold more rows than a
  small budget has room for. The budget gate then refuses and counts,
  and network coverage says `partial`. The dashboard lists the ten
  buckets with the most lifetime interning, with their usage.
- **Hostile growth on a large host.** Large hosts are exempt from
  buckets and rely on their own write limits and anti-abuse.
  Per-author caps and interning rates still bound each account
  (1,000,000 blocks, and 1,000,000 interned rows a day), but nothing
  bounds the number of accounts. Enough of them can fill the budget
  and reach the hard ceiling, which then refuses everyone.
- **Co-tenants share keys.** Accounts on one non-large PDS share one
  owner key (five pending lists that take effect) and one admission
  key (the daily rate). One tenant can use them up for all.
- **Admitting real lists nobody else blocks.** Attacker accounts can
  listblock real, large lists, up to the trigger cap times the number
  of accounts, and so grow `list_items` — within the bucket caps and
  the budget.
- **Many `unavailable` lists.** A `did:web` owner whose host
  blackholes Farsight can cause many lists to be `unavailable` (an
  error is not a not-found). They are counted honestly and bounded by
  the admission rate, but the instance-wide count is under the
  attacker's influence.
- **A list declared dead by its own host.** An owner's PDS that
  answers `RecordNotFound` to Farsight alone, for a list Farsight has
  never seen, makes the list `dead`. It needs the owner's own PDS, and
  the owner could delete the list anyway.
- **The warming queue.** Anyone who loads pages fills the handle
  warming queue (2,000 DIDs). A flood of page views, each within its
  own class, can keep it full of accounts nobody else wants; warming
  then helps no one. It cannot take the reserve from requests and
  cannot raise the outbound rate. The hosts contacted are the PLC
  source and the hosts that handles name, through the safe client.
- **The shared card budget.** Visitors who keep it empty leave
  everyone, the operator included, with short cards (stored handle and
  DID). The lookup pages are not affected.
- **The sign-in budget.** The process-wide bucket refills at 1 start
  a second, 60 a minute, and one address may attempt 5 a minute: 12
  addresses, each within its own limit, keep it empty, and a sign-in
  from an address with no success in the last 7 days then gets `429`.
  They cannot quickly displace a sign-in in progress: the bucket
  admits at most 10 + *t* starts in *t* seconds and 256 are held, so
  one is safe for about four minutes (246 s). The admin token API is
  not affected.
- **Connection slots.** One address (IPv6: one /48) holds at most 128
  of the 2,048 connections of the public listener, so a single host
  cannot fill it with requests sent a byte at a time. Sixteen
  addresses, each at its bound, still can, for as long as they keep
  reopening connections that are closed after 120 seconds. Peers with
  an address that is not public are not bounded, so behind a proxy or
  a NAT that hides client addresses the bound is the proxy's to keep.
- **Row order is as good as the witness clock.** The time a row is
  ordered by cannot be set by the record's author, but it can by
  whoever runs the firehose source. While the firehose is
  disconnected the witness clock stands still, and records stored
  meanwhile sort together at the moment it stopped.

## What caps cost

Every cap trades coverage for a bound. An author past
`blocks_per_author` has made more blocks than Farsight stores; a list
past `list_items_per_list` has more members than Farsight serves; an
author in a closed bucket is not indexed further until it reopens.

None of this is silent. A cap hit is recorded on its row
(`relist_debt`, the list's state, `host_usage.capped_mask`), counted
in metrics, shown on the dashboard, and — wherever a query can observe
it — reported in that query's `coverage.exceptions`: a capped author
in `cappedAuthors`, a capped list by its state, a closed budget gate
as `storage_refusal` on network scope. A consumer that needs a
complete answer can tell that it did not get one, and for whom.
[coverage.md](coverage.md) defines the fields.
