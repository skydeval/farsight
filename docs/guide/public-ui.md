# Public UI

Farsight can serve a public lookup site from the same binary: search by
handle, DID, `at://` URI or bsky.app link, then a page per account (who
blocks it, which listblocked lists name it) and per list (members, who
blocks it). It is an independent equivalent of Clearsky's lookup pages.

It is **off by default**, and served at the root of the hostname: `/`,
`/did/<did>`, `/list/<did>/<rkey>`, `/search`. Turn it on in the wizard
or in Settings → Public UI; either way Farsight first shows what
becomes reachable without login and asks you to confirm. It needs
`access.reads = "public"`: a public site in front of a gated API is not
a supported combination. It does not depend on the admin UI: with
`access.admin_ui = false` the instance serves the public site and
nothing else in a browser.

## The home page

With the public UI on, `/` is its home page: the instance's
description, the search box, three totals (blocks
indexed, lists tracked, accounts seen; the counts `getStats` gives) and
a short guide to the tabs and the account tags, which its heading
("How to read a page") opens and closes. The other public pages have
the same guide behind a button of that name in the bar.

With `show_top_blockers` or `show_top_blocked` on, the home page also
has top lists: see "Top lists" below.

With the public UI off, `/` redirects to `/admin`, or, on an instance
with neither interface, shows a few lines of text.

## What it shows, and what it never shows

- **Inactive accounts.** A table leaves out accounts that are
  deactivated or deleted. An account its host has **suspended** is
  shown, tagged "suspended". An account its host has **taken down**
  (tagged "taken down") is left out until the visitor ticks "Show taken down accounts"
  (`?takendown=1`); the heading gives both numbers, "4,114 (6,234 counting
  taken-down accounts)". The switch keeps the page the table is on; an
  address that names a page past a table's end is answered with its
  last page. The page of a suspended or taken-down account
  itself stays withheld.
- **Accounts that are not shown.** Deactivated, suspended, taken-down
  and deleted accounts have no page; which of them appear in rows is
  under "Inactive accounts". You can withhold more accounts with
  `excluded_dids`: they have no page, appear in no row and get the
  same neutral notice. Exclusion changes what the public pages show, nothing
  else: the API still returns the data.
- **Outgoing blocks** (`show_outgoing_blocks`, off by default): the
  blocks an account has made ("Blocking"), and the lists it subscribes
  to as block lists ("Blocking Lists"; only lists this instance
  serves). The admin DID lookup shows the subscriptions whatever this
  is set to.
- **No record addresses.** Public tables do not show `at://` record
  URIs, and no button copies an identifier. The admin lookup pages
  have a copy button for each record, and a link to it if you set
  `record_viewer_url` (a URL template with `{authority}`,
  `{collection}` and `{rkey}`).
- **Removed records are not public.** Blocks and list memberships
  Farsight stored and later removed are on admin pages, reached from
  the DID and list lookups after login.
- **No coverage detail.** A public page prints no coverage level. It
  says "None on record at this instance" for an empty section, says so
  when a list is not indexed, and states "Last updated" once, in its
  footer.
  The admin pages state coverage in full: in "Alerts" and on the lookup
  pages.
- **Search engines** are asked to stay out unless you set `crawlable`.
  Search, cards and error pages are never offered.
- **Link previews** carry a title, a fixed description and one static
  image, never data: a count in a preview is a stale claim with no date.

## Top lists

Two rankings for the home page, each off by default and switched on
separately:

- **Top blockers** (`show_top_blockers`): the 20 accounts that have
  made the most blocks.
- **Most blocked** (`show_top_blocked`): the 20 accounts that the most
  others block directly. Blocks through lists are not counted. This
  names accounts on the instance's front page; consider whether you
  want that.

Each has two tables, "Last 24 hours" and "All time".

- **They are counted in the background.** The page reads the stored
  result and says when it was counted. The 24-hour tables are counted
  every 10 minutes and "Top blockers" of all time every hour. "Most
  blocked" of all time reads every stored block (about a minute and a
  half, and a few GB of temporary files, for 150 million blocks), so it
  is counted once a day. Nothing is counted while both switches are
  off; after switching one on, the first lists appear within a few
  minutes.
- **"Last 24 hours"** means blocks this instance stored in the last 24
  hours whose own date is also within a day of when they arrived, and
  that are still stored. Old blocks read by the backfill do not count,
  nor does a block that was made and removed again. The count starts
  with the version that has this feature: its first day is partial.
- **Counts are of what this instance stores.** An account past its
  per-author storage cap has made more blocks than the list shows.
- **The page's usual rule applies.** Excluded, deactivated, deleted and
  taken-down accounts are left out and the next ones move up; a
  suspended account is shown with its tag.

## What visitors' browsers and Farsight fetch from others

- **Avatars come from the account's own server** (`show_avatars`, on by
  default) unless `avatar_thumbnails` is on: the visitor's browser
  fetches the image there, so that server's operator sees the
  visitor's address. Set `show_avatars` to `false` and visitors'
  browsers talk only to your instance; cards keep everything else.
- **Avatar thumbnails** (`avatar_thumbnails`, off by default). By
  default a visitor's browser loads each avatar, and a list's image,
  as the original upload from the account's own server: about 300 kB
  on average, up to 1 MB. With the setting on it loads a thumbnail of
  a few kB from Bluesky's image service (`cdn.bsky.app`), which a
  browser keeps for a week. That service then sees the visitor's
  address in place of each account's server, and an image appears
  only if the service has it. Either way Farsight stores which image
  an account uses (table `avatar_cache`, read again after a day), so
  a card does not read the profile record every time, and never the
  image.
- **Profile cards.** Resting the pointer on an account in a row (or
  focusing it with the keyboard) opens a card with its avatar, verified
  handle, DID and the date the DID was created, with its age. Farsight
  fetches that from the PLC directory and the account's own server
  when the card is asked for. Of it, it stores the verified handle and
  which image the avatar is, never the image. Touch devices get no
  cards.
- **A list's description and image.** A list page shows the
  description its owner gave it, as plain text (no links, at most 300
  characters), and its image. Farsight stores the text and which image
  it is; the image itself is fetched by the visitor's browser (from
  the owner's server, or as a thumbnail: see "Avatar thumbnails"), and only with
  `show_avatars`.

## Handles

- **Handles.** A row shows the handle only once Farsight has verified
  it in both directions. A background worker checks the accounts a
  page is about to show (`handle_warming_enabled`, on by default) at
  `handle_rps` checks a second (20 by default; each is up to two
  requests, to the PLC directory and to the handle's own host). A
  public table leaves out an account that has not been checked yet,
  says how many it left out, and adds them in place as they pass: at
  20 a second a page of 50 accounts nobody has seen is complete in
  about three seconds. An account whose check finds no valid handle is
  shown as its DID. The result of every check is stored (table
  `handle_cache`) and survives a restart. A handle verified more than
  seven days ago is still shown and is verified again in the
  background; if that fails, the old handle stays. To start over,
  `DELETE FROM handle_cache;`. With `handle_warming_enabled = false`
  nothing does that background work: no row is held back, an account
  not seen before shows as a DID (unless its own page or card is
  opened), and a stored handle is never checked again.
- **The handle pass.** With `handle_pass_rps` above 0 (off by
  default) Farsight checks the handle of every account it holds, in
  the background, so that a page's rows have their handles before
  anyone opens it. Accounts the firehose reports an identity change
  for are checked first (table `handle_due`); then the pass walks the
  accounts in the order they were stored, skipping deactivated and
  deleted ones and those already answered. It keeps its own pace,
  apart from `handle_rps`: at 10 a second, ten million accounts take
  about twelve days, around the clock, and each check is up to two
  requests to servers Farsight does not run (the PLC directory and
  the handle's host). When most checks of a batch establish nothing
  it waits, a minute at first and up to half an hour; an account
  whose check established nothing is checked once more afterwards
  before "nothing to show" is stored for it. A check that
  shows the account's document no longer names the stored handle, or
  that the handle now belongs to another account, removes the stored
  handle; an unreachable host does not. Progress:
  `farsight_handle_pass_position` (the account id reached),
  `farsight_handle_pass_total{outcome}` and
  `farsight_handle_pass_laps_total` on the metrics listener. A restart
  begins the walk again and passes over answered accounts without a
  request. While the pass is on, lists stored before descriptions were
  kept have their record read too, one a second.

## How the pages behave

- **A bar on every page** with search, the guide and a light / dark /
  system theme toggle; the home page's bar has the toggle only, since
  the page has its own search box and guide. Public pages link only to
  other public pages: no login link, no admin route.
- **Tabs and times.** An account or list page shows one table at a
  time, chosen with tabs under its header (`?tab=…`). Row times carry
  no zone; one line under the header names it ("All times are in
  EDT."; UTC without the page's script).
  A list page has "Members" and "Subscribers" (`?tab=subscribers`):
  the accounts that subscribe to the list as a block list. Mute
  subscriptions are private and never appear.
- **Tabs.** Every page's browser tab is titled "Farsight" and carries
  the same icon; the title does not name the account or list on
  screen. The preview tags of a shared link still do.
- **Header.** The account page's header shows the account's avatar,
  when its DID was created, with its age ("1 year, 11 months ago"), and
  which host holds the account. The
  page's script takes all three from the account's profile card (the
  avatar only with `show_avatars`); the visitor's browser fetches the
  image.
- **Headings.** A table's heading is its count: accounts that block
  the account, accounts it blocks, and, for "Blocked By Lists", the
  listblock records on the lists that name it, added up (an account
  that blocks two of the lists counts twice).
- **Newest first.** Tables that show a creation time are sorted by it.
  The time is the author's own claim, so the order uses the earlier of
  that and the moment Farsight first stored the record: a record dated
  in the future sits where it arrived, not at the top of the page.
- **Pages.** Every table shows 50 rows and ends with numbered page
  controls above and below it (`← 1 2 3 … 21 →`): as many page numbers
  as the row holds, with the first and last page and the arrows at the
  two edges. They are plain links with the page in
  the address (`?page=2`, and `lists`, `out`, `outlists`,
  `subscribers` for a page's other tables). The count in a table's heading and its last page are
  the real numbers, counted with the page's filters on every view; if
  that count cannot be read in time the heading shows none and the
  controls end in the next arrow. Turning a page changes the table in
  place when the page's script runs.
- **Filter box.** Each table of an account page has a small box that
  filters it (`?find=…`), across all its pages. A DID keeps that
  account's rows. Part of a handle keeps the accounts whose stored
  handle contains it, so an account whose handle this instance has
  never verified is not found that way; pressing Enter on a whole
  handle resolves it (one lookup from the handle budget) and finds the
  account either way. On "Blocked By Lists" the text is matched
  against the list's name and its owner.
- **History.** A "History" tab, at the right end of an account page's
  tabs, lists the handles and hosts the account has had, as the PLC
  directory's log records them, in two tables side by side, newest
  first. The handles are what
  the account claimed at the time; they are not verified. The log is
  read only when the tab is opened (`?tab=history`): one request to
  the PLC directory, under the profile-card budget.
- **Avatars** are rounded squares (squircles where the browser draws
  them), in the header and on the cards.
- **Times** are sent as absolute UTC and shown in the visitor's own
  timezone by the page's script.
- Pages are safe to cache at the edge (they do not depend on the
  visitor), carry a strict Content-Security-Policy with no inline
  script, and need no JavaScript to read.

## Settings

Every `[public_ui]` key applies on save, without a restart:

```toml
[access]
public_ui = false                 # the toggle

[public_ui]
instance_description = ""         # plain text on the home page
contact = ""                      # "" = server.contact; no public page shows it
show_outgoing_blocks = false
show_top_blockers = false         # home page: the accounts that block the most
show_top_blocked = false          # home page: the accounts that are blocked the most
record_viewer_url = ""            # admin lookup pages; "" = records are not links
show_avatars = true               # false = cards carry no image
avatar_thumbnails = false         # true = small thumbnails from cdn.bsky.app, not originals from each account's server
card_rps = 4                      # cards fetched per second, all visitors
card_burst = 8
show_opengraph_image = true
dark_mode_default = "system"      # "light" | "dark" | "system"
crawlable = false
rate_limit_rps = 5                # page views per second per address
rate_limit_burst = 20
query_concurrency = 8             # concurrent page renders
handle_cache_ttl = "1h"           # in memory; the stored copy refills it
handle_warming_enabled = true     # verify handles of shown accounts in the background
handle_rps = 20                   # handle checks per second, whole instance; 1-200
handle_pass_rps = 0               # background checks of every account, per second; 0 = off, at most 200
excluded_dids = []                # at most 10,000
```

`handle_warming_enabled` and `record_viewer_url` also apply to the admin
lookup and history pages, whether or not the public UI is on. There a
signed-in admin additionally gets profile cards on account links, and
the removed-records tables have a "First seen" column (when Farsight
first stored the record).

Search shares the lookup rate (`rate_limit.ui_lookup_rps`, 1 per second
per address). Cards have their own: 2 per second per address, and
`card_rps` / `card_burst` for the whole instance. Each card makes
Farsight send at most three requests (the PLC directory, the handle's
host, the account's server); these are not counted in
`backfill.plc_rps`, so lower one of the two if your PLC source has a
tight limit. With the budget used up, cards show the DID only. A PLC
mirror must serve `/{did}/log/audit` for cards to show a creation date.
