# Changelog

All notable changes to Farsight are recorded in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Before 1.0, a minor version may change addresses, settings or the
database schema; each entry says so where it does, and the README's
"Upgrading from an earlier version" has the steps.

No version has been tagged yet. The entries below were written
afterwards from the commit history, and each is linked to the range of
commits it covers.

## [Unreleased]

## [0.5.0] - 2026-10-04

Database schema version 11.

### Added

- The handle pass (`public_ui.handle_pass_rps`, off by default): a
  background worker that checks the handle of every account Farsight
  holds, so that a page's rows have their handles before anyone opens
  it. It keeps its own pace, waits by itself when checks fail, and
  reports its progress as metrics.
- Identity events from the firehose queue the account for a handle
  check ahead of everything else (new table `handle_due`), so a changed
  handle is picked up without anyone viewing the account.
- While the pass is on, lists stored by an earlier version have their
  description and image read in the background, not only when their
  page is opened.

- List pages show the list's description (plain text, no links, at
  most 300 characters) and its image. Farsight stores the text and
  which image it is (three new columns on `lists`); the image itself is
  fetched by the visitor's browser from the owner's server, and only
  with `show_avatars`. A list stored by an earlier version has its
  record read once, the first time its page is opened.
- The account page's header gives the account's age after its creation
  date, in its two largest units ("1 year, 11 months ago"). Profile
  cards state the age the same way.
- The home page shows three totals under the search box: blocks
  indexed, lists tracked and accounts seen.
- A guide to the tabs and the account tags, "How to read a page". It is
  collapsed under its heading on the home page, and behind a button in
  the bar on every other public page.

### Changed

- "Banned" is now "taken down" everywhere: the row tag, the switch
  ("Show taken down accounts"), the heading, and the address, which is
  `?takendown=1`. **`?banned=1` is no longer read.**
- A list page's second tab is "Subscribers" instead of "Blocked By",
  and says that mute subscriptions are private and not shown. Its
  address is `?tab=subscribers` and its page parameter `subscribers`;
  `?tab=listblockers` redirects to the list's first tab.
- Avatars are rounded squares (squircles where the browser draws them)
  instead of circles. The header avatar is larger: 88px, 64px on a
  phone.
- The bar shows Farsight's icon without the name, and the home page's
  heading is "Farsight" without the hostname.
- The home page's bar has no search box and no guide button: the page
  has both. "/" focuses the page's own search box where it has one.
- The default description on the home page is shorter.
- "Last updated" moved into the footer of every page that has one,
  where the "ATProto Block Graph Index" label was.
- The workspace version is 0.5.0 (it was 0.1.0 since the first commit).
- A handle check by the pass that shows the account's document no
  longer names the stored handle, or that the handle now belongs to
  another account, removes the stored handle. An unreachable host still
  leaves it in place.

### Fixed

- Outbound connections are closed ten seconds after their last request
  instead of ninety. Checking handles at a steady rate held one open
  connection per account checked, which reached the process's limit of
  open files about every two minutes; every outbound request then
  failed for a minute.

### Removed

- The "ATProto Block Graph Index" label above the home page's name and
  in the footer.
- The contact line on the home page. `public_ui.contact` remains a
  setting; no public page shows it.
- The line under the home page's search box that repeated the text
  inside it.
- Unused script and styles left from the copy buttons, the section
  links and the stat tiles.

## [0.4.0] - 2026-10-04

Database schema version 9.

### Added

- Verified handles are stored (`handle_cache` table) and survive a
  restart.
- `public_ui.handle_rps` (default 20): how many handle verifications a
  second the public pages may cause.
- Numbered page controls on every public table, 50 rows a page
  (`?page=N`), above and below the table, with as many page numbers as
  fit the row.
- Tabs on account and list pages (`?tab=…`): one table in view at a
  time.
- A History tab on the account page: the handles and hosts the account
  has had, read from the PLC directory when the tab is opened.
- A filter box on each table of the account page (`?find=…`): a DID, or
  part of a handle.
- A switch on each table that includes accounts their host has taken
  down; accounts their host has suspended are shown with a tag.
- The account's avatar, creation date and host in the account page's
  header.
- A phone layout for the public pages.

### Changed

- A public row appears only once Farsight has verified that the handle
  belongs to the account; rows still being verified are counted on the
  page and filled in as they settle.
- Table headings are the count itself, exact up to five million rows,
  with a description under it. "Blocked By Lists" adds up the
  subscriptions to the lists that name the account.
- Rows are more compact (28px), with the date column right-aligned and
  as narrow as its content.
- Row times carry no time zone; each table states the zone once.
- Old cursor addresses (`?bc=…` and the like) redirect to the first
  page of their table. A page number past a table's end is answered
  with its last page.
- The hero tiles and the section links are gone from account and list
  pages.

### Removed

- Every "copy" button on public pages. One-click copying of identifiers
  makes targeted harassment easier.
- "Load more" on public tables, replaced by the page controls.

### Fixed

- Pages name stylesheets and scripts with a fingerprint of the build,
  so a browser never shows a new page with an old stylesheet.
- Profile cards opened from rows near the bottom of a table are no
  longer cut off.

## [0.3.0] - 2026-10-03

### Added

- Public and admin tables are sorted by the time the block was created.
  The four indexes this needs are built by the server after start, in
  the background.
- Handle warming: handles of the accounts on a page are verified in the
  background and filled in.
- `access.admin_ui`: the admin UI has its own switch.
- Profile cards and a handle column on the admin lookup pages.
- A new visual design for the public pages, in light and dark.

### Changed

- **The public UI is served at the root** (`/did/…`, `/list/…`) and the
  admin UI under `/admin`. The old addresses under `/public` redirect.
- `access.ui` is replaced by `access.public_ui` and `access.admin_ui`.

### Fixed

- A profile card no longer opens underneath the sticky bar.
- Public pages carry no inline style, as their content security policy
  requires.

## [0.2.0] - 2026-10-02

### Added

- The public UI: who blocks an account, which listblocked lists name
  it, optionally whom it blocks, and the members and subscribers of a
  list. Off by default (`access.public_ui`), with a confirmation page
  that states what becomes public.
- A record of removed blocks, listblocks and list memberships, shown on
  the admin history pages.
- Profile cards on public pages: avatar, verified handle, DID and
  creation date. The avatar is fetched by the visitor's browser from
  the account's own server, never by Farsight.
- A bar on every public page with the search form and a light, dark or
  system theme choice.
- Links from records to a record viewer on the admin pages
  (`public_ui.record_viewer_url`).
- Admin sign-in with ATProto OAuth, and the `set-admin-did` and
  `admin-did` commands.
- Rate classes for the public pages and the profile cards.

### Changed

- **The admin signs in at `/enter` with their ATProto account.** The
  admin password is gone; the README describes the upgrade.
- Relative times on public pages are written by the page's script and
  kept current; the server renders absolute times only.

## [0.1.0] - 2026-10-01

### Added

- Core types (DID, AT-URI, TID, NSID), record parsing, configuration,
  and an outbound HTTP client that refuses private addresses.
- Storage on PostgreSQL: blocks, listblocks, lists and list items, with
  last-writer-wins apply, list tracking states, per-author and per-host
  caps, counters, a janitor and coverage accounting.
- Firehose ingest from Jetstream, with per-instance cursors and seam
  repair after a reconnect.
- The XRPC API: the stable queries (`getIncomingBlocks`,
  `getIncomingListBlocks`, `getListsNaming`, `getListMembers`,
  `checkBlocks`, `getBackfillStatus`, `getStats`), `requestBackfill`,
  API keys, rate limits and admin procedures, with their lexicons.
- The setup wizard and the admin pages: dashboard, lookup and settings.
- The backfill service: the repository sweep, list fetches, on-demand
  backfill and repair.
- A Docker image and a compose file that runs the server, the backfill
  service and PostgreSQL.

[Unreleased]: https://github.com/skydeval/horizon-farsight/compare/61ca30f...HEAD
[0.5.0]: https://github.com/skydeval/horizon-farsight/compare/380a503...61ca30f
[0.4.0]: https://github.com/skydeval/horizon-farsight/compare/d69003d...380a503
[0.3.0]: https://github.com/skydeval/horizon-farsight/compare/11408c3...d69003d
[0.2.0]: https://github.com/skydeval/horizon-farsight/compare/0207b49...11408c3
[0.1.0]: https://github.com/skydeval/horizon-farsight/compare/8f63563...0207b49
