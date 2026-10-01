# Jetstream zstd dictionaries

These dictionaries are copied unmodified from the Jetstream source
(`internal/subscribe/dictionaries/`, commit
`3fa54fdbb0f47ad3aa43de78a6fbbd8dc362f81d`). Jetstream frames are
compressed against them, so a client must hold the same bytes.

| File | Used for | zstd dictionary ID | SHA-256 |
|---|---|---|---|
| `legacy_subscribe.zdict` | v1 `/subscribe?compress=true` | from the header | `b8d77b46933aceecbdd5275a01241efc985c970c78162b8150cf67a26522f70c` |
| `subscribe_events_20260811.zdict` | v2 `subscribeEvents?zstdDictionary=<id>` | from the header | `f631a689a829b84b042ba45bcd7a7c2f792467192c5832fcfc1fc48afbec96aa` |

Copyright (c) 2022-2026 Bluesky Social PBC, and Contributors. Licensed
under the MIT license or the Apache License, Version 2.0, at your option
(the same licenses as this repository; see `LICENSE-MIT` and
`LICENSE-APACHE` at the repository root).
