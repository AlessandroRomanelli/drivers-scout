---
quick_id: 260527-bulk-lookup
slug: bulk-lookup
date: 2026-05-27
branch: quick/260527-bulk-lookup
status: complete
test_baseline: 20/21 passing (1 pre-existing flake)
test_after: 36/37 passing (same pre-existing flake; 16 new tests added)
---

# SUMMARY — Quick Task 260527-bulk-lookup

## What shipped

Six atomic commits on `quick/260527-bulk-lookup`:

| # | Commit | Files | Purpose |
|---|--------|-------|---------|
| 60d5d83 | `perf(db): add display_name index + enable WAL journal mode` | `app/db.py`, `app/models.py` | Index `display_name` + `lower(display_name)`; WAL mode for read/write concurrency. |
| c3d3c0b | `feat(members): add display_name_folded column for diacritics-insensitive lookup` | `app/models.py`, `app/services.py` | Add `display_name_folded` column + `fold_name()` helper + sync population + startup batched backfill. |
| b6efe02 | `fix(api): escape ILIKE wildcards and cap q length in /members/search` | `app/api.py` | Escape `\\`, `%`, `_` in the user term; cap `q` at 64 chars. |
| 6fc9da7 | `feat(api): POST /members/lookup bulk resolver` | `app/api.py`, `app/schemas.py`, `openapi.yaml` | New bulk endpoint with exact + folded resolution, max 500 names. |
| 47e5822 | `fix(db): server-side default for Member.created_at` | `app/models.py` | `server_default=CURRENT_TIMESTAMP` so raw `INSERT OR IGNORE` actually persists rows. |
| 36516a6 | `test: cover /members/lookup and /members/search hardening` | `tests/test_members_lookup.py` (new), `tests/test_member_search.py`, `tests/test_member_sync.py` | 16 new tests covering validation, exact/folded match, ambiguity, license gating, wildcard escaping, folded-column population. |

## Why this fixes the production hang

`/members/search` was getting hammered by `24hseriesesports-website`'s roster resolver — 200 fan-out calls per page render, each doing `Member.display_name.ilike("%full name%")` against 532k SQLite rows with no index. Full table scan × ~3 req/sec sustained = FastAPI threadpool exhausted, every other endpoint blocked.

After this change:

1. **Index on `display_name` and `lower(display_name)`** — the existing `/members/search` ILIKE path becomes index-friendly for prefix-anchored terms; the new bulk endpoint hits the `lower()` indexed expression for sub-millisecond resolution.
2. **`display_name_folded` column + index** — the bulk endpoint resolves diacritics-folded names ("Muller" → row "Müller") in the same single query, no extra round trips.
3. **`POST /members/lookup`** — caller goes from 200 parallel single-name searches to 1 batched call returning a map. Effective request rate against the database drops by ~200×.
4. **WAL journal mode** — the scheduler's 6-hourly `sync_members_from_snapshots` writer no longer blocks read endpoints holding shared connections. The 11:55 UTC sync that hung today (last logged before the 12:11 reboot) is the exact failure mode WAL removes.
5. **Wildcard escaping** — `q=%` no longer turns into a "match anything" scan.
6. **Server-side default on `created_at`** — pre-existing latent bug: every freshly created members table silently dropped every row from `sync_members_from_snapshots` because raw `INSERT OR IGNORE` violated the `created_at NOT NULL` constraint. The dict-count log line ("Upserted 532665 members") masked it. Production was unaffected because its members table predates the column, so `metadata.create_all` never added it there. New deploys would have walked into this with an empty table; this commit closes that gap.

## Bulk endpoint contract

```http
POST /members/lookup
Authorization: Bearer <license-key>   # or X-License-Key: <key>
Content-Type: application/json

{
  "names": ["Lukas Lindqvist", "Müller", "Pablo H Santos", ...],
  "category": "sports_car"
}
```

```json
{
  "resolutions": [
    {"query": "Lukas Lindqvist", "match_type": "exact",  "cust_id": 419877, "display_name": "Lukas Lindqvist", "location": "FI"},
    {"query": "Müller",          "match_type": "exact",  "cust_id": 12345,  "display_name": "Müller",          "location": "DE"},
    {"query": "Pablo H Santos",  "match_type": "folded", "cust_id": 1114497, "display_name": "Pablo H. Santos", "location": "BR"},
    {"query": "Unknown Driver",  "match_type": null,     "cust_id": null,    "display_name": null,             "location": null}
  ]
}
```

- Max 500 names per request (422 if exceeded).
- Each name 1–200 chars after strip (422 if empty post-strip or over 200).
- `match_type: "exact"` = case+whitespace-insensitive equality; `"folded"` = NFD-strip + lowercase equality; `null` = no match OR ambiguous (multiple rows tied at one key).
- Results returned in the same order as the input.

## Caller migration (separate PR, not in this task)

`24hseriesesports-website/src/lib/dal/entries-roster/resolver.ts` should batch its 200 `resolveDriverByName` calls into a single `/members/lookup` request, then fall back to the existing single `/members/search` only for the names that bulk lookup returned `match_type: null` (handles the numeric-suffix tolerance case "Martin Toth" → "Martin Toth2").

## Deployment notes

On the droplet:

1. `cd /root/drivers-scout && git pull` (or however deploys happen — the unit just runs `python -m app`).
2. Restart `drivers-scout.service`. The `lifespan` startup runs `init_db()` which creates the new indexes (`CREATE INDEX IF NOT EXISTS` semantics via `metadata.create_all`) and starts the one-shot `_backfill_display_name_folded`. For 532k rows the backfill runs in batches of 5,000; expect roughly a minute of warm-up work logged to `/root/drivers-scout/drivers-scout.log`.
3. `LICENSE_ADMIN_SECRET` is already set in `/root/drivers-scout/.env` (confirmed during investigation), so license gating is active.
4. WAL adds two sidecar files (`iracing_stats.db-wal`, `iracing_stats.db-shm`) — make sure backups/copy operations include them or stop the service before copying.

## Known issues left in place (out of scope)

- `test_sync_members_from_latest_snapshots` is still brittle. The engine is built at module import time and bound to whichever test file imports `app.db` first; subsequent test files' `os.environ["DATABASE_URL"]` assignments don't rebind the engine. Fixing this requires a `conftest.py` with engine isolation per file — out of scope.
- `/members/latest` still loads the full ~27 MB pickle into the threadpool on every cache miss; the bulk endpoint on the members table doesn't affect it.
- `IRacingClient` is still instantiated per request (full OAuth login on every cache miss).
- Sync `FileHandler` is still on the event loop.
- drivers-scout still runs as root from `/root/drivers-scout/` with `RestartSec=300`.
- nginx rate-limit on `/members/search` was discussed as an emergency stop-gap and is not part of this task.

## Verification

```bash
.venv/bin/python -m unittest discover -s tests
# Ran 37 tests in 0.158s
# FAILED (failures=1)  ← test_sync_members_from_latest_snapshots (pre-existing flake)
```

All 16 new tests pass. All 20 previously-passing tests still pass.
