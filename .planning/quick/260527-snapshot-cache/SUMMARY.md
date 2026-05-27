---
quick_id: 260527-snapshot-cache
slug: snapshot-cache
date: 2026-05-27
branch: quick/260527-snapshot-cache
status: complete
test_baseline: 37/38 passing (1 pre-existing flake)
test_after: 41/42 passing (same flake; 4 new cache tests)
---

# SUMMARY — Quick Task 260527-snapshot-cache

## What shipped

Three atomic commits on `quick/260527-snapshot-cache`:

| Commit | Subject | Files |
|---|---|---|
| `3c4b62f` | `perf(snapshots): re-introduce bounded LRU cache on snapshot pickle loads` | `app/settings.py`, `app/snapshots.py`, `.env.example` |
| `6edfbe9` | `test(snapshots): cover bounded LRU cache on snapshot pickle loads` | `tests/test_snapshot_cache.py` (new) |
| _this_ | `docs(quick-260527-snapshot-cache): PLAN + SUMMARY` | `.planning/quick/260527-snapshot-cache/*` |

## Why

Post-bulk-lookup verification showed `/members/latest` returning 499 on ~28% of requests over a 10-min window, plus a one-off `/health` outlier at 2.2 s — both symptoms of FastAPI threadpool saturation from `load_snapshot_map_cached` re-reading the ~27 MB pickle from disk on every call. The `(path, mtime)` cache signature on `_load_snapshot_map_binary` was already in place from when an `@lru_cache` previously wrapped it; commit `fd9a6a3` removed the wrapper without replacement, leaving the signature dead.

## What changed

- **`app/snapshots.py`** — re-wrapped `_load_snapshot_map_binary` with `functools.lru_cache(maxsize=settings.snapshot_map_cache_size)`. The `(path, mtime)` key means snapshot rewrites by the scheduler auto-invalidate the entry the next time a request reads them.
- **`app/settings.py`** — added `snapshot_map_cache_size: int = Field(4, ge=1, le=64, ...)`.
- **`.env.example`** — documented `SNAPSHOT_MAP_CACHE_SIZE=4`.
- **`tests/test_snapshot_cache.py`** — 4 new tests:
  - cache hit on second call with unchanged file
  - invalidation when mtime advances
  - eviction at maxsize boundary (size-agnostic)
  - CSV fallback on corrupt pickle (regression guard)

## Memory bound

Worst-case in-process cache memory ≈ `SNAPSHOT_MAP_CACHE_SIZE × ~27 MB`:

| Size | Worst-case | Use case |
|---|---|---|
| 2 | ~54 MB | Today × sports_car + formula_car |
| **4** (default) | **~108 MB** | Today + yesterday × sports_car + formula_car |
| 8 | ~216 MB | Last 4 days × 2 categories — useful for delta queries spanning longer windows |
| 16 | ~432 MB | Last 8 days × 2 — bumping room on a larger droplet |

The cap is enforced by `functools.lru_cache`; least-recently-used entries are released and garbage-collected.

## Deployment notes

`SNAPSHOT_MAP_CACHE_SIZE` is read at process startup from `.env` (no entry in `.env` falls back to the default of 4). After deploy:

1. `cd /root/drivers-scout && git pull`
2. `sudo systemctl restart drivers-scout.service`
3. Verify warm-up: first `/members/latest` request after restart still pays the disk-read cost (`cache_info().misses` increments). The second request for the same path should be near-instant.
4. Watch `nginx /var/log/nginx/access.log` for `/members/latest` 499 rate — expect a sharp drop from ~28% to near zero.

To raise the cache size later without a code change: edit `/root/drivers-scout/.env`, add `SNAPSHOT_MAP_CACHE_SIZE=8` (or higher), restart the service.

## Out of scope (still on the follow-up list)

- per-request `IRacingClient` instantiation
- sync `FileHandler` blocking the event loop
- arms-website `.next/cache` EACCES storm
- drivers-scout running as root from `/root/drivers-scout/`
- pre-existing `test_sync_members_from_latest_snapshots` cross-file engine flake

## Verification

```bash
.venv/bin/python -m unittest discover -s tests
# Ran 42 tests in 0.161s
# FAILED (failures=1)  ← pre-existing test_sync_members_from_latest_snapshots
```

41/42 passing — all 4 new cache tests + all previously-passing tests.
