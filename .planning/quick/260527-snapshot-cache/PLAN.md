---
quick_id: 260527-snapshot-cache
slug: snapshot-cache
description: Bounded LRU cache for snapshot pickle loads so /members/latest doesn't re-read 27 MB from disk per request.
branch: quick/260527-snapshot-cache
date: 2026-05-27
status: in-progress
---

# Quick Task 260527-snapshot-cache: bounded snapshot pickle cache

## Problem

Post-bulk-lookup deploy verification surfaced the next bottleneck: 28% of `/members/latest` requests over a 10-min window returned 499 (client timeout), and `/health` briefly took 2.2 s on one probe due to threadpool saturation. Root cause is unchanged from `CONCERNS.md`: `app/snapshots.py:load_snapshot_map_cached` re-reads the ~27 MB pickle from disk on every call. The original `@lru_cache` was removed in commit `fd9a6a3` and never replaced — the path/mtime signature on `_load_snapshot_map_binary` is dead weight today.

Each call:
- Holds a FastAPI threadpool worker for the full disk read + unpickle (~hundreds of ms each, more under disk contention)
- Re-allocates the ~411k-entry dict each time
- Compounds with `/leaders/growers` which calls it twice per request (start + end snapshot)

The 24h-series caller chunks 200 cust_ids into 4 parallel `/members/latest?cust_ids=...` calls per page render. 4 concurrent renders ≈ 16 pickle loads queued, threadpool saturates, downstream requests (including `/health`) wait.

## Approach

Re-introduce the LRU cache on `_load_snapshot_map_binary(path: str, mtime: float)`. The `(path, mtime)` key auto-invalidates when a new snapshot is written (mtime changes). Bounded by `SNAPSHOT_MAP_CACHE_SIZE` (default 4) so worst-case memory is ~4 × 27 MB ≈ 108 MB — well within the droplet's 2 GB even when the box also runs arms-website, ar-media-api, and Postgres.

## Scope — 2 atomic commits

### Commit 1: bounded snapshot pickle cache

- `app/settings.py`: add `snapshot_map_cache_size: int = Field(4, ge=1, le=64, description="Max number of snapshot pickle maps held in memory.")`.
- `app/snapshots.py`: wrap `_load_snapshot_map_binary` with `functools.lru_cache(maxsize=settings.snapshot_map_cache_size)`. The function already takes `(path: str, mtime: float)` precisely so the cache key auto-invalidates on file rewrite.
- `.env.example`: document the new env var.

**Files modified:** `app/settings.py`, `app/snapshots.py`, `.env.example`
**Tests:** none new in this commit (covered in commit 2).

### Commit 2: tests

- `tests/test_snapshot_cache.py` (new): cover
  - cache hit on second call with unchanged file (single disk read)
  - cache invalidation when file mtime changes
  - cache eviction when more than `maxsize` distinct paths are loaded
  - `load_snapshot_map_cached` falls back to CSV on pickle-load failure (existing behaviour — guard against regression)

**Files modified:** `tests/test_snapshot_cache.py` (new)

## Constraints

- Backward-compatible: `load_snapshot_map_cached(path: Path)` signature unchanged.
- No deployment in scope; ends at "branch ready for merge".
- Default cache size 4 — conservative, easy to tune via env once observed.

## Out of scope

- Caller-side `CHUNK_SIZE` increase (would also help; orthogonal)
- Async pickle loading
- Switching off pickle to msgpack/JSON
- The pre-existing `test_sync_members_from_latest_snapshots` flake

## Verification

- `python -m unittest discover -s tests` must show ≥36/37 passing (matching prior baseline + new tests).
- Post-deploy: re-check nginx 499 rate on `/members/latest` — expect drop to ~0% under similar load.
