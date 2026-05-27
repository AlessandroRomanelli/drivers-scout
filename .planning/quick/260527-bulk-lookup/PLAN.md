---
quick_id: 260527-bulk-lookup
slug: bulk-lookup
description: Add POST /members/lookup bulk endpoint and harden /members/search to stop the production /api hang.
branch: quick/260527-bulk-lookup
date: 2026-05-27
status: in-progress
---

# Quick Task 260527-bulk-lookup: bulk member lookup + search hardening

## Problem

`/members/search` is being hammered ~3 req/sec sustained by `24hseriesesports-website` for roster resolution (200 names per page render, parallel). Each call does `Member.display_name.ilike("%term%")` against a SQLite `members` table with 532,665 rows and no index — full table scan per request. SQLite serializes, SQLAlchemy connection pool saturates, FastAPI threadpool exhausts, every other endpoint hangs.

**Evidence (gathered during the investigation phase of this session):**

- nginx access log: 44,556 `/members/search` requests, 42,105 (94.5%) returned 499 (client timeout).
- Sustained 24-168 req/min, ongoing at the time of inspection.
- Localhost curl of `/members/search?q=...` with no auth header timed out at 15s (proves request queue is full even for would-be-401s).
- `journalctl -u drivers-scout` empty: logs go to `/root/drivers-scout/drivers-scout.log` only; the most recent `sync_members_from_snapshots_async` run at 11:55 UTC never logged completion before the 12:11 reboot — likely fighting the search flood for the SQLite write lock.

## Caller pattern (the use case driving the design)

`24hseriesesports-website/src/lib/dal/entries-roster/resolver.ts` resolves a driver list to cust_ids via:

1. Manual override map (4 entries, client-side, unchanged here)
2. Exact match on `display_name` (case + whitespace insensitive)
3. Diacritics-folded match (NFD-strip combining marks)
4. iRacing numeric-suffix tolerance (`"Martin Toth"` → `"Martin Toth2"`)

Steps 2 and 3 are what 95%+ of the names use. Step 4 is rare; can stay client-side as a fallback to a hardened single `/members/search`.

## Approach

Move steps 2 + 3 server-side via a new bulk endpoint backed by a denormalized indexed column. Harden the legacy single-search path. Leave manual overrides + suffix tolerance on the client.

## Scope — 5 atomic commits

### Commit 1: db hardening

- `app/db.py`: enable `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL` via `event.listens_for(engine, "connect")` (SQLite-only, guarded by URL prefix check). This decouples reads from the scheduler's bulk member-sync writes.
- `app/models.py`: add `Index("ix_members_display_name", Member.display_name)` and `Index("ix_members_display_name_lower", func.lower(Member.display_name))` to `Member.__table_args__`.
- `Base.metadata.create_all(engine)` (called by `init_db`) creates both indexes if missing.

**Files modified:** `app/db.py`, `app/models.py`
**Tests:** none new — existing tests must still pass.

### Commit 2: display_name_folded column

- `app/models.py`: add `display_name_folded: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)`.
- `app/services.py`: add `fold_name(s: str | None) -> str | None` helper. NFD-normalize, strip Unicode Mn (combining marks), lowercase, strip whitespace. Must match the caller's `fold()` exactly.
- `sync_members_from_snapshots`: extend the staging table + `INSERT OR IGNORE` to populate `display_name_folded`.
- `init_db`: after `metadata.create_all`, run a one-shot backfill: `UPDATE members SET display_name_folded = LOWER(...) WHERE display_name_folded IS NULL`. Since SQLite can't do NFD strip in SQL, fetch NULL rows in batches and `UPDATE` per row.

**Files modified:** `app/models.py`, `app/services.py`
**Tests:** `fold_name` unit tests (NFD strip, lowercase, trim, None handling).

### Commit 3: harden /members/search

- `app/api.py` `/members/search`:
  - Cap `q` length at 64 chars (raise 422).
  - Escape `\`, `%`, `_` in the user-supplied term; pass `escape="\\"` to `ilike`.
  - Keep current min length 3 and limit cap 100.
- One-line comment: "wildcards are escaped to prevent index-busting and matching anything".

**Files modified:** `app/api.py`
**Tests:** none new in this commit (covered in commit 5).

### Commit 4: bulk lookup endpoint

- `app/schemas.py`: `MemberLookupRequest` (`names: list[str]` with 1..500, each 1..200 after strip; `category: str | None = "sports_car"`), `MemberLookupResolution` (`query`, `match_type: Literal["exact","folded"] | None`, `cust_id`, `display_name`, `location`), `MemberLookupResponse` (`resolutions: list[MemberLookupResolution]`).
- `app/api.py`: `@router.post("/members/lookup")`. Build `exact_keys = {fold_strip_only(n) for n in names}` and `folded_keys = {fold_name(n) for n in names}`. Single query: `SELECT cust_id, display_name, location, display_name_folded FROM members WHERE LOWER(TRIM(display_name)) IN (...) OR display_name_folded IN (...)`. Group rows by both keys; for each input name, prefer unique exact match > unique folded match > null. Ambiguous (multiple rows) → null.
- `openapi.yaml`: add `POST /members/lookup` under `paths`, plus the 3 schemas under `components.schemas`.

**Files modified:** `app/api.py`, `app/schemas.py`, `openapi.yaml`
**Tests:** none new in this commit (covered in commit 5).

### Commit 5: tests

- `tests/test_members_lookup.py` (new): empty body → 422, >500 names → 422, single exact hit, folded hit ("Müller" → resolves to row "Muller"), ambiguous duplicate → null, mix of resolved/unresolved, license gating (missing X-License-Key when `LICENSE_ADMIN_SECRET` is set).
- `tests/test_member_search.py` (extend): `q > 64` → 422, `q = "Foo%Bar"` matches only literal `%`, `q = "a_b"` matches only literal `_`, all existing happy paths still pass.
- `tests/test_member_sync.py` (extend): syncs populate `display_name_folded`.

**Files modified:** `tests/test_members_lookup.py` (new), `tests/test_member_search.py`, `tests/test_member_sync.py`
**Tests:** all of the above plus existing suite must pass (1 pre-existing flake `test_sync_members_from_latest_snapshots` left as-is — out of scope).

## Constraints

- Backward-compatible: `/members/search` response shape unchanged.
- Caller (`24hseriesesports-website`) adopts `/members/lookup` in a separate PR — do not modify the caller.
- No deployment in scope; task ends at "commits on `quick/260527-bulk-lookup` branch ready for merge".

## Out of scope

- `/members/latest` pickle path
- per-request `IRacingClient` instantiation
- sync `FileHandler` on event loop
- `arms-website` `.next/cache` permission issue
- systemd unit hardening / drivers-scout running as root
- nginx rate-limiting (separate emergency stop-gap)
- pre-existing `test_sync_members_from_latest_snapshots` cross-file engine contamination flake

## Verification

Each commit: full `python -m unittest discover -s tests` must show ≥20 passing (the one pre-existing flake is the ceiling). After commit 5: also `21 + N_new_tests` minus the flake.
