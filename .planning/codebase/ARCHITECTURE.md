<!-- refreshed: 2026-05-27 -->
# Architecture

**Analysis Date:** 2026-05-27

## System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                     Static Dashboard (UI)                    │
│  `index.html`, `assets/index.js`, `assets/subscription.js`   │
│  `assets/style.css`  — served by external reverse proxy      │
└────────────────────────────┬────────────────────────────────┘
                             │ fetch /drivers-scout/api/...
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                FastAPI Application (uvicorn)                 │
│                        `app/main.py`                         │
├──────────────────┬──────────────────┬───────────────────────┤
│  public_router   │     router       │   AsyncIOScheduler    │
│  `app/api.py`    │   `app/api.py`   │   `app/scheduler.py`  │
│  (no license)    │ (license-gated)  │  (cron, in-process)   │
└────────┬─────────┴────────┬─────────┴──────────┬────────────┘
         │                  │                     │
         ▼                  ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│                     Business Services                        │
│                     `app/services.py`                        │
│  (fetch_and_store, get_top_growers, get_irating_delta,       │
│   get_latest_snapshot[s], sync_members_from_snapshots)       │
└────┬─────────────────────┬───────────────────────┬──────────┘
     │                     │                       │
     ▼                     ▼                       ▼
┌──────────────┐   ┌─────────────────┐   ┌────────────────────┐
│ iRacing API  │   │  CSV/PKL files  │   │   SQLite (ORM)     │
│ Client       │   │  on disk        │   │  `app/db.py`       │
│ `app/        │   │  `app/          │   │  `app/models.py`   │
│ iracing_     │   │  snapshots.py`  │   │  (members,         │
│ client.py`   │   │  → SNAPSHOTS_DIR│   │   licenses,        │
│              │   │   /<category>/  │   │   subscriptions)   │
│              │   │   YYYY-MM-DD.csv│   │                    │
│              │   │   YYYY-MM-DD.pkl│   │                    │
└──────────────┘   └─────────────────┘   └────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| ASGI bootstrap | Configure logging, run uvicorn, register lifespan hooks | `app/main.py` |
| Module entrypoint | Allow `python -m app` invocation | `app/__main__.py` |
| One-off CLI | Run a single fetch without starting the server | `app/fetch_once.py` |
| Public router | Health, license status, admin endpoints | `app/api.py` (`public_router`) |
| Licensed router | Member/leader queries, subscriptions | `app/api.py` (`router`) |
| License auth | Validate `X-License-Key` / `Authorization: Bearer` headers | `app/auth.py` |
| License repo | CRUD for `License` rows, key generation | `app/license_repository.py` |
| Member repo | Upsert helpers for `Member` rows | `app/repository.py` |
| Services layer | All business logic: fetch, cache, delta, top growers | `app/services.py` |
| iRacing client | OAuth `password_limited` flow, CSV download, throttling | `app/iracing_client.py` |
| Snapshot store | CSV/PKL persistence + lookup on disk | `app/snapshots.py` |
| Scheduler | APScheduler cron jobs (fetches + Discord delivery) | `app/scheduler.py` |
| Settings | Pydantic-settings env loader | `app/settings.py` |
| DB engine | SQLAlchemy engine + session context manager | `app/db.py` |
| ORM models | `Base`, `Member`, `License`, `Subscription` | `app/models.py` |
| Request schemas | Pydantic models for Subscription request/response | `app/schemas.py` |

## Pattern Overview

**Overall:** Single-process FastAPI service with co-located APScheduler, layered as routers → services → (iRacing client + disk snapshots + SQLAlchemy ORM).

**Key Characteristics:**
- File-system as the primary durable store for iRacing data (dated CSVs and pickled `cust_id`→row maps per category), backed by a small SQLite DB for relational concerns (members, licenses, subscriptions).
- All I/O against the iRacing API is async (`httpx.AsyncClient`); SQLAlchemy is synchronous and uses `fastapi.concurrency.run_in_threadpool` for offloading CPU-heavy snapshot computations.
- Two-tier authentication: public endpoints (health, license status) plus an admin-secret gate (`X-Admin-Secret`) and a license-key gate (`X-License-Key`) for the consumer-facing endpoints.
- Module-level state for in-memory caches (`_top_growers_cache`, `_latest_snapshot_cache` in `app/services.py`; `scheduler` singleton in `app/scheduler.py`) guarded by `asyncio.Lock` where needed.

## Layers

**Presentation / HTTP:**
- Purpose: Route declarations, request validation, header parsing, dependency wiring.
- Location: `app/api.py`
- Contains: Two `APIRouter` instances (`public_router`, `router`), admin guard helpers, query parsers.
- Depends on: `app.services`, `app.auth`, `app.license_repository`, `app.scheduler`, `app.schemas`, `app.models`.
- Used by: `app.main:app` (FastAPI) and per-test mini-apps in `tests/`.

**Auth:**
- Purpose: License key extraction, admin secret check, exemption rules.
- Location: `app/auth.py`
- Contains: `get_active_license`, `require_license`, `EXEMPT_PATHS = {"/health"}`.
- Depends on: `app.db.get_session`, `app.models.License`, `app.settings`.

**Service / Domain:**
- Purpose: All business logic — fetching snapshots, computing iRating delta and top growers, syncing members, cache management.
- Location: `app/services.py`
- Contains: Async public functions (`fetch_and_store`, `get_latest_snapshot`, `get_latest_snapshots`, `get_irating_delta`, `get_top_growers`, `sync_members_from_snapshots_async`), private helpers (`_ensure_snapshot`, `_download_snapshot`, `_next_cache_expiry`), `init_db`.
- Depends on: `app.iracing_client.IRacingClient`, `app.snapshots`, `app.db`, `app.models`, `app.settings`.

**Integration:**
- Purpose: Outbound calls to iRacing OAuth + data endpoints with throttle, retry, refresh-on-401.
- Location: `app/iracing_client.py`
- Contains: `IRacingClient`, `TokenInfo` dataclass, `normalize_row`, `normalize_rows`.

**Persistence (Disk):**
- Purpose: Write and read dated snapshot artifacts.
- Location: `app/snapshots.py`
- Contains: `snapshot_path`, `snapshot_map_path`, `store_snapshot` (writes both `.csv` and `.pkl`), `load_snapshot_rows`, `load_snapshot_map`, `load_snapshot_map_cached` (binary-first), `find_closest_snapshot`, `resolve_snapshot_path`, `get_oldest_snapshot_date`.

**Persistence (SQL):**
- Purpose: Relational data for members, licenses, subscriptions.
- Location: `app/db.py`, `app/models.py`, `app/repository.py`, `app/license_repository.py`
- Engine: SQLAlchemy 2.x with SQLite (`sqlite:///./iracing_stats.db` by default), `check_same_thread=False`.

**Background / Scheduler:**
- Purpose: Cron-driven snapshot fetches and weekly Discord deliveries; reuses service-layer functions.
- Location: `app/scheduler.py`
- Contains: `scheduler` (`AsyncIOScheduler` module singleton), `scheduled_job`, `deliver_discord_subscriptions`, `deliver_discord_subscriptions_guarded`, `start_scheduler`, `shutdown_scheduler`.

## Data Flow

### Snapshot fetch (scheduler-driven)

1. APScheduler cron trigger fires at minute 55 of UTC hours 23, 5, 11, 17 (`app/scheduler.py:28-29,279-291`).
2. `scheduled_job()` invokes `fetch_and_store("sports_car")`, sleeps 60s, then `fetch_and_store("formula_car")`, sleeps 60s, then `sync_members_from_snapshots_async()` (`app/scheduler.py:43-56`).
3. `fetch_and_store` constructs an `IRacingClient`, then for each target category awaits `_download_snapshot` which calls `client.download_category_csv(category)` (`app/services.py:192-217`, `app/iracing_client.py:200-212`).
4. CSV bytes are written via `store_snapshot(category, snapshot_day, content)` which writes both `<date>.csv` and `<date>.pkl` under `SNAPSHOTS_DIR/<category>/` (`app/snapshots.py:67-87`).
5. Counts per category are returned and logged.

### Top growers query (request-driven)

1. Client request → `GET /leaders/growers` (license-gated) (`app/api.py:272-311`).
2. `get_top_growers(category, days, limit, min_current_irating, start_date, end_date)` computes effective range, clamps to `get_oldest_snapshot_date(category)`, and consults `_top_growers_cache` keyed by `(category, start, end, limit, min_ir)` (`app/services.py:385-440`).
3. On cache miss it calls `_ensure_snapshot` twice (end with `fetch_if_missing=True`, start with `False`), each falling back to `find_closest_snapshot` if exact-date file is absent (`app/services.py:159-189`, `app/snapshots.py:125-143`).
4. Snapshot maps are loaded via `load_snapshot_map_cached` (`.pkl` preferred, CSV fallback) (`app/snapshots.py:162-179`).
5. CPU-bound diff is performed inside `run_in_threadpool(_compute)`, results sorted by `delta` desc and truncated to `limit` (`app/services.py:496-560`).
6. Payload (results + snapshot range) is cached until `_next_cache_expiry` (next 6-hour slot boundary: 00:00, 06:00, 12:00, 18:00 UTC) and returned (`app/services.py:138-144`, `562-585`).

### License-gated request

1. Request hits a route on `router` (defined with `dependencies=[Depends(require_license)]`) (`app/api.py:33`).
2. `require_license` exempts `/health`, `/admin/*`, and `/licenses/*`, otherwise calls `get_active_license` (`app/auth.py:73-95`).
3. `get_active_license` extracts the token from `X-License-Key` or `Authorization: Bearer`, looks up the `License` row, raises 401 if missing/inactive (`app/auth.py:46-70`).
4. For endpoints needing the license record (subscription routes), the same function is also declared as an explicit `Depends(get_active_license)` on the path operation (`app/api.py:168-179, 314-365`).

### Discord webhook delivery (weekly)

1. APScheduler cron at Mon 23:58 UTC fires `deliver_discord_subscriptions_guarded` (`app/scheduler.py:292-305`).
2. The guarded wrapper acquires `discord_delivery_lock` (`asyncio.Lock`, 1-second timeout) returning `status="busy"` on contention (`app/scheduler.py:228-243`).
3. Active subscriptions are loaded (joined with active `License` rows). For each, `get_top_growers(..., days=7, limit=10, min_current_irating=...)` is called, an embed payload is composed (iRacing week number, snapshot range), and `httpx.AsyncClient.post(subscription.webhook_url, json=payload)` posts to Discord (`app/scheduler.py:59-225`).
4. `POST /admin/discord-subscriptions/run` (admin-secret-protected) can invoke the same delivery for a single `subscription_id` (`app/api.py:102-117`).

**State Management:**
- In-memory caches in `app/services.py`: `_top_growers_cache`, `_latest_snapshot_cache` (each protected by an `asyncio.Lock`). Expiry aligned to the 6-hour fetch cadence via `_next_cache_expiry`.
- Module-level `AsyncIOScheduler` singleton in `app/scheduler.py` and module-level `discord_delivery_lock`.
- Per-`IRacingClient` token cache (`self._token: TokenInfo | None`) and per-instance throttle semaphore — note: a new `IRacingClient` is instantiated per service call, so tokens are not shared across calls.

## Key Abstractions

**`IRacingClient` (`app/iracing_client.py:34-216`):**
- Purpose: Encapsulate OAuth `password_limited` flow, token caching/refresh, throttling, and CSV streaming/download.
- Pattern: Async context-style with manual `close()` (callers use try/finally).
- Token lifecycle: `_ensure_token` → `login` or `refresh`; 401 responses trigger refresh, then login fallback (`app/iracing_client.py:115-148`).

**Snapshot file (`app/snapshots.py`):**
- Purpose: Date-stamped per-category CSV plus an adjacent pickled `dict[int, dict]` map for fast lookups.
- Naming: `SNAPSHOTS_DIR/<category>/<YYYY-MM-DD>.csv` and `<YYYY-MM-DD>.pkl`.
- Pattern: `store_snapshot` writes both; readers prefer `.pkl` via `load_snapshot_map_cached`.

**`Settings` (`app/settings.py:11-72`):**
- Purpose: Single source of truth for env-driven config (server, scheduler, iRacing creds, categories, DB URL, license params).
- Pattern: `pydantic_settings.BaseSettings` with `.env` autoload; module-level `settings` instance.

**Cache key tuples (`app/services.py:35-44`):**
- `_top_growers_cache: dict[tuple[str, date, date, int, int|None], dict]`
- `_latest_snapshot_cache: dict[tuple[str, date, int], dict]`

**Two-router split (`app/api.py:32-33`):**
- `public_router` — health, license-status, admin endpoints (admin endpoints have their own per-route `Depends(_require_admin)`).
- `router` — all consumer endpoints, gated globally by `Depends(require_license)`.

**`DiscordDeliveryResult` dataclass (`app/scheduler.py:36-41`):**
- Frozen dataclass returning `(status, delivered, message)` from delivery functions.

## Entry Points

**HTTP server (`python -m app` or `python -m app.main`):**
- Location: `app/__main__.py` → `app/main.py:main` → `uvicorn.run("app.main:app", ...)`
- Triggers: Manual invocation in development or production.
- Responsibilities: Configure root logger (`StreamHandler` + `FileHandler(settings.log_file)`), instantiate FastAPI `app` with `lifespan`, include both routers, start uvicorn.
- Lifespan: `init_db()` (creates ORM tables via `Base.metadata.create_all`), then `start_scheduler()`; on shutdown, `shutdown_scheduler()` (`app/main.py:33-48`).

**One-off CLI fetch (`python -m app.fetch_once`):**
- Location: `app/fetch_once.py`
- Triggers: Manual CLI use, e.g. ad-hoc backfill.
- Responsibilities: `init_db()` then `asyncio.run(fetch_and_store())` for all configured categories.

**Snapshot converter (`python scripts/convert_snapshots.py --root ...`):**
- Location: `scripts/convert_snapshots.py`
- Triggers: Manual CLI use to (re)generate `.pkl` maps for existing CSVs.
- Responsibilities: Iterate CSVs under `<root>` (optionally filtered by `--category`), call its own local `normalize_row`/`load_snapshot_map`, pickle to `<date>.pkl`.

**Admin-triggered tasks:**
- `POST /admin/run-fetch` → `fetch_and_store(category)` (`app/api.py:87-93`).
- `POST /admin/sync-members` → `sync_members_from_snapshots_async` (`app/api.py:96-99`).
- `POST /admin/discord-subscriptions/run?subscription_id=N` → `deliver_discord_subscriptions_guarded` (`app/api.py:102-117`).

## How the `openapi.yaml` contract is served

- `openapi.yaml` lives at the repository root and is **not** mounted or served by the FastAPI app. It is treated as a hand-authored OpenAPI 3.0.3 contract, kept alongside the code for reference and external consumption (e.g. served by a reverse proxy or used as a client-generation source).
- FastAPI's auto-generated schema at `/openapi.json` and the docs at `/docs`, `/redoc` remain available by default (no `docs_url=None` override is set in `app/main.py:51`).
- The static dashboard (`index.html`, `assets/*`) fetches APIs at `/drivers-scout/api/...` (see `assets/index.js:132,202` and `assets/subscription.js:53,93,145`), which strongly implies an external reverse proxy (e.g. nginx, Caddy, or a Vercel-style edge) strips the `/drivers-scout/api` prefix and forwards to FastAPI at `/`. The proxy is also presumed to serve `index.html`, `assets/`, and `openapi.yaml` as static files.

## Scheduler Architecture

- **Engine:** `apscheduler.schedulers.asyncio.AsyncIOScheduler`, configured with `timezone=ZoneInfo("UTC")` at module load (`app/scheduler.py:33`).
- **Module singleton:** `scheduler = AsyncIOScheduler(...)` is created at import time. `start_scheduler` is idempotent — it checks `scheduler.running` and returns early when already started (`app/scheduler.py:271-313`).
- **Job 1 — `sports_formula_fetch_pair`:** `CronTrigger(hour="23,5,11,17", minute=55, timezone=UTC)`. Runs `scheduled_job` which sequentially fetches `sports_car`, sleeps 60s, fetches `formula_car`, sleeps 60s, then `sync_members_from_snapshots_async`. `misfire_grace_time=None`, `replace_existing=True`.
- **Job 2 — `deliver_discord_subscriptions`:** `CronTrigger(day_of_week="mon", hour=23, minute=58, timezone=UTC)`. Runs `deliver_discord_subscriptions_guarded` with `max_instances=1` and `replace_existing=True`.
- **Toggle:** `SCHEDULER_ENABLED=false` (default `True`) short-circuits `start_scheduler` for local dev (`app/settings.py:21`, `app/scheduler.py:273-275`).
- **Lifecycle:** Started inside the FastAPI `lifespan` startup; stopped in the lifespan `finally` block via `shutdown_scheduler` (`app/main.py:39-48`).
- **Concurrency safety:** `deliver_discord_subscriptions_guarded` uses an `asyncio.Lock` with a 1-second `wait_for` timeout so manual admin-triggered runs do not collide with the cron run; returns `status="busy"` on contention.
- **iRacing week derivation:** `_iracing_week` derives a 1–13 week number from `IRACING_WEEK_EPOCH = datetime(2025, 12, 16, tzinfo=UTC)` for use in Discord embeds (`app/scheduler.py:31, 246-249`).

## Architectural Constraints

- **Single ASGI process:** Caches and the scheduler live in-process. Horizontal scaling beyond one worker would duplicate scheduled jobs and split caches — keep `uvicorn` at one worker (no `--workers`).
- **Threading:** Async event loop is the primary model. CPU-bound work is offloaded with `fastapi.concurrency.run_in_threadpool` (member sync, top growers compute). SQLAlchemy sessions are sync; `check_same_thread=False` is required for SQLite due to threadpool offload.
- **Global state:**
  - `scheduler` singleton — `app/scheduler.py:33`
  - `discord_delivery_lock` — `app/scheduler.py:228`
  - `_top_growers_cache`, `_top_growers_cache_lock`, `_latest_snapshot_cache`, `_latest_snapshot_cache_lock` — `app/services.py:35-44`
  - `engine`, `SessionLocal` — `app/db.py:12-15`
  - `settings` — `app/settings.py:72`
- **iRacing client lifetime:** Each service-level function creates a fresh `IRacingClient()` and closes it in `finally`. No client/token reuse across requests — every endpoint pays the OAuth login cost on cache miss.
- **Snapshot directory layout is fixed:** `<SNAPSHOTS_DIR>/<category>/<YYYY-MM-DD>.{csv,pkl}`. Other names are skipped with a warning by `parse_snapshot_date`.
- **License middleware does not block when `LICENSE_ADMIN_SECRET` is unset:** `require_license` short-circuits to allow all traffic if `settings.license_admin_secret` is falsy (`app/auth.py:84-85`). This effectively disables auth in dev unless the secret is configured.

## Anti-Patterns

### Per-request `IRacingClient` instantiation

**What happens:** Every service function that may need to fetch from iRacing constructs a new `IRacingClient()` and closes it inside `try/finally` (e.g. `app/services.py:200, 223, 244, 289, 341, 441`).
**Why it's wrong:** OAuth tokens (`self._token`) and the throttle semaphore are per-instance, so they cannot be reused across calls. Every request that requires a fresh fetch pays a full `login` round-trip.
**Do this instead:** Reuse a single long-lived `IRacingClient` (e.g. a module-level instance constructed inside the lifespan startup) and share it across handlers and scheduler jobs.

### Duplicated normalization helper in `scripts/convert_snapshots.py`

**What happens:** `scripts/convert_snapshots.py` re-declares its own `normalize_row`, `parse_snapshot_date`, `load_snapshot_rows`, and `load_snapshot_map` rather than importing from `app/snapshots.py` / `app/iracing_client.py`. It also references an undefined `Any` annotation in `normalize_row` (`scripts/convert_snapshots.py:16`).
**Why it's wrong:** Drift risk — any change to `normalize_row` in `app/iracing_client.py` must be mirrored manually. The `Any` annotation also causes a `NameError` at function definition under normal evaluation unless the file is run in a context where `from __future__ import annotations` defers it (it does, top of file).
**Do this instead:** Import the canonical helpers from `app.snapshots` / `app.iracing_client` after adding the repo root to `sys.path`, or run the converter via `python -m scripts.convert_snapshots` after promoting `scripts/` to a package.

### License gate bypass when `LICENSE_ADMIN_SECRET` is unset

**What happens:** `require_license` returns early without enforcement if `settings.license_admin_secret` is unset (`app/auth.py:84-85`).
**Why it's wrong:** Couples *consumer* authentication to an unrelated *admin* secret. Forgetting to set the admin secret silently makes the entire licensed API public.
**Do this instead:** Drive license enforcement from a dedicated `LICENSE_REQUIRED` flag (or always enforce when at least one `License` row exists).

### Recomputing the same cache key twice in `get_top_growers`

**What happens:** `get_top_growers` checks `_top_growers_cache` with `(category, effective_start, effective_end, ...)` (`app/services.py:419-440`), then after resolving real `start_used`/`end_used` recomputes a new key and checks again (`app/services.py:466-494`).
**Why it's wrong:** The first check is on the *requested* range, the second on the *resolved* range; the cache write later only persists under the resolved key, so the first check can never hit when the resolved range differs from the requested one. Reads still work but the dual-key check is dead code on the first call.
**Do this instead:** Cache exclusively under the resolved range (or normalize the requested range up front before any cache reads).

## Error Handling

**Strategy:** Convert validation and missing-data conditions into `HTTPException` at the router boundary; let unexpected exceptions propagate to FastAPI's default 500 handler (and to the lifespan handler in `app/main.py:43-45`).

**Patterns:**
- 400 for invalid category / malformed `cust_ids` / contradictory `days`+`start/end` (`app/api.py:47-64, 89-90, 251-263, 283-294`).
- 401 for missing/invalid license or admin secret (`app/auth.py:39-43`, `app/api.py:36-39`).
- 404 for missing snapshots / subscriptions / licenses (`app/api.py:111-116, 147-148, 159-160, 222-223, 236-237, 267-268, 358-361`).
- 409 for `inactive` or `busy` Discord delivery (`app/api.py:113-117`).
- 422 for member search query shorter than 3 characters (`app/api.py:190-194`).
- `IRacingClient._post_token` and `_authorized_get` perform exponential-backoff retry (up to 3 attempts) on transient failures (`app/iracing_client.py:57-70, 122-148`).
- `store_snapshot` swallows pickle errors and logs them — the CSV is still written (`app/snapshots.py:80-85`).
- Discord delivery wraps each subscription in `try/except` so one bad webhook does not abort the batch (`app/scheduler.py:213-217`).

## Cross-Cutting Concerns

**Logging:** Standard `logging` module; root logger configured in `app/main.py:21-29` with both `StreamHandler(sys.stdout)` and `FileHandler(settings.log_file)`. `force=True` is set so test imports do not lose handlers. Module loggers use `logging.getLogger(__name__)`.

**Validation:** Pydantic models for request bodies (`app/schemas.py`), FastAPI `Query`/`Path` validators for primitives, and manual guards in routes for category whitelist and date-range invariants.

**Authentication:**
- Admin: `X-Admin-Secret` header compared to `settings.license_admin_secret` via `_require_admin` (`app/api.py:36-39`).
- Consumer: `X-License-Key` or `Authorization: Bearer <key>`, validated against the `License` table.
- Health and admin/license-status routes are exempt.

**Caching:** In-process dict caches with `asyncio.Lock` and a 6-hour expiry boundary aligned to the fetch cadence (`app/services.py:138-144`).

**Persistence dual-write:** Snapshots are persisted as both `.csv` (human-readable) and `.pkl` (fast lookups). Readers prefer the pickle when present.

---

*Architecture analysis: 2026-05-27*
