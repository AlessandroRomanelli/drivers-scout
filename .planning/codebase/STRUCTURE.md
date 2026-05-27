# Codebase Structure

**Analysis Date:** 2026-05-27

## Directory Layout

```
drivers-scout/
├── app/                         # Python package — FastAPI service code
│   ├── __init__.py              # Package marker (one-line docstring)
│   ├── __main__.py              # `python -m app` entrypoint → main()
│   ├── main.py                  # FastAPI app, lifespan, uvicorn launcher
│   ├── api.py                   # All HTTP routers (public_router, router)
│   ├── auth.py                  # License/admin auth dependencies
│   ├── db.py                    # SQLAlchemy engine + get_session context
│   ├── fetch_once.py            # `python -m app.fetch_once` CLI
│   ├── iracing_client.py        # iRacing OAuth + CSV download client
│   ├── license_repository.py    # License CRUD + key generation
│   ├── models.py                # SQLAlchemy ORM models (Member/License/Subscription)
│   ├── repository.py            # Member upsert helpers
│   ├── scheduler.py             # APScheduler jobs + Discord delivery
│   ├── schemas.py               # Pydantic request/response schemas
│   ├── services.py              # Business logic (fetch, delta, top growers, caches)
│   ├── settings.py              # pydantic-settings configuration
│   └── snapshots.py             # Disk-based CSV/PKL snapshot store
├── assets/                      # Static frontend assets (served externally)
│   ├── favicon.ico
│   ├── index.js                 # Dashboard logic (license gate, gainers chart/table)
│   ├── style.css                # Dashboard styling
│   └── subscription.js          # Webhook subscription tab logic
├── scripts/                     # Stand-alone CLI utilities
│   └── convert_snapshots.py     # Bulk CSV → .pkl converter
├── tests/                       # unittest test suites (flat layout, no __init__.py)
│   ├── test_api.py              # /leaders/growers, /subscriptions endpoint tests
│   ├── test_license_admin.py    # Admin license endpoint tests
│   ├── test_license_status.py   # /licenses/{key}/status tests
│   ├── test_member_search.py    # /members/search tests
│   ├── test_member_sync.py      # sync_members_from_snapshots tests
│   ├── test_members_latest_batch.py  # /members/latest tests
│   ├── test_repository.py       # services-layer (get_top_growers, delta) tests
│   └── test_run_fetch.py        # /admin/run-fetch tests
├── .env.example                 # Sample environment configuration
├── .gitignore                   # Ignores *.db, snapshots/, *.log, .env, etc.
├── .planning/                   # GSD workflow artifacts (this directory)
│   └── codebase/                # Codebase map outputs
├── README.md                    # Setup, run, and curl examples
├── index.html                   # Dashboard HTML (served externally)
├── openapi.yaml                 # Hand-authored OpenAPI 3.0.3 contract
└── requirements.txt             # Runtime dependencies (fastapi, uvicorn, …)
```

## Directory Purposes

**`app/`:**
- Purpose: All server-side Python code organized as a flat Python package.
- Contains: One Python module per concern (api, auth, db, models, services, scheduler, snapshots, settings, iracing_client, repositories, schemas, entry points).
- Key files: `app/main.py` (FastAPI app + uvicorn), `app/api.py` (routers), `app/services.py` (business logic), `app/scheduler.py` (APScheduler jobs), `app/settings.py` (config).

**`assets/`:**
- Purpose: Browser-side static files for the operator dashboard at `index.html`.
- Contains: JS modules (`index.js`, `subscription.js`), `style.css`, `favicon.ico`. No bundler or `package.json` is present — files are served as-is.
- Key files: `assets/index.js` (license validation + top-growers chart), `assets/subscription.js` (Discord webhook subscription UI). Both call `/drivers-scout/api/...` — assumed to be rewritten by a reverse proxy.

**`scripts/`:**
- Purpose: Ad-hoc / maintenance CLI utilities that run outside the ASGI process.
- Contains: `convert_snapshots.py` only.
- Key files: `scripts/convert_snapshots.py` (argparse-based; takes `--root`, optional `--category`, `--overwrite`).

**`tests/`:**
- Purpose: `unittest` test suites covering routers, services, and repositories.
- Contains: Eight `test_*.py` files. Flat layout (no `__init__.py`, no `conftest.py`). Each file pre-seeds environment variables (`SNAPSHOTS_DIR`, `IRACING_*`, `DATABASE_URL`) before importing `app.*` modules.
- Key files: `tests/test_api.py` (largest, exercises `/leaders/growers` and `/subscriptions`), `tests/test_repository.py` (service-layer compute tests).

**`assets/` + `index.html` + `openapi.yaml` together:**
- Purpose: Static contract + UI bundle. The OpenAPI spec is hand-authored (not generated from FastAPI), and the dashboard is plain HTML/JS with no build step. All three are assumed to be served by an external reverse proxy that also rewrites `/drivers-scout/api/*` to the FastAPI service.

**`.planning/codebase/`:**
- Purpose: Generated codebase-map outputs for the GSD workflow (this file lives here).
- Contains: Markdown reports written by `/gsd:map-codebase`.

## Key File Locations

**Entry Points:**
- `app/__main__.py`: `python -m app` → calls `app.main.main()`.
- `app/main.py`: FastAPI `app` declaration plus `main()` that runs `uvicorn.run("app.main:app", host=settings.host, port=settings.port, ...)`.
- `app/fetch_once.py`: One-off `python -m app.fetch_once` CLI for backfill.
- `scripts/convert_snapshots.py`: Standalone CSV-to-pickle converter.

**Configuration:**
- `app/settings.py`: pydantic-settings `Settings` class + module-level `settings` instance. Loads `.env` from the repo root (`Path(__file__).parent.parent / ".env"`).
- `.env.example`: Reference values for `APP_TIMEZONE`, `SCHEDULER_ENABLED`, `DATABASE_URL`, `IRACING_*`, `CATEGORIES`, etc.
- `requirements.txt`: pinned-lower-bound runtime dependencies (`fastapi>=0.111`, `uvicorn[standard]>=0.30`, `apscheduler>=3.10.4`, `sqlalchemy>=2.0.30`, `pydantic-settings>=2.3.4`, `httpx>=0.27`, `python-dotenv>=1.0.1`, `TZData` on Windows).

**Core Logic:**
- `app/services.py`: `fetch_and_store`, `get_top_growers`, `get_irating_delta`, `get_latest_snapshot[s]`, `sync_members_from_snapshots[_async]`, in-memory caches.
- `app/scheduler.py`: `start_scheduler`, `scheduled_job`, `deliver_discord_subscriptions[_guarded]`.
- `app/iracing_client.py`: `IRacingClient` (OAuth + CSV download), `normalize_row`.
- `app/snapshots.py`: All disk I/O for snapshot CSV/PKL files.
- `app/api.py`: `public_router` and `router` route declarations.

**Persistence:**
- `app/db.py`: `engine`, `SessionLocal`, `get_session()` contextmanager.
- `app/models.py`: `Base`, `Member`, `License`, `Subscription`.
- `app/repository.py`: `ensure_members`, `fetch_all_cust_ids`.
- `app/license_repository.py`: `create_unique_license`, `ensure_license`, `list_licenses`, `revoke_license`, `activate_license`, `license_to_dict`.

**Schemas:**
- `app/schemas.py`: `SubscriptionCreate`, `SubscriptionResponse` (Pydantic).
- `openapi.yaml`: Hand-authored OpenAPI 3.0.3 description (lives at repo root, not served by FastAPI).

**Testing:**
- `tests/` — eight `unittest` files, run via `python -m unittest` discovery.

## Naming Conventions

**Files (Python modules):**
- Pattern: `snake_case.py`, singular nouns by concern. Examples: `services.py`, `scheduler.py`, `iracing_client.py`, `license_repository.py`, `snapshots.py`.
- Two-word modules use underscores (`license_repository.py`, `fetch_once.py`, `iracing_client.py`).

**Files (Tests):**
- Pattern: `test_<feature>.py` at the top level of `tests/`.
- Examples: `test_api.py`, `test_member_search.py`, `test_members_latest_batch.py`, `test_run_fetch.py`.

**Files (Snapshots on disk):**
- Pattern: `<YYYY-MM-DD>.csv` and adjacent `<YYYY-MM-DD>.pkl` under `<SNAPSHOTS_DIR>/<category>/`.
- Example: `snapshots/sports_car/2026-05-27.csv`.

**Files (Static assets):**
- Pattern: `kebab-case` or single-word lowercase. Examples: `index.html`, `index.js`, `subscription.js`, `style.css`.

**Directories:**
- Pattern: lowercase single words. Examples: `app/`, `assets/`, `scripts/`, `tests/`.

**Python identifiers:**
- Functions/variables: `snake_case` (`fetch_and_store`, `get_top_growers`, `license_to_dict`, `_top_growers_cache`).
- Private/internal helpers: leading underscore (`_ensure_snapshot`, `_download_snapshot`, `_require_admin`, `_get_db_session`, `_parse_cust_ids`, `_iracing_week`).
- Classes: `PascalCase` (`IRacingClient`, `TokenInfo`, `DiscordDeliveryResult`, `Settings`, `Member`, `License`, `Subscription`, `SubscriptionCreate`, `SubscriptionResponse`).
- Module-level constants: `UPPER_SNAKE_CASE` (`TOKEN_URL`, `DATA_URL_TEMPLATE`, `SCHEDULE_HOURS`, `SCHEDULE_HOURS_EXPRESSION`, `SCHEDULE_TIMEZONE`, `IRACING_WEEK_EPOCH`, `EXEMPT_PATHS`, `LICENSE_DB_NAME`).
- Type aliases: `PascalCase` (`SnapshotRow` in `app/snapshots.py:18`).

**Database tables:**
- Plural lowercase (`members`, `licenses`, `subscriptions`).

**Environment variables:**
- `UPPER_SNAKE_CASE`, no service prefix on most (`HOST`, `PORT`, `LOG_LEVEL`, `SNAPSHOTS_DIR`, `DATABASE_URL`), service prefix on iRacing-specific keys (`IRACING_USERNAME`, `IRACING_PASSWORD`, `IRACING_CLIENT_ID`, `IRACING_CLIENT_SECRET`, `IRACING_SCOPE`, `IRACING_RATE_LIMIT_RPM`).

## Where to Add New Code

**New API endpoint:**
- Implementation: add a handler to `app/api.py`.
  - If public/unauthenticated: register on `public_router`.
  - If license-gated: register on `router` (license dependency is already applied at the router level).
  - If admin-only: include `dependencies=[Depends(_require_admin)]` per route, like the existing `/admin/*` routes.
- If business logic is non-trivial: add a function to `app/services.py` and call it from the route.
- Add a Pydantic schema to `app/schemas.py` if a request/response body needs validation.
- Update `openapi.yaml` to keep the contract in sync (FastAPI's auto-generated `/openapi.json` is not the source of truth here).
- Tests: add a new `tests/test_<feature>.py` following the existing pattern (env vars set before importing `app.*`).

**New scheduled job:**
- Implementation: define an async function in `app/scheduler.py`. Register inside `start_scheduler` with `scheduler.add_job(...)`. Use `replace_existing=True` and a stable `id=` so restarts are idempotent.
- For shared resources protected by lock, follow the `discord_delivery_lock` pattern (`app/scheduler.py:228`).

**New SQLAlchemy model:**
- Implementation: add class to `app/models.py`, subclassing `Base`.
- Repository helpers: add a new `<thing>_repository.py` (matching `license_repository.py`) or extend `repository.py` for cross-cutting upsert helpers.
- Schema creation happens automatically via `Base.metadata.create_all(engine)` in `init_db()` (`app/services.py:147-151`). No migrations are used.

**New iRacing endpoint or category:**
- For new endpoints, extend `IRacingClient` in `app/iracing_client.py`.
- For new categories: extend the `CATEGORIES` env var (handled by `settings.categories_normalized`). No code change required because routes whitelist against `settings.categories_normalized` (`app/api.py:89, 219, 232, 249, 281`).

**New configuration value:**
- Add a `Field(...)` declaration on the `Settings` class in `app/settings.py`. Update `.env.example` with a sensible default.

**New CLI utility:**
- Add a new script under `scripts/` if it runs independently of the ASGI process (e.g. data migration). Mirror the argparse + `if __name__ == "__main__"` pattern in `scripts/convert_snapshots.py`.
- If it shares logic with the live service, prefer adding a `python -m app.<name>` module under `app/` (like `app/fetch_once.py`) so it can import siblings cleanly.

**New utility / shared helper:**
- For request-side helpers, prefer top-level functions in the relevant module (e.g. `_parse_cust_ids` in `app/api.py`).
- For domain-wide utilities (date math, parsing), add to the closest existing module — there is no `utils.py`.

**New frontend feature:**
- HTML: extend `index.html` (single page, no template engine).
- JS: extend `assets/index.js` (search/top growers) or `assets/subscription.js` (webhook subscriptions). Both call `/drivers-scout/api/...` — keep that prefix or reconfigure the reverse-proxy mapping.
- Style: extend `assets/style.css`. No build/bundler is present.

**New test:**
- Create `tests/test_<feature>.py`. Top of file: set required env vars (`SNAPSHOTS_DIR`, `IRACING_USERNAME`, `IRACING_PASSWORD`, `IRACING_CLIENT_SECRET`, `DATABASE_URL`) via `os.environ.setdefault(...)` **before** importing `app.*`. Use `fastapi.testclient.TestClient` against an ad-hoc `FastAPI()` with the routers attached, mirroring `tests/test_api.py`.

## Special Directories

**`snapshots/` (runtime, ignored):**
- Purpose: Default `SNAPSHOTS_DIR` root; populated by the scheduler and admin fetch.
- Layout: `snapshots/<category>/<YYYY-MM-DD>.csv` and `<YYYY-MM-DD>.pkl`.
- Generated: Yes (by `fetch_and_store` / `store_snapshot`).
- Committed: No (`snapshots` is in `.gitignore`).

**`*.db` (runtime, ignored):**
- Purpose: Default SQLite database file (`iracing_stats.db` per `DATABASE_URL` default).
- Generated: Yes (by `init_db()` on startup or test setup).
- Committed: No (`*.db` is in `.gitignore`).

**`.env` (runtime, ignored):**
- Purpose: Local secrets and configuration consumed by `pydantic-settings`.
- Generated: Manually copied from `.env.example`.
- Committed: No (`.env` is in `.gitignore`; only `.env.example` is tracked).

**`.planning/`:**
- Purpose: GSD workflow artifacts (codebase maps, plans).
- Generated: Yes (by `/gsd:map-codebase` and related commands).
- Committed: Typically yes for the codebase map subdirectory.

**`*.log` (runtime, ignored):**
- Purpose: Application log output (`settings.log_file`, default `drivers-scout.log`).
- Generated: Yes (by the logging config in `app/main.py:17-29`).
- Committed: No (`*.log` is in `.gitignore`).

---

*Structure analysis: 2026-05-27*
