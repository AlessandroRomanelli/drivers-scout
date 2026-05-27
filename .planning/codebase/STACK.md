# Technology Stack

**Analysis Date:** 2026-05-27

## Languages

**Primary:**
- Python 3.10+ — backend service implementation under `app/` (uses PEP 604 unions like `str | None`, `from __future__ import annotations`, `match`-friendly type hints, `zoneinfo`)

**Secondary:**
- HTML / CSS / vanilla JavaScript — small static dashboard in `index.html` and `assets/index.js`, `assets/subscription.js`, `assets/style.css`
- YAML — `openapi.yaml` HTTP contract document

## Runtime

**Environment:**
- CPython 3.10+ (uses `from __future__ import annotations` and PEP 604 union syntax; `zoneinfo` from stdlib).
- ASGI runtime via Uvicorn (`app/main.py:68-74`).

**Package Manager:**
- `pip` with a flat `requirements.txt` (no `pyproject.toml`, `setup.py`, `setup.cfg`, or lock file present).
- No virtualenv tooling is committed; setup instructions in `README.md:13-16` rely on a user-managed venv.
- Lockfile: missing (no `requirements.lock`, `poetry.lock`, `Pipfile.lock`, or `uv.lock`).

## Frameworks

**Core:**
- FastAPI >=0.111.0 — HTTP API framework (`app/main.py`, `app/api.py`)
- Uvicorn[standard] >=0.30.0 — ASGI server (`app/main.py:68-74`)
- APScheduler >=3.10.4 — async cron-style scheduler (`app/scheduler.py:13-14`, `app/scheduler.py:271-313`)
- SQLAlchemy >=2.0.30 — ORM + Core query API (`app/db.py`, `app/models.py`, `app/repository.py`, `app/license_repository.py`)
- pydantic-settings >=2.3.4 — environment-driven config (`app/settings.py`)
- Pydantic v2 — request/response schemas (`app/schemas.py`); brought in transitively by FastAPI / pydantic-settings
- httpx >=0.27.0 — async HTTP client used for both the iRacing OAuth/CSV calls and outbound Discord webhooks (`app/iracing_client.py:12`, `app/scheduler.py:119`)
- python-dotenv >=1.0.1 — used by pydantic-settings to load `.env`
- TZData — installed only on Windows hosts so `zoneinfo` has timezone data (`requirements.txt:8`)

**Testing:**
- Python `unittest` (standard library) — every file under `tests/` derives from `unittest.TestCase` (e.g. `tests/test_api.py:25-32`).
- FastAPI `TestClient` (built on Starlette + httpx) — exercises HTTP routes in `tests/test_api.py`.
- `unittest.mock` for patching iRacing client and scheduler internals.
- No `pytest`, `tox`, or dedicated coverage tool detected.

**Build/Dev:**
- No build step. The service is run directly with `python -m app` (`app/__main__.py`) and the static dashboard is served as-is.
- No bundler, linter, formatter, type-checker, Dockerfile, Makefile, or CI configuration present in the repo.

## Key Dependencies

**Critical:**
- `fastapi` — HTTP routing, dependency injection, OpenAPI surface (`app/api.py`)
- `uvicorn[standard]` — production-style ASGI server; the `[standard]` extra pulls in `httptools`/`uvloop`/`watchfiles` (`app/main.py:68-74`)
- `apscheduler` — owns the 4×daily UTC fetch cron and the weekly Discord delivery cron (`app/scheduler.py:28-30`, `app/scheduler.py:271-313`)
- `sqlalchemy` — declarative models, sessions, and SQLite-specific upserts (`app/models.py`, `app/repository.py:6-7`, `app/license_repository.py:7-8`)
- `pydantic-settings` — single source of truth for environment configuration (`app/settings.py:11-62`)
- `httpx` — both inbound test client and outbound integration client (`app/iracing_client.py`, `app/scheduler.py`)

**Infrastructure:**
- SQLite via SQLAlchemy `sqlite:///./iracing_stats.db` URL — embedded relational store for `members`, `licenses`, and `subscriptions` (`app/db.py:12-14`, `.env.example:3`)
- Local filesystem under `SNAPSHOTS_DIR/<category>/` — primary data store for daily CSV + pickle snapshots (`app/snapshots.py:21-34`)

## Configuration

**Environment:**
- All runtime configuration is loaded by `Settings` in `app/settings.py` from a `.env` file at the repo root and from process environment variables (`env_file=Path(__file__).parent.parent / ".env"`).
- Case-insensitive, prefix-less, extras ignored.
- `.env` is git-ignored; `.env.example` is the committed template (file exists at repo root; contents not reproduced here).

**Key environment variables consumed by `Settings`:**
- `APP_TIMEZONE` (default `UTC`) — timezone used to derive the `snapshot_day` in `fetch_and_store` (`app/services.py:196-197`).
- `SCHEDULER_ENABLED` (default `true`) — gates APScheduler startup (`app/scheduler.py:271-275`).
- `SNAPSHOTS_DIR` (default `snapshots`) — root folder for per-category CSV/pkl files (`app/snapshots.py:21-22`).
- `IRACING_USERNAME`, `IRACING_PASSWORD`, `IRACING_CLIENT_SECRET` — required, no defaults (`app/settings.py:27-30`).
- `IRACING_CLIENT_ID` (default `ar-pwlimited`), `IRACING_SCOPE` (default `iracing.auth`).
- `IRACING_RATE_LIMIT_RPM` (default 60), `RATE_LIMIT_BURST` (default 5), `HTTP_TIMEOUT_SECONDS` (default 15.0).
- `CATEGORIES` (default `sports_car`) — comma-separated; normalized via `categories_normalized` computed field.
- `DATABASE_URL` (default `sqlite:///./iracing_stats.db`).
- `LICENSE_KEY_LENGTH` (default 24, min 8), `LICENSE_KEY_ALPHABET` (default `ABCDEFGHJKLMNPQRSTUVWXYZ23456789`).
- `LICENSE_ADMIN_SECRET` — optional shared secret; when set, public license-protected routes enforce it via `X-Admin-Secret` and also turn on license enforcement on non-admin paths (`app/api.py:36-39`, `app/auth.py:84-95`).
- `LOG_LEVEL` (default `INFO`), `LOG_FILE` (default `drivers-scout.log`), `HOST` (default `0.0.0.0`), `PORT` (default `8000`).

**Build:**
- No build config files. The application is interpreted directly; the only build-time artifact is the optional `.pkl` snapshot file produced by `scripts/convert_snapshots.py`.

## Platform Requirements

**Development:**
- Python 3.10+ interpreter.
- Network reachability to `oauth.iracing.com` and `members-ng.iracing.com` for live fetches (`app/iracing_client.py:18-19`).
- Writable working directory for SQLite (`./iracing_stats.db`), the snapshots tree (`SNAPSHOTS_DIR`), and the log file (`settings.log_file`, default `drivers-scout.log`, parent auto-created in `app/main.py:16-19`).
- Windows hosts additionally need the `tzdata` wheel for `zoneinfo` (declared conditionally in `requirements.txt:8`).

**Production:**
- Single-process ASGI deployment with Uvicorn (`uvicorn.run("app.main:app", reload=False)` in `app/main.py:68-74`). No container image, systemd unit, or PaaS manifest is committed.
- Static dashboard (`index.html`, `assets/`) is expected to be served behind a reverse proxy under the `/drivers-scout/` path prefix — the frontend hits `/drivers-scout/api/...` (e.g. `assets/index.js:132`, `assets/subscription.js:53-145`), implying an upstream proxy strips the prefix before forwarding to FastAPI.
- The application is stateful: it owns local SQLite, local snapshot files, and an in-process APScheduler instance. Horizontal scaling is not supported as deployed.

---

*Stack analysis: 2026-05-27*
