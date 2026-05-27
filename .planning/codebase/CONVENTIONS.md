# Coding Conventions

**Analysis Date:** 2026-05-27

## Language and Tooling Baseline

**Python version:** Not pinned. No `pyproject.toml`, `setup.cfg`, `setup.py`, `ruff.toml`, `mypy.ini`, or `.flake8` is present. The codebase uses modern Python typing syntax (`int | None`, `list[int]`, `dict[str, object]`) so it targets **Python 3.10+** implicitly.

**Dependency manifest:** `requirements.txt` only — no lockfile, no dev dependencies, no test framework dependency declared (tests rely on the stdlib `unittest` + FastAPI's `TestClient`, which is pulled in transitively).

**No linter / formatter / type-checker is configured.** Apply ad-hoc style based on the patterns described below when contributing.

## Module Layout

Every module in `app/` follows the same skeleton:

```python
"""One-line docstring describing the module's purpose."""
from __future__ import annotations

import <stdlib>
from <stdlib> import <names>

import <third_party>

from .<local_module> import <names>

logger = logging.getLogger(__name__)
```

Conventions visible across `app/api.py`, `app/services.py`, `app/scheduler.py`, `app/snapshots.py`, `app/iracing_client.py`:

- `from __future__ import annotations` is at the top of **every** `app/*.py` module. New modules must include it.
- Module-level docstring is a single sentence in double quotes (e.g. `"""FastAPI routers and endpoints."""` in `app/api.py:1`).
- Imports are grouped: stdlib → third-party → relative (`from .module`). Blank line between groups.
- A module-level `logger = logging.getLogger(__name__)` is declared immediately after imports in modules that log (see `app/services.py:32`, `app/scheduler.py:26`, `app/iracing_client.py:16`).
- `__all__` is used sparingly to expose public surface in modules that act as facades (`app/models.py:58`, `app/schemas.py:36`, `app/auth.py:98`, `app/license_repository.py:95`).

## Naming Patterns

**Files / modules:** `snake_case.py` — e.g. `iracing_client.py`, `license_repository.py`, `fetch_once.py`.

**Functions and methods:** `snake_case` — e.g. `get_irating_delta`, `fetch_and_store`, `sync_members_from_snapshots_async` in `app/services.py`.

**Private helpers:** Leading underscore — `_get_db_session`, `_parse_cust_ids`, `_require_admin`, `_subscription_to_response` in `app/api.py:36-165`. Private helpers live in the same module as the public function that uses them, not in a separate `utils.py`.

**Classes:** `PascalCase` — `IRacingClient`, `TokenInfo`, `DiscordDeliveryResult` (`app/iracing_client.py:22, 34`; `app/scheduler.py:36`).

**Pydantic schemas:** `<Resource><Verb>` — `SubscriptionCreate`, `SubscriptionResponse` in `app/schemas.py:11, 25`.

**SQLAlchemy models:** Singular `PascalCase` — `Member`, `License`, `Subscription` in `app/models.py`. Table names are plural `snake_case` via `__tablename__` (`"members"`, `"licenses"`, `"subscriptions"`).

**Constants:** `UPPER_SNAKE_CASE` at module scope — `SCHEDULE_HOURS`, `SCHEDULE_HOURS_EXPRESSION`, `SCHEDULE_TIMEZONE`, `IRACING_WEEK_EPOCH` in `app/scheduler.py:28-31`; `TOKEN_URL`, `DATA_URL_TEMPLATE` in `app/iracing_client.py:18-19`; `EXEMPT_PATHS` in `app/auth.py:16`.

**Type aliases:** `PascalCase` — `SnapshotRow = Dict[str, object]` in `app/snapshots.py:18`.

## Type Hints

Type hints are **mandatory** on every function — including private helpers and test methods. The codebase is fully annotated.

Style observed:

- Modern union syntax preferred: `str | None`, `int | None`, `list[int]`, `dict[str, object]`. See `app/api.py:36`, `app/schemas.py:14`, `app/services.py:35-44`.
- Older `typing` imports (`Dict`, `List`, `Tuple`, `Iterable`, `Iterator`, `Optional`) appear when generic helpers are involved (e.g. `app/services.py:9`, `app/iracing_client.py:10`, `app/repository.py:4`, `app/api.py:5`). New code should prefer the lowercase built-in generics when possible — `app/services.py` mixes both.
- Return annotations are always present. Functions returning nothing use `-> None`.
- Keyword-only parameters are marked with `*` and used for boolean flags and configuration — see `ensure_license(session, *, key, label=None)` in `app/license_repository.py:21`, `_ensure_snapshot(..., *, fetch_if_missing, require_csv=False)` in `app/services.py:159-166`.
- SQLAlchemy 2.0 typed `Mapped[...]` style is used in `app/models.py` (`Mapped[int]`, `Mapped[str | None]`, `mapped_column(...)`).

## Pydantic Conventions

**Settings (`app/settings.py`):**
- `pydantic-settings` `BaseSettings` subclass with `SettingsConfigDict(env_file=..., env_prefix="", case_sensitive=False, extra="ignore")`.
- Every field uses `Field(default, description=...)` so that environment configuration is self-documenting.
- Required secrets use `Field(...)` (no default) — `iracing_username`, `iracing_password`, `iracing_client_secret`.
- Derived values use `@computed_field @property` — see `categories_normalized` (`app/settings.py:64`).
- A single module-level instance `settings = Settings()` is imported by every module that needs config (`from .settings import settings`).

**Request/response schemas (`app/schemas.py`):**
- Plain `BaseModel` subclasses. Constraints declared via `Field(...)` keyword arguments (`min_irating: int | None = Field(None, ge=0)`).
- Specialised types are used where applicable: `HttpUrl` for webhook URLs.
- Cross-field / domain validation is done with `@field_validator` decorated `@classmethod` methods that raise `ValueError` (Pydantic converts that to a 422). See `SubscriptionCreate.validate_category` in `app/schemas.py:16-22`.
- Response models that wrap ORM objects declare `model_config = ConfigDict(from_attributes=True)` and are instantiated with `Model.model_validate(orm_obj)` (see `_subscription_to_response` in `app/api.py:164`).

## FastAPI Conventions

**Two routers per app:**
- `public_router = APIRouter()` for unauthenticated and admin endpoints.
- `router = APIRouter(dependencies=[Depends(require_license)])` for license-gated endpoints.

Defined in `app/api.py:32-33` and registered in `app/main.py:51-53`. Place new authenticated routes on `router`; new admin or open routes on `public_router`.

**Endpoint signature style:**
- HTTP method decorators are stacked with full path: `@router.get("/members/{cust_id}/delta")`.
- Path parameters are declared as positional arguments with type hints — FastAPI infers them.
- Query parameters use `= Query(...)` with bounds and descriptions: `days: int | None = Query(None, ge=1)`, `limit: int = Query(20, ge=1, le=100)`. See `app/api.py:241-247, 272-279`.
- Body parameters are typed Pydantic models (`payload: SubscriptionCreate` in `app/api.py:316`). Single scalar bodies use `Body(..., embed=True)` (`app/api.py:122`).
- Headers use `Header(None, alias="X-License-Key")` with the canonical header name in the alias (`app/api.py:36`, `app/auth.py:49, 77`).
- DB sessions are injected with `session: Session = Depends(_get_db_session)`. The generator wraps the `get_session()` context manager — see `_get_db_session` in `app/api.py:42-44` and `app/auth.py:19-21`.
- Per-route admin protection uses `dependencies=[Depends(_require_admin)]` on the decorator, not a manual check inside the body (`app/api.py:87, 96, 102, 120, 133, 142, 152`).
- Response status overrides are done by mutating the injected `response: Response` (see `create_subscription` in `app/api.py:314-345`), not by raising or by passing `status_code=` if it changes between create/update.

## Dependency Injection Style

- Configuration is a module-level singleton: `from .settings import settings`. There is no DI container; tests mutate `settings.<field>` directly (e.g. `tests/test_run_fetch.py:25-26`).
- The database engine and `SessionLocal` factory live in `app/db.py:12-15` as module globals.
- Sessions are obtained via the `@contextmanager` helper `get_session()` in `app/db.py:18-29`, which commits on success and rolls back on exception. FastAPI routes wrap this in a small generator dependency (`_get_db_session`).
- The iRacing client is constructed per-request inside service functions (`client = IRacingClient()` ... `await client.close()`), not injected — see `app/services.py:200, 223, 244, 289, 341, 441`.

## Error Handling Strategy

**At HTTP layer (`app/api.py`):**
- Raise `HTTPException(status_code=<int>, detail=<str>)` for client-facing errors. Use literal status codes (`400`, `404`, `409`) most of the time, but reach for `fastapi.status.HTTP_*` constants for new/non-obvious codes (`status.HTTP_401_UNAUTHORIZED`, `status.HTTP_422_UNPROCESSABLE_ENTITY` — see `app/api.py:39, 192`).
- `detail` is a plain string for most endpoints. The license auth helper uses a dict shape `{"error": ..., "message": ...}` (`app/auth.py:39-43`) — match that pattern when new auth errors are added.
- Validation that depends on settings (e.g. category whitelist) lives in the route body before the service call (`app/api.py:89, 219, 232, 249, 281`).
- Cross-parameter validation (e.g. mutually exclusive `days` vs. `start/end`) is done with explicit `if` blocks in the route (`app/api.py:251-263, 283-295`).
- `from exc` is used when re-raising as `HTTPException` to preserve the original cause (`app/api.py:60-63`).

**At service layer (`app/services.py`, `app/scheduler.py`):**
- Domain functions return `None` or empty dicts to signal "no data" rather than raising — see `get_latest_snapshot`, `get_irating_delta`, `get_top_growers`. The route translates `None` into `404 Not Found`.
- `raise ValueError(...)` is used for programmer-error preconditions (`app/services.py:396, 401`).
- External-call failures are caught and logged with `logger.exception(...)`; the operation either retries (iRacing client, `app/iracing_client.py:65-69, 142-146, 156-160`) or falls back to a degraded result (snapshot lookup, `app/services.py:182-183`; pickle load, `app/snapshots.py:174-178`).
- The scheduler treats per-subscription delivery failures as isolated: each iteration sits inside a `try/except Exception: logger.exception(...)` so one bad webhook does not abort the run (`app/scheduler.py:213-217`).
- Concurrency guarding for one-at-a-time jobs uses an `asyncio.Lock` with `asyncio.wait_for(...)` and returns a typed `DiscordDeliveryResult(status="busy", ...)` rather than raising — see `deliver_discord_subscriptions_guarded` in `app/scheduler.py:228-243`.

**Retry policy (iRacing client):**
- Three attempts with exponential backoff `await asyncio.sleep(2 ** attempt)`.
- On HTTP 401, the first retry refreshes the token, the second re-logs in, the third gives up.
- See `_authorized_get` in `app/iracing_client.py:122-147`.

## Logging

**Framework:** stdlib `logging`. Configured once in `app/main.py:21-29`:

```python
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_file_path)],
    force=True,
)
```

Log level and log file path come from `Settings` (`LOG_LEVEL`, `LOG_FILE`).

**Conventions:**
- Each module that logs declares `logger = logging.getLogger(__name__)` immediately after imports.
- Use `%s` placeholders, **never f-strings**, in log calls. The codebase consistently uses lazy formatting: `logger.info("Started fetch for category %s", cat)` (`app/services.py:203`). New log calls must follow this pattern so disabled log levels do not pay the formatting cost.
- Levels:
  - `logger.debug(...)` for fine-grained progress (cache hits/misses, payload counts, snapshot file selection — heavily used in `app/services.py:426-498` and `app/scheduler.py:75-203`).
  - `logger.info(...)` for high-level lifecycle events (fetch start/complete, scheduler start, login obtained).
  - `logger.warning(...)` for recoverable problems (rate-limit waits, retry attempts, snapshot date parse failures, non-2xx webhook responses).
  - `logger.exception(...)` inside `except` blocks where the stack trace matters. **Do not** use `logger.error(... exc_info=True)` — the codebase consistently picks `logger.exception`.
- Structured context (when needed) is passed via the `extra={...}` kwarg — see `app/auth.py:55, 63, 68`. Keys used so far: `path`, `license_key`, `label`.
- Sensitive data (passwords, full webhook URLs, license keys) is **redacted** before logging — the scheduler logs `webhook_host` parsed from the URL via `urlparse(...).hostname` (`app/scheduler.py:191-203`), not the full URL.

## Function Design

- Public service functions are `async def` even when they only delegate to threadpool-bound sync work (e.g. `sync_members_from_snapshots_async` in `app/services.py:124`). The synchronous core is exposed as a sibling function for tests that want to call it directly (e.g. `sync_members_from_snapshots` is used in `tests/test_member_sync.py:51`).
- CPU-bound or blocking I/O work is wrapped with `await run_in_threadpool(callable)` from `fastapi.concurrency`. See `_compute` inside `get_top_growers` in `app/services.py:496-562`.
- Caching helpers use a module-level `dict` plus an `asyncio.Lock`. Cache key is a tuple of the inputs, cached value carries an `expires_at` datetime, and the expiry is aligned to a six-hour grid via `_next_cache_expiry` (`app/services.py:138-144`). New caches must follow this shape.
- Resource cleanup uses `try / finally` rather than `async with` for `IRacingClient` because the class does not implement the async context-manager protocol. New per-request resources should either implement `__aenter__/__aexit__` or follow the explicit `await x.close()` in `finally:` pattern (`app/services.py:213-216, 239-240`, etc.).

## Module Boundaries

- `app/api.py` is the HTTP layer. It must not contain business logic beyond input parsing and response shaping.
- `app/services.py` owns business logic and orchestrates snapshots + iRacing client.
- `app/repository.py` and `app/license_repository.py` own SQLAlchemy queries and dialect-specific upserts (`sqlite_insert(...).on_conflict_do_update(...)`).
- `app/snapshots.py` owns disk I/O for snapshot CSV/PKL files.
- `app/iracing_client.py` owns the external HTTP API integration.
- `app/scheduler.py` owns APScheduler jobs and the Discord delivery side-effects.

New code should respect these boundaries: routes call services, services call repositories / snapshots / iRacing client. Going the other direction (e.g. a repository importing from `api.py`) is not done anywhere in the codebase.

---

*Convention analysis: 2026-05-27*
