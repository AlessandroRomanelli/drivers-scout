# Testing Patterns

**Analysis Date:** 2026-05-27

## Test Framework

**Runner:** Python stdlib `unittest`.

- **No pytest installed.** `requirements.txt` does not declare pytest, and no `pyproject.toml`, `pytest.ini`, `setup.cfg`, or `tox.ini` exists in the repository.
- Despite the directory name `tests/`, every test file uses `unittest.TestCase` and the trailing `if __name__ == "__main__": unittest.main()` boilerplate.
- pytest *can* discover and run these tests because pytest auto-collects `unittest.TestCase` subclasses, but the canonical way to run the suite is `python -m unittest discover tests` or `python -m unittest tests.test_<name>`.

**Assertion style:** unittest's `self.assertEqual`, `self.assertTrue`, `self.assertFalse`, `self.assertIsNone`, `self.assertIsNotNone`, `self.assertSetEqual`, `self.assertIn`. No `assert` keyword and no pytest fixtures.

**HTTP testing:** `fastapi.testclient.TestClient` (which wraps `httpx`) is the canonical way to exercise routes. See `tests/test_api.py:17-32`.

**Mocking:** `unittest.mock.patch`, `unittest.mock.AsyncMock`. Both module-level patching and decorator-style patching are used.

**Run commands:**
```bash
python -m unittest discover tests           # Run all tests
python -m unittest tests.test_api           # Run a single module
python -m unittest tests.test_api.GrowersApiTests.test_leaders_endpoint_returns_growth  # Single test
```

There is no coverage tool wired up. No `.coveragerc`, no `coverage` in `requirements.txt`.

## Test File Organization

**Location:** Flat `tests/` directory at the repo root, **outside** the `app/` package. No `tests/__init__.py`, no `conftest.py`, no subdirectories.

**Files:**
```
tests/
├── test_api.py                   # Growers endpoint + cache + subscription scoping
├── test_license_admin.py         # Admin license CRUD lifecycle
├── test_license_status.py        # Public /licenses/{key}/status endpoint
├── test_member_search.py         # /members/search query/pagination
├── test_member_sync.py           # services.sync_members_from_snapshots (sync function)
├── test_members_latest_batch.py  # /members/latest batch endpoint
├── test_repository.py            # services.get_top_growers, services.get_irating_delta
└── test_run_fetch.py             # /admin/run-fetch endpoint with mocked fetch_and_store
```

**Naming:**
- File: `test_<area>.py` where `<area>` matches the route group or service function under test.
- Class: `<Area>Tests` in PascalCase — `GrowersApiTests`, `LicenseAdminTests`, `MemberSearchTests`, `LatestMembersBatchTests`, `SnapshotComputationTests`, `RunFetchEndpointTests`.
- Method: `test_<scenario_in_snake_case>` — e.g. `test_leaders_endpoint_returns_growth`, `test_latest_members_rejects_invalid_cust_ids`, `test_cache_reused_until_cutoff_then_refreshed`. Names read as full sentences and describe the observable outcome, not the implementation.

## Test Module Boilerplate

**Every test file follows this exact preamble before any `app.*` imports:**

```python
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("SNAPSHOTS_DIR", tempfile.mkdtemp(prefix="drivers-scout-test-<scope>-"))
os.environ.setdefault("IRACING_USERNAME", "user")
os.environ.setdefault("IRACING_PASSWORD", "pass")
os.environ.setdefault("IRACING_CLIENT_SECRET", "secret")
db_dir = Path(tempfile.mkdtemp(prefix="drivers-scout-test-<scope>-db-"))
os.environ["DATABASE_URL"] = f"sqlite:///{db_dir / 'drivers-scout-test.db'}"

from app.api import ...  # imports happen AFTER env is set
```

**Why this matters:**
- `app/settings.py` instantiates `settings = Settings()` at import time. Its `BaseSettings` requires `IRACING_USERNAME`, `IRACING_PASSWORD`, and `IRACING_CLIENT_SECRET` (no default). The dummy values above let the module import without an `.env` file.
- `SNAPSHOTS_DIR` is set to a fresh tempdir per process so tests cannot collide with the developer's real `snapshots/` directory.
- `DATABASE_URL` is overridden to a temporary SQLite file so each test module gets its own database.
- Note `setdefault` for the iRacing credentials (multiple files can run in one Python process under unittest discover and the first one to import wins), but **plain assignment** (`os.environ["DATABASE_URL"] = ...`) is used for `DATABASE_URL` so each module gets its own DB.

**Order is critical:** `os.environ` assignment must happen *before* `from app...` imports. The `# noqa` is omitted but lints would flag the placement — this is a deliberate pattern, do not move imports to the top.

See `tests/test_api.py:1-23`, `tests/test_license_admin.py:1-20`, `tests/test_run_fetch.py:1-19`, `tests/test_member_sync.py:1-18`, `tests/test_members_latest_batch.py:1-19`, `tests/test_repository.py:1-15` for instances of this exact pattern.

## Test Structure

**Class lifecycle methods used:**

| Method | Purpose | Example |
|--------|---------|---------|
| `setUpClass(cls)` | Build the FastAPI app + TestClient, call `init_db()`, set `settings.license_admin_secret`. Runs **once per class**. | `tests/test_api.py:27-33` |
| `setUp(self)` | Reset DB rows, wipe and recreate the snapshot directory, write fresh CSV fixtures, clear in-memory caches. Runs **before each test**. | `tests/test_api.py:35-60` |
| `tearDown(self)` | `shutil.rmtree(..., ignore_errors=True)` of the snapshot dir. | `tests/test_api.py:62-63` |

**Canonical FastAPI test class:**

```python
class GrowersApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        init_db()
        cls.app = FastAPI()
        cls.app.include_router(router)
        cls.client = TestClient(cls.app)
        cls.snapshots_dir = Path(os.environ["SNAPSHOTS_DIR"]) / "sports_car"

    def setUp(self) -> None:
        with get_session() as session:
            session.query(Subscription).delete()
            session.query(License).delete()
        ...
```

Source: `tests/test_api.py:26-60`.

**Notes on the app fixture:**
- Tests build a **fresh `FastAPI()`** in `setUpClass` and mount only the routers they need (`public_router` and/or `router`). They do **not** import `app.main.app`. This bypasses the production `lifespan` (which would start the APScheduler) and avoids logging side effects.
- `License`-gated tests typically set `settings.license_admin_secret = ""` (e.g. `tests/test_member_search.py:26`, `tests/test_members_latest_batch.py:25`) to disable the `require_license` short-circuit in `app/auth.py:84-88` so requests can hit the router without a valid license header.
- Admin tests set `settings.license_admin_secret = "letmein"` (e.g. `tests/test_license_admin.py:26`, `tests/test_run_fetch.py:25`) and pass `headers={"X-Admin-Secret": "letmein"}` on requests.

## Fixtures and Test Data

**No fixtures library.** No `conftest.py`, no `pytest.fixture`, no `factory_boy`. Test data is constructed inline.

**Two data-fixture patterns dominate:**

### 1. CSV snapshot fixtures (file-system)

Tests for snapshot-driven services and routes write CSV files into the per-process `SNAPSHOTS_DIR/<category>/<YYYY-MM-DD>.csv` directly:

```python
def _write_csv(self, snapshot_date: date, rows: list[list[str]]) -> None:
    path = self.snapshots_dir / f"{snapshot_date.isoformat()}.csv"
    content = "\n".join([",".join(row) for row in rows])
    path.write_text(content, encoding="utf-8")
```

Every CSV starts with the canonical iRacing header row `["CUSTID", "DRIVER", "LOCATION", "IRATING", "STARTS", "WINS"]`. This helper appears verbatim in `tests/test_api.py:65-68`, `tests/test_members_latest_batch.py:48-51`, `tests/test_repository.py:49-52`. A multi-category variant is in `tests/test_member_sync.py:44-49`.

When adding a new snapshot-dependent test, copy this helper into the test class rather than importing — it is not centralised.

### 2. ORM fixtures (database)

Database rows are constructed by instantiating models inside a `get_session()` block:

```python
with get_session() as session:
    session.add_all([
        Member(cust_id=1, display_name="Alice Johnson", location="USA"),
        Member(cust_id=2, display_name="Bob Smith", location="Canada"),
        ...
    ])
```

See `tests/test_member_search.py:33-42` and `tests/test_api.py:137-161` for the Member and License + Subscription patterns respectively. `session.flush()` is used before referencing autogenerated keys (`tests/test_api.py:139`).

**Cleanup between tests** is explicit: `session.query(Model).delete()` inside `setUp`. There is no transactional rollback strategy — each test commits and the next test deletes.

## Mocking

**Library:** `unittest.mock` (`patch`, `AsyncMock`).

### Async function mocking

Used for service calls invoked by route handlers — `fetch_and_store` is the main example.

```python
@patch("app.api.fetch_and_store", new_callable=AsyncMock)
def test_run_fetch_defaults_to_all_categories(self, mock_fetch: AsyncMock) -> None:
    mock_fetch.return_value = {"sports_car": 10, "formula_car": 5}
    response = self.client.post("/admin/run-fetch", headers=self.headers)
    self.assertEqual(response.status_code, 200)
    mock_fetch.assert_awaited_once_with(None)
```

Source: `tests/test_run_fetch.py:41-49`. Note the `@patch` target is the **import site** (`app.api.fetch_and_store`), not the **definition site** (`app.services.fetch_and_store`) — this is critical because `app/api.py` does `from .services import fetch_and_store`.

Assertions on async mocks use `assert_awaited_once_with(...)` and `assert_not_awaited()` (see `tests/test_run_fetch.py:48, 62, 74`).

### Patching as a context manager

Used to control time and bypass real work in cache-eviction tests:

```python
with patch("app.services._utcnow", return_value=early_now):
    response1 = self.client.get(...)

with patch("app.services._utcnow", return_value=later_same_day), \
     patch("app.services.run_in_threadpool") as mock_threadpool:
    mock_threadpool.side_effect = RuntimeError("cache should be used")
    response2 = self.client.get(...)
```

Source: `tests/test_api.py:92-118`. The `RuntimeError` is a "tripwire" — the test asserts the response succeeds, proving the cache short-circuited before `run_in_threadpool` could be called. The reverse pattern (`wraps=services.run_in_threadpool` to count calls without replacing behaviour) is at `tests/test_api.py:121-132`.

### Internal cache invalidation

`tests/test_api.py:44` clears the module-level cache manually with `services._top_growers_cache.clear()` in `setUp`. New cached service functions should expose their cache dict at module scope so tests can drop it the same way.

### What is NOT mocked

- **The database.** Tests run against a real SQLite file (one per test module) via the production `get_session()`. SQLAlchemy itself is exercised end-to-end.
- **The FastAPI app.** `TestClient` drives the real router stack, including `Depends(require_license)` and Pydantic validation.
- **CSV parsing / `app.snapshots`.** Tests write real CSV files to disk and let the production loaders parse them.
- **The iRacing HTTP client.** No tests touch `app/iracing_client.py` directly. The only path that would call it (`fetch_and_store`) is mocked out at the route layer (`tests/test_run_fetch.py`), and snapshot-consuming tests pre-seed CSV files so `_ensure_snapshot` finds them without an HTTP call.

This means there is **no test coverage today for `app/iracing_client.py`, `app/scheduler.py`, or the OAuth refresh/retry paths**. Treat that as a known gap when adding integration-prone changes (it shows up in `CONCERNS.md`).

## How Each Layer Is Tested

### HTTP / route layer

- Use `fastapi.testclient.TestClient` with a hand-built `FastAPI()` that mounts only the routers in scope.
- Send requests with `self.client.get/post/delete(path, params=..., json=..., headers=...)`.
- Assert against `response.status_code` and `response.json()` (not `response.text`).
- For input validation, the assertion shape is:
  ```python
  self.assertEqual(response.status_code, 400)
  self.assertIn("Invalid cust_id", response.json()["detail"])
  ```
  See `tests/test_members_latest_batch.py:71-84` for invalid-input cases and `tests/test_run_fetch.py:66-74` for unsupported-category cases. The "Validate date range inputs for member delta" pattern follows the same shape, asserting on `response.status_code == 400` with a substring of `detail`.
- For license scoping, send `headers={"X-License-Key": license_a.key}` and assert returned rows all belong to that license — `tests/test_api.py:163-168`.

### Database / repository layer

There are **no dedicated repository tests** today. `app/repository.py` and `app/license_repository.py` are exercised transitively through route tests (`tests/test_license_admin.py`, `tests/test_member_sync.py`). If you add a new repository helper, add a focused unittest that wraps `with get_session() as session:` and asserts on the DB state directly, mirroring `test_sync_members_from_latest_snapshots` (`tests/test_member_sync.py:51-68`).

### Service layer (snapshot computation)

- Call `async def` services with `asyncio.run(coroutine)` — see `tests/test_repository.py:55-69`. There is no anyio plugin and no `IsolatedAsyncioTestCase`; the codebase deliberately bridges sync→async with `asyncio.run`.
- Seed snapshots with the `_write_csv` helper and assert on the returned `dict["results"]` payload.

### Scheduler layer

No tests for `app/scheduler.py`. The scheduler is not started in tests because tests build a fresh `FastAPI()` rather than importing `app.main.app`. APScheduler jobs and Discord webhook delivery are untested — adding tests would require mocking `httpx.AsyncClient` and `get_top_growers`.

## Common Patterns

### Time control

Patch `app.services._utcnow` (a one-line wrapper around `datetime.now(timezone.utc)` in `app/services.py:130-131`) and feed it timezone-aware UTC `datetime` instances. The "next cache expiry" boundary is at every 6-hour mark (00, 06, 12, 18 UTC) — pick `early_now=10:00 UTC` to land mid-window and `after_cutoff=12:01 UTC` to cross the next boundary.

### Async testing

- For routes: just use `TestClient` — it handles the async/sync bridge.
- For direct service calls: `asyncio.run(coro)`.
- For async mocks: `@patch(..., new_callable=AsyncMock)` and assert with `.assert_awaited_once_with(...)`.

### Error testing

Assert on `response.status_code` plus a substring of `response.json()["detail"]`. Example (`tests/test_members_latest_batch.py:71-84`):

```python
def test_latest_members_rejects_invalid_cust_ids(self) -> None:
    response = self.client.get("/members/latest", params={"cust_ids": "1,abc"})
    self.assertEqual(response.status_code, 400)
    self.assertIn("Invalid cust_id", response.json()["detail"])
```

### Idempotency / re-run testing

Run the same operation twice in one test and assert the post-condition is unchanged — see `tests/test_member_sync.py:62-68`:

```python
counts_second_run = sync_members_from_snapshots()
self.assertEqual(counts_second_run, 3)
with get_session() as session:
    members = session.query(Member).all()
    self.assertEqual(len(members), 3)
```

## Coverage Signals

There is no automated coverage tool, but qualitative coverage is:

| Module | Covered by | Coverage |
|--------|------------|----------|
| `app/api.py` | All test files | High (most routes hit) |
| `app/auth.py` | `test_api.py` (license scoping), `test_member_search.py` (admin-secret disabled) | Medium |
| `app/db.py` | All test files (transitive via `get_session()`) | High |
| `app/license_repository.py` | `test_license_admin.py`, `test_license_status.py` | High |
| `app/models.py` | All test files | High |
| `app/repository.py` | None directly | Untested |
| `app/schemas.py` | `test_api.py` create_subscription path | Medium |
| `app/services.py` | `test_api.py`, `test_repository.py`, `test_member_sync.py`, `test_members_latest_batch.py` | High for snapshot/growers paths |
| `app/scheduler.py` | None | Untested |
| `app/iracing_client.py` | None | Untested |
| `app/snapshots.py` | Transitive via services | Medium |
| `app/main.py` | None (tests build their own `FastAPI()`) | Untested |

When adding tests for previously untested modules, follow the `test_<module>.py` naming convention, copy the env-setup preamble verbatim, and prefer building a minimal `FastAPI()` over importing `app.main.app`.

---

*Testing analysis: 2026-05-27*
