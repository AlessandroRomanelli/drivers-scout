# Codebase Concerns

**Analysis Date:** 2026-05-27

## Tech Debt

**Module size — `app/services.py` is doing too much:**
- Issue: Single 587-line module owns DB init, member sync, CSV download orchestration, two in-memory caches with their own asyncio locks, snapshot resolution, delta math, and top-grower computation. There is no domain seam between transport-level concerns and business logic.
- Files: `app/services.py`
- Impact: Hard to reason about; every feature touches the same file. Caches are module-globals (`_top_growers_cache`, `_latest_snapshot_cache`) which couple all callers and complicate testing — tests must `services._top_growers_cache.clear()` manually (see `tests/test_api.py:44`).
- Fix approach: Split into `services/snapshots.py`, `services/growers.py`, `services/members.py`. Move caches into a small dedicated cache module with explicit `invalidate()` hooks. Inject the cache as a dependency in tests instead of clearing globals.

**Duplicated cache-lookup block in `get_top_growers`:**
- Issue: The cache lookup and "compute key / check expiry" code at lines `app/services.py:419–439` is repeated again at lines `app/services.py:466–494` after snapshot resolution, with a slightly different cache key. The first lookup uses the user-requested `effective_start/effective_end`, the second uses `start_used/end_used` from disk. This works but is hard to follow and easy to break.
- Files: `app/services.py`
- Impact: Risk of cache drift if either half is modified; readers cannot tell whether the second key is intentional invalidation or copy-paste.
- Fix approach: Extract `_get_cached(key, now)` and `_store_cache(key, payload)` helpers, then call them with the two keys explicitly. Add a comment explaining why both keys are used.

**Duplicate code between `app/snapshots.py` and `scripts/convert_snapshots.py`:**
- Issue: `scripts/convert_snapshots.py` reimplements `normalize_row`, `load_snapshot_rows`, `parse_snapshot_date`, and `load_snapshot_map` inline rather than importing from `app/snapshots.py` / `app/iracing_client.py`. The two copies have already drifted (the script is missing the `from typing import Any` import, see the **Known Bug** below).
- Files: `scripts/convert_snapshots.py` lines 16–53 vs `app/snapshots.py` lines 146–159 and `app/iracing_client.py:218–234`.
- Impact: Schema changes to snapshot rows (column rename, new typed field) must be made in two places. The script silently goes stale.
- Fix approach: Make the `scripts/` directory importable from `app.*` (already works since the project ships as a package) and replace inline definitions with imports.

**Module-level `Settings()` instantiation:**
- Issue: `settings.py:72` runs `settings = Settings()` at import time, which loads `.env` and validates required fields. Every test file therefore has to `os.environ.setdefault(...)` before the first `from app.*` import — see the boilerplate at the top of every test in `tests/`.
- Files: `app/settings.py:72`, all of `tests/*.py` lines 1–15.
- Impact: Importing any module from `app.*` in any tool (REPL, script, CI lint) requires `IRACING_USERNAME`, `IRACING_PASSWORD`, `IRACING_CLIENT_SECRET` to exist or it crashes with a pydantic validation error. This is fragile and makes the test files visually noisy.
- Fix approach: Provide a `get_settings()` accessor (lazy) and update consumers. Or supply non-required defaults for non-runtime contexts and assert presence only on first iRacing call.

**No `pyproject.toml` / `setup.cfg` / `tool.pytest.ini_options`:**
- Issue: The project is a Python package but there is no `pyproject.toml`, no `setup.cfg`, no `pytest.ini`, no `tox.ini`, no `conftest.py`. Tests rely on `unittest` discovery only.
- Files: project root.
- Impact: No declarative way to pin Python version, declare test paths, configure coverage, or install the package. Contributors cannot `pip install -e .` to get a real package; they must rely on running scripts from the repo root.
- Fix approach: Add a `pyproject.toml` with `[project]` metadata, declare test discovery under `[tool.pytest.ini_options]`, and add a `conftest.py` that centralizes the test env-var setup currently duplicated across all 8 test files.

**Two parallel member-sync code paths:**
- Issue: `app/repository.py` exposes `ensure_members()` with batch upserts and conflict resolution. `app/services.py:59` (`sync_members_from_snapshots`) uses a hand-rolled `CREATE TEMPORARY TABLE member_staging` + `INSERT OR IGNORE` instead of calling `ensure_members`. They achieve overlapping goals via different SQL.
- Files: `app/repository.py:13–69`, `app/services.py:59–121`.
- Impact: Maintenance burden; new member fields must be added in two places. The `INSERT OR IGNORE` path also will never update an existing member's `display_name` or `location`, whereas `ensure_members` does.
- Fix approach: Pick one. The `ensure_members` upsert path is more correct (preserves latest non-null values via `COALESCE`).

## Known Bugs

**`scripts/convert_snapshots.py` will crash on import — missing `Any` import:**
- Symptoms: `NameError: name 'Any' is not defined` raised the moment `normalize_row` is defined.
- Files: `scripts/convert_snapshots.py:16` uses `Dict[str, Any]` but line 8 only imports `Dict, Iterator, Tuple` from `typing`.
- Trigger: Running `python scripts/convert_snapshots.py --root ...` as documented in `README.md:64`.
- Workaround: Add `Any` to the `from typing import ...` line.

**`location.lower()` in scheduler crashes when location is `None`:**
- Symptoms: `AttributeError: 'NoneType' object has no attribute 'lower'` during Discord delivery.
- Files: `app/scheduler.py:176` — `f":flag_{item.get('location').lower() or 'aq'}: {driver}"`.
- Trigger: Any top-grower entry whose CSV `LOCATION` column is empty (`normalize_row` returns `None` for missing strings, see `app/iracing_client.py:230`). Test fixture `tests/test_member_sync.py:40` already exercises an empty-location row.
- Workaround: Use `(item.get("location") or "aq").lower()` (note: the `or` precedence in the current code is wrong — `.lower()` runs before `or`, so even an empty string `""` returns `""` rather than `"aq"`).

**Per-request `IRacingClient()` creates a new HTTPX client + new token on every API call:**
- Symptoms: Every call to `/members/{cust_id}/latest`, `/members/latest`, `/members/{cust_id}/delta`, `/leaders/growers` invokes `IRacingClient()` and `await client.close()` (see e.g. `app/services.py:223–240`, `:244–285`, `:289–324`, `:341–382`, `:441–587`). Each construction opens an `httpx.AsyncClient` and forces a new login on first use (token is instance-scoped, not module-scoped).
- Files: `app/iracing_client.py:37–40` (token stored on instance), `app/services.py` (multiple `IRacingClient()` call sites).
- Trigger: Every authenticated read endpoint; latency for cache misses compounds the OAuth round-trip.
- Workaround: None at runtime — there is just always an extra token request.

**Rate-limit semaphore reset is racy and resets in-flight permits:**
- Symptoms: Inside `_throttle`, when `now >= self._rate_reset`, the code re-creates the semaphore (`self._rate_limit_lock = asyncio.Semaphore(...)`, `app/iracing_client.py:50`). Any tasks currently blocked on the old semaphore remain blocked on a now-orphaned object while new tasks race on a fresh one with full capacity.
- Files: `app/iracing_client.py:43–55`.
- Trigger: Any concurrent burst of requests crossing a one-minute window boundary.
- Workaround: None — combined with the previous bug (one client per request), each request has its own semaphore anyway, so the issue is masked; once the client is cached/shared the bug becomes observable.

**`fetch_and_store` swallows snapshot-day timezone differences silently:**
- Symptoms: `snapshot_day = datetime.now(tz).date()` (`app/services.py:197`) uses `APP_TIMEZONE`, but `_ensure_snapshot` / `get_latest_snapshot` use `date.today()` (`app/services.py:225, :246, :291`). If `APP_TIMEZONE=Europe/Zurich` (as in `.env.example:1`), a fetch at 01:00 local writes a snapshot for "today local" but read endpoints look for "today UTC" — at the UTC day boundary they will not find it.
- Files: `app/services.py:197` vs `:225, :246, :291`.
- Trigger: `APP_TIMEZONE` set to anything not equivalent to UTC.
- Workaround: Set `APP_TIMEZONE=UTC` or always read with explicit dates.

**`get_irating_delta` `days` semantics differ from `get_top_growers`:**
- Symptoms: When the user passes only `end_date` (no `start`, no `days`), `get_irating_delta` defaults `start_date = end_date - 1 day` (`app/services.py:339`). When `days` is provided, it uses `end_date - days`. The router enforces "both `start` and `end` together" for the user-facing case (`app/api.py:251–263`), but the internal service still has the fallback path. If a future caller invokes the service with only `end_date`, results are surprising.
- Files: `app/services.py:327–339`.
- Trigger: Direct service callers.
- Workaround: Always pass `days` or both dates.

**`shutdown_scheduler` runs even when startup failed:**
- Symptoms: In `app/main.py:33–48`, the `finally:` block calls `shutdown_scheduler()` even if `start_scheduler()` raised. If `start_scheduler` raised after `scheduler.start()` succeeded but before `add_job` finished, partial state may leak; conversely, if it never started, `shutdown_scheduler` does nothing — but the broader concern is that DB init errors still try to shut down the scheduler.
- Files: `app/main.py:33–48`.
- Trigger: Any error in `init_db` or `start_scheduler`.
- Workaround: Move shutdown into a separate try/except path.

## Security Considerations

**License key authentication bypasses entire API when `LICENSE_ADMIN_SECRET` is unset:**
- Risk: `app/auth.py:84–85` — `require_license` returns immediately (skips license check) when `settings.license_admin_secret` is falsy. This means an unset admin secret disables license validation for *all* licensed endpoints (`/members/*`, `/leaders/*`, `/subscriptions/*`), not just admin endpoints. A deployment that forgets to set `LICENSE_ADMIN_SECRET` is fully open.
- Files: `app/auth.py:73–95`.
- Current mitigation: None — the `.env.example` does not list `LICENSE_ADMIN_SECRET`, so first-time operators are likely to skip it.
- Recommendations:
  1. Decouple "admin endpoint protection" from "license check enabled" — these are independent concerns.
  2. Fail closed: if `LICENSE_ADMIN_SECRET` is missing in production, refuse to start, or at minimum refuse `/admin/*` requests. Do not silently disable license validation.
  3. Add `LICENSE_ADMIN_SECRET=` to `.env.example` with a comment that empty disables license checks (current behaviour).

**Admin endpoints rely on a single shared static header secret:**
- Risk: `app/api.py:36–39` compares `X-Admin-Secret` with `==`, not `secrets.compare_digest`. Theoretically timing-attackable; more practically, a static long-lived secret with no rotation story.
- Files: `app/api.py:36–39`, `app/settings.py:52–54`.
- Current mitigation: HTTPS at the edge would shield the header in transit, but the codebase does not enforce HTTPS.
- Recommendations: Use `secrets.compare_digest`; consider per-operator API keys backed by the existing `License` model with an admin flag.

**Webhook URL is fetched without scheme/host allow-listing:**
- Risk: `app/scheduler.py:197` posts to `subscription.webhook_url` (any URL the licensee set). A malicious or compromised license could point at internal services (SSRF) — e.g. `http://localhost:8000/admin/...`. `httpx.AsyncClient` follows redirects too.
- Files: `app/scheduler.py:119–217`, `app/schemas.py:11–22`.
- Current mitigation: Pydantic's `HttpUrl` validates URL syntax but not host/scheme allowlist. Schema does not pin to `https`.
- Recommendations: Enforce `https://`, validate host against a known Discord domain (`discord.com`, `discordapp.com`) before submission and again at delivery time, disable redirect-following.

**`pickle.load` on snapshot maps is unsafe in principle:**
- Risk: `app/snapshots.py:164` and `scripts/convert_snapshots.py:78` use `pickle.load` / `pickle.dumps`. If an attacker gained write access to the `SNAPSHOTS_DIR`, a malicious `.pkl` would execute arbitrary code on load. This is a defense-in-depth concern, not a current exploit, because the directory is operator-controlled.
- Files: `app/snapshots.py:162–164, 167–179`.
- Current mitigation: Snapshots are written only by the service itself.
- Recommendations: Switch to `msgpack` or a checksummed JSON envelope if snapshot files might ever be shared across machines, restored from backups, or written by other tooling.

**Member display-name search injects user input into ILIKE pattern:**
- Risk: `app/api.py:199` — `Member.display_name.ilike(f"%{term}%")`. SQLAlchemy parameterizes the value so it is not a SQL-injection vector, but the wildcard characters `%` and `_` in `term` are not escaped, allowing a caller to short-circuit indices and force a full table scan.
- Files: `app/api.py:182–214`.
- Current mitigation: `limit` is capped at 100; query length minimum 3.
- Recommendations: Escape `%` and `_` in `term` (`term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")` with `escape="\\"`).

**Logs may include license keys at WARN level:**
- Risk: `app/auth.py:60–64` logs `license_key=token` with the supplied header value when license validation fails. Licenses are bearer credentials; logging them is roughly equivalent to logging passwords.
- Files: `app/auth.py:54–68`.
- Current mitigation: Only logged on failure, but failure log volume is uncontrolled and could be tailed by anyone with log access.
- Recommendations: Log only a hash prefix (`token[:6] + '…'`).

**Credentials and tokens in memory only, no rotation hooks:**
- Risk: iRacing username/password are loaded from `.env` and held in `Settings`. Refresh tokens live on an `IRacingClient` instance; there is no signing or audit of when credentials are reused. Acceptable for current scale but worth documenting.
- Files: `app/settings.py:27–31`, `app/iracing_client.py:23–32`.
- Current mitigation: `.env` is git-ignored (`.gitignore:69–71`).
- Recommendations: None urgent; document the credential lifecycle in `README.md`.

## Performance Bottlenecks

**Sequential snapshot loading inside `get_top_growers`:**
- Problem: `start_map = load_snapshot_map_cached(start_path)` and `end_map = load_snapshot_map_cached(end_path)` run sequentially inside the threadpool function (`app/services.py:505–506`). Each map can be 10k+ rows.
- Files: `app/services.py:496–560`.
- Cause: Both files unpickled or CSV-parsed back to back.
- Improvement path: Load in parallel with `asyncio.to_thread` for each, or memoize by `(path, mtime)` at the module level (the previous `lru_cache` was removed — commit `fd9a6a3` — but a path/mtime-keyed dict cache would still help for the hot weekly window).

**`load_snapshot_map_cached` re-reads `.pkl` from disk on every call:**
- Problem: `_load_snapshot_map_binary` takes `(path, mtime)` as a clear cache key but is no longer wrapped in `@lru_cache` (commit `fd9a6a3` "removed lru_cache for loading of pickle files"). The `mtime` parameter is now unused dead weight.
- Files: `app/snapshots.py:162–179`.
- Cause: Cache was intentionally removed (likely to fix memory growth or stale cache after re-fetch) but no replacement bounded cache exists. Every grower call re-reads two pickle files from disk.
- Improvement path: Re-introduce a small bounded cache keyed by `(str(path), mtime)` with explicit invalidation when a new snapshot is stored.

**`fetch_and_store` writes a `.pkl` companion for every CSV synchronously inside the request path:**
- Problem: `store_snapshot` calls `store_snapshot_map` after writing the CSV (`app/snapshots.py:76–86`), which re-parses the CSV content and pickles it. For 60k-row CSVs this measurably extends the admin endpoint latency.
- Files: `app/snapshots.py:67–87`.
- Cause: Inline conversion at write time.
- Improvement path: Offload to a background task (`asyncio.create_task`) or to APScheduler.

**`fetch_category_csv` rebuilds `csv.DictReader` per line:**
- Problem: `for row in csv.DictReader([line], fieldnames=fieldnames)` (`app/iracing_client.py:192`) constructs a new reader per data line. This is wasteful at 60k+ lines.
- Files: `app/iracing_client.py:189–198`.
- Cause: Avoiding holding the full body in memory by streaming line-by-line.
- Improvement path: Use `csv.reader(line_iter)` once after consuming the header, or buffer chunks rather than one line at a time. Note: the production path actually goes through `download_category_csv` (line 200), which uses `csv_resp.text` and ignores the streaming code entirely — `fetch_category_csv` is effectively dead code now.

**Scheduled job runs three multi-minute steps serially with hard-coded sleeps:**
- Problem: `scheduled_job` calls `fetch_and_store("sports_car")` → `asyncio.sleep(60)` → `fetch_and_store("formula_car")` → `asyncio.sleep(60)` → `sync_members_from_snapshots_async()` (`app/scheduler.py:43–56`). Total wall clock ≥ 120s of sleep plus three blocking phases. The 60s sleeps are hand-tuned to dodge iRacing rate limits but there is no adaptive back-off — if the iRacing API tightens its limits the schedule silently fails (caught by `IRacingClient` retry, but with no alert).
- Files: `app/scheduler.py:43–56`.
- Cause: Manual pacing.
- Improvement path: Replace `asyncio.sleep(60)` with rate-limiter-aware sequencing, or schedule the two categories as independent jobs with separate cron entries.

**Module-global caches grow unbounded:**
- Problem: `_top_growers_cache` and `_latest_snapshot_cache` (`app/services.py:35–44`) have no size cap. Keys include `(category, start_date, end_date, limit, min_current_irating)` and `(category, date, cust_id)`. A caller varying `limit`, `min_current_irating`, or `cust_id` widely can grow the cache without bound until the next 6-hour expiry sweep — and that sweep is lazy (entries are checked at lookup time, never proactively evicted).
- Files: `app/services.py:35–44, 254–285, 419–494, 579–584`.
- Cause: Hand-rolled cache without `OrderedDict`/`LRU` semantics.
- Improvement path: Wrap with `cachetools.TTLCache` (size-bounded, time-evicting) or implement explicit LRU eviction.

## Fragile Areas

**Scheduler job registration — recently stabilized but still relies on global singleton:**
- Files: `app/scheduler.py:33` (`scheduler = AsyncIOScheduler(...)`), `:271–305`.
- Why fragile: Recent commit "Stabilize scheduler job registration" (`0f748be`) added the `if scheduler.running: ... return` guard at line 276 and `replace_existing=True` on `add_job`. This handles re-import / double-startup, but the scheduler is still a module-level singleton — multiple `start_scheduler()` calls from different event loops (rare but possible during testing with multiple TestClients) would fail because APScheduler binds to the current loop on `.start()`.
- Safe modification: Always change schedule via the constants `SCHEDULE_HOURS` and `SCHEDULE_HOURS_EXPRESSION` at the top of the file; do not re-instantiate `scheduler` mid-process.
- Test coverage: **None** for the scheduler — `tests/` contains zero scheduler tests. The recent stabilization is unverified by automated tests.

**Binary snapshot map persistence — recently added, fallback path under-tested:**
- Files: `app/snapshots.py:31–179`, `scripts/convert_snapshots.py`.
- Why fragile: The `.pkl` side-channel was introduced in commits `f5e96b5` and `7e9408e`. `load_snapshot_map_cached` (lines 167–179) silently falls back to CSV on `pickle` exceptions — good for resilience, but it means a corrupted `.pkl` will not raise; performance just degrades. The companion script that creates `.pkl` files (`convert_snapshots.py`) is broken on import (see Known Bugs). `_load_snapshot_map_binary` has a `mtime` parameter that is unused dead weight after the `lru_cache` removal.
- Safe modification: Always write `.csv` first, `.pkl` second (current order is correct). Treat `.pkl` as a cache, never as the source of truth.
- Test coverage: No tests in `tests/` exercise the pickle path or `load_snapshot_map_cached`. `test_run_fetch.py` mocks `fetch_and_store` entirely.

**Date range validation — newly added but inconsistent between endpoints:**
- Files: `app/api.py:241–311` (validation), `app/services.py:327–403`.
- Why fragile: The validation logic at `app/api.py:251–263` (member delta) and `:283–295` (growers) is **almost** identical but copy-pasted — there is no shared helper. The growers default of `days=30` and the delta default of `days=1` are encoded as magic numbers in different branches (`app/api.py:263, 295`). The corresponding service `get_irating_delta` still allows the "only end_date" path that the router has just rejected.
- Safe modification: Extract `_resolve_date_range(start, end, days, default_days)` helper into `app/api.py` and reuse from both endpoints; align service-layer behaviour.
- Test coverage: No tests exercise the 400 error paths added for `start`/`end` validation — `tests/test_repository.py` only covers happy paths through the service layer, not the API surface.

**Module-global state shared across tests:**
- Files: `app/services.py:35–44, 39, 44`, `app/scheduler.py:228` (`discord_delivery_lock`).
- Why fragile: `_top_growers_cache`, `_top_growers_cache_lock`, `_latest_snapshot_cache`, `_latest_snapshot_cache_lock`, `discord_delivery_lock`, and `settings` are all module singletons that survive between tests. Tests already paper over this by mutating `settings.categories`, `settings.license_admin_secret`, and clearing the cache manually (e.g. `tests/test_api.py:44`, `tests/test_run_fetch.py:26, 39`). Adding a new test that forgets these resets will fail nondeterministically.
- Safe modification: Add a `conftest.py` with a `pytest` fixture that snapshots and restores settings, and a clean-cache fixture; or move caches behind a class.
- Test coverage: N/A — the fragility is in the test infrastructure itself.

**OAuth retry sleeps with exponential backoff but no jitter and no cap:**
- Files: `app/iracing_client.py:65–69, 142–146, 156–160`.
- Why fragile: `await asyncio.sleep(2 ** attempt)` with `attempt in range(3)` is fine for the current 3-retry limit, but the broad `except Exception` swallows everything including `asyncio.CancelledError` (until Python 3.8 it was a subclass of `Exception`; in 3.11+ it is not, but the code does not distinguish). Mixing transport errors (timeouts) with logic errors (`KeyError` on missing JSON fields) under one retry loop hides bugs.
- Safe modification: Narrow the `except` to `httpx.HTTPError` and `httpx.TransportError`. Let logic errors propagate.

## Scaling Limits

**SQLite as the only supported database:**
- Current capacity: Default `DATABASE_URL=sqlite:///./iracing_stats.db` (`app/settings.py:43`, `.env.example:3`). `engine = create_engine(..., connect_args={"check_same_thread": False})` (`app/db.py:12–14`) hard-codes a SQLite-specific argument.
- Limit: Single-writer; cross-host scaling is impossible without a Postgres swap. The `app/services.py:80` path also uses `CREATE TEMPORARY TABLE` + `INSERT OR IGNORE`, which are SQLite-specific syntax.
- Scaling path: Abstract DB dialect; replace `INSERT OR IGNORE` with `INSERT ... ON CONFLICT DO NOTHING` (Postgres-compatible) via `dialects.sqlite.insert` (already used elsewhere — `app/repository.py:52`). Drop `check_same_thread=False` for non-SQLite backends.

**Snapshot storage is per-host filesystem:**
- Current capacity: `SNAPSHOTS_DIR` defaults to `./snapshots` (`app/settings.py:23–25`). Each daily CSV per category is on the order of a few MB; growth is linear and bounded.
- Limit: Cannot scale horizontally — every replica needs the same disk. The `.pkl` side-channel doubles storage.
- Scaling path: Move to object storage (S3/GCS) with a small local LRU. Migration would require swapping `Path` operations in `app/snapshots.py` with an abstraction.

**Rate limit burst is global to one client instance:**
- Current capacity: `RATE_LIMIT_BURST=5`, `IRACING_RATE_LIMIT_RPM=60` (`.env.example:9, 11`).
- Limit: With multiple uvicorn workers each having their own `IRacingClient`, the effective RPM is `60 × workers`, which will trip iRacing rate limits.
- Scaling path: Run a single worker (current default `reload=False` in `app/main.py:73`), or move rate limiting to an external store (Redis token bucket).

**Caches share one process:**
- Current capacity: All caching is in-process (`_top_growers_cache`, `_latest_snapshot_cache`).
- Limit: A second uvicorn worker doubles the iRacing load and disk reads for the same query.
- Scaling path: Move cache to Redis when crossing one process.

## Dependencies at Risk

**Unpinned upper bounds in `requirements.txt`:**
- Risk: All entries use `>=` only (`requirements.txt:1–8`): `fastapi>=0.111.0`, `uvicorn[standard]>=0.30.0`, `apscheduler>=3.10.4`, `sqlalchemy>=2.0.30`, `pydantic-settings>=2.3.4`, `httpx>=0.27.0`, `python-dotenv>=1.0.1`. No lockfile, no `requirements.lock`, no `uv.lock`, no `poetry.lock`.
- Impact: Two installs days apart can produce different dependency versions. APScheduler 4.x is a substantial rewrite — a future `pip install -r requirements.txt` would silently pull it and break the scheduler API used in `app/scheduler.py:13–14, 33`. Same risk for Pydantic v3 breaking `pydantic-settings`.
- Migration plan: Add a `requirements.lock` produced by `pip-compile`/`uv pip compile`, or migrate to `pyproject.toml` with `[project.dependencies]` and a `uv.lock`. Add upper bounds: `apscheduler>=3.10.4,<4`, `pydantic-settings>=2.3.4,<3`, `httpx>=0.27.0,<1`.

**`pydantic-settings` `extra="ignore"` masks misconfiguration:**
- Risk: `app/settings.py:61` sets `extra="ignore"`. A typo in `.env` (e.g. `IRACING_USRENAME=...`) is silently ignored.
- Impact: Production misconfigurations surface only at runtime when the missing required field is accessed.
- Migration plan: Switch to `extra="forbid"` and explicitly accept the small set of legacy variables if any.

**No production WSGI/ASGI process manager configuration:**
- Risk: The README documents `python -m app` only (`README.md:33–35`). `uvicorn` runs as a single process with `reload=False`. There is no Gunicorn config, no `uvicorn` `--workers`, no systemd unit, no container manifest.
- Impact: Deployment is undocumented; recovery from a crash relies on whatever orchestrator the operator chose.
- Migration plan: Add a `Dockerfile` and at minimum a documented `uvicorn` invocation with worker count constrained to 1 (to match the global-cache design).

## Missing Critical Features

**No automated database migrations:**
- Problem: Schema changes rely on `Base.metadata.create_all(engine)` (`app/services.py:151`). This *adds* tables but never alters them. Adding a column to `Subscription` or `Member` will not propagate.
- Blocks: Any backwards-compatible schema evolution. Operators must drop the SQLite file or manually `ALTER TABLE`.
- Fix: Adopt Alembic; generate baseline migration from current models.

**No structured logging / no request IDs:**
- Problem: `logging.basicConfig` (`app/main.py:21–29`) is plain text format. No correlation ID per request, no JSON output for log aggregation, no per-license rate metrics.
- Blocks: Debugging production incidents involving a single subscription or license requires grepping by `subscription.id` across unstructured lines.
- Fix: Add a request-ID middleware and switch to structured (JSON) handler; emit `extra={"license_key_hash": ...}` at INFO.

**No health-check distinguishes liveness from readiness:**
- Problem: `/health` (`app/api.py:67–69`) returns `{"status": "ok"}` unconditionally. It does not check DB connectivity, scheduler running, snapshots dir writable, or iRacing client reachable.
- Blocks: Kubernetes / Render / Fly readiness probes cannot tell when the service is degraded.
- Fix: Split into `/livez` (process up) and `/readyz` (DB query, scheduler alive, last successful fetch < threshold).

**No metrics endpoint:**
- Problem: No `/metrics`, no Prometheus, no OpenTelemetry hooks. Cache hit rates, iRacing call duration, fetch counts are only visible in unstructured logs.
- Blocks: Capacity planning, SLO tracking, alerting.
- Fix: Add `prometheus-fastapi-instrumentator` or OpenTelemetry exporter.

**No webhook retry on transient Discord failures:**
- Problem: `app/scheduler.py:204–212` logs a warning on non-2xx Discord responses but does not retry. A transient 503 means the user misses that week's update entirely.
- Blocks: Reliable weekly delivery.
- Fix: Retry with exponential backoff on 5xx and 429 (honoring `Retry-After`).

**No tests for the scheduled Discord delivery path:**
- Problem: `deliver_discord_subscriptions` is 170 lines of business logic with iRacing-week math, embed building, and webhook posting (`app/scheduler.py:59–225`). Zero unit tests cover it.
- Blocks: Any refactoring or formatting change is high-risk.
- Fix: Add tests that mock `get_top_growers` and `httpx.AsyncClient`, then assert payload shape and lock behaviour.

## Test Coverage Gaps

**Scheduler & Discord delivery:**
- What's not tested: `app/scheduler.py:43–225` — `scheduled_job`, `deliver_discord_subscriptions`, `deliver_discord_subscriptions_guarded`, `_iracing_week`, `_snapshot_end_datetime`, `_format_snapshot_range`, `start_scheduler`, `shutdown_scheduler`.
- Files: `app/scheduler.py`.
- Risk: The very behaviour the recent "Stabilize scheduler job registration" PR (#67) and "Guard discord subscription deliveries" PR (#61) introduced is unverified. The `:flag_{location.lower()}` `None`-crash bug above would be caught by a single test.
- Priority: **High**.

**`scripts/convert_snapshots.py`:**
- What's not tested: The entire file. It has an unimported `Any` symbol that would crash on first run — a smoke test would catch this.
- Files: `scripts/convert_snapshots.py`.
- Risk: Operators following `README.md:60–67` hit `NameError`.
- Priority: **High** (because it's broken).

**Snapshot pickle round-trip and fallback:**
- What's not tested: `app/snapshots.py:58–87` (`store_snapshot_map`, `_snapshot_map_from_content`, `store_snapshot` with `emit_map=True`), `app/snapshots.py:162–179` (`_load_snapshot_map_binary`, `load_snapshot_map_cached` fallback path on corruption), `app/snapshots.py:125–143` (`find_closest_snapshot` with `include_pkl=True`).
- Files: `app/snapshots.py`.
- Risk: Silent regression in the perf-critical hot path that all read endpoints depend on.
- Priority: **High**.

**License auth dependency:**
- What's not tested: `app/auth.py:46–95` — header extraction (Bearer vs X-License-Key), exempt-path logic, the dangerous fallback at `:84–85` (license disabled when admin secret unset).
- Files: `app/auth.py`.
- Risk: A future refactor to fix the open-by-default issue would have nothing to catch regressions.
- Priority: **High** (because of the security implication).

**iRacing client retry / refresh paths:**
- What's not tested: `app/iracing_client.py:43–161` — `_throttle` window reset, 401 retry → refresh → re-login chain in `_authorized_get`, `_build_token` defaults, CSV streaming.
- Files: `app/iracing_client.py`.
- Risk: Token expiry mid-fetch is unhandled in tests; the iRacing API behaviour at the 401 boundary is critical to daily fetch reliability.
- Priority: **Medium**.

**Date-range validation endpoint paths:**
- What's not tested: `app/api.py:251–263` (member delta `start/end` mutual exclusion with `days`, required-together), `:283–295` (same for growers).
- Files: `app/api.py`, no corresponding test in `tests/`.
- Risk: The recently merged PR #66 (`Validate date range inputs for member delta`) has no regression test. A future refactor can re-introduce the bug.
- Priority: **Medium**.

**Cache invalidation after a fresh fetch:**
- What's not tested: The interaction between `fetch_and_store` writing a new snapshot and the still-cached `_top_growers_cache` / `_latest_snapshot_cache` payloads for the same date. The cache TTL is aligned to 6-hour boundaries (`_next_cache_expiry`, `app/services.py:138–144`) which may match the schedule, but there is no test that proves cache and schedule stay aligned.
- Files: `app/services.py:138–144`, `app/services.py:192–217`.
- Risk: After a fetch, reads can return stale data for up to 6 hours.
- Priority: **Medium**.

**Subscription upsert behaviour:**
- What's not tested: `app/api.py:314–345` — the branch that detects an existing `(license_key, category)` row and updates `webhook_url` / `min_irating` (and returns 200 instead of 201). `tests/test_api.py:134–168` only lists subscriptions.
- Files: `app/api.py:314–345`.
- Risk: A regression in the upsert branch would silently create duplicate subscriptions and violate the `uq_subscriptions_license_category` unique constraint.
- Priority: **Medium**.

**Member sync from snapshots when no snapshots exist:**
- What's not tested: `sync_members_from_snapshots` early-return path when `_latest_snapshot_for_category` returns `None` (`app/services.py:65–67`).
- Files: `app/services.py:59–122`.
- Risk: Cold-start behaviour on a fresh deployment.
- Priority: **Low**.

**Generally: no `conftest.py`, no coverage measurement:**
- What's not tested: There is no `conftest.py`, no `pytest-cov` configuration, no CI workflow visible in the repo, and no coverage badge. The 8 test files use `unittest` only, with hand-rolled env-var setup at the top of each file.
- Risk: Coverage is unknown; regressions slip in unnoticed.
- Priority: **Medium** — add `pytest-cov` to `requirements.txt` and a `pyproject.toml` `[tool.coverage]` section.

---

*Concerns audit: 2026-05-27*
