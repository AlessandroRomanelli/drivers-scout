# External Integrations

**Analysis Date:** 2026-05-27

## APIs & External Services

**iRacing Members API (primary upstream):**
- iRacing OAuth token endpoint — `https://oauth.iracing.com/oauth2/token` (`app/iracing_client.py:18`)
  - SDK/Client: custom `IRacingClient` built on `httpx.AsyncClient` (`app/iracing_client.py:34-216`)
  - Auth: OAuth 2.0 `password_limited` grant for initial login (`app/iracing_client.py:72-88`); subsequent `refresh_token` grant with client credentials (`app/iracing_client.py:90-106`). Tokens cached in-memory in a `TokenInfo` dataclass with `expires_at` (`app/iracing_client.py:22-31`).
  - Refresh behaviour: token treated as expiring within 60s of expiry (`app/iracing_client.py:30-31`); on `401` the client retries once with `refresh`, then once with a full re-`login` (`app/iracing_client.py:122-147`). All POSTs retry up to 3× with exponential backoff (`app/iracing_client.py:57-69`).
  - Env vars: `IRACING_USERNAME`, `IRACING_PASSWORD`, `IRACING_CLIENT_ID` (default `ar-pwlimited`), `IRACING_CLIENT_SECRET`, `IRACING_SCOPE` (default `iracing.auth`) (`app/settings.py:27-31`).

- iRacing driver-stats CSV endpoint — `https://members-ng.iracing.com/data/driver_stats_by_category/{category}` (`app/iracing_client.py:19`, `app/iracing_client.py:163-212`)
  - Two-stage protocol: GET the data URL with `Authorization: Bearer <token>` to receive a JSON envelope whose `"link"` field is a signed CSV URL (`app/iracing_client.py:167-171`), then GET that link without auth to stream/download the CSV (`app/iracing_client.py:173-211`).
  - Two access modes: streaming (`fetch_category_csv`, line-by-line via `httpx.AsyncClient.stream`) and bulk (`download_category_csv`, single `client.get(...).text`). The scheduler path uses the bulk form via `services.fetch_and_store` → `_download_snapshot` (`app/services.py:154-156`).
  - Rate limiting: per-client `asyncio.Semaphore` initially sized to `RATE_LIMIT_BURST` (default 5), then reset to `IRACING_RATE_LIMIT_RPM` (default 60) on each new 60-second window (`app/iracing_client.py:39-55`).
  - Timeout: `HTTP_TIMEOUT_SECONDS` (default 15.0) (`app/iracing_client.py:39`).
  - Categories fetched: configured via `CATEGORIES` (default `sports_car`); the scheduled job hard-codes a pair run of `sports_car` then `formula_car` with 60s gaps (`app/scheduler.py:43-56`).

**Discord (outbound notifications):**
- Per-subscription webhook URLs stored on `Subscription.webhook_url` (`app/models.py:42-55`). The scheduler POSTs Discord embed payloads to these URLs weekly (`app/scheduler.py:59-225`).
  - Client: `httpx.AsyncClient(timeout=settings.http_timeout_seconds)` (`app/scheduler.py:119`).
  - Payload: a single `embeds[0]` object with a "Weekly Top iRating Growers" title, a `Subscription Data` field, and one field per top grower including iRating delta, wins/starts diff, and flag emoji (`app/scheduler.py:156-185`).
  - Auth: none beyond the webhook URL itself (Discord webhook URLs are bearer-secrets in the URL path).
  - Reliability: each delivery is wrapped in `try/except` and logged at `exception` level; non-2xx responses log a warning but do not retry (`app/scheduler.py:197-217`). Concurrency is gated by `discord_delivery_lock`, an `asyncio.Lock` with 1s acquisition timeout (`app/scheduler.py:228-243`).
  - Scheduling: Monday 23:58 UTC cron (`app/scheduler.py:292-305`); also runnable on demand via `POST /admin/discord-subscriptions/run?subscription_id=...` (`app/api.py:102-117`).

**FlagCDN (frontend only, static dashboard):**
- `https://flagcdn.com/{size}/{cc}.png` referenced in `assets/index.js:295-297` for country flag thumbnails next to driver names. No server-side dependency.

**jsDelivr CDN (frontend only):**
- `https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js` loaded by `index.html:8` to render dashboard charts.

## Data Storage

**Databases:**
- SQLite (default) via SQLAlchemy
  - Connection: `DATABASE_URL` env var, default `sqlite:///./iracing_stats.db` (`app/settings.py:43-45`, `.env.example:3`).
  - Engine bootstrap: `create_engine(settings.database_url, connect_args={"check_same_thread": False})` in `app/db.py:12-14`.
  - Session factory: `sessionmaker(autoflush=False, autocommit=False, expire_on_commit=False)` exposed via `get_session()` contextmanager in `app/db.py:15-29`.
  - Schema migrations: none. Tables are auto-created by `Base.metadata.create_all(engine)` in `services.init_db()` (`app/services.py:147-151`), invoked at startup from the FastAPI `lifespan` (`app/main.py:33-48`).
  - Tables (`app/models.py`):
    - `members(cust_id PK, display_name, location, created_at)` — synced from latest CSV snapshot via `sync_members_from_snapshots()` (`app/services.py:59-122`), which uses a SQLite `TEMPORARY TABLE` + `INSERT OR IGNORE` strategy.
    - `licenses(key PK, label, active, revoked_at, created_at)` — managed by `app/license_repository.py` using SQLite-flavoured `INSERT ... ON CONFLICT DO UPDATE` (`app/license_repository.py:7-31`).
    - `subscriptions(id PK, license_key FK→licenses.key ON DELETE CASCADE, webhook_url, category, min_irating, created_at)` with `UniqueConstraint(license_key, category)` (`app/models.py:39-55`).

**File Storage:**
- Local filesystem only. The authoritative time-series store is `SNAPSHOTS_DIR/<category>/<YYYY-MM-DD>.csv` with an optional sibling `<YYYY-MM-DD>.pkl` cache (`app/snapshots.py:21-34`, `app/snapshots.py:58-87`).
- Snapshot lifecycle:
  - Writers: `store_snapshot` (CSV) and `store_snapshot_map` (pickled `{cust_id: row}` map) (`app/snapshots.py:58-87`).
  - Readers: `load_snapshot_rows` for streaming and `load_snapshot_map_cached` for indexed access, which prefers `.pkl` and falls back to CSV on any load error (`app/snapshots.py:146-179`).
  - Maintenance: `scripts/convert_snapshots.py` regenerates `.pkl` siblings from existing CSVs (`scripts/convert_snapshots.py:96-127`).
- No object storage (S3/GCS/Azure), no remote blob backend.

**Caching:**
- In-process Python dicts only, guarded by `asyncio.Lock`:
  - `_top_growers_cache` keyed by `(category, start, end, limit, min_current_irating)` in `app/services.py:35-39`.
  - `_latest_snapshot_cache` keyed by `(category, snapshot_date, cust_id)` in `app/services.py:40-44`.
  - Cache entries expire at the next 6-hour slot boundary aligned to UTC midnight (`app/services.py:138-144`).
- No Redis, Memcached, or other shared cache.

## Authentication & Identity

**Service-level auth (inbound, custom):**
- License-key API authentication implemented in `app/auth.py`.
  - Accepted headers: `X-License-Key` or `Authorization: Bearer <key>` (`app/auth.py:24-36`).
  - Validation: lookup in `licenses` table; reject when missing or `active=False` (`app/auth.py:46-70`).
  - Enforcement is **conditional**: `require_license` is a no-op unless `LICENSE_ADMIN_SECRET` is set, and it always exempts `/health`, `/admin/*`, and `/licenses/*` paths (`app/auth.py:16, 73-95`).
  - The licensed-only routes (`/subscriptions`, `/members/*`, `/leaders/growers`) are mounted on `router = APIRouter(dependencies=[Depends(require_license)])` in `app/api.py:33`; the open routes plus admin endpoints live on `public_router`.
- Admin auth: any endpoint under `/admin/...` and `/admin/licenses/...` is guarded by `_require_admin`, which compares the `X-Admin-Secret` header to `settings.license_admin_secret` (`app/api.py:36-39`). When the env var is unset, admin routes are effectively unprotected — call sites all use the same dependency, but the check short-circuits on `configured is None`.
- License issuance and revocation: `POST/GET/POST /admin/licenses[...]` endpoints in `app/api.py:120-161`, backed by `app/license_repository.py`. Keys are generated with `secrets.choice` over a configurable alphabet (`app/license_repository.py:14-17`).

**Auth Provider (outbound, to iRacing):**
- Custom OAuth 2.0 client described above in `app/iracing_client.py:72-147`. No third-party identity broker; credentials are read directly from `.env`.

**No SSO, OIDC, JWT signing, or session middleware** is configured.

## Monitoring & Observability

**Error Tracking:**
- None. No Sentry, Datadog, Honeycomb, OpenTelemetry, Rollbar, or Bugsnag SDKs are imported anywhere in the repo.

**Logs:**
- Python stdlib `logging`, configured once in `app/main.py:21-29` with two handlers: a `StreamHandler(sys.stdout)` and a `FileHandler(settings.log_file)` (default `drivers-scout.log`). The log file's parent directory is auto-created (`app/main.py:17-19`).
- Level controlled by `LOG_LEVEL` (default `INFO`). Uvicorn is started with `log_level=settings.log_level.lower()` (`app/main.py:72`).
- Domain code uses module-level loggers (`logger = logging.getLogger(__name__)`) and emits structured key/value style messages for fetch timings, rate-limit waits, scheduler runs, license validation outcomes, and Discord deliveries (e.g. `app/iracing_client.py:54-55`, `app/scheduler.py:75-101`, `app/auth.py:54-69`).
- No structured/JSON log formatter; logs are plain text.

**Health checks:**
- `GET /health` returning `{"status": "ok"}` (`app/api.py:67-69`). It is in the auth-exempt set (`app/auth.py:16`).

## CI/CD & Deployment

**Hosting:**
- Not declared in-repo. Indirect evidence from the static dashboard (`assets/index.js:132,202` and `assets/subscription.js:53,93,145`) shows the API is expected to be reachable at `/drivers-scout/api/...`, suggesting deployment behind a reverse proxy (e.g. Nginx/Caddy/Traefik) that strips the `/drivers-scout/api` prefix before forwarding to FastAPI.

**CI Pipeline:**
- None. There are no `.github/workflows`, `.gitlab-ci.yml`, `.circleci/`, `Jenkinsfile`, `azure-pipelines.yml`, or similar files committed.

**Container/IaC:**
- No Dockerfile, docker-compose, Helm chart, Terraform, Pulumi, CloudFormation, or systemd unit files in the repository.

## Environment Configuration

**Required env vars (no default, must be supplied):**
- `IRACING_USERNAME` (`app/settings.py:27`)
- `IRACING_PASSWORD` (`app/settings.py:28`)
- `IRACING_CLIENT_SECRET` (`app/settings.py:30`)

**Strongly recommended for production:**
- `LICENSE_ADMIN_SECRET` — without it, license enforcement is fully bypassed for all non-admin routes (`app/auth.py:84-95`) and admin endpoints accept any caller (`app/api.py:36-39`).

**Other consumed env vars** (defaults shown earlier in STACK.md): `APP_TIMEZONE`, `SCHEDULER_ENABLED`, `SNAPSHOTS_DIR`, `IRACING_CLIENT_ID`, `IRACING_SCOPE`, `IRACING_RATE_LIMIT_RPM`, `HTTP_TIMEOUT_SECONDS`, `RATE_LIMIT_BURST`, `CATEGORIES`, `DATABASE_URL`, `LICENSE_KEY_LENGTH`, `LICENSE_KEY_ALPHABET`, `LOG_LEVEL`, `LOG_FILE`, `HOST`, `PORT`.

**Secrets location:**
- A local `.env` file at the repo root, loaded by pydantic-settings (`app/settings.py:56-62`). `.env` is git-ignored (`.gitignore:69-71`). `.env.example` exists as a committed template — its presence is noted; contents are not reproduced.
- No secret manager (AWS Secrets Manager, GCP Secret Manager, Vault, Doppler, 1Password) is wired up.

## Webhooks & Callbacks

**Incoming webhooks:**
- None. No third-party webhook receivers (Stripe, GitHub, Twilio, etc.) are present. Every route in `openapi.yaml` and `app/api.py` is a service-initiated REST endpoint or admin trigger.

**Outgoing webhooks:**
- Discord webhooks to user-supplied URLs stored in `Subscription.webhook_url` (`app/models.py:42-55`). Triggered on the weekly cron (`app/scheduler.py:292-305`) and the admin endpoint `POST /admin/discord-subscriptions/run` (`app/api.py:102-117`). Payload schema documented above under "APIs & External Services / Discord".

## HTTP Contract Summary

The committed OpenAPI 3.0.3 spec at `openapi.yaml` documents all public routes and aligns with `app/api.py`:

- Public (no auth): `GET /health`, `GET /licenses/{license_key}/status`.
- Admin (header `X-Admin-Secret`): `POST /admin/run-fetch`, `POST /admin/sync-members`, `POST /admin/discord-subscriptions/run`, `POST/GET /admin/licenses`, `POST /admin/licenses/{key}/revoke`, `POST /admin/licenses/{key}/activate`.
- License-protected (header `X-License-Key` or `Authorization: Bearer ...`): `GET/POST /subscriptions`, `DELETE /subscriptions/{id}`, `GET /members/search`, `GET /members/{cust_id}/latest`, `GET /members/latest`, `GET /members/{cust_id}/delta`, `GET /leaders/growers`.

---

*Integration audit: 2026-05-27*
