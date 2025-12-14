# Drivers Scout

A small FastAPI service that archives iRacing member statistics daily and exposes historical deltas.

## Features
- Daily scheduled fetch at configurable time and timezone.
- OAuth password_limited login with refresh handling.
- CSV snapshots stored on disk by date instead of a database.
- REST endpoints for latest snapshot, history, deltas, and top growers.
- One-off fetch CLI.

## Setup
1. Create and activate a virtual environment.
2. Install dependencies: `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` and fill in credentials.
4. Run the server with `python -m app`.

## Configuration
Environment variables (via `.env`):
- `APP_TIMEZONE` (default `Europe/Zurich`)
- `SCHEDULE_HOUR`, `SCHEDULE_MINUTE` (daily run time)
- `SCHEDULER_ENABLED` (set `false` to disable during local dev)
- `SNAPSHOTS_DIR` root directory for downloaded CSVs
- `IRACING_USERNAME`, `IRACING_PASSWORD` (opaque string), `IRACING_CLIENT_SECRET`
- `IRACING_CLIENT_ID` (default `ar-pwlimited`), `IRACING_SCOPE` (default `iracing.auth`)
- `IRACING_RATE_LIMIT_RPM`, `RATE_LIMIT_BURST`
- `CATEGORIES` comma-separated categories (default `sports_car`)

All members returned by the iRacing category CSV are written to dated CSV files; no manual filtering is required.

Tokens are cached in memory. Refresh is attempted when expiry nears or on 401 responses; if refresh fails, a new login is issued. The token endpoint currently accepts `grant_type=refresh_token` with the `refresh_token` plus client credentials.

## Running
```bash
python -m app
```

One-off fetch without running the server:
```bash
python -m app.fetch_once
```

## Example API usage
Assuming server on http://localhost:8000

```bash
curl http://localhost:8000/health
curl -X POST http://localhost:8000/admin/run-fetch
curl "http://localhost:8000/members/419877/latest?category=sports_car"
curl "http://localhost:8000/members/419877/history?category=sports_car&start=2024-01-01&end=2024-02-01"
curl "http://localhost:8000/members/419877/delta?category=sports_car&days=30"
curl "http://localhost:8000/leaders/growers?category=sports_car&days=30&limit=10"
```

## Notes
- Snapshots are written as `{YYYY-MM-DD}.csv` files beneath `SNAPSHOTS_DIR/<category>`.
- Scheduler uses APScheduler with the configured timezone.
- Logging avoids sensitive credential data.
