# orca_polymarket

## Data Platform

The shared data/database code now lives under `data_platform/`:

- `data_platform/api/` for the internal FastAPI server
- `data_platform/db/` for engine/session/bootstrap code
- `data_platform/models/` for ORM schema definitions
- `data_platform/ingest/` for DB write helpers
- `data_platform/services/` for read/query and dashboard builders

Database setup, operation, reset, and maintenance instructions are documented in:

- [`data_platform/README.md`](data_platform/README.md)

From the repository root, set the application and `psql` connection strings first:

```bash
export DATABASE_URL="postgresql+psycopg://postgres:postgres@localhost:5432/whaling"
export PSQL_URL="postgresql://postgres:postgres@localhost:5432/whaling"
```

Docker option (recommended for collaborators):

```bash
docker compose -f app/compose.yaml up -d db
export DATABASE_URL="postgresql+psycopg://app:password@localhost:5433/app_db"
export PSQL_URL="postgresql://app:password@localhost:5433/app_db"
```

One-command collaborator DB setup (cross-platform: macOS/Linux/Windows):

```bash
.venv/bin/python scripts/setup_collab_db.py --snapshot path/to/shared_data_snapshot.sql
```

If you prefer manual import:

```bash
./data_platform/open_psql.sh < path/to/shared_data_snapshot.sql
```

Windows PowerShell equivalent:

```powershell
.venv\Scripts\python.exe scripts\setup_collab_db.py --snapshot path\to\shared_data_snapshot.sql
```

Windows `psql` access without local PostgreSQL CLI:

```powershell
docker exec -it orcaDB psql -U app -d app_db
```

Or open the database directly with:

```bash
./data_platform/open_psql.sh
```

Optional shell shortcut from anywhere inside this repo:

```bash
whalingdb() {
  local repo_root
  repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || return 1
  "$repo_root/data_platform/open_psql.sh" "$@"
}
```

To keep that function across shell sessions, add it to `~/.zshrc` and run `source ~/.zshrc`.

Create the schemas and tables:

```bash
.venv/bin/alembic -c alembic.ini upgrade head
```

Compatibility option:

```bash
.venv/bin/python bootstrap_db.py
```

Run the internal read-only API:

```bash
.venv/bin/python -m uvicorn main:app --reload
```

Build a derived dashboard snapshot from the normalized tables:

```bash
.venv/bin/python build_dashboard_snapshot.py
```

Run one automated ingest cycle:

```bash
.venv/bin/python data_platform/jobs/run_ingest_cycle.py \
  --polymarket-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --polymarket-trades-limit 200 \
  --enable-dune
```

When `--enable-dune` is used, the runner reads `DUNE_API_KEY` and `DUNE_QUERY_ID` from the repo-level `.env` file.

Inspect the database schema quickly:

```bash
psql "$PSQL_URL" -c "\dt analytics.*"
psql "$PSQL_URL" -c "\d analytics.transaction_fact"
```

Read live data directly from PostgreSQL:

```bash
psql "$PSQL_URL" -c "
SELECT transaction_id, source_transaction_id, side, price, shares, notional_value, transaction_time
FROM analytics.transaction_fact
ORDER BY transaction_id DESC
LIMIT 10;
"
```

Run the checked-in deliverable query pack:

```bash
./data_platform/open_psql.sh -f data_platform/sql/deliverable_queries.sql
```

Run the lightweight smoke validator:

```bash
.venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data
```

To include a live dashboard rebuild in the validation:

```bash
.venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data --build-dashboard
```

Run data-quality checks:

```bash
.venv/bin/python data_platform/tests/data_quality_check.py --require-data
```

Run Week 4/5 readiness gate (strict, non-Dune):

```bash
.venv/bin/python data_platform/tests/week45_readiness_check.py --require-data
```

Include Dune in strict readiness when needed:

```bash
.venv/bin/python data_platform/tests/week45_readiness_check.py --require-data --require-dune
```

Run repository secret scan:

```bash
.venv/bin/python scripts/secret_scan.py
```

Snapshot release process:

- [`SNAPSHOT_RELEASE.md`](SNAPSHOT_RELEASE.md)

## Polymarket Positions Scraper

`polymarket_positions_scraper.py` fetches positions for a wallet and appends JSONL records.

### Run every hour (default)

```bash
.venv/bin/python polymarket_positions_scraper.py \
  --user-wallet 0x92a54267b56800430b2be9af0f768d18134f9631
```

### Customize interval

Every 30 minutes:

```bash
.venv/bin/python polymarket_positions_scraper.py \
  --user-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --interval-minutes 30
```

Every 10 seconds (quick test):

```bash
.venv/bin/python polymarket_positions_scraper.py \
  --user-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --interval-seconds 10
```

### Run only during a daily time window

This runs every 10 minutes, but only between 9:00 AM and 5:00 PM New York time:

```bash
.venv/bin/python polymarket_positions_scraper.py \
  --user-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --interval-minutes 10 \
  --window-start 09:00 \
  --window-end 17:00 \
  --timezone America/New_York
```

### Useful flags

- `--interval-hours`, `--interval-minutes`, or `--interval-seconds` (use one; default is hourly)
- `--window-start`, `--window-end` (daily HH:MM window)
- `--timezone`
- `--output-file` (default: `polymarket_data/current_positions.jsonl`)
- `--max-requests` (set finite runs for testing, e.g. `--max-requests 3`)
- `--jitter-seconds` (random delay to avoid strict fixed cadence)

## DB-Backed Ingestion

Polymarket discovery can now also write into PostgreSQL:

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --fetch-full-details \
  --write-to-db \
  --max-requests 1
```

Polymarket positions can now also write into PostgreSQL:

```bash
.venv/bin/python polymarket_positions_scraper.py \
  --user-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --write-to-db \
  --max-requests 1
```

Polymarket trades can now also write into PostgreSQL:

```bash
.venv/bin/python data_platform/jobs/polymarket_trades_ingest.py \
  --limit 200 \
  --max-requests 1
```

Kalshi can now also write into PostgreSQL:

```bash
cd kalshi-scraper
.venv/bin/python main.py \
  --environment prod \
  --endpoint trades \
  --write-to-db \
  --max-requests 1
```

Dune query results can now also write into PostgreSQL:

```bash
.venv/bin/python data_platform/jobs/dune_query_ingest.py \
  --query-id 2103719 \
  --max-requests 1
```

The Dune job loads `DUNE_API_KEY` and `DUNE_QUERY_ID` from the repo-level `.env` file automatically.
