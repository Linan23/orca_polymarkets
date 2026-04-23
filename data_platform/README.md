# Database Operations Guide

## Purpose

This document explains how to run, inspect, reset, and maintain the local PostgreSQL database used by the Whaling data platform.

It covers:
- the `data_platform/` layout
- local PostgreSQL setup
- connection settings
- schema bootstrap and migrations
- ingestion into the database
- dashboard snapshot generation
- ML starter export/training
- routine maintenance
- recovery/reset workflows

This project currently uses:
- shared data-platform code under `data_platform/`
- PostgreSQL 16 in Docker as the default local database
- SQLAlchemy ORM models in [`models/`](models/)
- Alembic migrations via [`../alembic.ini`](../alembic.ini)
- optional compatibility bootstrap via [`../bootstrap_db.py`](../bootstrap_db.py)

Important:
- Alembic is now the preferred way to create and evolve the schema.
- `bootstrap_db.py` remains available as a compatibility helper for quick local bootstraps.
- Migration instructions live in [`migrations/README.md`](migrations/README.md).

## Historical-Preserving Lifecycle

The database no longer assumes periodic delete/reload behavior.

Current design:

- canonical current-state tables keep the latest mutable entity rows
- history tables capture material changes for mutable entities
- append-only domains are mirrored into partition-shadow tables
- compatibility views provide a safe read layer while legacy tables are still present
- older high-frequency snapshots are rolled into aggregate tables

Primary lifecycle objects:

- history tables:
  - `analytics.user_account_history`
  - `analytics.market_event_history`
  - `analytics.market_contract_history`
  - `analytics.market_tag_map_history`
- rollup tables:
  - `analytics.orderbook_snapshot_hourly`
  - `analytics.orderbook_snapshot_daily`
  - `analytics.position_snapshot_daily`
- compatibility views:
  - `analytics.scrape_run_all`
  - `raw.api_payload_all`
  - `analytics.transaction_fact_all`
  - `analytics.orderbook_snapshot_all`
  - `analytics.position_snapshot_all`
  - `analytics.whale_score_snapshot_all`

Validate rollout state with:

```bash
.venv/bin/python data_platform/tests/history_partition_check.py
```

## Current Local Defaults

The default local development database is the Docker PostgreSQL service:

- host: `localhost`
- port: `5433`
- database: `app_db`
- username: `app`
- password: `password`

Start it:

```bash
docker compose -f app/compose.yaml up -d db
```

Application connection string:

```bash
export DATABASE_URL="postgresql+psycopg://app:password@localhost:5433/app_db"
```

`psql` connection string:

```bash
export PSQL_URL="postgresql://app:password@localhost:5433/app_db"
```

Quick open helper:

```bash
./data_platform/open_psql.sh
```

`open_psql.sh` defaults to Docker and falls back to `docker exec` if local `psql` is unavailable.

## Legacy Homebrew Profile

The Homebrew/PostgreSQL profile is still supported when needed:

- host: `localhost`
- port: `5432`
- database: `whaling`
- username: `postgres`
- password: `postgres`

Application connection string:

```bash
export DATABASE_URL="postgresql+psycopg://postgres:postgres@localhost:5432/whaling"
```

`psql` connection string:

```bash
export PSQL_URL="postgresql://postgres:postgres@localhost:5432/whaling"
```

Important:
- `DATABASE_URL` is for the Python application (`SQLAlchemy` + `psycopg`)
- `PSQL_URL` is for the `psql` CLI
- `psql` does not understand the `postgresql+psycopg://...` SQLAlchemy URL format

This is acceptable for local development only.
Change these credentials before using any shared or remote environment.

## Collaborator Docker Setup

Preferred macOS and Ubuntu onboarding path:

```bash
./scripts/bootstrap.sh
```

Optional variants:

```bash
./scripts/bootstrap.sh --snapshot path/to/shared_data_snapshot.sql
./scripts/bootstrap.sh --empty-db
./scripts/bootstrap.sh --reset-db
```

`bootstrap.sh` validates machine prerequisites, creates `.venv`, installs Python and frontend dependencies, copies `.env.example` files when needed, starts Docker PostgreSQL, applies migrations, imports a snapshot only when the DB is empty, runs verification, and tracks the active dependency/snapshot hashes in a local bootstrap state file so reruns can stay current.

If no bundled snapshot is present, use `--snapshot PATH` or `--empty-db`.

Common collaborator commands from the repo root:

```bash
# Full setup
./scripts/bootstrap.sh

# Setup variants
./scripts/bootstrap.sh --empty-db
./scripts/bootstrap.sh --snapshot path/to/shared_data_snapshot.sql
./scripts/bootstrap.sh --reset-db
./scripts/bootstrap.sh --help

# DB-only helper
.venv/bin/python scripts/setup_collab_db.py
.venv/bin/python scripts/setup_collab_db.py --help

# Start local services after setup
.venv/bin/python -m uvicorn data_platform.api.server:app --reload --host 127.0.0.1 --port 8000
npm --prefix my-app run dev
```

If a maintainer shared a database snapshot and you only need the DB import path, you can still load it manually with:

```bash
./data_platform/open_psql.sh < path/to/shared_data_snapshot.sql
```

Or use the DB-only collaborator helper directly:

```bash
.venv/bin/python scripts/setup_collab_db.py --snapshot path/to/shared_data_snapshot.sql
```

macOS or Ubuntu/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
.venv/bin/python scripts/setup_collab_db.py --snapshot path/to/shared_data_snapshot.sql
```

Windows PowerShell:

```powershell
.venv\Scripts\python.exe scripts\setup_collab_db.py --snapshot path\to\shared_data_snapshot.sql
```

Direct Docker `psql` access without installing host PostgreSQL CLI:

```bash
docker exec -it orcaDB psql -U app -d app_db
```

Windows PowerShell:

```powershell
docker exec -it orcaDB psql -U app -d app_db
```

## PostgreSQL Service Management

### Start PostgreSQL

```bash
brew services start postgresql@16
```

### Stop PostgreSQL

```bash
brew services stop postgresql@16
```

### Restart PostgreSQL

```bash
brew services restart postgresql@16
```

### Check service status

```bash
brew services list | rg postgresql
```

### Check whether the server is accepting connections

```bash
pg_isready -h localhost -p 5432
```

Expected healthy output:

```text
localhost:5432 - accepting connections
```

## First-Time Setup

If PostgreSQL is not installed yet:

```bash
brew install postgresql@16
```

The project expects the following local role and database:

- role: `postgres`
- database: `whaling`

If you need to recreate them:

```bash
psql -d postgres <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN
        CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres';
    ELSE
        ALTER ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres';
    END IF;
END
$$;
SQL
```

```bash
if ! psql -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname='whaling'" | grep -q 1; then
  createdb -O postgres whaling
fi
```

## Schema Bootstrap

Once `DATABASE_URL` is set and PostgreSQL is running, create the schemas and tables:

```bash
.venv/bin/alembic -c alembic.ini upgrade head
```

Compatibility option:

```bash
.venv/bin/python bootstrap_db.py
```

`bootstrap_db.py` now applies Alembic migrations instead of relying on raw `metadata.create_all()`, because the lifecycle rollout includes partition parents and compatibility views.

## Near-Live Runtime Split

The runtime is now split into three jobs instead of one all-purpose crawler:

1. `data_platform/jobs/run_live_ingest.py`
   - fast market ingest
   - default cadence: every 2 minutes
   - Polymarket and Kalshi market/trade/orderbook collection
   - wallet positions only when wallets are configured
2. `data_platform/jobs/run_analytics_refresh.py`
   - whale score rebuild
   - dashboard rebuild
   - default cadence: every 15 minutes
3. `data_platform/jobs/run_retention_maintenance.py`
   - partition creation
   - shadow-table backfill
   - snapshot rollups
   - optional backup snapshot export

Useful local commands:

```bash
.venv/bin/python data_platform/jobs/run_live_ingest.py --window-start 00:00 --window-end 23:59 --max-cycles 1
.venv/bin/python data_platform/jobs/run_analytics_refresh.py --max-cycles 1
.venv/bin/python data_platform/jobs/run_retention_maintenance.py --skip-snapshot
```

VM wrapper scripts are included in `scripts/` and example `systemd` units live in `deploy/systemd/`.

What the baseline migration creates:

- schema `analytics`
- schema `raw`
- all ORM-backed application tables

Use Alembic for all schema evolution after the baseline.

## Verify the Database

### Test the connection

```bash
psql "$PSQL_URL" -Atqc "SELECT current_database(), current_user;"
```

Expected:

```text
app_db|app
```

### List the project tables

```bash
psql "$PSQL_URL" -Atqc \
"SELECT table_schema || '.' || table_name
 FROM information_schema.tables
 WHERE table_schema IN ('analytics','raw')
 ORDER BY 1;"
```

### View the full column-level schema

```bash
psql "$PSQL_URL" -c "
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema IN ('analytics', 'raw')
ORDER BY table_schema, table_name, ordinal_position;
"
```

### Inspect one table definition

```bash
psql "$PSQL_URL" -c "\d analytics.transaction_fact"
psql "$PSQL_URL" -c "\d analytics.market_event"
psql "$PSQL_URL" -c "\d raw.api_payload"
```

### Check row counts

```bash
psql "$PSQL_URL" -Atqc \
"SELECT 'scrape_run='||count(*) FROM analytics.scrape_run
 UNION ALL
 SELECT 'api_payload='||count(*) FROM raw.api_payload
 UNION ALL
 SELECT 'market_event='||count(*) FROM analytics.market_event
 UNION ALL
 SELECT 'market_contract='||count(*) FROM analytics.market_contract
 UNION ALL
 SELECT 'transaction_fact='||count(*) FROM analytics.transaction_fact
 UNION ALL
 SELECT 'dashboard='||count(*) FROM analytics.dashboard;"
```

## Connect to the Database Manually

### Interactive shell

```bash
psql "$PSQL_URL"
```

Or use the helper script:

```bash
./data_platform/open_psql.sh
```

Or, if you added the shell function above:

```bash
whalingdb
```

### Useful `psql` commands

Inside `psql`:

```sql
\dn
\dt analytics.*
\dt raw.*
\d analytics.transaction_fact
\d analytics.user_account
\d analytics.market_event
\d analytics.market_contract
\d analytics.dashboard
\q
```

### Useful direct data queries

Latest scrape runs:

```bash
psql "$PSQL_URL" -c "
SELECT scrape_run_id, job_name, endpoint_name, status, records_written, error_count, started_at, finished_at
FROM analytics.scrape_run
ORDER BY scrape_run_id DESC
LIMIT 10;
"
```

Latest transactions:

```bash
psql "$PSQL_URL" -c "
SELECT transaction_id, user_id, market_contract_id, source_transaction_id, transaction_type, side, price, shares, notional_value, transaction_time
FROM analytics.transaction_fact
ORDER BY transaction_id DESC
LIMIT 10;
"
```

Latest markets:

```bash
psql "$PSQL_URL" -c "
SELECT market_contract_id, external_market_ref, market_slug, question, last_trade_price, volume, is_active, is_closed, updated_at
FROM analytics.market_contract
ORDER BY market_contract_id DESC
LIMIT 10;
"
```

### Run the checked-in deliverable query pack

For repeatable demos and report snapshots, use the checked-in SQL script:

```bash
./data_platform/open_psql.sh -f data_platform/sql/deliverable_queries.sql
```

## Smoke Validation

Run the lightweight validator to confirm the local stack is healthy:

```bash
.venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data
```

What it checks:
- database connectivity
- Alembic baseline revision
- required `analytics` and `raw` tables
- current FastAPI read endpoints

Optional deeper validation:

```bash
.venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data --build-dashboard
```

Optional flags:
- `--run-bootstrap` to exercise the compatibility bootstrap helper
- `--json` for machine-readable output

Run data-quality checks:

```bash
.venv/bin/python data_platform/tests/data_quality_check.py --require-data
```

Run Week 4/5 readiness gate (strict, non-Dune):

```bash
.venv/bin/python data_platform/tests/week45_readiness_check.py --require-data
```

Optional strict Dune coverage:

```bash
.venv/bin/python data_platform/tests/week45_readiness_check.py --require-data --require-dune
```

Snapshot publishing runbook:

- [`../SNAPSHOT_RELEASE.md`](../SNAPSHOT_RELEASE.md)

Latest dashboard snapshots:

```bash
psql "$PSQL_URL" -c "
SELECT dashboard_id, dashboard_date, generated_at, timeframe, scope_label
FROM analytics.dashboard
ORDER BY dashboard_id DESC
LIMIT 10;
"
```

## Running Ingestion into PostgreSQL

## Polymarket: event discovery

This writes:
- scrape run metadata
- raw event payloads
- normalized events
- normalized markets
- normalized tags

Command:

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --fetch-full-details \
  --write-to-db \
  --max-requests 1
```

You can combine filtering:

```bash
.venv/bin/python polymarket-data/discover_events_scraper.py \
  --tag crypto \
  --fetch-full-details \
  --write-to-db \
  --max-requests 1
```

## Polymarket: trades

This writes:
- scrape run metadata
- raw trades payloads
- normalized user rows
- normalized event rows
- normalized market rows
- normalized `transaction_fact` rows

Command:

```bash
.venv/bin/python data_platform/jobs/polymarket_trades_ingest.py \
  --limit 200 \
  --max-requests 1
```

Inspect imported Polymarket transactions:

```bash
psql "$PSQL_URL" -c "
SELECT transaction_id, user_id, source_transaction_id, side, outcome_label, price, shares, notional_value, transaction_time
FROM analytics.transaction_fact
WHERE platform_id = (SELECT platform_id FROM analytics.platform WHERE platform_name = 'polymarket')
ORDER BY transaction_id DESC
LIMIT 10;
"
```

## Polymarket: positions

This writes:
- scrape run metadata
- raw positions payloads
- normalized user rows
- normalized event rows
- normalized market rows
- `position_snapshot` rows

Command:

```bash
.venv/bin/python polymarket_positions_scraper.py \
  --user-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --write-to-db \
  --max-requests 1
```

Inspect the imported position snapshots:

```bash
psql "$PSQL_URL" -c "
SELECT position_snapshot_id, user_id, market_contract_id, position_size, avg_entry_price, current_mark_price, market_value, cash_pnl, snapshot_time
FROM analytics.position_snapshot
ORDER BY position_snapshot_id DESC
LIMIT 10;
"
```

## Kalshi: trades

This writes:
- scrape run metadata
- raw trade payloads
- normalized `transaction_fact` rows
- canonical market rows for observed tickers

Command:

```bash
cd kalshi-scraper
.venv/bin/python main.py \
  --environment prod \
  --endpoint trades \
  --limit 5 \
  --write-to-db \
  --max-requests 1
```

Note:
- The current Kalshi trade endpoint does not expose trader identity.
- The system uses a placeholder canonical user row (`__unknown__`) to satisfy the `transaction_fact.user_id` foreign key.

## Dune: saved query results

This writes:
- scrape run metadata
- raw Dune result payloads
- normalized `user_account` rows for maker wallets
- synthetic `market_event` and `market_contract` rows under the `dune` platform
- normalized `transaction_fact` rows using the saved query output

The Dune ingest expects a saved query whose result rows include fields compatible with:
- `block_time`
- `block_number`
- `tx_hash`
- `maker`
- `question`
- `token_outcome_name` or `token_outcome`
- `price`
- `amount` or `amount_usdc`
- `shares`
- `maker_action` or `action`

Required:

Put your key in the repo-level `.env` file:

```env
DUNE_API_KEY=your_dune_api_key_here
DUNE_QUERY_ID=2103719
```

Command:

```bash
.venv/bin/python data_platform/jobs/dune_query_ingest.py \
  --query-id 2103719 \
  --max-requests 1
```

Inspect the imported Dune-backed transactions:

```bash
psql "$PSQL_URL" -c "
SELECT transaction_id, user_id, source_transaction_id, side, price, shares, notional_value, transaction_time
FROM analytics.transaction_fact
WHERE platform_id = (SELECT platform_id FROM analytics.platform WHERE platform_name = 'dune')
ORDER BY transaction_id DESC
LIMIT 10;
"
```

## Building the Derived Dashboard Tables

The normalized layer feeds the dashboard-facing tables.

To build the preliminary whale score snapshot:

```bash
.venv/bin/python build_whale_scores.py
```

Current profitability and resolution logic:

- closed Polymarket binary markets are treated as resolved only when `last_trade_price` is effectively `1` or `0`
- the winning outcome is inferred from the stored normalized outcome labels
- captured trade signals are also used conservatively, so a condition can count as resolved when one outcome trades at `>= 0.98` and the opposite outcome trades at `<= 0.02`
- realized profitability is computed only for users whose captured trade history in that resolved market is internally consistent enough to avoid overstating PnL from partial history
- if no resolved trade overlap exists yet, `profitability_score` stays `0`

Week 6 scoring methodology (`week6_v3`):

- scoring is computed per platform from normalized `transaction_fact` and `position_snapshot`
- trust score inputs:
  - `raw_volume_score`: percentile rank of `SUM(notional_value)`
  - `market_breadth_score`: percentile rank of distinct markets traded
  - `consistency_score`: percentile rank of active UTC trade days
  - `current_exposure_score`: percentile rank of latest marked position exposure
  - `profitability_score`: weighted percentile blend of realized PnL, realized ROI, and resolved-market win rate
- trust score formula:
  - `0.50 * raw_volume_score`
  - `0.20 * market_breadth_score`
  - `0.20 * consistency_score`
  - `0.10 * current_exposure_score`
  - `+ 0.15 * profitability_score`
  - `- 0.25` insider penalty when `is_likely_insider = true`
- whale classification:
  - minimum `10` trades
  - minimum `3` active trade days
  - minimum notional `5000`
  - insider-flagged users are excluded
  - final `is_whale` label = top `30%` of eligible users by trust score
- trusted whale classification:
  - already whale-eligible
  - minimum `15` trades
  - minimum `5` active trade days
  - minimum `2` resolved markets
  - minimum win rate `0.60`
  - positive profitability score
  - final `is_trusted_whale` label = top `5%` of eligible trusted users by trust score
- conservative profitability rules:
  - only Polymarket resolved markets are used right now
  - only `buy`/`sell` trades are considered for realized profitability
  - a market is excluded for that user when captured sells exceed captured buys for an outcome, because that indicates incomplete local history
- current limitation:
  - Polymarket whale analytics are materially useful now
  - Kalshi user-level whale analytics are still incomplete because current Kalshi ingest does not yet provide strong trader identity coverage
- practical Kalshi note:
  - the current public Kalshi `trades` payload does not expose trader ids
  - the ingest layer now captures `user_id` automatically when it appears in Kalshi payloads or authenticated order responses
  - to seed a stable authenticated Kalshi account id into `analytics.user_account`, use a custom authenticated endpoint such as `/trade-api/v2/portfolio/orders`

To backfill deterministically resolved closed-market trades into `transaction_fact`:

```bash
.venv/bin/python data_platform/jobs/polymarket_resolved_trades_backfill.py \
  --market-limit 5 \
  --trade-limit 200 \
  --max-pages-per-market 5
```

To backfill only deterministically resolved conditions that still have no ingested trades:

```bash
.venv/bin/python data_platform/jobs/polymarket_resolved_trades_backfill.py \
  --only-uncovered \
  --market-limit 10 \
  --trade-limit 100 \
  --max-pages-per-market 3
```

To build one snapshot:

```bash
.venv/bin/python build_dashboard_snapshot.py
```

This populates:

- `analytics.whale_score_snapshot`
- `analytics.dashboard`
- `analytics.dashboard_market`
- `analytics.market_profile`
- `analytics.user_profile`
- `analytics.user_leaderboard`

Recommended order:

1. build whale scores
2. build the dashboard snapshot

This can be rerun to generate additional snapshots over time.

## Running the Internal Read API

Start the API:

```bash
.venv/bin/python -m uvicorn data_platform.api.server:app --reload --host 127.0.0.1 --port 8000
```

Useful endpoints:

- `GET /health`
- `GET /api/status/ingestion`
- `GET /api/home/summary`
- `GET /api/analytics/top-profitable-users`
- `GET /api/analytics/market-whale-concentration`
- `GET /api/markets`
- `GET /api/users`
- `GET /api/transactions`
- `GET /api/positions`
- `GET /api/whales/latest`
- `GET /api/leaderboards/trusted/latest`
- `GET /api/users/{user_id}/whale-profile`
- `GET /api/leaderboards/latest`
- `GET /api/dashboards/latest`

Quick test:

```bash
curl -s http://127.0.0.1:8000/health
```

## Common Maintenance Tasks

## 1. Apply migrations after model changes

If you change the ORM models:

1. generate a migration:
2. review the generated revision
3. run `alembic upgrade head`

Example:

```bash
.venv/bin/alembic -c alembic.ini revision --autogenerate -m "describe_change"
.venv/bin/alembic -c alembic.ini upgrade head
```

## 2. Reset the local database

This deletes all collected data and recreates a clean local database.

```bash
dropdb --if-exists whaling
createdb -O postgres whaling
.venv/bin/alembic -c alembic.ini upgrade head
```

Use this only when you intentionally want a full local reset.

## 3. Back up the database

Create a dump:

```bash
pg_dump "$PSQL_URL" > whaling_backup.sql
```

## 4. Restore the database

```bash
psql "$PSQL_URL" < whaling_backup.sql
```

## 5. Run basic maintenance

For a local development database:

```bash
psql "$PSQL_URL" -c "VACUUM ANALYZE;"
```

Use this after a large amount of inserts, deletes, or a reset/reload cycle.

## 6. Change the local dev password

If you want to rotate the password:

```bash
psql -d postgres -c "ALTER ROLE postgres WITH PASSWORD 'new_password_here';"
```

Then update `DATABASE_URL` accordingly.

## Troubleshooting

## Problem: API `/health` returns `503`

Cause:
- PostgreSQL is not running
- `DATABASE_URL` is wrong
- credentials do not match the local role

Check:

```bash
pg_isready -h localhost -p 5432
echo "$DATABASE_URL"
echo "$PSQL_URL"
```

## Problem: scraper says `Database ingest failed`

Cause:
- DB connection failure
- schema not bootstrapped yet
- a constraint error from malformed data

Check:

1. verify PostgreSQL is running
2. verify the baseline migration was applied (`alembic upgrade head`) or the compatibility bootstrap was run
3. try a manual `psql "$PSQL_URL"` connection

## Problem: `psql` command not found

This formula is keg-only. Use the full path:

```bash
/opt/homebrew/opt/postgresql@16/bin/psql
```

Or add it to your shell:

```bash
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc
```

## Problem: tables do not reflect model changes

Cause:
- the database has not been migrated to match the current models

Current fix:
- generate and apply an Alembic migration
- or reset the local DB if it is safe to do so

## Recommended Operating Pattern for This Phase

For this phase, the stable local workflow is:

1. Start PostgreSQL
2. Export `DATABASE_URL` and `PSQL_URL`
3. Run `alembic upgrade head` when setting up a fresh DB
4. Export `DUNE_API_KEY` if the Dune step is enabled
5. Run one or more scrapers with `--write-to-db` or use `data_platform/jobs/run_ingest_cycle.py`
6. Build `analytics.whale_score_snapshot` with `build_whale_scores.py`
7. Build the dashboard snapshot with `build_dashboard_snapshot.py`

For broad Polymarket market/trader population from public flow, prefer:

```bash
.venv/bin/python data_platform/jobs/run_ingest_cycle.py \
  --enable-polymarket-public-crawl \
  --public-crawl-market-limit 25 \
  --public-crawl-closed-market-limit 10 \
  --public-crawl-closed-within-days 7 \
  --public-crawl-global-pages 2 \
  --public-crawl-max-pages-per-market 3 \
  --public-crawl-max-total-trade-pages 20 \
  --skip-positions
```

That path populates:
- `analytics.market_event`
- `analytics.market_contract`
- `analytics.transaction_fact`
- `analytics.user_account`

without needing a fixed wallet list.

## Validation Commands

Week 4/5 readiness:

```bash
.venv/bin/python data_platform/tests/week45_readiness_check.py --require-data
```

Week 6 whale analytics:

```bash
.venv/bin/python data_platform/tests/week6_whale_check.py --build --require-data
```
6. Run `build_dashboard_snapshot.py`
7. Inspect via `psql` and the FastAPI read endpoints

That gives you:
- raw payload retention
- normalized analytics tables
- derived dashboard tables
- a reproducible local research workflow
