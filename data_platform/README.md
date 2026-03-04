# Database Operations Guide

## Purpose

This document explains how to run, inspect, reset, and maintain the local PostgreSQL database used by the Whaling data platform.

It covers:
- the `data_platform/` layout
- local PostgreSQL setup
- connection settings
- schema bootstrap
- ingestion into the database
- dashboard snapshot generation
- routine maintenance
- recovery/reset workflows

This project currently uses:
- shared data-platform code under `data_platform/`
- PostgreSQL 16 (Homebrew)
- SQLAlchemy ORM models in [`models/`](models/)
- schema bootstrap via [`../bootstrap_db.py`](../bootstrap_db.py)

Important:
- Alembic is not fully initialized yet.
- For now, schema creation is done with the root wrapper `bootstrap_db.py`.
- The local `migrations/` folder is reserved for the next step when migration files are added.

## Current Local Defaults

The current local development database is configured as:

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
.venv/bin/python bootstrap_db.py
```

What this creates:

- schema `analytics`
- schema `raw`
- all ORM-backed application tables

This command is safe to rerun for table creation, but it is not a replacement for real migrations long term.

## Verify the Database

### Test the connection

```bash
psql "$PSQL_URL" -Atqc "SELECT current_database(), current_user;"
```

Expected:

```text
whaling|postgres
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

## Building the Derived Dashboard Tables

The normalized layer feeds the dashboard-facing tables.

To build one snapshot:

```bash
.venv/bin/python build_dashboard_snapshot.py
```

This populates:

- `analytics.dashboard`
- `analytics.dashboard_market`
- `analytics.market_profile`
- `analytics.user_profile`
- `analytics.user_leaderboard`

This can be rerun to generate additional snapshots over time.

## Running the Internal Read API

Start the API:

```bash
.venv/bin/python -m uvicorn main:app --reload
```

Useful endpoints:

- `GET /health`
- `GET /api/status/ingestion`
- `GET /api/markets`
- `GET /api/users`
- `GET /api/transactions`
- `GET /api/positions`
- `GET /api/leaderboards/latest`
- `GET /api/dashboards/latest`

Quick test:

```bash
curl -s http://127.0.0.1:8000/health
```

## Common Maintenance Tasks

## 1. Re-bootstrap after model changes

If you change the ORM models and are still in the pre-Alembic stage:

1. decide whether the database can be reset safely
2. if yes, drop and recreate the database
3. rerun `bootstrap_db.py`

Do not rely on `create_all()` for complex schema evolution long term.
That is what Alembic is for once migrations are initialized.

## 2. Reset the local database

This deletes all collected data and recreates a clean local database.

```bash
dropdb --if-exists whaling
createdb -O postgres whaling
.venv/bin/python bootstrap_db.py
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
2. verify `bootstrap_db.py` was run
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
- `bootstrap_db.py` does not apply complex schema changes to existing tables

Current workaround:
- reset the local DB if safe

Long-term solution:
- initialize Alembic and use real migrations

## Recommended Operating Pattern for This Phase

For this phase, the stable local workflow is:

1. Start PostgreSQL
2. Export `DATABASE_URL` and `PSQL_URL`
3. Run `bootstrap_db.py` when setting up a fresh DB
4. Run one or more scrapers with `--write-to-db`
5. Run `build_dashboard_snapshot.py`
6. Inspect via `psql` and the FastAPI read endpoints

That gives you:
- raw payload retention
- normalized analytics tables
- derived dashboard tables
- a reproducible local research workflow
