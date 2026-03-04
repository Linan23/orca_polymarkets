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

Create the schemas and tables:

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

Kalshi can now also write into PostgreSQL:

```bash
cd kalshi-scraper
.venv/bin/python main.py \
  --environment prod \
  --endpoint trades \
  --write-to-db \
  --max-requests 1
```
