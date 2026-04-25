# orca_polymarket

## Data Platform

The shared data/database code now lives under `data_platform/`:

- `data_platform/api/` for the internal FastAPI server
- `data_platform/db/` for engine/session/bootstrap code
- `data_platform/models/` for ORM schema definitions
- `data_platform/ingest/` for DB write helpers
- `data_platform/services/` for read/query and dashboard builders
- `data_platform/ml/` for the first model-ready dataset export and baseline training code

Database setup, operation, reset, and maintenance instructions are documented in:

- [`data_platform/README.md`](data_platform/README.md)

## Database Lifecycle

The database now follows a historical-preserving lifecycle:

- current mutable entities stay in the canonical tables for fast reads
- material changes are copied into append-only history tables
- append-only operational domains are dual-written into partition-shadow tables
- compatibility views expose combined legacy-plus-shadow reads during rollout
- old high-frequency snapshots are rolled into summary tables instead of forcing delete/reload cycles

Current lifecycle objects include:

- history: `analytics.user_account_history`, `analytics.market_event_history`, `analytics.market_contract_history`, `analytics.market_tag_map_history`
- rollups: `analytics.orderbook_snapshot_hourly`, `analytics.orderbook_snapshot_daily`, `analytics.position_snapshot_daily`
- shadow tables: `analytics.scrape_run_part`, `raw.api_payload_part`, `analytics.transaction_fact_part`, `analytics.orderbook_snapshot_part`, `analytics.position_snapshot_part`, `analytics.whale_score_snapshot_part`
- compatibility views: `analytics.scrape_run_all`, `raw.api_payload_all`, `analytics.transaction_fact_all`, `analytics.orderbook_snapshot_all`, `analytics.position_snapshot_all`, `analytics.whale_score_snapshot_all`

Use this rollout model:

1. implement and validate locally
2. push code and migrations to GitHub
3. pull on the VM
4. run Alembic migrations
5. restart services

Do not use snapshot restore for ordinary VM refreshes. Snapshots remain backup/bootstrap artifacts only.

## Local Bootstrap

For macOS and Ubuntu collaborators, use the repo bootstrap as the primary setup path:

```bash
./scripts/bootstrap.sh
```

If you were given a snapshot outside the default locations:

```bash
./scripts/bootstrap.sh --snapshot path/to/shared_data_snapshot.sql
```

If you want a migrated but empty Docker database:

```bash
./scripts/bootstrap.sh --empty-db
```

What `bootstrap.sh` does:

- validates `docker`, `docker compose`, `python3.12`, `node`, and `npm`
- creates `.venv` and installs the Python dependencies used across the repo
- installs frontend dependencies in `my-app/`
- copies `.env.example` and `kalshi-scraper/.env.example` only when local `.env` files are missing
- starts Docker PostgreSQL, applies Alembic migrations, imports the bundled snapshot only when the DB is empty, and runs verification
- tracks dependency and snapshot hashes in a local bootstrap state file so reruns can skip unchanged installs and refresh the local DB when the tracked snapshot changes

If no bundled snapshot is present, pass `--snapshot PATH` or `--empty-db`.

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

From the repository root, use the Docker database as the default local database:

```bash
docker compose -f app/compose.yaml up -d db
export DATABASE_URL="postgresql+psycopg://app:password@localhost:5433/app_db"
export PSQL_URL="postgresql://app:password@localhost:5433/app_db"
```

Legacy Homebrew/PostgreSQL fallback remains available if you explicitly need it:

```bash
export DATABASE_URL="postgresql+psycopg://postgres:postgres@localhost:5432/whaling"
export PSQL_URL="postgresql://postgres:postgres@localhost:5432/whaling"
```

Lower-level DB-only setup helper (macOS, Ubuntu/Linux, Windows):

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

`open_psql.sh` now defaults to the Docker database and falls back to `docker exec` when local `psql` is not installed.

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

Validate the historical lifecycle objects:

```bash
.venv/bin/python data_platform/tests/history_partition_check.py
```

Compatibility option:

```bash
.venv/bin/python bootstrap_db.py
```

Run the internal read-only API:

```bash
.venv/bin/python -m uvicorn data_platform.api.server:app --reload --host 127.0.0.1 --port 8000
```

Run the frontend dev server:

```bash
npm --prefix my-app run dev
```

Build a derived dashboard snapshot from the normalized tables:

```bash
.venv/bin/python build_dashboard_snapshot.py
```

Build the preliminary whale score snapshot first when you want the dashboard to include raw whale rankings and market whale counts:

```bash
.venv/bin/python build_whale_scores.py
```

Run the near-live service split locally:

```bash
# fast ingest loop, 2-minute cadence
.venv/bin/python data_platform/jobs/run_live_ingest.py

# slower analytics refresh, 15-minute cadence
.venv/bin/python data_platform/jobs/run_analytics_refresh.py

# nightly rollup/backfill/backup maintenance
.venv/bin/python data_platform/jobs/run_retention_maintenance.py --skip-snapshot
```

Near-live ingest scope defaults to the focused categories used by the live VM deployment:
- `politics` / geopolitics
- `crypto`
- `technology`
- `video-games`

One-shot validation modes:

```bash
.venv/bin/python data_platform/jobs/run_live_ingest.py --window-start 00:00 --window-end 23:59 --max-cycles 1
.venv/bin/python data_platform/jobs/run_analytics_refresh.py --max-cycles 1
.venv/bin/python data_platform/jobs/run_retention_maintenance.py --skip-snapshot
```

Export the first ML-ready dataset:

```bash
.venv/bin/python data_platform/jobs/export_ml_dataset.py
```

Train the first baseline ML model:

```bash
.venv/bin/python data_platform/jobs/train_ml_baseline.py
```

Validate the ML starter:

```bash
.venv/bin/python data_platform/tests/ml_baseline_check.py --require-data
```

ML starter scope:
- target: conservative `positive_realized_pnl` on resolved Polymarket `user x market` rows
- current model: baseline random-forest classifier
- current role: establish a reproducible feature export and benchmark, not finalize the semester model choice
- interpretation caution: this is built from full resolved trade trajectories, so it is a starter benchmark, not a final forward-looking prediction model

Primary next ML dataset:

```bash
.venv/bin/python data_platform/jobs/export_market_ml_dataset.py
```

That export defines the main project-aligned ML target:
- one row = one resolved Polymarket market side snapshot at a fixed pre-close horizon
- target = whether that side eventually wins
- role = build the point-in-time market dataset needed for later time-based outcome modeling

Train the canonical grouped market model with LightGBM plus rolling diagnostics:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task outcome --evaluation-mode rolling
```

Compatibility baseline trainer:

```bash
.venv/bin/python data_platform/jobs/train_market_ml_baseline.py
```

Compatibility LightGBM trainer:

```bash
.venv/bin/python data_platform/jobs/train_market_lightgbm.py
```

Compare price-only and price-plus-whale market models:

```bash
.venv/bin/python data_platform/jobs/compare_market_feature_sets.py
```

Compare Random Forest and LightGBM on the same grouped market split:

```bash
.venv/bin/python data_platform/jobs/compare_market_model_families.py
```

Analyze residual whale signal beyond price:

```bash
.venv/bin/python data_platform/jobs/analyze_market_whale_signal.py
```

Compare LightGBM price-only vs price-plus-whale market models:

```bash
.venv/bin/python data_platform/jobs/compare_market_feature_sets_lightgbm.py
```

LightGBM note for macOS:
- install the Python package with `pip install -r requirements.txt`
- if the runtime fails with `libomp.dylib` missing, install the native dependency with `brew install libomp`

Current market ML dataset behavior:
- dataset version = `ml_market_snapshot_v3`
- whale participation features are computed from trade and resolved-market history available on or before each observation cutoff
- historical current exposure is approximated from open shares valued at average buy price
- `price_baseline` uses the cutoff-time side price with an average-price fallback
- `resolution_edge` is the residual whale-signal target used to test lift beyond price

Current scoring behavior:
- raw whale ranking uses trade size, breadth, activity, and current exposure
- profitability is added only when a Polymarket market is closed and its final outcome can be inferred conservatively from normalized market data
- trusted whales remain rare until the database contains resolved trade history, not just open-market trades
- LightGBM is the primary market model family; Random Forest remains a benchmark and rollback reference

Week 6 whale methodology (`week6_v3`):
- scoring is per platform, not cross-platform
- trust score formula:
  - `0.50 * raw_volume_score`
  - `0.20 * market_breadth_score`
  - `0.20 * consistency_score`
  - `0.10 * current_exposure_score`
  - `+ 0.15 * profitability_score`
  - `- 0.25` insider penalty when `is_likely_insider = true`
- whale eligibility:
  - minimum `10` trades
  - minimum `3` active trade days
  - minimum traded notional `5000`
  - excluded if insider-flagged
  - then keep the top `30%` of eligible users by trust score
- trusted whale eligibility:
  - already whale-eligible
  - minimum `15` trades
  - minimum `5` active trade days
  - minimum `2` resolved markets
  - minimum `60%` resolved-market win rate
  - positive profitability score
  - then keep the top `5%` of eligible trusted users by trust score
- resolved Polymarket markets are inferred conservatively:
  - closed binary market
  - one side at `>= 0.98` and the opposite side at `<= 0.02`, using normalized market data and captured trade signals
- profitability is conservative:
  - only buy/sell rows are used
  - markets are excluded when sells exceed captured buys for an outcome, which protects against overstating PnL from partial history
- current limitation:
  - user-level whale analytics are meaningful for Polymarket
  - Kalshi ingestion still lacks strong trader identity coverage, so Kalshi whale scoring should be treated as incomplete

Kalshi identity note:
- the current public Kalshi `trades` payload does not expose trader ids, so public-trade whale attribution is not reliable there
- the ingest layer now captures `user_id` when it is present in Kalshi payloads
- if you want a stable authenticated Kalshi account id in `analytics.user_account`, scrape authenticated order endpoints such as `/trade-api/v2/portfolio/orders` with `--endpoint custom --write-to-db`

Run one automated ingest cycle:

```bash
.venv/bin/python data_platform/jobs/run_ingest_cycle.py \
  --polymarket-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --polymarket-trades-limit 200 \
  --enable-dune
```

When `--enable-dune` is used, the runner reads `DUNE_API_KEY` and `DUNE_QUERY_ID` from the repo-level `.env` file.

Run a broad Polymarket market/trader crawl without wallet-specific positions:

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

Run the automated crawler on a schedule with a daily active window:

```bash
.venv/bin/python data_platform/jobs/run_ingest_cycle.py \
  --polymarket-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --loop \
  --interval-hours 1 \
  --window-start 09:00 \
  --window-end 17:00 \
  --timezone America/New_York \
  --jitter-seconds 30
```

Useful runner flags:
- `--loop`
- `--max-cycles`
- `--interval-hours`, `--interval-minutes`, or `--interval-seconds`
- `--window-start`, `--window-end`
- `--timezone`
- `--jitter-seconds`
- `--summary-log-file`
- `--enable-polymarket-public-crawl`
- `--public-crawl-market-limit`, `--public-crawl-closed-market-limit`, `--public-crawl-global-pages`, `--public-crawl-max-pages-per-market`
- `--public-crawl-closed-within-hours` or `--public-crawl-closed-within-days`
- `--public-crawl-max-total-trade-pages`

Backfill resolved Polymarket trades for deterministically resolved closed markets:

```bash
.venv/bin/python data_platform/jobs/polymarket_resolved_trades_backfill.py \
  --market-limit 5 \
  --trade-limit 200 \
  --max-pages-per-market 5
```

Backfill only deterministically resolved conditions that still have no ingested trades:

```bash
.venv/bin/python data_platform/jobs/polymarket_resolved_trades_backfill.py \
  --only-uncovered \
  --market-limit 10 \
  --trade-limit 100 \
  --max-pages-per-market 3
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

Run the Week 6 whale analytics validation:

```bash
.venv/bin/python data_platform/tests/week6_whale_check.py --build --require-data
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
