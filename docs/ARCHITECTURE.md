# Architecture

Orca has four main layers: data collection, storage, backend API, and frontend dashboard.

## Data Flow

```text
Polymarket Gamma/Data APIs
  -> data_platform/jobs
  -> data_platform/ingest
  -> PostgreSQL raw + analytics schemas
  -> data_platform/services
  -> data_platform/api/server.py
  -> my-app React dashboard
```

## Data Collection

Polymarket jobs live in `data_platform/jobs/`.

Main jobs:

- `polymarket_market_trader_crawl.py`: refreshes events/markets and public trade flow.
- `polymarket_trades_ingest.py`: ingests recent trade rows.
- `polymarket_orderbook_snapshot.py`: captures market depth and live odds snapshots.
- `polymarket_resolved_trades_backfill.py`: collects closed-market history for validation.
- `run_live_ingest.py`: near-live loop for VM ingest.
- `run_analytics_refresh.py`: rebuilds whale scores, snapshots, summaries, and derived dashboard data.

## Storage

PostgreSQL stores two broad groups of data:

- `raw`: API payloads and source snapshots for audit/debugging.
- `analytics`: normalized events, markets, trades, users, whale scores, snapshots, and dashboard-ready tables.

Migrations live in `data_platform/migrations/versions/` and are applied with Alembic.

## Backend API

The FastAPI app is `data_platform/api/server.py`.

Backend services live in `data_platform/services/`:

- `read_api.py`: read payload assembly for dashboard endpoints.
- `dashboard_builder.py`: derived dashboard snapshot construction.
- `home_summary_snapshot.py`: homepage summary aggregation.
- `research_analytics_snapshot.py`: research and category summaries.
- `whale_scoring.py`: whale score and trusted whale logic.
- `market_scope.py`: focused category matching.
- `account_auth.py`: account, session, verification, and auth security helpers.

## Frontend

The React app lives in `my-app/`.

Important frontend areas:

- `src/pages/`: route-level pages.
- `src/homepage/`: homepage cards, charts, and leaderboards.
- `src/profile/`: trader profile visualizations.
- `src/lib/api.ts`: API client and response types.
- `src/auth/`: login/session context and protected route handling.

## ML System

ML jobs use closed-market history and whale activity to generate 12h/24h forecast snapshots. The Market Profile page displays these snapshots as a whale-driven market probability forecast, plus validation and confidence wording for users.

See [ML_SYSTEM.md](ML_SYSTEM.md) for details.
