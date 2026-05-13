# Jobs

`data_platform/jobs/` contains active CLI entrypoints for Polymarket ingest, analytics refresh, data retention, and ML report generation.

## Ingest

Refresh active/recent markets and public trader flow:

```bash
python data_platform/jobs/polymarket_market_trader_crawl.py \
  --market-limit 25 \
  --closed-market-limit 10 \
  --closed-within-days 7
```

Ingest recent trades:

```bash
python data_platform/jobs/polymarket_trades_ingest.py --limit 200 --max-requests 1
```

Capture orderbook snapshots:

```bash
python data_platform/jobs/polymarket_orderbook_snapshot.py --market-limit 25 --max-requests 1
```

Run the combined ingest cycle:

```bash
python data_platform/jobs/run_ingest_cycle.py --enable-polymarket-public-crawl
```

Run the near-live loop:

```bash
python data_platform/jobs/run_live_ingest.py
```

## Analytics

```bash
python data_platform/jobs/run_analytics_refresh.py
python data_platform/jobs/build_whale_scores.py
python build_dashboard_snapshot.py
```

Root dashboard snapshot wrappers are kept for deployment compatibility. New job code should live under `data_platform/jobs/`.

## ML

Common report jobs:

```bash
python data_platform/jobs/evaluate_ml_category_validation.py
python data_platform/jobs/evaluate_ml_trend_direction_classifier.py
python data_platform/jobs/evaluate_ml_whale_anchored_delta.py
python data_platform/jobs/generate_ml_market_prediction_snapshots.py
```

Generated ML report JSON/Markdown should normally stay outside commits unless the file is intentionally part of handoff documentation.

## Retention

```bash
python data_platform/jobs/run_retention_maintenance.py --dry-run
python data_platform/jobs/run_retention_maintenance.py --apply
```

Review dry-run output before applying destructive cleanup.
