# Jobs

This directory holds runnable automation entrypoints for the data platform.

## Main Orchestration Runner

Primary runner:
- `run_ingest_cycle.py`

Example:

```bash
.venv/bin/python data_platform/jobs/run_ingest_cycle.py \
  --polymarket-wallet 0x92a54267b56800430b2be9af0f768d18134f9631 \
  --discovery-limit 5 \
  --polymarket-trades-limit 100 \
  --orderbook-market-limit 5 \
  --kalshi-environment prod \
  --kalshi-trades-limit 10 \
  --kalshi-orderbook-market-limit 5 \
  --enable-dune \
  --dune-query-id 2103719
```

Broad public market/trader crawl without wallet-specific positions:

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

Loop every hour during a daily active window with jitter:

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

What it does:
1. bootstraps the database schema
2. runs Polymarket discovery
3. optionally runs the broad Polymarket market/trader crawl
4. runs Polymarket trades
5. runs Polymarket order-book snapshots for top tracked markets
6. runs Polymarket positions for each configured wallet
7. runs Kalshi trades
8. runs Kalshi order-book snapshots for top tracked markets
9. runs the optional Dune query ingest
10. builds the preliminary whale score snapshot
11. builds the derived dashboard snapshot

The runner writes JSONL archives to `data_platform/runtime/` so normal pipeline runs do not modify tracked sample files in the repository.
It also writes one compact cycle summary per run to `data_platform/runtime/ingest_cycle_runs.jsonl` by default.

Useful runner flags:
- `--loop` to keep crawling until interrupted
- `--max-cycles` to stop after a fixed number of cycles when looping
- `--interval-hours`, `--interval-minutes`, or `--interval-seconds` (use one; default is 15 minutes)
- `--window-start`, `--window-end` for a daily HH:MM crawl window
- `--timezone` for window checks
- `--jitter-seconds` to avoid perfectly fixed cadence
- `--summary-log-file` to change or disable the JSONL cycle log
- `--enable-polymarket-public-crawl` to populate traders from public trade flow
- `--public-crawl-market-limit`, `--public-crawl-closed-market-limit`, `--public-crawl-global-pages`, `--public-crawl-max-pages-per-market`
- `--public-crawl-closed-within-hours` or `--public-crawl-closed-within-days` to bound recent closed-market crawl
- `--public-crawl-max-total-trade-pages` to cap one crawl cycle regardless of how many markets qualify

## Service-Split Jobs

Near-live operation is now split into dedicated jobs:

### Fast ingest loop

```bash
.venv/bin/python data_platform/jobs/run_live_ingest.py
```

Defaults:
- 2-minute cadence
- Polymarket discovery every 5 cycles
- Polymarket public crawl every cycle
- Polymarket positions every 5 cycles when wallets are configured
- Kalshi trades/orderbooks every cycle
- no whale/dashboard rebuild in this loop

One-shot validation:

```bash
.venv/bin/python data_platform/jobs/run_live_ingest.py --window-start 00:00 --window-end 23:59 --max-cycles 1
```

### Analytics refresh loop

```bash
.venv/bin/python data_platform/jobs/run_analytics_refresh.py
```

One-shot validation:

```bash
.venv/bin/python data_platform/jobs/run_analytics_refresh.py --max-cycles 1
```

### Nightly maintenance

```bash
.venv/bin/python data_platform/jobs/run_retention_maintenance.py --skip-snapshot
```

What it does:
1. create current and next-month partitions
2. backfill partition-shadow tables
3. roll up old orderbook and position snapshots
4. optionally write a full snapshot backup artifact

One-shot shadow backfill:

```bash
.venv/bin/python data_platform/jobs/backfill_partition_shadows.py --batch-size 2000
```

VM wrapper scripts:
- `scripts/run_ingest_live_vm.sh`
- `scripts/run_analytics_refresh_vm.sh`
- `scripts/run_retention_rollup_vm.sh`

Example `systemd` units:
- `deploy/systemd/orca-ingest-live.service`
- `deploy/systemd/orca-analytics-refresh.service`
- `deploy/systemd/orca-retention-rollup.service`
- `deploy/systemd/orca-backup-snapshot.timer`

You can also set wallets with:

```bash
export POLYMARKET_WALLETS="0xwallet1,0xwallet2"
```

The Dune step requires a repo-level `.env` file with:

```env
DUNE_API_KEY=your_dune_api_key
DUNE_QUERY_ID=2103719
```

Then enable it with:

```bash
--enable-dune
```

## Standalone Jobs

Run only the Dune step:

```bash
.venv/bin/python data_platform/jobs/dune_query_ingest.py --query-id 2103719
```

Run only the Polymarket order-book step:

```bash
.venv/bin/python data_platform/jobs/polymarket_orderbook_snapshot.py --market-limit 5 --max-requests 1
```

Run only the Polymarket trades step:

```bash
.venv/bin/python data_platform/jobs/polymarket_trades_ingest.py --limit 200 --max-requests 1
```

Run the broad Polymarket market/trader crawl:

```bash
.venv/bin/python data_platform/jobs/polymarket_market_trader_crawl.py \
  --fetch-full-details \
  --market-limit 25 \
  --closed-market-limit 10 \
  --closed-within-days 7 \
  --global-pages 2 \
  --trade-limit 200 \
  --max-pages-per-market 3 \
  --max-total-trade-pages 20
```

Backfill trades for deterministically resolved closed Polymarket markets:

```bash
.venv/bin/python data_platform/jobs/polymarket_resolved_trades_backfill.py \
  --market-limit 5 \
  --trade-limit 200 \
  --max-pages-per-market 5
```

Backfill only deterministically resolved conditions with no ingested trades yet:

```bash
.venv/bin/python data_platform/jobs/polymarket_resolved_trades_backfill.py \
  --only-uncovered \
  --market-limit 10 \
  --trade-limit 100 \
  --max-pages-per-market 3
```

Run batched resolved-market backfill until a budget is hit:

```bash
.venv/bin/python data_platform/jobs/polymarket_resolved_trades_backfill.py \
  --only-uncovered \
  --market-limit 5 \
  --trade-limit 200 \
  --max-pages-per-market 5 \
  --batch-count 4 \
  --target-written-trades 5000
```

Run only the Kalshi order-book step:

```bash
.venv/bin/python data_platform/jobs/kalshi_orderbook_snapshot.py --environment prod --market-limit 5 --max-requests 1
```

Build only the whale score snapshot:

```bash
.venv/bin/python data_platform/jobs/build_whale_scores.py
```

Export the first ML dataset from resolved Polymarket user/market history:

```bash
.venv/bin/python data_platform/jobs/export_ml_dataset.py
```

Train the baseline profitability model on that dataset:

```bash
.venv/bin/python data_platform/jobs/train_ml_baseline.py
```

Export the market-level snapshot dataset for outcome prediction:

```bash
.venv/bin/python data_platform/jobs/export_market_ml_dataset.py
```

Train the canonical grouped market model with LightGBM plus rolling diagnostics:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task outcome --evaluation-mode rolling
```

Train only the trade-covered regime as a separate model slice:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task outcome --evaluation-mode rolling --regime trade_covered
```

Train the cold-start regime with its metadata-and-prior feature path:

```bash
.venv/bin/python data_platform/jobs/train_market_model.py --task outcome --evaluation-mode rolling --regime cold_start
```

Train the grouped time-aware market outcome baseline:

```bash
.venv/bin/python data_platform/jobs/train_market_ml_baseline.py
```

Train the grouped time-aware LightGBM market outcome model:

```bash
.venv/bin/python data_platform/jobs/train_market_lightgbm.py
```

Compare price-only and price-plus-whale market models:

```bash
.venv/bin/python data_platform/jobs/compare_market_feature_sets.py
```

Compare LightGBM price-only and price-plus-whale market models:

```bash
.venv/bin/python data_platform/jobs/compare_market_feature_sets_lightgbm.py
```

Compare Random Forest and LightGBM on the same grouped market split:

```bash
.venv/bin/python data_platform/jobs/compare_market_model_families.py
```

Analyze residual whale signal beyond price:

```bash
.venv/bin/python data_platform/jobs/analyze_market_whale_signal.py
```

Analyze whale lift only where pre-cutoff trades exist:

```bash
.venv/bin/python data_platform/jobs/analyze_market_whale_signal.py --regime trade_covered
```

Note:
- the market export behind this comparison now uses historical-as-of-cutoff whale features (`ml_market_snapshot_v3`)
- this path is heavier than the earlier prototype, but the current incremental export path is fast enough for normal iteration
- LightGBM is the primary model family; Random Forest remains benchmark-only for transition safety
- whale lift is now gated on the `trade_covered` regime rather than the mixed all-row export

Load a shared snapshot into Docker PostgreSQL:

```bash
./data_platform/open_psql.sh < path/to/shared_data_snapshot.sql
```

For full collaborator onboarding, use:

```bash
./scripts/bootstrap.sh
```
