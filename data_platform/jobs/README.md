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

What it does:
1. bootstraps the database schema
2. runs Polymarket discovery
3. runs Polymarket trades
4. runs Polymarket order-book snapshots for top tracked markets
5. runs Polymarket positions for each configured wallet
6. runs Kalshi trades
7. runs Kalshi order-book snapshots for top tracked markets
8. runs the optional Dune query ingest
9. builds the preliminary whale score snapshot
10. builds the derived dashboard snapshot

The runner writes JSONL archives to `data_platform/runtime/` so normal pipeline runs do not modify tracked sample files in the repository.

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

Note:
- the market export behind this comparison now uses historical-as-of-cutoff whale features (`ml_market_snapshot_v3`)
- expect this export/comparison path to run noticeably slower than the earlier prototype

Load a shared snapshot into Docker PostgreSQL:

```bash
./data_platform/open_psql.sh < path/to/shared_data_snapshot.sql
```

For full collaborator onboarding, use:

```bash
.venv/bin/python scripts/setup_collab_db.py --snapshot path/to/shared_data_snapshot.sql
```
