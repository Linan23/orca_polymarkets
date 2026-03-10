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
9. builds the derived dashboard snapshot

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

Run only the Kalshi order-book step:

```bash
.venv/bin/python data_platform/jobs/kalshi_orderbook_snapshot.py --environment prod --market-limit 5 --max-requests 1
```

Load a shared snapshot into Docker PostgreSQL:

```bash
./data_platform/open_psql.sh < path/to/shared_data_snapshot.sql
```

For full collaborator onboarding, use:

```bash
.venv/bin/python scripts/setup_collab_db.py --snapshot path/to/shared_data_snapshot.sql
```
