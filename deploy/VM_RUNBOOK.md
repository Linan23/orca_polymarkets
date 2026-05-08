# VM Runbook

This runbook is intentionally generic. Replace the values below for your VM before using it:

```bash
export VM_USER="your-vm-user"
export VM_HOST="your-vm-host"
export VM_REPO_DIR="/home/$VM_USER/orca_polymarkets"
```

Then connect with:

```bash
ssh "$VM_USER@$VM_HOST"
cd "$VM_REPO_DIR"
```

## Service roles

- `orca-ingest-live.service`
  - near-live ingest loop
  - 2-minute cadence
  - focused domains: `politics`, `crypto`, `technology`, `video-games`, `finance`
- `orca-analytics-refresh.service`
  - 15-minute whale/dashboard refresh
- `orca-ml-confidence-cycle.timer`
  - 6-hour ML validation, confidence retraining, gated promotion, and prediction snapshot refresh
- `orca-retention-rollup.service`
  - nightly partitions, rollups, orphan-event cleanup, raw-payload GC, optional snapshot
- `orca-backup-snapshot.timer`
  - nightly trigger for `orca-retention-rollup.service`

## Normal deploy

```bash
cd "$VM_REPO_DIR"
git pull origin main
source .venv/bin/activate
python -m alembic -c alembic.ini upgrade head
sudo systemctl restart orca-api.service orca-frontend.service orca-ingest-live.service orca-analytics-refresh.service
./scripts/warm_dashboard_caches_vm.sh
sudo systemctl enable --now orca-ml-confidence-cycle.timer
```

Verify:

```bash
curl -s http://127.0.0.1:8001/health
curl -s "http://127.0.0.1:8001/api/dashboard/home?timeframe=all&limit=5" >/dev/null
sudo systemctl status orca-ingest-live.service orca-analytics-refresh.service orca-retention-rollup.service orca-ml-confidence-cycle.timer --no-pager
```

The VM frontend service runs `npm run build` and serves the built `dist/` bundle with Vite preview on the existing frontend port. The cache warmer fills homepage, research, leaderboard, whale, and hot market-profile reads so the first dashboard visit after restart is not a cold database path.

## Continuous ML confidence cycle

Manual run:

```bash
cd "$VM_REPO_DIR"
source .venv/bin/activate
.venv/bin/python data_platform/jobs/run_ml_prediction_confidence_cycle.py \
  --promotion-mode gated \
  --watch-precision-target 0.70 \
  --strong-precision-target 0.80 \
  --max-mae-regression-pts 0.5
```

What it does:

1. validates matured 12h/24h ML prediction snapshots
2. trains a candidate confidence artifact
3. compares candidate versus active artifact on chronological holdout rows
4. promotes only when guardrails pass
5. writes fresh market-profile prediction snapshots

Artifacts:

- active model: `data_platform/runtime/ml/market_prediction_confidence_model.json`
- candidates: `data_platform/runtime/ml/candidates/`
- promotion log: `data_platform/runtime/ml/model_promotion_manifest.jsonl`

## Live scope prune

Use the staged live-safe prune path on the VM. Do not run the fully destructive prune during active service hours.

```bash
cd "$VM_REPO_DIR"
source .venv/bin/activate
.venv/bin/python data_platform/jobs/prune_market_scope.py \
  --platform polymarket \
  --platform kalshi \
  --focus-domain politics \
  --focus-domain crypto \
  --focus-domain technology \
  --focus-domain video-games \
  --focus-domain finance \
  --sample-size 8 \
  --apply \
  --preserve-current-events \
  --skip-raw-payload-prune
```

Then rebuild derived outputs:

```bash
.venv/bin/python build_whale_scores.py
.venv/bin/python build_dashboard_snapshot.py
.venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data --build-dashboard
```

## Nightly maintenance

Preview retention candidates without changing data:

```bash
cd "$VM_REPO_DIR"
source .venv/bin/activate
.venv/bin/python data_platform/jobs/run_retention_maintenance.py --dry-run --skip-snapshot
```

The dry-run report uses capped counts and query timeouts. If a table is very large, `candidate_rows_is_lower_bound` or `count_error` is expected and safer than blocking the database.
By default, `auto` count mode uses fast planner estimates for high-volume raw/trade/whale tables and capped exact counts for smaller tables. Raw payload estimates are age-eligible estimates; the real GC still protects referenced payloads.

Manual run:

```bash
cd "$VM_REPO_DIR"
source .venv/bin/activate
.venv/bin/python data_platform/jobs/run_retention_maintenance.py --skip-snapshot
```

What it does:

1. creates current and next-month partitions
2. backfills partition shadow tables
3. rolls up old orderbook and position snapshots
4. deletes orphan `analytics.market_event` rows in batches
5. garbage-collects unreferenced `raw.api_payload` rows in batches
6. optionally writes a full snapshot backup artifact

Retention policy and handoff guidance:

- `data_platform/config/retention_policy.json`
- `docs/DATA_RETENTION.md`

If you want a smaller cleanup run:

```bash
.venv/bin/python data_platform/jobs/run_retention_maintenance.py \
  --skip-snapshot \
  --orphan-event-batch-size 250 \
  --orphan-event-max-batches 2 \
  --raw-payload-gc-batch-size 250 \
  --raw-payload-gc-max-batches 2
```

## Logs

```bash
sudo journalctl -u orca-ingest-live.service -f
sudo journalctl -u orca-analytics-refresh.service -f
sudo journalctl -u orca-ml-confidence-cycle.service -f
sudo journalctl -u orca-retention-rollup.service -f
tail -f "$VM_REPO_DIR/data_platform/runtime/ingest_live_runs.jsonl"
tail -f "$VM_REPO_DIR/data_platform/runtime/maintenance_runs.jsonl"
tail -f "$VM_REPO_DIR/data_platform/runtime/ml/model_promotion_manifest.jsonl"
```

## Notes

- The live ingest wrapper already injects the focused categories unless you explicitly pass different `--focus-domain` flags.
- The nightly maintenance job is the correct place to finish deleting preserved orphan events and unused raw payloads.
- Normal VM refresh should happen via `git pull`, Alembic migrations, and service restarts only. Do not use snapshot restore for routine updates.
