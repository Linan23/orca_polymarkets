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
  - focused domains: `politics`, `crypto`, `technology`, `video-games`
- `orca-analytics-refresh.service`
  - 15-minute whale/dashboard refresh
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
```

Verify:

```bash
curl -s http://127.0.0.1:8001/health
sudo systemctl status orca-ingest-live.service orca-analytics-refresh.service orca-retention-rollup.service --no-pager
```

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
sudo journalctl -u orca-retention-rollup.service -f
tail -f "$VM_REPO_DIR/data_platform/runtime/ingest_live_runs.jsonl"
tail -f "$VM_REPO_DIR/data_platform/runtime/maintenance_runs.jsonl"
```

## Notes

- The live ingest wrapper already injects the focused categories unless you explicitly pass different `--focus-domain` flags.
- The nightly maintenance job is the correct place to finish deleting preserved orphan events and unused raw payloads.
- Normal VM refresh should happen via `git pull`, Alembic migrations, and service restarts only. Do not use snapshot restore for routine updates.
