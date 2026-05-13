# Operations

This document covers routine checks and maintenance for the Orca dashboard.

## Daily Health Checks

```bash
sudo systemctl status orca-api.service
sudo systemctl status orca-frontend.service
sudo systemctl status orca-ingest-live.service
sudo systemctl status orca-analytics-refresh.service
curl -fsS http://localhost:8001/health
```

## Logs

```bash
journalctl -u orca-api.service -n 200 --no-pager
journalctl -u orca-frontend.service -n 200 --no-pager
journalctl -u orca-ingest-live.service -n 200 --no-pager
journalctl -u orca-analytics-refresh.service -n 200 --no-pager
```

Runtime logs and generated artifacts belong under `data_platform/runtime/`, which is ignored by Git.

## Scheduled Jobs

Near-live ingest:

```bash
scripts/run_ingest_live_vm.sh
```

Analytics refresh:

```bash
scripts/run_analytics_refresh_vm.sh
```

Retention maintenance:

```bash
scripts/run_retention_rollup_vm.sh
```

## Cache Warming

After API restart or deploy, visit or curl the high-traffic dashboard pages first:

```bash
curl -fsS http://localhost:8001/api/dashboard/home >/dev/null
curl -fsS http://localhost:8001/api/leaderboard/markets >/dev/null
curl -fsS http://localhost:8001/api/leaderboard/whales >/dev/null
```

## Credential Rotation

Rotate credentials immediately if they are pasted into chat, committed, logged, or exposed:

- Gmail app password.
- `AUTH_SECRET_KEY`.
- Database password.
- VM SSH password or key.
- Optional external API tokens.

After rotation, update only `/etc/orca.env` or local `.env`; never Git.

## Backups And Snapshots

Database snapshots are useful for handoff and debugging, but they are not source code.

- Store snapshots outside Git.
- Document snapshot date, database source, and schema revision.
- Use `scripts/setup_local.sh --snapshot PATH` for local import.
