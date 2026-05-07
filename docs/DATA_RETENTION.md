# Data Cleanup And Retention Plan

This project continuously pulls Polymarket data, so the database should keep recent detailed data hot, preserve useful ML history for retraining, and move older high-volume records into summaries before deletion. The first implementation is intentionally conservative: the maintenance job can produce a dry-run retention report, but it does not delete trade history or ML validation history yet.

## Policy

The active policy lives in `data_platform/config/retention_policy.json`.

Default windows:

- Full normalized trade history for ML retraining: last 180 days.
- Client-visible ML validation results: last 180 days.
- Raw API payloads in the active database: 60 days, unless still referenced by normalized tables.
- Detailed orderbook snapshots: 30 days, with hourly and daily rollups retained longer.
- Detailed dashboard snapshots: 30 days, with summary history retained for 180 days.
- Detailed whale score snapshots: 60 days.
- Trusted whales: preserved only while active and still meeting the whale/trust criteria.

## Storage Tiers

Hot data is detailed data used by the dashboard and live ML cards. Keep this small and recent:

- current market contracts/events
- recent trades and whale activity
- recent orderbook snapshots
- recent ML prediction snapshots and validations
- latest dashboard/profile snapshots

Warm data is compact history used for trend validation and retraining:

- six months of normalized trades
- six months of ML validation rows
- hourly/daily orderbook rollups
- daily position rollups
- dashboard and ML summary history

Cold data is audit/debug material that should not clutter the active query path:

- older raw Polymarket payloads
- older detailed snapshots that already have rollups
- snapshot backups or archived exports

Cold data should be archived before destructive cleanup is enabled.

## What Must Be Protected

Do not delete these records without a separate migration or archive review:

- referenced raw payloads
- followed users and followed markets
- active trusted whales
- unvalidated ML predictions that may still need a future actual result
- normalized trades inside the 180-day ML window
- validation rows inside the 180-day client freshness window

## Maintenance Commands

Preview the current cleanup candidates without changing data:

```bash
.venv/bin/python data_platform/jobs/run_retention_maintenance.py --dry-run --skip-snapshot
```

For large databases, the report uses capped counts and per-query timeouts:

```bash
.venv/bin/python data_platform/jobs/run_retention_maintenance.py \
  --dry-run \
  --skip-snapshot \
  --retention-report-count-mode auto \
  --retention-report-row-limit 10000 \
  --retention-report-timeout-ms 5000
```

`auto` mode uses fast PostgreSQL planner estimates for known high-volume tables such as raw payloads, trades, and whale score snapshots. Smaller tables still use capped exact counts. For raw payloads, auto mode estimates age-eligible rows only; the real garbage collector still applies the full referenced-payload protection before deleting anything. If `candidate_rows_is_lower_bound` is true, more rows exist beyond the configured cap. If a count times out, the report records `count_error` and falls back to a planner estimate when possible instead of blocking maintenance.

Run the existing nightly maintenance behavior:

```bash
.venv/bin/python data_platform/jobs/run_retention_maintenance.py --skip-snapshot
```

The nightly job currently:

1. creates current and next-month partitions
2. backfills partition shadow tables
3. rolls up old orderbook and position snapshots
4. deletes orphan market events in batches
5. garbage-collects unreferenced raw payloads in batches
6. optionally writes a backup snapshot

The dry-run report should be reviewed before enabling any new destructive cleanup phase.

## Recommended Rollout

Phase 1 is implemented first:

- add a versioned retention policy
- document the lifecycle rules for handoff
- add a dry-run report to show cleanup candidates
- keep existing nightly maintenance behavior unchanged

Phase 2 should add archive-first jobs:

- export old raw payloads before deleting active DB rows
- archive detailed snapshots after rollups are verified
- record archive manifests with row counts, date windows, and checksums

Phase 3 should enable controlled pruning:

- prune old unreferenced raw payloads beyond the active window
- prune detailed orderbook/position snapshots after rollup coverage passes
- keep six months of trade and ML validation detail
- keep dashboard detail short-lived and retain summary history

Phase 4 should add monitoring:

- database size by schema and top tables
- candidate row counts per retention category
- last successful archive timestamp
- last successful maintenance timestamp
- failed cleanup reason in the maintenance JSONL log

## Handoff Notes

For new developers, retention changes should follow this order:

1. update `data_platform/config/retention_policy.json`
2. run the dry-run report locally or on the VM
3. verify row counts and protected categories
4. add archive coverage if deleting a new data class
5. run Python compile and the smoke checks
6. deploy through GitHub, then pull on the VM

Avoid ad hoc database deletes. Cleanup should stay config-driven, logged, and reversible through archives.
