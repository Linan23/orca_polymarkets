# Data Retention

Orca continuously ingests Polymarket activity, so retention policy is required to keep storage and queries manageable.

## Retention Goals

- Keep full trade history for the most recent 6 months for ML retraining and client-visible validation.
- Keep normalized data and raw payloads when they support audit/debugging.
- Preserve trusted whales only while they remain active and continue meeting criteria.
- Keep dashboards focused on latest state plus summary history, not unlimited raw history.

## Recommended Policy

Full normalized trade and orderbook detail:

- Keep 6 months online.
- Partition large time-series tables.
- Archive older partitions before deleting when audit retention is needed.

Raw API payloads:

- Keep recent raw payloads online for debugging.
- Roll up or archive older payloads after they are normalized.
- Avoid counting large raw tables with unbounded `count(*)` in routine health checks.

ML validation:

- Keep client-visible validation within the last 6 months.
- Use older archived data only for offline experiments when needed.

Whale records:

- Keep active trusted whales.
- Downgrade or archive inactive whales that no longer meet trust criteria.
- Preserve historical IDs needed to explain old predictions.

## Maintenance Job

Use:

```bash
python data_platform/jobs/run_retention_maintenance.py --dry-run
python data_platform/jobs/run_retention_maintenance.py --apply
```

Run dry-runs first and review affected row counts. Large-table count checks should use timeout-safe estimates or partition metadata instead of blocking full-table counts.

## Developer Notes

- Do not rewrite historical Alembic migrations for retention changes.
- Add new retention behavior through migrations and `data_platform/services/storage_lifecycle.py`.
- Keep destructive cleanup behind explicit `--apply` flags.
