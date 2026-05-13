# Data Platform

`data_platform/` contains the backend data model, API, services, ingest jobs, migrations, and tests for Orca.

## Structure

- `api/`: FastAPI app entrypoint.
- `db/`: SQLAlchemy session and bootstrap helpers.
- `ingest/`: Polymarket payload normalization and persistence.
- `jobs/`: CLI jobs for ingest, analytics, retention, and ML snapshots.
- `migrations/`: Alembic migration history.
- `ml/`: ML dataset/model helpers and report documentation.
- `models/`: SQLAlchemy entities.
- `services/`: reusable business logic for reads, snapshots, auth, market scope, and whale scoring.
- `tests/`: smoke, data-quality, ML, and guardrail checks.

## Common Commands

Apply migrations:

```bash
python -m alembic -c alembic.ini upgrade heads
```

Run API:

```bash
uvicorn data_platform.api.server:app --reload --host 0.0.0.0 --port 8001
```

Run analytics refresh:

```bash
python data_platform/jobs/run_analytics_refresh.py
```

Run near-live ingest once:

```bash
python data_platform/jobs/run_live_ingest.py --max-cycles 1
```

## Design Rules

- Keep source-specific ingest code Polymarket-only.
- Put new jobs in `data_platform/jobs/`.
- Put reusable logic in `data_platform/services/`, not route handlers.
- Keep generated runtime outputs under `data_platform/runtime/`.
- Add migrations for schema changes; do not edit old migrations.
