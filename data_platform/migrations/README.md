# Alembic Migrations

This directory contains the versioned database migration files for the data platform.

## Current Status

Alembic is initialized and the current baseline revision is:

- `20260304_1200` in [`versions/20260304_1200_baseline.py`](versions/20260304_1200_baseline.py)

The baseline revision creates:
- schema `analytics`
- schema `raw`
- all ORM-backed tables currently defined in `data_platform/models/`

## Core Commands

From the repository root:

Upgrade a fresh database to the latest schema:

```bash
.venv/bin/alembic -c alembic.ini upgrade head
```

Show the current revision:

```bash
.venv/bin/alembic -c alembic.ini current
```

Show migration history:

```bash
.venv/bin/alembic -c alembic.ini history
```

Generate a new migration after model changes:

```bash
.venv/bin/alembic -c alembic.ini revision --autogenerate -m "describe_change"
```

Downgrade one revision:

```bash
.venv/bin/alembic -c alembic.ini downgrade -1
```

## Existing Databases Created Before Alembic

If your local database was created with `bootstrap_db.py` before Alembic was added, do not run `upgrade head` directly against that existing populated database.

Instead, mark the current schema as already being at the baseline revision:

```bash
.venv/bin/alembic -c alembic.ini stamp head
```

That creates the `alembic_version` row without recreating tables.

## Database URL Resolution

Alembic uses:

1. `DATABASE_URL` from the environment, if set
2. otherwise the default configured by `data_platform.settings`

That means the usual local setup still works:

```bash
export DATABASE_URL="postgresql+psycopg://postgres:postgres@localhost:5432/whaling"
```
