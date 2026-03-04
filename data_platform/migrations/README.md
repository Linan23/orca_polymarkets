# Migrations Placeholder

This directory is reserved for Alembic migration files used for versioned database migrations.

This directory will track PostgreSQL schema changes over time instead of relying on manual SQL or repeated bootstrap resets.

The current database layer includes:
- SQLAlchemy models in `data_platform/models/`
- schema bootstrap in `bootstrap_db.py`

Next migration step:
- initialize Alembic config
- generate the first baseline revision from the current models
