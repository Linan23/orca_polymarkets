# Local Setup

This guide assumes a new developer has Git, Docker Desktop, Python 3.12+, Node.js 20+, and npm installed.

## One-Command Setup

From the repo root:

```bash
scripts/setup_local.sh --empty-db
```

The script checks required tools, creates `.env` from `.env.example` when missing, creates `.venv`, installs Python dependencies, installs frontend dependencies, starts local PostgreSQL through Docker Compose, applies migrations, and runs smoke checks.

Useful options:

```bash
scripts/setup_local.sh --snapshot /path/to/shared_data_snapshot.sql
scripts/setup_local.sh --reset-db --empty-db
scripts/setup_local.sh --skip-smoke
scripts/setup_local.sh --skip-frontend
```

`--snapshot` imports an approved database snapshot. Do not commit snapshots to Git.

## Run The Website Locally

Start the API:

```bash
source .venv/bin/activate
uvicorn data_platform.api.server:app --reload --host 0.0.0.0 --port 8001
```

Start the frontend in a second terminal:

```bash
npm --prefix my-app run dev -- --host 0.0.0.0 --port 5173
```

Open:

```text
http://localhost:5173
```

## Local Database

Docker Compose exposes PostgreSQL on local port `5433`.

Default local connection strings:

```env
DATABASE_URL=postgresql+psycopg://app:password@localhost:5433/app_db
PSQL_URL=postgresql://app:password@localhost:5433/app_db
```

Apply migrations manually:

```bash
source .venv/bin/activate
python -m alembic -c alembic.ini upgrade heads
```

## Environment Variables

Copy `.env.example` to `.env` and fill local-only values as needed. Email verification and password reset require SMTP values, but normal public dashboard browsing does not.

Never commit real values for:

- `AUTH_SECRET_KEY`
- `SMTP_PASSWORD`
- `DATABASE_URL` with production credentials
- VM passwords or SSH keys
- Any generated snapshots

## Verification

```bash
python -m compileall data_platform scripts
python scripts/secret_scan.py
python data_platform/tests/smoke_validate.py
npm --prefix my-app run lint
npm --prefix my-app run build
```
