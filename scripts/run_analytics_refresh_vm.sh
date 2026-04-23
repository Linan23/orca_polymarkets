#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

source .venv/bin/activate
export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://app:password@localhost:5433/app_db}"
python -m alembic -c alembic.ini upgrade head
exec python data_platform/jobs/run_analytics_refresh.py "$@"
