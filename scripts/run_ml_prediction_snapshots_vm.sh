#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

source .venv/bin/activate
export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://app:password@localhost:5433/app_db}"

python -m alembic -c alembic.ini upgrade heads

exec python data_platform/jobs/generate_ml_market_prediction_snapshots.py \
  --platform-name "${ML_PREDICTION_SNAPSHOT_PLATFORM:-polymarket}" \
  --limit "${ML_PREDICTION_SNAPSHOT_LIMIT:-0}" \
  "$@"
