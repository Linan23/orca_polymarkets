#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

source .venv/bin/activate
export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://app:password@localhost:5433/app_db}"

python -m alembic -c alembic.ini upgrade heads

exec python data_platform/jobs/run_ml_prediction_confidence_cycle.py \
  --platform-name "${ML_PREDICTION_CYCLE_PLATFORM:-polymarket}" \
  --promotion-mode "${ML_PREDICTION_CONFIDENCE_PROMOTION_MODE:-gated}" \
  --watch-precision-target "${ML_PREDICTION_WATCH_PRECISION_TARGET:-0.70}" \
  --strong-precision-target "${ML_PREDICTION_STRONG_PRECISION_TARGET:-0.80}" \
  --max-mae-regression-pts "${ML_PREDICTION_MAX_MAE_REGRESSION_PTS:-0.5}" \
  "$@"
