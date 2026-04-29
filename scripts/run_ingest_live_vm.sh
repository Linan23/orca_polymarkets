#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

source .venv/bin/activate
export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://app:password@localhost:5433/app_db}"

docker compose -f app/compose.yaml up -d db
python -m alembic -c alembic.ini upgrade head

has_focus_domain=0
for arg in "$@"; do
  if [[ "$arg" == "--focus-domain" ]]; then
    has_focus_domain=1
    break
  fi
done

focus_args=()
if [[ "$has_focus_domain" -eq 0 ]]; then
  focus_args=(
    --focus-domain politics
    --focus-domain crypto
    --focus-domain technology
    --focus-domain video-games
  )
fi

exec python data_platform/jobs/run_live_ingest.py "${focus_args[@]}" "$@"
