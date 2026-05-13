#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

if [[ -f /etc/orca.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source /etc/orca.env
  set +a
fi

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://app:password@localhost:5433/app_db}"
export FRONTEND_ORIGIN="${FRONTEND_ORIGIN:-http://localhost:5173}"

if docker ps --format '{{.Names}}' | grep -qx 'orcaDB'; then
  :
else
  docker start orcaDB >/dev/null
fi

.venv/bin/python -m alembic -c alembic.ini upgrade heads
exec .venv/bin/python -m uvicorn data_platform.api.server:app --host 0.0.0.0 --port "${API_PORT:-8001}"
