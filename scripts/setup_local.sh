#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/setup_local.sh [options]

Prepare a local Orca development environment.

Options:
  --skip-frontend        Do not install frontend npm dependencies.
  --skip-smoke           Do not run smoke checks after setup.
  --snapshot PATH        Import a PostgreSQL SQL snapshot after migrations.
  --empty-db             Leave the database empty after migrations.
  --reset-db             Stop Docker and remove the local PostgreSQL volume first.
  --help                 Show this help message.
EOF
}

SKIP_FRONTEND=0
SKIP_SMOKE=0
SNAPSHOT_PATH=""
EMPTY_DB=0
RESET_DB=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-frontend) SKIP_FRONTEND=1 ;;
    --skip-smoke) SKIP_SMOKE=1 ;;
    --snapshot)
      SNAPSHOT_PATH="${2:-}"
      if [[ -z "$SNAPSHOT_PATH" ]]; then
        echo "--snapshot requires a path" >&2
        exit 2
      fi
      shift
      ;;
    --empty-db) EMPTY_DB=1 ;;
    --reset-db) RESET_DB=1 ;;
    --help) usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
  shift
done

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd git
require_cmd docker
require_cmd python3
require_cmd node
require_cmd npm

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose v2 is required: install Docker Desktop or docker compose." >&2
  exit 1
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Fill SMTP/auth values before testing auth email flows."
fi

if [[ "$RESET_DB" -eq 1 ]]; then
  docker compose down -v
fi

docker compose up -d db

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if [[ "$SKIP_FRONTEND" -eq 0 ]]; then
  npm --prefix my-app install
fi

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://app:password@localhost:5433/app_db}"
python -m alembic -c alembic.ini upgrade heads

if [[ -n "$SNAPSHOT_PATH" ]]; then
  require_cmd psql
  if [[ "$EMPTY_DB" -eq 1 ]]; then
    echo "Use either --snapshot or --empty-db, not both." >&2
    exit 2
  fi
  if [[ ! -f "$SNAPSHOT_PATH" ]]; then
    echo "Snapshot not found: $SNAPSHOT_PATH" >&2
    exit 1
  fi
  PSQL_URL="${PSQL_URL:-${DATABASE_URL/postgresql+psycopg:/postgresql:}}"
  psql "$PSQL_URL" -f "$SNAPSHOT_PATH"
elif [[ "$EMPTY_DB" -eq 1 ]]; then
  echo "Database migrated and left empty by request."
fi

if [[ "$SKIP_SMOKE" -eq 0 ]]; then
  python scripts/secret_scan.py
  python data_platform/tests/smoke_validate.py
fi

cat <<'EOF'

Local setup complete.

Start the API:
  source .venv/bin/activate
  uvicorn data_platform.api.server:app --reload --host 0.0.0.0 --port 8001

Start the frontend:
  npm --prefix my-app run dev -- --host 0.0.0.0 --port 5173

Open:
  http://localhost:5173
EOF
