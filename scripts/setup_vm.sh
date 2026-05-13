#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/setup_vm.sh [options]

Prepare or refresh the Orca VM deployment from the checked-out repo.

Options:
  --skip-frontend        Do not install/build the frontend.
  --skip-smoke           Do not run smoke checks after setup.
  --snapshot PATH        Import a PostgreSQL SQL snapshot after migrations.
  --empty-db             Leave the database empty after migrations.
  --reset-db             Refuse in VM mode unless ORCA_ALLOW_RESET_DB=1 is set.
  --no-service-restart   Do not restart systemd services.
  --help                 Show this help message.
EOF
}

SKIP_FRONTEND=0
SKIP_SMOKE=0
SNAPSHOT_PATH=""
EMPTY_DB=0
RESET_DB=0
NO_SERVICE_RESTART=0

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
    --no-service-restart) NO_SERVICE_RESTART=1 ;;
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
require_cmd python3
require_cmd node
require_cmd npm

if [[ ! -f /etc/orca.env ]]; then
  echo "Missing /etc/orca.env. Create it from .env.production.example and fill server-only secrets." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source /etc/orca.env
set +a

for required in DATABASE_URL FRONTEND_ORIGIN AUTH_SECRET_KEY; do
  if [[ -z "${!required:-}" ]]; then
    echo "Missing required /etc/orca.env value: $required" >&2
    exit 1
  fi
done

if [[ "$RESET_DB" -eq 1 && "${ORCA_ALLOW_RESET_DB:-0}" != "1" ]]; then
  echo "--reset-db is blocked on VM unless ORCA_ALLOW_RESET_DB=1 is set explicitly." >&2
  exit 2
fi

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if [[ "$SKIP_FRONTEND" -eq 0 ]]; then
  npm --prefix my-app ci
  npm --prefix my-app run build
fi

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
  PSQL_URL_EFFECTIVE="${PSQL_URL:-${DATABASE_URL/postgresql+psycopg:/postgresql:}}"
  psql "$PSQL_URL_EFFECTIVE" -f "$SNAPSHOT_PATH"
elif [[ "$EMPTY_DB" -eq 1 ]]; then
  echo "Database migrated and left empty by request."
fi

if [[ "$SKIP_SMOKE" -eq 0 ]]; then
  python scripts/secret_scan.py
  python data_platform/tests/smoke_validate.py
fi

if [[ "$NO_SERVICE_RESTART" -eq 0 ]]; then
  if command -v systemctl >/dev/null 2>&1; then
    sudo systemctl restart orca-api.service
    sudo systemctl restart orca-frontend.service
  else
    echo "systemctl not available; skipping service restart."
  fi
fi

if command -v curl >/dev/null 2>&1; then
  curl -fsS "${VITE_API_BASE_URL:-http://localhost:8001}/health" >/dev/null || true
  curl -fsS "${FRONTEND_ORIGIN}" >/dev/null || true
fi

echo "VM setup complete."
