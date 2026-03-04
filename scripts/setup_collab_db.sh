#!/usr/bin/env bash
set -euo pipefail

# One-command collaborator onboarding for the Docker PostgreSQL profile.
# - starts the Docker db service
# - applies Alembic migrations
# - optionally imports a shared snapshot
# - runs smoke validation

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TARGET_DATABASE_URL="${TARGET_DATABASE_URL:-postgresql+psycopg://app:password@localhost:5433/app_db}"
TARGET_PSQL_URL="${TARGET_PSQL_URL:-postgresql://app:password@localhost:5433/app_db}"

SNAPSHOT_PATH=""
IMPORT_SNAPSHOT=true
RUN_SAMPLE_VALIDATION=true

usage() {
  cat <<'EOF'
Usage:
  ./scripts/setup_collab_db.sh [--snapshot PATH] [--skip-import] [--no-sample-validation]

Options:
  --snapshot PATH          SQL snapshot file to import.
                           Default resolution order:
                             1) ./shared_data_snapshot.sql
                             2) ./data_platform/runtime/shared_data_snapshot.sql
  --skip-import            Skip snapshot import.
  --no-sample-validation   Run smoke validation without requiring sample rows.
  -h, --help               Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot)
      SNAPSHOT_PATH="${2:-}"
      shift 2
      ;;
    --skip-import)
      IMPORT_SNAPSHOT=false
      shift
      ;;
    --no-sample-validation)
      RUN_SAMPLE_VALIDATION=false
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found in PATH."
  echo "Install/start Docker Desktop, then retry."
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required but unavailable."
  exit 1
fi

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Missing .venv/bin/python. Create the virtualenv and install dependencies first."
  exit 1
fi

if [[ ! -x "./data_platform/open_psql.sh" ]]; then
  echo "Missing ./data_platform/open_psql.sh"
  exit 1
fi

if [[ "$IMPORT_SNAPSHOT" == true && -z "$SNAPSHOT_PATH" ]]; then
  if [[ -f "shared_data_snapshot.sql" ]]; then
    SNAPSHOT_PATH="shared_data_snapshot.sql"
  elif [[ -f "data_platform/runtime/shared_data_snapshot.sql" ]]; then
    SNAPSHOT_PATH="data_platform/runtime/shared_data_snapshot.sql"
  else
    echo "No snapshot file found at shared_data_snapshot.sql or data_platform/runtime/shared_data_snapshot.sql."
    echo "Use --snapshot PATH or --skip-import."
    exit 1
  fi
fi

echo "Starting Docker PostgreSQL..."
docker compose -f app/compose.yaml up -d db

echo "Waiting for Docker DB..."
for _ in {1..45}; do
  if PSQL_URL="$TARGET_PSQL_URL" ./data_platform/open_psql.sh -Atqc "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! PSQL_URL="$TARGET_PSQL_URL" ./data_platform/open_psql.sh -Atqc "SELECT 1" >/dev/null 2>&1; then
  echo "Docker DB is not reachable at $TARGET_PSQL_URL"
  exit 1
fi

echo "Applying migrations..."
DATABASE_URL="$TARGET_DATABASE_URL" .venv/bin/alembic -c alembic.ini upgrade head

if [[ "$IMPORT_SNAPSHOT" == true ]]; then
  echo "Importing snapshot: $SNAPSHOT_PATH"
  echo "Resetting analytics/raw tables before import..."
  TRUNCATE_SQL="$(PSQL_URL="$TARGET_PSQL_URL" ./data_platform/open_psql.sh -Atqc "
SELECT format('TRUNCATE TABLE %I.%I RESTART IDENTITY CASCADE;', schemaname, tablename)
FROM pg_tables
WHERE schemaname IN ('analytics', 'raw')
ORDER BY schemaname, tablename;
")"
  if [[ -n "$TRUNCATE_SQL" ]]; then
    printf '%s\n' "$TRUNCATE_SQL" | PSQL_URL="$TARGET_PSQL_URL" ./data_platform/open_psql.sh -v ON_ERROR_STOP=1 >/dev/null
  fi
  PSQL_URL="$TARGET_PSQL_URL" ./data_platform/open_psql.sh -v ON_ERROR_STOP=1 < "$SNAPSHOT_PATH"
fi

echo "Running smoke validation..."
if [[ "$RUN_SAMPLE_VALIDATION" == true ]]; then
  DATABASE_URL="$TARGET_DATABASE_URL" PSQL_URL="$TARGET_PSQL_URL" \
    .venv/bin/python data_platform/tests/smoke_validate.py --require-sample-data
else
  DATABASE_URL="$TARGET_DATABASE_URL" PSQL_URL="$TARGET_PSQL_URL" \
    .venv/bin/python data_platform/tests/smoke_validate.py
fi

echo
echo "Setup complete."
echo "Use these in your shell for Docker DB access:"
echo "  export DATABASE_URL=\"$TARGET_DATABASE_URL\""
echo "  export PSQL_URL=\"$TARGET_PSQL_URL\""
echo
echo "Quick access:"
echo "  PSQL_URL=\"$TARGET_PSQL_URL\" ./data_platform/open_psql.sh"
