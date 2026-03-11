#!/usr/bin/env bash
set -euo pipefail

DEFAULT_PSQL_URL="postgresql://app:password@localhost:5433/app_db"
DEFAULT_PSQL_BIN="/opt/homebrew/opt/postgresql@16/bin/psql"
DEFAULT_DOCKER_CONTAINER="orcaDB"
DEFAULT_DOCKER_DB_USER="app"
DEFAULT_DOCKER_DB_NAME="app_db"

if [[ -n "${PSQL_BIN:-}" ]]; then
  RESOLVED_PSQL_BIN="${PSQL_BIN}"
elif command -v psql >/dev/null 2>&1; then
  RESOLVED_PSQL_BIN="$(command -v psql)"
elif [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
  RESOLVED_PSQL_BIN="${DEFAULT_PSQL_BIN}"
else
  RESOLVED_PSQL_BIN=""
fi

RESOLVED_PSQL_URL="${PSQL_URL:-${DEFAULT_PSQL_URL}}"

if [[ -n "${RESOLVED_PSQL_BIN}" ]]; then
  exec "${RESOLVED_PSQL_BIN}" "${RESOLVED_PSQL_URL}" "$@"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Could not find psql or docker. Set PSQL_BIN, install PostgreSQL CLI tools, or install Docker." >&2
  exit 1
fi

DOCKER_EXEC_ARGS=(exec -i)
if [[ -t 0 && -t 1 ]]; then
  DOCKER_EXEC_ARGS=(exec -it)
fi

DOCKER_DB_CONTAINER="${DOCKER_DB_CONTAINER:-${DEFAULT_DOCKER_CONTAINER}}"
DOCKER_DB_USER="${DOCKER_DB_USER:-${DEFAULT_DOCKER_DB_USER}}"
DOCKER_DB_NAME="${DOCKER_DB_NAME:-${DEFAULT_DOCKER_DB_NAME}}"

exec docker "${DOCKER_EXEC_ARGS[@]}" "${DOCKER_DB_CONTAINER}" psql -U "${DOCKER_DB_USER}" -d "${DOCKER_DB_NAME}" "$@"
