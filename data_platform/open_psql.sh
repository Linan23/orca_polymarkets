#!/usr/bin/env bash
set -euo pipefail

DEFAULT_PSQL_URL="postgresql://postgres:postgres@localhost:5432/whaling"
DEFAULT_PSQL_BIN="/opt/homebrew/opt/postgresql@16/bin/psql"

if [[ -n "${PSQL_BIN:-}" ]]; then
  RESOLVED_PSQL_BIN="${PSQL_BIN}"
elif command -v psql >/dev/null 2>&1; then
  RESOLVED_PSQL_BIN="$(command -v psql)"
elif [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
  RESOLVED_PSQL_BIN="${DEFAULT_PSQL_BIN}"
else
  echo "Could not find psql. Set PSQL_BIN or install PostgreSQL CLI tools." >&2
  exit 1
fi

RESOLVED_PSQL_URL="${PSQL_URL:-${DEFAULT_PSQL_URL}}"

exec "${RESOLVED_PSQL_BIN}" "${RESOLVED_PSQL_URL}" "$@"
