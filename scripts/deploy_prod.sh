#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ENV_FILE="${ENV_FILE:-.env.production}"
COMPOSE_FILE="${COMPOSE_FILE:-compose.prod.yaml}"
COMPOSE=(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE")

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE. Copy .env.production.example and fill in production values." >&2
  exit 1
fi

echo "Building production images..."
"${COMPOSE[@]}" build api web

echo "Starting PostgreSQL..."
"${COMPOSE[@]}" up -d db

echo "Waiting for PostgreSQL healthcheck..."
for _ in {1..30}; do
  if "${COMPOSE[@]}" exec -T db sh -lc 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! "${COMPOSE[@]}" exec -T db sh -lc 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null 2>&1; then
  echo "PostgreSQL did not become healthy in time." >&2
  exit 1
fi

echo "Applying Alembic migrations..."
"${COMPOSE[@]}" run --rm api python -m alembic -c alembic.ini upgrade head

echo "Starting API and web services..."
"${COMPOSE[@]}" up -d api web

echo "Deployment finished."
echo "Smoke checks:"
echo "  curl -I http://127.0.0.1/health"
echo "  curl -I http://127.0.0.1/api/status/ingestion"
echo "  ${COMPOSE[*]} ps"
