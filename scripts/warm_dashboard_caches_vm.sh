#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

PYTHON_BIN="${PYTHON_BIN:-$REPO_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="${PYTHON_BIN_FALLBACK:-python3}"
fi

API_BASE_URL="${ORCA_API_BASE_URL:-http://127.0.0.1:${API_PORT:-8001}}"

exec "$PYTHON_BIN" data_platform/jobs/warm_dashboard_caches.py \
  --api-base-url "$API_BASE_URL" \
  --market-limit "${CACHE_WARM_MARKET_LIMIT:-20}" \
  --preview-limit "${CACHE_WARM_PREVIEW_LIMIT:-5}" \
  --timeout-seconds "${CACHE_WARM_TIMEOUT_SECONDS:-15}"
