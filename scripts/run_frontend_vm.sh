#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR/my-app"

if [[ -f /etc/orca.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source /etc/orca.env
  set +a
fi

export VITE_API_BASE_URL="${VITE_API_BASE_URL:-http://localhost:8001}"

if [[ ! -d node_modules ]]; then
  npm ci
fi

if [[ ! -d dist ]]; then
  npm run build
fi

exec npm run preview -- --host 0.0.0.0 --port "${FRONTEND_PORT:-5173}" --strictPort
