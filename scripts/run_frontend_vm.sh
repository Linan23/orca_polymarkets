#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR/my-app"

export VITE_API_BASE_URL="${VITE_API_BASE_URL:-http://139.147.9.248:8001}"

if [[ ! -x node_modules/.bin/vite ]]; then
  npm install
fi

npm run build

exec npm run preview -- --host 0.0.0.0 --port "${FRONTEND_PORT:-5173}" --strictPort
