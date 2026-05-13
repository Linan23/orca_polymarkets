#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Missing .venv/bin/python. Create the virtualenv and install requirements first." >&2
  exit 1
fi

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://app:password@localhost:5433/app_db}"

docker compose -f compose.yaml up -d db

.venv/bin/python -m alembic -c alembic.ini upgrade heads

exec .venv/bin/python data_platform/jobs/run_ingest_cycle.py \
  --enable-polymarket-public-crawl \
  --public-crawl-market-limit "${PUBLIC_CRAWL_MARKET_LIMIT:-25}" \
  --public-crawl-closed-market-limit "${PUBLIC_CRAWL_CLOSED_MARKET_LIMIT:-10}" \
  --public-crawl-closed-within-days "${PUBLIC_CRAWL_CLOSED_WITHIN_DAYS:-7}" \
  --public-crawl-global-pages "${PUBLIC_CRAWL_GLOBAL_PAGES:-2}" \
  --public-crawl-max-pages-per-market "${PUBLIC_CRAWL_MAX_PAGES_PER_MARKET:-3}" \
  --public-crawl-max-total-trade-pages "${PUBLIC_CRAWL_MAX_TOTAL_TRADE_PAGES:-20}" \
  --skip-positions \
  --loop \
  --interval-hours "${CRAWLER_INTERVAL_HOURS:-1}" \
  --window-start "${CRAWLER_WINDOW_START:-00:00}" \
  --window-end "${CRAWLER_WINDOW_END:-00:00}" \
  --timezone "${CRAWLER_TIMEZONE:-America/New_York}" \
  --jitter-seconds "${CRAWLER_JITTER_SECONDS:-30}" \
  --focus-domain politics \
  --focus-domain crypto \
  --focus-domain technology \
  --focus-domain video-games \
  --focus-domain finance
