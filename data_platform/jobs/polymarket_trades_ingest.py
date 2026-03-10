"""Capture Polymarket trades and persist normalized transaction facts."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
import sys

import httpx

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.ingest.polymarket import ingest_trades_record

POLYMARKET_TRADES_URL = "https://data-api.polymarket.com/trades"


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Capture Polymarket trades into PostgreSQL.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument("--limit", type=int, default=200, help="Maximum trade rows requested per cycle.")
    parser.add_argument(
        "--output-file",
        default="data_platform/runtime/polymarket_trades.jsonl",
        help="Optional JSONL archive path for cycle summaries.",
    )
    parser.add_argument("--interval-seconds", type=float, default=120.0, help="Sleep between cycles when looping.")
    parser.add_argument("--max-requests", type=int, default=1, help="Number of cycles to run. 0 means forever.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="HTTP timeout per request.")
    parser.add_argument("--max-retries", type=int, default=5, help="Retry count for transient failures.")
    parser.add_argument("--backoff-base-seconds", type=float, default=1.0)
    parser.add_argument("--backoff-cap-seconds", type=float, default=30.0)
    args = parser.parse_args()

    if args.limit <= 0:
        parser.error("--limit must be > 0.")
    if args.interval_seconds < 0:
        parser.error("--interval-seconds must be >= 0.")
    if args.max_requests < 0:
        parser.error("--max-requests must be >= 0.")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0.")
    if args.max_retries < 0:
        parser.error("--max-retries must be >= 0.")
    return args


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    """Append one JSONL record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str))
        handle.write("\n")


def compute_retry_delay(attempt: int, args: argparse.Namespace) -> float:
    """Compute capped exponential backoff delay."""
    return min(args.backoff_cap_seconds, args.backoff_base_seconds * (2 ** attempt))


def fetch_trades(client: httpx.Client, args: argparse.Namespace) -> list[dict[str, Any]]:
    """Fetch one trade batch with retry/backoff."""
    params: dict[str, Any] = {"limit": args.limit}
    for attempt in range(args.max_retries + 1):
        try:
            response = client.get(POLYMARKET_TRADES_URL, params=params, timeout=args.timeout_seconds)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError("Unexpected Polymarket trades response shape; expected a list.")
            return [item for item in payload if isinstance(item, dict)]
        except (httpx.RequestError, httpx.HTTPStatusError, ValueError):
            if attempt >= args.max_retries:
                raise
            time.sleep(compute_retry_delay(attempt, args))
    raise RuntimeError("Unreachable retry loop state.")


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    """Run one Polymarket trades cycle."""
    with httpx.Client() as client:
        trades = fetch_trades(client, args)

    started_at = datetime.now(timezone.utc)
    record = {
        "scraped_at_unix": int(started_at.timestamp()),
        "scraped_at_iso": started_at.isoformat(),
        "count": len(trades),
        "query": {"limit": args.limit},
        "trades": trades,
    }
    request_url = f"{POLYMARKET_TRADES_URL}?{urlencode(record['query'])}"
    with session_scope(args.database_url or None) as session:
        db_result = ingest_trades_record(
            session,
            record=record,
            request_url=request_url,
            raw_output_path=args.output_file,
        )

    summary = {
        "scraped_at": started_at.isoformat(),
        "count": len(trades),
        "db_ingest": db_result,
    }
    append_jsonl(Path(args.output_file), summary)
    return summary


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    request_count = 0
    while True:
        request_count += 1
        summary = run_once(args)
        print(json.dumps(summary, sort_keys=True, default=str))
        if args.max_requests and request_count >= args.max_requests:
            return 0
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())

