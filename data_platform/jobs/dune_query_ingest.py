"""Fetch a saved Dune query result and persist it into PostgreSQL."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from data_platform.db.session import session_scope
from data_platform.ingest.dune import extract_rows, ingest_query_pages

DEFAULT_BASE_URL = "https://api.dune.com/api/v1"
DEFAULT_QUERY_ID = "2103719"
DEFAULT_OUTPUT_FILE = "data_platform/runtime/dune_query_results.jsonl"


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Fetch a Dune query result and ingest it into PostgreSQL.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument("--query-id", default=os.getenv("DUNE_QUERY_ID", DEFAULT_QUERY_ID), help="Saved Dune query id.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base Dune API URL.")
    parser.add_argument("--output-file", default=DEFAULT_OUTPUT_FILE, help="Optional JSONL summary path.")
    parser.add_argument("--page-size", type=int, default=100, help="Rows requested per API page.")
    parser.add_argument("--max-pages", type=int, default=5, help="Maximum pages per cycle. 0 means fetch until exhausted.")
    parser.add_argument("--interval-seconds", type=float, default=900.0, help="Sleep between cycles when looping.")
    parser.add_argument("--max-requests", type=int, default=1, help="Number of cycles to run. 0 means forever.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP timeout per request.")
    parser.add_argument("--api-key-env", default="DUNE_API_KEY", help="Environment variable that stores the Dune API key.")
    args = parser.parse_args()

    if not args.query_id.strip():
        parser.error("--query-id must not be empty.")
    if args.page_size <= 0:
        parser.error("--page-size must be > 0.")
    if args.max_pages < 0:
        parser.error("--max-pages must be >= 0.")
    if args.interval_seconds < 0:
        parser.error("--interval-seconds must be >= 0.")
    if args.max_requests < 0:
        parser.error("--max-requests must be >= 0.")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0.")
    return args


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    """Append a summary record to a JSONL file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str))
        handle.write("\n")


def _extract_next_reference(payload: dict[str, Any]) -> tuple[str | None, int | None]:
    """Return next URI or offset when the Dune response exposes pagination."""
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    next_uri = payload.get("next_uri") or result.get("next_uri")
    next_offset = payload.get("next_offset") or result.get("next_offset")
    if isinstance(next_offset, str) and next_offset.strip().isdigit():
        next_offset = int(next_offset.strip())
    if not isinstance(next_offset, int):
        next_offset = None
    if next_uri is not None:
        next_uri = str(next_uri).strip() or None
    return next_uri, next_offset


def fetch_pages(args: argparse.Namespace, api_key: str) -> tuple[list[dict[str, Any]], int]:
    """Fetch one or more result pages from Dune."""
    endpoint = f"{args.base_url.rstrip('/')}/query/{args.query_id}/results"
    headers = {"X-Dune-API-Key": api_key}
    pages: list[dict[str, Any]] = []
    total_rows = 0
    next_url: str | None = endpoint
    offset = 0

    with httpx.Client(headers=headers, timeout=args.timeout_seconds) as client:
        while next_url:
            request_params = None if next_url != endpoint else {"limit": args.page_size, "offset": offset}
            response = client.get(next_url, params=request_params)
            response.raise_for_status()
            payload = response.json()
            pages.append(payload)

            rows = extract_rows(payload)
            total_rows += len(rows)

            if args.max_pages and len(pages) >= args.max_pages:
                break

            next_uri, next_offset = _extract_next_reference(payload)
            if next_uri:
                next_url = next_uri if next_uri.startswith("http") else f"{args.base_url.rstrip('/')}/{next_uri.lstrip('/')}"
                continue
            if next_offset is not None and next_offset > offset:
                offset = next_offset
                next_url = endpoint
                continue
            if len(rows) < args.page_size:
                break
            offset += len(rows)
            next_url = endpoint

    return pages, total_rows


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    """Fetch and ingest one Dune result batch."""
    api_key = os.getenv(args.api_key_env, "").strip()
    if not api_key:
        raise RuntimeError(f"Missing {args.api_key_env} environment variable.")

    pages, total_rows = fetch_pages(args, api_key)
    request_url = f"{args.base_url.rstrip('/')}/query/{args.query_id}/results"
    with session_scope(args.database_url or None) as session:
        db_result = ingest_query_pages(
            session,
            query_id=args.query_id,
            pages=pages,
            request_url=request_url,
            raw_output_path=args.output_file,
        )

    summary = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "query_id": args.query_id,
        "page_count": len(pages),
        "row_count": total_rows,
        "db_ingest": db_result,
    }
    append_jsonl(Path(args.output_file), summary)
    return summary


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    request_count = 0

    while args.max_requests == 0 or request_count < args.max_requests:
        request_count += 1
        try:
            summary = run_once(args)
        except Exception as exc:
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
            return 1
        print(json.dumps(summary, indent=2))
        if args.max_requests and request_count >= args.max_requests:
            break
        time.sleep(args.interval_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
