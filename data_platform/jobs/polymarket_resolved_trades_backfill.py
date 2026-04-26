"""Backfill Polymarket trades for resolved closed markets."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.ingest.polymarket import ingest_discovery_cycle, ingest_trades_record
from data_platform.models import MarketContract, MarketEvent, Platform, TransactionFact


POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events"
POLYMARKET_TRADES_URL = "https://data-api.polymarket.com/trades"
RESOLUTION_PRICE_HIGH = 0.98
RESOLUTION_PRICE_LOW = 0.02


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Backfill Polymarket trades for resolved closed markets.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument(
        "--output-file",
        default="data_platform/runtime/polymarket_resolved_trades_backfill.jsonl",
        help="JSONL file for run summaries.",
    )
    parser.add_argument(
        "--market-limit",
        type=int,
        default=5,
        help="Number of resolved closed markets selected from the DB for trade backfill.",
    )
    parser.add_argument(
        "--only-uncovered",
        action="store_true",
        help="Restrict selection to deterministically resolved conditions with no ingested trades yet.",
    )
    parser.add_argument(
        "--trade-limit",
        type=int,
        default=200,
        help="Number of trades requested per API page.",
    )
    parser.add_argument(
        "--max-pages-per-market",
        type=int,
        default=5,
        help="Maximum trade pages requested per market. 0 means no explicit page cap.",
    )
    parser.add_argument(
        "--refresh-closed-events",
        action="store_true",
        help="Refresh a page of closed events from Gamma before selecting target markets.",
    )
    parser.add_argument(
        "--closed-event-limit",
        type=int,
        default=50,
        help="Closed-event page size used when --refresh-closed-events is enabled.",
    )
    parser.add_argument(
        "--closed-event-offset",
        type=int,
        default=0,
        help="Closed-event offset used when --refresh-closed-events is enabled.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-base-seconds", type=float, default=1.0)
    parser.add_argument("--backoff-cap-seconds", type=float, default=30.0)
    parser.add_argument("--per-request-delay-seconds", type=float, default=1.0)
    parser.add_argument(
        "--batch-count",
        type=int,
        default=1,
        help="Number of sequential backfill batches to run. Each batch reselects target markets.",
    )
    parser.add_argument(
        "--target-written-trades",
        type=int,
        default=0,
        help="Stop once cumulative written trades across batches reaches this value. 0 disables the target.",
    )
    parser.add_argument(
        "--target-markets-processed",
        type=int,
        default=0,
        help="Stop once cumulative processed markets across batches reaches this value. 0 disables the target.",
    )
    args = parser.parse_args()

    if args.market_limit <= 0:
        parser.error("--market-limit must be > 0.")
    if args.trade_limit <= 0:
        parser.error("--trade-limit must be > 0.")
    if args.max_pages_per_market < 0:
        parser.error("--max-pages-per-market must be >= 0.")
    if args.closed_event_limit <= 0:
        parser.error("--closed-event-limit must be > 0.")
    if args.closed_event_offset < 0:
        parser.error("--closed-event-offset must be >= 0.")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0.")
    if args.max_retries < 0:
        parser.error("--max-retries must be >= 0.")
    if args.per_request_delay_seconds < 0:
        parser.error("--per-request-delay-seconds must be >= 0.")
    if args.batch_count <= 0:
        parser.error("--batch-count must be > 0.")
    if args.target_written_trades < 0:
        parser.error("--target-written-trades must be >= 0.")
    if args.target_markets_processed < 0:
        parser.error("--target-markets-processed must be >= 0.")
    return args


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    """Append one JSONL record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str))
        handle.write("\n")


def compute_retry_delay(attempt: int, args: argparse.Namespace) -> float:
    """Compute capped exponential backoff delay."""
    return min(args.backoff_cap_seconds, args.backoff_base_seconds * (2**attempt))


def request_json(
    client: httpx.Client,
    *,
    url: str,
    args: argparse.Namespace,
    params: dict[str, Any],
) -> Any:
    """Fetch JSON with retry/backoff."""
    for attempt in range(args.max_retries + 1):
        try:
            response = client.get(url, params=params, timeout=args.timeout_seconds)
            response.raise_for_status()
            return response.json()
        except (httpx.RequestError, httpx.HTTPStatusError, ValueError):
            if attempt >= args.max_retries:
                raise
            time.sleep(compute_retry_delay(attempt, args))
    raise RuntimeError("Unreachable retry loop state.")


def refresh_closed_events(session: Session, client: httpx.Client, args: argparse.Namespace) -> dict[str, Any]:
    """Refresh one page of closed Polymarket events into the normalized DB."""
    params = {
        "closed": "true",
        "active": "false",
        "limit": args.closed_event_limit,
        "offset": args.closed_event_offset,
    }
    payload = request_json(client, url=POLYMARKET_EVENTS_URL, args=args, params=params)
    events = [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []
    cycle = {
        "scraped_at_unix": int(time.time()),
        "scraped_at_iso": datetime.now(timezone.utc).isoformat(),
        "discovered_count": len(events),
        "matched_count": len(events),
        "results_count": len(events),
        "errors_count": 0,
        "results": events,
        "errors": [],
    }
    request_url = f"{POLYMARKET_EVENTS_URL}?{urlencode(params)}"
    db_result = ingest_discovery_cycle(
        session,
        cycle=cycle,
        request_url=request_url,
        raw_output_path=args.output_file,
    )
    return {
        "requested": len(events),
        "db_ingest": db_result,
        "request_url": request_url,
    }


def load_target_markets(session: Session, market_limit: int, *, only_uncovered: bool = False) -> list[MarketContract]:
    """Return distinct deterministically resolved Polymarket markets ordered by recency and volume."""
    rows = session.scalars(
        select(MarketContract)
        .join(MarketEvent, MarketEvent.event_id == MarketContract.event_id)
        .join(Platform, Platform.platform_id == MarketContract.platform_id)
        .where(Platform.platform_name == "polymarket")
        .where(MarketContract.is_closed.is_(True))
        .where(MarketContract.condition_ref.is_not(None))
        .where(MarketContract.outcome_b_label.is_not(None))
        .where(MarketContract.last_trade_price.is_not(None))
        .where(
            (MarketContract.last_trade_price >= RESOLUTION_PRICE_HIGH)
            | (MarketContract.last_trade_price <= RESOLUTION_PRICE_LOW)
        )
        .order_by(
            desc(MarketEvent.closed_time),
            desc(MarketContract.volume),
            desc(MarketContract.market_contract_id),
        )
    ).all()

    covered_condition_refs: set[str] = set()
    if only_uncovered:
        covered_condition_refs = {
            str(value)
            for value in session.scalars(
                select(MarketContract.condition_ref)
                .join(TransactionFact, TransactionFact.market_contract_id == MarketContract.market_contract_id)
                .join(Platform, Platform.platform_id == TransactionFact.platform_id)
                .where(Platform.platform_name == "polymarket")
                .where(MarketContract.condition_ref.is_not(None))
                .distinct()
            ).all()
        }

    seen: set[str] = set()
    selected: list[MarketContract] = []
    for row in rows:
        condition_ref = str(row.condition_ref)
        if condition_ref in seen:
            continue
        if only_uncovered and condition_ref in covered_condition_refs:
            continue
        seen.add(condition_ref)
        selected.append(row)
        if len(selected) >= market_limit:
            break
    return selected


def fetch_trades_page(
    client: httpx.Client,
    *,
    condition_ref: str,
    offset: int,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    """Fetch one trade page for a resolved market condition id."""
    params = {
        "market": condition_ref,
        "limit": args.trade_limit,
        "offset": offset,
    }
    payload = request_json(client, url=POLYMARKET_TRADES_URL, args=args, params=params)
    if not isinstance(payload, list):
        raise ValueError("Unexpected Polymarket trades response shape; expected a list.")
    return [item for item in payload if isinstance(item, dict)]


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    """Run one resolved-market trade backfill cycle."""
    started_at = datetime.now(timezone.utc)
    output_path = Path(args.output_file)
    with httpx.Client() as client, session_scope(args.database_url or None) as session:
        refresh_summary: dict[str, Any] | None = None
        if args.refresh_closed_events:
            refresh_summary = refresh_closed_events(session, client, args)

        markets = load_target_markets(session, args.market_limit, only_uncovered=args.only_uncovered)
        market_summaries: list[dict[str, Any]] = []
        total_pages = 0
        total_fetched_trades = 0
        total_written_trades = 0

        for market in markets:
            condition_ref = str(market.condition_ref)
            market_pages = 0
            market_fetched = 0
            market_written = 0
            offset = 0

            while True:
                trades = fetch_trades_page(client, condition_ref=condition_ref, offset=offset, args=args)
                batch_time = datetime.now(timezone.utc)
                record = {
                    "scraped_at_unix": int(batch_time.timestamp()),
                    "scraped_at_iso": batch_time.isoformat(),
                    "count": len(trades),
                    "query": {
                        "market": condition_ref,
                        "limit": args.trade_limit,
                        "offset": offset,
                    },
                    "trades": trades,
                }
                request_url = f"{POLYMARKET_TRADES_URL}?{urlencode(record['query'])}"
                db_result = ingest_trades_record(
                    session,
                    record=record,
                    request_url=request_url,
                    raw_output_path=args.output_file,
                )
                market_pages += 1
                total_pages += 1
                market_fetched += len(trades)
                total_fetched_trades += len(trades)
                market_written += int(db_result["records_written"])
                total_written_trades += int(db_result["records_written"])

                if len(trades) < args.trade_limit:
                    break
                if args.max_pages_per_market and market_pages >= args.max_pages_per_market:
                    break
                offset += args.trade_limit
                if args.per_request_delay_seconds > 0:
                    time.sleep(args.per_request_delay_seconds)

            market_summaries.append(
                {
                    "market_contract_id": market.market_contract_id,
                    "market_slug": market.market_slug,
                    "condition_ref": condition_ref,
                    "pages": market_pages,
                    "fetched_trades": market_fetched,
                    "written_trades": market_written,
                }
            )
            if args.per_request_delay_seconds > 0:
                time.sleep(args.per_request_delay_seconds)

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "refresh_closed_events": refresh_summary,
        "market_count": len(market_summaries),
        "page_count": total_pages,
        "fetched_trades": total_fetched_trades,
        "written_trades": total_written_trades,
        "markets": market_summaries,
    }
    append_jsonl(output_path, summary)
    return summary


def run_batches(args: argparse.Namespace) -> dict[str, Any]:
    """Run sequential backfill batches until a bound or target is hit."""
    started_at = datetime.now(timezone.utc)
    output_path = Path(args.output_file)
    batch_summaries: list[dict[str, Any]] = []
    total_pages = 0
    total_fetched_trades = 0
    total_written_trades = 0
    total_markets = 0
    stop_reason = "batch_limit_reached"

    for batch_index in range(1, args.batch_count + 1):
        batch_summary = run_once(args)
        batch_summary["batch_index"] = batch_index
        batch_summaries.append(batch_summary)
        total_pages += int(batch_summary["page_count"])
        total_fetched_trades += int(batch_summary["fetched_trades"])
        total_written_trades += int(batch_summary["written_trades"])
        total_markets += int(batch_summary["market_count"])

        if int(batch_summary["market_count"]) == 0:
            stop_reason = "no_markets_selected"
            break
        if int(batch_summary["written_trades"]) == 0:
            stop_reason = "no_new_trades_written"
            break
        if args.target_written_trades and total_written_trades >= args.target_written_trades:
            stop_reason = "target_written_trades_reached"
            break
        if args.target_markets_processed and total_markets >= args.target_markets_processed:
            stop_reason = "target_markets_processed_reached"
            break

    overall_summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "batch_count_requested": args.batch_count,
        "batch_count_completed": len(batch_summaries),
        "target_written_trades": args.target_written_trades,
        "target_markets_processed": args.target_markets_processed,
        "market_count": total_markets,
        "page_count": total_pages,
        "fetched_trades": total_fetched_trades,
        "written_trades": total_written_trades,
        "stop_reason": stop_reason,
        "batches": batch_summaries,
    }
    append_jsonl(output_path, {"batch_run_summary": overall_summary})
    return overall_summary


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    summary = (
        run_batches(args)
        if args.batch_count > 1 or args.target_written_trades > 0 or args.target_markets_processed > 0
        else run_once(args)
    )
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
