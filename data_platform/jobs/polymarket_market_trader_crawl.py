"""Broad public Polymarket crawler for markets and traders.

This job is meant for database population, not wallet tracking. It:
1. Optionally refreshes active Polymarket events into the normalized market tables.
2. Pages the latest global public trades feed.
3. Pages recent trades for configurable sets of active and recently closed markets.
4. Relies on trade payload wallet references to populate ``analytics.user_account``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
RUNTIME_DIR = ROOT_DIR / "data_platform" / "runtime"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.ingest.polymarket import ingest_discovery_cycle, ingest_trades_record
from data_platform.models import MarketContract, MarketEvent, Platform


POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events"
POLYMARKET_EVENT_BY_ID_URL = "https://gamma-api.polymarket.com/events/{event_id}"
POLYMARKET_TRADES_URL = "https://data-api.polymarket.com/trades"


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Broad Polymarket market/trader crawler that fills markets and users from public flow."
    )
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument(
        "--output-file",
        default=str(RUNTIME_DIR / "polymarket_market_trader_crawl.jsonl"),
        help="JSONL file for crawl summaries.",
    )
    parser.add_argument(
        "--skip-refresh-active-events",
        action="store_true",
        help="Skip refreshing active events from Gamma before selecting target markets.",
    )
    parser.add_argument(
        "--refresh-event-limit",
        type=int,
        default=25,
        help="Number of active events requested from Gamma when refresh is enabled.",
    )
    parser.add_argument(
        "--refresh-event-offset",
        type=int,
        default=0,
        help="Offset for the active Gamma event refresh.",
    )
    parser.add_argument(
        "--fetch-full-details",
        action="store_true",
        help="When refreshing active events, fetch /events/{id} for canonical event detail.",
    )
    parser.add_argument(
        "--global-pages",
        type=int,
        default=1,
        help="Number of latest global public trade pages to ingest before market-specific crawl.",
    )
    parser.add_argument(
        "--market-limit",
        type=int,
        default=20,
        help="Number of active Polymarket markets selected from the DB for market-specific trade paging.",
    )
    parser.add_argument(
        "--closed-market-limit",
        type=int,
        default=10,
        help="Number of recently closed Polymarket markets selected from the DB for trade paging. 0 disables closed-market crawl.",
    )
    parser.add_argument(
        "--closed-within-hours",
        type=float,
        default=None,
        help="Only include closed markets with closed_time within this many hours.",
    )
    parser.add_argument(
        "--closed-within-days",
        type=float,
        default=None,
        help="Only include closed markets with closed_time within this many days.",
    )
    parser.add_argument(
        "--trade-limit",
        type=int,
        default=200,
        help="Number of trades requested per page from the Polymarket trades endpoint.",
    )
    parser.add_argument(
        "--max-pages-per-market",
        type=int,
        default=2,
        help="Maximum market-specific trade pages requested per market. 0 means unlimited.",
    )
    parser.add_argument(
        "--max-total-trade-pages",
        type=int,
        default=0,
        help="Maximum total trade pages fetched in one crawl cycle across global and market-specific requests. 0 means unlimited.",
    )
    parser.add_argument(
        "--per-request-delay-seconds",
        type=float,
        default=0.5,
        help="Delay between trade page requests to avoid tight request bursts.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-base-seconds", type=float, default=1.0)
    parser.add_argument("--backoff-cap-seconds", type=float, default=30.0)
    args = parser.parse_args()

    if args.refresh_event_limit <= 0:
        parser.error("--refresh-event-limit must be > 0.")
    if args.refresh_event_offset < 0:
        parser.error("--refresh-event-offset must be >= 0.")
    if args.global_pages < 0:
        parser.error("--global-pages must be >= 0.")
    if args.market_limit <= 0:
        parser.error("--market-limit must be > 0.")
    if args.closed_market_limit < 0:
        parser.error("--closed-market-limit must be >= 0.")
    if args.closed_within_hours is not None and args.closed_within_days is not None:
        parser.error("Use only one of --closed-within-hours or --closed-within-days.")
    if args.closed_within_hours is not None and args.closed_within_hours <= 0:
        parser.error("--closed-within-hours must be > 0.")
    if args.closed_within_days is not None and args.closed_within_days <= 0:
        parser.error("--closed-within-days must be > 0.")
    if args.trade_limit <= 0:
        parser.error("--trade-limit must be > 0.")
    if args.max_pages_per_market < 0:
        parser.error("--max-pages-per-market must be >= 0.")
    if args.max_total_trade_pages < 0:
        parser.error("--max-total-trade-pages must be >= 0.")
    if args.per_request_delay_seconds < 0:
        parser.error("--per-request-delay-seconds must be >= 0.")
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
    return min(args.backoff_cap_seconds, args.backoff_base_seconds * (2**attempt))


def request_json(
    client: httpx.Client,
    *,
    url: str,
    args: argparse.Namespace,
    params: dict[str, Any] | None = None,
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


def refresh_active_events(session: Session, client: httpx.Client, args: argparse.Namespace) -> dict[str, Any]:
    """Refresh one page of active Polymarket events into the normalized DB."""
    params = {
        "active": "true",
        "closed": "false",
        "limit": args.refresh_event_limit,
        "offset": args.refresh_event_offset,
    }
    payload = request_json(client, url=POLYMARKET_EVENTS_URL, args=args, params=params)
    discovered = [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []
    if args.fetch_full_details:
        detailed: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for item in discovered:
            event_id = item.get("id")
            if event_id is None:
                errors.append({"event_id": "", "error": "Missing event id in active event payload."})
                continue
            try:
                detail = request_json(
                    client,
                    url=POLYMARKET_EVENT_BY_ID_URL.format(event_id=event_id),
                    args=args,
                )
                if isinstance(detail, dict):
                    detailed.append(detail)
                else:
                    errors.append({"event_id": str(event_id), "error": "Unexpected /events/{id} response shape."})
            except Exception as exc:  # pragma: no cover - runtime isolation
                errors.append({"event_id": str(event_id), "error": str(exc)})
            if args.per_request_delay_seconds > 0:
                time.sleep(args.per_request_delay_seconds)
        results = detailed
    else:
        results = discovered
        errors = []

    cycle = {
        "scraped_at_unix": int(time.time()),
        "scraped_at_iso": datetime.now(timezone.utc).isoformat(),
        "discovered_count": len(discovered),
        "matched_count": len(results),
        "results_count": len(results),
        "errors_count": len(errors),
        "results": results,
        "errors": errors,
    }
    request_url = f"{POLYMARKET_EVENTS_URL}?{urlencode(params)}"
    db_result = ingest_discovery_cycle(
        session,
        cycle=cycle,
        request_url=request_url,
        raw_output_path=args.output_file,
    )
    return {
        "requested": len(discovered),
        "detailed": len(results),
        "errors": len(errors),
        "db_ingest": db_result,
        "request_url": request_url,
    }


def load_active_target_markets(session: Session, market_limit: int) -> list[MarketContract]:
    """Return high-volume active Polymarket markets that can be crawled by condition id."""
    rows = session.scalars(
        select(MarketContract)
        .join(MarketEvent, MarketEvent.event_id == MarketContract.event_id)
        .join(Platform, Platform.platform_id == MarketContract.platform_id)
        .where(Platform.platform_name == "polymarket")
        .where(MarketContract.is_active.is_(True))
        .where(MarketContract.is_closed.is_(False))
        .where(MarketContract.condition_ref.is_not(None))
        .order_by(
            desc(MarketContract.volume),
            desc(MarketEvent.volume),
            desc(MarketContract.updated_at),
        )
        .limit(market_limit * 4)
    ).all()
    selected: list[MarketContract] = []
    seen_condition_refs: set[str] = set()
    for row in rows:
        condition_ref = str(row.condition_ref or "").strip()
        if not condition_ref or condition_ref in seen_condition_refs:
            continue
        seen_condition_refs.add(condition_ref)
        selected.append(row)
        if len(selected) >= market_limit:
            break
    return selected


def load_recent_closed_markets(
    session: Session,
    market_limit: int,
    *,
    closed_after: datetime | None = None,
) -> list[MarketContract]:
    """Return recently closed Polymarket markets that can be crawled by condition id."""
    if market_limit == 0:
        return []
    query = (
        select(MarketContract)
        .join(MarketEvent, MarketEvent.event_id == MarketContract.event_id)
        .join(Platform, Platform.platform_id == MarketContract.platform_id)
        .where(Platform.platform_name == "polymarket")
        .where(MarketContract.is_closed.is_(True))
        .where(MarketContract.condition_ref.is_not(None))
        .where(MarketEvent.closed_time.is_not(None))
        .order_by(
            desc(MarketEvent.closed_time),
            desc(MarketContract.volume),
            desc(MarketContract.updated_at),
        )
        .limit(market_limit * 4)
    )
    if closed_after is not None:
        query = query.where(MarketEvent.closed_time >= closed_after)
    rows = session.scalars(query).all()
    selected: list[MarketContract] = []
    seen_condition_refs: set[str] = set()
    for row in rows:
        condition_ref = str(row.condition_ref or "").strip()
        if not condition_ref or condition_ref in seen_condition_refs:
            continue
        seen_condition_refs.add(condition_ref)
        selected.append(row)
        if len(selected) >= market_limit:
            break
    return selected


def fetch_trades_page(
    client: httpx.Client,
    *,
    args: argparse.Namespace,
    offset: int,
    condition_ref: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch one public Polymarket trades page."""
    params: dict[str, Any] = {
        "limit": args.trade_limit,
        "offset": offset,
    }
    if condition_ref:
        params["market"] = condition_ref
    payload = request_json(client, url=POLYMARKET_TRADES_URL, args=args, params=params)
    if not isinstance(payload, list):
        raise ValueError("Unexpected Polymarket trades response shape; expected a list.")
    return [item for item in payload if isinstance(item, dict)]


def ingest_trade_batch(
    session: Session,
    *,
    args: argparse.Namespace,
    trades: list[dict[str, Any]],
    query: dict[str, Any],
) -> dict[str, Any]:
    """Persist one fetched trade page and return the DB summary."""
    batch_time = datetime.now(timezone.utc)
    record = {
        "scraped_at_unix": int(batch_time.timestamp()),
        "scraped_at_iso": batch_time.isoformat(),
        "count": len(trades),
        "query": query,
        "trades": trades,
    }
    request_url = f"{POLYMARKET_TRADES_URL}?{urlencode(query)}"
    return ingest_trades_record(
        session,
        record=record,
        request_url=request_url,
        raw_output_path=args.output_file,
    )


def trade_page_budget_reached(args: argparse.Namespace, total_pages: int) -> bool:
    """Return whether the crawl has already used its per-cycle trade page budget."""
    return args.max_total_trade_pages > 0 and total_pages >= args.max_total_trade_pages


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    """Run one broad market/trader crawl cycle."""
    started_at = datetime.now(timezone.utc)
    output_path = Path(args.output_file)
    closed_after: datetime | None = None
    if args.closed_within_hours is not None:
        closed_after = started_at - timedelta(hours=args.closed_within_hours)
    elif args.closed_within_days is not None:
        closed_after = started_at - timedelta(days=args.closed_within_days)

    with httpx.Client() as client, session_scope(args.database_url or None) as session:
        refresh_summary: dict[str, Any] | None = None
        if not args.skip_refresh_active_events:
            refresh_summary = refresh_active_events(session, client, args)

        active_markets = load_active_target_markets(session, args.market_limit)
        closed_markets = load_recent_closed_markets(session, args.closed_market_limit, closed_after=closed_after)
        target_markets: list[tuple[str, MarketContract]] = []
        seen_condition_refs: set[str] = set()
        for market in active_markets:
            condition_ref = str(market.condition_ref or "").strip()
            if not condition_ref or condition_ref in seen_condition_refs:
                continue
            seen_condition_refs.add(condition_ref)
            target_markets.append(("active", market))
        for market in closed_markets:
            condition_ref = str(market.condition_ref or "").strip()
            if not condition_ref or condition_ref in seen_condition_refs:
                continue
            seen_condition_refs.add(condition_ref)
            target_markets.append(("closed", market))
        market_summaries: list[dict[str, Any]] = []
        global_summaries: list[dict[str, Any]] = []
        total_pages = 0
        total_fetched_trades = 0
        total_written_trades = 0
        trade_page_budget_hit = False

        for page_index in range(args.global_pages):
            if trade_page_budget_reached(args, total_pages):
                trade_page_budget_hit = True
                break
            offset = page_index * args.trade_limit
            trades = fetch_trades_page(client, args=args, offset=offset)
            query = {"limit": args.trade_limit, "offset": offset}
            db_result = ingest_trade_batch(session, args=args, trades=trades, query=query)
            global_summaries.append(
                {
                    "page": page_index + 1,
                    "offset": offset,
                    "fetched_trades": len(trades),
                    "written_trades": int(db_result["records_written"]),
                }
            )
            total_pages += 1
            total_fetched_trades += len(trades)
            total_written_trades += int(db_result["records_written"])
            if len(trades) < args.trade_limit:
                break
            if args.per_request_delay_seconds > 0:
                time.sleep(args.per_request_delay_seconds)

        for market_state, market in target_markets:
            if trade_page_budget_reached(args, total_pages):
                trade_page_budget_hit = True
                break
            condition_ref = str(market.condition_ref or "").strip()
            if not condition_ref:
                continue

            market_pages = 0
            market_fetched = 0
            market_written = 0
            offset = 0
            while True:
                if trade_page_budget_reached(args, total_pages):
                    trade_page_budget_hit = True
                    break
                trades = fetch_trades_page(client, args=args, offset=offset, condition_ref=condition_ref)
                query = {"market": condition_ref, "limit": args.trade_limit, "offset": offset}
                db_result = ingest_trade_batch(session, args=args, trades=trades, query=query)
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
                    "state": market_state,
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
        "refresh_active_events": refresh_summary,
        "closed_after": closed_after.isoformat() if closed_after is not None else None,
        "global_pages": global_summaries,
        "max_total_trade_pages": args.max_total_trade_pages,
        "trade_page_budget_hit": trade_page_budget_hit,
        "active_market_count": sum(1 for item in market_summaries if item["state"] == "active"),
        "closed_market_count": sum(1 for item in market_summaries if item["state"] == "closed"),
        "market_count": len(market_summaries),
        "page_count": total_pages,
        "fetched_trades": total_fetched_trades,
        "written_trades": total_written_trades,
        "markets": market_summaries,
    }
    append_jsonl(output_path, summary)
    return summary


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    summary = run_once(args)
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
