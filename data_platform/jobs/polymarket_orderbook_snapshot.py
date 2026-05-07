"""Capture Polymarket order-book snapshots for top tracked markets.

This job derives CLOB token ids from the latest stored raw Polymarket event payloads,
fetches aggregated order-book summaries from the public CLOB API, and persists one
normalized market-level snapshot per market into PostgreSQL.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
import sys

import httpx
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.ingest.polymarket import ingest_orderbook_batch
from data_platform.ingest.store import parse_datetime
from data_platform.models import (
    ApiPayload,
    Dashboard,
    DashboardMarket,
    MarketContract,
    MarketEvent,
    MarketTag,
    MarketTagMap,
    Platform,
    TransactionFact,
)
from data_platform.services.market_scope import (
    add_focus_domain_argument,
    build_market_scope_texts,
    canonicalize_focus_domains,
    matches_focus_domains,
)

CLOB_BOOK_URL = "https://clob.polymarket.com/book"
ZERO = Decimal("0")
MIN_SANE_SNAPSHOT_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)
MAX_SANE_SNAPSHOT_FUTURE = timedelta(days=1)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Capture Polymarket order-book snapshots into PostgreSQL.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument("--market-limit", type=int, default=10, help="Number of top Polymarket markets to sample.")
    add_focus_domain_argument(parser)
    parser.add_argument(
        "--output-file",
        default="polymarket_data/orderbook_snapshots.jsonl",
        help="Optional JSONL file for archived snapshot summaries.",
    )
    parser.add_argument("--interval-seconds", type=float, default=60.0, help="Sleep between cycles when looping.")
    parser.add_argument("--max-requests", type=int, default=1, help="Number of cycles to run. 0 means forever.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-base-seconds", type=float, default=1.0)
    parser.add_argument("--backoff-cap-seconds", type=float, default=30.0)
    args = parser.parse_args()

    if args.market_limit <= 0:
        parser.error("--market-limit must be > 0.")
    if args.interval_seconds < 0:
        parser.error("--interval-seconds must be >= 0.")
    if args.max_requests < 0:
        parser.error("--max-requests must be >= 0.")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0.")
    if args.max_retries < 0:
        parser.error("--max-retries must be >= 0.")
    try:
        args.focus_domains = canonicalize_focus_domains(args.focus_domain)
    except ValueError as exc:
        parser.error(str(exc))

    return args



def _parse_token_ids(raw_value: Any) -> list[str]:
    """Parse token ids from a CLOB token list field."""
    if raw_value in (None, ""):
        return []
    values: list[Any]
    if isinstance(raw_value, list):
        values = raw_value
    else:
        try:
            decoded = json.loads(str(raw_value))
            values = decoded if isinstance(decoded, list) else [raw_value]
        except (TypeError, ValueError, json.JSONDecodeError):
            values = [raw_value]
    return [str(item).strip() for item in values if str(item).strip()]



def _market_matches_payload(market: MarketContract, payload: dict[str, Any]) -> bool:
    """Return whether a raw market payload corresponds to the normalized market row."""
    candidates = {
        str(market.external_market_ref).strip(),
        str(market.condition_ref).strip() if market.condition_ref else "",
        str(market.market_slug).strip() if market.market_slug else "",
    }
    payload_candidates = {
        str(payload.get("id")).strip() if payload.get("id") is not None else "",
        str(payload.get("conditionId")).strip() if payload.get("conditionId") is not None else "",
        str(payload.get("slug")).strip() if payload.get("slug") is not None else "",
    }
    candidates.discard("")
    payload_candidates.discard("")
    return bool(candidates & payload_candidates)



def _resolve_market_tokens(session: Session, market: MarketContract) -> list[str]:
    """Resolve the CLOB token ids for a market from its stored raw event payload."""
    event = session.get(MarketEvent, market.event_id)
    if event is None or event.raw_payload_id is None:
        return _fallback_market_token_ids(market)
    payload_row = session.get(ApiPayload, event.raw_payload_id)
    if payload_row is None or not isinstance(payload_row.payload, dict):
        return _fallback_market_token_ids(market)

    markets_payload = payload_row.payload.get("markets")
    if not isinstance(markets_payload, list):
        return _fallback_market_token_ids(market)

    fallback_tokens: list[str] = []
    if len(markets_payload) == 1 and isinstance(markets_payload[0], dict):
        fallback_tokens = _parse_token_ids(markets_payload[0].get("clobTokenIds"))

    for market_payload in markets_payload:
        if not isinstance(market_payload, dict):
            continue
        if _market_matches_payload(market, market_payload):
            return _parse_token_ids(market_payload.get("clobTokenIds"))
    return fallback_tokens or _fallback_market_token_ids(market)


def _fallback_market_token_ids(market: MarketContract) -> list[str]:
    """Return contract-level CLOB token ids when market metadata payloads are unavailable."""
    token = str(market.external_market_ref or "").strip()
    return [token] if token.isdigit() else []



def _load_event_tags(session: Session, event_ids: set[int]) -> dict[int, list[str]]:
    if not event_ids:
        return {}
    rows = session.execute(
        select(MarketTagMap.event_id, MarketTag.tag_label, MarketTag.tag_slug)
        .join(MarketTag, MarketTag.tag_id == MarketTagMap.tag_id)
        .where(MarketTagMap.event_id.in_(event_ids))
    ).all()
    grouped: dict[int, list[str]] = {}
    for event_id, tag_label, tag_slug in rows:
        grouped.setdefault(int(event_id), []).extend(
            [value for value in (tag_label, tag_slug) if isinstance(value, str) and value.strip()]
        )
    return grouped


def _load_target_markets(session: Session, market_limit: int, *, focus_domains: list[str]) -> list[tuple[MarketContract, list[str]]]:
    """Return top Polymarket markets that have resolvable CLOB token ids."""
    observed_volume = (
        select(func.coalesce(func.sum(TransactionFact.notional_value), 0))
        .where(TransactionFact.market_contract_id == MarketContract.market_contract_id)
        .correlate(MarketContract)
        .scalar_subquery()
    )

    latest_dashboard = session.scalars(select(Dashboard).order_by(desc(Dashboard.generated_at)).limit(1)).first()
    dashboard_rows = []
    if latest_dashboard is not None:
        dashboard_rows = session.execute(
            select(MarketContract, MarketEvent)
            .join(Platform, Platform.platform_id == MarketContract.platform_id)
            .join(MarketEvent, MarketEvent.event_id == MarketContract.event_id)
            .join(DashboardMarket, DashboardMarket.market_contract_id == MarketContract.market_contract_id)
            .where(DashboardMarket.dashboard_id == latest_dashboard.dashboard_id)
            .where(Platform.platform_name == "polymarket")
            .where(MarketContract.is_active.is_(True))
            .where(MarketContract.is_closed.is_(False))
            .order_by(
                desc(DashboardMarket.trusted_whale_count),
                desc(DashboardMarket.whale_count),
                desc(func.coalesce(DashboardMarket.volume, MarketContract.volume, observed_volume)),
                DashboardMarket.market_id.asc(),
            )
            .limit(market_limit)
        ).all()

    volume_rows = session.execute(
        select(MarketContract, MarketEvent)
        .join(Platform, Platform.platform_id == MarketContract.platform_id)
        .join(MarketEvent, MarketEvent.event_id == MarketContract.event_id)
        .where(Platform.platform_name == "polymarket")
        .where(MarketContract.is_active.is_(True))
        .where(MarketContract.is_closed.is_(False))
        .where((MarketContract.volume.is_not(None)) | (observed_volume > 0))
        .order_by(
            desc(func.coalesce(MarketContract.volume, observed_volume)),
            desc(MarketContract.updated_at),
        )
        .limit(market_limit * 12)
    ).all()
    rows = [*dashboard_rows, *volume_rows]

    event_tags = _load_event_tags(session, {event.event_id for _, event in rows})
    selected: list[tuple[MarketContract, list[str]]] = []
    seen_market_ids: set[int] = set()
    for market, event in rows:
        if market.market_contract_id in seen_market_ids:
            continue
        if focus_domains and not matches_focus_domains(
            build_market_scope_texts(
                platform_name="polymarket",
                event=event,
                market=market,
                tags=event_tags.get(event.event_id, []),
            ),
            focus_domains,
        ):
            continue
        token_ids = _resolve_market_tokens(session, market)
        if not token_ids:
            continue
        selected.append((market, token_ids))
        seen_market_ids.add(market.market_contract_id)
        if len(selected) >= market_limit:
            break
    return selected



def _decimal_field(value: Any) -> Decimal | None:
    """Parse a decimal-compatible numeric field."""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None



def _parse_retry_after(value: str | None) -> float | None:
    """Return a Retry-After delay in seconds when the header is usable."""
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return max(float(text), 0.0)
    except ValueError:
        parsed = parse_datetime(text)
        if parsed is None:
            return None
        return max((parsed - datetime.now(timezone.utc)).total_seconds(), 0.0)


def _is_sane_snapshot_time(value: datetime) -> bool:
    """Return whether an API timestamp can be used for partitioned raw storage."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return MIN_SANE_SNAPSHOT_TIME <= value <= now + MAX_SANE_SNAPSHOT_FUTURE


def _parse_clob_timestamp(value: Any) -> datetime | None:
    """Parse CLOB timestamps, treating numeric strings as Unix seconds/ms."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        parsed = parse_datetime(value)
        return parsed if parsed is not None and _is_sane_snapshot_time(parsed) else None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        for divisor in (1, 1000):
            try:
                parsed = datetime.fromtimestamp(int(text) / divisor, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                continue
            if _is_sane_snapshot_time(parsed):
                return parsed
        return None
    parsed = parse_datetime(text)
    return parsed if parsed is not None and _is_sane_snapshot_time(parsed) else None


def _aggregate_book_payload(market: MarketContract, token_ids: list[str], books: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate token-level books into one market-level snapshot payload.

    The normalized columns are derived from the first token's order book only.
    Binary Polymarket markets expose complementary YES/NO token books, so
    combining their prices directly produces invalid spreads. Both token books
    are still retained in the raw payload for later side-aware analysis.
    """
    primary_token_id = token_ids[0]
    primary_book = next(
        (book for book in books if str(book.get("asset_id") or "").strip() == primary_token_id),
        books[0],
    )
    primary_bids: list[tuple[Decimal, Decimal]] = []
    primary_asks: list[tuple[Decimal, Decimal]] = []
    snapshot_times: list[datetime] = []

    for book in books:
        book_time = _parse_clob_timestamp(book.get("timestamp"))
        if book_time is not None:
            snapshot_times.append(book_time)
    for level in primary_book.get("bids", []) if isinstance(primary_book.get("bids"), list) else []:
        if not isinstance(level, dict):
            continue
        price = _decimal_field(level.get("price"))
        size = _decimal_field(level.get("size"))
        if price is None or size is None:
            continue
        primary_bids.append((price, size))
    for level in primary_book.get("asks", []) if isinstance(primary_book.get("asks"), list) else []:
        if not isinstance(level, dict):
            continue
        price = _decimal_field(level.get("price"))
        size = _decimal_field(level.get("size"))
        if price is None or size is None:
            continue
        primary_asks.append((price, size))

    best_bid = max((price for price, _ in primary_bids), default=None)
    best_ask = min((price for price, _ in primary_asks), default=None)
    mid_price = None
    if best_bid is not None and best_ask is not None:
        mid_price = (best_bid + best_ask) / Decimal("2")
    elif best_bid is not None:
        mid_price = best_bid
    elif best_ask is not None:
        mid_price = best_ask
    spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
    bid_depth_notional = sum((price * size for price, size in primary_bids), start=ZERO)
    ask_depth_notional = sum((price * size for price, size in primary_asks), start=ZERO)
    snapshot_time = max(snapshot_times) if snapshot_times else datetime.now(timezone.utc)

    return {
        "market": market,
        "snapshot_time": snapshot_time,
        "depth_levels": len(primary_bids) + len(primary_asks),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid_price,
        "spread": spread,
        "bid_depth_notional": bid_depth_notional,
        "ask_depth_notional": ask_depth_notional,
        "raw_payload": {
            "market_contract_id": market.market_contract_id,
            "market_slug": market.market_slug,
            "primary_token_id": primary_token_id,
            "token_ids": token_ids,
            "books": books,
            "aggregated": {
                "depth_levels": len(primary_bids) + len(primary_asks),
                "best_bid": str(best_bid) if best_bid is not None else None,
                "best_ask": str(best_ask) if best_ask is not None else None,
                "mid_price": str(mid_price) if mid_price is not None else None,
                "spread": str(spread) if spread is not None else None,
                "bid_depth_notional": str(bid_depth_notional),
                "ask_depth_notional": str(ask_depth_notional),
            },
        },
    }



def compute_retry_delay(attempt: int, args: argparse.Namespace) -> float:
    """Compute capped exponential backoff."""
    return min(args.backoff_cap_seconds, args.backoff_base_seconds * (2 ** attempt))


def _http_retry_delay(exc: Exception, attempt: int, args: argparse.Namespace) -> float:
    """Return Retry-After when present, otherwise exponential backoff."""
    delay = compute_retry_delay(attempt, args)
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        retry_after = _parse_retry_after(exc.response.headers.get("Retry-After"))
        if retry_after is not None:
            delay = max(delay, min(retry_after, args.backoff_cap_seconds))
    return delay



def fetch_books(client: httpx.Client, token_ids: list[str], args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    """Fetch order-book summaries for the requested token ids."""
    books: dict[str, dict[str, Any]] = {}
    for token_id in token_ids:
        for attempt in range(args.max_retries + 1):
            try:
                response = client.get(
                    CLOB_BOOK_URL,
                    params={"token_id": token_id},
                    timeout=args.timeout_seconds,
                )
                if response.status_code == 404:
                    break
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict):
                    books[token_id] = payload
                break
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
                    if exc.response.status_code not in RETRYABLE_STATUS_CODES:
                        raise
                if attempt >= args.max_retries:
                    raise
                time.sleep(_http_retry_delay(exc, attempt, args))
    return books



def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    """Append one JSON line to the output file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str))
        handle.write("\n")



def run_once(args: argparse.Namespace) -> dict[str, Any]:
    """Run one order-book snapshot cycle."""
    with session_scope(args.database_url or None) as session:
        target_markets = _load_target_markets(session, args.market_limit, focus_domains=args.focus_domains)
        if not target_markets:
            summary = {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "market_count": 0,
                "token_count": 0,
                "db_ingest": {"records_written": 0, "error_count": 1},
                "warning": "No Polymarket markets with resolvable CLOB token ids were found.",
            }
            append_jsonl(Path(args.output_file), summary)
            return summary

        token_to_books: dict[str, dict[str, Any]] = {}
        request_token_ids = sorted({token_id for _, token_ids in target_markets for token_id in token_ids})
        with httpx.Client() as client:
            token_to_books = fetch_books(client, request_token_ids, args)

        aggregated_books: list[dict[str, Any]] = []
        missing_market_count = 0
        for market, token_ids in target_markets:
            matched_books = [token_to_books[token_id] for token_id in token_ids if token_id in token_to_books]
            if not matched_books:
                missing_market_count += 1
                continue
            aggregated_books.append(_aggregate_book_payload(market, token_ids, matched_books))

        request_url = f"{CLOB_BOOK_URL} (tokens={len(request_token_ids)})"
        db_result = ingest_orderbook_batch(
            session,
            books=aggregated_books,
            request_url=request_url,
            raw_output_path=args.output_file,
        )
        summary = {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "market_count": len(aggregated_books),
            "token_count": len(request_token_ids),
            "missing_market_count": missing_market_count,
            "markets": [
                {
                    "market_contract_id": item["market"].market_contract_id,
                    "market_slug": item["market"].market_slug,
                    "depth_levels": item["depth_levels"],
                    "best_bid": str(item["best_bid"]) if item["best_bid"] is not None else None,
                    "best_ask": str(item["best_ask"]) if item["best_ask"] is not None else None,
                }
                for item in aggregated_books
            ],
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
