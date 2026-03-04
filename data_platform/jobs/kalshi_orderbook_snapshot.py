"""Capture Kalshi order-book snapshots for top tracked markets."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
KALSHI_DIR = ROOT_DIR / "kalshi-scraper"
if str(KALSHI_DIR) not in sys.path:
    sys.path.insert(0, str(KALSHI_DIR))

from clients import Environment, KalshiHttpClient
from data_platform.db.session import session_scope
from data_platform.ingest.kalshi import ingest_orderbook_batch
from data_platform.ingest.store import parse_datetime
from data_platform.models import MarketContract, Platform

ZERO = Decimal("0")
DEFAULT_ENVIRONMENT = "prod"


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Capture Kalshi order-book snapshots into PostgreSQL.")
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument("--environment", choices=["demo", "prod"], default=DEFAULT_ENVIRONMENT)
    parser.add_argument("--market-limit", type=int, default=10, help="Number of top Kalshi markets to sample.")
    parser.add_argument(
        "--output-file",
        default="kalshi-scraper/kalshi_data/orderbook_snapshots.jsonl",
        help="Optional JSONL archive path for snapshot summaries.",
    )
    parser.add_argument("--interval-seconds", type=float, default=60.0, help="Sleep between cycles when looping.")
    parser.add_argument("--max-requests", type=int, default=1, help="Number of cycles to run. 0 means forever.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    args = parser.parse_args()

    if args.market_limit <= 0:
        parser.error("--market-limit must be > 0.")
    if args.interval_seconds < 0:
        parser.error("--interval-seconds must be >= 0.")
    if args.max_requests < 0:
        parser.error("--max-requests must be >= 0.")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be > 0.")
    return args



def _load_client(environment_name: str) -> KalshiHttpClient:
    """Load Kalshi credentials and return an authenticated client."""
    load_dotenv(KALSHI_DIR / ".env")
    environment = Environment.DEMO if environment_name == "demo" else Environment.PROD
    key_id_env = "DEMO_KEYID" if environment is Environment.DEMO else "PROD_KEYID"
    key_file_env = "DEMO_KEYFILE" if environment is Environment.DEMO else "PROD_KEYFILE"
    key_id = os.getenv(key_id_env, "").strip()
    key_file = os.getenv(key_file_env, "").strip()
    if not key_id:
        raise RuntimeError(f"Missing {key_id_env} for Kalshi orderbook job.")
    if not key_file:
        raise RuntimeError(f"Missing {key_file_env} for Kalshi orderbook job.")
    with open(key_file, "rb") as private_key_file:
        private_key = serialization.load_pem_private_key(private_key_file.read(), password=None)
    return KalshiHttpClient(key_id=key_id, private_key=private_key, environment=environment)



def _load_target_markets(session: Session, market_limit: int) -> list[MarketContract]:
    """Return top active Kalshi markets already tracked in the normalized DB."""
    return session.scalars(
        select(MarketContract)
        .join(Platform, Platform.platform_id == MarketContract.platform_id)
        .where(Platform.platform_name == "kalshi")
        .where(MarketContract.market_slug.is_not(None))
        .where(MarketContract.is_active.is_(True))
        .where(MarketContract.is_closed.is_(False))
        .order_by(desc(MarketContract.volume), desc(MarketContract.updated_at))
        .limit(market_limit * 5)
    ).all()



def _decimal_from_market_price(value: Any) -> Decimal | None:
    """Parse Kalshi dollar price strings into Decimal."""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None



def _sum_depth(levels: list[Any]) -> Decimal:
    """Compute price * size notional for orderbook levels."""
    total = ZERO
    for level in levels:
        if not isinstance(level, (list, tuple)) or len(level) != 2:
            continue
        try:
            price = Decimal(str(level[0]))
            size = Decimal(str(level[1]))
        except Exception:
            continue
        total += price * size
    return total



def _build_snapshot(market: MarketContract, market_payload: dict[str, Any], orderbook_payload: dict[str, Any]) -> dict[str, Any]:
    """Build one normalized Kalshi orderbook snapshot payload."""
    market_data = market_payload.get("market") if isinstance(market_payload.get("market"), dict) else {}
    orderbook_data = orderbook_payload.get("orderbook") if isinstance(orderbook_payload.get("orderbook"), dict) else {}

    yes_levels = orderbook_data.get("yes_dollars") if isinstance(orderbook_data.get("yes_dollars"), list) else []
    no_levels = orderbook_data.get("no_dollars") if isinstance(orderbook_data.get("no_dollars"), list) else []

    best_bid = _decimal_from_market_price(market_data.get("yes_bid_dollars"))
    best_ask = _decimal_from_market_price(market_data.get("yes_ask_dollars"))
    mid_price = None
    if best_bid is not None and best_ask is not None:
        mid_price = (best_bid + best_ask) / Decimal("2")
    elif best_bid is not None:
        mid_price = best_bid
    elif best_ask is not None:
        mid_price = best_ask
    spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None

    snapshot_time = parse_datetime(orderbook_data.get("timestamp")) or datetime.now(timezone.utc)
    return {
        "market": market,
        "market_payload": market_payload,
        "snapshot_time": snapshot_time,
        "depth_levels": len(yes_levels) + len(no_levels),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid_price,
        "spread": spread,
        "bid_depth_notional": _sum_depth(yes_levels),
        "ask_depth_notional": _sum_depth(no_levels),
        "raw_payload": {
            "market": market_payload,
            "orderbook": orderbook_payload,
        },
    }



def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    """Append one JSONL record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str))
        handle.write("\n")



def run_once(args: argparse.Namespace) -> dict[str, Any]:
    """Run one Kalshi orderbook snapshot cycle."""
    with session_scope(args.database_url or None) as session:
        markets = _load_target_markets(session, args.market_limit)
        if not markets:
            summary = {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "market_count": 0,
                "db_ingest": {"records_written": 0, "error_count": 1},
                "warning": "No tracked Kalshi markets are available for orderbook snapshots.",
            }
            append_jsonl(Path(args.output_file), summary)
            return summary

        client = _load_client(args.environment)
        snapshots: list[dict[str, Any]] = []
        missing_market_count = 0
        for market in markets:
            ticker = str(market.market_slug or market.external_market_ref)
            market_payload = client.get_path(f"/trade-api/v2/markets/{ticker}", timeout_seconds=args.timeout_seconds)
            orderbook_payload = client.get_path(f"/trade-api/v2/markets/{ticker}/orderbook", timeout_seconds=args.timeout_seconds)
            orderbook_data = orderbook_payload.get("orderbook") if isinstance(orderbook_payload.get("orderbook"), dict) else {}
            if not orderbook_data.get("yes") and not orderbook_data.get("no") and not orderbook_data.get("yes_dollars") and not orderbook_data.get("no_dollars"):
                missing_market_count += 1
                continue
            snapshots.append(_build_snapshot(market, market_payload, orderbook_payload))
            if len(snapshots) >= args.market_limit:
                break

        request_url = f"/trade-api/v2/markets/{{ticker}}/orderbook (markets={len(markets)})"
        db_result = ingest_orderbook_batch(
            session,
            snapshots=snapshots,
            request_url=request_url,
            raw_output_path=args.output_file,
        )
        summary = {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "market_count": len(snapshots),
            "missing_market_count": missing_market_count,
            "markets": [
                {
                    "market_contract_id": item["market"].market_contract_id,
                    "market_slug": item["market"].market_slug,
                    "depth_levels": item["depth_levels"],
                    "best_bid": str(item["best_bid"]) if item["best_bid"] is not None else None,
                    "best_ask": str(item["best_ask"]) if item["best_ask"] is not None else None,
                }
                for item in snapshots
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
