"""Validate live Polymarket status overlay for market concentration rows."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_platform.services import read_api
from data_platform.services.polymarket_live import LivePolymarketMarket


def _base_row(status: str = "Open") -> dict[str, Any]:
    return {
        "market_id": 1,
        "market_contract_id": 1,
        "platform_name": "polymarket",
        "market_slug": "test-market",
        "market_url": "https://polymarket.com/event/test-market",
        "question": "Test market?",
        "price": 0.42,
        "volume": 1000.0,
        "whale_count": 5,
        "trusted_whale_count": 2,
        "orderbook_depth": None,
        "read_time": "2026-05-11T12:00:00+00:00",
        "last_entry_time": "2026-05-11T11:00:00+00:00",
        "market_status_label": status,
        "whale_bias_label": "Mostly Yes",
    }


def _live_market(*, closed: bool) -> LivePolymarketMarket:
    return LivePolymarketMarket(
        slug="test-market",
        event_slug="test-event",
        question="Live test market?",
        outcomes=("Yes", "No"),
        outcome_prices=(1.0, 0.0),
        last_trade_price=1.0,
        best_bid=None,
        best_ask=None,
        spread=None,
        volume=2000.0,
        liquidity=None,
        active=False,
        closed=closed,
        archived=False,
        updated_at=datetime(2026, 5, 11, 13, 0, tzinfo=timezone.utc),
        closed_time=datetime(2026, 5, 11, 12, 30, tzinfo=timezone.utc),
    )


def main() -> int:
    calls: list[str] = []
    original_fetch = read_api.fetch_live_polymarket_market_by_slug

    def fake_fetch(slug: str) -> LivePolymarketMarket | None:
        calls.append(slug)
        return _live_market(closed=True)

    try:
        read_api.fetch_live_polymarket_market_by_slug = fake_fetch
        corrected = read_api._apply_live_polymarket_market_status_overlay(_base_row("Open"))
        already_closed = read_api._apply_live_polymarket_market_status_overlay(_base_row("Closed"))

        read_api.fetch_live_polymarket_market_by_slug = lambda slug: None
        unchanged = read_api._apply_live_polymarket_market_status_overlay(_base_row("Open"))
    finally:
        read_api.fetch_live_polymarket_market_by_slug = original_fetch

    checks = [
        {
            "name": "live_closed_overlays_open_row",
            "ok": corrected["market_status_label"] == "Closed"
            and corrected["closed_status_source"] == "polymarket_gamma_live"
            and corrected["database_is_closed"] is False
            and corrected["closed_time"] == "2026-05-11T12:30:00+00:00",
        },
        {
            "name": "already_closed_skips_live_lookup",
            "ok": already_closed["market_status_label"] == "Closed" and calls == ["test-market"],
        },
        {
            "name": "live_lookup_failure_keeps_database_status",
            "ok": unchanged["market_status_label"] == "Open" and "closed_status_source" not in unchanged,
        },
    ]
    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
