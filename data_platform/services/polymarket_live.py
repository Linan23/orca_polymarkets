"""Small live Polymarket Gamma helpers for user-facing market profiles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
from time import monotonic
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


GAMMA_MARKET_BY_SLUG_URL = "https://gamma-api.polymarket.com/markets/slug/{slug}"
LIVE_MARKET_CACHE_TTL_SECONDS = 30.0
LIVE_MARKET_TIMEOUT_SECONDS = 4.0
_LIVE_MARKET_CACHE: dict[str, tuple[float, "LivePolymarketMarket | None"]] = {}


def normalize_outcome_label(value: str | None) -> str:
    """Normalize outcome labels for safe cross-source matching."""
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _parse_jsonish_list(value: Any) -> list[Any]:
    """Return a list from Gamma fields that may arrive as lists or JSON strings."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _safe_float(value: Any) -> float | None:
    """Return a float or None for nullable numeric Gamma fields."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    """Parse the datetime formats used by Gamma responses."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class LivePolymarketMarket:
    """Live market metadata and outcome prices from Polymarket Gamma."""

    slug: str
    event_slug: str | None
    question: str | None
    outcomes: tuple[str, ...]
    outcome_prices: tuple[float | None, ...]
    last_trade_price: float | None
    best_bid: float | None
    best_ask: float | None
    spread: float | None
    volume: float | None
    liquidity: float | None
    active: bool | None
    closed: bool | None
    archived: bool | None
    updated_at: datetime | None
    closed_time: datetime | None

    @property
    def market_url(self) -> str:
        """Return the public Polymarket event URL for this slug."""
        public_slug = self.event_slug or self.slug
        return f"https://polymarket.com/event/{quote(public_slug, safe='')}"

    @property
    def observed_at(self) -> datetime | None:
        """Return the best live timestamp for freshness metadata."""
        return self.updated_at or self.closed_time

    def price_for_side(self, side_label: str | None) -> float | None:
        """Return the live probability for a specific side label in 0-1 units."""
        normalized_side = normalize_outcome_label(side_label)
        if not normalized_side:
            return None
        for outcome, price in zip(self.outcomes, self.outcome_prices, strict=False):
            if normalize_outcome_label(outcome) == normalized_side:
                return price
        return None

    def outcome_probabilities(self) -> list[dict[str, float | str | None]]:
        """Return label/probability rows in the same order as Polymarket."""
        return [
            {"label": outcome, "probability": price}
            for outcome, price in zip(self.outcomes, self.outcome_prices, strict=False)
        ]

    def primary_side_label(self) -> str | None:
        """Pick a sensible primary side for chart focus."""
        priced = [
            (outcome, price)
            for outcome, price in zip(self.outcomes, self.outcome_prices, strict=False)
            if price is not None
        ]
        if not priced:
            return self.outcomes[0] if self.outcomes else None
        if self.closed:
            return max(priced, key=lambda item: item[1])[0]
        return self.outcomes[0] if self.outcomes else priced[0][0]


def _market_from_gamma_payload(payload: dict[str, Any]) -> LivePolymarketMarket:
    """Build a typed live-market object from Gamma JSON."""
    outcomes = tuple(str(value) for value in _parse_jsonish_list(payload.get("outcomes")))
    outcome_prices = tuple(_safe_float(value) for value in _parse_jsonish_list(payload.get("outcomePrices")))
    events = payload.get("events") if isinstance(payload.get("events"), list) else []
    first_event = events[0] if events and isinstance(events[0], dict) else {}
    event_slug = str(first_event.get("slug") or "").strip().casefold() or None
    return LivePolymarketMarket(
        slug=str(payload.get("slug") or "").strip().casefold(),
        event_slug=event_slug,
        question=str(payload.get("question") or "").strip() or None,
        outcomes=outcomes,
        outcome_prices=outcome_prices,
        last_trade_price=_safe_float(payload.get("lastTradePrice")),
        best_bid=_safe_float(payload.get("bestBid")),
        best_ask=_safe_float(payload.get("bestAsk")),
        spread=_safe_float(payload.get("spread")),
        volume=_safe_float(payload.get("volumeNum") or payload.get("volume")),
        liquidity=_safe_float(payload.get("liquidityNum") or payload.get("liquidity")),
        active=bool(payload.get("active")) if payload.get("active") is not None else None,
        closed=bool(payload.get("closed")) if payload.get("closed") is not None else None,
        archived=bool(payload.get("archived")) if payload.get("archived") is not None else None,
        updated_at=_parse_datetime(payload.get("updatedAt")),
        closed_time=_parse_datetime(payload.get("closedTime")),
    )


def fetch_live_polymarket_market_by_slug(slug: str) -> LivePolymarketMarket | None:
    """Fetch a live Gamma market by slug, falling back to None on network/API failures."""
    normalized_slug = str(slug or "").strip().casefold()
    if not normalized_slug:
        return None

    now = monotonic()
    cached = _LIVE_MARKET_CACHE.get(normalized_slug)
    if cached and cached[0] > now:
        return cached[1]

    url = GAMMA_MARKET_BY_SLUG_URL.format(slug=quote(normalized_slug, safe=""))
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 OrcaResearchDashboard/1.0",
        },
    )
    live_market: LivePolymarketMarket | None = None
    try:
        with urlopen(request, timeout=LIVE_MARKET_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if isinstance(payload, dict):
            live_market = _market_from_gamma_payload(payload)
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError):
        live_market = None

    _LIVE_MARKET_CACHE[normalized_slug] = (now + LIVE_MARKET_CACHE_TTL_SECONDS, live_market)
    return live_market
