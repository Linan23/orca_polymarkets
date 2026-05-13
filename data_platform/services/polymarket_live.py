"""Small Polymarket Gamma live-market helper used by read endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx


GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
REQUEST_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class LivePolymarketMarket:
    """Normalized subset of a Polymarket Gamma market response."""

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
    active: bool
    closed: bool
    archived: bool
    updated_at: datetime | None
    closed_time: datetime | None
    market_url: str | None = None
    observed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.observed_at is None:
            object.__setattr__(self, "observed_at", datetime.now(timezone.utc))
        if self.market_url is None and self.slug:
            object.__setattr__(self, "market_url", f"https://polymarket.com/event/{self.slug}")

    def outcome_probabilities(self) -> dict[str, float]:
        """Return outcome labels mapped to probabilities from 0.0 to 1.0."""
        probabilities: dict[str, float] = {}
        for label, price in zip(self.outcomes, self.outcome_prices):
            if not label or price is None:
                continue
            probabilities[str(label)] = _clamp_probability(price)
        return probabilities

    def price_for_side(self, side_label: str | None) -> float | None:
        """Return the current probability for a side label, if present."""
        if not side_label:
            return None
        normalized = _normalize_label(side_label)
        for label, price in zip(self.outcomes, self.outcome_prices):
            if _normalize_label(label) == normalized:
                return _clamp_probability(price) if price is not None else None
        return None

    def primary_side_label(self) -> str | None:
        """Return the highest-priced side label, typically the resolved side for closed markets."""
        best_label: str | None = None
        best_price: float | None = None
        for label, price in zip(self.outcomes, self.outcome_prices):
            if price is None:
                continue
            if best_price is None or price > best_price:
                best_label = label
                best_price = price
        return best_label


def fetch_live_polymarket_market_by_slug(slug: str) -> LivePolymarketMarket | None:
    """Fetch and normalize a live Gamma market by slug.

    Lookup failures intentionally return None so dashboard reads can fall back to
    database snapshots instead of failing the whole endpoint.
    """
    normalized_slug = str(slug or "").strip()
    if not normalized_slug:
        return None

    try:
        response = httpx.get(
            GAMMA_MARKETS_URL,
            params={"slug": normalized_slug},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
    except (httpx.HTTPError, ValueError):
        return None

    market_payload = _first_market_payload(data, normalized_slug)
    if not market_payload:
        return None
    return _market_from_payload(market_payload, observed_at=datetime.now(timezone.utc))


def _first_market_payload(data: Any, slug: str) -> dict[str, Any] | None:
    """Return the first Gamma market payload matching the requested slug."""
    if isinstance(data, dict):
        candidates = data.get("markets") or data.get("data") or data.get("results")
        if candidates is None and (data.get("slug") or data.get("marketSlug")):
            candidates = [data]
    elif isinstance(data, list):
        candidates = data
    else:
        candidates = None

    if not isinstance(candidates, list):
        return None
    normalized_slug = slug.casefold()
    fallback: dict[str, Any] | None = None
    for item in candidates:
        if not isinstance(item, dict):
            continue
        fallback = fallback or item
        item_slug = str(item.get("slug") or item.get("marketSlug") or "").casefold()
        if item_slug == normalized_slug:
            return item
    return fallback


def _market_from_payload(payload: dict[str, Any], *, observed_at: datetime) -> LivePolymarketMarket:
    outcomes = tuple(str(value) for value in _as_list(payload.get("outcomes") or payload.get("outcomeNames")) if value)
    prices = tuple(_to_probability(value) for value in _as_list(payload.get("outcomePrices")))
    if outcomes and len(prices) < len(outcomes):
        prices = prices + tuple(None for _ in range(len(outcomes) - len(prices)))

    slug = str(payload.get("slug") or payload.get("marketSlug") or "")
    event_slug = _event_slug(payload)
    return LivePolymarketMarket(
        slug=slug,
        event_slug=event_slug,
        question=payload.get("question") or payload.get("title"),
        outcomes=outcomes,
        outcome_prices=prices,
        last_trade_price=_to_probability(payload.get("lastTradePrice")),
        best_bid=_to_probability(payload.get("bestBid")),
        best_ask=_to_probability(payload.get("bestAsk")),
        spread=_to_probability(payload.get("spread")),
        volume=_to_float(payload.get("volume") or payload.get("volumeNum")),
        liquidity=_to_float(payload.get("liquidity") or payload.get("liquidityNum")),
        active=bool(payload.get("active")),
        closed=bool(payload.get("closed")),
        archived=bool(payload.get("archived")),
        updated_at=_parse_datetime(payload.get("updatedAt") or payload.get("updated_at")),
        closed_time=_parse_datetime(payload.get("closedTime") or payload.get("closed_time")),
        market_url=payload.get("marketUrl") or payload.get("url") or (f"https://polymarket.com/event/{slug}" if slug else None),
        observed_at=observed_at,
    )


def _event_slug(payload: dict[str, Any]) -> str | None:
    event = payload.get("event")
    if isinstance(event, dict):
        event_slug = event.get("slug")
        if event_slug:
            return str(event_slug)
    events = payload.get("events")
    if isinstance(events, list):
        for item in events:
            if isinstance(item, dict) and item.get("slug"):
                return str(item["slug"])
    value = payload.get("eventSlug")
    return str(value) if value else None


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            import json

            parsed = json.loads(stripped)
        except ValueError:
            return [stripped]
        return parsed if isinstance(parsed, list) else [parsed]
    return [value]


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_probability(value: Any) -> float | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    return _clamp_probability(parsed)


def _clamp_probability(value: float) -> float:
    if value > 1.0 and value <= 100.0:
        value = value / 100.0
    return max(0.0, min(1.0, float(value)))


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _normalize_label(value: Any) -> str:
    return str(value or "").strip().casefold()
