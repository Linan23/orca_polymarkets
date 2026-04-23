"""Application settings and environment helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


DEFAULT_DATABASE_URL = "postgresql+psycopg://app:password@localhost:5433/app_db"


def _env_bool(name: str, default: bool) -> bool:
    """Return a boolean environment flag with common truthy/falsey parsing."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value.strip())
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    app_env: str
    database_url: str
    frontend_origin: str
    session_cookie_secure: bool
    session_cookie_samesite: str
    session_cookie_domain: str
    polymarket_active_window_start: str
    polymarket_active_window_end: str
    kalshi_active_window_start: str
    kalshi_active_window_end: str
    market_stale_minutes: int
    orderbook_stale_minutes: int
    trade_feed_stale_minutes: int
    positions_stale_minutes: int
    analytics_stale_minutes: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return memoized application settings."""
    return Settings(
        app_env=os.getenv("APP_ENV", "development"),
        database_url=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL),
        frontend_origin=os.getenv("FRONTEND_ORIGIN", "http://localhost:5173"),
        session_cookie_secure=_env_bool("SESSION_COOKIE_SECURE", False),
        session_cookie_samesite=os.getenv("SESSION_COOKIE_SAMESITE", "lax").strip().lower() or "lax",
        session_cookie_domain=os.getenv("SESSION_COOKIE_DOMAIN", "").strip(),
        polymarket_active_window_start=os.getenv("POLYMARKET_ACTIVE_WINDOW_START", ""),
        polymarket_active_window_end=os.getenv("POLYMARKET_ACTIVE_WINDOW_END", ""),
        kalshi_active_window_start=os.getenv("KALSHI_ACTIVE_WINDOW_START", ""),
        kalshi_active_window_end=os.getenv("KALSHI_ACTIVE_WINDOW_END", ""),
        market_stale_minutes=_env_int("MARKET_STALE_MINUTES", 30),
        orderbook_stale_minutes=_env_int("ORDERBOOK_STALE_MINUTES", 10),
        trade_feed_stale_minutes=_env_int("TRADE_FEED_STALE_MINUTES", 10),
        positions_stale_minutes=_env_int("POSITIONS_STALE_MINUTES", 30),
        analytics_stale_minutes=_env_int("ANALYTICS_STALE_MINUTES", 30),
    )
