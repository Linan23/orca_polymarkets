"""Application settings and environment helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


DEFAULT_DATABASE_URL = "postgresql+psycopg://app:password@localhost:5433/app_db"


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    app_env: str
    database_url: str
    frontend_origin: str
    polymarket_active_window_start: str
    polymarket_active_window_end: str
    kalshi_active_window_start: str
    kalshi_active_window_end: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return memoized application settings."""
    return Settings(
        app_env=os.getenv("APP_ENV", "development"),
        database_url=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL),
        frontend_origin=os.getenv("FRONTEND_ORIGIN", "http://localhost:5173"),
        polymarket_active_window_start=os.getenv("POLYMARKET_ACTIVE_WINDOW_START", ""),
        polymarket_active_window_end=os.getenv("POLYMARKET_ACTIVE_WINDOW_END", ""),
        kalshi_active_window_start=os.getenv("KALSHI_ACTIVE_WINDOW_START", ""),
        kalshi_active_window_end=os.getenv("KALSHI_ACTIVE_WINDOW_END", ""),
    )
