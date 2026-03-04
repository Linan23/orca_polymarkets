"""SQLAlchemy declarative base and shared column helpers."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase


JSON_VARIANT = JSON().with_variant(JSONB, "postgresql")
TZ_TIMESTAMP = DateTime(timezone=True)


class Base(DeclarativeBase):
    """Base class for all ORM models."""



def utc_now() -> datetime:
    """Return timezone-aware UTC timestamps for default columns."""
    return datetime.now(timezone.utc)
