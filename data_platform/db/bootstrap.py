"""Schema bootstrap helpers for local development."""

from __future__ import annotations

from sqlalchemy import text

from data_platform.db.session import get_engine
from data_platform.models import Base


def create_database_objects(database_url: str | None = None) -> None:
    """Create the required schemas and tables in the configured database."""
    engine = get_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS app"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    Base.metadata.create_all(bind=engine)
