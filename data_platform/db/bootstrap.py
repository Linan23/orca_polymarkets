"""Schema bootstrap helpers for local development."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import text

from data_platform.db.session import get_engine
from data_platform.settings import get_settings


ROOT_DIR = Path(__file__).resolve().parents[2]
ALEMBIC_INI = ROOT_DIR / "alembic.ini"


def create_database_objects(database_url: str | None = None) -> None:
    """Create required schemas and migrate the database to the latest revision."""
    engine = get_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS app"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", database_url or get_settings().database_url)
    command.upgrade(config, "head")
