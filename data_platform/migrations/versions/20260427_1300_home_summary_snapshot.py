"""add cached home summary snapshot

Revision ID: 20260427_1300
Revises: 20260427_1200
Create Date: 2026-04-27 13:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260427_1300"
down_revision = "20260427_1200"
branch_labels = None
depends_on = None


JSON_VARIANT = postgresql.JSONB(astext_type=sa.Text())


def _table_exists(schema: str, table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def _index_exists(schema: str, table_name: str, index_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(index.get("name") == index_name for index in inspector.get_indexes(table_name, schema=schema))


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str], *, schema: str) -> None:
    if _index_exists(schema, table_name, index_name):
        return
    op.create_index(index_name, table_name, columns, unique=False, schema=schema)


def _drop_index_if_exists(index_name: str, table_name: str, *, schema: str) -> None:
    if not _table_exists(schema, table_name) or not _index_exists(schema, table_name, index_name):
        return
    op.drop_index(index_name, table_name=table_name, schema=schema)


def upgrade() -> None:
    if not _table_exists("analytics", "home_summary_snapshot"):
        op.create_table(
            "home_summary_snapshot",
            sa.Column("home_summary_snapshot_id", sa.Integer(), primary_key=True),
            sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("scoring_version", sa.String(length=64)),
            sa.Column("whales_detected", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("trusted_whales", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("resolved_markets_available", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("resolved_markets_observed", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("profitability_users", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("latest_successful_ingest_at", sa.DateTime(timezone=True)),
            sa.Column("summary_payload", JSON_VARIANT, nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            schema="analytics",
        )
    _create_index_if_missing(
        "ix_home_summary_snapshot_generated_at",
        "home_summary_snapshot",
        ["generated_at"],
        schema="analytics",
    )


def downgrade() -> None:
    _drop_index_if_exists("ix_home_summary_snapshot_generated_at", "home_summary_snapshot", schema="analytics")
    if _table_exists("analytics", "home_summary_snapshot"):
        op.drop_table("home_summary_snapshot", schema="analytics")
