"""add cached research analytics snapshots

Revision ID: 20260427_1400
Revises: 20260427_1300
Create Date: 2026-04-27 14:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260427_1400"
down_revision = "20260427_1300"
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
    if not _table_exists("analytics", "research_analytics_snapshot"):
        op.create_table(
            "research_analytics_snapshot",
            sa.Column("research_analytics_snapshot_id", sa.Integer(), primary_key=True),
            sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("timeframe", sa.String(length=16), nullable=False),
            sa.Column("top_profitable_payload", JSON_VARIANT, nullable=False),
            sa.Column("recent_entries_payload", JSON_VARIANT, nullable=False),
            sa.Column("market_concentration_payload", JSON_VARIANT, nullable=False),
            sa.Column("whale_entry_payload", JSON_VARIANT, nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            schema="analytics",
        )
    _create_index_if_missing(
        "ix_research_analytics_snapshot_timeframe_generated",
        "research_analytics_snapshot",
        ["timeframe", "generated_at"],
        schema="analytics",
    )
    _create_index_if_missing(
        "ix_whale_score_snapshot_batch_sort",
        "whale_score_snapshot",
        ["snapshot_time", "scoring_version", "is_trusted_whale", "is_whale", "trust_score", "sample_trade_count"],
        schema="analytics",
    )


def downgrade() -> None:
    _drop_index_if_exists("ix_whale_score_snapshot_batch_sort", "whale_score_snapshot", schema="analytics")
    _drop_index_if_exists(
        "ix_research_analytics_snapshot_timeframe_generated",
        "research_analytics_snapshot",
        schema="analytics",
    )
    if _table_exists("analytics", "research_analytics_snapshot"):
        op.drop_table("research_analytics_snapshot", schema="analytics")
