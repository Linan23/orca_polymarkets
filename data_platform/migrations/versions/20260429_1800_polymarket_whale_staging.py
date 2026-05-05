"""add Polymarket whale candidate staging tables

Revision ID: 20260429_1800
Revises: 20260427_1400
Create Date: 2026-04-29 18:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260429_1800"
down_revision = "20260427_1400"
branch_labels = None
depends_on = None


JSON_VARIANT = postgresql.JSONB(astext_type=sa.Text())


def _table_exists(schema: str, table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def upgrade() -> None:
    if not _table_exists("raw", "polymarket_candidate_trade"):
        op.create_table(
            "polymarket_candidate_trade",
            sa.Column("candidate_trade_id", sa.Integer(), primary_key=True),
            sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
            sa.Column("raw_payload_id", sa.Integer(), sa.ForeignKey("raw.api_payload.payload_id"), nullable=False),
            sa.Column("wallet_address", sa.String(length=255), nullable=False),
            sa.Column("source_transaction_id", sa.String(length=255), nullable=False),
            sa.Column("source_fill_id", sa.String(length=255)),
            sa.Column("trade_time", sa.DateTime(timezone=True), nullable=False),
            sa.Column("market_ref", sa.String(length=255), nullable=False),
            sa.Column("condition_ref", sa.String(length=255)),
            sa.Column("event_ref", sa.String(length=255), nullable=False),
            sa.Column("notional_value", sa.Numeric(20, 8)),
            sa.Column("payload", JSON_VARIANT, nullable=False),
            sa.Column("promoted_at", sa.DateTime(timezone=True)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.UniqueConstraint("platform_id", "source_transaction_id", name="uq_polymarket_candidate_trade_source"),
            schema="raw",
        )
        op.create_index(
            "ix_polymarket_candidate_trade_wallet_time",
            "polymarket_candidate_trade",
            ["wallet_address", "trade_time"],
            schema="raw",
        )
        op.create_index(
            "ix_polymarket_candidate_trade_promoted",
            "polymarket_candidate_trade",
            ["promoted_at"],
            schema="raw",
        )

    if not _table_exists("raw", "polymarket_candidate_position"):
        op.create_table(
            "polymarket_candidate_position",
            sa.Column("candidate_position_id", sa.Integer(), primary_key=True),
            sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
            sa.Column("raw_payload_id", sa.Integer(), sa.ForeignKey("raw.api_payload.payload_id"), nullable=False),
            sa.Column("wallet_address", sa.String(length=255), nullable=False),
            sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
            sa.Column("market_ref", sa.String(length=255), nullable=False),
            sa.Column("condition_ref", sa.String(length=255)),
            sa.Column("event_ref", sa.String(length=255), nullable=False),
            sa.Column("exposure_value", sa.Numeric(20, 8)),
            sa.Column("payload", JSON_VARIANT, nullable=False),
            sa.Column("promoted_at", sa.DateTime(timezone=True)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            schema="raw",
        )
        op.create_index(
            "ix_polymarket_candidate_position_wallet_time",
            "polymarket_candidate_position",
            ["wallet_address", "snapshot_time"],
            schema="raw",
        )
        op.create_index(
            "ix_polymarket_candidate_position_promoted",
            "polymarket_candidate_position",
            ["promoted_at"],
            schema="raw",
        )


def downgrade() -> None:
    if _table_exists("raw", "polymarket_candidate_position"):
        op.drop_index("ix_polymarket_candidate_position_promoted", table_name="polymarket_candidate_position", schema="raw")
        op.drop_index(
            "ix_polymarket_candidate_position_wallet_time",
            table_name="polymarket_candidate_position",
            schema="raw",
        )
        op.drop_table("polymarket_candidate_position", schema="raw")
    if _table_exists("raw", "polymarket_candidate_trade"):
        op.drop_index("ix_polymarket_candidate_trade_promoted", table_name="polymarket_candidate_trade", schema="raw")
        op.drop_index("ix_polymarket_candidate_trade_wallet_time", table_name="polymarket_candidate_trade", schema="raw")
        op.drop_table("polymarket_candidate_trade", schema="raw")
