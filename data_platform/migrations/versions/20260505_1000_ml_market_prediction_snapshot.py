"""add ml market prediction snapshot table

Revision ID: 20260505_1000
Revises: 20260427_1400
Create Date: 2026-05-05 10:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260505_1000"
down_revision = "20260427_1400"
branch_labels = None
depends_on = None

JSON_VARIANT = sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def _table_exists(schema: str, table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def upgrade() -> None:
    if _table_exists("analytics", "ml_market_prediction_snapshot"):
        return
    op.create_table(
        "ml_market_prediction_snapshot",
        sa.Column("ml_market_prediction_snapshot_id", sa.Integer(), primary_key=True),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column(
            "market_contract_id",
            sa.Integer(),
            sa.ForeignKey("analytics.market_contract.market_contract_id"),
            nullable=False,
        ),
        sa.Column("market_slug", sa.String(length=255), nullable=False),
        sa.Column("side_label", sa.String(length=128), nullable=False),
        sa.Column("prediction_window_hours", sa.Integer(), nullable=False),
        sa.Column("observation_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("whale_entry_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("prediction_target_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_odds_pct", sa.Numeric(20, 8), nullable=True),
        sa.Column("predicted_future_odds_pct", sa.Numeric(20, 8), nullable=True),
        sa.Column("predicted_delta_pts", sa.Numeric(20, 8), nullable=True),
        sa.Column("signal_tier", sa.String(length=32), nullable=False, server_default="unavailable"),
        sa.Column("display_tier", sa.String(length=32), nullable=False, server_default="unavailable"),
        sa.Column("prediction_status", sa.String(length=64), nullable=False),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("feature_schema_version", sa.String(length=128), nullable=False),
        sa.Column("trained_as_of", sa.DateTime(timezone=True), nullable=True),
        sa.Column("prediction_generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data_freshness_status", sa.String(length=64), nullable=False, server_default="unknown"),
        sa.Column("prediction_source", sa.String(length=128), nullable=False),
        sa.Column("reliability_payload", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("prediction_payload", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint(
            "market_contract_id",
            "side_label",
            "prediction_window_hours",
            "prediction_generated_at",
            name="uq_ml_market_prediction_snapshot_market_side_window_generated",
        ),
        schema="analytics",
    )
    op.create_index(
        "ix_ml_market_prediction_snapshot_market_latest",
        "ml_market_prediction_snapshot",
        ["market_slug", "prediction_generated_at"],
        schema="analytics",
    )
    op.create_index(
        "ix_ml_market_prediction_snapshot_market_side_window",
        "ml_market_prediction_snapshot",
        ["market_contract_id", "side_label", "prediction_window_hours"],
        schema="analytics",
    )


def downgrade() -> None:
    if _table_exists("analytics", "ml_market_prediction_snapshot"):
        op.drop_table("ml_market_prediction_snapshot", schema="analytics")
