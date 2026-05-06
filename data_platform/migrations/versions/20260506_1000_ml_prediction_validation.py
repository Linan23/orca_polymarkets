"""add ml prediction validation table

Revision ID: 20260506_1000
Revises: 20260505_1400
Create Date: 2026-05-06 10:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260506_1000"
down_revision = "20260505_1400"
branch_labels = None
depends_on = None

JSON_VARIANT = sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def _table_exists(schema: str, table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def upgrade() -> None:
    if _table_exists("analytics", "ml_market_prediction_validation"):
        return
    op.create_table(
        "ml_market_prediction_validation",
        sa.Column("ml_market_prediction_validation_id", sa.Integer(), primary_key=True),
        sa.Column(
            "ml_market_prediction_snapshot_id",
            sa.Integer(),
            sa.ForeignKey("analytics.ml_market_prediction_snapshot.ml_market_prediction_snapshot_id"),
            nullable=False,
        ),
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
        sa.Column("prediction_target_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("prediction_generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_odds_pct", sa.Numeric(20, 8), nullable=True),
        sa.Column("predicted_future_odds_pct", sa.Numeric(20, 8), nullable=True),
        sa.Column("predicted_delta_pts", sa.Numeric(20, 8), nullable=True),
        sa.Column("actual_future_odds_pct", sa.Numeric(20, 8), nullable=True),
        sa.Column("actual_delta_pts", sa.Numeric(20, 8), nullable=True),
        sa.Column("signed_error_pts", sa.Numeric(20, 8), nullable=True),
        sa.Column("absolute_error_pts", sa.Numeric(20, 8), nullable=True),
        sa.Column("squared_error_pts", sa.Numeric(20, 8), nullable=True),
        sa.Column("predicted_direction", sa.String(length=16), nullable=True),
        sa.Column("actual_direction", sa.String(length=16), nullable=True),
        sa.Column("direction_match", sa.Boolean(), nullable=True),
        sa.Column("validation_status", sa.String(length=32), nullable=False, server_default="missing_actual"),
        sa.Column("actual_source", sa.String(length=64), nullable=True),
        sa.Column("actual_source_detail", sa.Text(), nullable=True),
        sa.Column("actual_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("target_delay_seconds", sa.Integer(), nullable=True),
        sa.Column("validation_payload", JSON_VARIANT, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("validated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint(
            "ml_market_prediction_snapshot_id",
            name="uq_ml_market_prediction_validation_snapshot",
        ),
        schema="analytics",
    )
    op.create_index(
        "ix_ml_market_prediction_validation_market_window",
        "ml_market_prediction_validation",
        ["market_slug", "prediction_window_hours"],
        schema="analytics",
    )
    op.create_index(
        "ix_ml_market_prediction_validation_status",
        "ml_market_prediction_validation",
        ["validation_status", "validated_at"],
        schema="analytics",
    )


def downgrade() -> None:
    if _table_exists("analytics", "ml_market_prediction_validation"):
        op.drop_table("ml_market_prediction_validation", schema="analytics")
