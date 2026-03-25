"""add following batch performance indexes

Revision ID: 20260325_1100
Revises: 20260324_2200
Create Date: 2026-03-25 11:00:00
"""

from __future__ import annotations

from alembic import op


revision = "20260325_1100"
down_revision = "20260324_2200"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create indexes for watchlist batch loading and hot profile lookups."""
    op.create_index(
        "ix_market_contract_market_slug",
        "market_contract",
        ["market_slug"],
        unique=False,
        schema="analytics",
    )
    op.create_index(
        "ix_transaction_fact_user_time",
        "transaction_fact",
        ["user_id", "transaction_time"],
        unique=False,
        schema="analytics",
    )
    op.create_index(
        "ix_transaction_fact_user_market_time",
        "transaction_fact",
        ["user_id", "market_contract_id", "transaction_time"],
        unique=False,
        schema="analytics",
    )
    op.create_index(
        "ix_position_snapshot_user_market_time",
        "position_snapshot",
        ["user_id", "market_contract_id", "snapshot_time"],
        unique=False,
        schema="analytics",
    )
    op.create_index(
        "ix_whale_score_snapshot_batch_user",
        "whale_score_snapshot",
        ["snapshot_time", "scoring_version", "user_id"],
        unique=False,
        schema="analytics",
    )
    op.create_index(
        "ix_dashboard_market_dashboard_slug",
        "dashboard_market",
        ["dashboard_id", "market_slug"],
        unique=False,
        schema="analytics",
    )
    op.create_index(
        "ix_user_profile_dashboard_user",
        "user_profile",
        ["dashboard_id", "user_id"],
        unique=False,
        schema="analytics",
    )
    op.create_index(
        "ix_market_profile_dashboard_market",
        "market_profile",
        ["dashboard_id", "market_contract_id"],
        unique=False,
        schema="analytics",
    )


def downgrade() -> None:
    """Drop the batch-loading indexes."""
    op.drop_index("ix_market_profile_dashboard_market", table_name="market_profile", schema="analytics")
    op.drop_index("ix_user_profile_dashboard_user", table_name="user_profile", schema="analytics")
    op.drop_index("ix_dashboard_market_dashboard_slug", table_name="dashboard_market", schema="analytics")
    op.drop_index("ix_whale_score_snapshot_batch_user", table_name="whale_score_snapshot", schema="analytics")
    op.drop_index("ix_position_snapshot_user_market_time", table_name="position_snapshot", schema="analytics")
    op.drop_index("ix_transaction_fact_user_market_time", table_name="transaction_fact", schema="analytics")
    op.drop_index("ix_transaction_fact_user_time", table_name="transaction_fact", schema="analytics")
    op.drop_index("ix_market_contract_market_slug", table_name="market_contract", schema="analytics")
