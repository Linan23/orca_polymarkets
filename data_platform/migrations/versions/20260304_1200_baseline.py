"""baseline schema

Revision ID: 20260304_1200
Revises:
Create Date: 2026-03-04 12:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260304_1200"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the baseline schemas and tables."""
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS analytics"))
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS raw"))

    op.create_table(
        "dashboard",
        sa.Column("dashboard_id", sa.Integer(), nullable=False),
        sa.Column("dashboard_date", sa.Date(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timeframe", sa.String(length=32), nullable=False),
        sa.Column("scope_label", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("dashboard_id"),
        schema="analytics",
    )
    op.create_table(
        "platform",
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("platform_name", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("platform_id"),
        sa.UniqueConstraint("platform_name"),
        schema="analytics",
    )
    op.create_table(
        "market_tag",
        sa.Column("tag_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("external_tag_ref", sa.String(length=255), nullable=True),
        sa.Column("tag_slug", sa.String(length=255), nullable=False),
        sa.Column("tag_label", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.PrimaryKeyConstraint("tag_id"),
        sa.UniqueConstraint("platform_id", "tag_slug", name="uq_market_tag_platform_slug"),
        schema="analytics",
    )
    op.create_table(
        "scrape_run",
        sa.Column("scrape_run_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("job_name", sa.String(length=128), nullable=False),
        sa.Column("endpoint_name", sa.String(length=128), nullable=False),
        sa.Column("request_url", sa.Text(), nullable=True),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("records_written", sa.Integer(), nullable=False),
        sa.Column("error_count", sa.Integer(), nullable=False),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("raw_output_path", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.PrimaryKeyConstraint("scrape_run_id"),
        schema="analytics",
    )
    op.create_table(
        "user_account",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("external_user_ref", sa.String(length=255), nullable=False),
        sa.Column("wallet_address", sa.String(length=255), nullable=True),
        sa.Column("display_label", sa.String(length=255), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("is_likely_insider", sa.Boolean(), nullable=False),
        sa.Column("insider_flag_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.PrimaryKeyConstraint("user_id"),
        sa.UniqueConstraint("platform_id", "external_user_ref", name="uq_user_account_platform_external"),
        schema="analytics",
    )
    op.create_table(
        "user_profile",
        sa.Column("user_profile_id", sa.Integer(), nullable=False),
        sa.Column("dashboard_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("primary_market_ref", sa.String(length=255), nullable=True),
        sa.Column(
            "historical_actions_summary",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
        sa.Column(
            "insider_stats",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
        sa.Column("profit_loss", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("wallet_balance", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column(
            "wallet_transactions_summary",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
        sa.Column(
            "markets_invested_summary",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
        sa.Column(
            "trusted_traders_summary",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
        sa.Column(
            "preference_probabilities",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
        sa.Column("total_volume", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("total_shares", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("win_rate", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("win_rate_chart_type", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dashboard_id"], ["analytics.dashboard.dashboard_id"]),
        sa.ForeignKeyConstraint(["user_id"], ["analytics.user_account.user_id"]),
        sa.PrimaryKeyConstraint("user_profile_id"),
        schema="analytics",
    )
    op.create_table(
        "whale_score_snapshot",
        sa.Column("whale_score_snapshot_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_volume_score", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("consistency_score", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("profitability_score", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("trust_score", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("insider_penalty", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("is_whale", sa.Boolean(), nullable=False),
        sa.Column("is_trusted_whale", sa.Boolean(), nullable=False),
        sa.Column("sample_trade_count", sa.Integer(), nullable=False),
        sa.Column("scoring_version", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.ForeignKeyConstraint(["user_id"], ["analytics.user_account.user_id"]),
        sa.PrimaryKeyConstraint("whale_score_snapshot_id"),
        schema="analytics",
    )
    op.create_table(
        "api_payload",
        sa.Column("payload_id", sa.Integer(), nullable=False),
        sa.Column("scrape_run_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("entity_external_id", sa.String(length=255), nullable=True),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"), nullable=False),
        sa.Column("payload_hash", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.ForeignKeyConstraint(["scrape_run_id"], ["analytics.scrape_run.scrape_run_id"]),
        sa.PrimaryKeyConstraint("payload_id"),
        schema="raw",
    )
    op.create_index(
        "ix_raw_api_payload_entity_external",
        "api_payload",
        ["entity_type", "entity_external_id"],
        unique=False,
        schema="raw",
    )
    op.create_index(
        "ix_raw_api_payload_platform_entity_time",
        "api_payload",
        ["platform_id", "entity_type", "collected_at"],
        unique=False,
        schema="raw",
    )
    op.create_table(
        "market_event",
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("external_event_ref", sa.String(length=255), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.String(length=255), nullable=True),
        sa.Column("resolution_source", sa.Text(), nullable=True),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("is_closed", sa.Boolean(), nullable=False),
        sa.Column("is_archived", sa.Boolean(), nullable=False),
        sa.Column("liquidity", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("volume", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("open_interest", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("raw_payload_id", sa.Integer(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.ForeignKeyConstraint(["raw_payload_id"], ["raw.api_payload.payload_id"]),
        sa.PrimaryKeyConstraint("event_id"),
        sa.UniqueConstraint("platform_id", "external_event_ref", name="uq_market_event_platform_external"),
        schema="analytics",
    )
    op.create_table(
        "market_contract",
        sa.Column("market_contract_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("external_market_ref", sa.String(length=255), nullable=False),
        sa.Column("market_url", sa.Text(), nullable=True),
        sa.Column("market_slug", sa.String(length=255), nullable=True),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("condition_ref", sa.String(length=255), nullable=True),
        sa.Column("outcome_a_label", sa.String(length=128), nullable=True),
        sa.Column("outcome_b_label", sa.String(length=128), nullable=True),
        sa.Column("tick_size", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("min_order_size", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("is_closed", sa.Boolean(), nullable=False),
        sa.Column("accepting_orders", sa.Boolean(), nullable=True),
        sa.Column("liquidity", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("volume", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("last_trade_price", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("best_bid", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("best_ask", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("spread", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("raw_payload_id", sa.Integer(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["analytics.market_event.event_id"]),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.ForeignKeyConstraint(["raw_payload_id"], ["raw.api_payload.payload_id"]),
        sa.PrimaryKeyConstraint("market_contract_id"),
        sa.UniqueConstraint("platform_id", "external_market_ref", name="uq_market_contract_platform_external"),
        schema="analytics",
    )
    op.create_table(
        "market_tag_map",
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("tag_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["analytics.market_event.event_id"]),
        sa.ForeignKeyConstraint(["tag_id"], ["analytics.market_tag.tag_id"]),
        sa.PrimaryKeyConstraint("event_id", "tag_id"),
        schema="analytics",
    )
    op.create_table(
        "dashboard_market",
        sa.Column("market_id", sa.Integer(), nullable=False),
        sa.Column("dashboard_id", sa.Integer(), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), nullable=False),
        sa.Column("market_url", sa.Text(), nullable=True),
        sa.Column("market_slug", sa.String(length=255), nullable=True),
        sa.Column("orderbook_depth", sa.Integer(), nullable=True),
        sa.Column("price", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("volume", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("odds", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("read_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("whale_count", sa.Integer(), nullable=False),
        sa.Column("trusted_whale_count", sa.Integer(), nullable=False),
        sa.Column("whale_market_focus", sa.Text(), nullable=True),
        sa.Column(
            "whale_entry_prices",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(["dashboard_id"], ["analytics.dashboard.dashboard_id"]),
        sa.ForeignKeyConstraint(["market_contract_id"], ["analytics.market_contract.market_contract_id"]),
        sa.PrimaryKeyConstraint("market_id"),
        schema="analytics",
    )
    op.create_table(
        "market_profile",
        sa.Column("market_profile_id", sa.Integer(), nullable=False),
        sa.Column("dashboard_id", sa.Integer(), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), nullable=False),
        sa.Column("market_ref", sa.String(length=255), nullable=False),
        sa.Column("realtime_source", sa.String(length=64), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "realtime_payload",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dashboard_id"], ["analytics.dashboard.dashboard_id"]),
        sa.ForeignKeyConstraint(["market_contract_id"], ["analytics.market_contract.market_contract_id"]),
        sa.PrimaryKeyConstraint("market_profile_id"),
        schema="analytics",
    )
    op.create_table(
        "orderbook_snapshot",
        sa.Column("orderbook_snapshot_id", sa.Integer(), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("depth_levels", sa.Integer(), nullable=False),
        sa.Column("best_bid", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("best_ask", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("mid_price", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("spread", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("bid_depth_notional", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("ask_depth_notional", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("raw_payload_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["market_contract_id"], ["analytics.market_contract.market_contract_id"]),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.ForeignKeyConstraint(["raw_payload_id"], ["raw.api_payload.payload_id"]),
        sa.PrimaryKeyConstraint("orderbook_snapshot_id"),
        schema="analytics",
    )
    op.create_index(
        "ix_orderbook_snapshot_market_time",
        "orderbook_snapshot",
        ["market_contract_id", "snapshot_time"],
        unique=False,
        schema="analytics",
    )
    op.create_table(
        "position_snapshot",
        sa.Column("position_snapshot_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("position_size", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.Column("avg_entry_price", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("current_mark_price", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("market_value", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("cash_pnl", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("unrealized_pnl", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("is_redeemable", sa.Boolean(), nullable=True),
        sa.Column("is_mergeable", sa.Boolean(), nullable=True),
        sa.Column("raw_payload_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["analytics.market_event.event_id"]),
        sa.ForeignKeyConstraint(["market_contract_id"], ["analytics.market_contract.market_contract_id"]),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.ForeignKeyConstraint(["raw_payload_id"], ["raw.api_payload.payload_id"]),
        sa.ForeignKeyConstraint(["user_id"], ["analytics.user_account.user_id"]),
        sa.PrimaryKeyConstraint("position_snapshot_id"),
        schema="analytics",
    )
    op.create_table(
        "transaction_fact",
        sa.Column("transaction_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), nullable=False),
        sa.Column("source_transaction_id", sa.String(length=255), nullable=False),
        sa.Column("source_fill_id", sa.String(length=255), nullable=True),
        sa.Column("source_order_id", sa.String(length=255), nullable=True),
        sa.Column("transaction_type", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=64), nullable=True),
        sa.Column("outcome_label", sa.String(length=128), nullable=True),
        sa.Column("price", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("shares", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("notional_value", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("fee_amount", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("profit_loss_realized", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("transaction_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sequence_ts", sa.BigInteger(), nullable=True),
        sa.Column("raw_payload_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["analytics.market_event.event_id"]),
        sa.ForeignKeyConstraint(["market_contract_id"], ["analytics.market_contract.market_contract_id"]),
        sa.ForeignKeyConstraint(["platform_id"], ["analytics.platform.platform_id"]),
        sa.ForeignKeyConstraint(["raw_payload_id"], ["raw.api_payload.payload_id"]),
        sa.ForeignKeyConstraint(["user_id"], ["analytics.user_account.user_id"]),
        sa.PrimaryKeyConstraint("transaction_id"),
        sa.UniqueConstraint("platform_id", "source_transaction_id", name="uq_transaction_platform_source"),
        schema="analytics",
    )
    op.create_table(
        "user_leaderboard",
        sa.Column("leaderboard_id", sa.Integer(), nullable=False),
        sa.Column("dashboard_id", sa.Integer(), nullable=False),
        sa.Column("timeframe", sa.String(length=32), nullable=False),
        sa.Column("board_type", sa.String(length=32), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("score_metric", sa.String(length=128), nullable=False),
        sa.Column("score_value", sa.Numeric(precision=20, scale=8), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dashboard_id"], ["analytics.dashboard.dashboard_id"]),
        sa.ForeignKeyConstraint(["market_contract_id"], ["analytics.market_contract.market_contract_id"]),
        sa.ForeignKeyConstraint(["user_id"], ["analytics.user_account.user_id"]),
        sa.PrimaryKeyConstraint("leaderboard_id"),
        schema="analytics",
    )


def downgrade() -> None:
    """Drop the baseline schemas and tables."""
    op.drop_table("user_leaderboard", schema="analytics")
    op.drop_table("transaction_fact", schema="analytics")
    op.drop_table("position_snapshot", schema="analytics")
    op.drop_index("ix_orderbook_snapshot_market_time", table_name="orderbook_snapshot", schema="analytics")
    op.drop_table("orderbook_snapshot", schema="analytics")
    op.drop_table("market_profile", schema="analytics")
    op.drop_table("dashboard_market", schema="analytics")
    op.drop_table("market_tag_map", schema="analytics")
    op.drop_table("market_contract", schema="analytics")
    op.drop_table("market_event", schema="analytics")
    op.drop_index("ix_raw_api_payload_platform_entity_time", table_name="api_payload", schema="raw")
    op.drop_index("ix_raw_api_payload_entity_external", table_name="api_payload", schema="raw")
    op.drop_table("api_payload", schema="raw")
    op.drop_table("whale_score_snapshot", schema="analytics")
    op.drop_table("user_profile", schema="analytics")
    op.drop_table("user_account", schema="analytics")
    op.drop_table("scrape_run", schema="analytics")
    op.drop_table("market_tag", schema="analytics")
    op.drop_table("platform", schema="analytics")
    op.drop_table("dashboard", schema="analytics")
    op.execute(sa.text("DROP SCHEMA IF EXISTS analytics CASCADE"))
    op.execute(sa.text("DROP SCHEMA IF EXISTS raw CASCADE"))
