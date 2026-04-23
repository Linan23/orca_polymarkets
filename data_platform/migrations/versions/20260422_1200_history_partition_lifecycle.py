"""add history tables, rollups, and partition shadows

Revision ID: 20260422_1200
Revises: 20260408_1400
Create Date: 2026-04-22 12:00:00
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260422_1200"
down_revision = "20260408_1400"
branch_labels = None
depends_on = None


MONEY = sa.Numeric(20, 8)
JSON_VARIANT = postgresql.JSONB(astext_type=sa.Text())


def _month_floor(value: datetime) -> datetime:
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _add_month(value: datetime) -> datetime:
    base = _month_floor(value)
    if base.month == 12:
        return base.replace(year=base.year + 1, month=1)
    return base.replace(month=base.month + 1)


def _create_month_partition(schema: str, table_name: str, month_start: datetime) -> None:
    month_end = _add_month(month_start)
    partition_name = f"{table_name}_{month_start.strftime('%Y%m')}"
    op.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {schema}."{partition_name}"
        PARTITION OF {schema}."{table_name}"
        FOR VALUES FROM ('{month_start.isoformat()}') TO ('{month_end.isoformat()}')
        '''
    )


def _table_exists(schema: str, table_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names(schema=schema)


def _index_exists(schema: str, table_name: str, index_name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(index.get("name") == index_name for index in inspector.get_indexes(table_name, schema=schema))


def _create_table_if_missing(table_name: str, *columns: sa.Column, schema: str, **kwargs: object) -> None:
    if _table_exists(schema, table_name):
        return
    op.create_table(table_name, *columns, schema=schema, **kwargs)


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str], *, schema: str, **kwargs: object) -> None:
    if _index_exists(schema, table_name, index_name):
        return
    op.create_index(index_name, table_name, columns, schema=schema, **kwargs)


def _create_compatibility_view(schema: str, view_name: str, legacy_table: str, shadow_table: str, pk: str, columns: list[str]) -> None:
    column_list = ", ".join(columns)
    op.execute(
        f'''
        CREATE OR REPLACE VIEW {schema}."{view_name}" AS
        SELECT {column_list}
        FROM {schema}."{shadow_table}"
        UNION ALL
        SELECT {', '.join(f'l.{col}' for col in columns)}
        FROM {schema}."{legacy_table}" AS l
        WHERE NOT EXISTS (
          SELECT 1
          FROM {schema}."{shadow_table}" AS p
          WHERE p."{pk}" = l."{pk}"
        )
        '''
    )


def upgrade() -> None:
    _create_table_if_missing(
        "user_account_history",
        sa.Column("user_account_history_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("analytics.user_account.user_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("external_user_ref", sa.String(length=255), nullable=False),
        sa.Column("wallet_address", sa.String(length=255)),
        sa.Column("preferred_username", sa.String(length=255)),
        sa.Column("display_label", sa.String(length=255)),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("is_likely_insider", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("insider_flag_reason", sa.Text()),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True)),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("change_hash", sa.String(length=64), nullable=False),
        sa.Column("source_scrape_run_id", sa.Integer(), sa.ForeignKey("analytics.scrape_run.scrape_run_id")),
        sa.Column("source_raw_payload_id", sa.Integer(), sa.ForeignKey("raw.api_payload.payload_id")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
    )
    _create_index_if_missing(
        "ix_user_account_history_user_valid_from",
        "user_account_history",
        ["user_id", "valid_from"],
        schema="analytics",
    )
    _create_index_if_missing(
        "ix_user_account_history_current_unique",
        "user_account_history",
        ["user_id"],
        unique=True,
        schema="analytics",
        postgresql_where=sa.text("is_current"),
    )

    _create_table_if_missing(
        "market_event_history",
        sa.Column("market_event_history_id", sa.Integer(), primary_key=True),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("analytics.market_event.event_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("external_event_ref", sa.String(length=255), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("slug", sa.String(length=255)),
        sa.Column("description", sa.Text()),
        sa.Column("category", sa.String(length=255)),
        sa.Column("resolution_source", sa.Text()),
        sa.Column("start_time", sa.DateTime(timezone=True)),
        sa.Column("end_time", sa.DateTime(timezone=True)),
        sa.Column("closed_time", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_closed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_archived", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("liquidity", MONEY),
        sa.Column("volume", MONEY),
        sa.Column("open_interest", MONEY),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True)),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("change_hash", sa.String(length=64), nullable=False),
        sa.Column("source_scrape_run_id", sa.Integer(), sa.ForeignKey("analytics.scrape_run.scrape_run_id")),
        sa.Column("source_raw_payload_id", sa.Integer(), sa.ForeignKey("raw.api_payload.payload_id")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
    )
    _create_index_if_missing("ix_market_event_history_event_valid_from", "market_event_history", ["event_id", "valid_from"], schema="analytics")
    _create_index_if_missing(
        "ix_market_event_history_current_unique",
        "market_event_history",
        ["event_id"],
        unique=True,
        schema="analytics",
        postgresql_where=sa.text("is_current"),
    )

    _create_table_if_missing(
        "market_contract_history",
        sa.Column("market_contract_history_id", sa.Integer(), primary_key=True),
        sa.Column("market_contract_id", sa.Integer(), sa.ForeignKey("analytics.market_contract.market_contract_id"), nullable=False),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("analytics.market_event.event_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("external_market_ref", sa.String(length=255), nullable=False),
        sa.Column("market_url", sa.Text()),
        sa.Column("market_slug", sa.String(length=255)),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("condition_ref", sa.String(length=255)),
        sa.Column("outcome_a_label", sa.String(length=128)),
        sa.Column("outcome_b_label", sa.String(length=128)),
        sa.Column("tick_size", MONEY),
        sa.Column("min_order_size", MONEY),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_closed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("accepting_orders", sa.Boolean()),
        sa.Column("liquidity", MONEY),
        sa.Column("volume", MONEY),
        sa.Column("last_trade_price", MONEY),
        sa.Column("best_bid", MONEY),
        sa.Column("best_ask", MONEY),
        sa.Column("spread", MONEY),
        sa.Column("start_time", sa.DateTime(timezone=True)),
        sa.Column("end_time", sa.DateTime(timezone=True)),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True)),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("change_hash", sa.String(length=64), nullable=False),
        sa.Column("source_scrape_run_id", sa.Integer(), sa.ForeignKey("analytics.scrape_run.scrape_run_id")),
        sa.Column("source_raw_payload_id", sa.Integer(), sa.ForeignKey("raw.api_payload.payload_id")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
    )
    _create_index_if_missing(
        "ix_market_contract_history_market_valid_from",
        "market_contract_history",
        ["market_contract_id", "valid_from"],
        schema="analytics",
    )
    _create_index_if_missing(
        "ix_market_contract_history_current_unique",
        "market_contract_history",
        ["market_contract_id"],
        unique=True,
        schema="analytics",
        postgresql_where=sa.text("is_current"),
    )

    _create_table_if_missing(
        "market_tag_map_history",
        sa.Column("market_tag_map_history_id", sa.Integer(), primary_key=True),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("analytics.market_event.event_id"), nullable=False),
        sa.Column("tag_id", sa.Integer(), sa.ForeignKey("analytics.market_tag.tag_id"), nullable=False),
        sa.Column("tag_slug", sa.String(length=255)),
        sa.Column("tag_label", sa.String(length=255)),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True)),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("change_hash", sa.String(length=64), nullable=False),
        sa.Column("source_scrape_run_id", sa.Integer(), sa.ForeignKey("analytics.scrape_run.scrape_run_id")),
        sa.Column("source_raw_payload_id", sa.Integer(), sa.ForeignKey("raw.api_payload.payload_id")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
    )
    _create_index_if_missing(
        "ix_market_tag_map_history_event_valid_from",
        "market_tag_map_history",
        ["event_id", "valid_from"],
        schema="analytics",
    )
    _create_index_if_missing(
        "ix_market_tag_map_history_current_unique",
        "market_tag_map_history",
        ["event_id", "tag_id"],
        unique=True,
        schema="analytics",
        postgresql_where=sa.text("is_current"),
    )

    _create_table_if_missing(
        "orderbook_snapshot_hourly",
        sa.Column("orderbook_snapshot_hourly_id", sa.Integer(), primary_key=True),
        sa.Column("market_contract_id", sa.Integer(), sa.ForeignKey("analytics.market_contract.market_contract_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_snapshot_time", sa.DateTime(timezone=True)),
        sa.Column("last_snapshot_time", sa.DateTime(timezone=True)),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_depth_levels", sa.Integer()),
        sa.Column("avg_best_bid", MONEY),
        sa.Column("avg_best_ask", MONEY),
        sa.Column("avg_mid_price", MONEY),
        sa.Column("avg_spread", MONEY),
        sa.Column("avg_bid_depth_notional", MONEY),
        sa.Column("avg_ask_depth_notional", MONEY),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("market_contract_id", "platform_id", "bucket_start", name="uq_orderbook_snapshot_hourly_bucket"),
        schema="analytics",
    )
    _create_table_if_missing(
        "orderbook_snapshot_daily",
        sa.Column("orderbook_snapshot_daily_id", sa.Integer(), primary_key=True),
        sa.Column("market_contract_id", sa.Integer(), sa.ForeignKey("analytics.market_contract.market_contract_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("bucket_date", sa.Date(), nullable=False),
        sa.Column("first_snapshot_time", sa.DateTime(timezone=True)),
        sa.Column("last_snapshot_time", sa.DateTime(timezone=True)),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_depth_levels", sa.Integer()),
        sa.Column("avg_best_bid", MONEY),
        sa.Column("avg_best_ask", MONEY),
        sa.Column("avg_mid_price", MONEY),
        sa.Column("avg_spread", MONEY),
        sa.Column("avg_bid_depth_notional", MONEY),
        sa.Column("avg_ask_depth_notional", MONEY),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("market_contract_id", "platform_id", "bucket_date", name="uq_orderbook_snapshot_daily_bucket"),
        schema="analytics",
    )
    _create_table_if_missing(
        "position_snapshot_daily",
        sa.Column("position_snapshot_daily_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("analytics.user_account.user_id"), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), sa.ForeignKey("analytics.market_contract.market_contract_id"), nullable=False),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("analytics.market_event.event_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("bucket_date", sa.Date(), nullable=False),
        sa.Column("first_snapshot_time", sa.DateTime(timezone=True)),
        sa.Column("last_snapshot_time", sa.DateTime(timezone=True)),
        sa.Column("sample_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("avg_position_size", MONEY),
        sa.Column("avg_entry_price", MONEY),
        sa.Column("avg_mark_price", MONEY),
        sa.Column("avg_market_value", MONEY),
        sa.Column("avg_realized_pnl", MONEY),
        sa.Column("avg_unrealized_pnl", MONEY),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("user_id", "market_contract_id", "platform_id", "bucket_date", name="uq_position_snapshot_daily_bucket"),
        schema="analytics",
    )

    _create_table_if_missing(
        "scrape_run_part",
        sa.Column("scrape_run_id", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("job_name", sa.String(length=128), nullable=False),
        sa.Column("endpoint_name", sa.String(length=128), nullable=False),
        sa.Column("request_url", sa.Text()),
        sa.Column("window_started_at", sa.DateTime(timezone=True)),
        sa.Column("started_at", sa.DateTime(timezone=True), primary_key=True, nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'running'")),
        sa.Column("records_written", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("error_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("error_summary", sa.Text()),
        sa.Column("raw_output_path", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
        postgresql_partition_by="RANGE (started_at)",
    )
    _create_index_if_missing("ix_scrape_run_part_started_at", "scrape_run_part", ["started_at"], schema="analytics")

    _create_table_if_missing(
        "api_payload_part",
        sa.Column("payload_id", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("scrape_run_id", sa.Integer(), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("entity_external_id", sa.String(length=255)),
        sa.Column("collected_at", sa.DateTime(timezone=True), primary_key=True, nullable=False),
        sa.Column("payload", JSON_VARIANT, nullable=False),
        sa.Column("payload_hash", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="raw",
        postgresql_partition_by="RANGE (collected_at)",
    )
    _create_index_if_missing("ix_raw_api_payload_part_platform_entity_time", "api_payload_part", ["platform_id", "entity_type", "collected_at"], schema="raw")
    _create_index_if_missing("ix_raw_api_payload_part_entity_external", "api_payload_part", ["entity_type", "entity_external_id"], schema="raw")

    _create_table_if_missing(
        "transaction_fact_part",
        sa.Column("transaction_id", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("analytics.user_account.user_id"), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), sa.ForeignKey("analytics.market_contract.market_contract_id"), nullable=False),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("analytics.market_event.event_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("source_transaction_id", sa.String(length=255), nullable=False),
        sa.Column("source_fill_id", sa.String(length=255)),
        sa.Column("source_order_id", sa.String(length=255)),
        sa.Column("transaction_type", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=64)),
        sa.Column("outcome_label", sa.String(length=128)),
        sa.Column("price", MONEY),
        sa.Column("shares", MONEY),
        sa.Column("notional_value", MONEY),
        sa.Column("fee_amount", MONEY),
        sa.Column("profit_loss_realized", MONEY),
        sa.Column("transaction_time", sa.DateTime(timezone=True), primary_key=True, nullable=False),
        sa.Column("sequence_ts", sa.BigInteger()),
        sa.Column("raw_payload_id", sa.Integer()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
        postgresql_partition_by="RANGE (transaction_time)",
    )
    _create_index_if_missing("ix_transaction_fact_part_user_time", "transaction_fact_part", ["user_id", "transaction_time"], schema="analytics")
    _create_index_if_missing(
        "ix_transaction_fact_part_user_market_time",
        "transaction_fact_part",
        ["user_id", "market_contract_id", "transaction_time"],
        schema="analytics",
    )

    _create_table_if_missing(
        "orderbook_snapshot_part",
        sa.Column("orderbook_snapshot_id", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("market_contract_id", sa.Integer(), sa.ForeignKey("analytics.market_contract.market_contract_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), primary_key=True, nullable=False),
        sa.Column("depth_levels", sa.Integer(), nullable=False),
        sa.Column("best_bid", MONEY),
        sa.Column("best_ask", MONEY),
        sa.Column("mid_price", MONEY),
        sa.Column("spread", MONEY),
        sa.Column("bid_depth_notional", MONEY),
        sa.Column("ask_depth_notional", MONEY),
        sa.Column("raw_payload_id", sa.Integer()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
        postgresql_partition_by="RANGE (snapshot_time)",
    )
    _create_index_if_missing("ix_orderbook_snapshot_part_market_time", "orderbook_snapshot_part", ["market_contract_id", "snapshot_time"], schema="analytics")

    _create_table_if_missing(
        "position_snapshot_part",
        sa.Column("position_snapshot_id", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("analytics.user_account.user_id"), nullable=False),
        sa.Column("market_contract_id", sa.Integer(), sa.ForeignKey("analytics.market_contract.market_contract_id"), nullable=False),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("analytics.market_event.event_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), primary_key=True, nullable=False),
        sa.Column("position_size", MONEY, nullable=False),
        sa.Column("avg_entry_price", MONEY),
        sa.Column("current_mark_price", MONEY),
        sa.Column("market_value", MONEY),
        sa.Column("cash_pnl", MONEY),
        sa.Column("realized_pnl", MONEY),
        sa.Column("unrealized_pnl", MONEY),
        sa.Column("is_redeemable", sa.Boolean()),
        sa.Column("is_mergeable", sa.Boolean()),
        sa.Column("raw_payload_id", sa.Integer()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
        postgresql_partition_by="RANGE (snapshot_time)",
    )
    _create_index_if_missing(
        "ix_position_snapshot_part_user_market_time",
        "position_snapshot_part",
        ["user_id", "market_contract_id", "snapshot_time"],
        schema="analytics",
    )

    _create_table_if_missing(
        "whale_score_snapshot_part",
        sa.Column("whale_score_snapshot_id", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("analytics.user_account.user_id"), nullable=False),
        sa.Column("platform_id", sa.Integer(), sa.ForeignKey("analytics.platform.platform_id"), nullable=False),
        sa.Column("snapshot_time", sa.DateTime(timezone=True), primary_key=True, nullable=False),
        sa.Column("raw_volume_score", MONEY, nullable=False, server_default=sa.text("0")),
        sa.Column("consistency_score", MONEY, nullable=False, server_default=sa.text("0")),
        sa.Column("profitability_score", MONEY, nullable=False, server_default=sa.text("0")),
        sa.Column("trust_score", MONEY, nullable=False, server_default=sa.text("0")),
        sa.Column("insider_penalty", MONEY, nullable=False, server_default=sa.text("0")),
        sa.Column("is_whale", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_trusted_whale", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("sample_trade_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("scoring_version", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        schema="analytics",
        postgresql_partition_by="RANGE (snapshot_time)",
    )
    _create_index_if_missing(
        "ix_whale_score_snapshot_part_batch_user",
        "whale_score_snapshot_part",
        ["snapshot_time", "scoring_version", "user_id"],
        schema="analytics",
    )

    current_month = _month_floor(datetime.now(timezone.utc))
    next_month = _add_month(current_month)
    for month_start in (current_month, next_month):
        _create_month_partition("analytics", "scrape_run_part", month_start)
        _create_month_partition("raw", "api_payload_part", month_start)
        _create_month_partition("analytics", "transaction_fact_part", month_start)
        _create_month_partition("analytics", "orderbook_snapshot_part", month_start)
        _create_month_partition("analytics", "position_snapshot_part", month_start)
        _create_month_partition("analytics", "whale_score_snapshot_part", month_start)

    op.execute(
        """
        INSERT INTO analytics.user_account_history (
            user_id, platform_id, external_user_ref, wallet_address, preferred_username, display_label,
            is_active, is_likely_insider, insider_flag_reason, valid_from, valid_to, is_current,
            change_hash, source_scrape_run_id, source_raw_payload_id, created_at
        )
        SELECT
            ua.user_id,
            ua.platform_id,
            ua.external_user_ref,
            ua.wallet_address,
            ua.preferred_username,
            ua.display_label,
            ua.is_active,
            ua.is_likely_insider,
            ua.insider_flag_reason,
            COALESCE(ua.first_seen_at, ua.created_at, CURRENT_TIMESTAMP),
            NULL,
            TRUE,
            md5(concat_ws('||',
                COALESCE(ua.external_user_ref, ''),
                COALESCE(ua.wallet_address, ''),
                COALESCE(ua.preferred_username, ''),
                COALESCE(ua.display_label, ''),
                COALESCE(ua.is_active::text, ''),
                COALESCE(ua.is_likely_insider::text, ''),
                COALESCE(ua.insider_flag_reason, '')
            )),
            NULL,
            NULL,
            CURRENT_TIMESTAMP
        FROM analytics.user_account ua
        WHERE NOT EXISTS (
            SELECT 1
            FROM analytics.user_account_history uah
            WHERE uah.user_id = ua.user_id AND uah.is_current
        )
        """
    )
    op.execute(
        """
        INSERT INTO analytics.market_event_history (
            event_id, platform_id, external_event_ref, title, slug, description, category, resolution_source,
            start_time, end_time, closed_time, status, is_active, is_closed, is_archived,
            liquidity, volume, open_interest, valid_from, valid_to, is_current,
            change_hash, source_scrape_run_id, source_raw_payload_id, created_at
        )
        SELECT
            me.event_id,
            me.platform_id,
            me.external_event_ref,
            me.title,
            me.slug,
            me.description,
            me.category,
            me.resolution_source,
            me.start_time,
            me.end_time,
            me.closed_time,
            me.status,
            me.is_active,
            me.is_closed,
            me.is_archived,
            me.liquidity,
            me.volume,
            me.open_interest,
            COALESCE(me.first_seen_at, me.created_at, CURRENT_TIMESTAMP),
            NULL,
            TRUE,
            md5(concat_ws('||',
                COALESCE(me.external_event_ref, ''),
                COALESCE(me.title, ''),
                COALESCE(me.slug, ''),
                COALESCE(me.description, ''),
                COALESCE(me.category, ''),
                COALESCE(me.resolution_source, ''),
                COALESCE(me.start_time::text, ''),
                COALESCE(me.end_time::text, ''),
                COALESCE(me.closed_time::text, ''),
                COALESCE(me.status, ''),
                COALESCE(me.is_active::text, ''),
                COALESCE(me.is_closed::text, ''),
                COALESCE(me.is_archived::text, ''),
                COALESCE(me.liquidity::text, ''),
                COALESCE(me.volume::text, ''),
                COALESCE(me.open_interest::text, '')
            )),
            NULL,
            me.raw_payload_id,
            CURRENT_TIMESTAMP
        FROM analytics.market_event me
        WHERE NOT EXISTS (
            SELECT 1
            FROM analytics.market_event_history meh
            WHERE meh.event_id = me.event_id AND meh.is_current
        )
        """
    )
    op.execute(
        """
        INSERT INTO analytics.market_contract_history (
            market_contract_id, event_id, platform_id, external_market_ref, market_url, market_slug, question,
            condition_ref, outcome_a_label, outcome_b_label, tick_size, min_order_size,
            is_active, is_closed, accepting_orders, liquidity, volume, last_trade_price,
            best_bid, best_ask, spread, start_time, end_time, valid_from, valid_to, is_current,
            change_hash, source_scrape_run_id, source_raw_payload_id, created_at
        )
        SELECT
            mc.market_contract_id,
            mc.event_id,
            mc.platform_id,
            mc.external_market_ref,
            mc.market_url,
            mc.market_slug,
            mc.question,
            mc.condition_ref,
            mc.outcome_a_label,
            mc.outcome_b_label,
            mc.tick_size,
            mc.min_order_size,
            mc.is_active,
            mc.is_closed,
            mc.accepting_orders,
            mc.liquidity,
            mc.volume,
            mc.last_trade_price,
            mc.best_bid,
            mc.best_ask,
            mc.spread,
            mc.start_time,
            mc.end_time,
            COALESCE(mc.first_seen_at, mc.created_at, CURRENT_TIMESTAMP),
            NULL,
            TRUE,
            md5(concat_ws('||',
                COALESCE(mc.external_market_ref, ''),
                COALESCE(mc.market_url, ''),
                COALESCE(mc.market_slug, ''),
                COALESCE(mc.question, ''),
                COALESCE(mc.condition_ref, ''),
                COALESCE(mc.outcome_a_label, ''),
                COALESCE(mc.outcome_b_label, ''),
                COALESCE(mc.tick_size::text, ''),
                COALESCE(mc.min_order_size::text, ''),
                COALESCE(mc.is_active::text, ''),
                COALESCE(mc.is_closed::text, ''),
                COALESCE(mc.accepting_orders::text, ''),
                COALESCE(mc.liquidity::text, ''),
                COALESCE(mc.volume::text, ''),
                COALESCE(mc.last_trade_price::text, ''),
                COALESCE(mc.best_bid::text, ''),
                COALESCE(mc.best_ask::text, ''),
                COALESCE(mc.spread::text, ''),
                COALESCE(mc.start_time::text, ''),
                COALESCE(mc.end_time::text, '')
            )),
            NULL,
            mc.raw_payload_id,
            CURRENT_TIMESTAMP
        FROM analytics.market_contract mc
        WHERE NOT EXISTS (
            SELECT 1
            FROM analytics.market_contract_history mch
            WHERE mch.market_contract_id = mc.market_contract_id AND mch.is_current
        )
        """
    )
    op.execute(
        """
        INSERT INTO analytics.market_tag_map_history (
            event_id, tag_id, tag_slug, tag_label, valid_from, valid_to, is_current,
            change_hash, source_scrape_run_id, source_raw_payload_id, created_at
        )
        SELECT
            mtm.event_id,
            mtm.tag_id,
            mt.tag_slug,
            mt.tag_label,
            COALESCE(me.first_seen_at, me.created_at, CURRENT_TIMESTAMP),
            NULL,
            TRUE,
            md5(concat_ws('||', mtm.event_id::text, mtm.tag_id::text, COALESCE(mt.tag_slug, ''), COALESCE(mt.tag_label, ''))),
            NULL,
            me.raw_payload_id,
            CURRENT_TIMESTAMP
        FROM analytics.market_tag_map mtm
        JOIN analytics.market_event me ON me.event_id = mtm.event_id
        JOIN analytics.market_tag mt ON mt.tag_id = mtm.tag_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM analytics.market_tag_map_history h
            WHERE h.event_id = mtm.event_id AND h.tag_id = mtm.tag_id AND h.is_current
        )
        """
    )

    _create_compatibility_view(
        "analytics",
        "scrape_run_all",
        "scrape_run",
        "scrape_run_part",
        "scrape_run_id",
        [
            "scrape_run_id",
            "platform_id",
            "job_name",
            "endpoint_name",
            "request_url",
            "window_started_at",
            "started_at",
            "finished_at",
            "status",
            "records_written",
            "error_count",
            "error_summary",
            "raw_output_path",
            "created_at",
        ],
    )
    _create_compatibility_view(
        "raw",
        "api_payload_all",
        "api_payload",
        "api_payload_part",
        "payload_id",
        [
            "payload_id",
            "scrape_run_id",
            "platform_id",
            "entity_type",
            "entity_external_id",
            "collected_at",
            "payload",
            "payload_hash",
            "created_at",
        ],
    )
    _create_compatibility_view(
        "analytics",
        "transaction_fact_all",
        "transaction_fact",
        "transaction_fact_part",
        "transaction_id",
        [
            "transaction_id",
            "user_id",
            "market_contract_id",
            "event_id",
            "platform_id",
            "source_transaction_id",
            "source_fill_id",
            "source_order_id",
            "transaction_type",
            "side",
            "outcome_label",
            "price",
            "shares",
            "notional_value",
            "fee_amount",
            "profit_loss_realized",
            "transaction_time",
            "sequence_ts",
            "raw_payload_id",
            "created_at",
        ],
    )
    _create_compatibility_view(
        "analytics",
        "orderbook_snapshot_all",
        "orderbook_snapshot",
        "orderbook_snapshot_part",
        "orderbook_snapshot_id",
        [
            "orderbook_snapshot_id",
            "market_contract_id",
            "platform_id",
            "snapshot_time",
            "depth_levels",
            "best_bid",
            "best_ask",
            "mid_price",
            "spread",
            "bid_depth_notional",
            "ask_depth_notional",
            "raw_payload_id",
            "created_at",
        ],
    )
    _create_compatibility_view(
        "analytics",
        "position_snapshot_all",
        "position_snapshot",
        "position_snapshot_part",
        "position_snapshot_id",
        [
            "position_snapshot_id",
            "user_id",
            "market_contract_id",
            "event_id",
            "platform_id",
            "snapshot_time",
            "position_size",
            "avg_entry_price",
            "current_mark_price",
            "market_value",
            "cash_pnl",
            "realized_pnl",
            "unrealized_pnl",
            "is_redeemable",
            "is_mergeable",
            "raw_payload_id",
            "created_at",
        ],
    )
    _create_compatibility_view(
        "analytics",
        "whale_score_snapshot_all",
        "whale_score_snapshot",
        "whale_score_snapshot_part",
        "whale_score_snapshot_id",
        [
            "whale_score_snapshot_id",
            "user_id",
            "platform_id",
            "snapshot_time",
            "raw_volume_score",
            "consistency_score",
            "profitability_score",
            "trust_score",
            "insider_penalty",
            "is_whale",
            "is_trusted_whale",
            "sample_trade_count",
            "scoring_version",
            "created_at",
        ],
    )


def downgrade() -> None:
    for schema, name in (
        ("analytics", "whale_score_snapshot_all"),
        ("analytics", "position_snapshot_all"),
        ("analytics", "orderbook_snapshot_all"),
        ("analytics", "transaction_fact_all"),
        ("raw", "api_payload_all"),
        ("analytics", "scrape_run_all"),
    ):
        op.execute(f'DROP VIEW IF EXISTS {schema}."{name}"')

    for schema, table_name in (
        ("analytics", "whale_score_snapshot_part"),
        ("analytics", "position_snapshot_part"),
        ("analytics", "orderbook_snapshot_part"),
        ("analytics", "transaction_fact_part"),
        ("raw", "api_payload_part"),
        ("analytics", "scrape_run_part"),
        ("analytics", "position_snapshot_daily"),
        ("analytics", "orderbook_snapshot_daily"),
        ("analytics", "orderbook_snapshot_hourly"),
        ("analytics", "market_tag_map_history"),
        ("analytics", "market_contract_history"),
        ("analytics", "market_event_history"),
        ("analytics", "user_account_history"),
    ):
        op.drop_table(table_name, schema=schema)
