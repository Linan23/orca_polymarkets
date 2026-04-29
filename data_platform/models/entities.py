"""ORM entities for the ingestion and dashboard schemas."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from data_platform.models.base import Base, JSON_VARIANT, utc_now


MONEY = Numeric(20, 8)


class Platform(Base):
    __tablename__ = "platform"
    __table_args__ = {"schema": "analytics"}

    platform_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class ScrapeRun(Base):
    __tablename__ = "scrape_run"
    __table_args__ = {"schema": "analytics"}

    scrape_run_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    job_name: Mapped[str] = mapped_column(String(128), nullable=False)
    endpoint_name: Mapped[str] = mapped_column(String(128), nullable=False)
    request_url: Mapped[str | None] = mapped_column(Text)
    window_started_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(32), default="running", nullable=False)
    records_written: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_summary: Mapped[str | None] = mapped_column(Text)
    raw_output_path: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class ApiPayload(Base):
    __tablename__ = "api_payload"
    __table_args__ = (
        Index("ix_raw_api_payload_platform_entity_time", "platform_id", "entity_type", "collected_at"),
        Index("ix_raw_api_payload_entity_external", "entity_type", "entity_external_id"),
        {"schema": "raw"},
    )

    payload_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scrape_run_id: Mapped[int] = mapped_column(ForeignKey("analytics.scrape_run.scrape_run_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_external_id: Mapped[str | None] = mapped_column(String(255))
    collected_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class UserAccount(Base):
    __tablename__ = "user_account"
    __table_args__ = (
        UniqueConstraint("platform_id", "external_user_ref", name="uq_user_account_platform_external"),
        {"schema": "analytics"},
    )

    user_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    external_user_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    wallet_address: Mapped[str | None] = mapped_column(String(255))
    preferred_username: Mapped[str | None] = mapped_column(String(255))
    display_label: Mapped[str | None] = mapped_column(String(255))
    first_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_likely_insider: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    insider_flag_reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class MarketEvent(Base):
    __tablename__ = "market_event"
    __table_args__ = (
        UniqueConstraint("platform_id", "external_event_ref", name="uq_market_event_platform_external"),
        {"schema": "analytics"},
    )

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    external_event_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str | None] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    category: Mapped[str | None] = mapped_column(String(255))
    resolution_source: Mapped[str | None] = mapped_column(Text)
    start_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    closed_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_archived: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    liquidity: Mapped[float | None] = mapped_column(MONEY)
    volume: Mapped[float | None] = mapped_column(MONEY)
    open_interest: Mapped[float | None] = mapped_column(MONEY)
    raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    first_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class MarketContract(Base):
    __tablename__ = "market_contract"
    __table_args__ = (
        Index("ix_market_contract_market_slug", "market_slug"),
        Index("ix_market_contract_event_id", "event_id"),
        UniqueConstraint("platform_id", "external_market_ref", name="uq_market_contract_platform_external"),
        {"schema": "analytics"},
    )

    market_contract_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    external_market_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    market_url: Mapped[str | None] = mapped_column(Text)
    market_slug: Mapped[str | None] = mapped_column(String(255))
    question: Mapped[str] = mapped_column(Text, nullable=False)
    condition_ref: Mapped[str | None] = mapped_column(String(255))
    outcome_a_label: Mapped[str | None] = mapped_column(String(128))
    outcome_b_label: Mapped[str | None] = mapped_column(String(128))
    tick_size: Mapped[float | None] = mapped_column(MONEY)
    min_order_size: Mapped[float | None] = mapped_column(MONEY)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    accepting_orders: Mapped[bool | None] = mapped_column(Boolean)
    liquidity: Mapped[float | None] = mapped_column(MONEY)
    volume: Mapped[float | None] = mapped_column(MONEY)
    last_trade_price: Mapped[float | None] = mapped_column(MONEY)
    best_bid: Mapped[float | None] = mapped_column(MONEY)
    best_ask: Mapped[float | None] = mapped_column(MONEY)
    spread: Mapped[float | None] = mapped_column(MONEY)
    start_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    first_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class MarketTag(Base):
    __tablename__ = "market_tag"
    __table_args__ = (
        UniqueConstraint("platform_id", "tag_slug", name="uq_market_tag_platform_slug"),
        {"schema": "analytics"},
    )

    tag_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    external_tag_ref: Mapped[str | None] = mapped_column(String(255))
    tag_slug: Mapped[str] = mapped_column(String(255), nullable=False)
    tag_label: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class MarketTagMap(Base):
    __tablename__ = "market_tag_map"
    __table_args__ = {"schema": "analytics"}

    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_tag.tag_id"), primary_key=True)


class ResolvedCondition(Base):
    __tablename__ = "resolved_condition"
    __table_args__ = (
        UniqueConstraint("platform_id", "condition_ref", name="uq_resolved_condition_platform_condition"),
        Index("ix_resolved_condition_method", "resolver_method"),
        Index("ix_resolved_condition_resolved_at", "resolved_at"),
        {"schema": "analytics"},
    )

    resolved_condition_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    condition_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    resolver_method: Mapped[str] = mapped_column(String(64), nullable=False)
    winning_outcome_label: Mapped[str] = mapped_column(String(128), nullable=False)
    resolved_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    max_winning_price: Mapped[float | None] = mapped_column(MONEY)
    min_losing_price: Mapped[float | None] = mapped_column(MONEY)
    trade_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    confidence: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class UserAccountHistory(Base):
    __tablename__ = "user_account_history"
    __table_args__ = (
        Index("ix_user_account_history_user_valid_from", "user_id", "valid_from"),
        {"schema": "analytics"},
    )

    user_account_history_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    external_user_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    wallet_address: Mapped[str | None] = mapped_column(String(255))
    preferred_username: Mapped[str | None] = mapped_column(String(255))
    display_label: Mapped[str | None] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_likely_insider: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    insider_flag_reason: Mapped[str | None] = mapped_column(Text)
    valid_from: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_to: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    change_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_scrape_run_id: Mapped[int | None] = mapped_column(ForeignKey("analytics.scrape_run.scrape_run_id"))
    source_raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class MarketEventHistory(Base):
    __tablename__ = "market_event_history"
    __table_args__ = (
        Index("ix_market_event_history_event_valid_from", "event_id", "valid_from"),
        {"schema": "analytics"},
    )

    market_event_history_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    external_event_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str | None] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    category: Mapped[str | None] = mapped_column(String(255))
    resolution_source: Mapped[str | None] = mapped_column(Text)
    start_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    closed_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_archived: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    liquidity: Mapped[float | None] = mapped_column(MONEY)
    volume: Mapped[float | None] = mapped_column(MONEY)
    open_interest: Mapped[float | None] = mapped_column(MONEY)
    valid_from: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_to: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    change_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_scrape_run_id: Mapped[int | None] = mapped_column(ForeignKey("analytics.scrape_run.scrape_run_id"))
    source_raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class MarketContractHistory(Base):
    __tablename__ = "market_contract_history"
    __table_args__ = (
        Index("ix_market_contract_history_market_valid_from", "market_contract_id", "valid_from"),
        {"schema": "analytics"},
    )

    market_contract_history_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    external_market_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    market_url: Mapped[str | None] = mapped_column(Text)
    market_slug: Mapped[str | None] = mapped_column(String(255))
    question: Mapped[str] = mapped_column(Text, nullable=False)
    condition_ref: Mapped[str | None] = mapped_column(String(255))
    outcome_a_label: Mapped[str | None] = mapped_column(String(128))
    outcome_b_label: Mapped[str | None] = mapped_column(String(128))
    tick_size: Mapped[float | None] = mapped_column(MONEY)
    min_order_size: Mapped[float | None] = mapped_column(MONEY)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    accepting_orders: Mapped[bool | None] = mapped_column(Boolean)
    liquidity: Mapped[float | None] = mapped_column(MONEY)
    volume: Mapped[float | None] = mapped_column(MONEY)
    last_trade_price: Mapped[float | None] = mapped_column(MONEY)
    best_bid: Mapped[float | None] = mapped_column(MONEY)
    best_ask: Mapped[float | None] = mapped_column(MONEY)
    spread: Mapped[float | None] = mapped_column(MONEY)
    start_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    valid_from: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_to: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    change_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_scrape_run_id: Mapped[int | None] = mapped_column(ForeignKey("analytics.scrape_run.scrape_run_id"))
    source_raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class MarketTagMapHistory(Base):
    __tablename__ = "market_tag_map_history"
    __table_args__ = (
        Index("ix_market_tag_map_history_event_valid_from", "event_id", "valid_from"),
        {"schema": "analytics"},
    )

    market_tag_map_history_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    tag_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_tag.tag_id"), nullable=False)
    tag_slug: Mapped[str | None] = mapped_column(String(255))
    tag_label: Mapped[str | None] = mapped_column(String(255))
    valid_from: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_to: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    change_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_scrape_run_id: Mapped[int | None] = mapped_column(ForeignKey("analytics.scrape_run.scrape_run_id"))
    source_raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class OrderbookSnapshot(Base):
    __tablename__ = "orderbook_snapshot"
    __table_args__ = (
        Index("ix_orderbook_snapshot_market_time", "market_contract_id", "snapshot_time"),
        {"schema": "analytics"},
    )

    orderbook_snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    snapshot_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    depth_levels: Mapped[int] = mapped_column(Integer, nullable=False)
    best_bid: Mapped[float | None] = mapped_column(MONEY)
    best_ask: Mapped[float | None] = mapped_column(MONEY)
    mid_price: Mapped[float | None] = mapped_column(MONEY)
    spread: Mapped[float | None] = mapped_column(MONEY)
    bid_depth_notional: Mapped[float | None] = mapped_column(MONEY)
    ask_depth_notional: Mapped[float | None] = mapped_column(MONEY)
    raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class TransactionFact(Base):
    __tablename__ = "transaction_fact"
    __table_args__ = (
        Index("ix_transaction_fact_user_time", "user_id", "transaction_time"),
        Index("ix_transaction_fact_user_market_time", "user_id", "market_contract_id", "transaction_time"),
        Index("ix_transaction_fact_event_id", "event_id"),
        Index("ix_transaction_fact_market_contract_id", "market_contract_id"),
        UniqueConstraint("platform_id", "source_transaction_id", name="uq_transaction_platform_source"),
        {"schema": "analytics"},
    )

    transaction_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    source_transaction_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_fill_id: Mapped[str | None] = mapped_column(String(255))
    source_order_id: Mapped[str | None] = mapped_column(String(255))
    transaction_type: Mapped[str] = mapped_column(String(64), nullable=False)
    side: Mapped[str | None] = mapped_column(String(64))
    outcome_label: Mapped[str | None] = mapped_column(String(128))
    price: Mapped[float | None] = mapped_column(MONEY)
    shares: Mapped[float | None] = mapped_column(MONEY)
    notional_value: Mapped[float | None] = mapped_column(MONEY)
    fee_amount: Mapped[float | None] = mapped_column(MONEY)
    profit_loss_realized: Mapped[float | None] = mapped_column(MONEY)
    transaction_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    sequence_ts: Mapped[int | None] = mapped_column(BigInteger)
    raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class PositionSnapshot(Base):
    __tablename__ = "position_snapshot"
    __table_args__ = (
        Index("ix_position_snapshot_user_market_time", "user_id", "market_contract_id", "snapshot_time"),
        Index("ix_position_snapshot_event_id", "event_id"),
        Index("ix_position_snapshot_market_contract_id", "market_contract_id"),
        {"schema": "analytics"},
    )

    position_snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    snapshot_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    position_size: Mapped[float] = mapped_column(MONEY, nullable=False)
    avg_entry_price: Mapped[float | None] = mapped_column(MONEY)
    current_mark_price: Mapped[float | None] = mapped_column(MONEY)
    market_value: Mapped[float | None] = mapped_column(MONEY)
    cash_pnl: Mapped[float | None] = mapped_column(MONEY)
    realized_pnl: Mapped[float | None] = mapped_column(MONEY)
    unrealized_pnl: Mapped[float | None] = mapped_column(MONEY)
    is_redeemable: Mapped[bool | None] = mapped_column(Boolean)
    is_mergeable: Mapped[bool | None] = mapped_column(Boolean)
    raw_payload_id: Mapped[int | None] = mapped_column(ForeignKey("raw.api_payload.payload_id"))
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class WhaleScoreSnapshot(Base):
    __tablename__ = "whale_score_snapshot"
    __table_args__ = (
        Index("ix_whale_score_snapshot_batch_user", "snapshot_time", "scoring_version", "user_id"),
        {"schema": "analytics"},
    )

    whale_score_snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    snapshot_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_volume_score: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    consistency_score: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    profitability_score: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    trust_score: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    insider_penalty: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    is_whale: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_trusted_whale: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    sample_trade_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    scoring_version: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class OrderbookSnapshotHourly(Base):
    __tablename__ = "orderbook_snapshot_hourly"
    __table_args__ = (
        UniqueConstraint("market_contract_id", "platform_id", "bucket_start", name="uq_orderbook_snapshot_hourly_bucket"),
        {"schema": "analytics"},
    )

    orderbook_snapshot_hourly_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    bucket_start: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_snapshot_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    last_snapshot_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_depth_levels: Mapped[int | None] = mapped_column(Integer)
    avg_best_bid: Mapped[float | None] = mapped_column(MONEY)
    avg_best_ask: Mapped[float | None] = mapped_column(MONEY)
    avg_mid_price: Mapped[float | None] = mapped_column(MONEY)
    avg_spread: Mapped[float | None] = mapped_column(MONEY)
    avg_bid_depth_notional: Mapped[float | None] = mapped_column(MONEY)
    avg_ask_depth_notional: Mapped[float | None] = mapped_column(MONEY)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class OrderbookSnapshotDaily(Base):
    __tablename__ = "orderbook_snapshot_daily"
    __table_args__ = (
        UniqueConstraint("market_contract_id", "platform_id", "bucket_date", name="uq_orderbook_snapshot_daily_bucket"),
        {"schema": "analytics"},
    )

    orderbook_snapshot_daily_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    bucket_date: Mapped[Date] = mapped_column(Date, nullable=False)
    first_snapshot_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    last_snapshot_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_depth_levels: Mapped[int | None] = mapped_column(Integer)
    avg_best_bid: Mapped[float | None] = mapped_column(MONEY)
    avg_best_ask: Mapped[float | None] = mapped_column(MONEY)
    avg_mid_price: Mapped[float | None] = mapped_column(MONEY)
    avg_spread: Mapped[float | None] = mapped_column(MONEY)
    avg_bid_depth_notional: Mapped[float | None] = mapped_column(MONEY)
    avg_ask_depth_notional: Mapped[float | None] = mapped_column(MONEY)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class PositionSnapshotDaily(Base):
    __tablename__ = "position_snapshot_daily"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "market_contract_id",
            "platform_id",
            "bucket_date",
            name="uq_position_snapshot_daily_bucket",
        ),
        Index("ix_position_snapshot_daily_event_id", "event_id"),
        Index("ix_position_snapshot_daily_market_contract_id", "market_contract_id"),
        {"schema": "analytics"},
    )

    position_snapshot_daily_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    bucket_date: Mapped[Date] = mapped_column(Date, nullable=False)
    first_snapshot_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    last_snapshot_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_position_size: Mapped[float | None] = mapped_column(MONEY)
    avg_entry_price: Mapped[float | None] = mapped_column(MONEY)
    avg_mark_price: Mapped[float | None] = mapped_column(MONEY)
    avg_market_value: Mapped[float | None] = mapped_column(MONEY)
    avg_realized_pnl: Mapped[float | None] = mapped_column(MONEY)
    avg_unrealized_pnl: Mapped[float | None] = mapped_column(MONEY)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class ScrapeRunPart(Base):
    __tablename__ = "scrape_run_part"
    __table_args__ = (
        Index("ix_scrape_run_part_started_at", "started_at"),
        {"schema": "analytics", "postgresql_partition_by": "RANGE (started_at)"},
    )

    scrape_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    job_name: Mapped[str] = mapped_column(String(128), nullable=False)
    endpoint_name: Mapped[str] = mapped_column(String(128), nullable=False)
    request_url: Mapped[str | None] = mapped_column(Text)
    window_started_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(32), default="running", nullable=False)
    records_written: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_summary: Mapped[str | None] = mapped_column(Text)
    raw_output_path: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class ApiPayloadPart(Base):
    __tablename__ = "api_payload_part"
    __table_args__ = (
        Index("ix_raw_api_payload_part_platform_entity_time", "platform_id", "entity_type", "collected_at"),
        Index("ix_raw_api_payload_part_entity_external", "entity_type", "entity_external_id"),
        {"schema": "raw", "postgresql_partition_by": "RANGE (collected_at)"},
    )

    payload_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    scrape_run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_external_id: Mapped[str | None] = mapped_column(String(255))
    collected_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class TransactionFactPart(Base):
    __tablename__ = "transaction_fact_part"
    __table_args__ = (
        Index("ix_transaction_fact_part_user_time", "user_id", "transaction_time"),
        Index("ix_transaction_fact_part_user_market_time", "user_id", "market_contract_id", "transaction_time"),
        Index("ix_transaction_fact_part_event_id", "event_id"),
        Index("ix_transaction_fact_part_market_contract_id", "market_contract_id"),
        {"schema": "analytics", "postgresql_partition_by": "RANGE (transaction_time)"},
    )

    transaction_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    source_transaction_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_fill_id: Mapped[str | None] = mapped_column(String(255))
    source_order_id: Mapped[str | None] = mapped_column(String(255))
    transaction_type: Mapped[str] = mapped_column(String(64), nullable=False)
    side: Mapped[str | None] = mapped_column(String(64))
    outcome_label: Mapped[str | None] = mapped_column(String(128))
    price: Mapped[float | None] = mapped_column(MONEY)
    shares: Mapped[float | None] = mapped_column(MONEY)
    notional_value: Mapped[float | None] = mapped_column(MONEY)
    fee_amount: Mapped[float | None] = mapped_column(MONEY)
    profit_loss_realized: Mapped[float | None] = mapped_column(MONEY)
    transaction_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    sequence_ts: Mapped[int | None] = mapped_column(BigInteger)
    raw_payload_id: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class OrderbookSnapshotPart(Base):
    __tablename__ = "orderbook_snapshot_part"
    __table_args__ = (
        Index("ix_orderbook_snapshot_part_market_time", "market_contract_id", "snapshot_time"),
        {"schema": "analytics", "postgresql_partition_by": "RANGE (snapshot_time)"},
    )

    orderbook_snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    snapshot_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    depth_levels: Mapped[int] = mapped_column(Integer, nullable=False)
    best_bid: Mapped[float | None] = mapped_column(MONEY)
    best_ask: Mapped[float | None] = mapped_column(MONEY)
    mid_price: Mapped[float | None] = mapped_column(MONEY)
    spread: Mapped[float | None] = mapped_column(MONEY)
    bid_depth_notional: Mapped[float | None] = mapped_column(MONEY)
    ask_depth_notional: Mapped[float | None] = mapped_column(MONEY)
    raw_payload_id: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class PositionSnapshotPart(Base):
    __tablename__ = "position_snapshot_part"
    __table_args__ = (
        Index("ix_position_snapshot_part_user_market_time", "user_id", "market_contract_id", "snapshot_time"),
        Index("ix_position_snapshot_part_event_id", "event_id"),
        Index("ix_position_snapshot_part_market_contract_id", "market_contract_id"),
        {"schema": "analytics", "postgresql_partition_by": "RANGE (snapshot_time)"},
    )

    position_snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    event_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_event.event_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    snapshot_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    position_size: Mapped[float] = mapped_column(MONEY, nullable=False)
    avg_entry_price: Mapped[float | None] = mapped_column(MONEY)
    current_mark_price: Mapped[float | None] = mapped_column(MONEY)
    market_value: Mapped[float | None] = mapped_column(MONEY)
    cash_pnl: Mapped[float | None] = mapped_column(MONEY)
    realized_pnl: Mapped[float | None] = mapped_column(MONEY)
    unrealized_pnl: Mapped[float | None] = mapped_column(MONEY)
    is_redeemable: Mapped[bool | None] = mapped_column(Boolean)
    is_mergeable: Mapped[bool | None] = mapped_column(Boolean)
    raw_payload_id: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class WhaleScoreSnapshotPart(Base):
    __tablename__ = "whale_score_snapshot_part"
    __table_args__ = (
        Index("ix_whale_score_snapshot_part_batch_user", "snapshot_time", "scoring_version", "user_id"),
        {"schema": "analytics", "postgresql_partition_by": "RANGE (snapshot_time)"},
    )

    whale_score_snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey("analytics.platform.platform_id"), nullable=False)
    snapshot_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
    raw_volume_score: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    consistency_score: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    profitability_score: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    trust_score: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    insider_penalty: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    is_whale: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_trusted_whale: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    sample_trade_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    scoring_version: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class Dashboard(Base):
    __tablename__ = "dashboard"
    __table_args__ = {"schema": "analytics"}

    dashboard_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dashboard_date: Mapped[Date] = mapped_column(Date, nullable=False)
    generated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(32), nullable=False)
    scope_label: Mapped[str | None] = mapped_column(String(255))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class HomeSummarySnapshot(Base):
    __tablename__ = "home_summary_snapshot"
    __table_args__ = (
        Index("ix_home_summary_snapshot_generated_at", "generated_at"),
        {"schema": "analytics"},
    )

    home_summary_snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    generated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    scoring_version: Mapped[str | None] = mapped_column(String(64))
    whales_detected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    trusted_whales: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    resolved_markets_available: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    resolved_markets_observed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    profitability_users: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    latest_successful_ingest_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    summary_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class ResearchAnalyticsSnapshot(Base):
    __tablename__ = "research_analytics_snapshot"
    __table_args__ = (
        Index("ix_research_analytics_snapshot_timeframe_generated", "timeframe", "generated_at"),
        {"schema": "analytics"},
    )

    research_analytics_snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    generated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False)
    top_profitable_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    recent_entries_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    market_concentration_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    whale_entry_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class DashboardMarket(Base):
    __tablename__ = "dashboard_market"
    __table_args__ = (
        Index("ix_dashboard_market_dashboard_slug", "dashboard_id", "market_slug"),
        {"schema": "analytics"},
    )

    market_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dashboard_id: Mapped[int] = mapped_column(ForeignKey("analytics.dashboard.dashboard_id"), nullable=False)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    market_url: Mapped[str | None] = mapped_column(Text)
    market_slug: Mapped[str | None] = mapped_column(String(255))
    orderbook_depth: Mapped[int | None] = mapped_column(Integer)
    price: Mapped[float | None] = mapped_column(MONEY)
    volume: Mapped[float | None] = mapped_column(MONEY)
    odds: Mapped[float | None] = mapped_column(MONEY)
    read_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    whale_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    trusted_whale_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    whale_market_focus: Mapped[str | None] = mapped_column(Text)
    whale_entry_prices: Mapped[dict | None] = mapped_column(JSON_VARIANT)


class UserProfile(Base):
    __tablename__ = "user_profile"
    __table_args__ = (
        Index("ix_user_profile_dashboard_user", "dashboard_id", "user_id"),
        {"schema": "analytics"},
    )

    user_profile_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dashboard_id: Mapped[int] = mapped_column(ForeignKey("analytics.dashboard.dashboard_id"), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    primary_market_ref: Mapped[str | None] = mapped_column(String(255))
    historical_actions_summary: Mapped[dict | None] = mapped_column(JSON_VARIANT)
    insider_stats: Mapped[dict | None] = mapped_column(JSON_VARIANT)
    profit_loss: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    wallet_balance: Mapped[float | None] = mapped_column(MONEY)
    wallet_transactions_summary: Mapped[dict | None] = mapped_column(JSON_VARIANT)
    markets_invested_summary: Mapped[dict | None] = mapped_column(JSON_VARIANT)
    trusted_traders_summary: Mapped[dict | None] = mapped_column(JSON_VARIANT)
    preference_probabilities: Mapped[dict | None] = mapped_column(JSON_VARIANT)
    total_volume: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    total_shares: Mapped[float] = mapped_column(MONEY, nullable=False, default=0)
    win_rate: Mapped[float | None] = mapped_column(MONEY)
    win_rate_chart_type: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class MarketProfile(Base):
    __tablename__ = "market_profile"
    __table_args__ = (
        Index("ix_market_profile_dashboard_market", "dashboard_id", "market_contract_id"),
        {"schema": "analytics"},
    )

    market_profile_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dashboard_id: Mapped[int] = mapped_column(ForeignKey("analytics.dashboard.dashboard_id"), nullable=False)
    market_contract_id: Mapped[int] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"), nullable=False)
    market_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    realtime_source: Mapped[str] = mapped_column(String(64), nullable=False)
    snapshot_time: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    realtime_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class UserLeaderboard(Base):
    __tablename__ = "user_leaderboard"
    __table_args__ = {"schema": "analytics"}

    leaderboard_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dashboard_id: Mapped[int] = mapped_column(ForeignKey("analytics.dashboard.dashboard_id"), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(32), nullable=False)
    board_type: Mapped[str] = mapped_column(String(32), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), nullable=False)
    market_contract_id: Mapped[int | None] = mapped_column(ForeignKey("analytics.market_contract.market_contract_id"))
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score_metric: Mapped[str] = mapped_column(String(128), nullable=False)
    score_value: Mapped[float | None] = mapped_column(MONEY)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class AppAccount(Base):
    __tablename__ = "app_account"
    __table_args__ = {"schema": "app"}

    account_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(512), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(32), default="viewer", nullable=False)
    last_login_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class AppSession(Base):
    __tablename__ = "app_session"
    __table_args__ = (
        UniqueConstraint("session_token_hash", name="uq_app_session_token_hash"),
        {"schema": "app"},
    )

    session_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("app.app_account.account_id", ondelete="CASCADE"), nullable=False)
    session_token_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    expires_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class AppWatchlistUser(Base):
    __tablename__ = "app_watchlist_user"
    __table_args__ = {"schema": "app"}

    account_id: Mapped[int] = mapped_column(
        ForeignKey("app.app_account.account_id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("analytics.user_account.user_id"), primary_key=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class AppWatchlistMarket(Base):
    __tablename__ = "app_watchlist_market"
    __table_args__ = {"schema": "app"}

    account_id: Mapped[int] = mapped_column(
        ForeignKey("app.app_account.account_id", ondelete="CASCADE"),
        primary_key=True,
    )
    market_slug: Mapped[str] = mapped_column(String(255), primary_key=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)


class AppAccountPreferences(Base):
    __tablename__ = "app_account_preferences"
    __table_args__ = (
        UniqueConstraint("account_id", name="uq_app_account_preferences_account_id"),
        {"schema": "app"},
    )

    account_preferences_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("app.app_account.account_id", ondelete="CASCADE"), nullable=False)
    preference_payload: Mapped[dict] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
