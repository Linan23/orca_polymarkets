"""History-capture and shadow-write helpers for the ingestion layer."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from data_platform.models import (
    ApiPayload,
    ApiPayloadPart,
    MarketContract,
    MarketContractHistory,
    MarketEvent,
    MarketEventHistory,
    MarketTag,
    MarketTagMapHistory,
    OrderbookSnapshot,
    OrderbookSnapshotPart,
    PositionSnapshot,
    PositionSnapshotDaily,
    PositionSnapshotPart,
    ScrapeRun,
    ScrapeRunPart,
    TransactionFact,
    TransactionFactPart,
    UserAccount,
    UserAccountHistory,
    WhaleScoreSnapshot,
    WhaleScoreSnapshotPart,
)


def _now(value: datetime | None = None) -> datetime:
    return value or datetime.now(timezone.utc)


def stable_change_hash(payload: Any) -> str:
    """Return a stable SHA-256 hash for tracked-field payloads."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _upsert_shadow_row(
    session: Session,
    *,
    model: type[Any],
    pk_field: str,
    pk_value: Any,
    values: dict[str, Any],
) -> Any:
    row = session.scalar(select(model).where(getattr(model, pk_field) == pk_value).limit(1))
    if row is None:
        row = model(**{pk_field: pk_value, **values})
        session.add(row)
        session.flush()
        return row
    for key, value in values.items():
        setattr(row, key, value)
    session.flush()
    return row


def mirror_scrape_run_part(session: Session, scrape_run: ScrapeRun) -> ScrapeRunPart:
    return _upsert_shadow_row(
        session,
        model=ScrapeRunPart,
        pk_field="scrape_run_id",
        pk_value=scrape_run.scrape_run_id,
        values={
            "platform_id": scrape_run.platform_id,
            "job_name": scrape_run.job_name,
            "endpoint_name": scrape_run.endpoint_name,
            "request_url": scrape_run.request_url,
            "window_started_at": scrape_run.window_started_at,
            "started_at": scrape_run.started_at,
            "finished_at": scrape_run.finished_at,
            "status": scrape_run.status,
            "records_written": scrape_run.records_written,
            "error_count": scrape_run.error_count,
            "error_summary": scrape_run.error_summary,
            "raw_output_path": scrape_run.raw_output_path,
            "created_at": scrape_run.created_at,
        },
    )


def mirror_api_payload_part(session: Session, payload_row: ApiPayload) -> ApiPayloadPart:
    return _upsert_shadow_row(
        session,
        model=ApiPayloadPart,
        pk_field="payload_id",
        pk_value=payload_row.payload_id,
        values={
            "scrape_run_id": payload_row.scrape_run_id,
            "platform_id": payload_row.platform_id,
            "entity_type": payload_row.entity_type,
            "entity_external_id": payload_row.entity_external_id,
            "collected_at": payload_row.collected_at,
            "payload": payload_row.payload,
            "payload_hash": payload_row.payload_hash,
            "created_at": payload_row.created_at,
        },
    )


def mirror_transaction_fact_part(session: Session, row: TransactionFact) -> TransactionFactPart:
    return _upsert_shadow_row(
        session,
        model=TransactionFactPart,
        pk_field="transaction_id",
        pk_value=row.transaction_id,
        values={
            "user_id": row.user_id,
            "market_contract_id": row.market_contract_id,
            "event_id": row.event_id,
            "platform_id": row.platform_id,
            "source_transaction_id": row.source_transaction_id,
            "source_fill_id": row.source_fill_id,
            "source_order_id": row.source_order_id,
            "transaction_type": row.transaction_type,
            "side": row.side,
            "outcome_label": row.outcome_label,
            "price": row.price,
            "shares": row.shares,
            "notional_value": row.notional_value,
            "fee_amount": row.fee_amount,
            "profit_loss_realized": row.profit_loss_realized,
            "transaction_time": row.transaction_time,
            "sequence_ts": row.sequence_ts,
            "raw_payload_id": row.raw_payload_id,
            "created_at": row.created_at,
        },
    )


def mirror_orderbook_snapshot_part(session: Session, row: OrderbookSnapshot) -> OrderbookSnapshotPart:
    return _upsert_shadow_row(
        session,
        model=OrderbookSnapshotPart,
        pk_field="orderbook_snapshot_id",
        pk_value=row.orderbook_snapshot_id,
        values={
            "market_contract_id": row.market_contract_id,
            "platform_id": row.platform_id,
            "snapshot_time": row.snapshot_time,
            "depth_levels": row.depth_levels,
            "best_bid": row.best_bid,
            "best_ask": row.best_ask,
            "mid_price": row.mid_price,
            "spread": row.spread,
            "bid_depth_notional": row.bid_depth_notional,
            "ask_depth_notional": row.ask_depth_notional,
            "raw_payload_id": row.raw_payload_id,
            "created_at": row.created_at,
        },
    )


def mirror_position_snapshot_part(session: Session, row: PositionSnapshot) -> PositionSnapshotPart:
    return _upsert_shadow_row(
        session,
        model=PositionSnapshotPart,
        pk_field="position_snapshot_id",
        pk_value=row.position_snapshot_id,
        values={
            "user_id": row.user_id,
            "market_contract_id": row.market_contract_id,
            "event_id": row.event_id,
            "platform_id": row.platform_id,
            "snapshot_time": row.snapshot_time,
            "position_size": row.position_size,
            "avg_entry_price": row.avg_entry_price,
            "current_mark_price": row.current_mark_price,
            "market_value": row.market_value,
            "cash_pnl": row.cash_pnl,
            "realized_pnl": row.realized_pnl,
            "unrealized_pnl": row.unrealized_pnl,
            "is_redeemable": row.is_redeemable,
            "is_mergeable": row.is_mergeable,
            "raw_payload_id": row.raw_payload_id,
            "created_at": row.created_at,
        },
    )


def mirror_whale_score_snapshot_part(session: Session, row: WhaleScoreSnapshot) -> WhaleScoreSnapshotPart:
    return _upsert_shadow_row(
        session,
        model=WhaleScoreSnapshotPart,
        pk_field="whale_score_snapshot_id",
        pk_value=row.whale_score_snapshot_id,
        values={
            "user_id": row.user_id,
            "platform_id": row.platform_id,
            "snapshot_time": row.snapshot_time,
            "raw_volume_score": row.raw_volume_score,
            "consistency_score": row.consistency_score,
            "profitability_score": row.profitability_score,
            "trust_score": row.trust_score,
            "insider_penalty": row.insider_penalty,
            "is_whale": row.is_whale,
            "is_trusted_whale": row.is_trusted_whale,
            "sample_trade_count": row.sample_trade_count,
            "scoring_version": row.scoring_version,
            "created_at": row.created_at,
        },
    )


def _sync_history_row(
    session: Session,
    *,
    model: type[Any],
    fk_field: str,
    fk_value: Any,
    values: dict[str, Any],
    as_of: datetime,
) -> Any:
    current = session.scalar(
        select(model).where(
            getattr(model, fk_field) == fk_value,
            model.is_current.is_(True),
        )
    )
    change_hash = values["change_hash"]
    if current is not None and current.change_hash == change_hash:
        return current
    if current is not None:
        current.valid_to = as_of
        current.is_current = False
    row = model(**values, valid_from=as_of, valid_to=None, is_current=True)
    session.add(row)
    session.flush()
    return row


def sync_user_account_history(
    session: Session,
    row: UserAccount,
    *,
    as_of: datetime | None = None,
    source_scrape_run_id: int | None = None,
    source_raw_payload_id: int | None = None,
) -> UserAccountHistory:
    observed_at = _now(as_of)
    values = {
        "user_id": row.user_id,
        "platform_id": row.platform_id,
        "external_user_ref": row.external_user_ref,
        "wallet_address": row.wallet_address,
        "preferred_username": row.preferred_username,
        "display_label": row.display_label,
        "is_active": row.is_active,
        "is_likely_insider": row.is_likely_insider,
        "insider_flag_reason": row.insider_flag_reason,
        "change_hash": stable_change_hash(
            {
                "external_user_ref": row.external_user_ref,
                "wallet_address": row.wallet_address,
                "preferred_username": row.preferred_username,
                "display_label": row.display_label,
                "is_active": row.is_active,
                "is_likely_insider": row.is_likely_insider,
                "insider_flag_reason": row.insider_flag_reason,
            }
        ),
        "source_scrape_run_id": source_scrape_run_id,
        "source_raw_payload_id": source_raw_payload_id,
        "created_at": observed_at,
    }
    return _sync_history_row(
        session,
        model=UserAccountHistory,
        fk_field="user_id",
        fk_value=row.user_id,
        values=values,
        as_of=observed_at,
    )


def sync_market_event_history(
    session: Session,
    row: MarketEvent,
    *,
    as_of: datetime | None = None,
    source_scrape_run_id: int | None = None,
    source_raw_payload_id: int | None = None,
) -> MarketEventHistory:
    observed_at = _now(as_of)
    values = {
        "event_id": row.event_id,
        "platform_id": row.platform_id,
        "external_event_ref": row.external_event_ref,
        "title": row.title,
        "slug": row.slug,
        "description": row.description,
        "category": row.category,
        "resolution_source": row.resolution_source,
        "start_time": row.start_time,
        "end_time": row.end_time,
        "closed_time": row.closed_time,
        "status": row.status,
        "is_active": row.is_active,
        "is_closed": row.is_closed,
        "is_archived": row.is_archived,
        "liquidity": row.liquidity,
        "volume": row.volume,
        "open_interest": row.open_interest,
        "change_hash": stable_change_hash(
            {
                "external_event_ref": row.external_event_ref,
                "title": row.title,
                "slug": row.slug,
                "description": row.description,
                "category": row.category,
                "resolution_source": row.resolution_source,
                "start_time": row.start_time,
                "end_time": row.end_time,
                "closed_time": row.closed_time,
                "status": row.status,
                "is_active": row.is_active,
                "is_closed": row.is_closed,
                "is_archived": row.is_archived,
                "liquidity": row.liquidity,
                "volume": row.volume,
                "open_interest": row.open_interest,
            }
        ),
        "source_scrape_run_id": source_scrape_run_id,
        "source_raw_payload_id": source_raw_payload_id or row.raw_payload_id,
        "created_at": observed_at,
    }
    return _sync_history_row(
        session,
        model=MarketEventHistory,
        fk_field="event_id",
        fk_value=row.event_id,
        values=values,
        as_of=observed_at,
    )


def sync_market_contract_history(
    session: Session,
    row: MarketContract,
    *,
    as_of: datetime | None = None,
    source_scrape_run_id: int | None = None,
    source_raw_payload_id: int | None = None,
) -> MarketContractHistory:
    observed_at = _now(as_of)
    values = {
        "market_contract_id": row.market_contract_id,
        "event_id": row.event_id,
        "platform_id": row.platform_id,
        "external_market_ref": row.external_market_ref,
        "market_url": row.market_url,
        "market_slug": row.market_slug,
        "question": row.question,
        "condition_ref": row.condition_ref,
        "outcome_a_label": row.outcome_a_label,
        "outcome_b_label": row.outcome_b_label,
        "tick_size": row.tick_size,
        "min_order_size": row.min_order_size,
        "is_active": row.is_active,
        "is_closed": row.is_closed,
        "accepting_orders": row.accepting_orders,
        "liquidity": row.liquidity,
        "volume": row.volume,
        "last_trade_price": row.last_trade_price,
        "best_bid": row.best_bid,
        "best_ask": row.best_ask,
        "spread": row.spread,
        "start_time": row.start_time,
        "end_time": row.end_time,
        "change_hash": stable_change_hash(
            {
                "external_market_ref": row.external_market_ref,
                "market_url": row.market_url,
                "market_slug": row.market_slug,
                "question": row.question,
                "condition_ref": row.condition_ref,
                "outcome_a_label": row.outcome_a_label,
                "outcome_b_label": row.outcome_b_label,
                "tick_size": row.tick_size,
                "min_order_size": row.min_order_size,
                "is_active": row.is_active,
                "is_closed": row.is_closed,
                "accepting_orders": row.accepting_orders,
                "liquidity": row.liquidity,
                "volume": row.volume,
                "last_trade_price": row.last_trade_price,
                "best_bid": row.best_bid,
                "best_ask": row.best_ask,
                "spread": row.spread,
                "start_time": row.start_time,
                "end_time": row.end_time,
            }
        ),
        "source_scrape_run_id": source_scrape_run_id,
        "source_raw_payload_id": source_raw_payload_id or row.raw_payload_id,
        "created_at": observed_at,
    }
    return _sync_history_row(
        session,
        model=MarketContractHistory,
        fk_field="market_contract_id",
        fk_value=row.market_contract_id,
        values=values,
        as_of=observed_at,
    )


def ensure_market_tag_map_history(
    session: Session,
    *,
    event_id: int,
    tag: MarketTag,
    as_of: datetime | None = None,
    source_scrape_run_id: int | None = None,
    source_raw_payload_id: int | None = None,
) -> MarketTagMapHistory:
    observed_at = _now(as_of)
    change_hash = stable_change_hash(
        {
            "event_id": event_id,
            "tag_id": tag.tag_id,
            "tag_slug": tag.tag_slug,
            "tag_label": tag.tag_label,
        }
    )
    current = session.scalar(
        select(MarketTagMapHistory).where(
            MarketTagMapHistory.event_id == event_id,
            MarketTagMapHistory.tag_id == tag.tag_id,
            MarketTagMapHistory.is_current.is_(True),
        )
    )
    if current is not None and current.change_hash == change_hash:
        return current
    if current is not None:
        current.valid_to = observed_at
        current.is_current = False
    row = MarketTagMapHistory(
        event_id=event_id,
        tag_id=tag.tag_id,
        tag_slug=tag.tag_slug,
        tag_label=tag.tag_label,
        valid_from=observed_at,
        valid_to=None,
        is_current=True,
        change_hash=change_hash,
        source_scrape_run_id=source_scrape_run_id,
        source_raw_payload_id=source_raw_payload_id,
        created_at=observed_at,
    )
    session.add(row)
    session.flush()
    return row


def close_market_tag_map_history(session: Session, *, event_id: int, tag_id: int, as_of: datetime | None = None) -> None:
    observed_at = _now(as_of)
    row = session.scalar(
        select(MarketTagMapHistory).where(
            MarketTagMapHistory.event_id == event_id,
            MarketTagMapHistory.tag_id == tag_id,
            MarketTagMapHistory.is_current.is_(True),
        )
    )
    if row is None:
        return
    row.valid_to = observed_at
    row.is_current = False
    session.flush()


def upsert_position_snapshot_daily_rollup(
    session: Session,
    *,
    user_id: int,
    market_contract_id: int,
    event_id: int,
    platform_id: int,
    bucket_date: Any,
    values: dict[str, Any],
) -> PositionSnapshotDaily:
    row = session.scalar(
        select(PositionSnapshotDaily).where(
            PositionSnapshotDaily.user_id == user_id,
            PositionSnapshotDaily.market_contract_id == market_contract_id,
            PositionSnapshotDaily.platform_id == platform_id,
            PositionSnapshotDaily.bucket_date == bucket_date,
        )
    )
    if row is None:
        row = PositionSnapshotDaily(
            user_id=user_id,
            market_contract_id=market_contract_id,
            event_id=event_id,
            platform_id=platform_id,
            bucket_date=bucket_date,
            **values,
        )
        session.add(row)
        session.flush()
        return row
    for key, value in values.items():
        setattr(row, key, value)
    session.flush()
    return row
