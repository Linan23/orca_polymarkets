"""Shared ingestion utilities for scrape logging, raw payloads, and upserts."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from string import hexdigits
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from data_platform.models import (
    ApiPayload,
    MarketContract,
    MarketEvent,
    MarketTag,
    MarketTagMap,
    OrderbookSnapshot,
    Platform,
    PositionSnapshot,
    ScrapeRun,
    TransactionFact,
    UserAccount,
)


UNKNOWN_USER_EXTERNAL_REF = "__unknown__"


def normalize_wallet_ref(value: str | None) -> str | None:
    """Normalize hex wallet references to lowercase ``0x...`` format."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) > 2 and text.startswith("0x") and all(char in hexdigits for char in text[2:]):
        return "0x" + text[2:].lower()
    return text


def parse_datetime(value: Any) -> datetime | None:
    """Parse common API datetime values into timezone-aware UTC datetimes."""
    if value in (None, "", 0):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if " " in text and "+" in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def stable_payload_hash(payload: Any) -> str:
    """Return a stable SHA-256 hash of a JSON-serializable payload."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def get_platform(session: Session, platform_name: str) -> Platform:
    """Get or create a platform row."""
    existing = session.scalar(select(Platform).where(Platform.platform_name == platform_name))
    if existing is not None:
        return existing
    created = Platform(platform_name=platform_name)
    session.add(created)
    session.flush()
    return created


def start_scrape_run(
    session: Session,
    *,
    platform_name: str,
    job_name: str,
    endpoint_name: str,
    request_url: str,
    raw_output_path: str | None = None,
    window_started_at: datetime | None = None,
) -> ScrapeRun:
    """Insert and return a new scrape run row."""
    platform = get_platform(session, platform_name)
    row = ScrapeRun(
        platform_id=platform.platform_id,
        job_name=job_name,
        endpoint_name=endpoint_name,
        request_url=request_url,
        raw_output_path=raw_output_path,
        window_started_at=window_started_at,
    )
    session.add(row)
    session.flush()
    return row


def finalize_scrape_run(
    session: Session,
    scrape_run: ScrapeRun,
    *,
    status: str,
    records_written: int,
    error_count: int,
    error_summary: str | None = None,
) -> None:
    """Mark a scrape run complete."""
    scrape_run.status = status
    scrape_run.records_written = records_written
    scrape_run.error_count = error_count
    scrape_run.error_summary = error_summary
    scrape_run.finished_at = datetime.now(timezone.utc)
    session.flush()


def store_api_payload(
    session: Session,
    *,
    scrape_run: ScrapeRun,
    platform_name: str,
    entity_type: str,
    entity_external_id: str | None,
    payload: Any,
    collected_at: datetime | None = None,
) -> ApiPayload:
    """Persist a raw API payload row."""
    platform = get_platform(session, platform_name)
    row = ApiPayload(
        scrape_run_id=scrape_run.scrape_run_id,
        platform_id=platform.platform_id,
        entity_type=entity_type,
        entity_external_id=entity_external_id,
        collected_at=collected_at or datetime.now(timezone.utc),
        payload=payload,
        payload_hash=stable_payload_hash(payload),
    )
    session.add(row)
    session.flush()
    return row


def upsert_user_account(
    session: Session,
    *,
    platform_name: str,
    external_user_ref: str,
    wallet_address: str | None = None,
    display_label: str | None = None,
) -> UserAccount:
    """Create or update a canonical user account row."""
    platform = get_platform(session, platform_name)
    canonical_external_ref = normalize_wallet_ref(external_user_ref) or external_user_ref.strip()
    if not canonical_external_ref:
        canonical_external_ref = UNKNOWN_USER_EXTERNAL_REF
    canonical_wallet_address = normalize_wallet_ref(wallet_address)
    canonical_display_label = display_label.strip() if isinstance(display_label, str) and display_label.strip() else None
    row = session.scalar(
        select(UserAccount).where(
            UserAccount.platform_id == platform.platform_id,
            UserAccount.external_user_ref == canonical_external_ref,
        )
    )
    now = datetime.now(timezone.utc)
    if row is None:
        row = UserAccount(
            platform_id=platform.platform_id,
            external_user_ref=canonical_external_ref,
            wallet_address=canonical_wallet_address,
            display_label=canonical_display_label,
            first_seen_at=now,
            last_seen_at=now,
        )
        session.add(row)
        session.flush()
        return row

    row.wallet_address = canonical_wallet_address or row.wallet_address
    row.display_label = canonical_display_label or row.display_label
    row.last_seen_at = now
    row.updated_at = now
    session.flush()
    return row


def _status_from_flags(*, is_active: bool, is_closed: bool, is_archived: bool) -> str:
    if is_archived:
        return "archived"
    if is_closed:
        return "closed"
    if is_active:
        return "active"
    return "inactive"


def upsert_market_event(
    session: Session,
    *,
    platform_name: str,
    external_event_ref: str,
    title: str,
    slug: str | None = None,
    description: str | None = None,
    category: str | None = None,
    resolution_source: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    closed_time: datetime | None = None,
    is_active: bool = False,
    is_closed: bool = False,
    is_archived: bool = False,
    liquidity: Any = None,
    volume: Any = None,
    open_interest: Any = None,
    raw_payload_id: int | None = None,
) -> MarketEvent:
    """Create or update a canonical event row."""
    platform = get_platform(session, platform_name)
    row = session.scalar(
        select(MarketEvent).where(
            MarketEvent.platform_id == platform.platform_id,
            MarketEvent.external_event_ref == external_event_ref,
        )
    )
    now = datetime.now(timezone.utc)
    values = {
        "title": title,
        "slug": slug,
        "description": description,
        "category": category,
        "resolution_source": resolution_source,
        "start_time": start_time,
        "end_time": end_time,
        "closed_time": closed_time,
        "status": _status_from_flags(is_active=is_active, is_closed=is_closed, is_archived=is_archived),
        "is_active": is_active,
        "is_closed": is_closed,
        "is_archived": is_archived,
        "liquidity": liquidity,
        "volume": volume,
        "open_interest": open_interest,
        "raw_payload_id": raw_payload_id,
        "last_seen_at": now,
        "updated_at": now,
    }
    if row is None:
        row = MarketEvent(
            platform_id=platform.platform_id,
            external_event_ref=external_event_ref,
            first_seen_at=now,
            created_at=now,
            **values,
        )
        session.add(row)
        session.flush()
        return row

    for field, value in values.items():
        setattr(row, field, value)
    session.flush()
    return row


def upsert_market_contract(
    session: Session,
    *,
    platform_name: str,
    event: MarketEvent,
    external_market_ref: str,
    question: str,
    market_url: str | None = None,
    market_slug: str | None = None,
    condition_ref: str | None = None,
    outcome_a_label: str | None = None,
    outcome_b_label: str | None = None,
    tick_size: Any = None,
    min_order_size: Any = None,
    is_active: bool = False,
    is_closed: bool = False,
    accepting_orders: bool | None = None,
    liquidity: Any = None,
    volume: Any = None,
    last_trade_price: Any = None,
    best_bid: Any = None,
    best_ask: Any = None,
    spread: Any = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    raw_payload_id: int | None = None,
) -> MarketContract:
    """Create or update a canonical market contract row."""
    platform = get_platform(session, platform_name)
    row = session.scalar(
        select(MarketContract).where(
            MarketContract.platform_id == platform.platform_id,
            MarketContract.external_market_ref == external_market_ref,
        )
    )
    now = datetime.now(timezone.utc)
    values = {
        "event_id": event.event_id,
        "market_url": market_url,
        "market_slug": market_slug,
        "question": question,
        "condition_ref": condition_ref,
        "outcome_a_label": outcome_a_label,
        "outcome_b_label": outcome_b_label,
        "tick_size": tick_size,
        "min_order_size": min_order_size,
        "is_active": is_active,
        "is_closed": is_closed,
        "accepting_orders": accepting_orders,
        "liquidity": liquidity,
        "volume": volume,
        "last_trade_price": last_trade_price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "start_time": start_time,
        "end_time": end_time,
        "raw_payload_id": raw_payload_id,
        "last_seen_at": now,
        "updated_at": now,
    }
    if row is None:
        row = MarketContract(
            event_id=event.event_id,
            platform_id=platform.platform_id,
            external_market_ref=external_market_ref,
            first_seen_at=now,
            created_at=now,
            **{k: v for k, v in values.items() if k != "event_id"},
        )
        session.add(row)
        session.flush()
        return row

    for field, value in values.items():
        setattr(row, field, value)
    session.flush()
    return row


def upsert_market_tag(session: Session, *, platform_name: str, tag_payload: dict[str, Any]) -> MarketTag:
    """Create or update a market tag row."""
    platform = get_platform(session, platform_name)
    slug = str(tag_payload.get("slug") or tag_payload.get("label") or "untagged").strip()
    label = str(tag_payload.get("label") or slug).strip()
    row = session.scalar(
        select(MarketTag).where(
            MarketTag.platform_id == platform.platform_id,
            MarketTag.tag_slug == slug,
        )
    )
    now = datetime.now(timezone.utc)
    if row is None:
        row = MarketTag(
            platform_id=platform.platform_id,
            external_tag_ref=str(tag_payload.get("id")) if tag_payload.get("id") is not None else None,
            tag_slug=slug,
            tag_label=label,
            created_at=now,
            updated_at=now,
        )
        session.add(row)
        session.flush()
        return row
    row.external_tag_ref = str(tag_payload.get("id")) if tag_payload.get("id") is not None else row.external_tag_ref
    row.tag_label = label
    row.updated_at = now
    session.flush()
    return row


def ensure_event_tag_map(session: Session, *, event: MarketEvent, tag: MarketTag) -> None:
    """Create an event-tag link when missing."""
    existing = session.get(MarketTagMap, {"event_id": event.event_id, "tag_id": tag.tag_id})
    if existing is None:
        session.add(MarketTagMap(event_id=event.event_id, tag_id=tag.tag_id))
        session.flush()


def insert_position_snapshot(
    session: Session,
    *,
    user: UserAccount,
    market: MarketContract,
    platform_name: str,
    snapshot_time: datetime,
    position_size: Any,
    avg_entry_price: Any = None,
    current_mark_price: Any = None,
    market_value: Any = None,
    cash_pnl: Any = None,
    realized_pnl: Any = None,
    unrealized_pnl: Any = None,
    is_redeemable: bool | None = None,
    is_mergeable: bool | None = None,
    raw_payload_id: int | None = None,
) -> PositionSnapshot:
    """Insert a point-in-time position snapshot."""
    platform = get_platform(session, platform_name)
    row = PositionSnapshot(
        user_id=user.user_id,
        market_contract_id=market.market_contract_id,
        event_id=market.event_id,
        platform_id=platform.platform_id,
        snapshot_time=snapshot_time,
        position_size=position_size,
        avg_entry_price=avg_entry_price,
        current_mark_price=current_mark_price,
        market_value=market_value,
        cash_pnl=cash_pnl,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        is_redeemable=is_redeemable,
        is_mergeable=is_mergeable,
        raw_payload_id=raw_payload_id,
    )
    session.add(row)
    session.flush()
    return row


def insert_transaction_fact(
    session: Session,
    *,
    user: UserAccount,
    market: MarketContract,
    platform_name: str,
    source_transaction_id: str,
    transaction_type: str,
    transaction_time: datetime,
    side: str | None = None,
    outcome_label: str | None = None,
    price: Any = None,
    shares: Any = None,
    notional_value: Any = None,
    fee_amount: Any = None,
    profit_loss_realized: Any = None,
    source_fill_id: str | None = None,
    source_order_id: str | None = None,
    sequence_ts: int | None = None,
    raw_payload_id: int | None = None,
) -> TransactionFact:
    """Insert or return an existing normalized transaction row."""
    platform = get_platform(session, platform_name)
    existing = session.scalar(
        select(TransactionFact).where(
            TransactionFact.platform_id == platform.platform_id,
            TransactionFact.source_transaction_id == source_transaction_id,
        )
    )
    if existing is not None:
        return existing

    row = TransactionFact(
        user_id=user.user_id,
        market_contract_id=market.market_contract_id,
        event_id=market.event_id,
        platform_id=platform.platform_id,
        source_transaction_id=source_transaction_id,
        source_fill_id=source_fill_id,
        source_order_id=source_order_id,
        transaction_type=transaction_type,
        side=side,
        outcome_label=outcome_label,
        price=price,
        shares=shares,
        notional_value=notional_value,
        fee_amount=fee_amount,
        profit_loss_realized=profit_loss_realized,
        transaction_time=transaction_time,
        sequence_ts=sequence_ts,
        raw_payload_id=raw_payload_id,
    )
    session.add(row)
    session.flush()
    return row


def insert_orderbook_snapshot(
    session: Session,
    *,
    market: MarketContract,
    platform_name: str,
    snapshot_time: datetime,
    depth_levels: int,
    best_bid: Any = None,
    best_ask: Any = None,
    mid_price: Any = None,
    spread: Any = None,
    bid_depth_notional: Any = None,
    ask_depth_notional: Any = None,
    raw_payload_id: int | None = None,
) -> OrderbookSnapshot:
    """Insert one normalized order-book snapshot row."""
    platform = get_platform(session, platform_name)
    row = OrderbookSnapshot(
        market_contract_id=market.market_contract_id,
        platform_id=platform.platform_id,
        snapshot_time=snapshot_time,
        depth_levels=depth_levels,
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=mid_price,
        spread=spread,
        bid_depth_notional=bid_depth_notional,
        ask_depth_notional=ask_depth_notional,
        raw_payload_id=raw_payload_id,
    )
    session.add(row)
    session.flush()
    return row
