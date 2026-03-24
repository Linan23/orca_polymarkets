"""Polymarket-specific ingestion helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from data_platform.ingest.store import (
    UNKNOWN_USER_EXTERNAL_REF,
    ensure_event_tag_map,
    finalize_scrape_run,
    insert_orderbook_snapshot,
    insert_position_snapshot,
    insert_transaction_fact,
    parse_datetime,
    start_scrape_run,
    store_api_payload,
    upsert_market_contract,
    upsert_market_event,
    upsert_market_tag,
    upsert_user_account,
)


def _parse_outcomes(raw_value: Any) -> tuple[str | None, str | None]:
    if raw_value is None:
        return None, None
    if isinstance(raw_value, list):
        values = [str(item) for item in raw_value]
    else:
        try:
            decoded = json.loads(str(raw_value))
            values = [str(item) for item in decoded] if isinstance(decoded, list) else [str(raw_value)]
        except (TypeError, ValueError, json.JSONDecodeError):
            values = [str(raw_value)]
    first = values[0] if values else None
    second = values[1] if len(values) > 1 else None
    return first, second


def _polymarket_trade_source_id(trade: dict[str, Any]) -> str:
    """Build a stable Polymarket transaction identity."""
    tx_hash = str(trade.get("transactionHash") or "missing-tx-hash")
    asset = str(trade.get("asset") or "missing-asset")
    outcome_index = str(trade.get("outcomeIndex") if trade.get("outcomeIndex") is not None else "missing-outcome-index")
    timestamp = str(trade.get("timestamp") if trade.get("timestamp") is not None else "missing-timestamp")
    return f"{tx_hash}:{asset}:{outcome_index}:{timestamp}"


def ingest_trades_record(
    session: Session,
    *,
    record: dict[str, Any],
    request_url: str,
    raw_output_path: str | None = None,
) -> dict[str, int]:
    """Persist one Polymarket trades batch into the database."""
    scraped_at = parse_datetime(record.get("scraped_at_iso"))
    batch_time = scraped_at or parse_datetime(record.get("scraped_at_unix")) or datetime.now(timezone.utc)
    scrape_run = start_scrape_run(
        session,
        platform_name="polymarket",
        job_name="polymarket-trades",
        endpoint_name="trades",
        request_url=request_url,
        raw_output_path=raw_output_path,
        window_started_at=scraped_at,
    )
    payload_row = store_api_payload(
        session,
        scrape_run=scrape_run,
        platform_name="polymarket",
        entity_type="trades",
        entity_external_id=None,
        payload=record,
        collected_at=scraped_at,
    )

    trades = record.get("trades") if isinstance(record.get("trades"), list) else []
    records_written = 0
    error_count = 0

    for trade in trades:
        if not isinstance(trade, dict):
            error_count += 1
            continue

        wallet_ref = str(trade.get("proxyWallet") or "").strip()
        user_row = upsert_user_account(
            session,
            platform_name="polymarket",
            external_user_ref=wallet_ref or UNKNOWN_USER_EXTERNAL_REF,
            wallet_address=wallet_ref or None,
            preferred_username=str(trade.get("name") or "").strip() or None,
            display_label=str(trade.get("pseudonym") or trade.get("name") or wallet_ref or "unknown-polymarket-user"),
        )

        event_slug = str(trade.get("eventSlug") or trade.get("slug") or "").strip()
        condition_ref = str(trade.get("conditionId") or "").strip()
        event_ref = event_slug or condition_ref or str(trade.get("asset") or "unknown-event")
        title = str(trade.get("title") or trade.get("slug") or "Untitled Event")
        trade_time = parse_datetime(trade.get("timestamp")) or scraped_at
        event_row = upsert_market_event(
            session,
            platform_name="polymarket",
            external_event_ref=event_ref,
            title=title,
            slug=event_slug or None,
            end_time=None,
            is_active=True,
            is_closed=False,
            is_archived=False,
            raw_payload_id=payload_row.payload_id,
        )

        outcome = str(trade.get("outcome")) if trade.get("outcome") is not None else None
        question = f"{title} [{outcome}]" if outcome else title
        market_ref = str(trade.get("asset") or f"{condition_ref}:{trade.get('outcomeIndex')}" or event_ref)
        market_row = upsert_market_contract(
            session,
            platform_name="polymarket",
            event=event_row,
            external_market_ref=market_ref,
            question=question,
            market_url=trade.get("icon"),
            market_slug=str(trade.get("slug")) if trade.get("slug") is not None else None,
            condition_ref=condition_ref or None,
            outcome_a_label=outcome,
            is_active=True,
            is_closed=False,
            accepting_orders=None,
            last_trade_price=trade.get("price"),
            end_time=None,
            raw_payload_id=payload_row.payload_id,
        )

        price = trade.get("price")
        shares = trade.get("size")
        notional_value = None
        try:
            if price is not None and shares is not None:
                notional_value = float(price) * float(shares)
        except (TypeError, ValueError):
            notional_value = None

        insert_transaction_fact(
            session,
            user=user_row,
            market=market_row,
            platform_name="polymarket",
            source_transaction_id=_polymarket_trade_source_id(trade),
            source_fill_id=str(trade.get("transactionHash")) if trade.get("transactionHash") is not None else None,
            transaction_type="trade",
            transaction_time=trade_time or batch_time,
            side=str(trade.get("side")).lower() if trade.get("side") is not None else None,
            outcome_label=outcome,
            price=price,
            shares=shares,
            notional_value=notional_value,
            raw_payload_id=payload_row.payload_id,
        )
        records_written += 1

    status = "success"
    if error_count:
        status = "partial" if records_written else "failed"
    finalize_scrape_run(
        session,
        scrape_run,
        status=status,
        records_written=records_written,
        error_count=error_count,
        error_summary=f"Skipped {error_count} malformed trade rows." if error_count else None,
    )
    return {"records_written": records_written, "error_count": error_count}


def ingest_discovery_cycle(
    session: Session,
    *,
    cycle: dict[str, Any],
    request_url: str,
    raw_output_path: str | None = None,
) -> dict[str, int]:
    """Persist one Polymarket discovery cycle into the database."""
    scrape_run = start_scrape_run(
        session,
        platform_name="polymarket",
        job_name="polymarket-discover-events",
        endpoint_name="events",
        request_url=request_url,
        raw_output_path=raw_output_path,
    )
    records_written = 0
    for event_payload in cycle.get("results", []):
        if not isinstance(event_payload, dict):
            continue
        payload_row = store_api_payload(
            session,
            scrape_run=scrape_run,
            platform_name="polymarket",
            entity_type="event",
            entity_external_id=str(event_payload.get("id")) if event_payload.get("id") is not None else None,
            payload=event_payload,
        )
        tags = event_payload.get("tags") if isinstance(event_payload.get("tags"), list) else []
        event_row = upsert_market_event(
            session,
            platform_name="polymarket",
            external_event_ref=str(event_payload.get("id") or event_payload.get("slug") or event_payload.get("ticker") or "unknown-event"),
            title=str(event_payload.get("title") or event_payload.get("slug") or "Untitled Event"),
            slug=str(event_payload.get("slug")) if event_payload.get("slug") is not None else None,
            description=event_payload.get("description"),
            category=str(tags[0].get("label")) if tags else None,
            resolution_source=event_payload.get("resolutionSource"),
            start_time=parse_datetime(event_payload.get("startDate") or event_payload.get("startTime")),
            end_time=parse_datetime(event_payload.get("endDate")),
            closed_time=parse_datetime(event_payload.get("closedTime")),
            is_active=bool(event_payload.get("active")),
            is_closed=bool(event_payload.get("closed")),
            is_archived=bool(event_payload.get("archived")),
            liquidity=event_payload.get("liquidity"),
            volume=event_payload.get("volume"),
            open_interest=event_payload.get("openInterest"),
            raw_payload_id=payload_row.payload_id,
        )
        for tag_payload in tags:
            if not isinstance(tag_payload, dict):
                continue
            tag_row = upsert_market_tag(session, platform_name="polymarket", tag_payload=tag_payload)
            ensure_event_tag_map(session, event=event_row, tag=tag_row)

        markets = event_payload.get("markets") if isinstance(event_payload.get("markets"), list) else []
        for market_payload in markets:
            if not isinstance(market_payload, dict):
                continue
            outcome_a, outcome_b = _parse_outcomes(market_payload.get("outcomes"))
            upsert_market_contract(
                session,
                platform_name="polymarket",
                event=event_row,
                external_market_ref=str(
                    market_payload.get("id")
                    or market_payload.get("conditionId")
                    or market_payload.get("slug")
                    or event_payload.get("id")
                ),
                question=str(market_payload.get("question") or event_row.title),
                market_url=market_payload.get("url") or event_payload.get("url"),
                market_slug=str(market_payload.get("slug")) if market_payload.get("slug") is not None else None,
                condition_ref=str(market_payload.get("conditionId")) if market_payload.get("conditionId") is not None else None,
                outcome_a_label=outcome_a,
                outcome_b_label=outcome_b,
                tick_size=market_payload.get("orderPriceMinTickSize"),
                min_order_size=market_payload.get("orderMinSize"),
                is_active=bool(market_payload.get("active")),
                is_closed=bool(market_payload.get("closed")),
                accepting_orders=market_payload.get("acceptingOrders"),
                liquidity=market_payload.get("liquidityNum") or market_payload.get("liquidity"),
                volume=market_payload.get("volumeNum") or market_payload.get("volume"),
                last_trade_price=market_payload.get("lastTradePrice"),
                best_bid=market_payload.get("bestBid"),
                best_ask=market_payload.get("bestAsk"),
                spread=market_payload.get("spread"),
                start_time=parse_datetime(market_payload.get("startDate")),
                end_time=parse_datetime(market_payload.get("endDate")),
                raw_payload_id=payload_row.payload_id,
            )
            records_written += 1
        if not markets:
            records_written += 1

    errors = cycle.get("errors", [])
    status = "success"
    if errors:
        status = "partial" if records_written else "failed"
    finalize_scrape_run(
        session,
        scrape_run,
        status=status,
        records_written=records_written,
        error_count=len(errors),
        error_summary="; ".join(item.get("error", "") for item in errors[:5]) if errors else None,
    )
    return {"records_written": records_written, "error_count": len(errors)}


def ingest_positions_record(
    session: Session,
    *,
    record: dict[str, Any],
    request_url: str,
    raw_output_path: str | None = None,
) -> dict[str, int]:
    """Persist one Polymarket positions batch into the database."""
    snapshot_time = parse_datetime(record.get("scraped_at_iso"))
    scrape_run = start_scrape_run(
        session,
        platform_name="polymarket",
        job_name="polymarket-positions-watch",
        endpoint_name="positions",
        request_url=request_url,
        raw_output_path=raw_output_path,
        window_started_at=snapshot_time,
    )
    payload_row = store_api_payload(
        session,
        scrape_run=scrape_run,
        platform_name="polymarket",
        entity_type="positions",
        entity_external_id=str(record.get("user_wallet")) if record.get("user_wallet") is not None else None,
        payload=record,
        collected_at=snapshot_time,
    )

    positions = record.get("positions") if isinstance(record.get("positions"), list) else []
    records_written = 0
    for position in positions:
        if not isinstance(position, dict):
            continue

        proxy_wallet = str(position.get("proxyWallet") or record.get("user_wallet") or "").strip()
        if not proxy_wallet:
            proxy_wallet = str(record.get("user_wallet") or "unknown-wallet")
        user_row = upsert_user_account(
            session,
            platform_name="polymarket",
            external_user_ref=proxy_wallet,
            wallet_address=proxy_wallet,
            preferred_username=None,
            display_label=proxy_wallet,
        )

        end_time = parse_datetime(position.get("endDate"))
        is_closed = bool(position.get("redeemable")) or (
            end_time is not None and snapshot_time is not None and end_time <= snapshot_time
        )
        event_row = upsert_market_event(
            session,
            platform_name="polymarket",
            external_event_ref=str(position.get("eventId") or position.get("eventSlug") or position.get("slug") or "unknown-event"),
            title=str(position.get("title") or position.get("slug") or "Untitled Event"),
            slug=str(position.get("eventSlug") or position.get("slug")) if (position.get("eventSlug") or position.get("slug")) is not None else None,
            end_time=end_time,
            is_active=not is_closed,
            is_closed=is_closed,
            is_archived=False,
            raw_payload_id=payload_row.payload_id,
        )

        outcome = str(position.get("outcome")) if position.get("outcome") is not None else None
        opposite_outcome = str(position.get("oppositeOutcome")) if position.get("oppositeOutcome") is not None else None
        title = str(position.get("title") or event_row.title)
        question = f"{title} [{outcome}]" if outcome else title
        market_row = upsert_market_contract(
            session,
            platform_name="polymarket",
            event=event_row,
            external_market_ref=str(position.get("asset") or position.get("conditionId") or position.get("slug") or event_row.external_event_ref),
            question=question,
            market_url=position.get("icon"),
            market_slug=str(position.get("slug")) if position.get("slug") is not None else None,
            condition_ref=str(position.get("conditionId")) if position.get("conditionId") is not None else None,
            outcome_a_label=outcome,
            outcome_b_label=opposite_outcome,
            is_active=not is_closed,
            is_closed=is_closed,
            accepting_orders=not bool(position.get("redeemable")),
            last_trade_price=position.get("curPrice"),
            end_time=end_time,
            raw_payload_id=payload_row.payload_id,
        )

        current_value = position.get("currentValue")
        initial_value = position.get("initialValue")
        unrealized_pnl = None
        try:
            if current_value is not None and initial_value is not None:
                unrealized_pnl = float(current_value) - float(initial_value)
        except (TypeError, ValueError):
            unrealized_pnl = None

        insert_position_snapshot(
            session,
            user=user_row,
            market=market_row,
            platform_name="polymarket",
            snapshot_time=snapshot_time or parse_datetime(record.get("scraped_at_unix")) or parse_datetime(record.get("scraped_at_iso")),
            position_size=position.get("size") or 0,
            avg_entry_price=position.get("avgPrice"),
            current_mark_price=position.get("curPrice"),
            market_value=current_value,
            cash_pnl=position.get("cashPnl"),
            realized_pnl=position.get("realizedPnl"),
            unrealized_pnl=unrealized_pnl,
            is_redeemable=position.get("redeemable"),
            is_mergeable=position.get("mergeable"),
            raw_payload_id=payload_row.payload_id,
        )
        records_written += 1

    finalize_scrape_run(
        session,
        scrape_run,
        status="success",
        records_written=records_written,
        error_count=0,
        error_summary=None,
    )
    return {"records_written": records_written, "error_count": 0}


def ingest_orderbook_batch(
    session: Session,
    *,
    books: list[dict[str, Any]],
    request_url: str,
    raw_output_path: str | None = None,
) -> dict[str, int]:
    """Persist one Polymarket order-book snapshot batch into the database."""
    scrape_run = start_scrape_run(
        session,
        platform_name="polymarket",
        job_name="polymarket-orderbook",
        endpoint_name="orderbook",
        request_url=request_url,
        raw_output_path=raw_output_path,
    )

    records_written = 0
    for item in books:
        if not isinstance(item, dict):
            continue
        market = item.get("market")
        if market is None:
            continue
        snapshot_time = parse_datetime(item.get("snapshot_time")) or parse_datetime(item.get("scraped_at_iso"))
        payload_row = store_api_payload(
            session,
            scrape_run=scrape_run,
            platform_name="polymarket",
            entity_type="orderbook",
            entity_external_id=str(getattr(market, "external_market_ref", None) or getattr(market, "market_slug", None) or ""),
            payload=item.get("raw_payload", item),
            collected_at=snapshot_time,
        )

        insert_orderbook_snapshot(
            session,
            market=market,
            platform_name="polymarket",
            snapshot_time=snapshot_time or parse_datetime(item.get("snapshot_time")),
            depth_levels=int(item.get("depth_levels") or 0),
            best_bid=item.get("best_bid"),
            best_ask=item.get("best_ask"),
            mid_price=item.get("mid_price"),
            spread=item.get("spread"),
            bid_depth_notional=item.get("bid_depth_notional"),
            ask_depth_notional=item.get("ask_depth_notional"),
            raw_payload_id=payload_row.payload_id,
        )
        records_written += 1

    finalize_scrape_run(
        session,
        scrape_run,
        status="success",
        records_written=records_written,
        error_count=0,
        error_summary=None,
    )
    return {"records_written": records_written, "error_count": 0}
