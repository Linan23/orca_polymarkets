"""Kalshi-specific ingestion helpers."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from data_platform.ingest.store import (
    UNKNOWN_USER_EXTERNAL_REF,
    finalize_scrape_run,
    insert_orderbook_snapshot,
    insert_transaction_fact,
    parse_datetime,
    start_scrape_run,
    store_api_payload,
    upsert_market_contract,
    upsert_market_event,
    upsert_user_account,
)
from data_platform.services.market_scope import canonicalize_focus_domains, kalshi_trade_payload_matches_focus_domains


def _kalshi_user_ref(payload: dict[str, Any]) -> str:
    """Return a stable Kalshi user reference when a payload exposes one."""
    user_id = payload.get("user_id")
    if user_id in (None, ""):
        return UNKNOWN_USER_EXTERNAL_REF
    return str(user_id).strip() or UNKNOWN_USER_EXTERNAL_REF


def _upsert_kalshi_users_from_orders(session: Session, payload: dict[str, Any]) -> int:
    """Upsert authenticated Kalshi users from order payloads when user ids are present."""
    orders: list[dict[str, Any]] = []
    if isinstance(payload.get("orders"), list):
        orders = [item for item in payload["orders"] if isinstance(item, dict)]
    elif isinstance(payload.get("order"), dict):
        orders = [payload["order"]]

    users_written = 0
    for order in orders:
        user_ref = _kalshi_user_ref(order)
        if user_ref == UNKNOWN_USER_EXTERNAL_REF:
            continue
        upsert_user_account(
            session,
            platform_name="kalshi",
            external_user_ref=user_ref,
            preferred_username=None,
            display_label=f"Kalshi user {user_ref}",
        )
        users_written += 1
    return users_written


def ingest_scrape_record(
    session: Session,
    *,
    record: dict[str, Any],
    request_url: str,
    raw_output_path: str | None = None,
    focus_domains: list[str] | None = None,
) -> dict[str, int]:
    """Persist one Kalshi scrape record, with trade normalization when available."""
    focus_domains = canonicalize_focus_domains(focus_domains)
    endpoint_name = str(record.get("endpoint") or "custom")
    data = record.get("data") if isinstance(record.get("data"), dict) else {}
    filtered_data = dict(data)
    filtered_out_of_scope = 0
    if endpoint_name == "trades":
        trades = data.get("trades") if isinstance(data.get("trades"), list) else []
        filtered_trades = [
            trade
            for trade in trades
            if isinstance(trade, dict) and kalshi_trade_payload_matches_focus_domains(trade, focus_domains)
        ]
        filtered_data["trades"] = filtered_trades
        filtered_out_of_scope = max(len(trades) - len(filtered_trades), 0)
    filtered_record = dict(record)
    filtered_record["data"] = filtered_data
    scrape_run = start_scrape_run(
        session,
        platform_name="kalshi",
        job_name=f"kalshi-{endpoint_name}",
        endpoint_name=endpoint_name,
        request_url=request_url,
        raw_output_path=raw_output_path,
        window_started_at=parse_datetime(record.get("scraped_at_iso")),
    )
    payload_row = store_api_payload(
        session,
        scrape_run=scrape_run,
        platform_name="kalshi",
        entity_type=endpoint_name,
        entity_external_id=None,
        payload=filtered_record,
        collected_at=parse_datetime(record.get("scraped_at_iso")),
    )

    records_written = 0
    error_count = 0
    if endpoint_name == "trades":
        trades = filtered_data.get("trades") if isinstance(filtered_data.get("trades"), list) else []
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            ticker = str(trade.get("ticker") or "unknown-ticker")
            event_row = upsert_market_event(
                session,
                platform_name="kalshi",
                external_event_ref=ticker,
                title=ticker,
                slug=ticker,
                is_active=True,
                is_closed=False,
                is_archived=False,
                raw_payload_id=payload_row.payload_id,
            )
            market_row = upsert_market_contract(
                session,
                platform_name="kalshi",
                event=event_row,
                external_market_ref=ticker,
                question=ticker,
                market_slug=ticker,
                is_active=True,
                is_closed=False,
                volume=trade.get("count_fp") or trade.get("count"),
                last_trade_price=trade.get("price"),
                best_bid=trade.get("yes_price_dollars"),
                best_ask=trade.get("no_price_dollars"),
                raw_payload_id=payload_row.payload_id,
            )
            user_ref = _kalshi_user_ref(trade)
            user_row = upsert_user_account(
                session,
                platform_name="kalshi",
                external_user_ref=user_ref,
                preferred_username=None,
                display_label=(
                    "Unknown Kalshi participant"
                    if user_ref == UNKNOWN_USER_EXTERNAL_REF
                    else f"Kalshi user {user_ref}"
                ),
            )
            price = trade.get("price")
            shares = trade.get("count_fp") or trade.get("count")
            try:
                notional_value = (float(price) if price is not None else 0.0) * (float(shares) if shares is not None else 0.0)
            except (TypeError, ValueError):
                notional_value = None
            insert_transaction_fact(
                session,
                user=user_row,
                market=market_row,
                platform_name="kalshi",
                source_transaction_id=str(trade.get("trade_id") or ticker),
                transaction_type="trade",
                transaction_time=parse_datetime(trade.get("created_time")) or parse_datetime(record.get("scraped_at_iso")),
                side=str(trade.get("taker_side")) if trade.get("taker_side") is not None else None,
                outcome_label=str(trade.get("taker_side")) if trade.get("taker_side") is not None else None,
                price=price,
                shares=shares,
                notional_value=notional_value,
                raw_payload_id=payload_row.payload_id,
            )
            records_written += 1
    elif endpoint_name == "custom":
        path = str(record.get("path") or "")
        if "/portfolio/orders" in path or "/historical/orders" in path:
            records_written += _upsert_kalshi_users_from_orders(session, data)

    finalize_scrape_run(
        session,
        scrape_run,
        status="success" if error_count == 0 else ("partial" if records_written else "failed"),
        records_written=records_written,
        error_count=error_count,
        error_summary=None,
    )
    return {
        "records_written": records_written,
        "error_count": error_count,
        "skipped_out_of_scope": filtered_out_of_scope,
    }


def ingest_orderbook_batch(
    session: Session,
    *,
    snapshots: list[dict[str, Any]],
    request_url: str,
    raw_output_path: str | None = None,
) -> dict[str, int]:
    """Persist one Kalshi order-book snapshot batch into the database."""
    scrape_run = start_scrape_run(
        session,
        platform_name="kalshi",
        job_name="kalshi-orderbook",
        endpoint_name="orderbook",
        request_url=request_url,
        raw_output_path=raw_output_path,
    )

    records_written = 0
    for item in snapshots:
        if not isinstance(item, dict):
            continue
        market = item.get("market")
        if market is None:
            continue
        snapshot_time = parse_datetime(item.get("snapshot_time")) or parse_datetime(item.get("scraped_at_iso"))

        payload_row = store_api_payload(
            session,
            scrape_run=scrape_run,
            platform_name="kalshi",
            entity_type="orderbook",
            entity_external_id=str(getattr(market, "external_market_ref", None) or getattr(market, "market_slug", None) or ""),
            payload=item.get("raw_payload", item),
            collected_at=snapshot_time,
        )
        insert_orderbook_snapshot(
            session,
            market=market,
            platform_name="kalshi",
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
