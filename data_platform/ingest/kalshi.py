"""Kalshi-specific ingestion helpers."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from data_platform.ingest.store import (
    UNKNOWN_USER_EXTERNAL_REF,
    finalize_scrape_run,
    insert_transaction_fact,
    parse_datetime,
    start_scrape_run,
    store_api_payload,
    upsert_market_contract,
    upsert_market_event,
    upsert_user_account,
)


def ingest_scrape_record(
    session: Session,
    *,
    record: dict[str, Any],
    request_url: str,
    raw_output_path: str | None = None,
) -> dict[str, int]:
    """Persist one Kalshi scrape record, with trade normalization when available."""
    endpoint_name = str(record.get("endpoint") or "custom")
    data = record.get("data") if isinstance(record.get("data"), dict) else {}
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
        payload=record,
        collected_at=parse_datetime(record.get("scraped_at_iso")),
    )

    records_written = 0
    error_count = 0
    if endpoint_name == "trades":
        trades = data.get("trades") if isinstance(data.get("trades"), list) else []
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
            user_row = upsert_user_account(
                session,
                platform_name="kalshi",
                external_user_ref=UNKNOWN_USER_EXTERNAL_REF,
                display_label="Unknown Kalshi participant",
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

    finalize_scrape_run(
        session,
        scrape_run,
        status="success" if error_count == 0 else ("partial" if records_written else "failed"),
        records_written=records_written,
        error_count=error_count,
        error_summary=None,
    )
    return {"records_written": records_written, "error_count": error_count}
