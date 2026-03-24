"""Dune-specific ingestion helpers."""

from __future__ import annotations

import re
from hashlib import sha1
from typing import Any

from sqlalchemy.orm import Session

from data_platform.ingest.store import (
    finalize_scrape_run,
    parse_datetime,
    start_scrape_run,
    store_api_payload,
    upsert_market_contract,
    upsert_market_event,
    upsert_user_account,
    insert_transaction_fact,
)


def extract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract result rows from a Dune results payload."""
    if not isinstance(payload, dict):
        return []
    result = payload.get("result")
    if isinstance(result, dict) and isinstance(result.get("rows"), list):
        return [row for row in result["rows"] if isinstance(row, dict)]
    rows = payload.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _slugify(value: str, *, max_length: int = 80) -> str:
    """Create a simple slug suitable for synthetic identifiers."""
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    if not slug:
        return "unknown"
    if len(slug) <= max_length:
        return slug
    return slug[:max_length].rstrip("-")


def _question_event_ref(question: str) -> str:
    """Build a stable synthetic event id for a Dune question string."""
    return f"dune-event-{sha1(question.encode('utf-8')).hexdigest()[:20]}"


def _market_ref(question: str, outcome: str | None) -> str:
    """Build a stable synthetic market id for a Dune row."""
    suffix = outcome or "unknown-outcome"
    basis = f"{question}|{suffix}"
    return f"dune-market-{sha1(basis.encode('utf-8')).hexdigest()[:20]}"


def _source_transaction_id(row: dict[str, Any]) -> str | None:
    """Build a stable source transaction id from a Dune result row."""
    tx_hash = row.get("tx_hash")
    if tx_hash is None:
        return None
    parts = [
        str(tx_hash),
        str(row.get("maker") or ""),
        str(row.get("token_outcome") or row.get("token_outcome_name") or ""),
        str(row.get("maker_action") or row.get("action") or ""),
        str(row.get("block_number") or ""),
    ]
    return ":".join(parts)


def ingest_query_pages(
    session: Session,
    *,
    query_id: str,
    pages: list[dict[str, Any]],
    request_url: str,
    raw_output_path: str | None = None,
) -> dict[str, int]:
    """Persist Dune query result pages into the database."""
    scrape_run = start_scrape_run(
        session,
        platform_name="dune",
        job_name="dune-query-results",
        endpoint_name="query-results",
        request_url=request_url,
        raw_output_path=raw_output_path,
    )

    records_written = 0
    error_count = 0
    error_messages: list[str] = []

    for page in pages:
        payload_row = store_api_payload(
            session,
            scrape_run=scrape_run,
            platform_name="dune",
            entity_type="query_result",
            entity_external_id=str(query_id),
            payload=page,
        )
        for row in extract_rows(page):
            maker = str(row.get("maker") or "").strip().lower()
            question = str(row.get("question") or "").strip()
            source_transaction_id = _source_transaction_id(row)
            if not maker or not question or not source_transaction_id:
                error_count += 1
                if len(error_messages) < 5:
                    error_messages.append("Skipped Dune row missing maker, question, or tx_hash.")
                continue

            outcome_label = str(row.get("token_outcome_name") or row.get("token_outcome") or "").strip() or None
            event_ref = _question_event_ref(question)
            event_slug = _slugify(question)
            market_ref = _market_ref(question, outcome_label)
            market_slug = _slugify(f"{question}-{outcome_label or 'unknown'}")

            user_row = upsert_user_account(
                session,
                platform_name="dune",
                external_user_ref=maker,
                wallet_address=maker if maker.startswith("0x") else None,
                preferred_username=None,
                display_label=maker,
            )
            event_row = upsert_market_event(
                session,
                platform_name="dune",
                external_event_ref=event_ref,
                title=question,
                slug=event_slug,
                is_active=False,
                is_closed=False,
                is_archived=False,
                raw_payload_id=payload_row.payload_id,
            )
            market_row = upsert_market_contract(
                session,
                platform_name="dune",
                event=event_row,
                external_market_ref=market_ref,
                question=question,
                market_slug=market_slug,
                outcome_a_label=outcome_label,
                is_active=False,
                is_closed=False,
                last_trade_price=row.get("price"),
                raw_payload_id=payload_row.payload_id,
            )

            transaction_time = parse_datetime(row.get("block_time"))
            if transaction_time is None:
                error_count += 1
                if len(error_messages) < 5:
                    error_messages.append("Skipped Dune row missing block_time.")
                continue

            insert_transaction_fact(
                session,
                user=user_row,
                market=market_row,
                platform_name="dune",
                source_transaction_id=source_transaction_id,
                transaction_type="onchain_trade",
                transaction_time=transaction_time,
                side=str(row.get("maker_action") or row.get("action") or "").strip() or None,
                outcome_label=outcome_label,
                price=row.get("price"),
                shares=row.get("shares"),
                notional_value=row.get("amount") or row.get("amount_usdc"),
                sequence_ts=row.get("block_number"),
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
        error_summary="; ".join(error_messages) if error_messages else None,
    )
    return {"records_written": records_written, "error_count": error_count}
