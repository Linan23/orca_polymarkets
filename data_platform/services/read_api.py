"""Read-only query helpers for the internal FastAPI endpoints."""

from __future__ import annotations

from typing import Any

from sqlalchemy import desc, func, select, text
from sqlalchemy.orm import Session

from data_platform.models import (
    Dashboard,
    DashboardMarket,
    MarketContract,
    PositionSnapshot,
    ScrapeRun,
    TransactionFact,
    UserAccount,
    UserLeaderboard,
)


DEFAULT_LIMIT = 50


def database_health(session: Session) -> bool:
    """Return whether the database responds to a trivial query."""
    session.execute(text("SELECT 1"))
    return True


def latest_scrape_run(session: Session) -> dict[str, Any] | None:
    """Return the latest scrape run summary."""
    row = session.scalars(select(ScrapeRun).order_by(desc(ScrapeRun.started_at)).limit(1)).first()
    if row is None:
        return None
    return {
        "scrape_run_id": row.scrape_run_id,
        "job_name": row.job_name,
        "endpoint_name": row.endpoint_name,
        "status": row.status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "records_written": row.records_written,
        "error_count": row.error_count,
        "error_summary": row.error_summary,
    }


def list_markets(session: Session, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    """Return recent market contracts."""
    rows = session.scalars(select(MarketContract).order_by(desc(MarketContract.updated_at)).limit(limit)).all()
    return [
        {
            "market_contract_id": row.market_contract_id,
            "external_market_ref": row.external_market_ref,
            "market_slug": row.market_slug,
            "question": row.question,
            "is_active": row.is_active,
            "is_closed": row.is_closed,
            "last_trade_price": float(row.last_trade_price) if row.last_trade_price is not None else None,
            "volume": float(row.volume) if row.volume is not None else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        for row in rows
    ]


def list_users(session: Session, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    """Return recent users."""
    rows = session.scalars(select(UserAccount).order_by(desc(UserAccount.updated_at)).limit(limit)).all()
    return [
        {
            "user_id": row.user_id,
            "external_user_ref": row.external_user_ref,
            "wallet_address": row.wallet_address,
            "display_label": row.display_label,
            "is_active": row.is_active,
            "is_likely_insider": row.is_likely_insider,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        for row in rows
    ]


def list_transactions(session: Session, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    """Return recent normalized transactions."""
    rows = session.scalars(select(TransactionFact).order_by(desc(TransactionFact.transaction_time)).limit(limit)).all()
    return [
        {
            "transaction_id": row.transaction_id,
            "user_id": row.user_id,
            "market_contract_id": row.market_contract_id,
            "source_transaction_id": row.source_transaction_id,
            "transaction_type": row.transaction_type,
            "side": row.side,
            "price": float(row.price) if row.price is not None else None,
            "shares": float(row.shares) if row.shares is not None else None,
            "notional_value": float(row.notional_value) if row.notional_value is not None else None,
            "transaction_time": row.transaction_time.isoformat() if row.transaction_time else None,
        }
        for row in rows
    ]


def list_positions(session: Session, limit: int = DEFAULT_LIMIT) -> list[dict[str, Any]]:
    """Return recent position snapshots."""
    rows = session.scalars(
        select(PositionSnapshot).order_by(desc(PositionSnapshot.snapshot_time), desc(PositionSnapshot.position_snapshot_id)).limit(limit)
    ).all()
    return [
        {
            "position_snapshot_id": row.position_snapshot_id,
            "user_id": row.user_id,
            "market_contract_id": row.market_contract_id,
            "event_id": row.event_id,
            "position_size": float(row.position_size) if row.position_size is not None else None,
            "avg_entry_price": float(row.avg_entry_price) if row.avg_entry_price is not None else None,
            "current_mark_price": float(row.current_mark_price) if row.current_mark_price is not None else None,
            "market_value": float(row.market_value) if row.market_value is not None else None,
            "cash_pnl": float(row.cash_pnl) if row.cash_pnl is not None else None,
            "realized_pnl": float(row.realized_pnl) if row.realized_pnl is not None else None,
            "unrealized_pnl": float(row.unrealized_pnl) if row.unrealized_pnl is not None else None,
            "is_redeemable": row.is_redeemable,
            "is_mergeable": row.is_mergeable,
            "snapshot_time": row.snapshot_time.isoformat() if row.snapshot_time else None,
        }
        for row in rows
    ]


def latest_leaderboard(session: Session) -> dict[str, Any] | None:
    """Return leaderboard rows for the latest dashboard snapshot."""
    dashboard = session.scalars(select(Dashboard).order_by(desc(Dashboard.generated_at)).limit(1)).first()
    if dashboard is None:
        return None
    rows = session.scalars(
        select(UserLeaderboard)
        .where(UserLeaderboard.dashboard_id == dashboard.dashboard_id)
        .order_by(UserLeaderboard.rank.asc())
        .limit(DEFAULT_LIMIT)
    ).all()
    return {
        "dashboard_id": dashboard.dashboard_id,
        "generated_at": dashboard.generated_at.isoformat(),
        "timeframe": dashboard.timeframe,
        "rows": [
            {
                "leaderboard_id": row.leaderboard_id,
                "user_id": row.user_id,
                "board_type": row.board_type,
                "rank": row.rank,
                "score_metric": row.score_metric,
                "score_value": float(row.score_value) if row.score_value is not None else None,
            }
            for row in rows
        ],
    }


def latest_dashboard_snapshot(session: Session) -> dict[str, Any] | None:
    """Return a summary of the latest dashboard snapshot."""
    dashboard = session.scalars(select(Dashboard).order_by(desc(Dashboard.generated_at)).limit(1)).first()
    if dashboard is None:
        return None
    market_count = session.scalar(
        select(func.count(DashboardMarket.market_id)).where(DashboardMarket.dashboard_id == dashboard.dashboard_id)
    )
    return {
        "dashboard_id": dashboard.dashboard_id,
        "dashboard_date": dashboard.dashboard_date.isoformat(),
        "generated_at": dashboard.generated_at.isoformat(),
        "timeframe": dashboard.timeframe,
        "scope_label": dashboard.scope_label,
        "market_count": int(market_count or 0),
    }
