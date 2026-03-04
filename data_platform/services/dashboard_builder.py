"""Derived dashboard snapshot builder for the analytics layer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from data_platform.models import (
    Dashboard,
    DashboardMarket,
    MarketContract,
    MarketProfile,
    TransactionFact,
    UserLeaderboard,
    UserProfile,
)


DEFAULT_MARKET_LIMIT = 25
DEFAULT_USER_LIMIT = 25


def build_dashboard_snapshot(
    session: Session,
    *,
    timeframe: str = "24h",
    scope_label: str = "all_markets",
    market_limit: int = DEFAULT_MARKET_LIMIT,
    user_limit: int = DEFAULT_USER_LIMIT,
) -> dict[str, int]:
    """Build one derived dashboard snapshot from the normalized source layer."""
    now = datetime.now(timezone.utc)
    dashboard = Dashboard(
        dashboard_date=now.date(),
        generated_at=now,
        timeframe=timeframe,
        scope_label=scope_label,
        notes="Auto-generated from normalized source data.",
    )
    session.add(dashboard)
    session.flush()

    market_rows = session.scalars(
        select(MarketContract)
        .order_by(desc(MarketContract.volume), desc(MarketContract.updated_at))
        .limit(market_limit)
    ).all()

    dashboard_market_count = 0
    market_profile_count = 0
    for market in market_rows:
        session.add(
            DashboardMarket(
                dashboard_id=dashboard.dashboard_id,
                market_contract_id=market.market_contract_id,
                market_url=market.market_url,
                market_slug=market.market_slug,
                orderbook_depth=None,
                price=market.last_trade_price,
                volume=market.volume,
                odds=market.last_trade_price,
                read_time=market.updated_at or now,
                whale_count=0,
                trusted_whale_count=0,
                whale_market_focus=None,
                whale_entry_prices=None,
            )
        )
        session.add(
            MarketProfile(
                dashboard_id=dashboard.dashboard_id,
                market_contract_id=market.market_contract_id,
                market_ref=market.external_market_ref,
                realtime_source="normalized_source",
                snapshot_time=market.updated_at or now,
                realtime_payload={
                    "question": market.question,
                    "last_trade_price": float(market.last_trade_price) if market.last_trade_price is not None else None,
                    "volume": float(market.volume) if market.volume is not None else None,
                    "is_active": market.is_active,
                    "is_closed": market.is_closed,
                },
            )
        )
        dashboard_market_count += 1
        market_profile_count += 1

    leaderboard_rows = session.execute(
        select(
            TransactionFact.user_id,
            func.coalesce(func.sum(TransactionFact.notional_value), 0).label("total_notional"),
            func.coalesce(func.sum(TransactionFact.shares), 0).label("total_shares"),
            func.count(TransactionFact.transaction_id).label("trade_count"),
        )
        .group_by(TransactionFact.user_id)
        .order_by(desc("total_notional"))
        .limit(user_limit)
    ).all()

    user_profile_count = 0
    user_leaderboard_count = 0
    for rank, row in enumerate(leaderboard_rows, start=1):
        session.add(
            UserProfile(
                dashboard_id=dashboard.dashboard_id,
                user_id=row.user_id,
                primary_market_ref=None,
                historical_actions_summary={"trade_count": int(row.trade_count)},
                insider_stats={"flagged": False},
                profit_loss=0,
                wallet_balance=None,
                wallet_transactions_summary={"trade_count": int(row.trade_count)},
                markets_invested_summary=None,
                trusted_traders_summary=None,
                preference_probabilities=None,
                total_volume=row.total_notional,
                total_shares=row.total_shares,
                win_rate=None,
                win_rate_chart_type="line",
            )
        )
        session.add(
            UserLeaderboard(
                dashboard_id=dashboard.dashboard_id,
                timeframe=timeframe,
                board_type="public_raw",
                user_id=row.user_id,
                market_contract_id=None,
                rank=rank,
                score_metric="total_notional",
                score_value=row.total_notional,
            )
        )
        user_profile_count += 1
        user_leaderboard_count += 1

    session.flush()
    return {
        "dashboard_id": dashboard.dashboard_id,
        "dashboard_market_count": dashboard_market_count,
        "market_profile_count": market_profile_count,
        "user_profile_count": user_profile_count,
        "user_leaderboard_count": user_leaderboard_count,
    }
