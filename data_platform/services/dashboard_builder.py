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
    OrderbookSnapshot,
    TransactionFact,
    UserAccount,
    UserLeaderboard,
    UserProfile,
)
from data_platform.services.whale_scoring import latest_whale_scores_by_user
from data_platform.services.whale_scoring import load_resolved_user_performance


DEFAULT_MARKET_LIMIT = 25
DEFAULT_USER_LIMIT = 25


def _market_focus_identity(preferred_username: str | None, external_user_ref: str | None) -> str:
    """Return the best available user-facing name for market focus summaries."""
    preferred = (preferred_username or "").strip()
    if preferred:
        return preferred
    fallback = (external_user_ref or "").strip()
    return fallback or "Unknown trader"


def _load_latest_orderbook_depths(session: Session, market_ids: list[int]) -> dict[int, int]:
    """Return the latest observed order-book depth for each tracked market."""
    if not market_ids:
        return {}
    ranked_snapshots = (
        select(
            OrderbookSnapshot.orderbook_snapshot_id.label("orderbook_snapshot_id"),
            func.row_number()
            .over(
                partition_by=OrderbookSnapshot.market_contract_id,
                order_by=(desc(OrderbookSnapshot.snapshot_time), desc(OrderbookSnapshot.orderbook_snapshot_id)),
            )
            .label("rn"),
        )
        .where(OrderbookSnapshot.market_contract_id.in_(market_ids))
        .subquery()
    )
    rows = session.execute(
        select(
            OrderbookSnapshot.market_contract_id,
            OrderbookSnapshot.depth_levels,
        )
        .join(
            ranked_snapshots,
            OrderbookSnapshot.orderbook_snapshot_id == ranked_snapshots.c.orderbook_snapshot_id,
        )
        .where(ranked_snapshots.c.rn == 1)
    ).all()
    return {int(row.market_contract_id): int(row.depth_levels) for row in rows}


def _load_user_trade_aggregates(session: Session) -> dict[int, dict[str, float | int]]:
    """Return aggregate transaction metrics for each user."""
    rows = session.execute(
        select(
            TransactionFact.user_id,
            func.coalesce(func.sum(TransactionFact.notional_value), 0).label("total_notional"),
            func.coalesce(func.sum(TransactionFact.shares), 0).label("total_shares"),
            func.count(TransactionFact.transaction_id).label("trade_count"),
            func.count(func.distinct(TransactionFact.market_contract_id)).label("distinct_markets"),
            func.count(func.distinct(func.date(TransactionFact.transaction_time))).label("active_trade_days"),
        )
        .group_by(TransactionFact.user_id)
    ).all()
    return {
        int(row.user_id): {
            "total_notional": float(row.total_notional or 0),
            "total_shares": float(row.total_shares or 0),
            "trade_count": int(row.trade_count or 0),
            "distinct_markets": int(row.distinct_markets or 0),
            "active_trade_days": int(row.active_trade_days or 0),
        }
        for row in rows
    }


def _load_market_whale_activity(
    session: Session,
    *,
    market_ids: list[int],
    latest_scores: dict[int, Any],
) -> dict[int, dict[str, Any]]:
    """Return trade-based whale activity summaries for dashboard market rows."""
    if not market_ids:
        return {}

    rows = session.execute(
        select(
            TransactionFact.market_contract_id,
            TransactionFact.user_id,
            UserAccount.preferred_username,
            UserAccount.external_user_ref,
            func.count(TransactionFact.transaction_id).label("trade_count"),
            func.coalesce(func.sum(TransactionFact.notional_value), 0).label("total_notional"),
            func.avg(TransactionFact.price).label("avg_trade_price"),
        )
        .join(UserAccount, UserAccount.user_id == TransactionFact.user_id)
        .where(TransactionFact.market_contract_id.in_(market_ids))
        .group_by(
            TransactionFact.market_contract_id,
            TransactionFact.user_id,
            UserAccount.preferred_username,
            UserAccount.external_user_ref,
        )
    ).all()

    activity: dict[int, dict[str, Any]] = {
        market_id: {
            "whale_users": set(),
            "trusted_users": set(),
            "focus_refs": [],
            "whale_entry_prices": [],
        }
        for market_id in market_ids
    }
    for row in rows:
        score = latest_scores.get(int(row.user_id))
        if score is None:
            continue
        market_activity = activity.setdefault(
            int(row.market_contract_id),
            {"whale_users": set(), "trusted_users": set(), "focus_refs": [], "whale_entry_prices": []},
        )
        if score.is_whale:
            market_activity["whale_users"].add(int(row.user_id))
            identity_label = _market_focus_identity(row.preferred_username, row.external_user_ref)
            market_activity["focus_refs"].append((float(score.trust_score or 0), identity_label))
            market_activity["whale_entry_prices"].append(
                {
                    "user_id": int(row.user_id),
                    "external_user_ref": str(row.external_user_ref),
                    "preferred_username": row.preferred_username,
                    "display_name": identity_label,
                    "avg_trade_price": float(row.avg_trade_price) if row.avg_trade_price is not None else None,
                    "trade_count": int(row.trade_count or 0),
                    "total_notional": float(row.total_notional or 0),
                    "is_trusted_whale": bool(score.is_trusted_whale),
                }
            )
        if score.is_trusted_whale:
            market_activity["trusted_users"].add(int(row.user_id))

    summary: dict[int, dict[str, Any]] = {}
    for market_id, item in activity.items():
        focus_refs = [ref for _, ref in sorted(item["focus_refs"], reverse=True)[:3]]
        summary[market_id] = {
            "whale_count": len(item["whale_users"]),
            "trusted_whale_count": len(item["trusted_users"]),
            "whale_market_focus": ", ".join(focus_refs) if focus_refs else None,
            "whale_entry_prices": item["whale_entry_prices"] or None,
        }
    return summary


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
    latest_scores = latest_whale_scores_by_user(session)
    resolved_performance_by_user, _ = load_resolved_user_performance(session)
    score_note = (
        "Includes latest whale score snapshot."
        if latest_scores
        else "No whale score snapshot available; whale counts default to zero."
    )
    dashboard = Dashboard(
        dashboard_date=now.date(),
        generated_at=now,
        timeframe=timeframe,
        scope_label=scope_label,
        notes=f"Auto-generated from normalized source data. {score_note}",
    )
    session.add(dashboard)
    session.flush()

    market_rows = session.scalars(
        select(MarketContract)
        .order_by(desc(MarketContract.volume), desc(MarketContract.updated_at))
        .limit(market_limit)
    ).all()
    market_ids = [market.market_contract_id for market in market_rows]
    latest_orderbook_depths = _load_latest_orderbook_depths(session, market_ids)
    market_whale_activity = _load_market_whale_activity(session, market_ids=market_ids, latest_scores=latest_scores)

    dashboard_market_count = 0
    market_profile_count = 0
    for market in market_rows:
        whale_activity = market_whale_activity.get(
            market.market_contract_id,
            {
                "whale_count": 0,
                "trusted_whale_count": 0,
                "whale_market_focus": None,
                "whale_entry_prices": None,
            },
        )
        session.add(
            DashboardMarket(
                dashboard_id=dashboard.dashboard_id,
                market_contract_id=market.market_contract_id,
                market_url=market.market_url,
                market_slug=market.market_slug,
                orderbook_depth=latest_orderbook_depths.get(market.market_contract_id),
                price=market.last_trade_price,
                volume=market.volume,
                odds=market.last_trade_price,
                read_time=market.updated_at or now,
                whale_count=whale_activity["whale_count"],
                trusted_whale_count=whale_activity["trusted_whale_count"],
                whale_market_focus=whale_activity["whale_market_focus"],
                whale_entry_prices=whale_activity["whale_entry_prices"],
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

    user_trade_aggregates = _load_user_trade_aggregates(session)
    scored_users = [
        (user_id, score)
        for user_id, score in latest_scores.items()
        if user_id in user_trade_aggregates
    ]
    scored_users.sort(
        key=lambda item: (
            float(item[1].trust_score or 0),
            float(user_trade_aggregates[item[0]]["total_notional"]),
            int(item[1].sample_trade_count),
        ),
        reverse=True,
    )

    user_profile_count = 0
    user_leaderboard_count = 0
    for rank, (user_id, score) in enumerate(scored_users[:user_limit], start=1):
        aggregates = user_trade_aggregates[user_id]
        resolved_performance = resolved_performance_by_user.get(user_id)
        resolved_summary = {
            "resolved_market_count": int(resolved_performance.resolved_market_count),
            "winning_market_count": int(resolved_performance.winning_market_count),
            "realized_pnl": float(resolved_performance.realized_pnl),
            "realized_roi": float(resolved_performance.realized_roi),
            "excluded_market_count": int(resolved_performance.excluded_market_count),
            "win_rate": (
                round(
                    resolved_performance.winning_market_count / resolved_performance.resolved_market_count,
                    6,
                )
                if resolved_performance.resolved_market_count > 0
                else None
            ),
        } if resolved_performance is not None else {
            "resolved_market_count": 0,
            "winning_market_count": 0,
            "realized_pnl": 0.0,
            "realized_roi": 0.0,
            "excluded_market_count": 0,
            "win_rate": None,
        }
        session.add(
            UserProfile(
                dashboard_id=dashboard.dashboard_id,
                user_id=user_id,
                primary_market_ref=None,
                historical_actions_summary={
                    "trade_count": int(aggregates["trade_count"]),
                    "distinct_markets": int(aggregates["distinct_markets"]),
                    "active_trade_days": int(aggregates["active_trade_days"]),
                    "scoring_version": score.scoring_version,
                },
                insider_stats={
                    "flagged": bool(score.insider_penalty and float(score.insider_penalty) > 0),
                    "penalty": float(score.insider_penalty or 0),
                },
                profit_loss=0,
                wallet_balance=None,
                wallet_transactions_summary={
                    "trade_count": int(aggregates["trade_count"]),
                    "sample_trade_count": int(score.sample_trade_count),
                },
                markets_invested_summary={"distinct_markets": int(aggregates["distinct_markets"])},
                trusted_traders_summary={
                    "raw_volume_score": float(score.raw_volume_score or 0),
                    "consistency_score": float(score.consistency_score or 0),
                    "profitability_score": float(score.profitability_score or 0),
                    "trust_score": float(score.trust_score or 0),
                    "is_whale": bool(score.is_whale),
                    "is_trusted_whale": bool(score.is_trusted_whale),
                    "resolved_performance": resolved_summary,
                },
                preference_probabilities=None,
                total_volume=aggregates["total_notional"],
                total_shares=aggregates["total_shares"],
                win_rate=None,
                win_rate_chart_type="line",
            )
        )
        session.add(
            UserLeaderboard(
                dashboard_id=dashboard.dashboard_id,
                timeframe=timeframe,
                board_type="public_raw",
                user_id=user_id,
                market_contract_id=None,
                rank=rank,
                score_metric="trust_score",
                score_value=score.trust_score,
            )
        )
        user_profile_count += 1
        user_leaderboard_count += 1

    trusted_users = [item for item in scored_users if item[1].is_trusted_whale]
    for rank, (user_id, score) in enumerate(trusted_users[:user_limit], start=1):
        session.add(
            UserLeaderboard(
                dashboard_id=dashboard.dashboard_id,
                timeframe=timeframe,
                board_type="internal_trusted",
                user_id=user_id,
                market_contract_id=None,
                rank=rank,
                score_metric="trust_score",
                score_value=score.trust_score,
            )
        )
        user_leaderboard_count += 1

    session.flush()
    return {
        "dashboard_id": dashboard.dashboard_id,
        "dashboard_market_count": dashboard_market_count,
        "market_profile_count": market_profile_count,
        "user_profile_count": user_profile_count,
        "user_leaderboard_count": user_leaderboard_count,
    }
