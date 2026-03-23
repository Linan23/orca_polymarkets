"""Read-only query helpers for the internal FastAPI endpoints."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc, func, select, text
from sqlalchemy.orm import Session

from data_platform.models import (
    Dashboard,
    DashboardMarket,
    MarketContract,
    MarketProfile,
    OrderbookSnapshot,
    Platform,
    PositionSnapshot,
    ScrapeRun,
    TransactionFact,
    UserAccount,
    UserLeaderboard,
    UserProfile,
    WhaleScoreSnapshot,
)
from data_platform.services.whale_scoring import load_resolved_user_performance


DEFAULT_LIMIT = 50
VALID_TIMEFRAMES = {"7d": 7, "30d": 30, "90d": 90, "all": None}


def timeframe_start(timeframe: str) -> datetime | None:
    """Return the UTC lower-bound datetime for a supported timeframe label."""
    if timeframe not in VALID_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe '{timeframe}'. Use one of: {', '.join(sorted(VALID_TIMEFRAMES))}.")
    days = VALID_TIMEFRAMES[timeframe]
    if days is None:
        return None
    return datetime.now(timezone.utc) - timedelta(days=days)


def _latest_whale_batch(session: Session) -> tuple[Any, Any] | None:
    """Return the latest coherent whale-score batch identifiers."""
    return session.execute(
        select(
            WhaleScoreSnapshot.snapshot_time,
            WhaleScoreSnapshot.scoring_version,
        )
        .order_by(
            desc(WhaleScoreSnapshot.snapshot_time),
            desc(WhaleScoreSnapshot.whale_score_snapshot_id),
        )
        .limit(1)
    ).first()


def _resolved_summary(session: Session, user_id: int) -> dict[str, Any]:
    """Return resolved-market performance details for one user."""
    resolved_performance_by_user, _ = load_resolved_user_performance(session)
    resolved = resolved_performance_by_user.get(user_id)
    if resolved is None:
        return {
            "resolved_market_count": 0,
            "winning_market_count": 0,
            "realized_pnl": 0.0,
            "realized_roi": 0.0,
            "excluded_market_count": 0,
            "win_rate": None,
        }
    return {
        "resolved_market_count": int(resolved.resolved_market_count),
        "winning_market_count": int(resolved.winning_market_count),
        "realized_pnl": float(resolved.realized_pnl),
        "realized_roi": float(resolved.realized_roi),
        "excluded_market_count": int(resolved.excluded_market_count),
        "win_rate": (
            round(resolved.winning_market_count / resolved.resolved_market_count, 6)
            if resolved.resolved_market_count > 0
            else None
        ),
    }


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


def latest_leaderboard(session: Session, *, board_type: str | None = None) -> dict[str, Any] | None:
    """Return leaderboard rows for the latest dashboard snapshot."""
    dashboard = session.scalars(select(Dashboard).order_by(desc(Dashboard.generated_at)).limit(1)).first()
    if dashboard is None:
        return None
    statement = (
        select(UserLeaderboard)
        .where(UserLeaderboard.dashboard_id == dashboard.dashboard_id)
        .order_by(UserLeaderboard.board_type.asc(), UserLeaderboard.rank.asc())
        .limit(DEFAULT_LIMIT)
    )
    if board_type:
        statement = (
            select(UserLeaderboard)
            .where(
                UserLeaderboard.dashboard_id == dashboard.dashboard_id,
                UserLeaderboard.board_type == board_type,
            )
            .order_by(UserLeaderboard.rank.asc())
            .limit(DEFAULT_LIMIT)
        )
    rows = session.scalars(statement).all()
    user_refs = {
        row.user_id: row.external_user_ref
        for row in session.scalars(
            select(UserAccount).where(UserAccount.user_id.in_([row.user_id for row in rows]))
        ).all()
    } if rows else {}
    return {
        "dashboard_id": dashboard.dashboard_id,
        "generated_at": dashboard.generated_at.isoformat(),
        "timeframe": dashboard.timeframe,
        "board_type": board_type or "all",
        "rows": [
            {
                "leaderboard_id": row.leaderboard_id,
                "user_id": row.user_id,
                "external_user_ref": user_refs.get(row.user_id),
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


def latest_dashboard_markets(session: Session, limit: int = DEFAULT_LIMIT) -> dict[str, Any] | None:
    """Return latest derived market rows from the dashboard layer."""
    dashboard = session.scalars(select(Dashboard).order_by(desc(Dashboard.generated_at)).limit(1)).first()
    if dashboard is None:
        return None
    rows = session.execute(
        select(DashboardMarket, MarketContract)
        .join(MarketContract, MarketContract.market_contract_id == DashboardMarket.market_contract_id)
        .where(DashboardMarket.dashboard_id == dashboard.dashboard_id)
        .order_by(
            desc(DashboardMarket.trusted_whale_count),
            desc(DashboardMarket.whale_count),
            desc(DashboardMarket.volume),
            DashboardMarket.market_id.asc(),
        )
        .limit(limit)
    ).all()
    items = [
        {
            "market_id": market.market_id,
            "market_contract_id": market.market_contract_id,
            "market_slug": market.market_slug,
            "market_url": market.market_url,
            "question": contract.question,
            "price": float(market.price) if market.price is not None else None,
            "volume": float(market.volume) if market.volume is not None else None,
            "odds": float(market.odds) if market.odds is not None else None,
            "orderbook_depth": int(market.orderbook_depth or 0) if market.orderbook_depth is not None else None,
            "whale_count": int(market.whale_count or 0),
            "trusted_whale_count": int(market.trusted_whale_count or 0),
            "whale_market_focus": market.whale_market_focus,
            "read_time": market.read_time.isoformat() if market.read_time else None,
        }
        for market, contract in rows
    ]
    return {
        "dashboard_id": dashboard.dashboard_id,
        "generated_at": dashboard.generated_at.isoformat(),
        "count": len(items),
        "items": items,
    }


def home_summary(session: Session) -> dict[str, Any]:
    """Return a compact homepage summary for the React dashboard."""
    latest_batch = _latest_whale_batch(session)
    whales_detected = 0
    trusted_whales = 0
    top_trusted_whale: dict[str, Any] | None = None
    scoring_version: str | None = None

    if latest_batch is not None:
        rows = session.execute(
            select(WhaleScoreSnapshot, UserAccount)
            .join(UserAccount, UserAccount.user_id == WhaleScoreSnapshot.user_id)
            .where(
                WhaleScoreSnapshot.snapshot_time == latest_batch.snapshot_time,
                WhaleScoreSnapshot.scoring_version == latest_batch.scoring_version,
            )
            .order_by(desc(WhaleScoreSnapshot.trust_score), desc(WhaleScoreSnapshot.sample_trade_count))
        ).all()
        whales_detected = sum(1 for score, _ in rows if score.is_whale)
        trusted_whales = sum(1 for score, _ in rows if score.is_trusted_whale)
        scoring_version = latest_batch.scoring_version
        trusted_rows = [(score, account) for score, account in rows if score.is_trusted_whale]
        if trusted_rows:
            score, account = trusted_rows[0]
            top_trusted_whale = {
                "user_id": account.user_id,
                "external_user_ref": account.external_user_ref,
                "trust_score": float(score.trust_score or 0),
                "profitability_score": float(score.profitability_score or 0),
                "sample_trade_count": int(score.sample_trade_count or 0),
            }

    latest_market_block = latest_dashboard_markets(session, limit=1)
    most_whale_concentrated_market = None
    if latest_market_block and latest_market_block["items"]:
        market = latest_market_block["items"][0]
        most_whale_concentrated_market = {
            "market_slug": market["market_slug"],
            "question": market["question"],
            "whale_count": market["whale_count"],
            "trusted_whale_count": market["trusted_whale_count"],
            "price": market["price"],
        }

    _, profitability_summary = load_resolved_user_performance(session)

    platform_rows = session.scalars(select(Platform).order_by(Platform.platform_name.asc())).all()
    platform_coverage: list[dict[str, Any]] = []
    for platform in platform_rows:
        platform_coverage.append(
            {
                "platform_name": platform.platform_name,
                "user_count": int(
                    session.scalar(
                        select(func.count(UserAccount.user_id)).where(UserAccount.platform_id == platform.platform_id)
                    )
                    or 0
                ),
                "market_count": int(
                    session.scalar(
                        select(func.count(MarketContract.market_contract_id)).where(
                            MarketContract.platform_id == platform.platform_id
                        )
                    )
                    or 0
                ),
                "transaction_count": int(
                    session.scalar(
                        select(func.count(TransactionFact.transaction_id)).where(
                            TransactionFact.platform_id == platform.platform_id
                        )
                    )
                    or 0
                ),
                "orderbook_snapshot_count": int(
                    session.scalar(
                        select(func.count(OrderbookSnapshot.orderbook_snapshot_id)).where(
                            OrderbookSnapshot.platform_id == platform.platform_id
                        )
                    )
                    or 0
                ),
            }
        )

    return {
        "scoring_version": scoring_version,
        "whales_detected": whales_detected,
        "trusted_whales": trusted_whales,
        "resolved_markets_available": profitability_summary["resolved_markets_available"],
        "resolved_markets_observed": profitability_summary["resolved_markets_observed"],
        "profitability_users": profitability_summary["profitability_users"],
        "top_trusted_whale": top_trusted_whale,
        "most_whale_concentrated_market": most_whale_concentrated_market,
        "latest_ingestion": latest_scrape_run(session),
        "platform_coverage": platform_coverage,
    }


def top_profitable_resolved_users(
    session: Session,
    limit: int = DEFAULT_LIMIT,
    *,
    timeframe: str = "all",
) -> dict[str, Any]:
    """Return the top Polymarket users ranked by conservative resolved-market profitability."""
    start_time = timeframe_start(timeframe)
    resolved_performance_by_user, profitability_summary = load_resolved_user_performance(
        session,
        start_time=start_time,
    )
    latest_batch = _latest_whale_batch(session)
    if latest_batch is None:
        return {
            "scope": "polymarket_users",
            "timeframe": timeframe,
            "scoring_version": None,
            "resolved_markets_observed": profitability_summary["resolved_markets_observed"],
            "count": 0,
            "items": [],
        }

    score_rows = session.execute(
        select(WhaleScoreSnapshot, UserAccount, Platform)
        .join(UserAccount, UserAccount.user_id == WhaleScoreSnapshot.user_id)
        .join(Platform, Platform.platform_id == WhaleScoreSnapshot.platform_id)
        .where(
            WhaleScoreSnapshot.snapshot_time == latest_batch.snapshot_time,
            WhaleScoreSnapshot.scoring_version == latest_batch.scoring_version,
        )
    ).all()
    score_by_user = {
        account.user_id: {
            "score": score,
            "account": account,
            "platform": platform,
        }
        for score, account, platform in score_rows
    }

    ranked_items: list[dict[str, Any]] = []
    for user_id, resolved in resolved_performance_by_user.items():
        if resolved.resolved_market_count <= 0 or resolved.realized_pnl <= 0:
            continue
        row = score_by_user.get(user_id)
        if row is None:
            continue
        win_rate = (
            resolved.winning_market_count / resolved.resolved_market_count
            if resolved.resolved_market_count > 0
            else None
        )
        ranked_items.append(
            {
                "user_id": int(row["account"].user_id),
                "external_user_ref": row["account"].external_user_ref,
                "platform_name": row["platform"].platform_name,
                "resolved_market_count": int(resolved.resolved_market_count),
                "winning_market_count": int(resolved.winning_market_count),
                "realized_pnl": float(resolved.realized_pnl),
                "realized_roi": float(resolved.realized_roi),
                "win_rate": round(win_rate, 6) if win_rate is not None else None,
                "trust_score": float(row["score"].trust_score or 0),
                "profitability_score": float(row["score"].profitability_score or 0),
                "is_whale": bool(row["score"].is_whale),
                "is_trusted_whale": bool(row["score"].is_trusted_whale),
            }
        )

    ranked_items.sort(
        key=lambda item: (
            item["realized_pnl"],
            item["realized_roi"],
            item["win_rate"] if item["win_rate"] is not None else -1.0,
            item["trust_score"],
        ),
        reverse=True,
    )
    items = ranked_items[:limit]
    return {
        "scope": "polymarket_users",
        "timeframe": timeframe,
        "scoring_version": latest_batch.scoring_version,
        "resolved_markets_observed": profitability_summary["resolved_markets_observed"],
        "count": len(items),
        "items": items,
    }


def market_whale_concentration(
    session: Session,
    limit: int = DEFAULT_LIMIT,
    *,
    timeframe: str = "all",
) -> dict[str, Any] | None:
    """Return the most whale-concentrated markets within a timeframe."""
    latest_batch = _latest_whale_batch(session)
    if latest_batch is None:
        return None
    start_time = timeframe_start(timeframe)
    transaction_window_clause = ""
    params: dict[str, Any] = {
        "snapshot_time": latest_batch.snapshot_time,
        "scoring_version": latest_batch.scoring_version,
        "limit": limit,
    }
    if start_time is not None:
        transaction_window_clause = "AND tf.transaction_time >= :start_time"
        params["start_time"] = start_time

    rows = session.execute(
        text(
            f"""
            WITH latest_scores AS (
              SELECT
                w.user_id,
                w.is_whale,
                w.is_trusted_whale
              FROM analytics.whale_score_snapshot w
              WHERE w.snapshot_time = :snapshot_time
                AND w.scoring_version = :scoring_version
            )
            SELECT
              mc.market_contract_id,
              mc.market_slug,
              mc.market_url,
              mc.question,
              mc.last_trade_price,
              mc.volume,
              p.platform_name,
              COUNT(DISTINCT CASE WHEN ls.is_whale THEN tf.user_id END) AS whale_count,
              COUNT(DISTINCT CASE WHEN ls.is_trusted_whale THEN tf.user_id END) AS trusted_whale_count,
              MAX(tf.transaction_time) AS last_seen_trade_time
            FROM analytics.transaction_fact tf
            JOIN latest_scores ls
              ON ls.user_id = tf.user_id
            JOIN analytics.market_contract mc
              ON mc.market_contract_id = tf.market_contract_id
            JOIN analytics.platform p
              ON p.platform_id = mc.platform_id
            WHERE (ls.is_whale = TRUE OR ls.is_trusted_whale = TRUE)
              {transaction_window_clause}
            GROUP BY
              mc.market_contract_id,
              mc.market_slug,
              mc.market_url,
              mc.question,
              mc.last_trade_price,
              mc.volume,
              p.platform_name
            ORDER BY
              trusted_whale_count DESC,
              whale_count DESC,
              mc.volume DESC NULLS LAST,
              mc.market_contract_id ASC
            LIMIT :limit
            """
        )
        ,
        params,
    ).mappings().all()
    items = [
        {
            "market_id": int(row["market_contract_id"]),
            "market_contract_id": int(row["market_contract_id"]),
            "platform_name": row["platform_name"],
            "market_slug": row["market_slug"],
            "market_url": row["market_url"],
            "question": row["question"],
            "price": float(row["last_trade_price"]) if row["last_trade_price"] is not None else None,
            "volume": float(row["volume"]) if row["volume"] is not None else None,
            "whale_count": int(row["whale_count"] or 0),
            "trusted_whale_count": int(row["trusted_whale_count"] or 0),
            "orderbook_depth": None,
            "read_time": row["last_seen_trade_time"].isoformat() if row["last_seen_trade_time"] else None,
        }
        for row in rows
    ]
    return {
        "dashboard_id": None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "cross_platform_markets",
        "timeframe": timeframe,
        "count": len(items),
        "items": items,
    }


def whale_entry_behavior(
    session: Session,
    limit: int = DEFAULT_LIMIT,
    *,
    timeframe: str = "all",
) -> dict[str, Any] | None:
    """Return entry-price behavior for whale-labelled Polymarket users."""
    latest_batch = _latest_whale_batch(session)
    if latest_batch is None:
        return None
    start_time = timeframe_start(timeframe)
    transaction_window_clause = ""
    params: dict[str, Any] = {
        "snapshot_time": latest_batch.snapshot_time,
        "scoring_version": latest_batch.scoring_version,
        "limit": limit,
    }
    if start_time is not None:
        transaction_window_clause = "AND tf.transaction_time >= :start_time"
        params["start_time"] = start_time

    rows = session.execute(
        text(
            f"""
            WITH latest_scores AS (
              SELECT
                w.user_id,
                w.trust_score,
                w.profitability_score,
                w.is_whale,
                w.is_trusted_whale
              FROM analytics.whale_score_snapshot w
              JOIN analytics.platform p
                ON p.platform_id = w.platform_id
              WHERE w.snapshot_time = :snapshot_time
                AND w.scoring_version = :scoring_version
                AND p.platform_name = 'polymarket'
                AND (w.is_whale = TRUE OR w.is_trusted_whale = TRUE)
            )
            SELECT
              ua.user_id,
              ua.external_user_ref,
              ls.trust_score,
              ls.profitability_score,
              ls.is_whale,
              ls.is_trusted_whale,
              COUNT(tf.transaction_id) AS entry_trade_count,
              COUNT(DISTINCT tf.market_contract_id) AS distinct_markets,
              COALESCE(SUM(tf.shares), 0) AS total_entry_shares,
              COALESCE(SUM(tf.notional_value), 0) AS total_entry_notional,
              COALESCE(SUM(tf.notional_value) / NULLIF(SUM(tf.shares), 0), 0) AS weighted_avg_entry_price,
              COALESCE(AVG(tf.shares), 0) AS avg_entry_shares,
              COALESCE(MIN(tf.price), 0) AS min_entry_price,
              COALESCE(MAX(tf.price), 0) AS max_entry_price
            FROM analytics.transaction_fact tf
            JOIN latest_scores ls
              ON ls.user_id = tf.user_id
            JOIN analytics.user_account ua
              ON ua.user_id = tf.user_id
            WHERE tf.side = 'buy'
              AND tf.price IS NOT NULL
              AND tf.shares IS NOT NULL
              {transaction_window_clause}
            GROUP BY
              ua.user_id,
              ua.external_user_ref,
              ls.trust_score,
              ls.profitability_score,
              ls.is_whale,
              ls.is_trusted_whale
            ORDER BY
              entry_trade_count DESC,
              distinct_markets DESC,
              ls.trust_score DESC,
              ua.user_id ASC
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    items = [
        {
            "user_id": int(row["user_id"]),
            "external_user_ref": row["external_user_ref"],
            "trust_score": float(row["trust_score"] or 0),
            "profitability_score": float(row["profitability_score"] or 0),
            "is_whale": bool(row["is_whale"]),
            "is_trusted_whale": bool(row["is_trusted_whale"]),
            "entry_trade_count": int(row["entry_trade_count"] or 0),
            "distinct_markets": int(row["distinct_markets"] or 0),
            "total_entry_shares": float(row["total_entry_shares"] or 0),
            "total_entry_notional": float(row["total_entry_notional"] or 0),
            "weighted_avg_entry_price": float(row["weighted_avg_entry_price"] or 0),
            "avg_entry_shares": float(row["avg_entry_shares"] or 0),
            "min_entry_price": float(row["min_entry_price"] or 0),
            "max_entry_price": float(row["max_entry_price"] or 0),
        }
        for row in rows
    ]
    return {
        "scope": "polymarket_whales",
        "timeframe": timeframe,
        "scoring_version": latest_batch.scoring_version,
        "count": len(items),
        "items": items,
    }


def latest_market_profile(session: Session, market_slug: str) -> dict[str, Any] | None:
    """Return latest dashboard-backed market profile details for one market slug."""
    dashboard = session.scalars(select(Dashboard).order_by(desc(Dashboard.generated_at)).limit(1)).first()
    if dashboard is None:
        return None
    row = session.execute(
        select(DashboardMarket, MarketContract, MarketProfile)
        .join(MarketContract, MarketContract.market_contract_id == DashboardMarket.market_contract_id)
        .join(
            MarketProfile,
            (MarketProfile.dashboard_id == DashboardMarket.dashboard_id)
            & (MarketProfile.market_contract_id == DashboardMarket.market_contract_id),
        )
        .where(
            DashboardMarket.dashboard_id == dashboard.dashboard_id,
            DashboardMarket.market_slug == market_slug,
        )
        .limit(1)
    ).first()
    if row is None:
        return None
    market, contract, profile = row
    return {
        "dashboard_id": dashboard.dashboard_id,
        "market_id": market.market_id,
        "market_contract_id": market.market_contract_id,
        "market_slug": market.market_slug,
        "market_url": market.market_url,
        "question": contract.question,
        "price": float(market.price) if market.price is not None else None,
        "volume": float(market.volume) if market.volume is not None else None,
        "odds": float(market.odds) if market.odds is not None else None,
        "orderbook_depth": int(market.orderbook_depth or 0) if market.orderbook_depth is not None else None,
        "whale_count": int(market.whale_count or 0),
        "trusted_whale_count": int(market.trusted_whale_count or 0),
        "whale_market_focus": market.whale_market_focus,
        "read_time": market.read_time.isoformat() if market.read_time else None,
        "realtime_source": profile.realtime_source,
        "snapshot_time": profile.snapshot_time.isoformat() if profile.snapshot_time else None,
        "realtime_payload": profile.realtime_payload,
    }


def latest_whale_scores(
    session: Session,
    *,
    limit: int = DEFAULT_LIMIT,
    whales_only: bool = False,
    trusted_only: bool = False,
) -> dict[str, Any] | None:
    """Return the latest whale-score rows with optional whale/trusted filters."""
    latest_batch = _latest_whale_batch(session)
    if latest_batch is None:
        return None

    statement = (
        select(WhaleScoreSnapshot, UserAccount, Platform)
        .join(UserAccount, UserAccount.user_id == WhaleScoreSnapshot.user_id)
        .join(Platform, Platform.platform_id == WhaleScoreSnapshot.platform_id)
        .where(
            WhaleScoreSnapshot.snapshot_time == latest_batch.snapshot_time,
            WhaleScoreSnapshot.scoring_version == latest_batch.scoring_version,
        )
        .order_by(
            desc(WhaleScoreSnapshot.is_trusted_whale),
            desc(WhaleScoreSnapshot.is_whale),
            desc(WhaleScoreSnapshot.trust_score),
            desc(WhaleScoreSnapshot.sample_trade_count),
        )
        .limit(limit)
    )
    if trusted_only:
        statement = statement.where(WhaleScoreSnapshot.is_trusted_whale.is_(True))
    elif whales_only:
        statement = statement.where(WhaleScoreSnapshot.is_whale.is_(True))

    rows = session.execute(statement).all()
    items = [
        {
            "user_id": account.user_id,
            "external_user_ref": account.external_user_ref,
            "platform_name": platform.platform_name,
            "snapshot_time": score.snapshot_time.isoformat() if score.snapshot_time else None,
            "scoring_version": score.scoring_version,
            "trust_score": float(score.trust_score or 0),
            "profitability_score": float(score.profitability_score or 0),
            "sample_trade_count": int(score.sample_trade_count or 0),
            "is_whale": bool(score.is_whale),
            "is_trusted_whale": bool(score.is_trusted_whale),
        }
        for score, account, platform in rows
    ]
    return {
        "snapshot_time": latest_batch.snapshot_time.isoformat() if latest_batch.snapshot_time else None,
        "scoring_version": latest_batch.scoring_version,
        "count": len(items),
        "items": items,
    }


def latest_user_whale_profile(session: Session, user_id: int) -> dict[str, Any] | None:
    """Return whale-specific user details for the latest score batch and dashboard snapshot."""
    user = session.get(UserAccount, user_id)
    if user is None:
        return None

    latest_batch = _latest_whale_batch(session)
    score = None
    if latest_batch is not None:
        score = session.scalars(
            select(WhaleScoreSnapshot).where(
                WhaleScoreSnapshot.user_id == user_id,
                WhaleScoreSnapshot.snapshot_time == latest_batch.snapshot_time,
                WhaleScoreSnapshot.scoring_version == latest_batch.scoring_version,
            )
        ).first()

    dashboard = session.scalars(select(Dashboard).order_by(desc(Dashboard.generated_at)).limit(1)).first()
    profile = None
    if dashboard is not None:
        profile = session.scalars(
            select(UserProfile).where(
                UserProfile.dashboard_id == dashboard.dashboard_id,
                UserProfile.user_id == user_id,
            )
        ).first()

    return {
        "user_id": user.user_id,
        "external_user_ref": user.external_user_ref,
        "wallet_address": user.wallet_address,
        "display_label": user.display_label,
        "is_likely_insider": bool(user.is_likely_insider),
        "latest_whale_score": (
            {
                "snapshot_time": score.snapshot_time.isoformat() if score.snapshot_time else None,
                "scoring_version": score.scoring_version,
                "trust_score": float(score.trust_score or 0),
                "profitability_score": float(score.profitability_score or 0),
                "sample_trade_count": int(score.sample_trade_count or 0),
                "is_whale": bool(score.is_whale),
                "is_trusted_whale": bool(score.is_trusted_whale),
            }
            if score is not None
            else None
        ),
        "resolved_performance": (
            (profile.trusted_traders_summary or {}).get("resolved_performance")
            if profile is not None and isinstance(profile.trusted_traders_summary, dict)
            else _resolved_summary(session, user_id)
        ),
        "dashboard_profile": (
            {
                "dashboard_id": profile.dashboard_id,
                "historical_actions_summary": profile.historical_actions_summary,
                "insider_stats": profile.insider_stats,
                "trusted_traders_summary": profile.trusted_traders_summary,
                "total_volume": float(profile.total_volume or 0),
                "total_shares": float(profile.total_shares or 0),
                "created_at": profile.created_at.isoformat() if profile.created_at else None,
            }
            if profile is not None
            else None
        ),
    }
