"""Internal read-only FastAPI surface for the data platform."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError

from data_platform.db.session import session_scope
from data_platform.services.read_api import (
    database_health,
    home_summary,
    market_whale_concentration,
    latest_dashboard_snapshot,
    latest_dashboard_markets,
    latest_leaderboard,
    latest_market_profile,
    latest_scrape_run,
    latest_user_whale_profile,
    latest_whale_scores,
    list_markets,
    list_positions,
    list_transactions,
    list_users,
    top_profitable_resolved_users,
    user_activity_insights,
    whale_entry_behavior,
)

app = FastAPI(
    title="Whaling Data Platform API",
    version="0.1.0",
    description="Internal read-only API for inspecting collected prediction market data.",
)


def _service_error(exc: Exception) -> HTTPException:
    """Convert backend exceptions into consistent API errors."""
    return HTTPException(status_code=503, detail=f"Database service unavailable: {exc}")


@app.get("/")
async def root() -> dict[str, str]:
    """Return a short API summary."""
    return {
        "message": "Whaling Data Platform API",
        "health_endpoint": "/health",
        "status_endpoint": "/api/status/ingestion",
    }


@app.get("/health")
async def health() -> dict[str, str]:
    """Return application and database health."""
    try:
        with session_scope() as session:
            database_health(session)
        return {"status": "ok", "database": "ok"}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/status/ingestion")
async def ingestion_status() -> dict[str, object | None]:
    """Return the latest scrape-run status."""
    try:
        with session_scope() as session:
            return {"latest_scrape_run": latest_scrape_run(session)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/markets")
async def get_markets(limit: int = Query(50, ge=1, le=250)) -> dict[str, object]:
    """Return recent market rows from the normalized source layer."""
    try:
        with session_scope() as session:
            return {"count": limit, "items": list_markets(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/users")
async def get_users(limit: int = Query(50, ge=1, le=250)) -> dict[str, object]:
    """Return recent user rows from the normalized source layer."""
    try:
        with session_scope() as session:
            return {"count": limit, "items": list_users(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/transactions")
async def get_transactions(limit: int = Query(50, ge=1, le=250)) -> dict[str, object]:
    """Return recent normalized transaction rows."""
    try:
        with session_scope() as session:
            return {"count": limit, "items": list_transactions(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/positions")
async def get_positions(limit: int = Query(50, ge=1, le=250)) -> dict[str, object]:
    """Return recent normalized position snapshots."""
    try:
        with session_scope() as session:
            return {"count": limit, "items": list_positions(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/leaderboards/latest")
async def get_latest_leaderboard() -> dict[str, object | None]:
    """Return the latest derived leaderboard snapshot, if present."""
    try:
        with session_scope() as session:
            return {"leaderboard": latest_leaderboard(session)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/leaderboards/trusted/latest")
async def get_latest_trusted_leaderboard() -> dict[str, object | None]:
    """Return the latest trusted-whale leaderboard rows, if present."""
    try:
        with session_scope() as session:
            return {"leaderboard": latest_leaderboard(session, board_type="internal_trusted")}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/dashboards/latest")
async def get_latest_dashboard() -> dict[str, object | None]:
    """Return the latest derived dashboard snapshot summary, if present."""
    try:
        with session_scope() as session:
            return {"dashboard": latest_dashboard_snapshot(session)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/dashboards/latest/markets")
async def get_latest_dashboard_markets(limit: int = Query(50, ge=1, le=250)) -> dict[str, object | None]:
    """Return the latest dashboard-market rows for frontend leaderboard use."""
    try:
        with session_scope() as session:
            return {"markets": latest_dashboard_markets(session, limit=limit)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/home/summary")
async def get_home_summary() -> dict[str, object]:
    """Return homepage summary cards backed by the latest analytics state."""
    try:
        with session_scope() as session:
            return {"summary": home_summary(session)}
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/analytics/top-profitable-users")
async def get_top_profitable_users(
    limit: int = Query(10, ge=1, le=100),
    timeframe: str = Query("all"),
) -> dict[str, object]:
    """Return the top Polymarket users by conservative resolved-market profitability."""
    try:
        with session_scope() as session:
            return {"analytics": top_profitable_resolved_users(session, limit=limit, timeframe=timeframe)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/analytics/market-whale-concentration")
async def get_market_whale_concentration(
    limit: int = Query(10, ge=1, le=100),
    timeframe: str = Query("all"),
) -> dict[str, object | None]:
    """Return the latest cross-platform market concentration ranking."""
    try:
        with session_scope() as session:
            return {"analytics": market_whale_concentration(session, limit=limit, timeframe=timeframe)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/analytics/whale-entry-behavior")
async def get_whale_entry_behavior(
    limit: int = Query(10, ge=1, le=100),
    timeframe: str = Query("all"),
) -> dict[str, object | None]:
    """Return Polymarket whale entry-price behavior for the selected timeframe."""
    try:
        with session_scope() as session:
            return {"analytics": whale_entry_behavior(session, limit=limit, timeframe=timeframe)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/whales/latest")
async def get_latest_whales(
    limit: int = Query(50, ge=1, le=250),
    whales_only: bool = Query(False),
    trusted_only: bool = Query(False),
) -> dict[str, object | None]:
    """Return the latest whale-score rows with optional whale/trusted filters."""
    try:
        with session_scope() as session:
            return {
                "whales": latest_whale_scores(
                    session,
                    limit=limit,
                    whales_only=whales_only,
                    trusted_only=trusted_only,
                )
            }
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/users/{user_id}/whale-profile")
async def get_user_whale_profile(user_id: int) -> dict[str, object]:
    """Return whale-specific resolved-performance details for one user."""
    try:
        with session_scope() as session:
            profile = latest_user_whale_profile(session, user_id=user_id)
            if profile is None:
                raise HTTPException(status_code=404, detail=f"User {user_id} not found")
            return {"profile": profile}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/users/{user_id}/activity-insights")
async def get_user_activity_insights(
    user_id: int,
    timeframe: str = Query("all"),
) -> dict[str, object]:
    """Return user-specific activity insights for the selected timeframe."""
    try:
        with session_scope() as session:
            insights = user_activity_insights(session, user_id=user_id, timeframe=timeframe)
            if insights is None:
                raise HTTPException(status_code=404, detail=f"User {user_id} not found")
            return {"insights": insights}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc


@app.get("/api/markets/{market_slug}/profile")
async def get_market_profile(market_slug: str) -> dict[str, object]:
    """Return latest dashboard-backed market profile details for one market slug."""
    try:
        with session_scope() as session:
            profile = latest_market_profile(session, market_slug=market_slug)
            if profile is None:
                raise HTTPException(status_code=404, detail=f"Market {market_slug} not found")
            return {"profile": profile}
    except HTTPException:
        raise
    except (OSError, SQLAlchemyError) as exc:
        raise _service_error(exc) from exc
