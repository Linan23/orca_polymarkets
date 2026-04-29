"""Read cached research analytics snapshots."""

from __future__ import annotations

from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from data_platform.models import ResearchAnalyticsSnapshot


RESEARCH_ANALYTICS_TIMEFRAMES = ("all", "90d", "30d", "7d")
RESEARCH_ANALYTICS_VIEW_FIELDS = {
    "top_profitable_users": "top_profitable_payload",
    "recent_whale_entries": "recent_entries_payload",
    "market_whale_concentration": "market_concentration_payload",
    "whale_entry_behavior": "whale_entry_payload",
}


def latest_research_analytics_view(
    session: Session,
    *,
    timeframe: str,
    view_name: str,
    limit: int,
) -> dict[str, Any] | None:
    """Return one cached research view payload, trimmed to the requested limit."""
    field_name = RESEARCH_ANALYTICS_VIEW_FIELDS.get(view_name)
    if field_name is None:
        return None
    row = session.scalars(
        select(ResearchAnalyticsSnapshot)
        .where(ResearchAnalyticsSnapshot.timeframe == timeframe)
        .order_by(
            desc(ResearchAnalyticsSnapshot.generated_at),
            desc(ResearchAnalyticsSnapshot.research_analytics_snapshot_id),
        )
        .limit(1)
    ).first()
    if row is None:
        return None

    payload = dict(getattr(row, field_name) or {})
    items = list(payload.get("items") or [])
    payload["items"] = items[:limit]
    payload["count"] = len(payload["items"])
    payload["generated_at"] = row.generated_at.isoformat() if row.generated_at else payload.get("generated_at")
    payload["cache_source"] = "analytics.research_analytics_snapshot"
    return payload
