"""Build cached research analytics snapshots for common dashboard timeframes."""

from __future__ import annotations

from datetime import datetime, timezone

from data_platform.db.session import session_scope
from data_platform.models import ResearchAnalyticsSnapshot
from data_platform.services.read_api import (
    _live_market_whale_concentration,
    _live_recent_whale_entries,
    _live_top_profitable_resolved_users,
    _live_whale_entry_behavior,
)
from data_platform.services.research_analytics_snapshot import RESEARCH_ANALYTICS_TIMEFRAMES


SNAPSHOT_LIMIT = 25


def build_research_analytics_snapshots() -> list[dict[str, object]]:
    """Persist one research analytics snapshot per common timeframe."""
    summaries: list[dict[str, object]] = []
    with session_scope() as session:
        generated_at = datetime.now(timezone.utc)
        for timeframe in RESEARCH_ANALYTICS_TIMEFRAMES:
            top_profitable = _live_top_profitable_resolved_users(session, limit=SNAPSHOT_LIMIT, timeframe=timeframe)
            recent_entries = _live_recent_whale_entries(session, limit=SNAPSHOT_LIMIT, timeframe=timeframe)
            market_concentration = _live_market_whale_concentration(session, limit=SNAPSHOT_LIMIT, timeframe=timeframe)
            whale_entry = _live_whale_entry_behavior(session, limit=SNAPSHOT_LIMIT, timeframe=timeframe)
            row = ResearchAnalyticsSnapshot(
                generated_at=generated_at,
                timeframe=timeframe,
                top_profitable_payload=top_profitable or {"items": [], "count": 0, "timeframe": timeframe},
                recent_entries_payload=recent_entries or {"items": [], "count": 0, "timeframe": timeframe},
                market_concentration_payload=market_concentration or {"items": [], "count": 0, "timeframe": timeframe},
                whale_entry_payload=whale_entry or {"items": [], "count": 0, "timeframe": timeframe},
            )
            session.add(row)
            session.flush()
            summaries.append(
                {
                    "research_analytics_snapshot_id": row.research_analytics_snapshot_id,
                    "generated_at": generated_at.isoformat(),
                    "timeframe": timeframe,
                    "top_profitable_count": len((top_profitable or {}).get("items") or []),
                    "recent_entries_count": len((recent_entries or {}).get("items") or []),
                    "market_concentration_count": len((market_concentration or {}).get("items") or []),
                    "whale_entry_count": len((whale_entry or {}).get("items") or []),
                }
            )
    return summaries


if __name__ == "__main__":
    print(build_research_analytics_snapshots())
