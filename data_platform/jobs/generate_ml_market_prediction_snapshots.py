"""Generate server-side ML market prediction snapshots for profile pages.

This first server snapshot pass covers every active Polymarket market with a
stable prediction status row. Markets present in the whale-anchored local report
receive 12h/24h prediction values; the rest are marked as waiting for live model
inference while still allowing profile pages to attach semi-live whale entries.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.db.session import session_scope
from data_platform.models import MlMarketPredictionSnapshot
from data_platform.models.base import utc_now
from data_platform.services.ml_reports import WHALE_ANCHORED_DELTA_JSON_PATH


DEFAULT_MODEL_VERSION = "whale_anchored_delta_snapshot_v0"
DEFAULT_FEATURE_SCHEMA_VERSION = "market_profile_prediction_snapshot_v1"
PREDICTION_WINDOWS = (12, 24)
PHYSICAL_SPORTS_TERMS = (
    "nba", "nfl", "mlb", "nhl", "ufc", "soccer", "football", "tennis", "golf",
    "cricket", "rugby", "baseball", "basketball", "hockey", "formula 1", "f1",
)
ESPORTS_TERMS = ("esports", "e-sports", "video game", "video-games", "gaming")


def is_physical_sports_market(texts: list[Any], *, category: Any = None) -> bool:
    """Return true for physical sports while keeping esports/video-games in scope."""
    joined = " ".join(str(value or "") for value in [category, *texts]).casefold()
    if any(term in joined for term in ESPORTS_TERMS):
        return False
    return any(term in joined for term in PHYSICAL_SPORTS_TERMS)

ACTIVE_MARKETS_SQL = text(
    """
    SELECT
      p.platform_id,
      mc.market_contract_id,
      LOWER(mc.market_slug) AS market_slug,
      mc.question,
      mc.outcome_a_label,
      mc.outcome_b_label,
      mc.last_trade_price,
      mc.updated_at,
      mc.is_closed,
      me.title AS event_title,
      me.slug AS event_slug,
      me.category AS event_category
    FROM analytics.market_contract mc
    JOIN analytics.platform p
      ON p.platform_id = mc.platform_id
    JOIN analytics.market_event me
      ON me.event_id = mc.event_id
    WHERE p.platform_name = :platform_name
      AND mc.market_slug IS NOT NULL
      AND (:include_closed = TRUE OR mc.is_closed = FALSE)
    ORDER BY mc.updated_at DESC NULLS LAST, mc.market_contract_id DESC
    LIMIT :limit
    """
)


def _read_local_prediction_index(path: Path) -> dict[str, Any]:
    """Return the local whale-anchored market profile prediction index."""
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    index = payload.get("market_profile_predictions", {}) if isinstance(payload, dict) else {}
    by_market = index.get("by_market_slug", {}) if isinstance(index, dict) else {}
    return by_market if isinstance(by_market, dict) else {}


def _parse_iso(value: str | None) -> datetime | None:
    """Return a timezone-aware datetime from an ISO string."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _normalize_label(value: str | None) -> str:
    """Normalize outcome labels for joining local predictions to markets."""
    return str(value or "").strip().casefold()


def _side_labels(row: dict[str, Any]) -> list[str]:
    """Return the market side labels that should receive prediction rows."""
    labels = [str(row.get("outcome_a_label") or "").strip(), str(row.get("outcome_b_label") or "").strip()]
    labels = [label for label in labels if label]
    if labels:
        return labels
    return ["Yes", "No"]


def _side_current_odds_pct(row: dict[str, Any], side_label: str) -> float | None:
    """Return current side odds in percentage points when last trade price is available."""
    last_trade_price = row.get("last_trade_price")
    if last_trade_price is None:
        return None
    price_pct = max(0.0, min(float(last_trade_price) * 100.0, 100.0))
    outcome_a = _normalize_label(str(row.get("outcome_a_label") or "yes"))
    if _normalize_label(side_label) == outcome_a:
        return round(price_pct, 4)
    return round(100.0 - price_pct, 4)


def _local_prediction_for(
    local_market: dict[str, Any] | None,
    *,
    side_label: str,
    window_hours: int,
) -> dict[str, Any] | None:
    """Return a local prediction case for a side/window when available."""
    if not isinstance(local_market, dict):
        return None
    cases = (local_market.get("windows") or {}).get(f"{window_hours}h") or []
    for case in cases:
        if isinstance(case, dict) and _normalize_label(str(case.get("side_label") or "")) == _normalize_label(side_label):
            return case
    return None


def _pending_payload(
    row: dict[str, Any],
    *,
    side_label: str,
    window_hours: int,
    generated_at: datetime,
) -> dict[str, Any]:
    """Return a profile-ready placeholder for markets awaiting live ML inference."""
    current_odds_pct = _side_current_odds_pct(row, side_label)
    target_time = generated_at + timedelta(hours=window_hours)
    return {
        "window": f"{window_hours}h",
        "market_slug": str(row["market_slug"]),
        "question": str(row.get("question") or ""),
        "side_label": side_label,
        "observation_time": generated_at.isoformat(),
        "event_category": str(row.get("event_category") or "uncategorized"),
        "focus_category": str(row.get("event_category") or "uncategorized"),
        "focused_fit_category": str(row.get("event_category") or "uncategorized"),
        "market_family": str(row.get("event_category") or "uncategorized"),
        "current_odds_pct": current_odds_pct,
        "predicted_future_odds_pct": None,
        "predicted_delta_pts": None,
        "predicted_direction": "unavailable",
        "prediction_source": "pending_live_model_inference",
        "display_tier": "unavailable",
        "display_reasons": ["waiting_for_live_model_inference"],
        "review_reasons": [],
        "direction_signal_tier": "unavailable",
        "direction_signal_tier_reason": "waiting_for_live_model_inference",
        "direction_signal_predicted_direction": "unavailable",
        "direction_signal_confidence": 0.0,
        "reliability_warnings": ["live_model_snapshot_missing"],
        "overlay_future_odds_pct": current_odds_pct,
        "overlay_delta_pts": 0.0,
        "overlay_direction": "flat",
        "interval_low_future_odds_pct": current_odds_pct,
        "interval_high_future_odds_pct": current_odds_pct,
        "trend_fit_error_type": "unavailable",
        "trend_shape_score": 0.0,
        "whale_anchor": {},
        "local_backtest_only": False,
        "prediction_start_time": generated_at.isoformat(),
        "prediction_target_time": target_time.isoformat(),
        "prediction_window_hours": window_hours,
        "prediction_timeline_source": "server_snapshot_observation_time",
        "prediction_status": "waiting_for_live_model_inference",
    }


def _snapshot_row(
    market_row: dict[str, Any],
    *,
    side_label: str,
    window_hours: int,
    generated_at: datetime,
    local_prediction: dict[str, Any] | None,
    model_version: str,
    feature_schema_version: str,
) -> dict[str, Any]:
    """Return one database row for a market side/window prediction snapshot."""
    payload = dict(local_prediction) if local_prediction else _pending_payload(
        market_row,
        side_label=side_label,
        window_hours=window_hours,
        generated_at=generated_at,
    )
    payload.setdefault("window", f"{window_hours}h")
    payload.setdefault("market_slug", str(market_row["market_slug"]))
    payload.setdefault("question", str(market_row.get("question") or ""))
    payload.setdefault("side_label", side_label)
    payload.setdefault("observation_time", generated_at.isoformat())
    payload.setdefault("prediction_window_hours", window_hours)
    payload.setdefault("prediction_target_time", (generated_at + timedelta(hours=window_hours)).isoformat())
    payload["prediction_generated_at"] = generated_at.isoformat()
    payload["model_version"] = model_version
    payload["feature_schema_version"] = feature_schema_version

    observation_time = _parse_iso(str(payload.get("observation_time") or "")) or generated_at
    prediction_target_time = _parse_iso(str(payload.get("prediction_target_time") or ""))
    whale_entry_time = _parse_iso(str(payload.get("whale_entry_time") or ""))
    prediction_available = payload.get("predicted_future_odds_pct") is not None
    prediction_status = (
        "prediction_available"
        if prediction_available
        else str(payload.get("prediction_status") or "waiting_for_live_model_inference")
    )
    prediction_source = (
        "local_whale_anchored_report"
        if local_prediction
        else "pending_live_model_inference"
    )
    reliability_payload = {
        "display_reasons": payload.get("display_reasons") or [],
        "review_reasons": payload.get("review_reasons") or [],
        "reliability_warnings": payload.get("reliability_warnings") or [],
        "direction_signal_tier_reason": payload.get("direction_signal_tier_reason"),
    }

    return {
        "platform_id": int(market_row["platform_id"]),
        "market_contract_id": int(market_row["market_contract_id"]),
        "market_slug": str(market_row["market_slug"]),
        "side_label": side_label,
        "prediction_window_hours": window_hours,
        "observation_time": observation_time,
        "whale_entry_time": whale_entry_time,
        "prediction_target_time": prediction_target_time,
        "current_odds_pct": payload.get("current_odds_pct"),
        "predicted_future_odds_pct": payload.get("predicted_future_odds_pct"),
        "predicted_delta_pts": payload.get("predicted_delta_pts"),
        "signal_tier": str(payload.get("direction_signal_tier") or "unavailable"),
        "display_tier": str(payload.get("display_tier") or "unavailable"),
        "prediction_status": prediction_status,
        "model_version": model_version,
        "feature_schema_version": feature_schema_version,
        "trained_as_of": None,
        "prediction_generated_at": generated_at,
        "data_freshness_status": "current_market_contract_snapshot",
        "prediction_source": prediction_source,
        "reliability_payload": reliability_payload,
        "prediction_payload": payload,
        "created_at": utc_now(),
    }


def _snapshot_table_exists(session: Session) -> bool:
    """Return whether the prediction snapshot table exists."""
    row = session.execute(
        text("SELECT to_regclass('analytics.ml_market_prediction_snapshot') IS NOT NULL AS table_exists")
    ).mappings().first()
    return bool(row and row.get("table_exists"))


def generate_prediction_snapshots(
    session: Session,
    *,
    platform_name: str,
    include_closed: bool,
    limit: int,
    local_report_path: Path,
    model_version: str,
    feature_schema_version: str,
    create_table: bool,
) -> dict[str, Any]:
    """Generate and persist all-market prediction snapshot rows."""
    if create_table:
        MlMarketPredictionSnapshot.__table__.create(bind=session.get_bind(), checkfirst=True)
    if not _snapshot_table_exists(session):
        return {
            "ok": False,
            "reason": "ml_market_prediction_snapshot_table_missing",
            "hint": "Run Alembic migration 20260505_1000 or pass --create-table for local testing.",
        }

    local_index = _read_local_prediction_index(local_report_path)
    generated_at = utc_now()
    raw_market_rows = session.execute(
        ACTIVE_MARKETS_SQL,
        {
            "platform_name": platform_name,
            "include_closed": include_closed,
            "limit": None if limit <= 0 else limit,
        },
    ).mappings().all()

    snapshot_rows: list[dict[str, Any]] = []
    excluded_sports_count = 0
    local_prediction_count = 0
    pending_prediction_count = 0
    for market_row in raw_market_rows:
        row = dict(market_row)
        if is_physical_sports_market(
            [row.get("market_slug"), row.get("question"), row.get("event_title"), row.get("event_slug")],
            category=row.get("event_category"),
        ):
            excluded_sports_count += 1
            continue
        local_market = local_index.get(str(row["market_slug"]))
        for side_label in _side_labels(row):
            for window_hours in PREDICTION_WINDOWS:
                local_prediction = _local_prediction_for(local_market, side_label=side_label, window_hours=window_hours)
                if local_prediction:
                    local_prediction_count += 1
                else:
                    pending_prediction_count += 1
                snapshot_rows.append(
                    _snapshot_row(
                        row,
                        side_label=side_label,
                        window_hours=window_hours,
                        generated_at=generated_at,
                        local_prediction=local_prediction,
                        model_version=model_version,
                        feature_schema_version=feature_schema_version,
                    )
                )

    if snapshot_rows:
        statement = pg_insert(MlMarketPredictionSnapshot).values(snapshot_rows)
        statement = statement.on_conflict_do_update(
            constraint="uq_ml_market_prediction_snapshot_market_side_window_generated",
            set_={
                "observation_time": statement.excluded.observation_time,
                "whale_entry_time": statement.excluded.whale_entry_time,
                "prediction_target_time": statement.excluded.prediction_target_time,
                "current_odds_pct": statement.excluded.current_odds_pct,
                "predicted_future_odds_pct": statement.excluded.predicted_future_odds_pct,
                "predicted_delta_pts": statement.excluded.predicted_delta_pts,
                "signal_tier": statement.excluded.signal_tier,
                "display_tier": statement.excluded.display_tier,
                "prediction_status": statement.excluded.prediction_status,
                "model_version": statement.excluded.model_version,
                "feature_schema_version": statement.excluded.feature_schema_version,
                "trained_as_of": statement.excluded.trained_as_of,
                "data_freshness_status": statement.excluded.data_freshness_status,
                "prediction_source": statement.excluded.prediction_source,
                "reliability_payload": statement.excluded.reliability_payload,
                "prediction_payload": statement.excluded.prediction_payload,
            },
        )
        session.execute(statement)

    return {
        "ok": True,
        "generated_at": generated_at.isoformat(),
        "platform": platform_name,
        "include_closed": include_closed,
        "queried_market_count": len(raw_market_rows),
        "excluded_physical_sports_market_count": excluded_sports_count,
        "snapshot_row_count": len(snapshot_rows),
        "local_prediction_row_count": local_prediction_count,
        "pending_prediction_row_count": pending_prediction_count,
        "model_version": model_version,
        "feature_schema_version": feature_schema_version,
    }


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default="", help="Optional database URL override.")
    parser.add_argument("--platform-name", default="polymarket")
    parser.add_argument("--include-closed", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Maximum active markets to snapshot. Use 0 for no cap.")
    parser.add_argument("--local-report-path", default=str(WHALE_ANCHORED_DELTA_JSON_PATH))
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION)
    parser.add_argument("--feature-schema-version", default=DEFAULT_FEATURE_SCHEMA_VERSION)
    parser.add_argument("--create-table", action="store_true", help="Create the table locally if the migration has not run.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    with session_scope(args.database_url or None) as session:
        summary = generate_prediction_snapshots(
            session,
            platform_name=args.platform_name,
            include_closed=bool(args.include_closed),
            limit=int(args.limit),
            local_report_path=Path(args.local_report_path),
            model_version=args.model_version,
            feature_schema_version=args.feature_schema_version,
            create_table=bool(args.create_table),
        )
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
