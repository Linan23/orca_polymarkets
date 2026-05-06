"""Generate server-side ML market prediction snapshots for profile pages.

The market profile snapshot covers every active Polymarket market. Markets
present in the tuned whale-anchored report keep those report predictions; all
other markets receive a live whale-signal prediction from current odds plus
recent trusted-whale entry/exit pressure so each profile can render 12h/24h
trend forecasts.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
import os
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
from data_platform.ml.prediction_confidence import (
    DEFAULT_CONFIDENCE_MODEL_PATH,
    apply_trained_confidence,
    load_confidence_artifact,
)
from data_platform.models import MlMarketPredictionSnapshot
from data_platform.models.base import utc_now
from data_platform.services.ml_reports import WHALE_ANCHORED_DELTA_JSON_PATH


DEFAULT_MODEL_VERSION = "market_profile_hybrid_whale_trend_v1"
DEFAULT_FEATURE_SCHEMA_VERSION = "market_profile_prediction_snapshot_v2"
PREDICTION_WINDOWS = (12, 24)
DEFAULT_INSERT_BATCH_SIZE = 1000
DEFAULT_LIVE_FEATURE_MARKET_LIMIT = 1000
MAX_SIGNAL_DELTA_BY_WINDOW = {12: 6.0, 24: 9.0}
HIGH_CONFIDENCE_DIRECTION_WINDOW_HOURS = 12
HIGH_CONFIDENCE_DIRECTION_MIN_DELTA_PTS = 5.0
HIGH_CONFIDENCE_24H_MIN_DELTA_PTS = 6.0
HIGH_CONFIDENCE_24H_MIN_TOTAL_PRESSURE = 1000.0
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

LIVE_WHALE_SIGNAL_SQL = text(
    """
    WITH latest_batch AS (
      SELECT
        w.snapshot_time,
        w.scoring_version
      FROM analytics.whale_score_snapshot w
      JOIN analytics.platform p
        ON p.platform_id = w.platform_id
      WHERE p.platform_name = :platform_name
      ORDER BY w.snapshot_time DESC, w.created_at DESC, w.whale_score_snapshot_id DESC
      LIMIT 1
    ),
    latest_scores AS (
      SELECT
        w.user_id,
        w.platform_id,
        w.trust_score,
        w.is_trusted_whale,
        w.is_whale
      FROM analytics.whale_score_snapshot w
      JOIN analytics.platform p
        ON p.platform_id = w.platform_id
      JOIN latest_batch lb
        ON lb.snapshot_time = w.snapshot_time
       AND lb.scoring_version = w.scoring_version
      WHERE p.platform_name = :platform_name
        AND (w.is_trusted_whale = TRUE OR w.is_whale = TRUE OR w.trust_score >= 1.08)
    ),
    recent_trades AS (
      SELECT
        tf.market_contract_id,
        LOWER(COALESCE(NULLIF(TRIM(tf.outcome_label), ''), '')) AS outcome_label,
        LOWER(COALESCE(NULLIF(TRIM(tf.side), ''), tf.transaction_type, '')) AS trade_side,
        tf.transaction_time,
        ABS(COALESCE(tf.notional_value, tf.price * tf.shares, 0)) AS notional_value,
        GREATEST(COALESCE(ls.trust_score, 1), 0) AS trust_score,
        ls.is_trusted_whale
      FROM analytics.transaction_fact tf
      JOIN latest_scores ls
        ON ls.user_id = tf.user_id
       AND ls.platform_id = tf.platform_id
      JOIN analytics.platform p
        ON p.platform_id = tf.platform_id
      WHERE p.platform_name = :platform_name
        AND tf.market_contract_id = ANY(CAST(:market_ids AS INTEGER[]))
        AND tf.transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'
        AND tf.transaction_time <= CAST(:as_of AS TIMESTAMPTZ) + INTERVAL '5 minutes'
    )
    SELECT
      market_contract_id,
      outcome_label,
      COUNT(*) FILTER (WHERE transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '12 hours') AS event_count_12h,
      COUNT(*) FILTER (WHERE transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours') AS event_count_24h,
      COUNT(*) FILTER (
        WHERE trade_side = 'buy' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '12 hours'
      ) AS entry_count_12h,
      COUNT(*) FILTER (
        WHERE trade_side = 'sell' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '12 hours'
      ) AS exit_count_12h,
      COUNT(*) FILTER (
        WHERE trade_side = 'buy' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'
      ) AS entry_count_24h,
      COUNT(*) FILTER (
        WHERE trade_side = 'sell' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'
      ) AS exit_count_24h,
      COALESCE(SUM(notional_value * trust_score) FILTER (
        WHERE trade_side = 'buy' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '12 hours'
      ), 0) AS weighted_entry_12h,
      COALESCE(SUM(notional_value * trust_score) FILTER (
        WHERE trade_side = 'sell' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '12 hours'
      ), 0) AS weighted_exit_12h,
      COALESCE(SUM(notional_value * trust_score) FILTER (
        WHERE trade_side = 'buy' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'
      ), 0) AS weighted_entry_24h,
      COALESCE(SUM(notional_value * trust_score) FILTER (
        WHERE trade_side = 'sell' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'
      ), 0) AS weighted_exit_24h,
      COUNT(*) FILTER (
        WHERE is_trusted_whale = TRUE AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '12 hours'
      ) AS trusted_event_count_12h,
      COUNT(*) FILTER (
        WHERE is_trusted_whale = TRUE AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'
      ) AS trusted_event_count_24h,
      MAX(transaction_time) FILTER (
        WHERE trade_side = 'buy' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '12 hours'
      ) AS latest_entry_time_12h,
      MAX(transaction_time) FILTER (
        WHERE trade_side = 'sell' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '12 hours'
      ) AS latest_exit_time_12h,
      MAX(transaction_time) FILTER (
        WHERE trade_side = 'buy' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'
      ) AS latest_entry_time_24h,
      MAX(transaction_time) FILTER (
        WHERE trade_side = 'sell' AND transaction_time >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'
      ) AS latest_exit_time_24h
    FROM recent_trades
    GROUP BY market_contract_id, outcome_label
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Return a finite float for DB numeric values."""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _safe_int(value: Any, default: int = 0) -> int:
    """Return an int for DB aggregate values."""
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp a float to a closed interval."""
    return max(low, min(value, high))


def _iso(value: Any) -> str | None:
    """Return an ISO datetime string for JSON payloads."""
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return parsed.isoformat()
    return str(value)


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


def _load_live_whale_signal_index(
    session: Session,
    *,
    platform_name: str,
    as_of: datetime,
    market_ids: list[int],
) -> dict[int, dict[str, dict[str, Any]]]:
    """Return recent whale pressure features grouped by market and side label."""
    if not market_ids:
        return {}
    rows = session.execute(
        LIVE_WHALE_SIGNAL_SQL,
        {
            "platform_name": platform_name,
            "as_of": as_of,
            "market_ids": market_ids,
        },
    ).mappings().all()
    by_market: dict[int, dict[str, dict[str, Any]]] = {}
    for raw_row in rows:
        row = dict(raw_row)
        market_id = _safe_int(row.get("market_contract_id"))
        if not market_id:
            continue
        label = _normalize_label(str(row.get("outcome_label") or ""))
        by_market.setdefault(market_id, {})[label] = {
            "event_count_12h": _safe_int(row.get("event_count_12h")),
            "event_count_24h": _safe_int(row.get("event_count_24h")),
            "entry_count_12h": _safe_int(row.get("entry_count_12h")),
            "exit_count_12h": _safe_int(row.get("exit_count_12h")),
            "entry_count_24h": _safe_int(row.get("entry_count_24h")),
            "exit_count_24h": _safe_int(row.get("exit_count_24h")),
            "weighted_entry_12h": _safe_float(row.get("weighted_entry_12h")),
            "weighted_exit_12h": _safe_float(row.get("weighted_exit_12h")),
            "weighted_entry_24h": _safe_float(row.get("weighted_entry_24h")),
            "weighted_exit_24h": _safe_float(row.get("weighted_exit_24h")),
            "trusted_event_count_12h": _safe_int(row.get("trusted_event_count_12h")),
            "trusted_event_count_24h": _safe_int(row.get("trusted_event_count_24h")),
            "latest_entry_time_12h": _iso(row.get("latest_entry_time_12h")),
            "latest_exit_time_12h": _iso(row.get("latest_exit_time_12h")),
            "latest_entry_time_24h": _iso(row.get("latest_entry_time_24h")),
            "latest_exit_time_24h": _iso(row.get("latest_exit_time_24h")),
        }
    return by_market


def _empty_live_side_features() -> dict[str, Any]:
    """Return empty feature values for markets without recent whale pressure."""
    return {
        "event_count_12h": 0,
        "event_count_24h": 0,
        "entry_count_12h": 0,
        "exit_count_12h": 0,
        "entry_count_24h": 0,
        "exit_count_24h": 0,
        "weighted_entry_12h": 0.0,
        "weighted_exit_12h": 0.0,
        "weighted_entry_24h": 0.0,
        "weighted_exit_24h": 0.0,
        "trusted_event_count_12h": 0,
        "trusted_event_count_24h": 0,
        "latest_entry_time_12h": None,
        "latest_exit_time_12h": None,
        "latest_entry_time_24h": None,
        "latest_exit_time_24h": None,
    }


def _combine_live_side_features(
    feature_index: dict[int, dict[str, dict[str, Any]]],
    *,
    market_contract_id: int,
    side_label: str,
    window_hours: int,
) -> dict[str, Any]:
    """Return side-relative whale pressure, using opposite-side trades for binary markets."""
    by_label = feature_index.get(market_contract_id) or {}
    normalized_side = _normalize_label(side_label)
    same = by_label.get(normalized_side) or _empty_live_side_features()
    opposite_rows = [
        features
        for label, features in by_label.items()
        if label and label != normalized_side
    ]
    entry_key = f"weighted_entry_{window_hours}h"
    exit_key = f"weighted_exit_{window_hours}h"
    count_key = f"event_count_{window_hours}h"
    entry_count_key = f"entry_count_{window_hours}h"
    exit_count_key = f"exit_count_{window_hours}h"
    trusted_count_key = f"trusted_event_count_{window_hours}h"

    same_entry = _safe_float(same.get(entry_key))
    same_exit = _safe_float(same.get(exit_key))
    opposite_entry = sum(_safe_float(features.get(entry_key)) for features in opposite_rows)
    opposite_exit = sum(_safe_float(features.get(exit_key)) for features in opposite_rows)
    side_entry_pressure = same_entry + opposite_exit
    side_exit_pressure = same_exit + opposite_entry
    total_pressure = side_entry_pressure + side_exit_pressure

    latest_entry_candidates = [
        same.get(f"latest_entry_time_{window_hours}h"),
        *[features.get(f"latest_exit_time_{window_hours}h") for features in opposite_rows],
    ]
    latest_exit_candidates = [
        same.get(f"latest_exit_time_{window_hours}h"),
        *[features.get(f"latest_entry_time_{window_hours}h") for features in opposite_rows],
    ]
    latest_entry_time = max((str(value) for value in latest_entry_candidates if value), default=None)
    latest_exit_time = max((str(value) for value in latest_exit_candidates if value), default=None)

    same_12h = {
        "recent_entry_count_12h": _safe_int(same.get("entry_count_12h")),
        "recent_exit_count_12h": _safe_int(same.get("exit_count_12h")),
        "recent_weighted_entry_12h": round(_safe_float(same.get("weighted_entry_12h")), 6),
        "recent_weighted_exit_12h": round(_safe_float(same.get("weighted_exit_12h")), 6),
        "recent_weighted_net_pressure_12h": round(
            _safe_float(same.get("weighted_entry_12h")) - _safe_float(same.get("weighted_exit_12h")),
            6,
        ),
    }
    return {
        "window_hours": window_hours,
        "event_count": _safe_int(same.get(count_key)) + sum(_safe_int(features.get(count_key)) for features in opposite_rows),
        "entry_count": _safe_int(same.get(entry_count_key)) + sum(_safe_int(features.get(exit_count_key)) for features in opposite_rows),
        "exit_count": _safe_int(same.get(exit_count_key)) + sum(_safe_int(features.get(entry_count_key)) for features in opposite_rows),
        "trusted_event_count": _safe_int(same.get(trusted_count_key))
        + sum(_safe_int(features.get(trusted_count_key)) for features in opposite_rows),
        "side_entry_pressure": round(side_entry_pressure, 6),
        "side_exit_pressure": round(side_exit_pressure, 6),
        "side_net_pressure": round(side_entry_pressure - side_exit_pressure, 6),
        "side_total_pressure": round(total_pressure, 6),
        "pressure_ratio": round((side_entry_pressure - side_exit_pressure) / max(total_pressure, 1.0), 6),
        "latest_entry_time": latest_entry_time,
        "latest_exit_time": latest_exit_time,
        **same_12h,
    }


def _live_prediction_for(
    row: dict[str, Any],
    *,
    side_label: str,
    window_hours: int,
    generated_at: datetime,
    live_feature_index: dict[int, dict[str, dict[str, Any]]],
) -> dict[str, Any]:
    """Return a live whale-signal prediction payload for one market side/window."""
    current_odds_pct = _side_current_odds_pct(row, side_label)
    if current_odds_pct is None:
        current_odds_pct = 50.0

    market_id = int(row["market_contract_id"])
    features = _combine_live_side_features(
        live_feature_index,
        market_contract_id=market_id,
        side_label=side_label,
        window_hours=window_hours,
    )
    total_pressure = _safe_float(features.get("side_total_pressure"))
    pressure_ratio = _safe_float(features.get("pressure_ratio"))
    event_count = _safe_int(features.get("event_count"))
    trusted_event_count = _safe_int(features.get("trusted_event_count"))
    has_whale_signal = total_pressure > 0 and event_count > 0

    activity_score = _clamp(math.log1p(total_pressure) / math.log1p(50000.0), 0.0, 1.0) if has_whale_signal else 0.0
    count_score = _clamp(math.log1p(event_count) / math.log1p(50.0), 0.0, 1.0) if has_whale_signal else 0.0
    trusted_share = _clamp(trusted_event_count / max(event_count, 1), 0.0, 1.0) if has_whale_signal else 0.0
    horizon_scale = 1.0 if window_hours == 12 else 1.35
    if has_whale_signal:
        whale_delta = pressure_ratio * (2.25 + 4.75 * activity_score) * horizon_scale
        mean_reversion_delta = (50.0 - current_odds_pct) * 0.0125 * horizon_scale
        raw_delta = whale_delta + mean_reversion_delta
    else:
        raw_delta = 0.0
    max_delta = MAX_SIGNAL_DELTA_BY_WINDOW.get(window_hours, 6.0)
    predicted_delta_pts = round(_clamp(raw_delta, -max_delta, max_delta), 4)
    if has_whale_signal:
        predicted_future_odds_pct = round(_clamp(current_odds_pct + predicted_delta_pts, 1.0, 99.0), 4)
        predicted_delta_pts = round(predicted_future_odds_pct - current_odds_pct, 4)
    else:
        predicted_future_odds_pct = round(current_odds_pct, 4)
        predicted_delta_pts = 0.0
    if predicted_delta_pts > 0.25:
        predicted_direction = "up"
    elif predicted_delta_pts < -0.25:
        predicted_direction = "down"
    else:
        predicted_direction = "flat"

    confidence = 0.22
    if has_whale_signal:
        confidence = _clamp(
            0.32 + 0.38 * activity_score + 0.18 * count_score + 0.12 * trusted_share,
            0.0,
            0.92,
        )
    is_high_confidence_12h_slice = (
        window_hours == HIGH_CONFIDENCE_DIRECTION_WINDOW_HOURS
        and predicted_direction != "flat"
        and confidence >= 0.7
        and abs(predicted_delta_pts) >= HIGH_CONFIDENCE_DIRECTION_MIN_DELTA_PTS
    )
    is_high_confidence_24h_slice = (
        window_hours == 24
        and predicted_direction != "flat"
        and confidence >= 0.7
        and abs(predicted_delta_pts) >= HIGH_CONFIDENCE_24H_MIN_DELTA_PTS
        and total_pressure >= HIGH_CONFIDENCE_24H_MIN_TOTAL_PRESSURE
    )
    is_high_confidence_validated_slice = is_high_confidence_12h_slice or is_high_confidence_24h_slice
    is_directional_watch = predicted_direction != "flat" and confidence >= 0.7 and abs(predicted_delta_pts) >= 1.0
    if predicted_direction == "flat":
        signal_tier = "abstain"
        display_tier = "review"
        tier_reason = "live model expects no material 12-24h move"
    elif is_high_confidence_validated_slice:
        signal_tier = "watch"
        display_tier = "show"
        tier_reason = (
            "historical validation supports this 24h Watch signal when predicted movement is at least 6 points with strong whale pressure"
            if is_high_confidence_24h_slice
            else "historical validation supports this 12h Watch signal when predicted movement is at least 5 points"
        )
    elif is_directional_watch:
        signal_tier = "watch"
        display_tier = "review"
        if window_hours == 24 and abs(predicted_delta_pts) >= HIGH_CONFIDENCE_DIRECTION_MIN_DELTA_PTS:
            tier_reason = "24h Watch signals remain review-only until validation reaches the 70% target"
        else:
            tier_reason = "directional whale signal is below the historically high-confidence movement threshold"
    else:
        signal_tier = "abstain"
        display_tier = "review"
        tier_reason = "live signal is below the dashboard watch threshold"

    interval_width = max(2.5, 8.0 * (1.0 - confidence) + (1.5 if not has_whale_signal else 0.0))
    interval_low = round(_clamp(predicted_future_odds_pct - interval_width, 0.0, 100.0), 4)
    interval_high = round(_clamp(predicted_future_odds_pct + interval_width, 0.0, 100.0), 4)
    target_time = generated_at + timedelta(hours=window_hours)
    event_category = str(row.get("event_category") or "uncategorized")
    if is_high_confidence_validated_slice:
        validation_tier = "high_confidence_historical_slice"
        validation_reason = (
            "24h Watch with at least 6pt predicted movement and whale pressure above 1000 reached 81.48% direction match in the older validation sample"
            if is_high_confidence_24h_slice
            else "12h Watch with at least 5pt predicted movement reached 81.82% direction match in the older validation sample"
        )
    elif signal_tier == "watch":
        validation_tier = "review_only"
        validation_reason = "watch signal is visible, but this slice has not validated above the 70% target yet"
    else:
        validation_tier = "insufficient_validated_accuracy"
        validation_reason = "abstain and weak-signal slices validated poorly and should not be treated as reliable direction forecasts"

    display_reasons = ["live_whale_signal_model", validation_tier]
    review_reasons = (
        []
        if validation_tier == "high_confidence_historical_slice"
        else [validation_tier if signal_tier == "watch" else "insufficient_watch_confidence"]
    )
    reliability_warnings = [] if has_whale_signal else ["no_recent_whale_signal_for_side"]
    if validation_tier != "high_confidence_historical_slice":
        reliability_warnings.append(validation_tier)
    latest_entry_time = features.get("latest_entry_time")

    whale_anchor = {
        "recent_entry_count_12h": _safe_int(features.get("recent_entry_count_12h")),
        "recent_exit_count_12h": _safe_int(features.get("recent_exit_count_12h")),
        "recent_weighted_entry_12h": _safe_float(features.get("recent_weighted_entry_12h")),
        "recent_weighted_exit_12h": _safe_float(features.get("recent_weighted_exit_12h")),
        "recent_weighted_net_pressure_12h": _safe_float(features.get("recent_weighted_net_pressure_12h")),
        "side_entry_pressure": _safe_float(features.get("side_entry_pressure")),
        "side_exit_pressure": _safe_float(features.get("side_exit_pressure")),
        "side_net_pressure": _safe_float(features.get("side_net_pressure")),
        "side_total_pressure": _safe_float(features.get("side_total_pressure")),
        "pressure_ratio": pressure_ratio,
        "event_count": event_count,
        "trusted_event_count": trusted_event_count,
        "latest_entry_time": latest_entry_time,
        "latest_exit_time": features.get("latest_exit_time"),
    }

    return {
        "window": f"{window_hours}h",
        "market_slug": str(row["market_slug"]),
        "question": str(row.get("question") or ""),
        "side_label": side_label,
        "observation_time": generated_at.isoformat(),
        "event_category": event_category,
        "focus_category": event_category,
        "focused_fit_category": event_category,
        "market_family": event_category,
        "current_odds_pct": round(current_odds_pct, 4),
        "predicted_future_odds_pct": predicted_future_odds_pct,
        "predicted_delta_pts": predicted_delta_pts,
        "predicted_direction": predicted_direction,
        "prediction_source": "live_whale_signal_model",
        "display_tier": display_tier,
        "display_reasons": display_reasons,
        "review_reasons": review_reasons,
        "direction_signal_tier": signal_tier,
        "direction_signal_tier_reason": tier_reason,
        "historical_validation_tier": validation_tier,
        "historical_validation_reason": validation_reason,
        "historical_validation_direction_match_pct": (
            81.48 if is_high_confidence_24h_slice else 81.82 if is_high_confidence_12h_slice else None
        ),
        "historical_validation_sample_size": (
            27 if is_high_confidence_24h_slice else 22 if is_high_confidence_12h_slice else None
        ),
        "direction_signal_predicted_direction": predicted_direction,
        "direction_signal_confidence": round(confidence, 4),
        "reliability_warnings": reliability_warnings,
        "overlay_future_odds_pct": predicted_future_odds_pct,
        "overlay_delta_pts": predicted_delta_pts,
        "overlay_direction": predicted_direction,
        "interval_low_future_odds_pct": interval_low,
        "interval_high_future_odds_pct": interval_high,
        "trend_fit_error_type": "live_profile_inference",
        "trend_shape_score": round(confidence if has_whale_signal else 0.0, 4),
        "whale_anchor": whale_anchor,
        "live_window_features": features,
        "local_backtest_only": False,
        "whale_entry_time": latest_entry_time,
        "prediction_start_time": generated_at.isoformat(),
        "prediction_target_time": target_time.isoformat(),
        "prediction_window_hours": window_hours,
        "prediction_timeline_source": "live_whale_signal_snapshot",
        "prediction_status": "prediction_available",
    }


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
    live_prediction: dict[str, Any] | None,
    model_version: str,
    feature_schema_version: str,
    confidence_artifact: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return one database row for a market side/window prediction snapshot."""
    payload = dict(
        local_prediction
        or live_prediction
        or _pending_payload(
            market_row,
            side_label=side_label,
            window_hours=window_hours,
            generated_at=generated_at,
        )
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
    payload = apply_trained_confidence(payload, confidence_artifact)

    observation_time = _parse_iso(str(payload.get("observation_time") or "")) or generated_at
    prediction_target_time = _parse_iso(str(payload.get("prediction_target_time") or ""))
    whale_entry_time = _parse_iso(str(payload.get("whale_entry_time") or ""))
    trained_as_of = _parse_iso(str(payload.get("trained_confidence_trained_at") or ""))
    prediction_available = payload.get("predicted_future_odds_pct") is not None
    prediction_status = (
        "prediction_available"
        if prediction_available
        else str(payload.get("prediction_status") or "waiting_for_live_model_inference")
    )
    prediction_source = (
        "whale_anchored_report"
        if local_prediction
        else str(payload.get("prediction_source") or "pending_live_model_inference")
    )
    internal_prediction_source = payload.get("prediction_source")
    if internal_prediction_source and internal_prediction_source != prediction_source:
        payload["model_signal_source"] = internal_prediction_source
    payload["prediction_source"] = prediction_source
    reliability_payload = {
        "display_reasons": payload.get("display_reasons") or [],
        "review_reasons": payload.get("review_reasons") or [],
        "reliability_warnings": payload.get("reliability_warnings") or [],
        "direction_signal_tier_reason": payload.get("direction_signal_tier_reason"),
        "trained_confidence_available": payload.get("trained_confidence_available"),
        "trained_confidence_score": payload.get("trained_confidence_score"),
        "confidence_source": payload.get("confidence_source"),
        "expected_direction_error_pts": payload.get("expected_direction_error_pts"),
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
        "trained_as_of": trained_as_of,
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
    live_feature_market_limit: int,
    confidence_model_path: Path | None = DEFAULT_CONFIDENCE_MODEL_PATH,
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
    confidence_artifact = load_confidence_artifact(confidence_model_path)
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
    eligible_market_rows: list[dict[str, Any]] = []
    excluded_sports_count = 0
    for market_row in raw_market_rows:
        row = dict(market_row)
        if is_physical_sports_market(
            [row.get("market_slug"), row.get("question"), row.get("event_title"), row.get("event_slug")],
            category=row.get("event_category"),
        ):
            excluded_sports_count += 1
            continue
        eligible_market_rows.append(row)

    live_feature_market_rows = (
        eligible_market_rows[:live_feature_market_limit]
        if live_feature_market_limit > 0
        else eligible_market_rows
    )
    live_feature_index = _load_live_whale_signal_index(
        session,
        platform_name=platform_name,
        as_of=generated_at,
        market_ids=[int(row["market_contract_id"]) for row in live_feature_market_rows],
    )

    local_prediction_count = 0
    live_prediction_count = 0
    pending_prediction_count = 0
    for row in eligible_market_rows:
        local_market = local_index.get(str(row["market_slug"]))
        for side_label in _side_labels(row):
            for window_hours in PREDICTION_WINDOWS:
                local_prediction = _local_prediction_for(local_market, side_label=side_label, window_hours=window_hours)
                if local_prediction:
                    local_prediction_count += 1
                    live_prediction = None
                else:
                    live_prediction = _live_prediction_for(
                        row,
                        side_label=side_label,
                        window_hours=window_hours,
                        generated_at=generated_at,
                        live_feature_index=live_feature_index,
                    )
                    if live_prediction.get("predicted_future_odds_pct") is not None:
                        live_prediction_count += 1
                    else:
                        pending_prediction_count += 1
                snapshot_rows.append(
                    _snapshot_row(
                        row,
                        side_label=side_label,
                        window_hours=window_hours,
                        generated_at=generated_at,
                        local_prediction=local_prediction,
                        live_prediction=live_prediction,
                        model_version=model_version,
                        feature_schema_version=feature_schema_version,
                        confidence_artifact=confidence_artifact,
                    )
                )

    for start_index in range(0, len(snapshot_rows), DEFAULT_INSERT_BATCH_SIZE):
        snapshot_batch = snapshot_rows[start_index : start_index + DEFAULT_INSERT_BATCH_SIZE]
        statement = pg_insert(MlMarketPredictionSnapshot).values(snapshot_batch)
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
        "live_feature_market_count": len(live_feature_market_rows),
        "live_feature_market_limit": live_feature_market_limit,
        "report_prediction_row_count": local_prediction_count,
        "local_prediction_row_count": local_prediction_count,
        "live_model_prediction_row_count": live_prediction_count,
        "pending_prediction_row_count": pending_prediction_count,
        "model_version": model_version,
        "feature_schema_version": feature_schema_version,
        "confidence_model_loaded": bool(confidence_artifact),
        "confidence_model_version": confidence_artifact.get("model_version") if confidence_artifact else None,
        "confidence_model_trained_at": confidence_artifact.get("trained_at") if confidence_artifact else None,
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
    parser.add_argument(
        "--confidence-model-path",
        default=os.getenv("ML_PREDICTION_CONFIDENCE_MODEL_PATH", str(DEFAULT_CONFIDENCE_MODEL_PATH)),
        help="Optional trained confidence artifact generated from closed-market validations.",
    )
    parser.add_argument(
        "--live-feature-market-limit",
        type=int,
        default=int(os.getenv("ML_LIVE_FEATURE_MARKET_LIMIT", str(DEFAULT_LIVE_FEATURE_MARKET_LIMIT))),
        help=(
            "Maximum newest active markets to include in the expensive recent-whale feature scan. "
            "Use 0 for no cap; all other markets still get flat current-odds fallback predictions."
        ),
    )
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
            live_feature_market_limit=int(args.live_feature_market_limit),
            confidence_model_path=Path(args.confidence_model_path) if args.confidence_model_path else None,
        )
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
