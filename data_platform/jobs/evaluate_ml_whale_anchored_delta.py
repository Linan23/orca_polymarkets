"""Evaluate a non-flat whale-anchored delta model for trend research."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import statistics
import sys
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.jobs.export_ml_market_projection_example import (
    DEFAULT_COMPARISON_PATH,
    PREDICTION_WINDOWS,
    _clip_probability,
    _current_odds,
    _enrich_trend_features,
    _is_sports_market,
    _market_family_segment,
    _round,
    _safe_float,
    _selected_prediction_specs,
)
from data_platform.ml.market_baseline_model import (
    GROUP_KEY_COLUMN,
    REGIME_TRADE_COVERED,
    _build_rolling_splits,
    _feature_matrix,
    _filter_rows_by_regime,
    _load_training_rows,
    _research_focus_segment,
)
from data_platform.services.market_scope import DEFAULT_FOCUS_DOMAINS, matched_focus_domains


DEFAULT_DATASET_PATH = Path("data_platform/runtime/ml/resolved_market_snapshot_features_current_db_asof.csv")
DEFAULT_OUTPUT_JSON_PATH = Path("data_platform/ml/ML_WHALE_ANCHORED_DELTA_CURRENT_DB_ASOF.json")
DEFAULT_OUTPUT_MARKDOWN_PATH = Path("data_platform/ml/ML_WHALE_ANCHORED_DELTA_CURRENT_DB_ASOF.md")
DEFAULT_DIRECTION_CLASSIFIER_PATH = Path("data_platform/ml/ML_TREND_DIRECTION_CLASSIFIER_CURRENT_DB_ASOF.json")
DEFAULT_NONFLAT_THRESHOLD = 0.005
MIN_TRAIN_ROWS = 80
MAGNITUDE_SCALE_MIN = 0.75
MAGNITUDE_SCALE_MAX = 2.5
OVERLAY_MAGNITUDE_SCALE_CANDIDATES = tuple(index / 4.0 for index in range(2, 17))
OVERLAY_MAGNITUDE_SCALE_SHRINKAGE = 0.35
OVERLAY_MAGNITUDE_SCALE_MIN = 0.75
OVERLAY_MAGNITUDE_SCALE_MAX = 1.75
DIRECTION_CONFIRMED_SCALE_SHRINKAGE = 0.65
DIRECTION_CONFIRMED_SCALE_DOWNSIDE_SHRINKAGE = 0.50
DIRECTION_CONFIRMED_DEFAULT_SCALE_CAP = 1.75
DIRECTION_CONFIRMED_DEFAULT_SCALE_FLOOR = 0.75
DIRECTION_CONFIRMED_CATEGORY_SCALE_CAPS = {
    "crypto": 2.30,
    "world_geopolitics": 2.10,
    "technology": 1.20,
    "politics": 1.00,
    "video_games_esports": 1.20,
}
DIRECTION_CONFIRMED_CATEGORY_SCALE_FLOORS = {
    "crypto": 0.90,
    "world_geopolitics": 0.90,
    "technology": 0.85,
    "politics": 0.75,
    "video_games_esports": 0.90,
}
DIRECTION_CONFIRMED_MIN_SEGMENT_ROWS = 4
DIRECTION_CONFIRMED_MIN_CATEGORY_ROWS = 6
DIRECTION_CONFIRMED_MIN_GLOBAL_ROWS = 12
CRYPTO_ABSOLUTE_DIRECTION_MIN_PRIOR_ROWS = 8
CRYPTO_ABSOLUTE_DIRECTION_SOURCES = (
    "direction_signal",
    "overlay_blend",
    "whale_pressure",
    "blend",
)
CRYPTO_DIRECTION_SPLIT_MIN_ROWS = 8
CRYPTO_DIRECTION_SOURCE_SELECTOR_MIN_PRIOR_ROWS = 8
CRYPTO_SEGMENT_GATE_MIN_PRIOR_ROWS = 8
CRYPTO_SEGMENT_GATE_MIN_DIRECTION_MATCH_PCT = 70.0
CRYPTO_SEGMENT_GATE_MIN_STRONG_WATCH_ALIGNMENT_PCT = 80.0
CRYPTO_SEGMENT_GATE_RECENT_ENTRY_BUCKETS = {"entry_0_1h", "entry_1_6h"}
ABSOLUTE_MOVE_REVIEW_ONLY_CATEGORIES = {"crypto"}
ABSOLUTE_MOVE_REVIEW_MAX_DIRECTION_REGRESSION_PTS = 3.0
ESPORTS_REVIEW_OVERLAY_SCALE_MAX = 1.20
TREND_FIT_BIAS_CAP = 0.05
TREND_FIT_INTERCEPT_CAP = 0.05
TREND_FIT_SLOPE_MIN = 0.50
TREND_FIT_SLOPE_MAX = 1.75
MIN_OVERLAY_MARKET_FAMILY_CALIBRATION_ROWS = 8
QUANTILE_LOW = 0.10
QUANTILE_HIGH = 0.90
BLEND_ALPHA_CANDIDATES = tuple(index / 10.0 for index in range(0, 11))
MIN_SEGMENT_CALIBRATION_ROWS = 20
MIN_EVENT_CATEGORY_CALIBRATION_ROWS = 20
MIN_DIRECTIONAL_CALIBRATION_ROWS = 12
MIN_OVERLAY_EVENT_DIRECTION_CALIBRATION_ROWS = 6
MIN_OVERLAY_EVENT_CALIBRATION_ROWS = 8
MIN_OVERLAY_CALIBRATION_SEGMENT_DIRECTION_ROWS = 6
MIN_OVERLAY_CALIBRATION_SEGMENT_ROWS = 8
MIN_OVERLAY_FOCUS_DIRECTION_CALIBRATION_ROWS = 10
MIN_OVERLAY_FOCUS_CALIBRATION_ROWS = 16
MIN_OVERLAY_GLOBAL_CALIBRATION_ROWS = 24
MIN_DIRECTION_CONDITIONED_SCALE_ROWS = 3
OVERLAY_GATE_MIN_ROWS = 20
OVERLAY_GATE_MIN_DIRECTION_MATCH_PCT = 50.0
OVERLAY_GATE_MAX_UNDERPREDICTION_PCT = 70.0
OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS = 35.0
OVERLAY_GATE_MIN_BAND_COVERAGE_PCT = 55.0
RECENT_WINDOWS = (1, 6, 12, 24)
SURFACED_DIRECTION_TIERS = {"strong", "watch"}
EVENT_CATEGORY_ORDER = (
    "crypto",
    "politics",
    "world_geopolitics",
    "world",
    "tech",
    "technology",
    "esports",
    "video-games",
    "uncategorized",
    "other",
)
FOCUSED_FIT_CATEGORY_ORDER = (
    "crypto",
    "politics",
    "world_geopolitics",
    "technology",
    "video_games_esports",
    "other",
)
PROTECTED_FIT_CATEGORIES = {"crypto", "politics", "world_geopolitics", "technology"}
REVIEW_ONLY_FIT_CATEGORIES = {"video_games_esports"}

BASE_FEATURE_COLUMNS = (
    "last_price_side",
    "last_price_opposite",
    "price_baseline",
    "hours_to_close",
    "market_age_hours",
    "market_duration_hours",
    "price_abs_distance_from_even",
    "price_gap_side_minus_opposite",
    "trade_density_per_day",
    "last_trade_age_hours",
    "last_trade_age_side_hours",
    "side_buy_notional",
    "side_sell_notional",
    "side_net_notional",
    "side_trade_share",
    "whale_side_trade_share",
    "whale_side_notional_share",
    "whale_side_buy_notional_share",
    "whale_side_sell_notional_share",
    "whale_side_net_notional_share",
    "whale_side_weighted_buy_pressure",
    "whale_side_weighted_sell_pressure",
    "whale_side_weighted_net_pressure",
    "whale_side_weighted_net_pressure_per_side_notional",
    "whale_side_weighted_net_pressure_per_total_notional",
    "whale_side_entry_trade_count",
    "whale_side_exit_trade_count",
    "whale_side_avg_trades_per_active_day",
    "whale_side_avg_holding_hours",
    "whale_side_realized_roi",
    "trusted_whale_side_trade_share",
    "trusted_whale_side_notional_share",
    "trusted_whale_side_weighted_buy_pressure",
    "trusted_whale_side_weighted_sell_pressure",
    "trusted_whale_side_weighted_net_pressure",
    "trusted_whale_side_entry_trade_count",
    "trusted_whale_side_exit_trade_count",
    "trusted_whale_side_avg_trades_per_active_day",
    "trusted_whale_side_avg_holding_hours",
    "trusted_whale_side_realized_roi",
    "whale_vs_crowd_side_net_notional_gap",
    "trusted_whale_vs_crowd_side_net_notional_gap",
    "top_whale_side_notional_share",
    "top_trusted_whale_side_notional_share",
    "first_whale_trade_age_side_hours",
    "last_whale_trade_age_side_hours",
    "first_trusted_whale_trade_age_side_hours",
    "last_trusted_whale_trade_age_side_hours",
    "family_crypto_updown",
    "current_above_even",
    "current_extreme",
)


def _recent_feature_columns() -> tuple[str, ...]:
    """Return recent whale-flow feature names for the anchor model."""
    columns: list[str] = []
    for scope in ("whale_side", "trusted_whale_side"):
        for hours in RECENT_WINDOWS:
            columns.extend(
                (
                    f"{scope}_recent_trade_count_{hours}h",
                    f"{scope}_recent_distinct_users_{hours}h",
                    f"{scope}_recent_entry_trade_count_{hours}h",
                    f"{scope}_recent_exit_trade_count_{hours}h",
                    f"{scope}_recent_entry_notional_{hours}h",
                    f"{scope}_recent_exit_notional_{hours}h",
                    f"{scope}_recent_net_notional_{hours}h",
                    f"{scope}_recent_net_notional_share_{hours}h",
                    f"{scope}_recent_weighted_entry_pressure_{hours}h",
                    f"{scope}_recent_weighted_exit_pressure_{hours}h",
                    f"{scope}_recent_weighted_net_pressure_{hours}h",
                    f"{scope}_recent_decay_weighted_entry_pressure_{hours}h",
                    f"{scope}_recent_decay_weighted_exit_pressure_{hours}h",
                    f"{scope}_recent_decay_weighted_net_pressure_{hours}h",
                    f"{scope}_recent_entry_exit_ratio_{hours}h",
                )
            )
    return tuple(columns)


def _trend_feature_columns() -> tuple[str, ...]:
    """Return short-term trend feature names for the anchor model."""
    columns: list[str] = []
    for hours in (1, 2, 3, 6, 12, 24):
        columns.extend(
            (
                f"trend_delta_{hours}h",
                f"trend_abs_delta_{hours}h",
                f"trend_slope_{hours}h",
                f"trend_observed_{hours}h",
            )
        )
    columns.extend(("trend_acceleration_6h", "trend_acceleration_24h", "trend_points_24h"))
    return tuple(columns)


def _safe_number(row: dict[str, Any], column: str) -> float:
    """Return a finite numeric row value."""
    try:
        value = float(row.get(column) or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return value if math.isfinite(value) else 0.0


def _pct(value: float) -> float:
    """Return probability units as percentage points."""
    return _round(float(value) * 100.0, 4)


def _direction(delta: float, threshold: float) -> str:
    """Return a thresholded movement direction."""
    if delta > threshold:
        return "up"
    if delta < -threshold:
        return "down"
    return "flat"


def _focus_category(row: dict[str, Any]) -> str:
    """Return the dashboard focus category for a model row."""
    texts = [
        row.get("event_category"),
        row.get("market_slug"),
        row.get("question"),
        row.get("event_title"),
        row.get("event_slug"),
    ]
    matches = matched_focus_domains(texts, DEFAULT_FOCUS_DOMAINS)
    for domain in DEFAULT_FOCUS_DOMAINS:
        if domain in matches:
            return domain
    return "other"


def _event_category(row: dict[str, Any]) -> str:
    """Return a normalized event category label."""
    category = str(row.get("event_category") or "uncategorized").strip().casefold().replace("_", "-")
    return category or "uncategorized"


def _focused_fit_category(row: dict[str, Any]) -> str:
    """Return the split focused category used for overlay fit diagnostics."""
    event_category = _event_category(row)
    if event_category == "crypto":
        return "crypto"
    if event_category == "politics":
        return "politics"
    if event_category in {"world", "geopolitics", "geopolitical"}:
        return "world_geopolitics"
    if event_category in {"tech", "technology"}:
        return "technology"
    if event_category in {"esports", "video-games", "gaming"}:
        return "video_games_esports"

    focus_category = _focus_category(row)
    if focus_category == "technology":
        return "technology"
    if focus_category == "video-games":
        return "video_games_esports"
    return focus_category if focus_category in {"crypto", "politics"} else "other"


def _market_text(row: dict[str, Any]) -> str:
    """Return normalized market text for lightweight segment heuristics."""
    return " ".join(
        str(row.get(column) or "")
        for column in ("event_category", "market_slug", "question", "event_title", "event_slug")
    ).lower().replace("-", " ")


def _crypto_asset_segment(row: dict[str, Any]) -> str:
    """Return a stable crypto asset segment when the market text is specific enough."""
    text = _market_text(row)
    asset_tokens = (
        ("btc", (" btc ", "bitcoin")),
        ("eth", (" eth ", "ethereum")),
        ("sol", (" sol ", "solana")),
        ("xrp", (" xrp ", "ripple")),
        ("doge", (" doge ", "dogecoin")),
        ("bnb", (" bnb ", "binance")),
        ("ada", (" ada ", "cardano")),
        ("link", (" link ", "chainlink")),
    )
    padded = f" {text} "
    for asset, tokens in asset_tokens:
        if any(token in padded for token in tokens):
            return asset
    return "basket_or_other"


def _trend_calibration_segment(row: dict[str, Any]) -> str:
    """Return the most specific segment used for guarded trend-fit calibration."""
    focused = _focused_fit_category(row)
    market_family = _market_family_segment(row)
    if focused == "crypto":
        asset = _crypto_asset_segment(row)
        research_focus = _research_focus_segment(row)
        if market_family == "crypto_updown":
            return f"{research_focus}_{asset}"
        return f"category_crypto_{asset}"
    if focused == "world_geopolitics":
        return "world_geopolitics"
    if focused == "video_games_esports":
        return "video_games_esports"
    if focused in {"politics", "technology"}:
        return focused
    return market_family


def _time_to_close_bucket(row: dict[str, Any]) -> str:
    """Return the time-to-close bucket used for direction-miss diagnostics."""
    hours = _safe_number(row, "hours_to_close")
    if hours <= 0:
        return "unknown"
    if hours <= 6:
        return "0_6h"
    if hours <= 12:
        return "6_12h"
    if hours <= 24:
        return "12_24h"
    return "24h_plus"


def _recent_whale_count(row: dict[str, Any], metric: str, hours: int) -> float:
    """Return recent side-level whale count, preferring the broader all-whale count."""
    all_whales = _safe_number(row, f"whale_side_recent_{metric}_{hours}h")
    trusted_whales = _safe_number(row, f"trusted_whale_side_recent_{metric}_{hours}h")
    return max(all_whales, trusted_whales)


def _whale_entry_timing_bucket(row: dict[str, Any]) -> str:
    """Return the most recent whale-entry bucket available from cumulative recent features."""
    for hours, label in (
        (1, "entry_0_1h"),
        (6, "entry_1_6h"),
        (12, "entry_6_12h"),
        (24, "entry_12_24h"),
    ):
        if _recent_whale_count(row, "entry_trade_count", hours) > 0:
            return label
    if (
        _safe_number(row, "whale_side_entry_trade_count") > 0
        or _safe_number(row, "trusted_whale_side_entry_trade_count") > 0
    ):
        return "entry_24h_plus"
    return "no_recent_entry"


def _whale_flow_timing_bucket(row: dict[str, Any]) -> str:
    """Return the most recent entry/exit flow bucket available from cumulative recent features."""
    previous_entries = 0.0
    previous_exits = 0.0
    for hours, label in (
        (1, "0_1h"),
        (6, "1_6h"),
        (12, "6_12h"),
        (24, "12_24h"),
    ):
        entries = _recent_whale_count(row, "entry_trade_count", hours)
        exits = _recent_whale_count(row, "exit_trade_count", hours)
        bucket_entries = max(0.0, entries - previous_entries)
        bucket_exits = max(0.0, exits - previous_exits)
        previous_entries = entries
        previous_exits = exits
        if bucket_entries <= 0 and bucket_exits <= 0:
            continue
        if bucket_entries > bucket_exits:
            return f"entry_{label}"
        if bucket_exits > bucket_entries:
            return f"exit_{label}"
        return f"mixed_{label}"
    if (
        _safe_number(row, "whale_side_entry_trade_count")
        + _safe_number(row, "whale_side_exit_trade_count")
        + _safe_number(row, "trusted_whale_side_entry_trade_count")
        + _safe_number(row, "trusted_whale_side_exit_trade_count")
    ) > 0:
        return "flow_24h_plus"
    return "no_recent_flow"


def _whale_pressure_value(row: dict[str, Any], window_name: str) -> float:
    """Return the most relevant recent whale pressure value for one window."""
    hours = 24 if window_name == "24h" else 12
    candidates = (
        f"trusted_whale_side_recent_weighted_net_pressure_{hours}h",
        f"whale_side_recent_weighted_net_pressure_{hours}h",
        f"trusted_whale_side_recent_decay_weighted_net_pressure_{hours}h",
        f"whale_side_recent_decay_weighted_net_pressure_{hours}h",
        "trusted_whale_side_weighted_net_pressure",
        "whale_side_weighted_net_pressure",
    )
    for column in candidates:
        value = _safe_number(row, column)
        if abs(value) > 0:
            return value
    return 0.0


def _pressure_direction(value: float) -> str:
    """Return directional label for whale pressure."""
    if value > 0:
        return "up"
    if value < 0:
        return "down"
    return "neutral"


def _record_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    """Return the stable prediction key shared with residual prediction exports."""
    return (
        str(row[GROUP_KEY_COLUMN]),
        str(row["market_slug"]),
        str(row["side_label"]),
        str(row["observation_time"]),
    )


def _direction_tier_key(window_name: str, row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    """Return the stable join key for direction-tier support."""
    return (
        window_name,
        str(row[GROUP_KEY_COLUMN]),
        str(row["market_slug"]),
        str(row["side_label"]),
        str(row["observation_time"]),
    )


def _load_direction_tier_lookup(path: Path) -> tuple[dict[tuple[str, str, str, str, str], dict[str, Any]], dict[str, Any]]:
    """Load the compact Strong/Watch/Abstain direction-tier index."""
    if not path.exists():
        return {}, {
            "available": False,
            "path": str(path),
            "row_count": 0,
            "status": "missing_direction_classifier_report",
        }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, {
            "available": False,
            "path": str(path),
            "row_count": 0,
            "status": "invalid_direction_classifier_report",
            "error": str(exc),
        }

    raw_index = payload.get("signal_tier_index")
    rows: list[dict[str, Any]] = []
    if isinstance(raw_index, dict):
        for window_rows in raw_index.values():
            if isinstance(window_rows, list):
                rows.extend(item for item in window_rows if isinstance(item, dict))
    elif isinstance(raw_index, list):
        rows.extend(item for item in raw_index if isinstance(item, dict))

    lookup: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for item in rows:
        key = (
            str(item.get("window") or ""),
            str(item.get("condition_ref") or ""),
            str(item.get("market_slug") or ""),
            str(item.get("side_label") or ""),
            str(item.get("observation_time") or ""),
        )
        if all(key):
            lookup[key] = item

    return lookup, {
        "available": bool(lookup),
        "path": str(path),
        "row_count": len(lookup),
        "status": "loaded" if lookup else "empty_direction_tier_index",
        "generated_at": payload.get("generated_at"),
    }


def _future_observed(row: dict[str, Any], window_name: str) -> bool:
    """Return whether the future odds label exists for one window."""
    return _safe_number(row, f"future_price_observed_{window_name}") >= 0.5


def _actual_delta(row: dict[str, Any], window_name: str) -> float:
    """Return actual future odds movement from the observation point."""
    return _safe_number(row, f"future_price_side_{window_name}") - _current_odds(row)


def _has_whale_anchor(row: dict[str, Any]) -> bool:
    """Return whether a row has recent whale entry/exit context."""
    if _is_sports_market(row):
        return False
    anchor_columns = [
        "whale_side_entry_trade_count",
        "whale_side_exit_trade_count",
        "trusted_whale_side_entry_trade_count",
        "trusted_whale_side_exit_trade_count",
    ]
    for hours in RECENT_WINDOWS:
        anchor_columns.extend(
            (
                f"whale_side_recent_trade_count_{hours}h",
                f"whale_side_recent_entry_trade_count_{hours}h",
                f"whale_side_recent_exit_trade_count_{hours}h",
                f"whale_side_recent_weighted_net_pressure_{hours}h",
                f"trusted_whale_side_recent_trade_count_{hours}h",
                f"trusted_whale_side_recent_entry_trade_count_{hours}h",
                f"trusted_whale_side_recent_exit_trade_count_{hours}h",
                f"trusted_whale_side_recent_weighted_net_pressure_{hours}h",
            )
        )
    return any(abs(_safe_number(row, column)) > 0 for column in anchor_columns)


def _anchor_rows(rows: list[dict[str, Any]], window_name: str, *, threshold: float) -> list[dict[str, Any]]:
    """Return rows that fit the non-flat whale-anchored research target."""
    return [
        row
        for row in rows
        if _future_observed(row, window_name)
        and _has_whale_anchor(row)
        and abs(_actual_delta(row, window_name)) >= threshold
    ]


def _rmse(actuals: list[float], predictions: list[float]) -> float:
    """Return root mean squared error."""
    if not actuals:
        return 0.0
    return math.sqrt(
        sum((actual - predicted) ** 2 for actual, predicted in zip(actuals, predictions, strict=True)) / len(actuals)
    )


def _mae(actuals: list[float], predictions: list[float]) -> float:
    """Return mean absolute error."""
    if not actuals:
        return 0.0
    return sum(abs(actual - predicted) for actual, predicted in zip(actuals, predictions, strict=True)) / len(actuals)


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    """Return a rounded percentage ratio."""
    denominator = float(denominator)
    if denominator <= 0:
        return 0.0
    return _round(float(numerator) / denominator * 100.0, 4)


def _prediction_metrics_from_values(
    actuals: list[float],
    predictions: list[float],
    *,
    threshold: float,
) -> dict[str, Any]:
    """Return movement metrics for aligned actual/predicted deltas."""
    direction_matches = sum(
        1
        for actual, predicted in zip(actuals, predictions, strict=True)
        if _direction(actual, threshold) == _direction(predicted, threshold)
    )
    opposite_misses = sum(
        1
        for actual, predicted in zip(actuals, predictions, strict=True)
        if _direction(actual, threshold) != "flat"
        and _direction(predicted, threshold) != "flat"
        and _direction(actual, threshold) != _direction(predicted, threshold)
    )
    return {
        "row_count": len(actuals),
        "rmse_pts": _pct(_rmse(actuals, predictions)),
        "mae_pts": _pct(_mae(actuals, predictions)),
        "direction_match_pct": _safe_ratio(direction_matches, len(actuals)),
        "opposite_direction_miss_pct": _safe_ratio(opposite_misses, len(actuals)),
        "underprediction_pct": _safe_ratio(
            sum(1 for actual, predicted in zip(actuals, predictions, strict=True) if abs(predicted) < abs(actual)),
            len(actuals),
        ),
        "average_abs_actual_delta_pts": _pct(sum(abs(value) for value in actuals) / len(actuals)) if actuals else 0.0,
        "average_abs_predicted_delta_pts": _pct(sum(abs(value) for value in predictions) / len(predictions))
        if predictions
        else 0.0,
    }


def _prediction_metrics(records: list[dict[str, Any]], prediction_key: str, *, threshold: float) -> dict[str, Any]:
    """Return movement metrics for a prediction column."""
    actuals = [float(record["actual_delta"]) for record in records]
    predictions = [float(record[prediction_key]) for record in records]
    return _prediction_metrics_from_values(actuals, predictions, threshold=threshold)


def _safe_correlation(left: list[float], right: list[float]) -> float:
    """Return a bounded Pearson correlation, or 0 when not identifiable."""
    if len(left) < 3 or len(left) != len(right):
        return 0.0
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    left_var = sum((value - left_mean) ** 2 for value in left)
    right_var = sum((value - right_mean) ** 2 for value in right)
    if left_var <= 0 or right_var <= 0:
        return 0.0
    correlation = sum((a - left_mean) * (b - right_mean) for a, b in zip(left, right, strict=True))
    correlation /= math.sqrt(left_var * right_var)
    return _round(max(-1.0, min(1.0, correlation)), 4)


def _delta_metrics(after: dict[str, Any], before: dict[str, Any]) -> dict[str, Any]:
    """Return after-vs-before fit deltas."""
    return {
        "mae_delta_pts": _round(float(after["mae_pts"]) - float(before["mae_pts"]), 4),
        "direction_match_delta_pts": _round(
            float(after["direction_match_pct"]) - float(before["direction_match_pct"]),
            4,
        ),
        "underprediction_delta_pts": _round(
            float(after["underprediction_pct"]) - float(before["underprediction_pct"]),
            4,
        ),
        "average_abs_predicted_delta_delta_pts": _round(
            float(after["average_abs_predicted_delta_pts"]) - float(before["average_abs_predicted_delta_pts"]),
            4,
        ),
    }


def _interval_metrics(
    records: list[dict[str, Any]],
    *,
    low_key: str = "quantile_low_delta",
    high_key: str = "quantile_high_delta",
) -> dict[str, Any]:
    """Return quantile interval coverage metrics."""
    if not records:
        return {
            "coverage_pct": 0.0,
            "average_width_pts": 0.0,
            "missed_low_pct": 0.0,
            "missed_high_pct": 0.0,
        }

    contains = 0
    missed_low = 0
    missed_high = 0
    widths: list[float] = []
    for record in records:
        actual = float(record["actual_delta"])
        low = float(record[low_key])
        high = float(record[high_key])
        lower = min(low, high)
        upper = max(low, high)
        widths.append(upper - lower)
        if lower <= actual <= upper:
            contains += 1
        elif actual < lower:
            missed_low += 1
        else:
            missed_high += 1

    return {
        "coverage_pct": _safe_ratio(contains, len(records)),
        "average_width_pts": _pct(sum(widths) / len(widths)),
        "missed_low_pct": _safe_ratio(missed_low, len(records)),
        "missed_high_pct": _safe_ratio(missed_high, len(records)),
    }


def _window_summary(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return all model comparisons for one prediction window."""
    price_metrics = _prediction_metrics(records, "price_delta", threshold=threshold)
    residual_metrics = _prediction_metrics(records, "residual_delta", threshold=threshold)
    raw_metrics = _prediction_metrics(records, "raw_delta", threshold=threshold)
    calibrated_metrics = _prediction_metrics(records, "calibrated_delta", threshold=threshold)
    blend_metrics = _prediction_metrics(records, "blend_delta", threshold=threshold)
    pair_blend_metrics = _prediction_metrics(records, "pair_normalized_blend_delta", threshold=threshold)
    overlay_base_metrics = _prediction_metrics(records, "overlay_base_blend_delta", threshold=threshold)
    overlay_blend_metrics = _prediction_metrics(records, "overlay_blend_delta", threshold=threshold)
    direction_confirmed_metrics = (
        _prediction_metrics(records, "direction_confirmed_overlay_delta", threshold=threshold)
        if records and "direction_confirmed_overlay_delta" in records[0]
        else overlay_blend_metrics
    )
    absolute_move_metrics = (
        _prediction_metrics(records, "absolute_move_delta", threshold=threshold)
        if records and "absolute_move_delta" in records[0]
        else overlay_blend_metrics
    )
    return {
        "row_count": len(records),
        "condition_count": len({str(record["condition_ref"]) for record in records}),
        "direction_counts": dict(sorted(Counter(str(record["actual_direction"]) for record in records).items())),
        "price_only": price_metrics,
        "current_residual_whale": residual_metrics,
        "nonflat_delta_raw": raw_metrics,
        "nonflat_delta_calibrated": calibrated_metrics,
        "nonflat_delta_blend": blend_metrics,
        "pair_normalized_blend": pair_blend_metrics,
        "overlay_base_blend": overlay_base_metrics,
        "overlay_blend": overlay_blend_metrics,
        "direction_confirmed_overlay": direction_confirmed_metrics,
        "absolute_move_overlay": absolute_move_metrics,
        "quantile_interval": _interval_metrics(records),
        "pair_normalized_quantile_interval": _interval_metrics(
            records,
            low_key="pair_normalized_quantile_low_delta",
            high_key="pair_normalized_quantile_high_delta",
        ),
        "overlay_base_quantile_interval": _interval_metrics(
            records,
            low_key="overlay_base_quantile_low_delta",
            high_key="overlay_base_quantile_high_delta",
        ),
        "overlay_quantile_interval": _interval_metrics(
            records,
            low_key="overlay_quantile_low_delta",
            high_key="overlay_quantile_high_delta",
        ),
        "overlay_calibration_delta": _delta_metrics(overlay_blend_metrics, overlay_base_metrics),
        "calibrated_rmse_delta_vs_residual_pts": _round(
            calibrated_metrics["rmse_pts"] - residual_metrics["rmse_pts"],
            4,
        ),
        "calibrated_mae_delta_vs_residual_pts": _round(
            calibrated_metrics["mae_pts"] - residual_metrics["mae_pts"],
            4,
        ),
        "calibrated_direction_match_delta_vs_residual_pts": _round(
            calibrated_metrics["direction_match_pct"] - residual_metrics["direction_match_pct"],
            4,
        ),
        "calibrated_underprediction_delta_vs_residual_pts": _round(
            calibrated_metrics["underprediction_pct"] - residual_metrics["underprediction_pct"],
            4,
        ),
        "blend_rmse_delta_vs_residual_pts": _round(
            blend_metrics["rmse_pts"] - residual_metrics["rmse_pts"],
            4,
        ),
        "blend_mae_delta_vs_residual_pts": _round(
            blend_metrics["mae_pts"] - residual_metrics["mae_pts"],
            4,
        ),
        "blend_direction_match_delta_vs_residual_pts": _round(
            blend_metrics["direction_match_pct"] - residual_metrics["direction_match_pct"],
            4,
        ),
        "blend_underprediction_delta_vs_residual_pts": _round(
            blend_metrics["underprediction_pct"] - residual_metrics["underprediction_pct"],
            4,
        ),
        "pair_normalized_rmse_delta_vs_blend_pts": _round(
            pair_blend_metrics["rmse_pts"] - blend_metrics["rmse_pts"],
            4,
        ),
        "pair_normalized_direction_match_delta_vs_blend_pts": _round(
            pair_blend_metrics["direction_match_pct"] - blend_metrics["direction_match_pct"],
            4,
        ),
        "pair_normalized_underprediction_delta_vs_blend_pts": _round(
            pair_blend_metrics["underprediction_pct"] - blend_metrics["underprediction_pct"],
            4,
        ),
        "overlay_direction_match_delta_vs_pair_pts": _round(
            overlay_blend_metrics["direction_match_pct"] - pair_blend_metrics["direction_match_pct"],
            4,
        ),
        "overlay_mae_delta_vs_pair_pts": _round(
            overlay_blend_metrics["mae_pts"] - pair_blend_metrics["mae_pts"],
            4,
        ),
        "direction_confirmed_mae_delta_vs_overlay_pts": _round(
            direction_confirmed_metrics["mae_pts"] - overlay_blend_metrics["mae_pts"],
            4,
        ),
        "direction_confirmed_underprediction_delta_vs_overlay_pts": _round(
            direction_confirmed_metrics["underprediction_pct"] - overlay_blend_metrics["underprediction_pct"],
            4,
        ),
        "absolute_move_mae_delta_vs_overlay_pts": _round(
            absolute_move_metrics["mae_pts"] - overlay_blend_metrics["mae_pts"],
            4,
        ),
        "absolute_move_underprediction_delta_vs_overlay_pts": _round(
            absolute_move_metrics["underprediction_pct"] - overlay_blend_metrics["underprediction_pct"],
            4,
        ),
    }


def _select_blend_alpha(actuals: list[float], calibrated_predictions: list[float]) -> float:
    """Return a fold-local alpha that damps the aggressive delta prediction when useful."""
    if not actuals or len(actuals) != len(calibrated_predictions):
        return 1.0

    def objective(alpha: float) -> tuple[float, float]:
        predictions = [alpha * prediction for prediction in calibrated_predictions]
        return (_rmse(actuals, predictions), _mae(actuals, predictions))

    return min(BLEND_ALPHA_CANDIDATES, key=objective)


def _scale_payload(
    *,
    category: str,
    ratios: list[float],
    fallback_scale: float,
    source: str,
    min_rows: int,
) -> dict[str, Any]:
    """Return a bounded fold-local magnitude scale payload."""
    if len(ratios) >= min_rows:
        scale = statistics.median(ratios)
        selected_source = source
    else:
        scale = fallback_scale
        selected_source = "global_fallback"
    scale = max(MAGNITUDE_SCALE_MIN, min(MAGNITUDE_SCALE_MAX, scale))
    return {
        "category": category,
        "scale": scale,
        "source": selected_source,
        "train_rows": len(ratios),
    }


def _magnitude_scales_by_category(
    *,
    rows: list[dict[str, Any]],
    actuals: list[float],
    predictions: list[float],
    fallback_scale: float,
) -> dict[str, dict[str, Any]]:
    """Return fold-local magnitude scales by dashboard focus category."""
    ratios_by_category: dict[str, list[float]] = {}
    for row, actual, predicted in zip(rows, actuals, predictions, strict=True):
        if abs(predicted) < 0.0025:
            continue
        ratios_by_category.setdefault(_focus_category(row), []).append(abs(actual) / max(abs(predicted), 0.0025))

    return {
        category: _scale_payload(
            category=category,
            ratios=ratios_by_category.get(category, []),
            fallback_scale=fallback_scale,
            source="focus_category",
            min_rows=MIN_SEGMENT_CALIBRATION_ROWS,
        )
        for category in (*DEFAULT_FOCUS_DOMAINS, "other")
    }


def _magnitude_scales_by_event_category(
    *,
    rows: list[dict[str, Any]],
    actuals: list[float],
    predictions: list[float],
    fallback_scale: float,
) -> dict[str, dict[str, Any]]:
    """Return fold-local magnitude scales by raw event category."""
    ratios_by_category: dict[str, list[float]] = {}
    for row, actual, predicted in zip(rows, actuals, predictions, strict=True):
        if abs(predicted) < 0.0025:
            continue
        ratios_by_category.setdefault(_event_category(row), []).append(abs(actual) / max(abs(predicted), 0.0025))

    categories = list(EVENT_CATEGORY_ORDER)
    categories.extend(
        category
        for category in sorted(ratios_by_category)
        if category not in categories
    )
    return {
        category: _scale_payload(
            category=category,
            ratios=ratios_by_category.get(category, []),
            fallback_scale=fallback_scale,
            source="event_category",
            min_rows=MIN_EVENT_CATEGORY_CALIBRATION_ROWS,
        )
        for category in categories
    }


def _directional_magnitude_scales(
    *,
    rows: list[dict[str, Any]],
    actuals: list[float],
    predictions: list[float],
    fallback_scale: float,
    category_key: str,
    threshold: float,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Return fold-local magnitude scales by category and predicted direction."""
    ratios_by_key: dict[tuple[str, str], list[float]] = {}
    for row, actual, predicted in zip(rows, actuals, predictions, strict=True):
        if abs(predicted) < 0.0025:
            continue
        category = _event_category(row) if category_key == "event_category" else _focus_category(row)
        direction = _direction(predicted, threshold)
        ratios_by_key.setdefault((category, direction), []).append(abs(actual) / max(abs(predicted), 0.0025))

    return {
        key: _scale_payload(
            category=f"{key[0]}:{key[1]}",
            ratios=ratios,
            fallback_scale=fallback_scale,
            source=f"{category_key}_direction",
            min_rows=MIN_DIRECTIONAL_CALIBRATION_ROWS,
        )
        for key, ratios in ratios_by_key.items()
    }


def _select_magnitude_scale(
    *,
    focus_category: str,
    event_category: str,
    raw_delta: float,
    fallback_scale: float,
    focus_scales: dict[str, dict[str, Any]],
    event_scales: dict[str, dict[str, Any]],
    focus_direction_scales: dict[tuple[str, str], dict[str, Any]],
    event_direction_scales: dict[tuple[str, str], dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Return the most specific reliable magnitude calibration for a prediction row."""
    predicted_direction = _direction(raw_delta, threshold)
    candidates = (
        event_direction_scales.get((event_category, predicted_direction)),
        focus_direction_scales.get((focus_category, predicted_direction)),
        event_scales.get(event_category),
        focus_scales.get(focus_category),
    )
    for candidate in candidates:
        if candidate and candidate.get("source") != "global_fallback":
            return candidate
    return {
        "category": "global",
        "scale": fallback_scale,
        "source": "global_fallback",
        "train_rows": 0,
    }


def _blend_alphas_by_category(
    *,
    rows: list[dict[str, Any]],
    actuals: list[float],
    calibrated_predictions: list[float],
    fallback_alpha: float,
) -> dict[str, dict[str, Any]]:
    """Return fold-local blend alphas by dashboard focus category."""
    grouped: dict[str, dict[str, list[float]]] = {}
    for row, actual, predicted in zip(rows, actuals, calibrated_predictions, strict=True):
        category = _focus_category(row)
        bucket = grouped.setdefault(category, {"actuals": [], "predictions": []})
        bucket["actuals"].append(actual)
        bucket["predictions"].append(predicted)

    alphas: dict[str, dict[str, Any]] = {}
    for category in (*DEFAULT_FOCUS_DOMAINS, "other"):
        bucket = grouped.get(category, {"actuals": [], "predictions": []})
        actual_values = bucket["actuals"]
        prediction_values = bucket["predictions"]
        if len(actual_values) >= MIN_SEGMENT_CALIBRATION_ROWS:
            alpha = _select_blend_alpha(actual_values, prediction_values)
            source = "category"
        else:
            alpha = fallback_alpha
            source = "global_fallback"
        alphas[category] = {
            "category": category,
            "alpha": alpha,
            "source": source,
            "train_rows": len(actual_values),
        }
    return alphas


def _future_from_delta(record: dict[str, Any], delta_key: str) -> float:
    """Return clipped future odds implied by a record delta."""
    return _clip_probability(float(record["current_odds"]) + float(record[delta_key]))


def _safe_normalized_probability(value: float, counterpart_value: float, fallback: float) -> float:
    """Normalize one side of a binary pair, falling back when the pair is invalid."""
    total = value + counterpart_value
    if not math.isfinite(total) or total <= 0:
        return fallback
    return _clip_probability(value / total)


def _set_pair_normalization_fallback(record: dict[str, Any], reason: str, *, threshold: float) -> None:
    """Populate pair-normalized fields with unnormalized values."""
    record["pair_normalized"] = False
    record["pair_normalization_reason"] = reason
    record["pair_normalized_residual_delta"] = float(record["residual_delta"])
    record["pair_normalized_blend_delta"] = float(record["blend_delta"])
    record["pair_normalized_quantile_low_delta"] = float(record["quantile_low_delta"])
    record["pair_normalized_quantile_high_delta"] = float(record["quantile_high_delta"])
    record["pair_normalized_blend_abs_error"] = abs(float(record["actual_delta"]) - float(record["blend_delta"]))
    record["pair_normalized_blend_direction"] = _direction(float(record["blend_delta"]), threshold)
    record["pair_normalized_interval_contains_actual"] = bool(record["interval_contains_actual"])


def _apply_pair_normalization(records: list[dict[str, Any]], *, threshold: float) -> None:
    """Normalize binary side predictions so paired outcomes sum to roughly 100%."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        _set_pair_normalization_fallback(record, "missing_binary_pair", threshold=threshold)
        grouped.setdefault((str(record["condition_ref"]), str(record["row"]["observation_time"])), []).append(record)

    for group_records in grouped.values():
        if len(group_records) != 2:
            reason = "nonbinary_or_incomplete_pair"
            for record in group_records:
                record["pair_normalization_reason"] = reason
            continue
        if len({str(record["row"]["side_label"]).lower() for record in group_records}) != 2:
            for record in group_records:
                record["pair_normalization_reason"] = "duplicate_side_pair"
            continue

        first, second = group_records
        first_blend_future = _future_from_delta(first, "blend_delta")
        second_blend_future = _future_from_delta(second, "blend_delta")
        if first_blend_future + second_blend_future <= 0:
            for record in group_records:
                record["pair_normalization_reason"] = "invalid_pair_sum"
            continue

        first_residual_future = _future_from_delta(first, "residual_delta")
        second_residual_future = _future_from_delta(second, "residual_delta")
        first_low_future = _future_from_delta(first, "quantile_low_delta")
        first_high_future = _future_from_delta(first, "quantile_high_delta")
        second_low_future = _future_from_delta(second, "quantile_low_delta")
        second_high_future = _future_from_delta(second, "quantile_high_delta")

        pair_values = (
            (first, first_blend_future, second_blend_future, first_residual_future, second_residual_future, first_low_future, first_high_future, second_low_future, second_high_future),
            (second, second_blend_future, first_blend_future, second_residual_future, first_residual_future, second_low_future, second_high_future, first_low_future, first_high_future),
        )
        for (
            record,
            blend_future,
            counterpart_blend_future,
            residual_future,
            counterpart_residual_future,
            low_future,
            high_future,
            counterpart_low_future,
            counterpart_high_future,
        ) in pair_values:
            current_odds = float(record["current_odds"])
            normalized_blend_future = _safe_normalized_probability(
                blend_future,
                counterpart_blend_future,
                blend_future,
            )
            normalized_residual_future = _safe_normalized_probability(
                residual_future,
                counterpart_residual_future,
                residual_future,
            )
            normalized_low_future = _safe_normalized_probability(
                low_future,
                counterpart_high_future,
                low_future,
            )
            normalized_high_future = _safe_normalized_probability(
                high_future,
                counterpart_low_future,
                high_future,
            )
            pair_low_delta = min(normalized_low_future, normalized_high_future) - current_odds
            pair_high_delta = max(normalized_low_future, normalized_high_future) - current_odds
            pair_blend_delta = normalized_blend_future - current_odds
            record["pair_normalized"] = True
            record["pair_normalization_reason"] = "binary_pair_normalized"
            record["pair_normalized_residual_delta"] = normalized_residual_future - current_odds
            record["pair_normalized_blend_delta"] = pair_blend_delta
            record["pair_normalized_quantile_low_delta"] = pair_low_delta
            record["pair_normalized_quantile_high_delta"] = pair_high_delta
            record["pair_normalized_blend_abs_error"] = abs(float(record["actual_delta"]) - pair_blend_delta)
            record["pair_normalized_blend_direction"] = _direction(pair_blend_delta, threshold)
            record["pair_normalized_interval_contains_actual"] = pair_low_delta <= float(record["actual_delta"]) <= pair_high_delta


def _pair_normalization_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return pair-normalization coverage for one window."""
    normalized = [record for record in records if bool(record.get("pair_normalized"))]
    normalized_groups = {
        (str(record["condition_ref"]), str(record["row"]["observation_time"]))
        for record in normalized
    }
    return {
        "normalized_row_count": len(normalized),
        "normalized_row_pct": _safe_ratio(len(normalized), len(records)),
        "normalized_pair_count": len(normalized_groups),
        "reason_counts": dict(sorted(Counter(str(record.get("pair_normalization_reason")) for record in records).items())),
    }


def _apply_overlay_blend_selection(records: list[dict[str, Any]], *, threshold: float) -> None:
    """Choose the displayed overlay delta without letting pair normalization force a direction conflict."""
    for record in records:
        signal_direction = str(record.get("direction_signal_predicted_direction") or "flat")
        pair_direction = str(record.get("pair_normalized_blend_direction") or "flat")
        raw_direction = str(record.get("blend_direction") or "flat")
        if signal_direction in {"up", "down"} and pair_direction == signal_direction:
            source = "pair_normalized_direction_aligned"
            delta_key = "pair_normalized_blend_delta"
            low_key = "pair_normalized_quantile_low_delta"
            high_key = "pair_normalized_quantile_high_delta"
        elif signal_direction in {"up", "down"} and raw_direction == signal_direction:
            source = "raw_blend_direction_preserved"
            delta_key = "blend_delta"
            low_key = "quantile_low_delta"
            high_key = "quantile_high_delta"
        else:
            source = "pair_normalized_default"
            delta_key = "pair_normalized_blend_delta"
            low_key = "pair_normalized_quantile_low_delta"
            high_key = "pair_normalized_quantile_high_delta"

        overlay_delta = float(record[delta_key])
        overlay_low = float(record[low_key])
        overlay_high = float(record[high_key])
        record["overlay_blend_source"] = source
        record["overlay_base_blend_delta"] = overlay_delta
        record["overlay_base_quantile_low_delta"] = min(overlay_low, overlay_high)
        record["overlay_base_quantile_high_delta"] = max(overlay_low, overlay_high)
        record["overlay_blend_delta"] = overlay_delta
        record["overlay_blend_direction"] = _direction(overlay_delta, threshold)
        record["overlay_blend_abs_error"] = abs(float(record["actual_delta"]) - overlay_delta)
        record["overlay_quantile_low_delta"] = float(record["overlay_base_quantile_low_delta"])
        record["overlay_quantile_high_delta"] = float(record["overlay_base_quantile_high_delta"])
        record["overlay_interval_contains_actual"] = (
            float(record["overlay_quantile_low_delta"])
            <= float(record["actual_delta"])
            <= float(record["overlay_quantile_high_delta"])
        )
        record["overlay_magnitude_scale"] = 1.0
        record["overlay_magnitude_scale_source"] = "identity"
        record["overlay_magnitude_scale_train_rows"] = 0
        record["overlay_trend_fit_method"] = "identity"
        record["selected_calibration_source"] = "identity"
        record["selected_calibration_method"] = "identity"
        record["selected_calibration_bias"] = 0.0
        record["selected_calibration_slope"] = 1.0
        record["selected_calibration_intercept"] = 0.0
        record["selected_calibration_train_direction_match_pct"] = 0.0
        record["selected_calibration_base_train_direction_match_pct"] = 0.0
        record["selected_calibration_train_mae_delta_pts"] = 0.0
        record["selected_calibration_train_direction_delta_pts"] = 0.0
        record["trend_fit_candidate_outcomes"] = []
        record["trend_fit_skipped_sources"] = []


def _fit_overlay_magnitude_scale(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a positive scale that minimizes historical overlay MAE."""
    if not records:
        return {"scale": 1.0, "mae_pts": 0.0, "base_mae_pts": 0.0}
    actuals = [float(record["actual_delta"]) for record in records]
    predictions = [float(record["overlay_base_blend_delta"]) for record in records]
    base_mae = _mae(actuals, predictions)

    def objective(scale: float) -> tuple[float, float]:
        scaled = [prediction * scale for prediction in predictions]
        return (_mae(actuals, scaled), _rmse(actuals, scaled))

    scale = min(OVERLAY_MAGNITUDE_SCALE_CANDIDATES, key=objective)
    scaled_mae, _ = objective(scale)
    if scaled_mae >= base_mae:
        scale = 1.0
        scaled_mae = base_mae
    scale = 1.0 + ((scale - 1.0) * OVERLAY_MAGNITUDE_SCALE_SHRINKAGE)
    scale = max(OVERLAY_MAGNITUDE_SCALE_MIN, min(OVERLAY_MAGNITUDE_SCALE_MAX, scale))
    scaled_mae, _ = objective(scale)
    return {
        "scale": scale,
        "mae_pts": _pct(scaled_mae),
        "base_mae_pts": _pct(base_mae),
    }


def _overlay_scale_payload(
    *,
    records: list[dict[str, Any]],
    source: str,
    min_rows: int,
) -> dict[str, Any] | None:
    """Return fitted overlay-scale payload if a historical slice is large enough."""
    if len(records) < min_rows:
        return None
    payload = _fit_overlay_magnitude_scale(records)
    return {
        "scale": float(payload["scale"]),
        "source": source,
        "train_rows": len(records),
        "train_mae_pts": payload["mae_pts"],
        "base_train_mae_pts": payload["base_mae_pts"],
    }


def _cap_signed(value: float, cap: float) -> float:
    """Return a finite signed value constrained to a symmetric cap."""
    if not math.isfinite(value):
        return 0.0
    return max(-cap, min(cap, value))


def _direction_sign(label: str) -> float:
    """Return signed direction multiplier for up/down labels."""
    if label == "up":
        return 1.0
    if label == "down":
        return -1.0
    return 0.0


def _direction_conditioned_scale_payload(
    *,
    records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Fit separate magnitude scales for Strong/Watch up and down direction signals."""
    del threshold
    direction_scales: dict[str, float] = {}
    for direction in ("up", "down"):
        direction_records = [
            record
            for record in records
            if str(record.get("direction_signal_predicted_direction") or "flat") == direction
        ]
        if len(direction_records) < MIN_DIRECTION_CONDITIONED_SCALE_ROWS:
            continue
        actuals = [float(record["actual_delta"]) for record in direction_records]
        sign = _direction_sign(direction)
        base_predictions = [sign * abs(float(record["overlay_base_blend_delta"])) for record in direction_records]
        base_mae = _mae(actuals, base_predictions)

        def objective(scale: float) -> tuple[float, float]:
            scaled = [prediction * scale for prediction in base_predictions]
            return (_mae(actuals, scaled), _rmse(actuals, scaled))

        scale = min(OVERLAY_MAGNITUDE_SCALE_CANDIDATES, key=objective)
        scaled_mae, _ = objective(scale)
        if scaled_mae >= base_mae:
            scale = 1.0
        scale = 1.0 + ((scale - 1.0) * OVERLAY_MAGNITUDE_SCALE_SHRINKAGE)
        direction_scales[direction] = max(OVERLAY_MAGNITUDE_SCALE_MIN, min(OVERLAY_MAGNITUDE_SCALE_MAX, scale))
    return direction_scales


def _direction_conditioned_predictions(
    records: list[dict[str, Any]],
    direction_scales: dict[str, float],
) -> list[float]:
    """Return signed magnitude predictions using the row's Strong/Watch direction signal."""
    predictions: list[float] = []
    for record in records:
        direction = str(record.get("direction_signal_predicted_direction") or "flat")
        sign = _direction_sign(direction)
        if sign == 0.0:
            predictions.append(float(record["overlay_base_blend_delta"]))
            continue
        predictions.append(sign * abs(float(record["overlay_base_blend_delta"])) * float(direction_scales.get(direction, 1.0)))
    return predictions


def _transform_delta(value: float, payload: dict[str, Any]) -> float:
    """Apply a selected trend-fit calibration transform to one delta."""
    method = str(payload.get("method") or "identity")
    if method == "magnitude_scale":
        return value * float(payload.get("scale") or 1.0)
    if method == "direction_conditioned_magnitude":
        return float(payload.get("direction_sign") or 0.0) * abs(value) * float(payload.get("scale") or 1.0)
    if method == "signed_bias":
        return value + float(payload.get("bias") or 0.0)
    if method == "slope_intercept":
        return value * float(payload.get("slope") or 1.0) + float(payload.get("intercept") or 0.0)
    return value


def _candidate_metrics(
    *,
    records: list[dict[str, Any]],
    predictions: list[float],
    threshold: float,
    source: str,
    method: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return train-fit metrics for one calibration candidate."""
    actuals = [float(record["actual_delta"]) for record in records]
    metrics = _prediction_metrics_from_values(actuals, predictions, threshold=threshold)
    payload = {
        "source": source,
        "method": method,
        "train_rows": len(records),
        "train_mae_pts": metrics["mae_pts"],
        "train_direction_match_pct": metrics["direction_match_pct"],
        "train_underprediction_pct": metrics["underprediction_pct"],
        "train_average_abs_predicted_delta_pts": metrics["average_abs_predicted_delta_pts"],
    }
    if params:
        payload.update(params)
    return payload


def _trend_fit_candidates_for_records(
    *,
    records: list[dict[str, Any]],
    source: str,
    threshold: float,
) -> list[dict[str, Any]]:
    """Return identity, scale, bias, and linear candidates for a prior OOS slice."""
    if not records:
        return []

    actuals = [float(record["actual_delta"]) for record in records]
    base_predictions = [float(record["overlay_base_blend_delta"]) for record in records]
    candidates = [
        _candidate_metrics(
            records=records,
            predictions=base_predictions,
            threshold=threshold,
            source=source,
            method="identity",
            params={"scale": 1.0, "bias": 0.0, "slope": 1.0, "intercept": 0.0},
        )
    ]

    scale_payload = _fit_overlay_magnitude_scale(records)
    scale = float(scale_payload["scale"])
    candidates.append(
        _candidate_metrics(
            records=records,
            predictions=[prediction * scale for prediction in base_predictions],
            threshold=threshold,
            source=source,
            method="magnitude_scale",
            params={
                "scale": scale,
                "bias": 0.0,
                "slope": 1.0,
                "intercept": 0.0,
            },
        )
    )

    direction_scales = _direction_conditioned_scale_payload(records=records, threshold=threshold)
    if direction_scales:
        candidates.append(
            _candidate_metrics(
                records=records,
                predictions=_direction_conditioned_predictions(records, direction_scales),
                threshold=threshold,
                source=source,
                method="direction_conditioned_magnitude",
                params={
                    "scale": 1.0,
                    "bias": 0.0,
                    "slope": 1.0,
                    "intercept": 0.0,
                    "direction_scales": direction_scales,
                },
            )
        )

    bias = _cap_signed(
        sum(actual - predicted for actual, predicted in zip(actuals, base_predictions, strict=True)) / len(records),
        TREND_FIT_BIAS_CAP,
    )
    candidates.append(
        _candidate_metrics(
            records=records,
            predictions=[prediction + bias for prediction in base_predictions],
            threshold=threshold,
            source=source,
            method="signed_bias",
            params={
                "scale": 1.0,
                "bias": bias,
                "slope": 1.0,
                "intercept": 0.0,
            },
        )
    )

    prediction_mean = sum(base_predictions) / len(base_predictions)
    actual_mean = sum(actuals) / len(actuals)
    prediction_var = sum((prediction - prediction_mean) ** 2 for prediction in base_predictions)
    if prediction_var > 0:
        covariance = sum(
            (prediction - prediction_mean) * (actual - actual_mean)
            for actual, prediction in zip(actuals, base_predictions, strict=True)
        )
        slope = covariance / prediction_var
    else:
        slope = 1.0
    slope = max(TREND_FIT_SLOPE_MIN, min(TREND_FIT_SLOPE_MAX, slope))
    intercept = _cap_signed(actual_mean - slope * prediction_mean, TREND_FIT_INTERCEPT_CAP)
    candidates.append(
        _candidate_metrics(
            records=records,
            predictions=[prediction * slope + intercept for prediction in base_predictions],
            threshold=threshold,
            source=source,
            method="slope_intercept",
            params={
                "scale": 1.0,
                "bias": 0.0,
                "slope": slope,
                "intercept": intercept,
            },
        )
    )
    base_mae = float(candidates[0]["train_mae_pts"])
    base_direction = float(candidates[0]["train_direction_match_pct"])
    for candidate in candidates:
        candidate["train_mae_delta_pts"] = _round(float(candidate["train_mae_pts"]) - base_mae, 4)
        candidate["train_direction_delta_pts"] = _round(
            float(candidate["train_direction_match_pct"]) - base_direction,
            4,
        )
        candidate["passes_train_guard"] = (
            float(candidate["train_mae_pts"]) <= base_mae + 0.0001
            and float(candidate["train_direction_match_pct"]) + 0.0001 >= base_direction
        )
    return candidates


def _select_trend_fit_candidate(
    *,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return the strongest train-safe calibration candidate."""
    if not candidates:
        return {
            "method": "identity",
            "source": "insufficient_prior_overlay_history",
            "train_rows": 0,
            "train_mae_pts": 0.0,
            "base_train_mae_pts": 0.0,
            "train_direction_match_pct": 0.0,
            "base_train_direction_match_pct": 0.0,
            "train_underprediction_pct": 0.0,
            "train_mae_delta_pts": 0.0,
            "train_direction_delta_pts": 0.0,
            "scale": 1.0,
            "bias": 0.0,
            "slope": 1.0,
            "intercept": 0.0,
            "candidate_outcomes": [],
        }
    identity = next((candidate for candidate in candidates if candidate["method"] == "identity"), candidates[0])
    safe_candidates = [candidate for candidate in candidates if bool(candidate.get("passes_train_guard"))]
    if not safe_candidates:
        safe_candidates = [identity]
    best = min(
        safe_candidates,
        key=lambda candidate: (
            float(candidate["train_mae_pts"]),
            -float(candidate["train_direction_match_pct"]),
            float(candidate["train_underprediction_pct"]),
            0 if candidate["method"] != "identity" else 1,
        ),
    )
    return {
        **best,
        "base_train_mae_pts": identity["train_mae_pts"],
        "base_train_direction_match_pct": identity["train_direction_match_pct"],
        "candidate_outcomes": [
            {
                "source": candidate["source"],
                "method": candidate["method"],
                "train_rows": candidate["train_rows"],
                "train_mae_pts": candidate["train_mae_pts"],
                "train_mae_delta_pts": candidate["train_mae_delta_pts"],
                "train_direction_match_pct": candidate["train_direction_match_pct"],
                "train_direction_delta_pts": candidate["train_direction_delta_pts"],
                "train_underprediction_pct": candidate["train_underprediction_pct"],
                "passes_train_guard": bool(candidate["passes_train_guard"]),
            }
            for candidate in candidates
        ],
    }


def _has_direction_scale_support(record: dict[str, Any]) -> bool:
    """Return whether the direction-tier signal supports magnitude scaling."""
    return (
        str(record.get("direction_signal_tier")) in SURFACED_DIRECTION_TIERS
        and str(record.get("direction_signal_predicted_direction")) == str(record.get("overlay_blend_direction"))
        and str(record.get("overlay_blend_direction")) in {"up", "down"}
    )


def _direction_confirmed_support_source(record: dict[str, Any]) -> str:
    """Return the non-leaky signal source that supports stronger magnitude display."""
    signal_direction = str(record.get("direction_signal_predicted_direction") or "flat")
    if str(record.get("direction_signal_tier")) in SURFACED_DIRECTION_TIERS and signal_direction in {"up", "down"}:
        return "strong_watch_direction"
    overlay_direction = str(record.get("overlay_blend_direction") or "flat")
    if overlay_direction not in {"up", "down"}:
        return "none"
    if (
        str(record.get("whale_pressure_direction") or "neutral") == overlay_direction
        and abs(float(record.get("whale_pressure_value") or 0.0)) > 0.0
    ):
        return "whale_pressure_aligned"
    return "none"


def _has_direction_confirmed_magnitude_support(record: dict[str, Any]) -> bool:
    """Return whether a row can receive the experimental magnitude-close overlay."""
    return _direction_confirmed_support_source(record) != "none"


def _direction_confirmed_base_direction(record: dict[str, Any]) -> str:
    """Return the direction used by the experimental magnitude-close overlay."""
    if _direction_confirmed_support_source(record) == "strong_watch_direction":
        return str(record.get("direction_signal_predicted_direction") or "flat")
    return str(record.get("overlay_blend_direction") or "flat")


def _direction_confirmed_base_delta(record: dict[str, Any]) -> float:
    """Return the signed base delta used before experimental magnitude scaling."""
    base_delta = float(record["overlay_blend_delta"])
    direction = _direction_confirmed_base_direction(record)
    sign = _direction_sign(direction)
    if sign == 0.0:
        return base_delta
    return sign * abs(base_delta)


def _select_overlay_magnitude_scale(
    *,
    calibration_records: list[dict[str, Any]],
    record: dict[str, Any],
    require_direction_support: bool = True,
) -> dict[str, Any]:
    """Return the most specific non-leaky overlay magnitude scale for a row."""
    eligible_records = [
        item
        for item in calibration_records
        if not require_direction_support or _has_direction_scale_support(item)
    ]
    event_category = str(record.get("event_category") or "uncategorized")
    focus_category = str(record.get("focus_category") or "other")
    direction = str(record.get("overlay_blend_direction") or "flat")

    candidate_specs = [
        (
            "event_category_direction",
            MIN_OVERLAY_EVENT_DIRECTION_CALIBRATION_ROWS,
            lambda item: str(item.get("event_category")) == event_category
            and str(item.get("overlay_blend_direction")) == direction,
        ),
        (
            "event_category",
            MIN_OVERLAY_EVENT_CALIBRATION_ROWS,
            lambda item: str(item.get("event_category")) == event_category,
        ),
        (
            "focus_category_direction",
            MIN_OVERLAY_FOCUS_DIRECTION_CALIBRATION_ROWS,
            lambda item: str(item.get("focus_category")) == focus_category
            and str(item.get("overlay_blend_direction")) == direction,
        ),
        (
            "focus_category",
            MIN_OVERLAY_FOCUS_CALIBRATION_ROWS,
            lambda item: str(item.get("focus_category")) == focus_category,
        ),
        (
            "global",
            MIN_OVERLAY_GLOBAL_CALIBRATION_ROWS,
            lambda item: True,
        ),
    ]

    for source, min_rows, predicate in candidate_specs:
        payload = _overlay_scale_payload(
            records=[item for item in eligible_records if predicate(item)],
            source=source,
            min_rows=min_rows,
        )
        if payload:
            return payload
    return {
        "scale": 1.0,
        "source": "insufficient_prior_overlay_history",
        "train_rows": 0,
        "train_mae_pts": 0.0,
        "base_train_mae_pts": 0.0,
    }


def _select_overlay_trend_fit_policy(
    *,
    calibration_records: list[dict[str, Any]],
    record: dict[str, Any],
    require_direction_support: bool = True,
    threshold: float,
) -> dict[str, Any]:
    """Return the most specific train-safe trend-fit calibration policy for a row."""
    eligible_records = [
        item
        for item in calibration_records
        if not require_direction_support or _has_direction_scale_support(item)
    ]
    event_category = str(record.get("event_category") or "uncategorized")
    focused_fit_category = str(record.get("focused_fit_category") or record.get("focus_category") or "other")
    market_family = str(record.get("market_family") or "unknown")
    calibration_segment = str(record.get("trend_calibration_segment") or market_family)
    direction = str(record.get("overlay_blend_direction") or "flat")

    candidate_specs = [
        (
            "calibration_segment_direction",
            MIN_OVERLAY_CALIBRATION_SEGMENT_DIRECTION_ROWS,
            lambda item: str(item.get("trend_calibration_segment")) == calibration_segment
            and str(item.get("overlay_blend_direction")) == direction,
        ),
        (
            "calibration_segment",
            MIN_OVERLAY_CALIBRATION_SEGMENT_ROWS,
            lambda item: str(item.get("trend_calibration_segment")) == calibration_segment,
        ),
        (
            "event_category_direction",
            MIN_OVERLAY_EVENT_DIRECTION_CALIBRATION_ROWS,
            lambda item: str(item.get("event_category")) == event_category
            and str(item.get("overlay_blend_direction")) == direction,
        ),
        (
            "event_category",
            MIN_OVERLAY_EVENT_CALIBRATION_ROWS,
            lambda item: str(item.get("event_category")) == event_category,
        ),
        (
            "market_family_in_focused_category_direction",
            MIN_OVERLAY_EVENT_DIRECTION_CALIBRATION_ROWS,
            lambda item: str(item.get("focused_fit_category")) == focused_fit_category
            and str(item.get("market_family")) == market_family
            and str(item.get("overlay_blend_direction")) == direction,
        ),
        (
            "market_family_in_focused_category",
            MIN_OVERLAY_MARKET_FAMILY_CALIBRATION_ROWS,
            lambda item: str(item.get("focused_fit_category")) == focused_fit_category
            and str(item.get("market_family")) == market_family,
        ),
        (
            "focused_fit_category_direction",
            MIN_OVERLAY_FOCUS_DIRECTION_CALIBRATION_ROWS,
            lambda item: str(item.get("focused_fit_category")) == focused_fit_category
            and str(item.get("overlay_blend_direction")) == direction,
        ),
        (
            "focused_fit_category",
            MIN_OVERLAY_FOCUS_CALIBRATION_ROWS,
            lambda item: str(item.get("focused_fit_category")) == focused_fit_category,
        ),
        (
            "global",
            MIN_OVERLAY_GLOBAL_CALIBRATION_ROWS,
            lambda item: True,
        ),
    ]

    skipped_sources: list[dict[str, Any]] = []
    source_payloads: list[dict[str, Any]] = []
    candidate_outcomes: list[dict[str, Any]] = []
    for priority, (source, min_rows, predicate) in enumerate(candidate_specs):
        source_records = [item for item in eligible_records if predicate(item)]
        if len(source_records) < min_rows:
            skipped_sources.append(
                {
                    "source": source,
                    "rows": len(source_records),
                    "min_rows": min_rows,
                    "reason": "insufficient_prior_rows",
                }
            )
            continue
        candidates = _trend_fit_candidates_for_records(
            records=source_records,
            source=source,
            threshold=threshold,
        )
        selected_for_source = _select_trend_fit_candidate(candidates=candidates)
        source_outcomes = list(selected_for_source.get("candidate_outcomes") or [])
        identity = next((candidate for candidate in candidates if candidate["method"] == "identity"), candidates[0])
        for candidate in candidates:
            source_payloads.append(
                {
                    **candidate,
                    "base_train_mae_pts": identity["train_mae_pts"],
                    "base_train_direction_match_pct": identity["train_direction_match_pct"],
                    "candidate_outcomes": source_outcomes,
                    "source_priority": priority,
                }
            )
        candidate_outcomes.extend(source_outcomes)

    if source_payloads:
        method_rank = {
            "direction_conditioned_magnitude": 0,
            "magnitude_scale": 1,
            "signed_bias": 2,
            "slope_intercept": 3,
            "identity": 4,
        }
        improved_payloads = [
            payload
            for payload in source_payloads
            if str(payload.get("method") or "identity") != "identity"
            and not (
                focused_fit_category == "crypto"
                and str(payload.get("method") or "identity") == "slope_intercept"
            )
            and float(payload.get("train_mae_delta_pts") or 0.0) < -0.0001
            and float(payload.get("train_direction_delta_pts") or 0.0) >= -0.0001
            and bool(payload.get("passes_train_guard"))
        ]
        if improved_payloads:
            payload = min(
                improved_payloads,
                key=lambda item: (
                    float(item.get("train_mae_pts") or 0.0),
                    method_rank.get(str(item.get("method") or "identity"), 99),
                    int(item.get("source_priority") or 999),
                    -float(item.get("train_direction_match_pct") or 0.0),
                ),
            )
        else:
            identity_payloads = [
                payload
                for payload in source_payloads
                if str(payload.get("method") or "identity") == "identity"
            ]
            payload = min(
                identity_payloads or source_payloads,
                key=lambda item: (
                    int(item.get("source_priority") or 999),
                    float(item.get("train_mae_pts") or 0.0),
                ),
            )
            if str(payload.get("method") or "identity") != "identity":
                payload = {
                    **payload,
                    "method": "identity",
                    "scale": 1.0,
                    "bias": 0.0,
                    "slope": 1.0,
                    "intercept": 0.0,
                    "source": str(payload.get("source") or "identity"),
                    "train_mae_pts": payload.get("base_train_mae_pts", payload.get("train_mae_pts", 0.0)),
                    "train_direction_match_pct": payload.get(
                        "base_train_direction_match_pct",
                        payload.get("train_direction_match_pct", 0.0),
                    ),
                    "train_mae_delta_pts": 0.0,
                    "train_direction_delta_pts": 0.0,
                }
        payload["skipped_sources"] = skipped_sources
        payload["candidate_outcomes"] = candidate_outcomes
        return payload

    payload = _select_trend_fit_candidate(candidates=[])
    payload["skipped_sources"] = skipped_sources
    return payload


def _apply_overlay_magnitude_recalibration(records: list[dict[str, Any]], *, threshold: float) -> None:
    """Calibrate overlay trend fit using only earlier out-of-sample folds."""
    fold_indexes = sorted({int(record.get("fold_index") or 0) for record in records})
    for fold_index in fold_indexes:
        calibration_records = [record for record in records if int(record.get("fold_index") or 0) < fold_index]
        fold_records = [record for record in records if int(record.get("fold_index") or 0) == fold_index]
        for record in fold_records:
            has_direction_support = _has_direction_scale_support(record)
            if has_direction_support:
                payload = _select_overlay_trend_fit_policy(
                    calibration_records=calibration_records,
                    record=record,
                    threshold=threshold,
                )
            elif str(record.get("event_category")) == "esports":
                payload = _select_overlay_trend_fit_policy(
                    calibration_records=calibration_records,
                    record=record,
                    require_direction_support=False,
                    threshold=threshold,
                )
                if payload["source"] == "insufficient_prior_overlay_history":
                    payload = {
                        **payload,
                        "method": "identity",
                        "source": "no_direction_tier_scale_support",
                    }
                else:
                    payload = {
                        **payload,
                        "scale": min(float(payload["scale"]), ESPORTS_REVIEW_OVERLAY_SCALE_MAX),
                        "source": f"esports_review_{payload['source']}",
                    }
            else:
                payload = {
                    "method": "identity",
                    "scale": 1.0,
                    "bias": 0.0,
                    "slope": 1.0,
                    "intercept": 0.0,
                    "source": "no_direction_tier_scale_support",
                    "train_rows": 0,
                    "train_mae_pts": 0.0,
                    "base_train_mae_pts": 0.0,
                    "train_direction_match_pct": 0.0,
                    "base_train_direction_match_pct": 0.0,
                    "train_mae_delta_pts": 0.0,
                    "train_direction_delta_pts": 0.0,
                    "candidate_outcomes": [],
                    "skipped_sources": [],
                }
            method = str(payload.get("method") or "identity")
            scale = float(payload.get("scale") or 1.0)
            bias = float(payload.get("bias") or 0.0)
            slope = float(payload.get("slope") or 1.0)
            intercept = float(payload.get("intercept") or 0.0)
            signal_direction = str(record.get("direction_signal_predicted_direction") or "flat")
            direction_sign = _direction_sign(signal_direction)
            if method == "direction_conditioned_magnitude":
                direction_scales = payload.get("direction_scales") or {}
                if direction_sign == 0.0 or signal_direction not in direction_scales:
                    method = "identity"
                    scale = 1.0
                    direction_sign = 0.0
                else:
                    scale = float(direction_scales.get(signal_direction, 1.0))
            base_delta = float(record["overlay_base_blend_delta"])
            base_low = float(record["overlay_base_quantile_low_delta"])
            base_high = float(record["overlay_base_quantile_high_delta"])
            base_width_pts = _pct(abs(base_high - base_low))
            if (
                str(record.get("event_category")) != "esports"
                and method in {"magnitude_scale", "direction_conditioned_magnitude"}
                and scale > 1.0
                and 0 < base_width_pts <= OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS
            ):
                scale = min(scale, OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS / base_width_pts)
            if (
                str(record.get("event_category")) != "esports"
                and method == "slope_intercept"
                and slope > 1.0
                and 0 < base_width_pts <= OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS
            ):
                slope = min(slope, OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS / base_width_pts)
            effective_payload = {
                **payload,
                "scale": scale,
                "bias": bias,
                "slope": slope,
                "intercept": intercept,
                "direction_sign": direction_sign,
            }
            calibrated_delta = _transform_delta(base_delta, effective_payload)
            calibrated_low = _transform_delta(base_low, effective_payload)
            calibrated_high = _transform_delta(base_high, effective_payload)
            record["overlay_magnitude_scale"] = scale
            record["overlay_trend_fit_method"] = method
            record["selected_calibration_source"] = str(payload["source"])
            record["selected_calibration_method"] = method
            record["selected_calibration_bias"] = bias
            record["selected_calibration_slope"] = slope
            record["selected_calibration_intercept"] = intercept
            record["selected_calibration_train_direction_match_pct"] = float(
                payload.get("train_direction_match_pct") or 0.0
            )
            record["selected_calibration_base_train_direction_match_pct"] = float(
                payload.get("base_train_direction_match_pct") or 0.0
            )
            record["selected_calibration_train_mae_delta_pts"] = float(payload.get("train_mae_delta_pts") or 0.0)
            record["selected_calibration_train_direction_delta_pts"] = float(
                payload.get("train_direction_delta_pts") or 0.0
            )
            record["trend_fit_candidate_outcomes"] = list(payload.get("candidate_outcomes") or [])
            record["trend_fit_skipped_sources"] = list(payload.get("skipped_sources") or [])
            record["overlay_magnitude_scale_source"] = str(payload["source"])
            record["overlay_magnitude_scale_train_rows"] = int(payload["train_rows"])
            record["overlay_magnitude_scale_train_mae_pts"] = float(payload["train_mae_pts"])
            record["overlay_magnitude_scale_base_train_mae_pts"] = float(payload["base_train_mae_pts"])
            record["overlay_blend_delta"] = calibrated_delta
            record["overlay_blend_direction"] = _direction(float(record["overlay_blend_delta"]), threshold)
            record["overlay_blend_abs_error"] = abs(float(record["actual_delta"]) - float(record["overlay_blend_delta"]))
            record["overlay_quantile_low_delta"] = min(calibrated_low, calibrated_high)
            record["overlay_quantile_high_delta"] = max(calibrated_low, calibrated_high)
            record["overlay_interval_contains_actual"] = (
                float(record["overlay_quantile_low_delta"])
                <= float(record["actual_delta"])
                <= float(record["overlay_quantile_high_delta"])
            )
            changed = (
                method != "identity"
                and (
                    abs(scale - 1.0) > 0.0001
                    or abs(bias) > 0.0001
                    or abs(slope - 1.0) > 0.0001
                    or abs(intercept) > 0.0001
                )
            )
            if changed:
                if str(record.get("event_category")) == "esports" and not has_direction_support:
                    record["magnitude_fit_tier"] = "review_only"
                    record["magnitude_fit_reason"] = f"esports_review_{method}_{payload['source']}"
                else:
                    record["magnitude_fit_tier"] = "applied"
                    record["magnitude_fit_reason"] = f"{method}_{payload['source']}"
            elif payload["source"] == "insufficient_prior_overlay_history":
                record["magnitude_fit_tier"] = "identity"
                record["magnitude_fit_reason"] = "insufficient_prior_overlay_history"
            else:
                record["magnitude_fit_tier"] = "blocked"
                record["magnitude_fit_reason"] = f"{method}_{payload['source']}"


def _reset_overlay_magnitude(record: dict[str, Any], reason: str, *, threshold: float) -> None:
    """Revert a row to the pre-calibration overlay when a segment guard fails."""
    record["overlay_blend_delta"] = float(record["overlay_base_blend_delta"])
    record["overlay_blend_direction"] = _direction(float(record["overlay_blend_delta"]), threshold)
    record["overlay_blend_abs_error"] = abs(float(record["actual_delta"]) - float(record["overlay_blend_delta"]))
    record["overlay_quantile_low_delta"] = float(record["overlay_base_quantile_low_delta"])
    record["overlay_quantile_high_delta"] = float(record["overlay_base_quantile_high_delta"])
    record["overlay_interval_contains_actual"] = (
        float(record["overlay_quantile_low_delta"])
        <= float(record["actual_delta"])
        <= float(record["overlay_quantile_high_delta"])
    )
    record["overlay_magnitude_scale"] = 1.0
    record["overlay_magnitude_scale_source"] = "regression_guard_reverted"
    record["overlay_magnitude_scale_train_rows"] = 0
    record["overlay_magnitude_scale_train_mae_pts"] = 0.0
    record["overlay_magnitude_scale_base_train_mae_pts"] = 0.0
    record["overlay_trend_fit_method"] = "identity"
    record["selected_calibration_source"] = "regression_guard_reverted"
    record["selected_calibration_method"] = "identity"
    record["selected_calibration_bias"] = 0.0
    record["selected_calibration_slope"] = 1.0
    record["selected_calibration_intercept"] = 0.0
    record["selected_calibration_train_direction_match_pct"] = 0.0
    record["selected_calibration_base_train_direction_match_pct"] = 0.0
    record["selected_calibration_train_mae_delta_pts"] = 0.0
    record["selected_calibration_train_direction_delta_pts"] = 0.0
    record["magnitude_fit_tier"] = "blocked"
    record["magnitude_fit_reason"] = reason


def _has_overlay_calibration_change(record: dict[str, Any]) -> bool:
    """Return whether a record changed from the base overlay by any calibration method."""
    method = str(record.get("selected_calibration_method") or record.get("overlay_trend_fit_method") or "identity")
    return (
        method != "identity"
        or abs(float(record.get("overlay_magnitude_scale") or 1.0) - 1.0) > 0.0001
        or abs(float(record.get("selected_calibration_bias") or 0.0)) > 0.0001
        or abs(float(record.get("selected_calibration_slope") or 1.0) - 1.0) > 0.0001
        or abs(float(record.get("selected_calibration_intercept") or 0.0)) > 0.0001
    )


def _apply_focused_fit_regression_guard(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Revert focused-category scales that damage protected fit or gates."""
    guard_actions: list[dict[str, Any]] = []
    segments = _segment_summaries(
        records,
        threshold=threshold,
        group_key="focused_fit_category",
        ordered_categories=FOCUSED_FIT_CATEGORY_ORDER,
    )
    for segment in segments:
        category = str(segment["category"])
        base = segment["overlay_base_blend"]
        after = segment["overlay_blend"]
        base_gate = segment["overlay_base_gate"]
        after_gate = segment["overlay_gate"]
        reasons: list[str] = []

        if category in PROTECTED_FIT_CATEGORIES:
            if bool(base_gate["allowed"]) and not bool(after_gate["allowed"]):
                reasons.append("gate_regression")
            if float(after["direction_match_pct"]) < float(base["direction_match_pct"]):
                reasons.append("direction_regression")
            if float(after["mae_pts"]) > float(base["mae_pts"]) + 0.0001:
                reasons.append("gated_mae_regression")
        elif category in REVIEW_ONLY_FIT_CATEGORIES:
            if float(after["mae_pts"]) > float(base["mae_pts"]) + 0.0001:
                reasons.append("review_mae_regression")

        if not reasons:
            continue
        affected = [
            record
            for record in records
            if str(record.get("focused_fit_category")) == category
            and _has_overlay_calibration_change(record)
        ]
        for record in affected:
            _reset_overlay_magnitude(record, ";".join(reasons), threshold=threshold)
        guard_actions.append(
            {
                "category": category,
                "reasons": reasons,
                "reverted_row_count": len(affected),
                "base_mae_pts": base["mae_pts"],
                "after_mae_pts": after["mae_pts"],
                "base_gate_allowed": bool(base_gate["allowed"]),
                "after_gate_allowed": bool(after_gate["allowed"]),
            }
        )

    return {
        "action_count": len(guard_actions),
        "actions": guard_actions,
    }


def _direction_confirmed_scale_cap(record: dict[str, Any]) -> float:
    """Return the category cap for the experimental magnitude-close overlay."""
    category = str(record.get("focused_fit_category") or record.get("focus_category") or "other")
    return DIRECTION_CONFIRMED_CATEGORY_SCALE_CAPS.get(category, DIRECTION_CONFIRMED_DEFAULT_SCALE_CAP)


def _direction_confirmed_scale_floor(record: dict[str, Any]) -> float:
    """Return the category floor for the experimental magnitude-close overlay."""
    category = str(record.get("focused_fit_category") or record.get("focus_category") or "other")
    return DIRECTION_CONFIRMED_CATEGORY_SCALE_FLOORS.get(category, DIRECTION_CONFIRMED_DEFAULT_SCALE_FLOOR)


def _direction_confirmed_training_records(records: list[dict[str, Any]], *, threshold: float) -> list[dict[str, Any]]:
    """Return prior rows with confirmed direction where magnitude can be learned."""
    eligible: list[dict[str, Any]] = []
    for record in records:
        if not _has_direction_confirmed_magnitude_support(record):
            continue
        actual_direction = _direction(float(record["actual_delta"]), threshold)
        confirmed_direction = _direction_confirmed_base_direction(record)
        if actual_direction == confirmed_direction and confirmed_direction in {"up", "down"}:
            eligible.append(record)
    return eligible


def _fit_direction_confirmed_scale(
    records: list[dict[str, Any]],
    *,
    scale_floor: float,
    scale_cap: float,
    threshold: float,
) -> dict[str, Any] | None:
    """Fit from correct-direction rows, but guard against all supported prior rows."""
    ratios: list[float] = []
    for record in records:
        actual_direction = _direction(float(record["actual_delta"]), threshold)
        confirmed_direction = _direction_confirmed_base_direction(record)
        if actual_direction != confirmed_direction or confirmed_direction not in {"up", "down"}:
            continue
        prediction = _direction_confirmed_base_delta(record)
        if abs(prediction) < 0.0025:
            continue
        actual = float(record["actual_delta"])
        ratios.append(abs(actual) / max(abs(prediction), 0.0025))
    if len(ratios) < 3:
        return None

    actuals = [float(record["actual_delta"]) for record in records]
    predictions = [_direction_confirmed_base_delta(record) for record in records]
    raw_scale = statistics.median(ratios)
    if raw_scale >= 1.0:
        scale = 1.0 + ((raw_scale - 1.0) * DIRECTION_CONFIRMED_SCALE_SHRINKAGE)
    else:
        scale = 1.0 - ((1.0 - raw_scale) * DIRECTION_CONFIRMED_SCALE_DOWNSIDE_SHRINKAGE)
    scale = max(scale_floor, min(scale_cap, scale))

    base_mae = _mae(actuals, predictions)
    scaled_predictions = [prediction * scale for prediction in predictions]
    scaled_mae = _mae(actuals, scaled_predictions)
    if scaled_mae > base_mae + 0.000001:
        return None
    metrics = _prediction_metrics_from_values(actuals, scaled_predictions, threshold=threshold)
    base_metrics = _prediction_metrics_from_values(actuals, predictions, threshold=threshold)
    return {
        "scale": scale,
        "raw_scale": raw_scale,
        "train_rows": len(predictions),
        "train_mae_pts": metrics["mae_pts"],
        "base_train_mae_pts": base_metrics["mae_pts"],
        "train_mae_delta_pts": _round(float(metrics["mae_pts"]) - float(base_metrics["mae_pts"]), 4),
        "train_underprediction_pct": metrics["underprediction_pct"],
        "base_train_underprediction_pct": base_metrics["underprediction_pct"],
    }


def _select_direction_confirmed_scale(
    *,
    calibration_records: list[dict[str, Any]],
    record: dict[str, Any],
    threshold: float,
) -> dict[str, Any]:
    """Select a prior-fold, category-aware magnitude scale for the experimental overlay."""
    if not _has_direction_confirmed_magnitude_support(record):
        return {
            "allowed": False,
            "scale": 1.0,
            "source": "no_direction_confirmed_support",
            "support_source": _direction_confirmed_support_source(record),
            "train_rows": 0,
            "train_mae_pts": 0.0,
            "base_train_mae_pts": 0.0,
            "train_mae_delta_pts": 0.0,
            "raw_scale": 1.0,
        }

    eligible_records = [
        item
        for item in calibration_records
        if _has_direction_confirmed_magnitude_support(item)
        and _direction_confirmed_base_direction(item) in {"up", "down"}
    ]
    direction = _direction_confirmed_base_direction(record)
    focused_fit_category = str(record.get("focused_fit_category") or record.get("focus_category") or "other")
    event_category = str(record.get("event_category") or "uncategorized")
    market_family = str(record.get("market_family") or "unknown")
    calibration_segment = str(record.get("trend_calibration_segment") or market_family)
    scale_cap = _direction_confirmed_scale_cap(record)
    scale_floor = _direction_confirmed_scale_floor(record)
    candidate_specs = [
        (
            "calibration_segment_direction",
            DIRECTION_CONFIRMED_MIN_SEGMENT_ROWS,
            lambda item: str(item.get("trend_calibration_segment")) == calibration_segment
            and _direction_confirmed_base_direction(item) == direction,
        ),
        (
            "market_family_in_focused_category_direction",
            DIRECTION_CONFIRMED_MIN_SEGMENT_ROWS,
            lambda item: str(item.get("focused_fit_category")) == focused_fit_category
            and str(item.get("market_family")) == market_family
            and _direction_confirmed_base_direction(item) == direction,
        ),
        (
            "event_category_direction",
            DIRECTION_CONFIRMED_MIN_CATEGORY_ROWS,
            lambda item: str(item.get("event_category")) == event_category
            and _direction_confirmed_base_direction(item) == direction,
        ),
        (
            "focused_fit_category_direction",
            DIRECTION_CONFIRMED_MIN_CATEGORY_ROWS,
            lambda item: str(item.get("focused_fit_category")) == focused_fit_category
            and _direction_confirmed_base_direction(item) == direction,
        ),
        (
            "global_direction",
            DIRECTION_CONFIRMED_MIN_GLOBAL_ROWS,
            lambda item: _direction_confirmed_base_direction(item) == direction,
        ),
    ]

    skipped_sources: list[dict[str, Any]] = []
    for source, min_rows, predicate in candidate_specs:
        source_records = [item for item in eligible_records if predicate(item)]
        if len(source_records) < min_rows:
            skipped_sources.append(
                {
                    "source": source,
                    "rows": len(source_records),
                    "min_rows": min_rows,
                    "reason": "insufficient_prior_correct_direction_rows",
                }
            )
            continue
        payload = _fit_direction_confirmed_scale(
            source_records,
            scale_floor=scale_floor,
            scale_cap=scale_cap,
            threshold=threshold,
        )
        if payload:
            return {
                **payload,
                "allowed": True,
                "source": source,
                "support_source": _direction_confirmed_support_source(record),
                "skipped_sources": skipped_sources,
                "scale_cap": scale_cap,
                "scale_floor": scale_floor,
            }
        skipped_sources.append(
            {
                "source": source,
                "rows": len(source_records),
                "min_rows": min_rows,
                "reason": "train_mae_guard_rejected",
            }
        )

    return {
        "allowed": False,
        "scale": 1.0,
        "source": "insufficient_prior_direction_confirmed_history",
        "support_source": _direction_confirmed_support_source(record),
        "train_rows": 0,
        "train_mae_pts": 0.0,
        "base_train_mae_pts": 0.0,
        "train_mae_delta_pts": 0.0,
        "raw_scale": 1.0,
        "skipped_sources": skipped_sources,
        "scale_cap": scale_cap,
        "scale_floor": scale_floor,
    }


def _apply_direction_confirmed_magnitude_overlay(records: list[dict[str, Any]], *, threshold: float) -> None:
    """Attach an experimental magnitude-close overlay without replacing the gated overlay."""
    fold_indexes = sorted({int(record.get("fold_index") or 0) for record in records})
    for fold_index in fold_indexes:
        calibration_records = [record for record in records if int(record.get("fold_index") or 0) < fold_index]
        fold_records = [record for record in records if int(record.get("fold_index") or 0) == fold_index]
        for record in fold_records:
            payload = _select_direction_confirmed_scale(
                calibration_records=calibration_records,
                record=record,
                threshold=threshold,
            )
            base_delta = _direction_confirmed_base_delta(record)
            scale = float(payload.get("scale") or 1.0)
            calibrated_delta = base_delta * scale
            record["direction_confirmed_overlay_delta"] = calibrated_delta
            record["direction_confirmed_overlay_direction"] = _direction(calibrated_delta, threshold)
            record["direction_confirmed_overlay_abs_error"] = abs(float(record["actual_delta"]) - calibrated_delta)
            record["direction_confirmed_overlay_scale"] = scale
            record["direction_confirmed_overlay_raw_scale"] = float(payload.get("raw_scale") or 1.0)
            record["direction_confirmed_overlay_allowed"] = bool(payload.get("allowed"))
            record["direction_confirmed_overlay_source"] = str(payload.get("source") or "")
            record["direction_confirmed_overlay_support_source"] = str(payload.get("support_source") or "")
            record["direction_confirmed_overlay_reason"] = (
                f"{payload.get('support_source')}_{payload.get('source')}"
                if bool(payload.get("allowed"))
                else str(payload.get("source") or "not_available")
            )
            record["direction_confirmed_overlay_train_rows"] = int(payload.get("train_rows") or 0)
            record["direction_confirmed_overlay_train_mae_pts"] = float(payload.get("train_mae_pts") or 0.0)
            record["direction_confirmed_overlay_base_train_mae_pts"] = float(
                payload.get("base_train_mae_pts") or 0.0
            )
            record["direction_confirmed_overlay_train_mae_delta_pts"] = float(
                payload.get("train_mae_delta_pts") or 0.0
            )
            record["direction_confirmed_overlay_skipped_sources"] = list(payload.get("skipped_sources") or [])


def _direction_confirmed_magnitude_summary(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return aggregate metrics for the experimental direction-confirmed overlay."""
    if not records:
        return {}
    eligible = [record for record in records if _has_direction_confirmed_magnitude_support(record)]
    applied = [record for record in records if bool(record.get("direction_confirmed_overlay_allowed"))]
    overlay_metrics = _prediction_metrics(records, "overlay_blend_delta", threshold=threshold)
    confirmed_metrics = _prediction_metrics(records, "direction_confirmed_overlay_delta", threshold=threshold)
    eligible_overlay = _prediction_metrics(eligible, "overlay_blend_delta", threshold=threshold) if eligible else {}
    eligible_confirmed = (
        _prediction_metrics(eligible, "direction_confirmed_overlay_delta", threshold=threshold) if eligible else {}
    )
    applied_overlay = _prediction_metrics(applied, "overlay_blend_delta", threshold=threshold) if applied else {}
    applied_confirmed = (
        _prediction_metrics(applied, "direction_confirmed_overlay_delta", threshold=threshold) if applied else {}
    )
    return {
        "policy": {
            "objective": "closer_visible_trend_magnitude_on_direction_confirmed_rows",
            "uses_prior_out_of_sample_folds_only": True,
            "training_rows_require_correct_direction": True,
            "support_sources": ["strong_watch_direction", "whale_pressure_aligned"],
            "scale_shrinkage": DIRECTION_CONFIRMED_SCALE_SHRINKAGE,
            "scale_downside_shrinkage": DIRECTION_CONFIRMED_SCALE_DOWNSIDE_SHRINKAGE,
            "category_scale_caps": DIRECTION_CONFIRMED_CATEGORY_SCALE_CAPS,
            "category_scale_floors": DIRECTION_CONFIRMED_CATEGORY_SCALE_FLOORS,
        },
        "eligible_row_count": len(eligible),
        "eligible_row_pct": _safe_ratio(len(eligible), len(records)),
        "applied_row_count": len(applied),
        "applied_row_pct": _safe_ratio(len(applied), len(records)),
        "support_source_counts": dict(
            sorted(Counter(str(record.get("direction_confirmed_overlay_support_source") or "none") for record in records).items())
        ),
        "source_counts": dict(
            sorted(Counter(str(record.get("direction_confirmed_overlay_source") or "none") for record in records).items())
        ),
        "average_scale": _round(
            sum(float(record.get("direction_confirmed_overlay_scale") or 1.0) for record in records) / len(records),
            4,
        ),
        "max_scale": _round(
            max((float(record.get("direction_confirmed_overlay_scale") or 1.0) for record in records), default=1.0),
            4,
        ),
        "all_rows": {
            "overlay": overlay_metrics,
            "direction_confirmed": confirmed_metrics,
            "mae_delta_pts": _round(float(confirmed_metrics["mae_pts"]) - float(overlay_metrics["mae_pts"]), 4),
            "average_abs_predicted_delta_delta_pts": _round(
                float(confirmed_metrics["average_abs_predicted_delta_pts"])
                - float(overlay_metrics["average_abs_predicted_delta_pts"]),
                4,
            ),
            "underprediction_delta_pts": _round(
                float(confirmed_metrics["underprediction_pct"]) - float(overlay_metrics["underprediction_pct"]),
                4,
            ),
        },
        "eligible_rows": {
            "overlay": eligible_overlay,
            "direction_confirmed": eligible_confirmed,
            "mae_delta_pts": _round(
                float((eligible_confirmed or {}).get("mae_pts") or 0.0)
                - float((eligible_overlay or {}).get("mae_pts") or 0.0),
                4,
            ),
        },
        "applied_rows": {
            "overlay": applied_overlay,
            "direction_confirmed": applied_confirmed,
            "mae_delta_pts": _round(
                float((applied_confirmed or {}).get("mae_pts") or 0.0)
                - float((applied_overlay or {}).get("mae_pts") or 0.0),
                4,
            ),
        },
    }


def _direction_confirmed_display_gate(
    *,
    overlay: dict[str, Any],
    confirmed: dict[str, Any],
    has_support: bool,
) -> dict[str, Any]:
    """Return whether the experimental magnitude-close line should be displayed."""
    reasons: list[str] = []
    if not has_support:
        reasons.append("no_direction_confirmed_support")
    if float(confirmed["mae_pts"]) > float(overlay["mae_pts"]) + 0.0001:
        reasons.append("category_window_mae_regression")
    if float(confirmed["direction_match_pct"]) + 0.0001 < float(overlay["direction_match_pct"]):
        reasons.append("category_window_direction_regression")
    return {
        "allowed": not reasons,
        "reasons": reasons,
        "mae_delta_pts": _round(float(confirmed["mae_pts"]) - float(overlay["mae_pts"]), 4),
        "direction_match_delta_pts": _round(
            float(confirmed["direction_match_pct"]) - float(overlay["direction_match_pct"]),
            4,
        ),
    }


def _apply_direction_confirmed_display_gates(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Attach category/window display gates for the experimental magnitude-close line."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get("focused_fit_category") or "unknown"), []).append(record)

    gate_rows: list[dict[str, Any]] = []
    for category, segment_records in sorted(grouped.items()):
        overlay = _prediction_metrics(segment_records, "overlay_blend_delta", threshold=threshold)
        confirmed = _prediction_metrics(segment_records, "direction_confirmed_overlay_delta", threshold=threshold)
        has_support = any(_has_direction_confirmed_magnitude_support(record) for record in segment_records)
        gate = _direction_confirmed_display_gate(overlay=overlay, confirmed=confirmed, has_support=has_support)
        for record in segment_records:
            record["direction_confirmed_display_allowed"] = bool(gate["allowed"])
            record["direction_confirmed_display_reasons"] = list(gate["reasons"])
            record["direction_confirmed_display_gate_category"] = category
            record["direction_confirmed_display_mae_delta_pts"] = float(gate["mae_delta_pts"])
            record["direction_confirmed_display_direction_delta_pts"] = float(gate["direction_match_delta_pts"])
        gate_rows.append(
            {
                "category": category,
                "row_count": len(segment_records),
                "allowed": bool(gate["allowed"]),
                "reasons": list(gate["reasons"]),
                "mae_delta_pts": gate["mae_delta_pts"],
                "direction_match_delta_pts": gate["direction_match_delta_pts"],
            }
        )
    return {
        "allowed_category_count": sum(1 for row in gate_rows if bool(row["allowed"])),
        "suppressed_category_count": sum(1 for row in gate_rows if not bool(row["allowed"])),
        "categories": gate_rows,
        "policy": {
            "requires_mae_neutral_or_better": True,
            "requires_direction_match_neutral_or_better": True,
            "scope": "focused_category_by_window",
        },
    }


def _direction_confirmed_fit_summary(
    records: list[dict[str, Any]],
    *,
    threshold: float,
) -> list[dict[str, Any]]:
    """Return focused-category metrics for the experimental magnitude-close overlay."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get("focused_fit_category") or "unknown"), []).append(record)
    rows: list[dict[str, Any]] = []
    ordered = list(FOCUSED_FIT_CATEGORY_ORDER)
    ordered.extend(
        category
        for category, _ in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
        if category not in ordered
    )
    for category in ordered:
        segment_records = grouped.get(category, [])
        if not segment_records:
            continue
        overlay = _prediction_metrics(segment_records, "overlay_blend_delta", threshold=threshold)
        confirmed = _prediction_metrics(segment_records, "direction_confirmed_overlay_delta", threshold=threshold)
        applied = [record for record in segment_records if bool(record.get("direction_confirmed_overlay_allowed"))]
        display_allowed = any(bool(record.get("direction_confirmed_display_allowed")) for record in segment_records)
        display_reasons = sorted(
            {
                str(reason)
                for record in segment_records
                for reason in list(record.get("direction_confirmed_display_reasons") or [])
            }
        )
        rows.append(
            {
                "category": category,
                "row_count": len(segment_records),
                "applied_row_count": len(applied),
                "applied_row_pct": _safe_ratio(len(applied), len(segment_records)),
                "overlay_mae_pts": overlay["mae_pts"],
                "direction_confirmed_mae_pts": confirmed["mae_pts"],
                "mae_delta_pts": _round(float(confirmed["mae_pts"]) - float(overlay["mae_pts"]), 4),
                "overlay_average_abs_predicted_delta_pts": overlay["average_abs_predicted_delta_pts"],
                "direction_confirmed_average_abs_predicted_delta_pts": confirmed["average_abs_predicted_delta_pts"],
                "average_abs_actual_delta_pts": confirmed["average_abs_actual_delta_pts"],
                "overlay_underprediction_pct": overlay["underprediction_pct"],
                "direction_confirmed_underprediction_pct": confirmed["underprediction_pct"],
                "direction_match_pct": confirmed["direction_match_pct"],
                "overlay_direction_match_pct": overlay["direction_match_pct"],
                "display_allowed": display_allowed,
                "display_reasons": display_reasons,
                "support_source_counts": dict(
                    sorted(
                        Counter(
                            str(record.get("direction_confirmed_overlay_support_source") or "none")
                            for record in segment_records
                        ).items()
                    )
                ),
            }
        )
    return rows


def _absolute_move_direction_payload(
    *,
    focused_fit_category: str,
    signal_tier: str,
    signal_direction: str,
    whale_pressure_direction: str,
    blend_direction: str,
) -> dict[str, str]:
    """Return the signed direction used by the absolute-move split model."""
    if signal_tier in SURFACED_DIRECTION_TIERS and signal_direction in {"up", "down"}:
        return {
            "direction": signal_direction,
            "source": f"{signal_tier}_direction_signal",
        }
    if focused_fit_category == "crypto" and blend_direction in {"up", "down"}:
        return {
            "direction": blend_direction,
            "source": "crypto_overlay_direction_fallback",
        }
    if whale_pressure_direction in {"up", "down"}:
        return {
            "direction": whale_pressure_direction,
            "source": "whale_pressure",
        }
    if blend_direction in {"up", "down"}:
        return {
            "direction": blend_direction,
            "source": "overlay_direction_fallback",
        }
    return {
        "direction": "flat",
        "source": "no_movement_direction",
    }


def _bounded_absolute_move_delta(
    *,
    current_odds: float,
    absolute_prediction: float,
    direction: str,
) -> float:
    """Return a signed absolute-move prediction bounded by the available odds range."""
    magnitude = max(0.0, float(absolute_prediction))
    sign = _direction_sign(direction)
    if sign > 0:
        return min(magnitude, max(0.0, 1.0 - current_odds))
    if sign < 0:
        return -min(magnitude, max(0.0, current_odds))
    return 0.0


def _absolute_move_has_direction(record: dict[str, Any]) -> bool:
    """Return whether the absolute-move split had a non-flat direction source."""
    return str(record.get("absolute_move_direction") or "flat") in {"up", "down"}


def _absolute_move_candidate_direction(record: dict[str, Any], source: str) -> str:
    """Return a candidate direction for the absolute-move split."""
    if source == "direction_signal":
        if (
            str(record.get("direction_signal_tier") or "") in SURFACED_DIRECTION_TIERS
            and str(record.get("direction_signal_predicted_direction") or "flat") in {"up", "down"}
        ):
            return str(record.get("direction_signal_predicted_direction"))
        return "flat"
    if source == "overlay_blend":
        direction = str(record.get("overlay_blend_direction") or "flat")
        return direction if direction in {"up", "down"} else "flat"
    if source == "whale_pressure":
        direction = str(record.get("whale_pressure_direction") or "neutral")
        return direction if direction in {"up", "down"} else "flat"
    if source == "blend":
        direction = str(record.get("blend_direction") or "flat")
        return direction if direction in {"up", "down"} else "flat"
    return "flat"


def _absolute_move_delta_for_source(record: dict[str, Any], source: str) -> float | None:
    """Return the signed absolute-move delta for one candidate source."""
    direction = _absolute_move_candidate_direction(record, source)
    if direction not in {"up", "down"}:
        return None
    return _bounded_absolute_move_delta(
        current_odds=float(record["current_odds"]),
        absolute_prediction=float(record["absolute_move_abs_prediction"]),
        direction=direction,
    )


def _absolute_move_source_backtest(
    records: list[dict[str, Any]],
    *,
    source: str,
    threshold: float,
) -> dict[str, Any] | None:
    """Return prior-fold fit for a candidate absolute-move direction source."""
    actuals: list[float] = []
    predictions: list[float] = []
    base_predictions: list[float] = []
    for record in records:
        prediction = _absolute_move_delta_for_source(record, source)
        if prediction is None:
            continue
        base_prediction = float(record.get("absolute_move_base_delta", record.get("absolute_move_delta", 0.0)))
        if _direction(prediction, threshold) == _direction(base_prediction, threshold):
            continue
        actuals.append(float(record["actual_delta"]))
        predictions.append(prediction)
        base_predictions.append(base_prediction)
    if len(actuals) < CRYPTO_ABSOLUTE_DIRECTION_MIN_PRIOR_ROWS:
        return None
    metrics = _prediction_metrics_from_values(actuals, predictions, threshold=threshold)
    base_metrics = _prediction_metrics_from_values(actuals, base_predictions, threshold=threshold)
    return {
        "source": source,
        "train_rows": len(actuals),
        "direction_match_pct": metrics["direction_match_pct"],
        "base_direction_match_pct": base_metrics["direction_match_pct"],
        "direction_delta_pts": _round(
            float(metrics["direction_match_pct"]) - float(base_metrics["direction_match_pct"]),
            4,
        ),
        "mae_pts": metrics["mae_pts"],
        "base_mae_pts": base_metrics["mae_pts"],
        "mae_delta_pts": _round(float(metrics["mae_pts"]) - float(base_metrics["mae_pts"]), 4),
        "underprediction_pct": metrics["underprediction_pct"],
        "base_underprediction_pct": base_metrics["underprediction_pct"],
    }


def _crypto_absolute_direction_prior_scopes(
    record: dict[str, Any],
    prior_records: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Return prior-fold scopes for crypto absolute-move source selection."""
    crypto_records = [item for item in prior_records if str(item.get("focused_fit_category")) == "crypto"]
    scopes: list[tuple[str, list[dict[str, Any]]]] = []
    asset = str(record.get("crypto_asset") or "")
    if asset and asset != "crypto_other":
        scopes.append(
            (
                f"crypto_asset:{asset}",
                [item for item in crypto_records if str(item.get("crypto_asset") or "") == asset],
            )
        )
    calibration_segment = str(record.get("trend_calibration_segment") or "")
    if calibration_segment:
        scopes.append(
            (
                f"calibration_segment:{calibration_segment}",
                [
                    item
                    for item in crypto_records
                    if str(item.get("trend_calibration_segment") or "") == calibration_segment
                ],
            )
        )
    time_bucket = str(record.get("time_to_close_bucket") or "")
    if time_bucket:
        scopes.append(
            (
                f"time_to_close:{time_bucket}",
                [item for item in crypto_records if str(item.get("time_to_close_bucket") or "") == time_bucket],
            )
        )
    scopes.append(("crypto_global", crypto_records))
    return scopes


def _select_crypto_absolute_direction_source(
    record: dict[str, Any],
    *,
    prior_records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Select a crypto absolute-move direction source from prior folds only."""
    skipped: list[dict[str, Any]] = []
    for scope_name, scoped_records in _crypto_absolute_direction_prior_scopes(record, prior_records):
        candidates: list[dict[str, Any]] = []
        for source in CRYPTO_ABSOLUTE_DIRECTION_SOURCES:
            if _absolute_move_delta_for_source(record, source) is None:
                continue
            payload = _absolute_move_source_backtest(scoped_records, source=source, threshold=threshold)
            if payload is None:
                skipped.append(
                    {
                        "scope": scope_name,
                        "source": source,
                        "reason": "insufficient_prior_rows",
                        "available_prior_rows": len(scoped_records),
                    }
                )
                continue
            payload["scope"] = scope_name
            if float(payload["direction_delta_pts"]) < -0.0001:
                payload["blocked_reason"] = "prior_direction_regression"
                skipped.append(payload)
                continue
            if float(payload["mae_delta_pts"]) > 0.0001:
                payload["blocked_reason"] = "prior_mae_regression"
                skipped.append(payload)
                continue
            candidates.append(payload)
        if candidates:
            selected = max(
                candidates,
                key=lambda item: (
                    float(item["direction_delta_pts"]),
                    -float(item["mae_delta_pts"]),
                    int(item["train_rows"]),
                    -CRYPTO_ABSOLUTE_DIRECTION_SOURCES.index(str(item["source"])),
                ),
            )
            selected["selected"] = True
            selected["skipped_candidates"] = skipped[-8:]
            return selected
    return {
        "source": "base",
        "scope": "none",
        "selected": False,
        "reason": "no_prior_safe_source",
        "train_rows": 0,
        "direction_match_pct": 0.0,
        "base_direction_match_pct": 0.0,
        "direction_delta_pts": 0.0,
        "mae_pts": 0.0,
        "base_mae_pts": 0.0,
        "mae_delta_pts": 0.0,
        "skipped_candidates": skipped[-8:],
    }


def _apply_crypto_absolute_move_direction_resolver(
    records: list[dict[str, Any]],
    *,
    threshold: float,
) -> dict[str, Any]:
    """Resolve crypto absolute-move direction using prior out-of-sample folds."""
    crypto_records = [record for record in records if str(record.get("focused_fit_category")) == "crypto"]
    before_metrics = (
        _prediction_metrics(crypto_records, "absolute_move_base_delta", threshold=threshold) if crypto_records else {}
    )
    before_overlay = _prediction_metrics(crypto_records, "overlay_blend_delta", threshold=threshold) if crypto_records else {}
    for record in records:
        record["absolute_move_direction_resolver_applied"] = False
        record["absolute_move_direction_resolver_source"] = "not_crypto"
        record["absolute_move_direction_resolver_scope"] = ""
        record["absolute_move_direction_resolver_reason"] = ""
        record["absolute_move_direction_resolver_train_rows"] = 0
        record["absolute_move_direction_resolver_direction_delta_pts"] = 0.0
        record["absolute_move_direction_resolver_mae_delta_pts"] = 0.0

    for record in sorted(crypto_records, key=lambda item: (int(item.get("fold_index") or 0), str(item.get("condition_ref") or ""))):
        prior_records = [
            item
            for item in crypto_records
            if int(item.get("fold_index") or 0) < int(record.get("fold_index") or 0)
        ]
        payload = _select_crypto_absolute_direction_source(
            record,
            prior_records=prior_records,
            threshold=threshold,
        )
        selected_source = str(payload.get("source") or "base")
        selected_delta = _absolute_move_delta_for_source(record, selected_source)
        base_delta = float(record.get("absolute_move_base_delta", record["absolute_move_delta"]))
        if (
            selected_source != "base"
            and selected_delta is not None
            and _direction(selected_delta, threshold) != _direction(base_delta, threshold)
        ):
            selected_direction = _absolute_move_candidate_direction(record, selected_source)
            record["absolute_move_delta"] = selected_delta
            record["absolute_move_direction"] = _direction(selected_delta, threshold)
            record["absolute_move_direction_source"] = f"crypto_resolver_{selected_source}"
            record["absolute_move_abs_error"] = abs(float(record["actual_delta"]) - selected_delta)
            record["absolute_move_direction_resolver_applied"] = True
            record["absolute_move_direction_resolver_source"] = selected_source
            record["absolute_move_direction_resolver_reason"] = f"prior_safe_{selected_source}_{selected_direction}"
        else:
            record["absolute_move_delta"] = float(record.get("absolute_move_base_delta", record["absolute_move_delta"]))
            record["absolute_move_direction"] = str(record.get("absolute_move_base_direction") or record["absolute_move_direction"])
            record["absolute_move_direction_source"] = str(
                record.get("absolute_move_base_direction_source") or record["absolute_move_direction_source"]
            )
            record["absolute_move_abs_error"] = float(record.get("absolute_move_base_abs_error", record["absolute_move_abs_error"]))
            record["absolute_move_direction_resolver_source"] = "base"
            record["absolute_move_direction_resolver_reason"] = str(payload.get("reason") or "base_kept")

        record["absolute_move_direction_resolver_scope"] = str(payload.get("scope") or "")
        record["absolute_move_direction_resolver_train_rows"] = int(payload.get("train_rows") or 0)
        record["absolute_move_direction_resolver_direction_delta_pts"] = float(payload.get("direction_delta_pts") or 0.0)
        record["absolute_move_direction_resolver_mae_delta_pts"] = float(payload.get("mae_delta_pts") or 0.0)

    after_metrics = _prediction_metrics(crypto_records, "absolute_move_delta", threshold=threshold) if crypto_records else {}
    applied_records = [record for record in crypto_records if bool(record.get("absolute_move_direction_resolver_applied"))]
    return {
        "row_count": len(crypto_records),
        "applied_row_count": len(applied_records),
        "applied_row_pct": _safe_ratio(len(applied_records), len(crypto_records)),
        "before": before_metrics,
        "after": after_metrics,
        "overlay": before_overlay,
        "direction_delta_vs_base_pts": _round(
            float(after_metrics.get("direction_match_pct") or 0.0) - float(before_metrics.get("direction_match_pct") or 0.0),
            4,
        ),
        "direction_delta_vs_overlay_pts": _round(
            float(after_metrics.get("direction_match_pct") or 0.0) - float(before_overlay.get("direction_match_pct") or 0.0),
            4,
        ),
        "mae_delta_vs_base_pts": _round(
            float(after_metrics.get("mae_pts") or 0.0) - float(before_metrics.get("mae_pts") or 0.0),
            4,
        ),
        "mae_delta_vs_overlay_pts": _round(
            float(after_metrics.get("mae_pts") or 0.0) - float(before_overlay.get("mae_pts") or 0.0),
            4,
        ),
        "source_counts": dict(
            sorted(Counter(str(record.get("absolute_move_direction_resolver_source") or "unknown") for record in crypto_records).items())
        ),
        "scope_counts": dict(
            sorted(Counter(str(record.get("absolute_move_direction_resolver_scope") or "unknown") for record in crypto_records).items())
        ),
        "policy": {
            "scope_order": [
                "crypto_asset",
                "calibration_segment",
                "time_to_close",
                "crypto_global",
            ],
            "candidate_sources": list(CRYPTO_ABSOLUTE_DIRECTION_SOURCES),
            "min_prior_rows": CRYPTO_ABSOLUTE_DIRECTION_MIN_PRIOR_ROWS,
            "requires_prior_direction_neutral_or_better": True,
            "requires_prior_mae_neutral_or_better": True,
            "uses_prior_out_of_sample_folds_only": True,
        },
    }


def _absolute_move_display_gate(
    *,
    category: str,
    overlay: dict[str, Any],
    absolute: dict[str, Any],
    has_direction: bool,
) -> dict[str, Any]:
    """Return whether the absolute-move split line should be displayed."""
    reasons: list[str] = []
    mae_delta = float(absolute["mae_pts"]) - float(overlay["mae_pts"])
    direction_delta = float(absolute["direction_match_pct"]) - float(overlay["direction_match_pct"])
    if not has_direction:
        reasons.append("no_absolute_move_direction")
    if mae_delta > 0.0001:
        reasons.append("category_window_mae_regression")
    if direction_delta < -0.0001:
        reasons.append("category_window_direction_regression")
    review_allowed = (
        bool(reasons)
        and category in ABSOLUTE_MOVE_REVIEW_ONLY_CATEGORIES
        and has_direction
        and mae_delta < -0.0001
        and direction_delta >= -ABSOLUTE_MOVE_REVIEW_MAX_DIRECTION_REGRESSION_PTS
    )
    review_reasons = ["direction_not_cleared"] if review_allowed else []
    display_tier = "show" if not reasons else "review" if review_allowed else "hidden"
    return {
        "allowed": not reasons,
        "review_allowed": review_allowed,
        "display_tier": display_tier,
        "reasons": reasons,
        "review_reasons": review_reasons,
        "mae_delta_pts": _round(mae_delta, 4),
        "direction_match_delta_pts": _round(direction_delta, 4),
    }


def _apply_absolute_move_display_gates(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Attach category/window display gates for the absolute-move split line."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get("focused_fit_category") or "unknown"), []).append(record)

    gate_rows: list[dict[str, Any]] = []
    for category, segment_records in sorted(grouped.items()):
        overlay = _prediction_metrics(segment_records, "overlay_blend_delta", threshold=threshold)
        absolute = _prediction_metrics(segment_records, "absolute_move_delta", threshold=threshold)
        has_direction = any(_absolute_move_has_direction(record) for record in segment_records)
        gate = _absolute_move_display_gate(
            category=category,
            overlay=overlay,
            absolute=absolute,
            has_direction=has_direction,
        )
        for record in segment_records:
            record["absolute_move_display_allowed"] = bool(gate["allowed"])
            record["absolute_move_review_allowed"] = bool(gate["review_allowed"])
            record["absolute_move_display_tier"] = str(gate["display_tier"])
            record["absolute_move_display_reasons"] = list(gate["reasons"])
            record["absolute_move_review_reasons"] = list(gate["review_reasons"])
            record["absolute_move_display_gate_category"] = category
            record["absolute_move_display_mae_delta_pts"] = float(gate["mae_delta_pts"])
            record["absolute_move_display_direction_delta_pts"] = float(gate["direction_match_delta_pts"])
        gate_rows.append(
            {
                "category": category,
                "row_count": len(segment_records),
                "allowed": bool(gate["allowed"]),
                "review_allowed": bool(gate["review_allowed"]),
                "display_tier": str(gate["display_tier"]),
                "reasons": list(gate["reasons"]),
                "review_reasons": list(gate["review_reasons"]),
                "mae_delta_pts": gate["mae_delta_pts"],
                "direction_match_delta_pts": gate["direction_match_delta_pts"],
            }
        )
    return {
        "allowed_category_count": sum(1 for row in gate_rows if bool(row["allowed"])),
        "review_category_count": sum(1 for row in gate_rows if bool(row["review_allowed"])),
        "suppressed_category_count": sum(1 for row in gate_rows if str(row["display_tier"]) == "hidden"),
        "categories": gate_rows,
        "policy": {
            "requires_mae_neutral_or_better": True,
            "requires_direction_match_neutral_or_better": True,
            "review_only_categories": sorted(ABSOLUTE_MOVE_REVIEW_ONLY_CATEGORIES),
            "review_requires_mae_improvement": True,
            "review_max_direction_regression_pts": ABSOLUTE_MOVE_REVIEW_MAX_DIRECTION_REGRESSION_PTS,
            "direction_source_order": [
                "strong_watch_direction_signal",
                "crypto_overlay_direction_fallback",
                "whale_pressure",
                "overlay_direction_fallback",
            ],
            "scope": "focused_category_by_window",
        },
    }


def _crypto_segment_gate_scopes(
    record: dict[str, Any],
    prior_records: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Return train-safe crypto segment scopes for display promotion."""
    crypto_records = [item for item in prior_records if str(item.get("focused_fit_category")) == "crypto"]
    asset = str(record.get("crypto_asset") or "unknown")
    market_family = str(record.get("market_family") or "unknown")
    entry_bucket = str(record.get("whale_entry_timing_bucket") or "unknown")
    flow_bucket = str(record.get("whale_flow_timing_bucket") or "unknown")
    scopes = [
        (
            f"asset_market_family:{asset}:{market_family}",
            [
                item
                for item in crypto_records
                if str(item.get("crypto_asset") or "unknown") == asset
                and str(item.get("market_family") or "unknown") == market_family
            ],
        ),
        (
            f"crypto_asset:{asset}",
            [item for item in crypto_records if str(item.get("crypto_asset") or "unknown") == asset],
        ),
        (
            f"entry_timing:{entry_bucket}",
            [
                item
                for item in crypto_records
                if str(item.get("whale_entry_timing_bucket") or "unknown") == entry_bucket
            ],
        ),
        (
            f"flow_timing:{flow_bucket}",
            [
                item
                for item in crypto_records
                if str(item.get("whale_flow_timing_bucket") or "unknown") == flow_bucket
            ],
        ),
        (
            f"market_family:{market_family}",
            [item for item in crypto_records if str(item.get("market_family") or "unknown") == market_family],
        ),
        ("crypto_global", crypto_records),
    ]
    return scopes


def _crypto_segment_gate_candidate(
    *,
    scope: str,
    records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Return a prior-fold crypto segment gate candidate."""
    if len(records) < CRYPTO_SEGMENT_GATE_MIN_PRIOR_ROWS:
        return {
            "scope": scope,
            "prior_rows": len(records),
            "allowed": False,
            "blocked_reason": "insufficient_prior_rows",
        }
    overlay = _prediction_metrics(records, "overlay_blend_delta", threshold=threshold)
    absolute = _prediction_metrics(records, "absolute_move_delta", threshold=threshold)
    signal = _strong_watch_direction_alignment(records)
    mae_delta = float(absolute["mae_pts"]) - float(overlay["mae_pts"])
    direction_delta = float(absolute["direction_match_pct"]) - float(overlay["direction_match_pct"])
    blocked_reasons: list[str] = []
    if mae_delta > 0.0001:
        blocked_reasons.append("prior_segment_mae_regression")
    if direction_delta < -0.0001:
        blocked_reasons.append("prior_segment_direction_regression")
    if float(absolute["direction_match_pct"]) < CRYPTO_SEGMENT_GATE_MIN_DIRECTION_MATCH_PCT:
        blocked_reasons.append("prior_segment_direction_below_gate")
    if (
        int(signal["row_count"]) >= 5
        and float(signal["alignment_pct"]) < CRYPTO_SEGMENT_GATE_MIN_STRONG_WATCH_ALIGNMENT_PCT
    ):
        blocked_reasons.append("prior_strong_watch_alignment_below_gate")
    return {
        "scope": scope,
        "prior_rows": len(records),
        "allowed": not blocked_reasons,
        "blocked_reason": blocked_reasons[0] if blocked_reasons else "",
        "blocked_reasons": blocked_reasons,
        "overlay_direction_match_pct": overlay["direction_match_pct"],
        "absolute_move_direction_match_pct": absolute["direction_match_pct"],
        "direction_match_delta_pts": _round(direction_delta, 4),
        "overlay_mae_pts": overlay["mae_pts"],
        "absolute_move_mae_pts": absolute["mae_pts"],
        "mae_delta_pts": _round(mae_delta, 4),
        "strong_watch_row_count": signal["row_count"],
        "strong_watch_alignment_pct": signal["alignment_pct"],
    }


def _select_crypto_segment_gate_candidate(
    record: dict[str, Any],
    *,
    prior_records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Select a prior-fold crypto segment display candidate."""
    candidates = [
        _crypto_segment_gate_candidate(scope=scope, records=scope_records, threshold=threshold)
        for scope, scope_records in _crypto_segment_gate_scopes(record, prior_records)
    ]
    asset = str(record.get("crypto_asset") or "")
    entry_bucket = str(record.get("whale_entry_timing_bucket") or "")
    if asset == "btc" and entry_bucket not in CRYPTO_SEGMENT_GATE_RECENT_ENTRY_BUCKETS:
        for candidate in candidates:
            scope = str(candidate.get("scope") or "")
            if scope.startswith("asset_market_family:btc") or scope.startswith("crypto_asset:btc"):
                candidate["allowed"] = False
                candidate["blocked_reason"] = "btc_stale_entry_requires_timing_scope"
                candidate["blocked_reasons"] = [
                    *list(candidate.get("blocked_reasons") or []),
                    "btc_stale_entry_requires_timing_scope",
                ]
    passing = [candidate for candidate in candidates if bool(candidate.get("allowed"))]
    if passing:
        selected = max(
            passing,
            key=lambda item: (
                float(item.get("absolute_move_direction_match_pct") or 0.0),
                -float(item.get("mae_delta_pts") or 0.0),
                float(item.get("strong_watch_alignment_pct") or 0.0),
                int(item.get("prior_rows") or 0),
            ),
        )
        selected["candidate_count"] = len(candidates)
        return selected
    best = max(
        candidates,
        key=lambda item: (
            int(item.get("prior_rows") or 0),
            float(item.get("absolute_move_direction_match_pct") or 0.0),
            -float(item.get("mae_delta_pts") or 0.0),
        ),
        default={"scope": "none", "prior_rows": 0, "blocked_reason": "no_prior_candidates"},
    )
    best["candidate_count"] = len(candidates)
    return best


def _crypto_direction_source_candidate(
    *,
    scope: str,
    records: list[dict[str, Any]],
    source: str,
    threshold: float,
) -> dict[str, Any]:
    """Return a prior-fold candidate for selecting the crypto direction source."""
    actuals: list[float] = []
    predictions: list[float] = []
    baseline_predictions: list[float] = []
    for record in records:
        prediction = _absolute_move_delta_for_source(record, source)
        if prediction is None:
            continue
        actuals.append(float(record["actual_delta"]))
        predictions.append(prediction)
        baseline_predictions.append(float(record.get("absolute_move_delta", record.get("absolute_move_base_delta", 0.0))))
    if len(actuals) < CRYPTO_DIRECTION_SOURCE_SELECTOR_MIN_PRIOR_ROWS:
        return {
            "scope": scope,
            "source": source,
            "prior_rows": len(actuals),
            "allowed": False,
            "blocked_reason": "insufficient_prior_rows",
        }
    metrics = _prediction_metrics_from_values(actuals, predictions, threshold=threshold)
    baseline = _prediction_metrics_from_values(actuals, baseline_predictions, threshold=threshold)
    direction_delta = float(metrics["direction_match_pct"]) - float(baseline["direction_match_pct"])
    mae_delta = float(metrics["mae_pts"]) - float(baseline["mae_pts"])
    blocked_reasons: list[str] = []
    if direction_delta < -0.0001:
        blocked_reasons.append("source_direction_regression")
    if mae_delta > 0.0001:
        blocked_reasons.append("source_mae_regression")
    return {
        "scope": scope,
        "source": source,
        "prior_rows": len(actuals),
        "allowed": not blocked_reasons,
        "blocked_reason": blocked_reasons[0] if blocked_reasons else "",
        "blocked_reasons": blocked_reasons,
        "direction_match_pct": metrics["direction_match_pct"],
        "baseline_direction_match_pct": baseline["direction_match_pct"],
        "direction_match_delta_pts": _round(direction_delta, 4),
        "mae_pts": metrics["mae_pts"],
        "baseline_mae_pts": baseline["mae_pts"],
        "mae_delta_pts": _round(mae_delta, 4),
        "underprediction_pct": metrics["underprediction_pct"],
    }


def _select_crypto_direction_source(
    record: dict[str, Any],
    *,
    prior_records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Select the safest crypto direction source from prior folds."""
    candidates: list[dict[str, Any]] = []
    for scope, scope_records in _crypto_segment_gate_scopes(record, prior_records):
        for source in CRYPTO_ABSOLUTE_DIRECTION_SOURCES:
            if _absolute_move_delta_for_source(record, source) is None:
                continue
            candidates.append(
                _crypto_direction_source_candidate(
                    scope=scope,
                    records=scope_records,
                    source=source,
                    threshold=threshold,
                )
            )
    passing = [candidate for candidate in candidates if bool(candidate.get("allowed"))]
    if passing:
        selected = max(
            passing,
            key=lambda item: (
                float(item.get("direction_match_pct") or 0.0),
                float(item.get("direction_match_delta_pts") or 0.0),
                -float(item.get("mae_delta_pts") or 0.0),
                int(item.get("prior_rows") or 0),
                -CRYPTO_ABSOLUTE_DIRECTION_SOURCES.index(str(item.get("source") or "blend")),
            ),
        )
        selected["candidate_count"] = len(candidates)
        selected["selected"] = True
        return selected
    best = max(
        candidates,
        key=lambda item: (
            int(item.get("prior_rows") or 0),
            float(item.get("direction_match_pct") or 0.0),
            -float(item.get("mae_delta_pts") or 0.0),
        ),
        default={"scope": "none", "source": "base", "prior_rows": 0, "blocked_reason": "no_source_candidates"},
    )
    best["candidate_count"] = len(candidates)
    best["selected"] = False
    return best


def _apply_crypto_direction_source_selector(
    records: list[dict[str, Any]],
    *,
    threshold: float,
) -> dict[str, Any]:
    """Select crypto absolute-move direction source using prior out-of-sample segment evidence."""
    crypto_records = [record for record in records if str(record.get("focused_fit_category")) == "crypto"]
    before_metrics = _prediction_metrics(crypto_records, "absolute_move_delta", threshold=threshold) if crypto_records else {}
    for record in records:
        record["crypto_direction_source_selector_applied"] = False
        record["crypto_direction_source_selector_source"] = "not_crypto"
        record["crypto_direction_source_selector_scope"] = ""
        record["crypto_direction_source_selector_reason"] = "not_crypto"
        record["crypto_direction_source_selector_prior_rows"] = 0
        record["crypto_direction_source_selector_direction_match_pct"] = 0.0
        record["crypto_direction_source_selector_direction_delta_pts"] = 0.0
        record["crypto_direction_source_selector_mae_delta_pts"] = 0.0
        record["crypto_direction_source_selector_previous_source"] = str(record.get("absolute_move_direction_source") or "")
        record["crypto_direction_source_selector_previous_delta"] = float(record.get("absolute_move_delta") or 0.0)

    for record in sorted(crypto_records, key=lambda item: (int(item.get("fold_index") or 0), str(item.get("condition_ref") or ""))):
        prior_records = [
            item
            for item in crypto_records
            if int(item.get("fold_index") or 0) < int(record.get("fold_index") or 0)
        ]
        selected = _select_crypto_direction_source(
            record,
            prior_records=prior_records,
            threshold=threshold,
        )
        selected_source = str(selected.get("source") or "base")
        selected_delta = _absolute_move_delta_for_source(record, selected_source)
        previous_delta = float(record.get("absolute_move_delta") or 0.0)
        previous_source = str(record.get("absolute_move_direction_source") or "")
        record["crypto_direction_source_selector_source"] = selected_source
        record["crypto_direction_source_selector_scope"] = str(selected.get("scope") or "")
        record["crypto_direction_source_selector_prior_rows"] = int(selected.get("prior_rows") or 0)
        record["crypto_direction_source_selector_direction_match_pct"] = float(
            selected.get("direction_match_pct") or 0.0
        )
        record["crypto_direction_source_selector_direction_delta_pts"] = float(
            selected.get("direction_match_delta_pts") or 0.0
        )
        record["crypto_direction_source_selector_mae_delta_pts"] = float(selected.get("mae_delta_pts") or 0.0)
        record["crypto_direction_source_selector_previous_source"] = previous_source
        record["crypto_direction_source_selector_previous_delta"] = previous_delta
        if bool(selected.get("allowed")) and selected_delta is not None and abs(selected_delta - previous_delta) > 0.0000001:
            record["crypto_direction_source_selector_reason"] = "candidate_report_only"
        elif bool(selected.get("allowed")):
            record["crypto_direction_source_selector_reason"] = "selected_source_matches_existing_delta"
        else:
            record["crypto_direction_source_selector_reason"] = str(
                selected.get("blocked_reason") or "no_prior_safe_source"
            )

    after_metrics = _prediction_metrics(crypto_records, "absolute_move_delta", threshold=threshold) if crypto_records else {}
    applied_records = [record for record in crypto_records if bool(record.get("crypto_direction_source_selector_applied"))]
    applied_previous = (
        _prediction_metrics_from_values(
            [float(record["actual_delta"]) for record in applied_records],
            [float(record.get("crypto_direction_source_selector_previous_delta") or 0.0) for record in applied_records],
            threshold=threshold,
        )
        if applied_records
        else {}
    )
    applied_after = _prediction_metrics(applied_records, "absolute_move_delta", threshold=threshold) if applied_records else {}
    return {
        "row_count": len(crypto_records),
        "applied_row_count": len(applied_records),
        "applied_row_pct": _safe_ratio(len(applied_records), len(crypto_records)),
        "before": before_metrics,
        "after": after_metrics,
        "direction_delta_pts": _round(
            float(after_metrics.get("direction_match_pct") or 0.0) - float(before_metrics.get("direction_match_pct") or 0.0),
            4,
        ),
        "mae_delta_pts": _round(
            float(after_metrics.get("mae_pts") or 0.0) - float(before_metrics.get("mae_pts") or 0.0),
            4,
        ),
        "applied_rows": {
            "before": applied_previous,
            "after": applied_after,
            "direction_delta_pts": _round(
                float(applied_after.get("direction_match_pct") or 0.0)
                - float(applied_previous.get("direction_match_pct") or 0.0),
                4,
            ),
            "mae_delta_pts": _round(
                float(applied_after.get("mae_pts") or 0.0) - float(applied_previous.get("mae_pts") or 0.0),
                4,
            ),
        },
        "source_counts": dict(
            sorted(Counter(str(record.get("crypto_direction_source_selector_source") or "unknown") for record in crypto_records).items())
        ),
        "applied_source_counts": dict(
            sorted(Counter(str(record.get("crypto_direction_source_selector_source") or "unknown") for record in applied_records).items())
        ),
        "scope_counts": dict(
            sorted(Counter(str(record.get("crypto_direction_source_selector_scope") or "none") for record in crypto_records).items())
        ),
        "reason_counts": dict(
            sorted(Counter(str(record.get("crypto_direction_source_selector_reason") or "unknown") for record in crypto_records).items())
        ),
        "policy": {
            "uses_prior_out_of_sample_folds_only": True,
            "candidate_report_only_until_applied_guard_passes": True,
            "candidate_sources": list(CRYPTO_ABSOLUTE_DIRECTION_SOURCES),
            "min_prior_rows": CRYPTO_DIRECTION_SOURCE_SELECTOR_MIN_PRIOR_ROWS,
            "requires_prior_mae_neutral_or_better": True,
            "requires_prior_direction_neutral_or_better": True,
            "scope_order": [
                "asset_market_family",
                "crypto_asset",
                "entry_timing",
                "flow_timing",
                "market_family",
                "crypto_global",
            ],
        },
    }


def _apply_crypto_segment_direction_display_gate(
    records: list[dict[str, Any]],
    *,
    threshold: float,
) -> dict[str, Any]:
    """Promote review-only crypto absolute-move rows when prior segment direction is safe."""
    crypto_records = [record for record in records if str(record.get("focused_fit_category")) == "crypto"]
    for record in records:
        record["crypto_segment_direction_gate_tier"] = str(record.get("absolute_move_display_tier") or "hidden")
        record["crypto_segment_direction_gate_reason"] = "not_crypto"
        record["crypto_segment_direction_gate_scope"] = ""
        record["crypto_segment_direction_gate_prior_rows"] = 0
        record["crypto_segment_direction_gate_direction_match_pct"] = 0.0
        record["crypto_segment_direction_gate_mae_delta_pts"] = 0.0
        record["crypto_segment_direction_gate_strong_watch_alignment_pct"] = 0.0

    promoted: list[dict[str, Any]] = []
    kept_review: list[dict[str, Any]] = []
    already_show: list[dict[str, Any]] = []
    for record in sorted(crypto_records, key=lambda item: (int(item.get("fold_index") or 0), str(item.get("condition_ref") or ""))):
        if str(record.get("absolute_move_display_tier") or "hidden") == "show":
            record["crypto_segment_direction_gate_reason"] = "category_gate_already_show"
            already_show.append(record)
            continue
        if str(record.get("absolute_move_display_tier") or "hidden") != "review":
            record["crypto_segment_direction_gate_reason"] = "category_gate_not_review"
            continue
        prior_records = [
            item
            for item in crypto_records
            if int(item.get("fold_index") or 0) < int(record.get("fold_index") or 0)
        ]
        selected = _select_crypto_segment_gate_candidate(
            record,
            prior_records=prior_records,
            threshold=threshold,
        )
        record["crypto_segment_direction_gate_scope"] = str(selected.get("scope") or "")
        record["crypto_segment_direction_gate_prior_rows"] = int(selected.get("prior_rows") or 0)
        record["crypto_segment_direction_gate_direction_match_pct"] = float(
            selected.get("absolute_move_direction_match_pct") or 0.0
        )
        record["crypto_segment_direction_gate_mae_delta_pts"] = float(selected.get("mae_delta_pts") or 0.0)
        record["crypto_segment_direction_gate_strong_watch_alignment_pct"] = float(
            selected.get("strong_watch_alignment_pct") or 0.0
        )
        if bool(selected.get("allowed")):
            record["absolute_move_display_allowed"] = True
            record["absolute_move_review_allowed"] = False
            record["absolute_move_display_tier"] = "show"
            record["absolute_move_display_reasons"] = []
            record["absolute_move_review_reasons"] = []
            record["absolute_move_display_gate_category"] = "crypto_segment_direction_gate"
            record["crypto_segment_direction_gate_tier"] = "show"
            record["crypto_segment_direction_gate_reason"] = "prior_segment_direction_safe"
            promoted.append(record)
        else:
            record["crypto_segment_direction_gate_tier"] = "review"
            record["crypto_segment_direction_gate_reason"] = str(
                selected.get("blocked_reason") or "no_prior_safe_segment"
            )
            kept_review.append(record)

    promoted_overlay = _prediction_metrics(promoted, "overlay_blend_delta", threshold=threshold) if promoted else {}
    promoted_absolute = _prediction_metrics(promoted, "absolute_move_delta", threshold=threshold) if promoted else {}
    kept_overlay = _prediction_metrics(kept_review, "overlay_blend_delta", threshold=threshold) if kept_review else {}
    kept_absolute = _prediction_metrics(kept_review, "absolute_move_delta", threshold=threshold) if kept_review else {}
    return {
        "row_count": len(crypto_records),
        "already_show_row_count": len(already_show),
        "eligible_review_row_count": len(promoted) + len(kept_review),
        "promoted_row_count": len(promoted),
        "promoted_row_pct": _safe_ratio(len(promoted), len(promoted) + len(kept_review)),
        "kept_review_row_count": len(kept_review),
        "kept_review_row_pct": _safe_ratio(len(kept_review), len(promoted) + len(kept_review)),
        "tier_counts": dict(
            sorted(Counter(str(record.get("crypto_segment_direction_gate_tier") or "hidden") for record in crypto_records).items())
        ),
        "reason_counts": dict(
            sorted(Counter(str(record.get("crypto_segment_direction_gate_reason") or "unknown") for record in crypto_records).items())
        ),
        "scope_counts": dict(
            sorted(
                Counter(
                    str(record.get("crypto_segment_direction_gate_scope") or "none")
                    for record in crypto_records
                    if str(record.get("crypto_segment_direction_gate_scope") or "")
                ).items()
            )
        ),
        "promoted_asset_counts": dict(
            sorted(Counter(str(record.get("crypto_asset") or "unknown") for record in promoted).items())
        ),
        "promoted_entry_timing_counts": dict(
            sorted(Counter(str(record.get("whale_entry_timing_bucket") or "unknown") for record in promoted).items())
        ),
        "kept_review_asset_counts": dict(
            sorted(Counter(str(record.get("crypto_asset") or "unknown") for record in kept_review).items())
        ),
        "kept_review_entry_timing_counts": dict(
            sorted(Counter(str(record.get("whale_entry_timing_bucket") or "unknown") for record in kept_review).items())
        ),
        "promoted_stale_btc_row_count": sum(
            1
            for record in promoted
            if str(record.get("crypto_asset") or "") == "btc"
            and str(record.get("whale_entry_timing_bucket") or "") not in CRYPTO_SEGMENT_GATE_RECENT_ENTRY_BUCKETS
        ),
        "promoted_rows": {
            "overlay": promoted_overlay,
            "absolute_move": promoted_absolute,
            "mae_delta_pts": _round(
                float(promoted_absolute.get("mae_pts") or 0.0) - float(promoted_overlay.get("mae_pts") or 0.0),
                4,
            ),
            "direction_match_delta_pts": _round(
                float(promoted_absolute.get("direction_match_pct") or 0.0)
                - float(promoted_overlay.get("direction_match_pct") or 0.0),
                4,
            ),
        },
        "kept_review_rows": {
            "overlay": kept_overlay,
            "absolute_move": kept_absolute,
            "mae_delta_pts": _round(
                float(kept_absolute.get("mae_pts") or 0.0) - float(kept_overlay.get("mae_pts") or 0.0),
                4,
            ),
            "direction_match_delta_pts": _round(
                float(kept_absolute.get("direction_match_pct") or 0.0)
                - float(kept_overlay.get("direction_match_pct") or 0.0),
                4,
            ),
        },
        "policy": {
            "uses_prior_out_of_sample_folds_only": True,
            "diagnostic_page_only": True,
            "only_promotes_review_crypto_rows": True,
            "min_prior_rows": CRYPTO_SEGMENT_GATE_MIN_PRIOR_ROWS,
            "min_absolute_direction_match_pct": CRYPTO_SEGMENT_GATE_MIN_DIRECTION_MATCH_PCT,
            "min_strong_watch_alignment_pct": CRYPTO_SEGMENT_GATE_MIN_STRONG_WATCH_ALIGNMENT_PCT,
            "btc_asset_scope_requires_recent_entry_bucket": sorted(CRYPTO_SEGMENT_GATE_RECENT_ENTRY_BUCKETS),
            "requires_prior_mae_neutral_or_better": True,
            "requires_prior_direction_neutral_or_better": True,
            "scope_order": [
                "asset_market_family",
                "crypto_asset",
                "entry_timing",
                "flow_timing",
                "market_family",
                "crypto_global",
            ],
        },
    }


def _absolute_move_summary(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return aggregate diagnostics for the absolute-move split model."""
    overlay_metrics = _prediction_metrics(records, "overlay_blend_delta", threshold=threshold)
    absolute_metrics = _prediction_metrics(records, "absolute_move_delta", threshold=threshold)
    direction_records = [record for record in records if _absolute_move_has_direction(record)]
    displayed = [record for record in records if bool(record.get("absolute_move_display_allowed"))]
    reviewed = [record for record in records if bool(record.get("absolute_move_review_allowed"))]
    visible = [
        record
        for record in records
        if bool(record.get("absolute_move_display_allowed")) or bool(record.get("absolute_move_review_allowed"))
    ]
    displayed_absolute = (
        _prediction_metrics(displayed, "absolute_move_delta", threshold=threshold) if displayed else {}
    )
    displayed_overlay = (
        _prediction_metrics(displayed, "overlay_blend_delta", threshold=threshold) if displayed else {}
    )
    visible_absolute = _prediction_metrics(visible, "absolute_move_delta", threshold=threshold) if visible else {}
    visible_overlay = _prediction_metrics(visible, "overlay_blend_delta", threshold=threshold) if visible else {}
    return {
        "policy": {
            "objective": "split_direction_from_absolute_move_size_for_trend_closeness",
            "uses_prior_out_of_sample_folds_only": True,
            "base_direction_order": [
                "strong_watch_direction_signal",
                "crypto_overlay_direction_fallback",
                "whale_pressure",
                "overlay_direction_fallback",
            ],
            "crypto_uses_overlay_direction_before_whale_pressure": True,
            "display_requires_category_mae_neutral_or_better": True,
            "display_requires_category_direction_neutral_or_better": True,
            "review_tier_is_diagnostic_only": True,
            "review_only_categories": sorted(ABSOLUTE_MOVE_REVIEW_ONLY_CATEGORIES),
            "review_max_direction_regression_pts": ABSOLUTE_MOVE_REVIEW_MAX_DIRECTION_REGRESSION_PTS,
            "crypto_segment_direction_gate_can_promote_review_rows": True,
        },
        "direction_row_count": len(direction_records),
        "direction_row_pct": _safe_ratio(len(direction_records), len(records)),
        "displayed_row_count": len(displayed),
        "displayed_row_pct": _safe_ratio(len(displayed), len(records)),
        "review_row_count": len(reviewed),
        "review_row_pct": _safe_ratio(len(reviewed), len(records)),
        "visible_row_count": len(visible),
        "visible_row_pct": _safe_ratio(len(visible), len(records)),
        "display_tier_counts": dict(
            sorted(Counter(str(record.get("absolute_move_display_tier") or "hidden") for record in records).items())
        ),
        "direction_source_counts": dict(
            sorted(Counter(str(record.get("absolute_move_direction_source") or "unknown") for record in records).items())
        ),
        "all_rows": {
            "overlay": overlay_metrics,
            "absolute_move": absolute_metrics,
            "mae_delta_pts": _round(
                float(absolute_metrics["mae_pts"]) - float(overlay_metrics["mae_pts"]),
                4,
            ),
            "average_abs_predicted_delta_delta_pts": _round(
                float(absolute_metrics["average_abs_predicted_delta_pts"])
                - float(overlay_metrics["average_abs_predicted_delta_pts"]),
                4,
            ),
            "underprediction_delta_pts": _round(
                float(absolute_metrics["underprediction_pct"]) - float(overlay_metrics["underprediction_pct"]),
                4,
            ),
        },
        "displayed_rows": {
            "overlay": displayed_overlay,
            "absolute_move": displayed_absolute,
            "mae_delta_pts": _round(
                float(displayed_absolute.get("mae_pts") or 0.0) - float(displayed_overlay.get("mae_pts") or 0.0),
                4,
            ),
        },
        "visible_rows": {
            "overlay": visible_overlay,
            "absolute_move": visible_absolute,
            "mae_delta_pts": _round(
                float(visible_absolute.get("mae_pts") or 0.0) - float(visible_overlay.get("mae_pts") or 0.0),
                4,
            ),
        },
    }


def _absolute_move_fit_summary(
    records: list[dict[str, Any]],
    *,
    threshold: float,
) -> list[dict[str, Any]]:
    """Return focused-category metrics for the absolute-move split overlay."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get("focused_fit_category") or "unknown"), []).append(record)
    rows: list[dict[str, Any]] = []
    ordered = list(FOCUSED_FIT_CATEGORY_ORDER)
    ordered.extend(
        category
        for category, _ in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
        if category not in ordered
    )
    for category in ordered:
        segment_records = grouped.get(category, [])
        if not segment_records:
            continue
        overlay = _prediction_metrics(segment_records, "overlay_blend_delta", threshold=threshold)
        absolute = _prediction_metrics(segment_records, "absolute_move_delta", threshold=threshold)
        absolute_base = _prediction_metrics(segment_records, "absolute_move_base_delta", threshold=threshold)
        direction_rows = [record for record in segment_records if _absolute_move_has_direction(record)]
        resolved = [record for record in segment_records if bool(record.get("absolute_move_direction_resolver_applied"))]
        display_allowed = any(bool(record.get("absolute_move_display_allowed")) for record in segment_records)
        display_reasons = sorted(
            {
                str(reason)
                for record in segment_records
                for reason in list(record.get("absolute_move_display_reasons") or [])
            }
        )
        review_allowed = any(bool(record.get("absolute_move_review_allowed")) for record in segment_records)
        review_reasons = sorted(
            {
                str(reason)
                for record in segment_records
                for reason in list(record.get("absolute_move_review_reasons") or [])
            }
        )
        display_tiers = Counter(str(record.get("absolute_move_display_tier") or "hidden") for record in segment_records)
        display_tier = "show" if display_allowed else "review" if review_allowed else "hidden"
        rows.append(
            {
                "category": category,
                "row_count": len(segment_records),
                "direction_row_count": len(direction_rows),
                "direction_row_pct": _safe_ratio(len(direction_rows), len(segment_records)),
                "overlay_mae_pts": overlay["mae_pts"],
                "absolute_move_mae_pts": absolute["mae_pts"],
                "absolute_move_base_mae_pts": absolute_base["mae_pts"],
                "mae_delta_pts": _round(float(absolute["mae_pts"]) - float(overlay["mae_pts"]), 4),
                "mae_delta_vs_base_pts": _round(float(absolute["mae_pts"]) - float(absolute_base["mae_pts"]), 4),
                "overlay_average_abs_predicted_delta_pts": overlay["average_abs_predicted_delta_pts"],
                "absolute_move_average_abs_predicted_delta_pts": absolute["average_abs_predicted_delta_pts"],
                "absolute_move_base_average_abs_predicted_delta_pts": absolute_base[
                    "average_abs_predicted_delta_pts"
                ],
                "average_abs_actual_delta_pts": absolute["average_abs_actual_delta_pts"],
                "overlay_underprediction_pct": overlay["underprediction_pct"],
                "absolute_move_underprediction_pct": absolute["underprediction_pct"],
                "direction_match_pct": absolute["direction_match_pct"],
                "absolute_move_base_direction_match_pct": absolute_base["direction_match_pct"],
                "overlay_direction_match_pct": overlay["direction_match_pct"],
                "resolved_row_count": len(resolved),
                "resolved_row_pct": _safe_ratio(len(resolved), len(segment_records)),
                "display_tier": display_tier,
                "display_tier_counts": dict(sorted(display_tiers.items())),
                "display_allowed": display_allowed,
                "display_reasons": display_reasons,
                "review_allowed": review_allowed,
                "review_reasons": review_reasons,
                "direction_source_counts": dict(
                    sorted(
                        Counter(
                            str(record.get("absolute_move_direction_source") or "unknown")
                            for record in segment_records
                        ).items()
                    )
                ),
            }
        )
    return rows


def _direction_tier_overlay_gate(record: dict[str, Any]) -> dict[str, Any]:
    """Return whether a prediction has enough direction-tier support for a trend overlay."""
    reasons: list[str] = []
    tier = str(record.get("direction_signal_tier") or "missing")
    predicted_direction = str(record.get("direction_signal_predicted_direction") or "flat")
    overlay_direction = str(
        record.get("overlay_blend_direction")
        or record.get("pair_normalized_blend_direction")
        or record.get("blend_direction")
        or "flat"
    )
    if not bool(record.get("direction_signal_lookup_available")):
        reasons.append("direction_tier_lookup_unavailable")
    elif not bool(record.get("direction_signal_matched")):
        reasons.append("direction_tier_missing_for_row")
    elif tier not in SURFACED_DIRECTION_TIERS:
        reasons.append("no_strong_watch_direction_tier")
    elif predicted_direction not in {"up", "down"}:
        reasons.append("direction_tier_not_movement")
    elif overlay_direction == "flat":
        reasons.append("pair_normalized_blend_flat")
    elif predicted_direction != overlay_direction:
        reasons.append("direction_tier_disagrees_with_overlay")

    return {
        "allowed": not reasons,
        "reasons": reasons,
        "policy": {
            "required_tiers": sorted(SURFACED_DIRECTION_TIERS),
            "requires_direction_agreement": True,
            "uses_overlay_blend_direction": True,
        },
    }


def _overlay_gate_for_segment(
    segment: dict[str, Any],
    *,
    blend_key: str = "overlay_blend",
    interval_key: str = "overlay_quantile_interval",
) -> dict[str, Any]:
    """Return whether a category/window segment should show the overlay by default."""
    reasons: list[str] = []
    blend = segment.get(blend_key) or segment["pair_normalized_blend"]
    interval = segment.get(interval_key) or segment["pair_normalized_quantile_interval"]
    if int(segment["row_count"]) < OVERLAY_GATE_MIN_ROWS:
        reasons.append("insufficient_segment_rows")
    if float(blend["direction_match_pct"]) < OVERLAY_GATE_MIN_DIRECTION_MATCH_PCT:
        reasons.append("direction_match_below_gate")
    if float(blend["underprediction_pct"]) > OVERLAY_GATE_MAX_UNDERPREDICTION_PCT:
        reasons.append("underprediction_above_gate")
    if float(interval["average_width_pts"]) > OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS:
        reasons.append("interval_too_wide")
    if float(interval["coverage_pct"]) < OVERLAY_GATE_MIN_BAND_COVERAGE_PCT:
        reasons.append("interval_coverage_below_gate")

    return {
        "allowed": not reasons,
        "reasons": reasons,
        "policy": {
            "min_rows": OVERLAY_GATE_MIN_ROWS,
            "min_direction_match_pct": OVERLAY_GATE_MIN_DIRECTION_MATCH_PCT,
            "max_underprediction_pct": OVERLAY_GATE_MAX_UNDERPREDICTION_PCT,
            "max_interval_width_pts": OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS,
            "min_band_coverage_pct": OVERLAY_GATE_MIN_BAND_COVERAGE_PCT,
        },
    }


def _segment_payload(
    *,
    category: str,
    records: list[dict[str, Any]],
    threshold: float,
    segment_type: str,
) -> dict[str, Any]:
    """Return metrics and gates for one segment."""
    segment = {
        "category": category,
        "segment_type": segment_type,
        "row_count": len(records),
        "condition_count": len({str(record["condition_ref"]) for record in records}),
        "direction_counts": dict(sorted(Counter(str(record["actual_direction"]) for record in records).items())),
        "current_residual_whale": _prediction_metrics(
            records,
            "residual_delta",
            threshold=threshold,
        ),
        "nonflat_delta_blend": _prediction_metrics(
            records,
            "blend_delta",
            threshold=threshold,
        ),
        "pair_normalized_blend": _prediction_metrics(
            records,
            "pair_normalized_blend_delta",
            threshold=threshold,
        ),
        "overlay_base_blend": _prediction_metrics(
            records,
            "overlay_base_blend_delta",
            threshold=threshold,
        ),
        "overlay_blend": _prediction_metrics(
            records,
            "overlay_blend_delta",
            threshold=threshold,
        ),
        "absolute_move_overlay": _prediction_metrics(
            records,
            "absolute_move_delta",
            threshold=threshold,
        ),
        "quantile_interval": _interval_metrics(records),
        "pair_normalized_quantile_interval": _interval_metrics(
            records,
            low_key="pair_normalized_quantile_low_delta",
            high_key="pair_normalized_quantile_high_delta",
        ),
        "overlay_base_quantile_interval": _interval_metrics(
            records,
            low_key="overlay_base_quantile_low_delta",
            high_key="overlay_base_quantile_high_delta",
        ),
        "overlay_quantile_interval": _interval_metrics(
            records,
            low_key="overlay_quantile_low_delta",
            high_key="overlay_quantile_high_delta",
        ),
        "average_magnitude_scale": _round(
            sum(float(record["segment_magnitude_scale"]) for record in records) / len(records),
            6,
        ),
        "average_blend_alpha": _round(
            sum(float(record["segment_blend_alpha"]) for record in records) / len(records),
            6,
        ),
    }
    segment["overlay_calibration_delta"] = _delta_metrics(
        segment["overlay_blend"],
        segment["overlay_base_blend"],
    )
    segment["overlay_base_gate"] = _overlay_gate_for_segment(
        segment,
        blend_key="overlay_base_blend",
        interval_key="overlay_base_quantile_interval",
    )
    segment["overlay_gate"] = _overlay_gate_for_segment(segment)
    return segment


def _segment_summaries(
    records: list[dict[str, Any]],
    *,
    threshold: float,
    group_key: str = "focus_category",
    ordered_categories: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    """Return segment-specific metrics and overlay gates."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(group_key) or "unknown"), []).append(record)

    segments: list[dict[str, Any]] = []
    ordered = list(ordered_categories or (*DEFAULT_FOCUS_DOMAINS, "other"))
    ordered.extend(
        category
        for category, _ in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
        if category not in ordered
    )
    for category in ordered:
        category_records = grouped.get(category, [])
        if not category_records:
            continue
        segments.append(
            _segment_payload(
                category=category,
                records=category_records,
                threshold=threshold,
                segment_type=group_key,
            )
        )
    return segments


def _apply_overlay_gates(
    records: list[dict[str, Any]],
    segments: list[dict[str, Any]],
    *,
    group_key: str = "focus_category",
) -> None:
    """Attach segment-gate outcomes to each record."""
    by_category = {str(segment["category"]): segment for segment in segments}
    for record in records:
        segment = by_category.get(str(record.get(group_key) or "unknown"))
        gate = segment.get("overlay_gate", {}) if segment else {}
        category_reasons = list(gate.get("reasons") or [])
        direction_gate = _direction_tier_overlay_gate(record)
        direction_reasons = list(direction_gate.get("reasons") or [])
        record["category_overlay_gate_allowed"] = bool(gate.get("allowed"))
        record["category_overlay_gate_reasons"] = category_reasons
        record["direction_tier_overlay_gate_allowed"] = bool(direction_gate.get("allowed"))
        record["direction_tier_overlay_gate_reasons"] = direction_reasons
        record["overlay_gate_allowed"] = bool(gate.get("allowed")) and bool(direction_gate.get("allowed"))
        record["overlay_gate_reasons"] = [*category_reasons, *direction_reasons]
        record["overlay_gate_segment_rows"] = int(segment.get("row_count", 0)) if segment else 0
        record["overlay_gate_segment_category"] = str(segment.get("category") or "") if segment else ""
        review_reasons = {"direction_tier_disagrees_with_overlay", "pair_normalized_blend_flat"}
        has_supported_direction = (
            str(record.get("direction_signal_tier")) in SURFACED_DIRECTION_TIERS
            and str(record.get("direction_signal_predicted_direction")) in {"up", "down"}
        )
        if record["overlay_gate_allowed"]:
            candidate_tier = "surfaced"
            candidate_reason = "strict_overlay_gate_passed"
        elif (
            bool(record["category_overlay_gate_allowed"])
            and has_supported_direction
            and bool(direction_reasons)
            and set(direction_reasons).issubset(review_reasons)
        ):
            candidate_tier = "review"
            candidate_reason = ";".join(direction_reasons)
        elif not bool(record["category_overlay_gate_allowed"]):
            candidate_tier = "suppressed"
            candidate_reason = "category_gate_failed"
        else:
            candidate_tier = "suppressed"
            candidate_reason = ";".join(direction_reasons) or "direction_gate_failed"
        record["overlay_candidate_tier"] = candidate_tier
        record["overlay_candidate_reason"] = candidate_reason


def _direction_tier_support_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return direction-tier support coverage for trend overlays."""
    matched = [record for record in records if bool(record.get("direction_signal_matched"))]
    supported = [record for record in records if str(record.get("direction_signal_tier")) in SURFACED_DIRECTION_TIERS]
    direction_aligned = [
        record
        for record in supported
        if str(record.get("direction_signal_predicted_direction")) == str(record.get("overlay_blend_direction"))
        and str(record.get("overlay_blend_direction")) in {"up", "down"}
    ]
    return {
        "matched_row_count": len(matched),
        "matched_row_pct": _safe_ratio(len(matched), len(records)),
        "strong_watch_row_count": len(supported),
        "strong_watch_row_pct": _safe_ratio(len(supported), len(records)),
        "direction_aligned_row_count": len(direction_aligned),
        "direction_aligned_row_pct": _safe_ratio(len(direction_aligned), len(records)),
        "tier_counts": dict(sorted(Counter(str(record.get("direction_signal_tier")) for record in records).items())),
        "gate_reason_counts": dict(
            sorted(
                Counter(
                    reason
                    for record in records
                    for reason in list(record.get("direction_tier_overlay_gate_reasons") or [])
                ).items()
            )
        ),
    }


def _overlay_candidate_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return strict and review overlay coverage counts."""
    tier_counts = Counter(str(record.get("overlay_candidate_tier") or "unknown") for record in records)
    review_records = [record for record in records if str(record.get("overlay_candidate_tier")) == "review"]
    surfaced_records = [record for record in records if str(record.get("overlay_candidate_tier")) == "surfaced"]
    surfaced_or_review = len(surfaced_records) + len(review_records)
    return {
        "surfaced_row_count": len(surfaced_records),
        "review_candidate_row_count": len(review_records),
        "surfaced_or_review_row_count": surfaced_or_review,
        "review_candidate_row_pct": _safe_ratio(len(review_records), len(records)),
        "surfaced_or_review_row_pct": _safe_ratio(surfaced_or_review, len(records)),
        "candidate_tier_counts": dict(sorted(tier_counts.items())),
        "review_reason_counts": dict(
            sorted(Counter(str(record.get("overlay_candidate_reason") or "") for record in review_records).items())
        ),
    }


def _overlay_blend_selection_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return how the displayed overlay blend was selected."""
    source_counts = Counter(str(record.get("overlay_blend_source") or "unknown") for record in records)
    raw_preserved_count = int(source_counts.get("raw_blend_direction_preserved", 0))
    return {
        "source_counts": dict(sorted(source_counts.items())),
        "raw_direction_preserved_row_count": raw_preserved_count,
        "raw_direction_preserved_row_pct": _safe_ratio(raw_preserved_count, len(records)),
        "pair_normalized_used_row_count": len(records) - raw_preserved_count,
        "pair_normalized_used_row_pct": _safe_ratio(len(records) - raw_preserved_count, len(records)),
    }


def _overlay_magnitude_scale_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return final overlay magnitude recalibration coverage."""
    source_counts = Counter(str(record.get("overlay_magnitude_scale_source") or "unknown") for record in records)
    scaled = [record for record in records if abs(float(record.get("overlay_magnitude_scale") or 1.0) - 1.0) > 0.0001]
    return {
        "source_counts": dict(sorted(source_counts.items())),
        "scaled_row_count": len(scaled),
        "scaled_row_pct": _safe_ratio(len(scaled), len(records)),
        "average_scale": _round(
            sum(float(record.get("overlay_magnitude_scale") or 1.0) for record in records) / len(records),
            4,
        )
        if records
        else 1.0,
        "max_scale": _round(max((float(record.get("overlay_magnitude_scale") or 1.0) for record in records), default=1.0), 4),
    }


def _fit_summary_from_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return compact before/after fit rows for report and dashboard."""
    rows: list[dict[str, Any]] = []
    for segment in segments:
        before = segment["overlay_base_blend"]
        after = segment["overlay_blend"]
        interval = segment["overlay_quantile_interval"]
        rows.append(
            {
                "category": segment["category"],
                "segment_type": segment.get("segment_type"),
                "row_count": segment["row_count"],
                "condition_count": segment["condition_count"],
                "direction_match_pct": after["direction_match_pct"],
                "pre_calibration_direction_match_pct": before["direction_match_pct"],
                "mae_pts": after["mae_pts"],
                "pre_calibration_mae_pts": before["mae_pts"],
                "mae_delta_pts": _round(float(after["mae_pts"]) - float(before["mae_pts"]), 4),
                "average_abs_actual_delta_pts": after["average_abs_actual_delta_pts"],
                "average_abs_predicted_delta_pts": after["average_abs_predicted_delta_pts"],
                "pre_calibration_average_abs_predicted_delta_pts": before["average_abs_predicted_delta_pts"],
                "underprediction_pct": after["underprediction_pct"],
                "band_coverage_pct": interval["coverage_pct"],
                "band_width_pts": interval["average_width_pts"],
                "gate_allowed": bool(segment["overlay_gate"]["allowed"]),
                "gate_reasons": list(segment["overlay_gate"]["reasons"]),
                "base_gate_allowed": bool(segment["overlay_base_gate"]["allowed"]),
                "base_gate_reasons": list(segment["overlay_base_gate"]["reasons"]),
            }
        )
    return rows


def _trend_fit_error_type(record: dict[str, Any], *, threshold: float) -> str:
    """Return the primary reason the overlay differs from the realized trend."""
    actual_delta = float(record["actual_delta"])
    predicted_delta = float(record["overlay_blend_delta"])
    actual_direction = _direction(actual_delta, threshold)
    predicted_direction = _direction(predicted_delta, threshold)
    if actual_direction != predicted_direction:
        return "direction_miss"
    if not bool(record.get("overlay_interval_contains_actual")):
        return "interval_miss"
    if abs(predicted_delta) < abs(actual_delta):
        return "correct_direction_underfit"
    return "correct_direction_overfit"


def _trend_shape_score(record: dict[str, Any], *, threshold: float) -> float:
    """Return a compact 0-100 endpoint-line fit score for dashboard cases."""
    actual_delta = float(record["actual_delta"])
    predicted_delta = float(record["overlay_blend_delta"])
    actual_direction = _direction(actual_delta, threshold)
    predicted_direction = _direction(predicted_delta, threshold)
    error_pts = _pct(abs(actual_delta - predicted_delta))
    actual_magnitude_pts = max(_pct(abs(actual_delta)), 0.5)
    relative_penalty = min(100.0, (error_pts / actual_magnitude_pts) * 100.0)
    score = max(0.0, 100.0 - relative_penalty)
    if actual_direction != predicted_direction:
        score = min(score, 40.0)
    return _round(score, 2)


def _overlay_decision_summary(record: dict[str, Any]) -> str:
    """Return a compact dashboard explanation for the overlay decision."""
    tier = str(record.get("overlay_candidate_tier") or "suppressed")
    signal_tier = str(record.get("direction_signal_tier") or "missing")
    calibration_method = str(record.get("selected_calibration_method") or "identity")
    calibration_source = str(record.get("selected_calibration_source") or "identity")
    if bool(record.get("overlay_gate_allowed")):
        return (
            f"surfaced: {signal_tier} direction agreed with the overlay and the category gate passed; "
            f"trend fit used {calibration_method} from {calibration_source}"
        )
    if tier == "review":
        reason = str(record.get("overlay_candidate_reason") or "direction review required").replace("_", " ")
        return f"review-only: category gate passed, but {reason}"
    if str(record.get("focused_fit_category")) == "video_games_esports":
        reasons = ", ".join(str(reason).replace("_", " ") for reason in list(record.get("overlay_gate_reasons") or []))
        return f"review-only: esports/video-games remains suppressed until gates pass ({reasons or 'insufficient support'})"
    reasons = ", ".join(str(reason).replace("_", " ") for reason in list(record.get("overlay_gate_reasons") or []))
    return f"suppressed: {reasons or 'overlay gate did not pass'}"


def _line_path_mae_pts(actual_delta: float, predicted_delta: float) -> float:
    """Return average path MAE for a straight line from current odds to one endpoint."""
    return _pct(abs(actual_delta - predicted_delta) / 2.0)


def _trend_fit_diagnostic_rows(
    records: list[dict[str, Any]],
    *,
    group_key: str,
    threshold: float,
    ordered_categories: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    """Return trend-fit diagnostics by category/family segment."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(group_key) or "unknown"), []).append(record)

    ordered = list(ordered_categories or ())
    ordered.extend(
        category
        for category, _ in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
        if category not in ordered
    )
    rows: list[dict[str, Any]] = []
    for category in ordered:
        segment_records = grouped.get(category, [])
        if not segment_records:
            continue
        metrics = _prediction_metrics(segment_records, "overlay_blend_delta", threshold=threshold)
        before = _prediction_metrics(segment_records, "overlay_base_blend_delta", threshold=threshold)
        interval = _interval_metrics(
            segment_records,
            low_key="overlay_quantile_low_delta",
            high_key="overlay_quantile_high_delta",
        )
        actuals = [float(record["actual_delta"]) for record in segment_records]
        predictions = [float(record["overlay_blend_delta"]) for record in segment_records]
        signed_bias_pts = _pct(sum(predicted - actual for actual, predicted in zip(actuals, predictions, strict=True)) / len(segment_records))
        magnitude_ratio = (
            _round(
                float(metrics["average_abs_predicted_delta_pts"]) / float(metrics["average_abs_actual_delta_pts"]),
                4,
            )
            if float(metrics["average_abs_actual_delta_pts"]) > 0
            else 0.0
        )
        rows.append(
            {
                "segment": category,
                "segment_type": group_key,
                "row_count": len(segment_records),
                "condition_count": len({str(record["condition_ref"]) for record in segment_records}),
                "mae_pts": metrics["mae_pts"],
                "pre_calibration_mae_pts": before["mae_pts"],
                "mae_delta_pts": _round(float(metrics["mae_pts"]) - float(before["mae_pts"]), 4),
                "signed_bias_pts": signed_bias_pts,
                "average_abs_actual_delta_pts": metrics["average_abs_actual_delta_pts"],
                "average_abs_predicted_delta_pts": metrics["average_abs_predicted_delta_pts"],
                "magnitude_ratio": magnitude_ratio,
                "underprediction_pct": metrics["underprediction_pct"],
                "direction_match_pct": metrics["direction_match_pct"],
                "pre_calibration_direction_match_pct": before["direction_match_pct"],
                "band_coverage_pct": interval["coverage_pct"],
                "band_width_pts": interval["average_width_pts"],
                "gate_allowed": bool(any(record.get("overlay_gate_allowed") for record in segment_records)),
                "error_type_counts": dict(
                    sorted(
                        Counter(
                            str(record.get("trend_fit_error_type") or _trend_fit_error_type(record, threshold=threshold))
                            for record in segment_records
                        ).items()
                    )
                ),
                "selected_method_counts": dict(
                    sorted(Counter(str(record.get("selected_calibration_method") or "identity") for record in segment_records).items())
                ),
                "selected_source_counts": dict(
                    sorted(Counter(str(record.get("selected_calibration_source") or "identity") for record in segment_records).items())
                ),
            }
        )
    return rows


def _trend_fit_diagnostics(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return focused, event, and market-family trend-fit diagnostic slices."""
    return {
        "focused_category": _trend_fit_diagnostic_rows(
            records,
            group_key="focused_fit_category",
            threshold=threshold,
            ordered_categories=FOCUSED_FIT_CATEGORY_ORDER,
        ),
        "event_category": _trend_fit_diagnostic_rows(
            records,
            group_key="event_category",
            threshold=threshold,
            ordered_categories=EVENT_CATEGORY_ORDER,
        ),
        "market_family": _trend_fit_diagnostic_rows(
            records,
            group_key="market_family",
            threshold=threshold,
        ),
        "calibration_segment": _trend_fit_diagnostic_rows(
            records,
            group_key="trend_calibration_segment",
            threshold=threshold,
        ),
    }


def _crypto_direction_audit_row(
    *,
    segment: str,
    segment_type: str,
    records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Return a crypto direction-miss diagnostic row."""
    metrics = _prediction_metrics(records, "overlay_blend_delta", threshold=threshold)
    direction_misses = [
        record
        for record in records
        if _direction(float(record["actual_delta"]), threshold)
        != _direction(float(record["overlay_blend_delta"]), threshold)
    ]
    pressure_aligned = [
        record
        for record in records
        if str(record.get("whale_pressure_direction") or "neutral") in {"up", "down"}
        and str(record.get("whale_pressure_direction")) == str(record.get("actual_direction"))
    ]
    pressure_opposed = [
        record
        for record in records
        if str(record.get("whale_pressure_direction") or "neutral") in {"up", "down"}
        and str(record.get("actual_direction")) in {"up", "down"}
        and str(record.get("whale_pressure_direction")) != str(record.get("actual_direction"))
    ]
    supported = [record for record in records if str(record.get("direction_signal_tier")) in SURFACED_DIRECTION_TIERS]
    signal_aligned = [
        record
        for record in supported
        if str(record.get("direction_signal_predicted_direction")) == str(record.get("actual_direction"))
    ]
    return {
        "segment": segment,
        "segment_type": segment_type,
        "row_count": len(records),
        "direction_match_pct": metrics["direction_match_pct"],
        "direction_miss_count": len(direction_misses),
        "direction_miss_pct": _safe_ratio(len(direction_misses), len(records)),
        "average_abs_actual_delta_pts": metrics["average_abs_actual_delta_pts"],
        "average_abs_predicted_delta_pts": metrics["average_abs_predicted_delta_pts"],
        "underprediction_pct": metrics["underprediction_pct"],
        "actual_direction_counts": dict(sorted(Counter(str(record.get("actual_direction")) for record in records).items())),
        "overlay_direction_counts": dict(
            sorted(Counter(str(record.get("overlay_blend_direction")) for record in records).items())
        ),
        "signal_direction_counts": dict(
            sorted(Counter(str(record.get("direction_signal_predicted_direction")) for record in records).items())
        ),
        "signal_tier_counts": dict(sorted(Counter(str(record.get("direction_signal_tier")) for record in records).items())),
        "strong_watch_row_count": len(supported),
        "strong_watch_actual_alignment_pct": _safe_ratio(len(signal_aligned), len(supported)),
        "whale_pressure_direction_counts": dict(
            sorted(Counter(str(record.get("whale_pressure_direction") or "neutral") for record in records).items())
        ),
        "whale_pressure_actual_alignment_pct": _safe_ratio(len(pressure_aligned), len(records)),
        "whale_pressure_actual_opposition_pct": _safe_ratio(len(pressure_opposed), len(records)),
    }


def _crypto_direction_miss_audit(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return crypto-specific direction miss diagnostics."""
    crypto_records = [record for record in records if str(record.get("focused_fit_category")) == "crypto"]
    if not crypto_records:
        return {
            "row_count": 0,
            "summary": None,
            "by_calibration_segment": [],
            "by_time_to_close_bucket": [],
            "by_crypto_asset": [],
            "by_signal_tier": [],
            "by_whale_pressure_direction": [],
        }

    def grouped_rows(group_key: str) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in crypto_records:
            grouped.setdefault(str(record.get(group_key) or "unknown"), []).append(record)
        return [
            _crypto_direction_audit_row(
                segment=segment,
                segment_type=group_key,
                records=rows,
                threshold=threshold,
            )
            for segment, rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
        ]

    return {
        "row_count": len(crypto_records),
        "summary": _crypto_direction_audit_row(
            segment="crypto",
            segment_type="focused_fit_category",
            records=crypto_records,
            threshold=threshold,
        ),
        "by_calibration_segment": grouped_rows("trend_calibration_segment"),
        "by_time_to_close_bucket": grouped_rows("time_to_close_bucket"),
        "by_crypto_asset": grouped_rows("crypto_asset"),
        "by_signal_tier": grouped_rows("direction_signal_tier"),
        "by_whale_pressure_direction": grouped_rows("whale_pressure_direction"),
    }


def _crypto_absolute_move_audit_row(
    *,
    segment: str,
    segment_type: str,
    records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Return crypto absolute-move review diagnostics for one segment."""
    overlay = _prediction_metrics(records, "overlay_blend_delta", threshold=threshold)
    absolute = _prediction_metrics(records, "absolute_move_delta", threshold=threshold)
    review_records = [record for record in records if str(record.get("absolute_move_display_tier")) == "review"]
    show_records = [record for record in records if str(record.get("absolute_move_display_tier")) == "show"]
    hidden_records = [record for record in records if str(record.get("absolute_move_display_tier")) == "hidden"]
    direction_misses = [
        record
        for record in records
        if _direction(float(record["actual_delta"]), threshold)
        != _direction(float(record["absolute_move_delta"]), threshold)
    ]
    overlay_direction_misses = [
        record
        for record in records
        if _direction(float(record["actual_delta"]), threshold)
        != _direction(float(record["overlay_blend_delta"]), threshold)
    ]
    return {
        "segment": segment,
        "segment_type": segment_type,
        "row_count": len(records),
        "show_row_count": len(show_records),
        "show_row_pct": _safe_ratio(len(show_records), len(records)),
        "review_row_count": len(review_records),
        "review_row_pct": _safe_ratio(len(review_records), len(records)),
        "hidden_row_count": len(hidden_records),
        "hidden_row_pct": _safe_ratio(len(hidden_records), len(records)),
        "mae_pts": absolute["mae_pts"],
        "overlay_mae_pts": overlay["mae_pts"],
        "mae_delta_pts": _round(float(absolute["mae_pts"]) - float(overlay["mae_pts"]), 4),
        "direction_match_pct": absolute["direction_match_pct"],
        "overlay_direction_match_pct": overlay["direction_match_pct"],
        "direction_match_delta_pts": _round(
            float(absolute["direction_match_pct"]) - float(overlay["direction_match_pct"]),
            4,
        ),
        "direction_miss_count": len(direction_misses),
        "direction_miss_pct": _safe_ratio(len(direction_misses), len(records)),
        "overlay_direction_miss_count": len(overlay_direction_misses),
        "overlay_direction_miss_pct": _safe_ratio(len(overlay_direction_misses), len(records)),
        "average_abs_actual_delta_pts": absolute["average_abs_actual_delta_pts"],
        "average_abs_predicted_delta_pts": absolute["average_abs_predicted_delta_pts"],
        "overlay_average_abs_predicted_delta_pts": overlay["average_abs_predicted_delta_pts"],
        "underprediction_pct": absolute["underprediction_pct"],
        "overlay_underprediction_pct": overlay["underprediction_pct"],
        "display_tier_counts": dict(
            sorted(Counter(str(record.get("absolute_move_display_tier") or "hidden") for record in records).items())
        ),
        "review_reason_counts": dict(
            sorted(
                Counter(
                    reason
                    for record in review_records
                    for reason in list(record.get("absolute_move_review_reasons") or [])
                ).items()
            )
        ),
        "display_reason_counts": dict(
            sorted(
                Counter(
                    reason
                    for record in records
                    for reason in list(record.get("absolute_move_display_reasons") or [])
                ).items()
            )
        ),
        "absolute_direction_counts": dict(
            sorted(Counter(str(record.get("absolute_move_direction") or "flat") for record in records).items())
        ),
        "overlay_direction_counts": dict(
            sorted(Counter(str(record.get("overlay_blend_direction") or "flat") for record in records).items())
        ),
        "actual_direction_counts": dict(sorted(Counter(str(record.get("actual_direction")) for record in records).items())),
        "absolute_direction_source_counts": dict(
            sorted(Counter(str(record.get("absolute_move_direction_source") or "unknown") for record in records).items())
        ),
        "signal_tier_counts": dict(sorted(Counter(str(record.get("direction_signal_tier") or "missing") for record in records).items())),
        "whale_pressure_direction_counts": dict(
            sorted(Counter(str(record.get("whale_pressure_direction") or "neutral") for record in records).items())
        ),
    }


def _crypto_absolute_move_review_audit(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return crypto absolute-move review-tier diagnostics."""
    crypto_records = [record for record in records if str(record.get("focused_fit_category")) == "crypto"]
    if not crypto_records:
        return {
            "row_count": 0,
            "summary": None,
            "by_crypto_asset": [],
            "by_time_to_close_bucket": [],
            "by_whale_pressure_direction": [],
            "by_signal_tier": [],
            "by_absolute_direction_source": [],
        }

    def grouped_rows(group_key: str) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in crypto_records:
            grouped.setdefault(str(record.get(group_key) or "unknown"), []).append(record)
        return [
            _crypto_absolute_move_audit_row(
                segment=segment,
                segment_type=group_key,
                records=rows,
                threshold=threshold,
            )
            for segment, rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
        ]

    grouped_by_source: dict[str, list[dict[str, Any]]] = {}
    for record in crypto_records:
        grouped_by_source.setdefault(str(record.get("absolute_move_direction_source") or "unknown"), []).append(record)

    return {
        "row_count": len(crypto_records),
        "summary": _crypto_absolute_move_audit_row(
            segment="crypto",
            segment_type="focused_fit_category",
            records=crypto_records,
            threshold=threshold,
        ),
        "by_crypto_asset": grouped_rows("crypto_asset"),
        "by_time_to_close_bucket": grouped_rows("time_to_close_bucket"),
        "by_whale_pressure_direction": grouped_rows("whale_pressure_direction"),
        "by_signal_tier": grouped_rows("direction_signal_tier"),
        "by_absolute_direction_source": [
            _crypto_absolute_move_audit_row(
                segment=segment,
                segment_type="absolute_move_direction_source",
                records=rows,
                threshold=threshold,
            )
            for segment, rows in sorted(grouped_by_source.items(), key=lambda item: (-len(item[1]), item[0]))
        ],
    }


def _crypto_promoted_example(record: dict[str, Any]) -> dict[str, Any]:
    """Return a compact promoted crypto example for audit payloads."""
    row = record["row"]
    return {
        "market_slug": str(row.get("market_slug") or ""),
        "question": str(row.get("question") or ""),
        "side_label": str(row.get("side_label") or ""),
        "observation_time": str(row.get("observation_time") or ""),
        "crypto_asset": str(record.get("crypto_asset") or ""),
        "market_family": str(record.get("market_family") or ""),
        "whale_entry_timing_bucket": str(record.get("whale_entry_timing_bucket") or ""),
        "actual_direction": str(record.get("actual_direction") or "flat"),
        "predicted_direction": str(record.get("absolute_move_direction") or "flat"),
        "actual_delta_pts": _pct(float(record.get("actual_delta") or 0.0)),
        "absolute_move_delta_pts": _pct(float(record.get("absolute_move_delta") or 0.0)),
        "overlay_blend_delta_pts": _pct(float(record.get("overlay_blend_delta") or 0.0)),
        "absolute_move_abs_error_pts": _pct(float(record.get("absolute_move_abs_error") or 0.0)),
        "overlay_blend_abs_error_pts": _pct(float(record.get("overlay_blend_abs_error") or 0.0)),
        "absolute_move_direction_source": str(record.get("absolute_move_direction_source") or ""),
        "source_selector_source": str(record.get("crypto_direction_source_selector_source") or ""),
        "source_selector_scope": str(record.get("crypto_direction_source_selector_scope") or ""),
        "source_selector_reason": str(record.get("crypto_direction_source_selector_reason") or ""),
        "gate_scope": str(record.get("crypto_segment_direction_gate_scope") or ""),
        "gate_prior_rows": int(record.get("crypto_segment_direction_gate_prior_rows") or 0),
        "gate_direction_match_pct": _round(
            float(record.get("crypto_segment_direction_gate_direction_match_pct") or 0.0),
            4,
        ),
    }


def _crypto_promoted_row_precision_audit(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return precision diagnostics for crypto rows promoted from Review to Show."""
    promoted = [
        record
        for record in records
        if str(record.get("focused_fit_category")) == "crypto"
        and str(record.get("crypto_segment_direction_gate_reason")) == "prior_segment_direction_safe"
        and str(record.get("absolute_move_display_tier")) == "show"
    ]
    if not promoted:
        return {
            "row_count": 0,
            "precision_pct": 0.0,
            "false_show_count": 0,
            "false_show_pct": 0.0,
            "metrics": {},
            "overlay_metrics": {},
            "source_selector_applied_row_count": 0,
            "source_selector_applied_row_pct": 0.0,
            "false_show_examples": [],
            "best_show_examples": [],
            "policy": {
                "definition": "crypto rows promoted from Review to Show by prior segment direction gate",
            },
        }
    metrics = _prediction_metrics(promoted, "absolute_move_delta", threshold=threshold)
    overlay = _prediction_metrics(promoted, "overlay_blend_delta", threshold=threshold)
    false_rows = [
        record
        for record in promoted
        if _direction(float(record["actual_delta"]), threshold)
        != _direction(float(record["absolute_move_delta"]), threshold)
    ]
    applied_selector = [record for record in promoted if bool(record.get("crypto_direction_source_selector_applied"))]
    best_rows = sorted(
        promoted,
        key=lambda record: float(record.get("overlay_blend_abs_error") or 0.0)
        - float(record.get("absolute_move_abs_error") or 0.0),
        reverse=True,
    )
    return {
        "row_count": len(promoted),
        "precision_pct": metrics["direction_match_pct"],
        "false_show_count": len(false_rows),
        "false_show_pct": _safe_ratio(len(false_rows), len(promoted)),
        "metrics": metrics,
        "overlay_metrics": overlay,
        "mae_delta_pts": _round(float(metrics["mae_pts"]) - float(overlay["mae_pts"]), 4),
        "direction_match_delta_pts": _round(
            float(metrics["direction_match_pct"]) - float(overlay["direction_match_pct"]),
            4,
        ),
        "source_selector_applied_row_count": len(applied_selector),
        "source_selector_applied_row_pct": _safe_ratio(len(applied_selector), len(promoted)),
        "asset_counts": dict(sorted(Counter(str(record.get("crypto_asset") or "unknown") for record in promoted).items())),
        "entry_timing_counts": dict(
            sorted(Counter(str(record.get("whale_entry_timing_bucket") or "unknown") for record in promoted).items())
        ),
        "direction_source_counts": dict(
            sorted(Counter(str(record.get("absolute_move_direction_source") or "unknown") for record in promoted).items())
        ),
        "source_selector_source_counts": dict(
            sorted(Counter(str(record.get("crypto_direction_source_selector_source") or "unknown") for record in promoted).items())
        ),
        "gate_scope_counts": dict(
            sorted(Counter(str(record.get("crypto_segment_direction_gate_scope") or "unknown") for record in promoted).items())
        ),
        "false_show_examples": [
            _crypto_promoted_example(record)
            for record in sorted(false_rows, key=lambda item: float(item.get("absolute_move_abs_error") or 0.0), reverse=True)[:6]
        ],
        "best_show_examples": [_crypto_promoted_example(record) for record in best_rows[:6]],
        "policy": {
            "definition": "crypto rows promoted from Review to Show by prior segment direction gate",
            "false_show": "absolute-move direction differs from actual 12h/24h movement direction",
        },
    }


def _direction_label_alignment(records: list[dict[str, Any]], label_key: str) -> dict[str, Any]:
    """Return how often a non-flat direction label agrees with actual movement."""
    labeled = [
        record
        for record in records
        if str(record.get(label_key) or "flat") in {"up", "down"}
        and str(record.get("actual_direction") or "flat") in {"up", "down"}
    ]
    aligned = [
        record
        for record in labeled
        if str(record.get(label_key) or "flat") == str(record.get("actual_direction") or "flat")
    ]
    return {
        "row_count": len(labeled),
        "row_pct": _safe_ratio(len(labeled), len(records)),
        "alignment_pct": _safe_ratio(len(aligned), len(labeled)),
    }


def _strong_watch_direction_alignment(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return Strong/Watch direction alignment for one diagnostic segment."""
    supported = [
        record
        for record in records
        if str(record.get("direction_signal_tier") or "") in SURFACED_DIRECTION_TIERS
        and str(record.get("direction_signal_predicted_direction") or "flat") in {"up", "down"}
        and str(record.get("actual_direction") or "flat") in {"up", "down"}
    ]
    aligned = [
        record
        for record in supported
        if str(record.get("direction_signal_predicted_direction")) == str(record.get("actual_direction"))
    ]
    return {
        "row_count": len(supported),
        "row_pct": _safe_ratio(len(supported), len(records)),
        "alignment_pct": _safe_ratio(len(aligned), len(supported)),
    }


def _crypto_direction_split_recommendation(
    *,
    row_count: int,
    overlay_direction_match_pct: float,
    absolute_direction_match_pct: float,
    strong_watch_row_count: int,
    strong_watch_alignment_pct: float,
    whale_pressure_alignment_pct: float,
) -> tuple[str, str]:
    """Return a diagnostic recommendation for a crypto direction split."""
    if row_count < CRYPTO_DIRECTION_SPLIT_MIN_ROWS:
        return "thin_data", "below_min_segment_rows"
    if strong_watch_row_count >= 5 and strong_watch_alignment_pct >= 80.0:
        return "prefer_strong_watch_direction", "strong_watch_direction_is_cleanest"
    if absolute_direction_match_pct >= overlay_direction_match_pct + 3.0:
        return "review_absolute_direction", "absolute_move_direction_beats_overlay"
    if overlay_direction_match_pct >= 70.0:
        return "overlay_direction_ok", "overlay_direction_clears_segment_bar"
    if whale_pressure_alignment_pct < 50.0:
        return "review_or_suppress", "whale_pressure_often_opposes_actual"
    return "monitor", "mixed_direction_edge"


def _crypto_direction_split_row(
    *,
    segment: str,
    segment_type: str,
    records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Return one crypto asset/style direction split row."""
    overlay = _prediction_metrics(records, "overlay_blend_delta", threshold=threshold)
    absolute = _prediction_metrics(records, "absolute_move_delta", threshold=threshold)
    signal = _strong_watch_direction_alignment(records)
    pressure = _direction_label_alignment(records, "whale_pressure_direction")
    action, reason = _crypto_direction_split_recommendation(
        row_count=len(records),
        overlay_direction_match_pct=float(overlay["direction_match_pct"]),
        absolute_direction_match_pct=float(absolute["direction_match_pct"]),
        strong_watch_row_count=int(signal["row_count"]),
        strong_watch_alignment_pct=float(signal["alignment_pct"]),
        whale_pressure_alignment_pct=float(pressure["alignment_pct"]),
    )
    return {
        "segment": segment,
        "segment_type": segment_type,
        "row_count": len(records),
        "condition_count": len({str(record.get("condition_ref") or "") for record in records}),
        "overlay_direction_match_pct": overlay["direction_match_pct"],
        "absolute_move_direction_match_pct": absolute["direction_match_pct"],
        "absolute_move_direction_delta_pts": _round(
            float(absolute["direction_match_pct"]) - float(overlay["direction_match_pct"]),
            4,
        ),
        "overlay_mae_pts": overlay["mae_pts"],
        "absolute_move_mae_pts": absolute["mae_pts"],
        "absolute_move_mae_delta_pts": _round(float(absolute["mae_pts"]) - float(overlay["mae_pts"]), 4),
        "average_abs_actual_delta_pts": overlay["average_abs_actual_delta_pts"],
        "overlay_average_abs_predicted_delta_pts": overlay["average_abs_predicted_delta_pts"],
        "absolute_move_average_abs_predicted_delta_pts": absolute["average_abs_predicted_delta_pts"],
        "overlay_underprediction_pct": overlay["underprediction_pct"],
        "absolute_move_underprediction_pct": absolute["underprediction_pct"],
        "strong_watch_row_count": signal["row_count"],
        "strong_watch_row_pct": signal["row_pct"],
        "strong_watch_direction_alignment_pct": signal["alignment_pct"],
        "whale_pressure_row_count": pressure["row_count"],
        "whale_pressure_row_pct": pressure["row_pct"],
        "whale_pressure_alignment_pct": pressure["alignment_pct"],
        "actual_direction_counts": dict(sorted(Counter(str(record.get("actual_direction")) for record in records).items())),
        "overlay_direction_counts": dict(
            sorted(Counter(str(record.get("overlay_blend_direction") or "flat") for record in records).items())
        ),
        "absolute_direction_counts": dict(
            sorted(Counter(str(record.get("absolute_move_direction") or "flat") for record in records).items())
        ),
        "signal_tier_counts": dict(sorted(Counter(str(record.get("direction_signal_tier") or "missing") for record in records).items())),
        "whale_entry_timing_counts": dict(
            sorted(Counter(str(record.get("whale_entry_timing_bucket") or "unknown") for record in records).items())
        ),
        "whale_flow_timing_counts": dict(
            sorted(Counter(str(record.get("whale_flow_timing_bucket") or "unknown") for record in records).items())
        ),
        "recommended_action": action,
        "recommended_reason": reason,
    }


def _grouped_diagnostic_rows(
    records: list[dict[str, Any]],
    *,
    group_key: str,
    row_builder: Any,
    threshold: float,
) -> list[dict[str, Any]]:
    """Return diagnostic rows grouped by a record field."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(group_key) or "unknown"), []).append(record)
    return [
        row_builder(segment=segment, segment_type=group_key, records=rows, threshold=threshold)
        for segment, rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0]))
    ]


def _crypto_direction_split_diagnostics(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return crypto direction diagnostics by asset, style, and whale timing."""
    crypto_records = [record for record in records if str(record.get("focused_fit_category")) == "crypto"]
    if not crypto_records:
        return {
            "row_count": 0,
            "summary": None,
            "by_crypto_asset": [],
            "by_market_family": [],
            "by_asset_market_family": [],
            "by_entry_timing_bucket": [],
            "by_flow_timing_bucket": [],
            "best_segments": [],
            "weakest_segments": [],
            "policy": {
                "min_segment_rows": CRYPTO_DIRECTION_SPLIT_MIN_ROWS,
                "diagnostic_only": True,
            },
        }

    asset_family_records: dict[str, list[dict[str, Any]]] = {}
    for record in crypto_records:
        segment = f"{record.get('crypto_asset') or 'unknown'}__{record.get('market_family') or 'unknown'}"
        asset_family_records.setdefault(segment, []).append(record)
    by_asset_family = [
        _crypto_direction_split_row(
            segment=segment,
            segment_type="crypto_asset_market_family",
            records=rows,
            threshold=threshold,
        )
        for segment, rows in sorted(asset_family_records.items(), key=lambda item: (-len(item[1]), item[0]))
    ]
    eligible_segments = [
        row
        for row in by_asset_family
        if int(row.get("row_count") or 0) >= CRYPTO_DIRECTION_SPLIT_MIN_ROWS
    ]
    return {
        "row_count": len(crypto_records),
        "summary": _crypto_direction_split_row(
            segment="crypto",
            segment_type="focused_fit_category",
            records=crypto_records,
            threshold=threshold,
        ),
        "by_crypto_asset": _grouped_diagnostic_rows(
            crypto_records,
            group_key="crypto_asset",
            row_builder=_crypto_direction_split_row,
            threshold=threshold,
        ),
        "by_market_family": _grouped_diagnostic_rows(
            crypto_records,
            group_key="market_family",
            row_builder=_crypto_direction_split_row,
            threshold=threshold,
        ),
        "by_asset_market_family": by_asset_family,
        "by_entry_timing_bucket": _grouped_diagnostic_rows(
            crypto_records,
            group_key="whale_entry_timing_bucket",
            row_builder=_crypto_direction_split_row,
            threshold=threshold,
        ),
        "by_flow_timing_bucket": _grouped_diagnostic_rows(
            crypto_records,
            group_key="whale_flow_timing_bucket",
            row_builder=_crypto_direction_split_row,
            threshold=threshold,
        ),
        "best_segments": sorted(
            eligible_segments,
            key=lambda row: (
                float(row.get("overlay_direction_match_pct") or 0.0),
                int(row.get("row_count") or 0),
            ),
            reverse=True,
        )[:5],
        "weakest_segments": sorted(
            eligible_segments,
            key=lambda row: (
                float(row.get("overlay_direction_match_pct") or 0.0),
                -int(row.get("row_count") or 0),
            ),
        )[:5],
        "policy": {
            "min_segment_rows": CRYPTO_DIRECTION_SPLIT_MIN_ROWS,
            "diagnostic_only": True,
            "segment_order": [
                "crypto_asset",
                "market_family",
                "crypto_asset_market_family",
                "whale_entry_timing_bucket",
                "whale_flow_timing_bucket",
            ],
            "objective": "identify crypto direction slices before changing gates or direction source selection",
        },
    }


def _whale_timing_diagnostic_row(
    *,
    segment: str,
    segment_type: str,
    records: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    """Return direction/magnitude diagnostics for a whale timing bucket."""
    overlay = _prediction_metrics(records, "overlay_blend_delta", threshold=threshold)
    absolute = _prediction_metrics(records, "absolute_move_delta", threshold=threshold)
    signal = _strong_watch_direction_alignment(records)
    pressure = _direction_label_alignment(records, "whale_pressure_direction")
    return {
        "segment": segment,
        "segment_type": segment_type,
        "row_count": len(records),
        "condition_count": len({str(record.get("condition_ref") or "") for record in records}),
        "overlay_direction_match_pct": overlay["direction_match_pct"],
        "absolute_move_direction_match_pct": absolute["direction_match_pct"],
        "absolute_move_direction_delta_pts": _round(
            float(absolute["direction_match_pct"]) - float(overlay["direction_match_pct"]),
            4,
        ),
        "overlay_mae_pts": overlay["mae_pts"],
        "absolute_move_mae_pts": absolute["mae_pts"],
        "absolute_move_mae_delta_pts": _round(float(absolute["mae_pts"]) - float(overlay["mae_pts"]), 4),
        "average_abs_actual_delta_pts": overlay["average_abs_actual_delta_pts"],
        "overlay_average_abs_predicted_delta_pts": overlay["average_abs_predicted_delta_pts"],
        "absolute_move_average_abs_predicted_delta_pts": absolute["average_abs_predicted_delta_pts"],
        "underprediction_pct": overlay["underprediction_pct"],
        "absolute_move_underprediction_pct": absolute["underprediction_pct"],
        "strong_watch_row_count": signal["row_count"],
        "strong_watch_direction_alignment_pct": signal["alignment_pct"],
        "whale_pressure_row_count": pressure["row_count"],
        "whale_pressure_alignment_pct": pressure["alignment_pct"],
        "focused_category_counts": dict(
            sorted(Counter(str(record.get("focused_fit_category") or "other") for record in records).items())
        ),
        "actual_direction_counts": dict(sorted(Counter(str(record.get("actual_direction")) for record in records).items())),
        "overlay_direction_counts": dict(
            sorted(Counter(str(record.get("overlay_blend_direction") or "flat") for record in records).items())
        ),
        "signal_tier_counts": dict(sorted(Counter(str(record.get("direction_signal_tier") or "missing") for record in records).items())),
    }


def _whale_timing_direction_diagnostics(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return direction diagnostics by whale entry/flow recency bucket."""
    if not records:
        return {
            "row_count": 0,
            "by_entry_timing_bucket": [],
            "by_flow_timing_bucket": [],
            "crypto_by_entry_timing_bucket": [],
            "crypto_by_flow_timing_bucket": [],
            "policy": {"diagnostic_only": True},
        }
    crypto_records = [record for record in records if str(record.get("focused_fit_category")) == "crypto"]
    return {
        "row_count": len(records),
        "by_entry_timing_bucket": _grouped_diagnostic_rows(
            records,
            group_key="whale_entry_timing_bucket",
            row_builder=_whale_timing_diagnostic_row,
            threshold=threshold,
        ),
        "by_flow_timing_bucket": _grouped_diagnostic_rows(
            records,
            group_key="whale_flow_timing_bucket",
            row_builder=_whale_timing_diagnostic_row,
            threshold=threshold,
        ),
        "crypto_by_entry_timing_bucket": _grouped_diagnostic_rows(
            crypto_records,
            group_key="whale_entry_timing_bucket",
            row_builder=_whale_timing_diagnostic_row,
            threshold=threshold,
        ),
        "crypto_by_flow_timing_bucket": _grouped_diagnostic_rows(
            crypto_records,
            group_key="whale_flow_timing_bucket",
            row_builder=_whale_timing_diagnostic_row,
            threshold=threshold,
        ),
        "policy": {
            "diagnostic_only": True,
            "bucket_source": "existing cumulative recent whale-side features",
            "entry_buckets": [
                "entry_0_1h",
                "entry_1_6h",
                "entry_6_12h",
                "entry_12_24h",
                "entry_24h_plus",
                "no_recent_entry",
            ],
            "flow_buckets": [
                "entry_0_1h",
                "exit_0_1h",
                "mixed_0_1h",
                "entry_1_6h",
                "exit_1_6h",
                "mixed_1_6h",
                "entry_6_12h",
                "exit_6_12h",
                "mixed_6_12h",
                "entry_12_24h",
                "exit_12_24h",
                "mixed_12_24h",
                "flow_24h_plus",
                "no_recent_flow",
            ],
        },
    }


def _trajectory_fit_summary(records: list[dict[str, Any]], *, threshold: float, window_name: str) -> dict[str, Any]:
    """Return endpoint-line path diagnostics for one prediction window."""
    if not records:
        return {
            "row_count": 0,
            "path_source": "endpoint_line_proxy",
            "correlation": 0.0,
            "signed_area_error_pts": 0.0,
            "average_path_mae_pts": 0.0,
            "pre_calibration_path_mae_pts": 0.0,
            "path_mae_delta_pts": 0.0,
            "direction_match_pct": 0.0,
        }
    actuals = [float(record["actual_delta"]) for record in records]
    predictions = [float(record["overlay_blend_delta"]) for record in records]
    base_predictions = [float(record["overlay_base_blend_delta"]) for record in records]
    post_path = sum(_line_path_mae_pts(actual, predicted) for actual, predicted in zip(actuals, predictions, strict=True)) / len(records)
    pre_path = sum(
        _line_path_mae_pts(actual, predicted)
        for actual, predicted in zip(actuals, base_predictions, strict=True)
    ) / len(records)
    direction_matches = sum(
        1
        for actual, predicted in zip(actuals, predictions, strict=True)
        if _direction(actual, threshold) == _direction(predicted, threshold)
    )
    return {
        "row_count": len(records),
        "window": window_name,
        "path_source": "endpoint_line_proxy",
        "correlation": _safe_correlation(actuals, predictions),
        "pre_calibration_correlation": _safe_correlation(actuals, base_predictions),
        "signed_area_error_pts": _pct(
            sum(predicted - actual for actual, predicted in zip(actuals, predictions, strict=True)) / len(records) / 2.0
        ),
        "average_path_mae_pts": _round(post_path, 4),
        "pre_calibration_path_mae_pts": _round(pre_path, 4),
        "path_mae_delta_pts": _round(post_path - pre_path, 4),
        "direction_match_pct": _safe_ratio(direction_matches, len(records)),
    }


def _calibration_candidate_backtests(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return aggregate train-safe candidate outcomes tested for row-level calibration."""
    grouped: dict[tuple[str, str], list[tuple[dict[str, Any], bool]]] = {}
    for record in records:
        selected_source = str(record.get("selected_calibration_source") or "identity")
        if selected_source.startswith("esports_review_"):
            selected_source = selected_source[len("esports_review_") :]
        selected_key = (
            selected_source,
            str(record.get("selected_calibration_method") or "identity"),
        )
        for candidate in list(record.get("trend_fit_candidate_outcomes") or []):
            key = (str(candidate.get("source") or "unknown"), str(candidate.get("method") or "identity"))
            grouped.setdefault(key, []).append((candidate, key == selected_key))

    rows: list[dict[str, Any]] = []
    for (source, method), entries in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        candidates = [entry[0] for entry in entries]
        selected_count = sum(1 for _, selected in entries if selected)
        rows.append(
            {
                "source": source,
                "method": method,
                "candidate_row_count": len(candidates),
                "selected_row_count": selected_count,
                "selected_row_pct": _safe_ratio(selected_count, len(candidates)),
                "average_train_rows": _round(
                    sum(float(candidate.get("train_rows") or 0) for candidate in candidates) / len(candidates),
                    2,
                ),
                "average_train_mae_delta_pts": _round(
                    sum(float(candidate.get("train_mae_delta_pts") or 0.0) for candidate in candidates) / len(candidates),
                    4,
                ),
                "average_train_direction_delta_pts": _round(
                    sum(float(candidate.get("train_direction_delta_pts") or 0.0) for candidate in candidates)
                    / len(candidates),
                    4,
                ),
                "pass_train_guard_pct": _safe_ratio(
                    sum(1 for candidate in candidates if bool(candidate.get("passes_train_guard"))),
                    len(candidates),
                ),
            }
        )
    return rows


def _selected_trend_fit_policy(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return report-visible selected calibration policy and coverage."""
    method_counts = Counter(str(record.get("selected_calibration_method") or "identity") for record in records)
    source_counts = Counter(str(record.get("selected_calibration_source") or "identity") for record in records)
    changed = [
        record
        for record in records
        if str(record.get("selected_calibration_method") or "identity") != "identity"
        and str(record.get("magnitude_fit_tier") or "") in {"applied", "review_only"}
    ]
    return {
        "objective": "lower_gated_mae_without_direction_or_gate_regression",
        "candidate_order": [
            "calibration_segment_direction",
            "calibration_segment",
            "event_category_direction",
            "event_category",
            "market_family_in_focused_category_direction",
            "market_family_in_focused_category",
            "focused_fit_category_direction",
            "focused_fit_category",
            "global",
        ],
        "candidate_methods": [
            "identity",
            "magnitude_scale",
            "direction_conditioned_magnitude",
            "signed_bias",
            "slope_intercept",
        ],
        "uses_prior_out_of_sample_folds_only": True,
        "requires_strong_or_watch_direction_support": True,
        "direction_first_calibration": True,
        "method_counts": dict(sorted(method_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "changed_row_count": len(changed),
        "changed_row_pct": _safe_ratio(len(changed), len(records)),
    }


def _gated_mae_before_after(records: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    """Return before/after MAE for currently surfaced overlay rows."""
    gated_records = [record for record in records if bool(record.get("overlay_gate_allowed"))]
    if not gated_records:
        return {
            "row_count": 0,
            "pre_calibration_mae_pts": 0.0,
            "post_calibration_mae_pts": 0.0,
            "mae_delta_pts": 0.0,
            "direction_match_pct": 0.0,
        }
    before = _prediction_metrics(gated_records, "overlay_base_blend_delta", threshold=threshold)
    after = _prediction_metrics(gated_records, "overlay_blend_delta", threshold=threshold)
    return {
        "row_count": len(gated_records),
        "pre_calibration_mae_pts": before["mae_pts"],
        "post_calibration_mae_pts": after["mae_pts"],
        "mae_delta_pts": _round(float(after["mae_pts"]) - float(before["mae_pts"]), 4),
        "direction_match_pct": after["direction_match_pct"],
        "pre_calibration_direction_match_pct": before["direction_match_pct"],
    }


def _calibration_policy() -> dict[str, Any]:
    """Return the report-visible constrained calibration policy."""
    return {
        "objective": "gated_mae_no_reliability_regressions",
        "uses_prior_out_of_sample_folds_only": True,
        "scale_candidates": list(OVERLAY_MAGNITUDE_SCALE_CANDIDATES),
        "scale_shrinkage": OVERLAY_MAGNITUDE_SCALE_SHRINKAGE,
        "scale_min": OVERLAY_MAGNITUDE_SCALE_MIN,
        "scale_max": OVERLAY_MAGNITUDE_SCALE_MAX,
        "esports_review_scale_max": ESPORTS_REVIEW_OVERLAY_SCALE_MAX,
        "candidate_methods": [
            "identity",
            "magnitude_scale",
            "direction_conditioned_magnitude",
            "signed_bias",
            "slope_intercept",
        ],
        "candidate_order": [
            "calibration_segment_direction",
            "calibration_segment",
            "event_category_direction",
            "event_category",
            "market_family_in_focused_category_direction",
            "market_family_in_focused_category",
            "focused_fit_category_direction",
            "focused_fit_category",
            "global",
        ],
        "bias_cap_pts": _pct(TREND_FIT_BIAS_CAP),
        "intercept_cap_pts": _pct(TREND_FIT_INTERCEPT_CAP),
        "slope_min": TREND_FIT_SLOPE_MIN,
        "slope_max": TREND_FIT_SLOPE_MAX,
        "protected_fit_categories": sorted(PROTECTED_FIT_CATEGORIES),
        "review_only_fit_categories": sorted(REVIEW_ONLY_FIT_CATEGORIES),
        "gate_policy": {
            "min_rows": OVERLAY_GATE_MIN_ROWS,
            "min_direction_match_pct": OVERLAY_GATE_MIN_DIRECTION_MATCH_PCT,
            "max_underprediction_pct": OVERLAY_GATE_MAX_UNDERPREDICTION_PCT,
            "max_interval_width_pts": OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS,
            "min_band_coverage_pct": OVERLAY_GATE_MIN_BAND_COVERAGE_PCT,
        },
    }


def _case_payload(record: dict[str, Any]) -> dict[str, Any]:
    """Return compact dashboard case details."""
    row = record["row"]
    current_odds = float(record["current_odds"])
    return {
        "window": record["window"],
        "market_slug": str(row["market_slug"]),
        "question": str(row.get("question") or ""),
        "side_label": str(row["side_label"]),
        "observation_time": str(row["observation_time"]),
        "event_category": str(row.get("event_category") or "uncategorized"),
        "focus_category": str(record["focus_category"]),
        "focused_fit_category": str(record.get("focused_fit_category") or record["focus_category"]),
        "market_family": _market_family_segment(row),
        "trend_calibration_segment": str(record.get("trend_calibration_segment") or _trend_calibration_segment(row)),
        "crypto_asset": str(record.get("crypto_asset") or ""),
        "time_to_close_bucket": str(record.get("time_to_close_bucket") or _time_to_close_bucket(row)),
        "whale_entry_timing_bucket": str(record.get("whale_entry_timing_bucket") or _whale_entry_timing_bucket(row)),
        "whale_flow_timing_bucket": str(record.get("whale_flow_timing_bucket") or _whale_flow_timing_bucket(row)),
        "whale_pressure_direction": str(record.get("whale_pressure_direction") or "neutral"),
        "whale_pressure_value": _round(float(record.get("whale_pressure_value") or 0.0), 6),
        "research_focus": _research_focus_segment(row),
        "current_odds_pct": _pct(current_odds),
        "actual_future_odds_pct": _pct(_clip_probability(current_odds + float(record["actual_delta"]))),
        "residual_future_odds_pct": _pct(_clip_probability(current_odds + float(record["residual_delta"]))),
        "calibrated_future_odds_pct": _pct(_clip_probability(current_odds + float(record["calibrated_delta"]))),
        "blend_future_odds_pct": _pct(_clip_probability(current_odds + float(record["blend_delta"]))),
        "pair_normalized_blend_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["pair_normalized_blend_delta"]))
        ),
        "overlay_blend_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["overlay_blend_delta"]))
        ),
        "direction_confirmed_overlay_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["direction_confirmed_overlay_delta"]))
        ),
        "absolute_move_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["absolute_move_delta"]))
        ),
        "absolute_move_base_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["absolute_move_base_delta"]))
        ),
        "quantile_low_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["quantile_low_delta"]))
        ),
        "quantile_high_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["quantile_high_delta"]))
        ),
        "pair_normalized_quantile_low_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["pair_normalized_quantile_low_delta"]))
        ),
        "pair_normalized_quantile_high_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["pair_normalized_quantile_high_delta"]))
        ),
        "overlay_quantile_low_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["overlay_quantile_low_delta"]))
        ),
        "overlay_quantile_high_future_odds_pct": _pct(
            _clip_probability(current_odds + float(record["overlay_quantile_high_delta"]))
        ),
        "actual_delta_pts": _pct(float(record["actual_delta"])),
        "price_delta_pts": _pct(float(record["price_delta"])),
        "residual_delta_pts": _pct(float(record["residual_delta"])),
        "raw_delta_pts": _pct(float(record["raw_delta"])),
        "calibrated_delta_pts": _pct(float(record["calibrated_delta"])),
        "blend_delta_pts": _pct(float(record["blend_delta"])),
        "pair_normalized_blend_delta_pts": _pct(float(record["pair_normalized_blend_delta"])),
        "overlay_blend_delta_pts": _pct(float(record["overlay_blend_delta"])),
        "direction_confirmed_overlay_delta_pts": _pct(float(record["direction_confirmed_overlay_delta"])),
        "absolute_move_delta_pts": _pct(float(record["absolute_move_delta"])),
        "absolute_move_base_delta_pts": _pct(float(record["absolute_move_base_delta"])),
        "absolute_move_abs_prediction_pts": _pct(float(record["absolute_move_abs_prediction"])),
        "pre_calibration_overlay_delta_pts": _pct(float(record["overlay_base_blend_delta"])),
        "post_calibration_overlay_delta_pts": _pct(float(record["overlay_blend_delta"])),
        "quantile_low_delta_pts": _pct(float(record["quantile_low_delta"])),
        "quantile_high_delta_pts": _pct(float(record["quantile_high_delta"])),
        "pair_normalized_quantile_low_delta_pts": _pct(float(record["pair_normalized_quantile_low_delta"])),
        "pair_normalized_quantile_high_delta_pts": _pct(float(record["pair_normalized_quantile_high_delta"])),
        "overlay_quantile_low_delta_pts": _pct(float(record["overlay_quantile_low_delta"])),
        "overlay_quantile_high_delta_pts": _pct(float(record["overlay_quantile_high_delta"])),
        "residual_abs_error_pts": _pct(float(record["residual_abs_error"])),
        "calibrated_abs_error_pts": _pct(float(record["calibrated_abs_error"])),
        "blend_abs_error_pts": _pct(float(record["blend_abs_error"])),
        "pair_normalized_blend_abs_error_pts": _pct(float(record["pair_normalized_blend_abs_error"])),
        "overlay_blend_abs_error_pts": _pct(float(record["overlay_blend_abs_error"])),
        "direction_confirmed_overlay_abs_error_pts": _pct(
            float(record["direction_confirmed_overlay_abs_error"])
        ),
        "absolute_move_abs_error_pts": _pct(float(record["absolute_move_abs_error"])),
        "absolute_move_base_abs_error_pts": _pct(float(record["absolute_move_base_abs_error"])),
        "calibrated_error_improvement_vs_residual_pts": _pct(
            float(record["residual_abs_error"]) - float(record["calibrated_abs_error"])
        ),
        "blend_error_improvement_vs_residual_pts": _pct(
            float(record["residual_abs_error"]) - float(record["blend_abs_error"])
        ),
        "pair_normalized_error_improvement_vs_residual_pts": _pct(
            float(record["residual_abs_error"]) - float(record["pair_normalized_blend_abs_error"])
        ),
        "overlay_error_improvement_vs_residual_pts": _pct(
            float(record["residual_abs_error"]) - float(record["overlay_blend_abs_error"])
        ),
        "actual_direction": record["actual_direction"],
        "calibrated_direction": record["calibrated_direction"],
        "blend_direction": record["blend_direction"],
        "pair_normalized_blend_direction": record["pair_normalized_blend_direction"],
        "overlay_blend_direction": record["overlay_blend_direction"],
        "direction_confirmed_overlay_direction": record["direction_confirmed_overlay_direction"],
        "absolute_move_direction": record["absolute_move_direction"],
        "absolute_move_base_direction": record["absolute_move_base_direction"],
        "absolute_move_direction_source": str(record.get("absolute_move_direction_source") or ""),
        "absolute_move_base_direction_source": str(record.get("absolute_move_base_direction_source") or ""),
        "overlay_blend_source": record["overlay_blend_source"],
        "magnitude_fit_tier": str(record.get("magnitude_fit_tier") or "identity"),
        "magnitude_fit_reason": str(record.get("magnitude_fit_reason") or ""),
        "trend_fit_error_type": str(record.get("trend_fit_error_type") or _trend_fit_error_type(record, threshold=DEFAULT_NONFLAT_THRESHOLD)),
        "selected_calibration_source": str(record.get("selected_calibration_source") or "identity"),
        "selected_calibration_method": str(record.get("selected_calibration_method") or "identity"),
        "overlay_decision_summary": _overlay_decision_summary(record),
        "trend_shape_score": _round(float(record.get("trend_shape_score") or 0.0), 2),
        "pre_calibration_path_mae_pts": _round(float(record.get("pre_calibration_path_mae_pts") or 0.0), 4),
        "post_calibration_path_mae_pts": _round(float(record.get("post_calibration_path_mae_pts") or 0.0), 4),
        "overlay_magnitude_scale": _round(float(record.get("overlay_magnitude_scale") or 1.0), 4),
        "overlay_magnitude_scale_source": str(record.get("overlay_magnitude_scale_source") or ""),
        "overlay_magnitude_scale_train_rows": int(record.get("overlay_magnitude_scale_train_rows") or 0),
        "overlay_magnitude_scale_train_mae_pts": _round(
            float(record.get("overlay_magnitude_scale_train_mae_pts") or 0.0),
            4,
        ),
        "overlay_magnitude_scale_base_train_mae_pts": _round(
            float(record.get("overlay_magnitude_scale_base_train_mae_pts") or 0.0),
            4,
        ),
        "direction_confirmed_overlay_allowed": bool(record.get("direction_confirmed_overlay_allowed")),
        "direction_confirmed_overlay_scale": _round(
            float(record.get("direction_confirmed_overlay_scale") or 1.0),
            4,
        ),
        "direction_confirmed_overlay_raw_scale": _round(
            float(record.get("direction_confirmed_overlay_raw_scale") or 1.0),
            4,
        ),
        "direction_confirmed_overlay_source": str(record.get("direction_confirmed_overlay_source") or ""),
        "direction_confirmed_overlay_support_source": str(
            record.get("direction_confirmed_overlay_support_source") or ""
        ),
        "direction_confirmed_overlay_reason": str(record.get("direction_confirmed_overlay_reason") or ""),
        "direction_confirmed_display_allowed": bool(record.get("direction_confirmed_display_allowed")),
        "direction_confirmed_display_reasons": list(record.get("direction_confirmed_display_reasons") or []),
        "direction_confirmed_display_gate_category": str(
            record.get("direction_confirmed_display_gate_category") or ""
        ),
        "direction_confirmed_display_mae_delta_pts": _round(
            float(record.get("direction_confirmed_display_mae_delta_pts") or 0.0),
            4,
        ),
        "direction_confirmed_display_direction_delta_pts": _round(
            float(record.get("direction_confirmed_display_direction_delta_pts") or 0.0),
            4,
        ),
        "direction_confirmed_overlay_train_rows": int(record.get("direction_confirmed_overlay_train_rows") or 0),
        "direction_confirmed_overlay_train_mae_pts": _round(
            float(record.get("direction_confirmed_overlay_train_mae_pts") or 0.0),
            4,
        ),
        "direction_confirmed_overlay_base_train_mae_pts": _round(
            float(record.get("direction_confirmed_overlay_base_train_mae_pts") or 0.0),
            4,
        ),
        "direction_confirmed_overlay_train_mae_delta_pts": _round(
            float(record.get("direction_confirmed_overlay_train_mae_delta_pts") or 0.0),
            4,
        ),
        "absolute_move_display_allowed": bool(record.get("absolute_move_display_allowed")),
        "absolute_move_review_allowed": bool(record.get("absolute_move_review_allowed")),
        "absolute_move_display_tier": str(record.get("absolute_move_display_tier") or "hidden"),
        "absolute_move_display_reasons": list(record.get("absolute_move_display_reasons") or []),
        "absolute_move_review_reasons": list(record.get("absolute_move_review_reasons") or []),
        "absolute_move_display_gate_category": str(record.get("absolute_move_display_gate_category") or ""),
        "absolute_move_display_mae_delta_pts": _round(
            float(record.get("absolute_move_display_mae_delta_pts") or 0.0),
            4,
        ),
        "absolute_move_display_direction_delta_pts": _round(
            float(record.get("absolute_move_display_direction_delta_pts") or 0.0),
            4,
        ),
        "absolute_move_direction_resolver_applied": bool(
            record.get("absolute_move_direction_resolver_applied")
        ),
        "absolute_move_direction_resolver_source": str(
            record.get("absolute_move_direction_resolver_source") or ""
        ),
        "absolute_move_direction_resolver_scope": str(
            record.get("absolute_move_direction_resolver_scope") or ""
        ),
        "absolute_move_direction_resolver_reason": str(
            record.get("absolute_move_direction_resolver_reason") or ""
        ),
        "absolute_move_direction_resolver_train_rows": int(
            record.get("absolute_move_direction_resolver_train_rows") or 0
        ),
        "absolute_move_direction_resolver_direction_delta_pts": _round(
            float(record.get("absolute_move_direction_resolver_direction_delta_pts") or 0.0),
            4,
        ),
        "absolute_move_direction_resolver_mae_delta_pts": _round(
            float(record.get("absolute_move_direction_resolver_mae_delta_pts") or 0.0),
            4,
        ),
        "crypto_direction_source_selector_applied": bool(record.get("crypto_direction_source_selector_applied")),
        "crypto_direction_source_selector_source": str(record.get("crypto_direction_source_selector_source") or ""),
        "crypto_direction_source_selector_scope": str(record.get("crypto_direction_source_selector_scope") or ""),
        "crypto_direction_source_selector_reason": str(record.get("crypto_direction_source_selector_reason") or ""),
        "crypto_direction_source_selector_prior_rows": int(record.get("crypto_direction_source_selector_prior_rows") or 0),
        "crypto_direction_source_selector_direction_match_pct": _round(
            float(record.get("crypto_direction_source_selector_direction_match_pct") or 0.0),
            4,
        ),
        "crypto_direction_source_selector_direction_delta_pts": _round(
            float(record.get("crypto_direction_source_selector_direction_delta_pts") or 0.0),
            4,
        ),
        "crypto_direction_source_selector_mae_delta_pts": _round(
            float(record.get("crypto_direction_source_selector_mae_delta_pts") or 0.0),
            4,
        ),
        "crypto_direction_source_selector_previous_source": str(
            record.get("crypto_direction_source_selector_previous_source") or ""
        ),
        "crypto_segment_direction_gate_tier": str(record.get("crypto_segment_direction_gate_tier") or ""),
        "crypto_segment_direction_gate_reason": str(record.get("crypto_segment_direction_gate_reason") or ""),
        "crypto_segment_direction_gate_scope": str(record.get("crypto_segment_direction_gate_scope") or ""),
        "crypto_segment_direction_gate_prior_rows": int(record.get("crypto_segment_direction_gate_prior_rows") or 0),
        "crypto_segment_direction_gate_direction_match_pct": _round(
            float(record.get("crypto_segment_direction_gate_direction_match_pct") or 0.0),
            4,
        ),
        "crypto_segment_direction_gate_mae_delta_pts": _round(
            float(record.get("crypto_segment_direction_gate_mae_delta_pts") or 0.0),
            4,
        ),
        "crypto_segment_direction_gate_strong_watch_alignment_pct": _round(
            float(record.get("crypto_segment_direction_gate_strong_watch_alignment_pct") or 0.0),
            4,
        ),
        "blend_alpha": _round(float(record["blend_alpha"]), 4),
        "segment_magnitude_scale": _round(float(record["segment_magnitude_scale"]), 4),
        "segment_magnitude_scale_source": str(record.get("segment_magnitude_scale_source") or ""),
        "segment_magnitude_scale_train_rows": int(record.get("segment_magnitude_scale_train_rows") or 0),
        "segment_magnitude_scale_category": str(record.get("segment_magnitude_scale_category") or ""),
        "segment_blend_alpha": _round(float(record["segment_blend_alpha"]), 4),
        "pair_normalized": bool(record.get("pair_normalized")),
        "pair_normalization_reason": str(record.get("pair_normalization_reason") or ""),
        "direction_signal_tier": str(record.get("direction_signal_tier") or "missing"),
        "direction_signal_tier_reason": str(record.get("direction_signal_tier_reason") or ""),
        "direction_signal_predicted_direction": str(record.get("direction_signal_predicted_direction") or "flat"),
        "direction_signal_confidence": _round(float(record.get("direction_signal_confidence") or 0.0), 6),
        "direction_signal_reliability_warnings": list(record.get("direction_signal_reliability_warnings") or []),
        "category_overlay_gate_allowed": bool(record.get("category_overlay_gate_allowed")),
        "category_overlay_gate_reasons": list(record.get("category_overlay_gate_reasons") or []),
        "category_gate_after_calibration": bool(record.get("category_overlay_gate_allowed")),
        "direction_tier_overlay_gate_allowed": bool(record.get("direction_tier_overlay_gate_allowed")),
        "direction_tier_overlay_gate_reasons": list(record.get("direction_tier_overlay_gate_reasons") or []),
        "overlay_candidate_tier": str(record.get("overlay_candidate_tier") or "suppressed"),
        "overlay_candidate_reason": str(record.get("overlay_candidate_reason") or ""),
        "overlay_gate_allowed": bool(record.get("overlay_gate_allowed")),
        "overlay_gate_reasons": list(record.get("overlay_gate_reasons") or []),
        "overlay_gate_segment_rows": int(record.get("overlay_gate_segment_rows") or 0),
        "interval_contains_actual": bool(record["interval_contains_actual"]),
        "pair_normalized_interval_contains_actual": bool(record["pair_normalized_interval_contains_actual"]),
        "overlay_interval_contains_actual": bool(record["overlay_interval_contains_actual"]),
        "whale_anchor": {
            "recent_trade_count_1h": _safe_float(row, "whale_side_recent_trade_count_1h"),
            "recent_entry_count_1h": _safe_float(row, "whale_side_recent_entry_trade_count_1h"),
            "recent_exit_count_1h": _safe_float(row, "whale_side_recent_exit_trade_count_1h"),
            "recent_weighted_net_pressure_1h": _safe_float(row, "whale_side_recent_weighted_net_pressure_1h"),
            "recent_trade_count_6h": _safe_float(row, "whale_side_recent_trade_count_6h"),
            "recent_entry_count_6h": _safe_float(row, "whale_side_recent_entry_trade_count_6h"),
            "recent_exit_count_6h": _safe_float(row, "whale_side_recent_exit_trade_count_6h"),
            "recent_weighted_net_pressure_6h": _safe_float(row, "whale_side_recent_weighted_net_pressure_6h"),
            "recent_trade_count_12h": _safe_float(row, "whale_side_recent_trade_count_12h"),
            "recent_trade_count_24h": _safe_float(row, "whale_side_recent_trade_count_24h"),
            "recent_entry_count_12h": _safe_float(row, "whale_side_recent_entry_trade_count_12h"),
            "recent_exit_count_12h": _safe_float(row, "whale_side_recent_exit_trade_count_12h"),
            "recent_weighted_net_pressure_12h": _safe_float(row, "whale_side_recent_weighted_net_pressure_12h"),
            "recent_entry_count_24h": _safe_float(row, "whale_side_recent_entry_trade_count_24h"),
            "recent_exit_count_24h": _safe_float(row, "whale_side_recent_exit_trade_count_24h"),
            "recent_weighted_net_pressure_24h": _safe_float(row, "whale_side_recent_weighted_net_pressure_24h"),
            "trusted_recent_trade_count_12h": _safe_float(row, "trusted_whale_side_recent_trade_count_12h"),
            "trusted_recent_weighted_net_pressure_12h": _safe_float(
                row,
                "trusted_whale_side_recent_weighted_net_pressure_12h",
            ),
        },
    }


def _case_samples(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Return example cases for dashboard review."""
    best = sorted(
        records,
        key=lambda record: (
            float(record["residual_abs_error"]) - float(record["pair_normalized_blend_abs_error"]),
            abs(float(record["actual_delta"])),
        ),
        reverse=True,
    )
    misses = sorted(
        records,
        key=lambda record: (
            float(record["pair_normalized_blend_abs_error"]) - float(record["residual_abs_error"]),
            abs(float(record["actual_delta"])),
        ),
        reverse=True,
    )
    high_magnitude = sorted(records, key=lambda record: abs(float(record["actual_delta"])), reverse=True)
    gated = [record for record in best if bool(record.get("overlay_gate_allowed"))]
    review = sorted(
        [record for record in records if str(record.get("overlay_candidate_tier")) == "review"],
        key=lambda record: (
            float(record.get("direction_signal_confidence") or 0.0),
            abs(float(record["actual_delta"])),
        ),
        reverse=True,
    )
    suppressed = [record for record in high_magnitude if not bool(record.get("overlay_gate_allowed"))]
    category_examples: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    categories = sorted({str(record.get("event_category") or "uncategorized") for record in records})
    priority = {"surfaced": 0, "review": 1, "suppressed": 2}
    for category in categories:
        category_records = sorted(
            [record for record in records if str(record.get("event_category") or "uncategorized") == category],
            key=lambda record: (
                priority.get(str(record.get("overlay_candidate_tier") or "suppressed"), 3),
                -abs(float(record["actual_delta"])),
                float(record["pair_normalized_blend_abs_error"]),
            ),
        )
        added = 0
        seen_markets: set[str] = set()
        fallback_records: list[dict[str, Any]] = []
        for record in category_records:
            key = (
                str(record["window"]),
                str(record["row"]["market_slug"]),
                str(record["row"]["side_label"]),
                str(record["row"]["observation_time"]),
            )
            if key in seen:
                continue
            market_slug = str(record["row"]["market_slug"])
            if market_slug in seen_markets:
                fallback_records.append(record)
                continue
            seen.add(key)
            seen_markets.add(market_slug)
            category_examples.append(_case_payload(record))
            added += 1
            if added >= 2:
                break
        for record in fallback_records:
            if added >= 2:
                break
            key = (
                str(record["window"]),
                str(record["row"]["market_slug"]),
                str(record["row"]["side_label"]),
                str(record["row"]["observation_time"]),
            )
            if key in seen:
                continue
            seen.add(key)
            category_examples.append(_case_payload(record))
            added += 1

    return {
        "best_calibrated_improvements": [_case_payload(record) for record in best[:8]],
        "largest_calibrated_misses": [_case_payload(record) for record in misses[:8]],
        "high_magnitude_examples": [_case_payload(record) for record in high_magnitude[:8]],
        "gated_overlay_examples": [_case_payload(record) for record in gated[:8]],
        "review_overlay_examples": [_case_payload(record) for record in review[:8]],
        "suppressed_overlay_examples": [_case_payload(record) for record in suppressed[:8]],
        "event_category_examples": category_examples,
    }


def _market_profile_prediction_case(record: dict[str, Any]) -> dict[str, Any]:
    """Return the compact local ML trend payload used by market profiles."""
    payload = _case_payload(record)
    return {
        "window": payload["window"],
        "market_slug": payload["market_slug"],
        "question": payload["question"],
        "side_label": payload["side_label"],
        "observation_time": payload["observation_time"],
        "event_category": payload["event_category"],
        "focus_category": payload["focus_category"],
        "focused_fit_category": payload["focused_fit_category"],
        "market_family": payload["market_family"],
        "current_odds_pct": payload["current_odds_pct"],
        "predicted_future_odds_pct": payload["absolute_move_future_odds_pct"],
        "predicted_delta_pts": payload["absolute_move_delta_pts"],
        "predicted_direction": payload["absolute_move_direction"],
        "prediction_source": payload["absolute_move_direction_source"],
        "display_tier": payload["absolute_move_display_tier"],
        "display_reasons": payload["absolute_move_display_reasons"],
        "review_reasons": payload["absolute_move_review_reasons"],
        "direction_signal_tier": payload["direction_signal_tier"],
        "direction_signal_tier_reason": payload["direction_signal_tier_reason"],
        "direction_signal_predicted_direction": payload["direction_signal_predicted_direction"],
        "direction_signal_confidence": payload["direction_signal_confidence"],
        "reliability_warnings": payload["direction_signal_reliability_warnings"],
        "overlay_future_odds_pct": payload["overlay_blend_future_odds_pct"],
        "overlay_delta_pts": payload["overlay_blend_delta_pts"],
        "overlay_direction": payload["overlay_blend_direction"],
        "interval_low_future_odds_pct": payload["overlay_quantile_low_future_odds_pct"],
        "interval_high_future_odds_pct": payload["overlay_quantile_high_future_odds_pct"],
        "actual_future_odds_pct": payload["actual_future_odds_pct"],
        "actual_delta_pts": payload["actual_delta_pts"],
        "actual_direction": payload["actual_direction"],
        "trend_fit_error_type": payload["trend_fit_error_type"],
        "trend_shape_score": payload["trend_shape_score"],
        "whale_anchor": payload["whale_anchor"],
        "crypto_segment_direction_gate_tier": payload["crypto_segment_direction_gate_tier"],
        "crypto_segment_direction_gate_reason": payload["crypto_segment_direction_gate_reason"],
        "crypto_direction_source_selector_reason": payload["crypto_direction_source_selector_reason"],
        "local_backtest_only": True,
    }


def _market_profile_prediction_index(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a market-slug keyed prediction index for local profile pages."""
    selected: dict[tuple[str, str, str], dict[str, Any]] = {}
    for record in records:
        row = record["row"]
        market_slug = str(row.get("market_slug") or "").strip().lower()
        if not market_slug:
            continue
        key = (market_slug, str(record["window"]), str(row.get("side_label") or ""))
        existing = selected.get(key)
        if existing is None or str(row.get("observation_time") or "") > str(
            existing["row"].get("observation_time") or ""
        ):
            selected[key] = record

    by_market: dict[str, dict[str, Any]] = {}
    tier_priority = {"show": 0, "review": 1, "hidden": 2}
    direction_priority = {"strong": 0, "watch": 1, "abstain": 2, "missing": 3}
    for (market_slug, _window, _side), record in selected.items():
        market_payload = by_market.setdefault(
            market_slug,
            {
                "available": True,
                "market_slug": market_slug,
                "question": str(record["row"].get("question") or ""),
                "production_use": False,
                "local_backtest_only": True,
                "source": "local_whale_anchored_delta_report",
                "windows": {"12h": [], "24h": []},
            },
        )
        case = _market_profile_prediction_case(record)
        market_payload["windows"].setdefault(str(case["window"]), []).append(case)

    prediction_count = 0
    for market_payload in by_market.values():
        for window_name, cases in list((market_payload.get("windows") or {}).items()):
            sorted_cases = sorted(
                cases,
                key=lambda item: (
                    tier_priority.get(str(item.get("display_tier") or "hidden"), 3),
                    direction_priority.get(str(item.get("direction_signal_tier") or "missing"), 4),
                    -abs(float(item.get("predicted_delta_pts") or 0.0)),
                    str(item.get("side_label") or ""),
                ),
            )
            market_payload["windows"][window_name] = sorted_cases
            prediction_count += len(sorted_cases)

    return {
        "available": True,
        "source": "local_whale_anchored_delta_report",
        "production_use": False,
        "local_backtest_only": True,
        "market_count": len(by_market),
        "prediction_count": prediction_count,
        "by_market_slug": dict(sorted(by_market.items())),
        "note": (
            "Local dashboard-only profile predictions from the whale-anchored delta backtest. "
            "Rows are latest observations by market side and prediction window."
        ),
    }


def _render_markdown(report: dict[str, Any]) -> str:
    """Render a compact markdown summary."""
    lines = [
        "# Whale-Anchored Non-Flat Delta Report",
        "",
        f"Generated: `{report['generated_at']}`",
        "",
        report["summary"],
        "",
        "| Window | Rows | Gated Rows | Review Rows | Strong/Watch Support | Raw Direction Preserved | Residual RMSE | Overlay RMSE | Overlay Direction Delta | Pair Underprediction Delta | Overlay Band Coverage |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for window_name, window in report["windows"].items():
        lines.append(
            "| {window} | {rows} | {gated} | {review} | {support} | {paired} | {residual:.2f} | {blend:.2f} | {direction:+.2f} | {under:+.2f} | {coverage:.1f}% |".format(
                window=window_name,
                rows=window["row_count"],
                gated=window["overlay_gate_summary"]["allowed_row_count"],
                review=window["overlay_candidate_summary"]["review_candidate_row_count"],
                support=window["direction_tier_support_summary"]["strong_watch_row_count"],
                paired=window["overlay_blend_selection_summary"]["raw_direction_preserved_row_count"],
                residual=window["current_residual_whale"]["rmse_pts"],
                blend=window["overlay_blend"]["rmse_pts"],
                direction=window["overlay_direction_match_delta_vs_pair_pts"],
                under=window["pair_normalized_underprediction_delta_vs_blend_pts"],
                coverage=window["overlay_quantile_interval"]["coverage_pct"],
            )
        )
    lines.extend(
        [
            "",
            "## Focused Category Fit",
            "",
            "| Window | Category | Rows | Gate | MAE | Pre MAE | Direction | Actual | Predicted | Underprediction | Band Coverage |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in report["windows"].items():
        for row in window.get("focused_category_fit_summary", []):
            gate = "show" if row["gate_allowed"] else ",".join(row["gate_reasons"])
            lines.append(
                "| {window} | {category} | {rows} | {gate} | {mae:.2f} | {pre:.2f} | {direction:.1f}% | {actual:.1f} | {predicted:.1f} | {under:.1f}% | {coverage:.1f}% |".format(
                    window=window_name,
                    category=row["category"],
                    rows=row["row_count"],
                    gate=gate,
                    mae=row["mae_pts"],
                    pre=row["pre_calibration_mae_pts"],
                    direction=row["direction_match_pct"],
                    actual=row["average_abs_actual_delta_pts"],
                    predicted=row["average_abs_predicted_delta_pts"],
                    under=row["underprediction_pct"],
                    coverage=row["band_coverage_pct"],
                )
            )
    lines.extend(
        [
            "",
            "## Direction-Confirmed Magnitude Overlay",
            "",
            "| Window | Category | Rows | Display | Applied | Overlay Pred | Confirmed Pred | Actual | MAE Delta | Direction Delta | Underfit Delta | Support |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for window_name, window in report["windows"].items():
        for row in window.get("direction_confirmed_fit_summary", []):
            support = ",".join(
                f"{key}:{value}"
                for key, value in (row.get("support_source_counts") or {}).items()
                if key != "none" and value
            ) or "none"
            lines.append(
                "| {window} | {category} | {rows} | {display} | {applied} | {overlay:.1f} | {confirmed:.1f} | {actual:.1f} | {mae:+.2f} | {direction:+.1f}% | {under:+.1f}% | {support} |".format(
                    window=window_name,
                    category=row["category"],
                    rows=row["row_count"],
                    display="show" if row.get("display_allowed") else ",".join(row.get("display_reasons") or []),
                    applied=row["applied_row_count"],
                    overlay=row["overlay_average_abs_predicted_delta_pts"],
                    confirmed=row["direction_confirmed_average_abs_predicted_delta_pts"],
                    actual=row["average_abs_actual_delta_pts"],
                    mae=row["mae_delta_pts"],
                    direction=float(row["direction_match_pct"]) - float(row["overlay_direction_match_pct"]),
                    under=(
                        float(row["direction_confirmed_underprediction_pct"])
                        - float(row["overlay_underprediction_pct"])
                    ),
                    support=support,
                )
            )
    lines.extend(
        [
            "",
            "## Absolute-Move Split Overlay",
            "",
            "| Window | Category | Rows | Display | Direction Rows | Resolved | Overlay Pred | Absolute Pred | Actual | MAE Delta | Base Delta | Direction Delta | Underfit Delta | Source |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for window_name, window in report["windows"].items():
        for row in window.get("absolute_move_fit_summary", []):
            source = ",".join(
                f"{key}:{value}"
                for key, value in (row.get("direction_source_counts") or {}).items()
                if key != "no_movement_direction" and value
            ) or "none"
            if row.get("display_tier") == "show":
                display_label = "show"
            elif row.get("display_tier") == "review":
                display_label = "review:" + ",".join(row.get("review_reasons") or [])
            else:
                display_label = ",".join(row.get("display_reasons") or [])
            lines.append(
                "| {window} | {category} | {rows} | {display} | {direction_rows} | {resolved} | {overlay:.1f} | {absolute:.1f} | {actual:.1f} | {mae:+.2f} | {base:+.2f} | {direction:+.1f}% | {under:+.1f}% | {source} |".format(
                    window=window_name,
                    category=row["category"],
                    rows=row["row_count"],
                    display=display_label,
                    direction_rows=row["direction_row_count"],
                    resolved=row.get("resolved_row_count", 0),
                    overlay=row["overlay_average_abs_predicted_delta_pts"],
                    absolute=row["absolute_move_average_abs_predicted_delta_pts"],
                    actual=row["average_abs_actual_delta_pts"],
                    mae=row["mae_delta_pts"],
                    base=row.get("mae_delta_vs_base_pts", 0.0),
                    direction=float(row["direction_match_pct"]) - float(row["overlay_direction_match_pct"]),
                    under=float(row["absolute_move_underprediction_pct"]) - float(row["overlay_underprediction_pct"]),
                    source=source,
                )
            )
    lines.extend(
        [
            "",
            "## Crypto Segment Direction Gate",
            "",
            "| Window | Crypto Rows | Already Show | Eligible Review | Promoted | Kept Review | Promoted MAE Delta | Promoted Direction Delta | Scopes |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for window_name, window in report["windows"].items():
        gate = window.get("crypto_segment_direction_gate") or {}
        promoted = gate.get("promoted_rows") or {}
        scope_counts = ",".join(
            f"{key}:{value}"
            for key, value in (gate.get("scope_counts") or {}).items()
        ) or "none"
        lines.append(
            "| {window} | {rows} | {show} | {eligible} | {promoted_count} | {review} | {mae:+.2f} | {direction:+.1f}% | {scopes} |".format(
                window=window_name,
                rows=gate.get("row_count", 0),
                show=gate.get("already_show_row_count", 0),
                eligible=gate.get("eligible_review_row_count", 0),
                promoted_count=gate.get("promoted_row_count", 0),
                review=gate.get("kept_review_row_count", 0),
                mae=promoted.get("mae_delta_pts", 0.0),
                direction=promoted.get("direction_match_delta_pts", 0.0),
                scopes=scope_counts,
            )
        )
    lines.extend(
        [
            "",
            "## Crypto Direction Source Selector",
            "",
            "Candidate-only until an applied source change clears the no-regression guardrail.",
            "",
            "| Window | Rows | Applied | Direction Delta | MAE Delta | Applied Direction Delta | Applied MAE Delta | Sources | Reasons |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for window_name, window in report["windows"].items():
        selector = window.get("crypto_direction_source_selector") or {}
        applied_rows = selector.get("applied_rows") or {}
        source_counts = ",".join(
            f"{key}:{value}" for key, value in (selector.get("applied_source_counts") or {}).items()
        ) or "none"
        reason_counts = ",".join(
            f"{key}:{value}" for key, value in (selector.get("reason_counts") or {}).items()
        ) or "none"
        lines.append(
            "| {window} | {rows} | {applied} | {direction:+.1f}% | {mae:+.2f} | {applied_direction:+.1f}% | {applied_mae:+.2f} | {sources} | {reasons} |".format(
                window=window_name,
                rows=selector.get("row_count", 0),
                applied=selector.get("applied_row_count", 0),
                direction=selector.get("direction_delta_pts", 0.0),
                mae=selector.get("mae_delta_pts", 0.0),
                applied_direction=applied_rows.get("direction_delta_pts", 0.0),
                applied_mae=applied_rows.get("mae_delta_pts", 0.0),
                sources=source_counts,
                reasons=reason_counts,
            )
        )
    lines.extend(
        [
            "",
            "## Crypto Promoted Row Precision Audit",
            "",
            "| Window | Promoted | Precision | False Shows | MAE Delta | Direction Delta | Selector Applied | Assets | Entry Timing |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for window_name, window in report["windows"].items():
        audit = window.get("crypto_promoted_row_precision_audit") or {}
        assets = ",".join(f"{key}:{value}" for key, value in (audit.get("asset_counts") or {}).items()) or "none"
        timing = ",".join(f"{key}:{value}" for key, value in (audit.get("entry_timing_counts") or {}).items()) or "none"
        lines.append(
            "| {window} | {rows} | {precision:.1f}% | {false_rows} ({false_pct:.1f}%) | {mae:+.2f} | {direction:+.1f}% | {selector:.1f}% | {assets} | {timing} |".format(
                window=window_name,
                rows=audit.get("row_count", 0),
                precision=audit.get("precision_pct", 0.0),
                false_rows=audit.get("false_show_count", 0),
                false_pct=audit.get("false_show_pct", 0.0),
                mae=audit.get("mae_delta_pts", 0.0),
                direction=audit.get("direction_match_delta_pts", 0.0),
                selector=audit.get("source_selector_applied_row_pct", 0.0),
                assets=assets,
                timing=timing,
            )
        )
    lines.extend(
        [
            "",
            "## Trend Fit Diagnostics",
            "",
            "| Window | Segment | Rows | MAE | Bias | Magnitude Ratio | Underfit | Direction | Band | Main Error Type |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for window_name, window in report["windows"].items():
        for row in (window.get("trend_fit_diagnostics") or {}).get("focused_category", []):
            error_counts = row.get("error_type_counts") or {}
            main_error = max(error_counts.items(), key=lambda item: item[1])[0] if error_counts else "none"
            lines.append(
                "| {window} | {segment} | {rows} | {mae:.2f} | {bias:+.2f} | {ratio:.2f} | {under:.1f}% | {direction:.1f}% | {band:.1f}% | {error} |".format(
                    window=window_name,
                    segment=row["segment"],
                    rows=row["row_count"],
                    mae=row["mae_pts"],
                    bias=row["signed_bias_pts"],
                    ratio=row["magnitude_ratio"],
                    under=row["underprediction_pct"],
                    direction=row["direction_match_pct"],
                    band=row["band_coverage_pct"],
                    error=main_error,
                )
            )
    lines.extend(
        [
            "",
            "## Crypto Direction Miss Audit",
            "",
            "| Window | Segment | Rows | Direction Miss | Direction Match | Strong/Watch Align | Whale Pressure Align | Actual | Predicted | Underfit |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in report["windows"].items():
        audit = window.get("crypto_direction_miss_audit") or {}
        rows = [audit.get("summary"), *((audit.get("by_calibration_segment") or [])[:4])]
        for row in rows:
            if not row:
                continue
            lines.append(
                "| {window} | {segment} | {rows} | {miss:.1f}% | {direction:.1f}% | {signal:.1f}% | {pressure:.1f}% | {actual:.1f} | {predicted:.1f} | {under:.1f}% |".format(
                    window=window_name,
                    segment=row["segment"],
                    rows=row["row_count"],
                    miss=row["direction_miss_pct"],
                    direction=row["direction_match_pct"],
                    signal=row["strong_watch_actual_alignment_pct"],
                    pressure=row["whale_pressure_actual_alignment_pct"],
                    actual=row["average_abs_actual_delta_pts"],
                    predicted=row["average_abs_predicted_delta_pts"],
                    under=row["underprediction_pct"],
                )
            )
    lines.extend(
        [
            "",
            "## Crypto Direction Split Diagnostics",
            "",
            "| Window | Segment | Rows | Overlay Dir | Absolute Dir | Strong/Watch Align | Pressure Align | Actual | Overlay Pred | Absolute Pred | Recommendation |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for window_name, window in report["windows"].items():
        diagnostics = window.get("crypto_direction_split_diagnostics") or {}
        rows = [
            diagnostics.get("summary"),
            *((diagnostics.get("by_crypto_asset") or [])[:4]),
            *((diagnostics.get("by_market_family") or [])[:2]),
        ]
        for row in rows:
            if not row:
                continue
            lines.append(
                "| {window} | {segment} | {rows} | {overlay:.1f}% | {absolute:.1f}% | {signal:.1f}% | {pressure:.1f}% | {actual:.1f} | {overlay_pred:.1f} | {absolute_pred:.1f} | {action}:{reason} |".format(
                    window=window_name,
                    segment=row["segment"],
                    rows=row["row_count"],
                    overlay=row["overlay_direction_match_pct"],
                    absolute=row["absolute_move_direction_match_pct"],
                    signal=row["strong_watch_direction_alignment_pct"],
                    pressure=row["whale_pressure_alignment_pct"],
                    actual=row["average_abs_actual_delta_pts"],
                    overlay_pred=row["overlay_average_abs_predicted_delta_pts"],
                    absolute_pred=row["absolute_move_average_abs_predicted_delta_pts"],
                    action=row["recommended_action"],
                    reason=row["recommended_reason"],
                )
            )
    lines.extend(
        [
            "",
            "## Whale Timing Direction Diagnostics",
            "",
            "| Window | Bucket | Rows | Overlay Dir | Absolute Dir | Strong/Watch Align | Pressure Align | Actual | Overlay Pred | Absolute Pred |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in report["windows"].items():
        diagnostics = window.get("whale_timing_direction_diagnostics") or {}
        rows = [
            *((diagnostics.get("by_entry_timing_bucket") or [])[:5]),
            *((diagnostics.get("crypto_by_entry_timing_bucket") or [])[:5]),
        ]
        for row in rows:
            if not row:
                continue
            lines.append(
                "| {window} | {segment} | {rows} | {overlay:.1f}% | {absolute:.1f}% | {signal:.1f}% | {pressure:.1f}% | {actual:.1f} | {overlay_pred:.1f} | {absolute_pred:.1f} |".format(
                    window=window_name,
                    segment=row["segment"],
                    rows=row["row_count"],
                    overlay=row["overlay_direction_match_pct"],
                    absolute=row["absolute_move_direction_match_pct"],
                    signal=row["strong_watch_direction_alignment_pct"],
                    pressure=row["whale_pressure_alignment_pct"],
                    actual=row["average_abs_actual_delta_pts"],
                    overlay_pred=row["overlay_average_abs_predicted_delta_pts"],
                    absolute_pred=row["absolute_move_average_abs_predicted_delta_pts"],
                )
            )
    lines.extend(
        [
            "",
            "## Calibration Candidate Backtests",
            "",
            "| Window | Source | Method | Candidates | Selected | Train MAE Delta | Direction Delta | Guard Pass |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in report["windows"].items():
        for row in window.get("calibration_candidate_backtests", []):
            if int(row.get("candidate_row_count") or 0) <= 0:
                continue
            lines.append(
                "| {window} | {source} | {method} | {candidates} | {selected} | {mae:+.3f} | {direction:+.2f} | {guard:.1f}% |".format(
                    window=window_name,
                    source=row["source"],
                    method=row["method"],
                    candidates=row["candidate_row_count"],
                    selected=row["selected_row_count"],
                    mae=row["average_train_mae_delta_pts"],
                    direction=row["average_train_direction_delta_pts"],
                    guard=row["pass_train_guard_pct"],
                )
            )
    lines.extend(
        [
            "",
            "## Trajectory Fit",
            "",
            "| Window | Rows | Correlation | Path MAE | Pre Path MAE | Path Delta | Signed Area Error |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in report["windows"].items():
        row = window.get("trajectory_fit_summary") or {}
        lines.append(
            "| {window} | {rows} | {corr:.3f} | {path:.2f} | {pre:.2f} | {delta:+.2f} | {area:+.2f} |".format(
                window=window_name,
                rows=row.get("row_count", 0),
                corr=row.get("correlation", 0.0),
                path=row.get("average_path_mae_pts", 0.0),
                pre=row.get("pre_calibration_path_mae_pts", 0.0),
                delta=row.get("path_mae_delta_pts", 0.0),
                area=row.get("signed_area_error_pts", 0.0),
            )
        )
    lines.extend(
        [
            "",
            "## Category Gates",
            "",
            "| Window | Category | Rows | Gate | Direction | Underprediction | Band Coverage | Band Width |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in report["windows"].items():
        for segment in window.get("category_segments", []):
            gate = "show" if segment["overlay_gate"]["allowed"] else ",".join(segment["overlay_gate"]["reasons"])
            lines.append(
                "| {window} | {category} | {rows} | {gate} | {direction:.1f}% | {under:.1f}% | {coverage:.1f}% | {width:.1f} |".format(
                    window=window_name,
                    category=segment["category"],
                    rows=segment["row_count"],
                    gate=gate,
                    direction=segment["overlay_blend"]["direction_match_pct"],
                    under=segment["overlay_blend"]["underprediction_pct"],
                    coverage=segment["overlay_quantile_interval"]["coverage_pct"],
                    width=segment["overlay_quantile_interval"]["average_width_pts"],
                )
            )
    lines.extend(
        [
            "",
            "## Event Category Support",
            "",
            "| Window | Event Category | Rows | Gate | Direction | Band Coverage |",
            "| --- | --- | ---: | --- | ---: | ---: |",
        ]
    )
    for window_name, window in report["windows"].items():
        for segment in window.get("event_category_segments", []):
            if segment["category"] not in {"esports", "video-games", "crypto", "politics", "world", "tech", "technology"}:
                continue
            gate = "show" if segment["overlay_gate"]["allowed"] else ",".join(segment["overlay_gate"]["reasons"])
            lines.append(
                "| {window} | {category} | {rows} | {gate} | {direction:.1f}% | {coverage:.1f}% |".format(
                    window=window_name,
                    category=segment["category"],
                    rows=segment["row_count"],
                    gate=gate,
                    direction=segment["overlay_blend"]["direction_match_pct"],
                    coverage=segment["overlay_quantile_interval"]["coverage_pct"],
                )
            )
    lines.extend(
        [
            "",
            "Interpretation: this model is meant to test whether whale-timed non-flat moves can be drawn closer to the",
            "actual trend magnitude. The learned blend dampens the aggressive delta when useful; pair normalization keeps",
            "binary market sides coherent; the quantile band shows whether the realized trend stayed inside the model's",
            "expected range. Category gates plus Strong/Watch direction-tier support prevent weak segments from being",
            "treated as dashboard-ready overlays.",
            "",
        ]
    )
    return "\n".join(lines)


def evaluate_whale_anchored_delta(
    *,
    dataset_path: Path = DEFAULT_DATASET_PATH,
    comparison_path: Path = DEFAULT_COMPARISON_PATH,
    direction_classifier_path: Path = DEFAULT_DIRECTION_CLASSIFIER_PATH,
    nonflat_threshold: float = DEFAULT_NONFLAT_THRESHOLD,
) -> dict[str, Any]:
    """Train and evaluate the whale-anchored non-flat delta model."""
    from sklearn.ensemble import HistGradientBoostingRegressor

    regime_rows = _filter_rows_by_regime(_load_training_rows(dataset_path), REGIME_TRADE_COVERED)
    rows = [row for row in regime_rows if not _is_sports_market(row)]
    excluded_physical_sports_rows = len(regime_rows) - len(rows)
    _enrich_trend_features(rows)
    feature_columns = tuple(
        column
        for column in dict.fromkeys(
            (
                *BASE_FEATURE_COLUMNS,
                *_recent_feature_columns(),
                *_trend_feature_columns(),
                "future_window_reaches_resolution_12h",
                "future_window_reaches_resolution_24h",
            )
        )
        if column in rows[0]
    )
    selected_specs = _selected_prediction_specs(comparison_path)
    direction_tier_lookup, direction_tier_metadata = _load_direction_tier_lookup(direction_classifier_path)

    from data_platform.jobs.export_ml_market_projection_example import _predict_rolling_rows

    residual_predictions = _predict_rolling_rows(rows=rows, selected_specs=selected_specs)
    splits, _, _ = _build_rolling_splits(rows)

    records_by_window: dict[str, list[dict[str, Any]]] = {window: [] for window in PREDICTION_WINDOWS}
    folds_by_window: dict[str, list[dict[str, Any]]] = {window: [] for window in PREDICTION_WINDOWS}

    for window_name in PREDICTION_WINDOWS:
        for split in splits:
            train_rows = _anchor_rows(split["train_rows"], window_name, threshold=nonflat_threshold)
            test_rows = _anchor_rows(split["test_rows"], window_name, threshold=nonflat_threshold)
            if len(train_rows) < MIN_TRAIN_ROWS or not test_rows:
                folds_by_window[window_name].append(
                    {
                        "fold_index": split["fold_index"],
                        "train_rows": len(train_rows),
                        "test_rows": len(test_rows),
                        "status": "skipped_insufficient_rows",
                    }
                )
                continue

            y_train = [_actual_delta(row, window_name) for row in train_rows]
            train_matrix = _feature_matrix(train_rows, feature_columns)
            model = HistGradientBoostingRegressor(
                max_iter=160,
                learning_rate=0.035,
                max_leaf_nodes=15,
                l2_regularization=0.2,
                random_state=42,
            )
            model.fit(train_matrix, y_train)
            train_predictions = [float(value) for value in model.predict(train_matrix)]
            y_abs_train = [abs(value) for value in y_train]
            absolute_move_model = HistGradientBoostingRegressor(
                max_iter=160,
                learning_rate=0.035,
                max_leaf_nodes=15,
                l2_regularization=0.2,
                random_state=45,
            )
            absolute_move_model.fit(train_matrix, y_abs_train)
            absolute_move_train_predictions = [
                max(0.0, float(value)) for value in absolute_move_model.predict(train_matrix)
            ]
            ratios = [
                abs(actual) / max(abs(predicted), 0.0025)
                for actual, predicted in zip(y_train, train_predictions, strict=True)
                if abs(predicted) >= 0.0025
            ]
            magnitude_scale = statistics.median(ratios) if ratios else 1.0
            magnitude_scale = max(MAGNITUDE_SCALE_MIN, min(MAGNITUDE_SCALE_MAX, magnitude_scale))
            scales_by_category = _magnitude_scales_by_category(
                rows=train_rows,
                actuals=y_train,
                predictions=train_predictions,
                fallback_scale=magnitude_scale,
            )
            scales_by_event_category = _magnitude_scales_by_event_category(
                rows=train_rows,
                actuals=y_train,
                predictions=train_predictions,
                fallback_scale=magnitude_scale,
            )
            focus_direction_scales = _directional_magnitude_scales(
                rows=train_rows,
                actuals=y_train,
                predictions=train_predictions,
                fallback_scale=magnitude_scale,
                category_key="focus_category",
                threshold=nonflat_threshold,
            )
            event_direction_scales = _directional_magnitude_scales(
                rows=train_rows,
                actuals=y_train,
                predictions=train_predictions,
                fallback_scale=magnitude_scale,
                category_key="event_category",
                threshold=nonflat_threshold,
            )
            calibrated_train_predictions = [
                prediction
                * float(
                    _select_magnitude_scale(
                        focus_category=_focus_category(row),
                        event_category=_event_category(row),
                        raw_delta=prediction,
                        fallback_scale=magnitude_scale,
                        focus_scales=scales_by_category,
                        event_scales=scales_by_event_category,
                        focus_direction_scales=focus_direction_scales,
                        event_direction_scales=event_direction_scales,
                        threshold=nonflat_threshold,
                    )["scale"]
                )
                for row, prediction in zip(train_rows, train_predictions, strict=True)
            ]
            blend_alpha = _select_blend_alpha(y_train, calibrated_train_predictions)
            alphas_by_category = _blend_alphas_by_category(
                rows=train_rows,
                actuals=y_train,
                calibrated_predictions=calibrated_train_predictions,
                fallback_alpha=blend_alpha,
            )

            quantile_low_model = HistGradientBoostingRegressor(
                loss="quantile",
                quantile=QUANTILE_LOW,
                max_iter=160,
                learning_rate=0.035,
                max_leaf_nodes=15,
                l2_regularization=0.2,
                random_state=43,
            )
            quantile_high_model = HistGradientBoostingRegressor(
                loss="quantile",
                quantile=QUANTILE_HIGH,
                max_iter=160,
                learning_rate=0.035,
                max_leaf_nodes=15,
                l2_regularization=0.2,
                random_state=44,
            )
            quantile_low_model.fit(train_matrix, y_train)
            quantile_high_model.fit(train_matrix, y_train)

            accepted_test_rows = 0
            test_matrix = _feature_matrix(test_rows, feature_columns)
            raw_predictions = [float(value) for value in model.predict(test_matrix)]
            absolute_move_predictions = [max(0.0, float(value)) for value in absolute_move_model.predict(test_matrix)]
            quantile_low_predictions = [float(value) for value in quantile_low_model.predict(test_matrix)]
            quantile_high_predictions = [float(value) for value in quantile_high_model.predict(test_matrix)]
            for row, raw_prediction, absolute_move_prediction, quantile_low_prediction, quantile_high_prediction in zip(
                test_rows,
                raw_predictions,
                absolute_move_predictions,
                quantile_low_predictions,
                quantile_high_predictions,
                strict=True,
            ):
                residual_item = residual_predictions.get(_record_key(row), {}).get("windows", {}).get(window_name)
                if not residual_item:
                    continue
                accepted_test_rows += 1
                actual_delta = _actual_delta(row, window_name)
                focus_category = _focus_category(row)
                event_category = _event_category(row)
                focused_fit_category = _focused_fit_category(row)
                market_family = _market_family_segment(row)
                trend_calibration_segment = _trend_calibration_segment(row)
                whale_pressure_value = _whale_pressure_value(row, window_name)
                direction_tier_item = direction_tier_lookup.get(_direction_tier_key(window_name, row))
                direction_signal_tier = str(
                    (direction_tier_item or {}).get("signal_tier")
                    or ("missing" if direction_tier_metadata["available"] else "unavailable")
                )
                scale_payload = _select_magnitude_scale(
                    focus_category=focus_category,
                    event_category=event_category,
                    raw_delta=raw_prediction,
                    fallback_scale=magnitude_scale,
                    focus_scales=scales_by_category,
                    event_scales=scales_by_event_category,
                    focus_direction_scales=focus_direction_scales,
                    event_direction_scales=event_direction_scales,
                    threshold=nonflat_threshold,
                )
                segment_magnitude_scale = float(scale_payload["scale"])
                segment_blend_alpha = float(alphas_by_category[focus_category]["alpha"])
                raw_delta = float(raw_prediction)
                calibrated_delta = raw_delta * segment_magnitude_scale
                raw_quantile_low_delta = float(quantile_low_prediction) * segment_magnitude_scale
                raw_quantile_high_delta = float(quantile_high_prediction) * segment_magnitude_scale
                quantile_low_delta = min(raw_quantile_low_delta, raw_quantile_high_delta)
                quantile_high_delta = max(raw_quantile_low_delta, raw_quantile_high_delta)
                residual_delta = float(residual_item["corrected_delta"])
                price_delta = float(residual_item["price_delta"])
                blend_delta = residual_delta + segment_blend_alpha * (calibrated_delta - residual_delta)
                current_odds = _current_odds(row)
                blend_direction = _direction(blend_delta, nonflat_threshold)
                whale_pressure_direction = _pressure_direction(whale_pressure_value)
                absolute_direction_payload = _absolute_move_direction_payload(
                    focused_fit_category=focused_fit_category,
                    signal_tier=direction_signal_tier,
                    signal_direction=str((direction_tier_item or {}).get("predicted_direction") or "flat"),
                    whale_pressure_direction=whale_pressure_direction,
                    blend_direction=blend_direction,
                )
                absolute_move_delta = _bounded_absolute_move_delta(
                    current_odds=current_odds,
                    absolute_prediction=float(absolute_move_prediction),
                    direction=str(absolute_direction_payload["direction"]),
                )
                records_by_window[window_name].append(
                    {
                        "row": row,
                        "condition_ref": str(row[GROUP_KEY_COLUMN]),
                        "window": window_name,
                        "focus_category": focus_category,
                        "event_category": event_category,
                        "focused_fit_category": focused_fit_category,
                        "market_family": market_family,
                        "trend_calibration_segment": trend_calibration_segment,
                        "crypto_asset": _crypto_asset_segment(row) if focused_fit_category == "crypto" else "",
                        "time_to_close_bucket": _time_to_close_bucket(row),
                        "whale_entry_timing_bucket": _whale_entry_timing_bucket(row),
                        "whale_flow_timing_bucket": _whale_flow_timing_bucket(row),
                        "whale_pressure_value": whale_pressure_value,
                        "whale_pressure_direction": whale_pressure_direction,
                        "current_odds": current_odds,
                        "actual_delta": actual_delta,
                        "price_delta": price_delta,
                        "residual_delta": residual_delta,
                        "raw_delta": raw_delta,
                        "calibrated_delta": calibrated_delta,
                        "blend_delta": blend_delta,
                        "absolute_move_delta": absolute_move_delta,
                        "absolute_move_base_delta": absolute_move_delta,
                        "absolute_move_abs_prediction": float(absolute_move_prediction),
                        "absolute_move_direction": _direction(absolute_move_delta, nonflat_threshold),
                        "absolute_move_base_direction": _direction(absolute_move_delta, nonflat_threshold),
                        "absolute_move_direction_source": str(absolute_direction_payload["source"]),
                        "absolute_move_base_direction_source": str(absolute_direction_payload["source"]),
                        "absolute_move_abs_error": abs(actual_delta - absolute_move_delta),
                        "absolute_move_base_abs_error": abs(actual_delta - absolute_move_delta),
                        "blend_alpha": blend_alpha,
                        "segment_magnitude_scale": segment_magnitude_scale,
                        "segment_magnitude_scale_source": str(scale_payload["source"]),
                        "segment_magnitude_scale_train_rows": int(scale_payload["train_rows"]),
                        "segment_magnitude_scale_category": str(scale_payload["category"]),
                        "segment_blend_alpha": segment_blend_alpha,
                        "direction_signal_lookup_available": bool(direction_tier_metadata["available"]),
                        "direction_signal_matched": bool(direction_tier_item),
                        "direction_signal_tier": direction_signal_tier,
                        "direction_signal_tier_reason": str(
                            (direction_tier_item or {}).get("signal_tier_reason") or ""
                        ),
                        "direction_signal_predicted_direction": str(
                            (direction_tier_item or {}).get("predicted_direction") or "flat"
                        ),
                        "direction_signal_confidence": float((direction_tier_item or {}).get("confidence") or 0.0),
                        "direction_signal_category_ece_pct": (direction_tier_item or {}).get("category_ece_pct"),
                        "direction_signal_time_to_close_ece_pct": (direction_tier_item or {}).get(
                            "time_to_close_ece_pct"
                        ),
                        "direction_signal_reliability_warnings": list(
                            (direction_tier_item or {}).get("reliability_warnings") or []
                        ),
                        "quantile_low_delta": quantile_low_delta,
                        "quantile_high_delta": quantile_high_delta,
                        "residual_abs_error": abs(actual_delta - residual_delta),
                        "calibrated_abs_error": abs(actual_delta - calibrated_delta),
                        "blend_abs_error": abs(actual_delta - blend_delta),
                        "actual_direction": _direction(actual_delta, nonflat_threshold),
                        "calibrated_direction": _direction(calibrated_delta, nonflat_threshold),
                        "blend_direction": blend_direction,
                        "interval_contains_actual": quantile_low_delta <= actual_delta <= quantile_high_delta,
                        "fold_index": split["fold_index"],
                        "magnitude_scale": magnitude_scale,
                    }
                )

            folds_by_window[window_name].append(
                {
                    "fold_index": split["fold_index"],
                    "train_rows": len(train_rows),
                    "test_rows": accepted_test_rows,
                    "status": "trained",
                    "magnitude_scale": _round(magnitude_scale, 6),
                    "blend_alpha": _round(blend_alpha, 4),
                    "quantile_low": QUANTILE_LOW,
                    "quantile_high": QUANTILE_HIGH,
                    "category_calibration": [
                        {
                            "category": category,
                            "magnitude_scale": _round(float(scale_payload["scale"]), 6),
                            "magnitude_source": scale_payload["source"],
                            "magnitude_train_rows": scale_payload["train_rows"],
                            "blend_alpha": _round(float(alphas_by_category[category]["alpha"]), 4),
                            "blend_source": alphas_by_category[category]["source"],
                            "blend_train_rows": alphas_by_category[category]["train_rows"],
                        }
                        for category, scale_payload in scales_by_category.items()
                    ],
                    "event_category_calibration": [
                        {
                            "category": category,
                            "magnitude_scale": _round(float(scale_payload["scale"]), 6),
                            "magnitude_source": scale_payload["source"],
                            "magnitude_train_rows": scale_payload["train_rows"],
                        }
                        for category, scale_payload in scales_by_event_category.items()
                        if scale_payload["train_rows"] > 0
                    ],
                    "directional_magnitude_calibration": [
                        {
                            "scope": scope,
                            "category": category,
                            "direction": direction,
                            "magnitude_scale": _round(float(scale_payload["scale"]), 6),
                            "magnitude_source": scale_payload["source"],
                            "magnitude_train_rows": scale_payload["train_rows"],
                        }
                        for scope, scale_map in (
                            ("focus_category", focus_direction_scales),
                            ("event_category", event_direction_scales),
                        )
                        for (category, direction), scale_payload in scale_map.items()
                        if scale_payload["source"] != "global_fallback"
                    ],
                    "raw_train_underprediction_pct": _safe_ratio(
                        sum(
                            1
                            for actual, predicted in zip(y_train, train_predictions, strict=True)
                            if abs(predicted) < abs(actual)
                        ),
                        len(y_train),
                    ),
                    "calibrated_train_underprediction_pct": _safe_ratio(
                        sum(
                            1
                            for actual, predicted in zip(y_train, train_predictions, strict=True)
                            if abs(predicted * magnitude_scale) < abs(actual)
                        ),
                        len(y_train),
                    ),
                    "blend_train_underprediction_pct": _safe_ratio(
                        sum(
                            1
                            for actual, predicted in zip(y_train, calibrated_train_predictions, strict=True)
                            if abs(predicted * blend_alpha) < abs(actual)
                        ),
                        len(y_train),
                    ),
                    "absolute_move_train_mae_pts": _pct(_mae(y_abs_train, absolute_move_train_predictions)),
                }
            )

    windows: dict[str, dict[str, Any]] = {}
    for window_name, records in records_by_window.items():
        _apply_pair_normalization(records, threshold=nonflat_threshold)
        _apply_overlay_blend_selection(records, threshold=nonflat_threshold)
        _apply_overlay_magnitude_recalibration(records, threshold=nonflat_threshold)
        regression_guard_summary = _apply_focused_fit_regression_guard(records, threshold=nonflat_threshold)
        crypto_absolute_direction_resolver = _apply_crypto_absolute_move_direction_resolver(
            records,
            threshold=nonflat_threshold,
        )
        crypto_direction_source_selector = _apply_crypto_direction_source_selector(
            records,
            threshold=nonflat_threshold,
        )
        _apply_direction_confirmed_magnitude_overlay(records, threshold=nonflat_threshold)
        direction_confirmed_display_gate = _apply_direction_confirmed_display_gates(
            records,
            threshold=nonflat_threshold,
        )
        absolute_move_display_gate = _apply_absolute_move_display_gates(
            records,
            threshold=nonflat_threshold,
        )
        crypto_segment_direction_gate = _apply_crypto_segment_direction_display_gate(
            records,
            threshold=nonflat_threshold,
        )
        category_segments = _segment_summaries(records, threshold=nonflat_threshold)
        focused_category_segments = _segment_summaries(
            records,
            threshold=nonflat_threshold,
            group_key="focused_fit_category",
            ordered_categories=FOCUSED_FIT_CATEGORY_ORDER,
        )
        event_category_segments = _segment_summaries(
            records,
            threshold=nonflat_threshold,
            group_key="event_category",
            ordered_categories=EVENT_CATEGORY_ORDER,
        )
        _apply_overlay_gates(records, focused_category_segments, group_key="focused_fit_category")
        for record in records:
            record["trend_fit_error_type"] = _trend_fit_error_type(record, threshold=nonflat_threshold)
            record["trend_shape_score"] = _trend_shape_score(record, threshold=nonflat_threshold)
            record["pre_calibration_path_mae_pts"] = _line_path_mae_pts(
                float(record["actual_delta"]),
                float(record["overlay_base_blend_delta"]),
            )
            record["post_calibration_path_mae_pts"] = _line_path_mae_pts(
                float(record["actual_delta"]),
                float(record["overlay_blend_delta"]),
            )
        allowed_row_count = sum(1 for record in records if bool(record.get("overlay_gate_allowed")))
        windows[window_name] = {
            **_window_summary(records, threshold=nonflat_threshold),
            "category_segments": category_segments,
            "focused_category_segments": focused_category_segments,
            "event_category_segments": event_category_segments,
            "focused_category_fit_summary": _fit_summary_from_segments(focused_category_segments),
            "event_category_fit_summary": _fit_summary_from_segments(event_category_segments),
            "trend_fit_diagnostics": _trend_fit_diagnostics(records, threshold=nonflat_threshold),
            "direction_confirmed_magnitude_summary": _direction_confirmed_magnitude_summary(
                records,
                threshold=nonflat_threshold,
            ),
            "direction_confirmed_display_gate": direction_confirmed_display_gate,
            "direction_confirmed_fit_summary": _direction_confirmed_fit_summary(
                records,
                threshold=nonflat_threshold,
            ),
            "absolute_move_summary": _absolute_move_summary(
                records,
                threshold=nonflat_threshold,
            ),
            "crypto_absolute_direction_resolver": crypto_absolute_direction_resolver,
            "crypto_direction_source_selector": crypto_direction_source_selector,
            "absolute_move_display_gate": absolute_move_display_gate,
            "crypto_segment_direction_gate": crypto_segment_direction_gate,
            "crypto_promoted_row_precision_audit": _crypto_promoted_row_precision_audit(
                records,
                threshold=nonflat_threshold,
            ),
            "absolute_move_fit_summary": _absolute_move_fit_summary(
                records,
                threshold=nonflat_threshold,
            ),
            "crypto_direction_miss_audit": _crypto_direction_miss_audit(records, threshold=nonflat_threshold),
            "crypto_direction_split_diagnostics": _crypto_direction_split_diagnostics(
                records,
                threshold=nonflat_threshold,
            ),
            "crypto_absolute_move_review_audit": _crypto_absolute_move_review_audit(
                records,
                threshold=nonflat_threshold,
            ),
            "whale_timing_direction_diagnostics": _whale_timing_direction_diagnostics(
                records,
                threshold=nonflat_threshold,
            ),
            "trajectory_fit_summary": _trajectory_fit_summary(
                records,
                threshold=nonflat_threshold,
                window_name=window_name,
            ),
            "calibration_candidate_backtests": _calibration_candidate_backtests(records),
            "selected_trend_fit_policy": _selected_trend_fit_policy(records),
            "gated_mae_before_after": _gated_mae_before_after(records, threshold=nonflat_threshold),
            "calibration_policy": _calibration_policy(),
            "calibration_regression_guard": regression_guard_summary,
            "pair_normalization_summary": _pair_normalization_summary(records),
            "overlay_blend_selection_summary": _overlay_blend_selection_summary(records),
            "overlay_magnitude_scale_summary": _overlay_magnitude_scale_summary(records),
            "direction_tier_support_summary": _direction_tier_support_summary(records),
            "overlay_candidate_summary": _overlay_candidate_summary(records),
            "overlay_gate_summary": {
                "allowed_row_count": allowed_row_count,
                "suppressed_row_count": len(records) - allowed_row_count,
                "allowed_row_pct": _safe_ratio(allowed_row_count, len(records)),
                "policy": {
                    "min_rows": OVERLAY_GATE_MIN_ROWS,
                    "min_direction_match_pct": OVERLAY_GATE_MIN_DIRECTION_MATCH_PCT,
                    "max_underprediction_pct": OVERLAY_GATE_MAX_UNDERPREDICTION_PCT,
                    "max_interval_width_pts": OVERLAY_GATE_MAX_INTERVAL_WIDTH_PTS,
                    "min_band_coverage_pct": OVERLAY_GATE_MIN_BAND_COVERAGE_PCT,
                },
            },
            "folds": folds_by_window[window_name],
        }
    all_records = [record for records in records_by_window.values() for record in records]
    report = {
        "available": bool(all_records),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "comparison_path": str(comparison_path),
        "direction_classifier_path": str(direction_classifier_path),
        "direction_tier_index": direction_tier_metadata,
        "excluded_physical_sports_rows": excluded_physical_sports_rows,
        "market_scope_note": "Physical sports are excluded; esports and video-game markets remain in scope.",
        "model_name": "whale_anchored_nonflat_delta_pair_normalized_tier_gated_trend_fit",
        "status": "experimental_research_overlay",
        "regime": REGIME_TRADE_COVERED,
        "nonflat_threshold_pts": _pct(nonflat_threshold),
        "feature_count": len(feature_columns),
        "feature_notes": [
            "Trains only on rows with recent whale anchor evidence and non-flat actual movement.",
            "Applies a fold-local magnitude scale learned from training rows to reduce systematic underprediction.",
            "Adds a fold-learned blend alpha so the aggressive delta can be dampened against the current residual model.",
            "Adds 10%-90% quantile regressors to show an expected trend range rather than one deterministic line.",
            "Calibrates magnitude by dashboard focus category when each category has enough fold-local training rows.",
            "Adds event-category and predicted-direction magnitude calibration before falling back to broader segments.",
            "Normalizes binary market-side trend predictions so paired sides do not imply more or less than 100% total probability.",
            "Uses a direction-preserving overlay selector when pair normalization conflicts with the Strong/Watch direction signal.",
            "Applies a final rolling trend-fit calibration using only earlier out-of-sample folds for the same segment.",
            "Adds an experimental direction-confirmed magnitude overlay that can scale visible trend lines when Strong/Watch direction or whale pressure agrees with the overlay.",
            "Splits crypto calibration by market style and asset when prior-fold history is available.",
            "Prioritizes direction-specific calibration slices before broader category fallbacks.",
            "Compares identity, magnitude scale, direction-conditioned magnitude, signed-bias, and capped slope/intercept candidates before applying a calibration.",
            "Adds crypto direction-miss audits by asset/style, time-to-close, direction tier, and whale-pressure direction.",
            "Reports trend-fit error types and endpoint-line trajectory diagnostics so misses are visible by segment.",
            "Adds category gates so weak backtest segments are suppressed on the ML page.",
            "Requires Strong or Watch direction-tier support, with direction agreement, before a trend overlay is surfaced.",
            "This is not a replacement for Strong/Watch direction tiers; it is a candidate trend-shape overlay gated by those tiers.",
            "The direction-confirmed magnitude line is dashboard-diagnostic only and does not replace the stricter surfaced overlay gate.",
        ],
        "summary": (
            "The whale-anchored delta model is designed to reduce the current tendency to flatten up/down moves. "
            "This version adds focus-category calibration, a learned blend, pair-normalized binary side forecasts, "
            "a direction-preserving overlay selector, a train-safe trend-fit calibration layer, a 10%-90% prediction "
            "band, Strong/Watch tier gates, and an experimental direction-confirmed magnitude line so only "
            "better-supported overlay segments are treated as dashboard candidates while closer trend-shape candidates "
            "can be reviewed separately."
        ),
        "windows": windows,
        "cases": _case_samples(all_records),
        "market_profile_predictions": _market_profile_prediction_index(all_records),
    }
    return report


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-path", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--comparison-path", default=str(DEFAULT_COMPARISON_PATH))
    parser.add_argument("--direction-classifier-path", default=str(DEFAULT_DIRECTION_CLASSIFIER_PATH))
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON_PATH))
    parser.add_argument("--output-markdown", default=str(DEFAULT_OUTPUT_MARKDOWN_PATH))
    parser.add_argument("--nonflat-threshold", type=float, default=DEFAULT_NONFLAT_THRESHOLD)
    args = parser.parse_args()

    report = evaluate_whale_anchored_delta(
        dataset_path=Path(args.dataset_path),
        comparison_path=Path(args.comparison_path),
        direction_classifier_path=Path(args.direction_classifier_path),
        nonflat_threshold=float(args.nonflat_threshold),
    )
    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    output_markdown = Path(args.output_markdown)
    output_markdown.parent.mkdir(parents=True, exist_ok=True)
    output_markdown.write_text(_render_markdown(report), encoding="utf-8")

    print(f"Wrote {output_json}")
    print(f"Wrote {output_markdown}")


if __name__ == "__main__":
    main()
