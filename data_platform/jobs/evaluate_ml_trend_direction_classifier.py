"""Evaluate up/flat/down trend classification with confidence abstention."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import inspect
import json
from pathlib import Path
import sys
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.jobs.export_ml_market_projection_example import (
    PREDICTION_WINDOWS,
    TREND_LOOKBACK_HOURS,
    _current_odds,
    _enrich_trend_features,
    _is_sports_market,
    _market_family_segment,
    _round,
    _safe_float,
)
from data_platform.ml.market_baseline_model import (
    GROUP_KEY_COLUMN,
    REGIME_TRADE_COVERED,
    _build_rolling_splits,
    _filter_rows_by_regime,
    _load_training_rows,
    _research_focus_segment,
)


DEFAULT_DATASET_PATH = Path("data_platform/runtime/ml/resolved_market_snapshot_features_current_db_asof.csv")
DEFAULT_OUTPUT_JSON_PATH = Path("data_platform/ml/ML_TREND_DIRECTION_CLASSIFIER_CURRENT_DB_ASOF.json")
DEFAULT_OUTPUT_MARKDOWN_PATH = Path("data_platform/ml/ML_TREND_DIRECTION_CLASSIFIER_CURRENT_DB_ASOF.md")
DEFAULT_DIRECTION_THRESHOLD = 0.005
DEFAULT_CONFIDENCE_THRESHOLDS = (0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95)
DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD = 0.6
DEFAULT_MIN_THRESHOLD_COVERAGE = 0.08
DEFAULT_CALIBRATION_METHOD = "sigmoid"
DEFAULT_STRONG_MIN_PRECISION_PCT = 90.0
DEFAULT_WATCH_MIN_PRECISION_PCT = 80.0
WATCH_ECE_LIMITS = (5.0, 7.5, 10.0, 12.5, 15.0, float("inf"))
WATCH_MISMATCH_LIMITS = (35.0, 45.0, float("inf"))
WATCH_MIN_SEGMENT_ROWS = (20, 50)
WATCH_SUPPORT_MODES = ("whale", "stability", "whale_or_stability")
WATCH_TIME_BUCKET_MIN_SIGNAL_COUNT = 10
WATCH_TIME_BUCKET_MIN_PRECISION_PCT = DEFAULT_WATCH_MIN_PRECISION_PCT
CRYPTO_WATCH_BLOCKED_PRESSURE_QUALITIES = (
    "weak_pressure",
    "raw_directional",
    "trusted_price_conflict",
    "raw_price_conflict",
)
STABILITY_LOOKBACK_RECORDS = 3
STABILITY_MIN_OBSERVATIONS = 3
STABILITY_MIN_AGREEMENT_PCT = 66.6667
LABELS = ("down", "flat", "up")
TOP_SEGMENT_LIMIT = 8
TIME_TO_CLOSE_BUCKET_ORDER = ("0-6h", "6-12h", "12-24h", "24-72h", "72h+")
TIER_DESCRIPTIONS = {
    "strong": (
        "High-confidence up/down signal using the strict recommended threshold. Treat this as the primary "
        "dashboard signal, with reliability and stability diagnostics still visible."
    ),
    "watch": (
        "Broader up/down signal below Strong. It must have selected support from whale evidence, stable recent "
        "direction, or both, pass the selected reliability rule, and pass the time-to-close bucket gate."
    ),
    "abstain": (
        "No surfaced directional signal. The model may still have an internal prediction, but confidence, "
        "whale evidence, stability, or reliability is not strong enough."
    ),
}

BASE_FEATURE_COLUMNS = (
    "last_price_side",
    "last_price_opposite",
    "avg_price_side",
    "avg_price_opposite",
    "min_price_side",
    "max_price_side",
    "price_baseline",
    "price_gap_side_minus_opposite",
    "price_abs_distance_from_even",
    "market_volume_log1p",
    "hours_to_close",
    "horizon_hours",
    "market_age_hours",
    "market_duration_hours",
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
    "whale_side_weighted_score_sum",
    "whale_side_weighted_buy_pressure",
    "whale_side_weighted_sell_pressure",
    "whale_side_weighted_net_pressure",
    "whale_side_weighted_net_pressure_per_side_notional",
    "whale_side_weighted_net_pressure_per_total_notional",
    "whale_side_weighted_net_pressure_per_market_liquidity",
    "whale_side_weighted_net_pressure_per_whale",
    "whale_side_buy_trade_count",
    "whale_side_sell_trade_count",
    "whale_side_buy_sell_ratio",
    "whale_side_entry_exit_gap",
    "whale_side_avg_trades_per_active_day",
    "whale_side_entry_trade_count",
    "whale_side_exit_trade_count",
    "whale_side_partial_exit_count",
    "whale_side_full_exit_count",
    "whale_side_unmatched_sell_count",
    "whale_side_avg_holding_hours",
    "whale_side_avg_open_holding_hours",
    "whale_side_realized_pnl",
    "whale_side_realized_roi",
    "whale_side_avg_exit_profit",
    "trusted_whale_side_trade_share",
    "trusted_whale_side_notional_share",
    "trusted_whale_side_buy_notional_share",
    "trusted_whale_side_sell_notional_share",
    "trusted_whale_side_net_notional_share",
    "trusted_whale_side_weighted_score_sum",
    "trusted_whale_side_weighted_buy_pressure",
    "trusted_whale_side_weighted_sell_pressure",
    "trusted_whale_side_weighted_net_pressure",
    "trusted_whale_side_entry_exit_gap",
    "trusted_whale_side_avg_trades_per_active_day",
    "trusted_whale_side_entry_trade_count",
    "trusted_whale_side_exit_trade_count",
    "trusted_whale_side_partial_exit_count",
    "trusted_whale_side_full_exit_count",
    "trusted_whale_side_unmatched_sell_count",
    "trusted_whale_side_avg_holding_hours",
    "trusted_whale_side_avg_open_holding_hours",
    "trusted_whale_side_realized_pnl",
    "trusted_whale_side_realized_roi",
    "trusted_whale_side_avg_exit_profit",
    "top_whale_side_notional_share",
    "top_trusted_whale_side_notional_share",
)

POSITION_DERIVED_COLUMNS = (
    "whale_exit_to_entry_ratio",
    "whale_unmatched_sell_ratio",
    "whale_partial_exit_ratio",
    "whale_full_exit_ratio",
    "whale_position_reconstruction_available",
    "whale_holding_profit_available",
    "trusted_whale_exit_to_entry_ratio",
    "trusted_whale_unmatched_sell_ratio",
    "trusted_whale_partial_exit_ratio",
    "trusted_whale_full_exit_ratio",
    "trusted_whale_position_reconstruction_available",
    "trusted_whale_holding_profit_available",
)

SHORT_TERM_DERIVED_COLUMNS = (
    "trend_short_mean_delta",
    "trend_short_max_abs_delta",
    "trend_long_max_abs_delta",
    "trend_short_long_gap",
    "trend_consistency_score",
    "trend_reversal_1h_vs_6h",
    "trend_reversal_2h_vs_24h",
    "trend_observed_short_count",
    "time_to_close_inverse",
    "near_close_6h",
    "near_close_12h",
    "whale_recent_pressure_accel_1h_6h",
    "whale_recent_pressure_accel_6h_24h",
    "trusted_whale_recent_pressure_accel_1h_6h",
    "trusted_whale_recent_pressure_accel_6h_24h",
    "whale_recent_entry_exit_gap_12h",
    "whale_recent_entry_exit_gap_24h",
    "trusted_whale_recent_entry_exit_gap_12h",
    "trusted_whale_recent_entry_exit_gap_24h",
    "trend_2h_x_whale_pressure_12h",
    "trend_6h_x_whale_pressure_24h",
    "trend_2h_x_trusted_whale_pressure_12h",
    "trend_6h_x_trusted_whale_pressure_24h",
    "near_close_x_whale_pressure_12h",
)


def _pct(value: float) -> float:
    """Return a probability value as percentage points."""
    return _round(float(value) * 100.0, 4)


def _safe_number(value: Any) -> float:
    """Coerce sparse CSV values into finite floats."""
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if number != number or number in (float("inf"), float("-inf")):
        return 0.0
    return number


def _safe_ratio(numerator: float, denominator: float) -> float:
    """Return zero-safe ratio."""
    return float(numerator) / float(denominator) if float(denominator) > 0 else 0.0


def _safe_segment(value: Any) -> str:
    """Return a compact feature-safe segment name."""
    raw = str(value or "unknown").strip().lower()
    safe = "".join(character if character.isalnum() else "_" for character in raw)
    safe = "_".join(part for part in safe.split("_") if part)
    return safe[:48] or "unknown"


def _parse_datetime(value: Any) -> datetime | None:
    """Parse dataset timestamps without failing report generation."""
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _direction(delta: float, threshold: float) -> str:
    """Return a thresholded movement direction."""
    if delta > threshold:
        return "up"
    if delta < -threshold:
        return "down"
    return "flat"


def _trend_columns() -> tuple[str, ...]:
    """Return time-series trend features added from current price history."""
    columns = [
        column
        for lookback_hours in TREND_LOOKBACK_HOURS
        for column in (
            f"trend_delta_{lookback_hours}h",
            f"trend_abs_delta_{lookback_hours}h",
            f"trend_slope_{lookback_hours}h",
            f"trend_observed_{lookback_hours}h",
        )
    ]
    return (*columns, "trend_acceleration_6h", "trend_acceleration_24h", "trend_points_24h")


def _recent_whale_columns(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    """Return recent whale pressure columns present in the dataset."""
    if not rows:
        return ()
    names = set().union(*(row.keys() for row in rows[: min(len(rows), 20)]))
    return tuple(
        sorted(
            name
            for name in names
            if name.startswith(("whale_side_recent_", "trusted_whale_side_recent_"))
        )
    )


def _top_values(rows: list[dict[str, Any]], key: str, *, limit: int) -> tuple[str, ...]:
    """Return frequent row values as safe feature labels."""
    counts = Counter(_safe_segment(row.get(key) or "uncategorized") for row in rows)
    return tuple(value for value, _ in counts.most_common(limit))


def _category_interaction_columns(categories: tuple[str, ...]) -> tuple[str, ...]:
    """Return category-specific whale-trust interaction features."""
    suffixes = (
        "whale_weighted_net_pressure",
        "whale_notional_share",
        "trusted_whale_weighted_net_pressure",
        "trusted_whale_notional_share",
        "whale_entry_exit_gap",
        "whale_realized_roi",
        "whale_recent_net_pressure_12h",
        "whale_recent_net_pressure_24h",
    )
    return tuple(f"category_{category}_{suffix}" for category in categories for suffix in suffixes)


def _segment_onehot_columns(*segments: tuple[str, ...]) -> tuple[str, ...]:
    """Return one-hot segment feature columns."""
    names: list[str] = []
    for prefix, values in segments:
        names.extend(f"{prefix}_{value}" for value in values)
    return tuple(names)


def _movement_direction(row: dict[str, Any], window_name: str, threshold: float) -> str:
    """Return the actual future movement direction for a prediction window."""
    current_odds = _current_odds(row)
    future_odds = _safe_float(row, f"future_price_side_{window_name}")
    return _direction(future_odds - current_odds, threshold)


def _time_to_close_bucket(hours_to_close: float) -> str:
    """Return a stable close-pressure bucket for diagnostics."""
    if hours_to_close <= 6:
        return "0-6h"
    if hours_to_close <= 12:
        return "6-12h"
    if hours_to_close <= 24:
        return "12-24h"
    if hours_to_close <= 72:
        return "24-72h"
    return "72h+"


def _baseline_direction(row: dict[str, Any], threshold: float) -> str:
    """Return a deterministic momentum-only trend baseline."""
    for lookback_hours in (2, 3, 6, 12, 24, 1):
        if _safe_number(row.get(f"trend_observed_{lookback_hours}h")) >= 0.5:
            return _direction(_safe_number(row.get(f"trend_delta_{lookback_hours}h")), threshold)
    return "flat"


def _market_text(row: dict[str, Any]) -> str:
    """Return normalized market text for lightweight segment diagnostics."""
    return " ".join(
        str(row.get(column) or "")
        for column in ("event_category", "market_slug", "question", "event_title", "event_slug")
    ).lower().replace("-", " ")


def _is_crypto_record(row: dict[str, Any]) -> bool:
    """Return whether a row belongs to the crypto focus set."""
    category = str(row.get("event_category") or "").strip().casefold().replace("_", "-")
    if category == "crypto":
        return True
    family = str(row.get("market_family") or _market_family_segment(row)).strip().casefold()
    if "crypto" in family:
        return True
    text = f" {_market_text(row)} "
    tokens = (
        " crypto ",
        " bitcoin",
        " btc ",
        " ethereum",
        " eth ",
        " solana",
        " sol ",
        " xrp ",
        " doge",
        " dogecoin",
        " token ",
    )
    return any(token in text for token in tokens)


def _crypto_asset_segment(row: dict[str, Any]) -> str:
    """Return a stable crypto asset segment when the market text is specific enough."""
    padded = f" {_market_text(row)} "
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
    for asset, tokens in asset_tokens:
        if any(token in padded for token in tokens):
            return asset
    return "basket_or_other"


def _pressure_direction(value: float, *, threshold: float = 1e-9) -> str:
    """Return a directional label for signed whale pressure."""
    if value > threshold:
        return "up"
    if value < -threshold:
        return "down"
    return "neutral"


def _crypto_volatility_bucket(row: dict[str, Any]) -> str:
    """Return a coarse crypto volatility segment from recent observed odds movement."""
    max_abs_delta = max(
        abs(_safe_number(row.get("trend_short_max_abs_delta"))),
        abs(_safe_number(row.get("trend_long_max_abs_delta"))),
        abs(_safe_number(row.get("trend_delta_6h"))),
        abs(_safe_number(row.get("trend_delta_24h"))),
    )
    if max_abs_delta < 0.01:
        return "calm_lt_1pt"
    if max_abs_delta < 0.03:
        return "moving_1_3pts"
    if max_abs_delta < 0.08:
        return "volatile_3_8pts"
    return "jump_8pts_plus"


def _crypto_pressure_payload(
    *,
    row: dict[str, Any],
    window_name: str,
    predicted_direction: str,
) -> dict[str, Any]:
    """Return pressure-quality diagnostics used for crypto Watch gating."""
    if not _is_crypto_record(row):
        return {
            "is_crypto": False,
            "crypto_asset": None,
            "crypto_volatility_bucket": None,
            "crypto_pressure_source": None,
            "crypto_pressure_direction": None,
            "crypto_pressure_quality": None,
            "crypto_signal_conflict": False,
            "crypto_price_signal_conflict": False,
            "crypto_pressure_prediction_aligned": None,
            "crypto_pressure_price_aligned": None,
        }

    trusted_pressure = _safe_number(row.get(f"trusted_whale_side_recent_weighted_net_pressure_{window_name}"))
    trusted_trade_count = _safe_number(row.get(f"trusted_whale_side_recent_trade_count_{window_name}"))
    raw_pressure = _safe_number(row.get(f"whale_side_recent_weighted_net_pressure_{window_name}"))
    raw_trade_count = _safe_number(row.get(f"whale_side_recent_trade_count_{window_name}"))
    trusted_aggregate_pressure = _safe_number(row.get("trusted_whale_side_weighted_net_pressure"))
    raw_aggregate_pressure = _safe_number(row.get("whale_side_weighted_net_pressure"))
    if trusted_trade_count or abs(trusted_pressure) > 1e-9:
        pressure = trusted_pressure
        source = "trusted_recent"
    elif raw_trade_count or abs(raw_pressure) > 1e-9:
        pressure = raw_pressure
        source = "raw_recent"
    elif abs(trusted_aggregate_pressure) > 1e-9:
        pressure = trusted_aggregate_pressure
        source = "trusted_aggregate"
    elif abs(raw_aggregate_pressure) > 1e-9:
        pressure = raw_aggregate_pressure
        source = "raw_aggregate"
    else:
        pressure = 0.0
        source = "none"

    pressure_direction = _pressure_direction(pressure)
    baseline_direction = str(row.get("baseline_direction") or "flat")
    predicted = str(predicted_direction or "flat")
    pressure_is_directional = pressure_direction in {"up", "down"}
    baseline_is_directional = baseline_direction in {"up", "down"}
    predicted_is_directional = predicted in {"up", "down"}
    pressure_price_aligned = (
        pressure_direction == baseline_direction if pressure_is_directional and baseline_is_directional else None
    )
    pressure_prediction_aligned = (
        pressure_direction == predicted if pressure_is_directional and predicted_is_directional else None
    )

    if source == "none" or not pressure_is_directional:
        quality = "weak_pressure"
    elif source.startswith("trusted") and pressure_price_aligned is False:
        quality = "trusted_price_conflict"
    elif source.startswith("raw") and pressure_price_aligned is False:
        quality = "raw_price_conflict"
    elif source.startswith("trusted") and pressure_price_aligned is True:
        quality = "trusted_price_aligned"
    elif source.startswith("raw") and pressure_price_aligned is True:
        quality = "raw_price_aligned"
    else:
        quality = f"{source.split('_', maxsplit=1)[0]}_directional"

    return {
        "is_crypto": True,
        "crypto_asset": _crypto_asset_segment(row),
        "crypto_volatility_bucket": _crypto_volatility_bucket(row),
        "crypto_pressure_source": source,
        "crypto_pressure_direction": pressure_direction,
        "crypto_pressure_quality": quality,
        "crypto_signal_conflict": bool(pressure_prediction_aligned is False),
        "crypto_price_signal_conflict": bool(
            baseline_is_directional and predicted_is_directional and baseline_direction != predicted
        ),
        "crypto_pressure_prediction_aligned": pressure_prediction_aligned,
        "crypto_pressure_price_aligned": pressure_price_aligned,
    }


def _add_position_features(row: dict[str, Any], prefix: str, output_prefix: str) -> None:
    """Add reconstructed entry/exit quality features without treating every sell as a clean exit."""
    entry_count = _safe_number(row.get(f"{prefix}_entry_trade_count"))
    exit_count = _safe_number(row.get(f"{prefix}_exit_trade_count"))
    partial_exit_count = _safe_number(row.get(f"{prefix}_partial_exit_count"))
    full_exit_count = _safe_number(row.get(f"{prefix}_full_exit_count"))
    unmatched_sell_count = _safe_number(row.get(f"{prefix}_unmatched_sell_count"))
    total_exit_like = exit_count + unmatched_sell_count
    row[f"{output_prefix}_exit_to_entry_ratio"] = _safe_ratio(exit_count, entry_count + 1.0)
    row[f"{output_prefix}_unmatched_sell_ratio"] = _safe_ratio(unmatched_sell_count, total_exit_like + 1.0)
    row[f"{output_prefix}_partial_exit_ratio"] = _safe_ratio(partial_exit_count, exit_count + 1.0)
    row[f"{output_prefix}_full_exit_ratio"] = _safe_ratio(full_exit_count, exit_count + 1.0)
    row[f"{output_prefix}_position_reconstruction_available"] = 1.0 if entry_count or exit_count else 0.0
    row[f"{output_prefix}_holding_profit_available"] = (
        1.0
        if _safe_number(row.get(f"{prefix}_avg_holding_hours"))
        or _safe_number(row.get(f"{prefix}_realized_roi"))
        or _safe_number(row.get(f"{prefix}_avg_exit_profit"))
        else 0.0
    )


def _sign(value: float, threshold: float) -> int:
    """Return thresholded numeric sign."""
    if value > threshold:
        return 1
    if value < -threshold:
        return -1
    return 0


def _add_short_term_features(row: dict[str, Any], direction_threshold: float) -> None:
    """Add short-term trend, close-pressure, and whale-pressure acceleration features."""
    short_deltas = [_safe_number(row.get(f"trend_delta_{hours}h")) for hours in (1, 2, 3, 6)]
    long_deltas = [_safe_number(row.get(f"trend_delta_{hours}h")) for hours in (12, 24)]
    observed_short = [_safe_number(row.get(f"trend_observed_{hours}h")) for hours in (1, 2, 3, 6)]
    row["trend_short_mean_delta"] = sum(short_deltas) / len(short_deltas)
    row["trend_short_max_abs_delta"] = max(abs(value) for value in short_deltas)
    row["trend_long_max_abs_delta"] = max(abs(value) for value in long_deltas)
    row["trend_short_long_gap"] = row["trend_short_mean_delta"] - (sum(long_deltas) / len(long_deltas))
    row["trend_consistency_score"] = sum(_sign(value, direction_threshold) for value in short_deltas)
    row["trend_reversal_1h_vs_6h"] = (
        1.0
        if _sign(_safe_number(row.get("trend_delta_1h")), direction_threshold)
        * _sign(_safe_number(row.get("trend_delta_6h")), direction_threshold)
        < 0
        else 0.0
    )
    row["trend_reversal_2h_vs_24h"] = (
        1.0
        if _sign(_safe_number(row.get("trend_delta_2h")), direction_threshold)
        * _sign(_safe_number(row.get("trend_delta_24h")), direction_threshold)
        < 0
        else 0.0
    )
    row["trend_observed_short_count"] = sum(1.0 for value in observed_short if value >= 0.5)
    hours_to_close = _safe_number(row.get("hours_to_close"))
    row["time_to_close_inverse"] = _safe_ratio(1.0, hours_to_close + 1.0)
    row["near_close_6h"] = 1.0 if 0.0 < hours_to_close <= 6.0 else 0.0
    row["near_close_12h"] = 1.0 if 0.0 < hours_to_close <= 12.0 else 0.0

    whale_pressure_1h = _safe_number(row.get("whale_side_recent_weighted_net_pressure_1h"))
    whale_pressure_6h = _safe_number(row.get("whale_side_recent_weighted_net_pressure_6h"))
    whale_pressure_12h = _safe_number(row.get("whale_side_recent_weighted_net_pressure_12h"))
    whale_pressure_24h = _safe_number(row.get("whale_side_recent_weighted_net_pressure_24h"))
    trusted_pressure_1h = _safe_number(row.get("trusted_whale_side_recent_weighted_net_pressure_1h"))
    trusted_pressure_6h = _safe_number(row.get("trusted_whale_side_recent_weighted_net_pressure_6h"))
    trusted_pressure_12h = _safe_number(row.get("trusted_whale_side_recent_weighted_net_pressure_12h"))
    trusted_pressure_24h = _safe_number(row.get("trusted_whale_side_recent_weighted_net_pressure_24h"))
    row["whale_recent_pressure_accel_1h_6h"] = whale_pressure_1h - whale_pressure_6h
    row["whale_recent_pressure_accel_6h_24h"] = whale_pressure_6h - whale_pressure_24h
    row["trusted_whale_recent_pressure_accel_1h_6h"] = trusted_pressure_1h - trusted_pressure_6h
    row["trusted_whale_recent_pressure_accel_6h_24h"] = trusted_pressure_6h - trusted_pressure_24h
    row["whale_recent_entry_exit_gap_12h"] = _safe_number(
        row.get("whale_side_recent_entry_trade_count_12h")
    ) - _safe_number(row.get("whale_side_recent_exit_trade_count_12h"))
    row["whale_recent_entry_exit_gap_24h"] = _safe_number(
        row.get("whale_side_recent_entry_trade_count_24h")
    ) - _safe_number(row.get("whale_side_recent_exit_trade_count_24h"))
    row["trusted_whale_recent_entry_exit_gap_12h"] = _safe_number(
        row.get("trusted_whale_side_recent_entry_trade_count_12h")
    ) - _safe_number(row.get("trusted_whale_side_recent_exit_trade_count_12h"))
    row["trusted_whale_recent_entry_exit_gap_24h"] = _safe_number(
        row.get("trusted_whale_side_recent_entry_trade_count_24h")
    ) - _safe_number(row.get("trusted_whale_side_recent_exit_trade_count_24h"))
    row["trend_2h_x_whale_pressure_12h"] = _safe_number(row.get("trend_delta_2h")) * whale_pressure_12h
    row["trend_6h_x_whale_pressure_24h"] = _safe_number(row.get("trend_delta_6h")) * whale_pressure_24h
    row["trend_2h_x_trusted_whale_pressure_12h"] = _safe_number(row.get("trend_delta_2h")) * trusted_pressure_12h
    row["trend_6h_x_trusted_whale_pressure_24h"] = _safe_number(row.get("trend_delta_6h")) * trusted_pressure_24h
    row["near_close_x_whale_pressure_12h"] = row["near_close_12h"] * whale_pressure_12h


def _prepare_rows(rows: list[dict[str, Any]], direction_threshold: float) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    """Add trend-classifier features and return the selected feature columns."""
    _enrich_trend_features(rows)
    categories = _top_values(rows, "event_category", limit=TOP_SEGMENT_LIMIT)
    families = tuple(sorted({_safe_segment(_market_family_segment(row)) for row in rows}))
    research_focuses = tuple(sorted({_safe_segment(_research_focus_segment(row)) for row in rows}))
    category_interactions = _category_interaction_columns(categories)
    onehot_columns = _segment_onehot_columns(
        ("event_category", categories),
        ("market_family", families),
        ("research_focus", research_focuses),
    )
    for row in rows:
        category = _safe_segment(row.get("event_category") or "uncategorized")
        family = _safe_segment(_market_family_segment(row))
        research_focus = _safe_segment(_research_focus_segment(row))
        row["baseline_direction"] = _baseline_direction(row, direction_threshold)
        row["market_family"] = _market_family_segment(row)
        row["research_focus"] = _research_focus_segment(row)
        _add_position_features(row, "whale_side", "whale")
        _add_position_features(row, "trusted_whale_side", "trusted_whale")
        _add_short_term_features(row, direction_threshold)
        for value in categories:
            active = 1.0 if category == value else 0.0
            row[f"event_category_{value}"] = active
            row[f"category_{value}_whale_weighted_net_pressure"] = active * _safe_number(
                row.get("whale_side_weighted_net_pressure")
            )
            row[f"category_{value}_whale_notional_share"] = active * _safe_number(
                row.get("whale_side_notional_share")
            )
            row[f"category_{value}_trusted_whale_weighted_net_pressure"] = active * _safe_number(
                row.get("trusted_whale_side_weighted_net_pressure")
            )
            row[f"category_{value}_trusted_whale_notional_share"] = active * _safe_number(
                row.get("trusted_whale_side_notional_share")
            )
            row[f"category_{value}_whale_entry_exit_gap"] = active * _safe_number(
                row.get("whale_side_entry_exit_gap")
            )
            row[f"category_{value}_whale_realized_roi"] = active * _safe_number(row.get("whale_side_realized_roi"))
            row[f"category_{value}_whale_recent_net_pressure_12h"] = active * _safe_number(
                row.get("whale_side_recent_weighted_net_pressure_12h")
            )
            row[f"category_{value}_whale_recent_net_pressure_24h"] = active * _safe_number(
                row.get("whale_side_recent_weighted_net_pressure_24h")
            )
        for value in families:
            row[f"market_family_{value}"] = 1.0 if family == value else 0.0
        for value in research_focuses:
            row[f"research_focus_{value}"] = 1.0 if research_focus == value else 0.0
    feature_columns = (
        *BASE_FEATURE_COLUMNS,
        *_trend_columns(),
        *_recent_whale_columns(rows),
        *POSITION_DERIVED_COLUMNS,
        *SHORT_TERM_DERIVED_COLUMNS,
        *category_interactions,
        *onehot_columns,
    )
    return rows, tuple(dict.fromkeys(feature_columns))


def _feature_matrix(rows: list[dict[str, Any]], feature_columns: tuple[str, ...]) -> list[list[float]]:
    """Return sparse-safe numeric feature vectors."""
    return [[_safe_number(row.get(column)) for column in feature_columns] for row in rows]


def _target_rows(rows: list[dict[str, Any]], window_name: str, direction_threshold: float) -> list[dict[str, Any]]:
    """Return rows with observed future price labels for one window."""
    target_rows: list[dict[str, Any]] = []
    for row in rows:
        if _safe_number(row.get(f"future_price_observed_{window_name}")) < 0.5:
            continue
        row[f"direction_target_{window_name}"] = _movement_direction(row, window_name, direction_threshold)
        target_rows.append(row)
    return target_rows


def _base_classifier(random_state: int) -> Any:
    """Return the base balanced up/flat/down classifier."""
    from sklearn.ensemble import RandomForestClassifier

    return RandomForestClassifier(
        n_estimators=220,
        max_depth=9,
        min_samples_leaf=4,
        class_weight="balanced_subsample",
        random_state=random_state,
        n_jobs=-1,
    )


def _fit_classifier(
    train_rows: list[dict[str, Any]],
    feature_columns: tuple[str, ...],
    random_state: int,
    calibration_method: str,
) -> tuple[Any, dict[str, Any]]:
    """Fit a balanced classifier with optional probability calibration."""
    from sklearn.calibration import CalibratedClassifierCV

    labels = [row["_direction_target"] for row in train_rows]
    model = _base_classifier(random_state)
    if calibration_method == "none":
        model.fit(_feature_matrix(train_rows, feature_columns), labels)
        return model, {"method": "none", "status": "disabled", "cv": 0}

    class_counts = Counter(labels)
    calibration_cv = min(3, min(class_counts.values())) if class_counts else 0
    if calibration_cv < 2:
        model.fit(_feature_matrix(train_rows, feature_columns), labels)
        return model, {
            "method": calibration_method,
            "status": "insufficient_class_count",
            "cv": 0,
            "min_class_count": min(class_counts.values()) if class_counts else 0,
        }

    calibration_kwargs: dict[str, Any] = {
        "method": calibration_method,
        "cv": calibration_cv,
        "n_jobs": -1,
    }
    signature = inspect.signature(CalibratedClassifierCV)
    if "estimator" in signature.parameters:
        calibration_kwargs["estimator"] = model
    else:
        calibration_kwargs["base_estimator"] = model
    calibrated_model = CalibratedClassifierCV(**calibration_kwargs)
    calibrated_model.fit(_feature_matrix(train_rows, feature_columns), labels)
    return calibrated_model, {
        "method": calibration_method,
        "status": "applied",
        "cv": calibration_cv,
        "min_class_count": min(class_counts.values()),
    }


def _probability_payload(model: Any, probabilities: list[float]) -> dict[str, float]:
    """Return a stable label-probability mapping from sklearn output."""
    classes = [str(value) for value in getattr(model, "classes_", [])]
    raw = {label: 0.0 for label in LABELS}
    raw.update({label: float(value) for label, value in zip(classes, probabilities, strict=True)})
    return {label: _round(raw[label], 6) for label in LABELS}


def _prediction_records(
    *,
    rows: list[dict[str, Any]],
    feature_columns: tuple[str, ...],
    window_name: str,
    direction_threshold: float,
    random_state: int,
    calibration_method: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Run grouped rolling classification and return records plus fold summaries."""
    target_rows = _target_rows(rows, window_name, direction_threshold)
    splits, _, _ = _build_rolling_splits(target_rows)
    records: list[dict[str, Any]] = []
    folds: list[dict[str, Any]] = []
    for split in splits:
        train_rows = [dict(row, _direction_target=row[f"direction_target_{window_name}"]) for row in split["train_rows"]]
        test_rows = [dict(row, _direction_target=row[f"direction_target_{window_name}"]) for row in split["test_rows"]]
        train_labels = {row["_direction_target"] for row in train_rows}
        if len(train_labels) < 2:
            continue
        model, calibration = _fit_classifier(
            train_rows,
            feature_columns,
            random_state + int(split["fold_index"]),
            calibration_method,
        )
        probabilities = model.predict_proba(_feature_matrix(test_rows, feature_columns))
        fold_records: list[dict[str, Any]] = []
        for row, probability_row in zip(test_rows, probabilities, strict=True):
            probability_map = _probability_payload(model, [float(value) for value in probability_row])
            ordered_probs = sorted(probability_map.items(), key=lambda item: item[1], reverse=True)
            predicted_direction = ordered_probs[0][0]
            confidence = float(ordered_probs[0][1])
            margin = confidence - float(ordered_probs[1][1]) if len(ordered_probs) > 1 else confidence
            actual_direction = str(row["_direction_target"])
            current_odds = _current_odds(row)
            future_odds = _safe_float(row, f"future_price_side_{window_name}")
            hours_to_close = _safe_number(row.get("hours_to_close"))
            whale_recent_trade_count = _safe_number(row.get(f"whale_side_recent_trade_count_{window_name}"))
            trusted_whale_recent_trade_count = _safe_number(
                row.get(f"trusted_whale_side_recent_trade_count_{window_name}")
            )
            whale_recent_pressure = _safe_number(row.get(f"whale_side_recent_weighted_net_pressure_{window_name}"))
            trusted_whale_recent_pressure = _safe_number(
                row.get(f"trusted_whale_side_recent_weighted_net_pressure_{window_name}")
            )
            whale_position_available = bool(_safe_number(row.get("whale_position_reconstruction_available")))
            trusted_whale_position_available = bool(
                _safe_number(row.get("trusted_whale_position_reconstruction_available"))
            )
            whale_holding_profit_available = bool(_safe_number(row.get("whale_holding_profit_available")))
            trusted_whale_holding_profit_available = bool(
                _safe_number(row.get("trusted_whale_holding_profit_available"))
            )
            whale_recent_activity_available = bool(whale_recent_trade_count or abs(whale_recent_pressure) > 0.0)
            trusted_whale_recent_activity_available = bool(
                trusted_whale_recent_trade_count or abs(trusted_whale_recent_pressure) > 0.0
            )
            whale_signal_available = bool(
                whale_recent_activity_available
                or trusted_whale_recent_activity_available
                or whale_position_available
                or trusted_whale_position_available
                or whale_holding_profit_available
                or trusted_whale_holding_profit_available
            )
            trusted_whale_signal_available = bool(
                trusted_whale_recent_activity_available
                or trusted_whale_position_available
                or trusted_whale_holding_profit_available
            )
            crypto_pressure = _crypto_pressure_payload(
                row=row,
                window_name=window_name,
                predicted_direction=predicted_direction,
            )
            record = {
                "window": window_name,
                "fold_index": int(split["fold_index"]),
                "condition_ref": str(row[GROUP_KEY_COLUMN]),
                "market_slug": str(row["market_slug"]),
                "question": str(row["question"]),
                "side_label": str(row["side_label"]),
                "observation_time": str(row["observation_time"]),
                "event_category": str(row.get("event_category") or "uncategorized"),
                "market_family": _market_family_segment(row),
                "research_focus": _research_focus_segment(row),
                "hours_to_close": _round(hours_to_close, 4),
                "time_to_close_bucket": _time_to_close_bucket(hours_to_close),
                "current_odds_pct": _pct(current_odds),
                "actual_future_odds_pct": _pct(future_odds),
                "actual_delta_pts": _pct(future_odds - current_odds),
                "actual_direction": actual_direction,
                "predicted_direction": predicted_direction,
                "baseline_direction": str(row.get("baseline_direction") or "flat"),
                "confidence": _round(confidence, 6),
                "margin": _round(margin, 6),
                "probabilities": probability_map,
                "correct": predicted_direction == actual_direction,
                "baseline_correct": str(row.get("baseline_direction") or "flat") == actual_direction,
                "nonflat_actual": actual_direction != "flat",
                "trend_delta_2h_pts": _pct(_safe_number(row.get("trend_delta_2h"))),
                "trend_delta_6h_pts": _pct(_safe_number(row.get("trend_delta_6h"))),
                "whale_recent_net_pressure_12h": _round(
                    _safe_number(row.get("whale_side_recent_weighted_net_pressure_12h"))
                ),
                "whale_recent_net_pressure_24h": _round(
                    _safe_number(row.get("whale_side_recent_weighted_net_pressure_24h"))
                ),
                "trusted_whale_recent_net_pressure": _round(trusted_whale_recent_pressure),
                "whale_recent_trade_count": _round(whale_recent_trade_count, 4),
                "trusted_whale_recent_trade_count": _round(trusted_whale_recent_trade_count, 4),
                "whale_recent_activity_available": whale_recent_activity_available,
                "trusted_whale_recent_activity_available": trusted_whale_recent_activity_available,
                "whale_position_reconstruction_available": whale_position_available,
                "trusted_whale_position_reconstruction_available": trusted_whale_position_available,
                "whale_holding_profit_available": whale_holding_profit_available,
                "trusted_whale_holding_profit_available": trusted_whale_holding_profit_available,
                "whale_signal_available": whale_signal_available,
                "trusted_whale_signal_available": trusted_whale_signal_available,
                **crypto_pressure,
            }
            records.append(record)
            fold_records.append(record)
        folds.append(
            {
                "fold_index": int(split["fold_index"]),
                "train_rows": len(train_rows),
                "test_rows": len(test_rows),
                "train_direction_counts": dict(sorted(Counter(row["_direction_target"] for row in train_rows).items())),
                "test_direction_counts": dict(sorted(Counter(row["_direction_target"] for row in test_rows).items())),
                "calibration_method": calibration["method"],
                "calibration_status": calibration["status"],
                "calibration_cv": calibration["cv"],
                "calibration_min_class_count": calibration.get("min_class_count"),
                "accuracy_pct": _accuracy(fold_records),
                "nonflat_accuracy_pct": _accuracy([record for record in fold_records if record["nonflat_actual"]]),
            }
        )
    return records, folds


def _annotate_prediction_stability(records: list[dict[str, Any]]) -> None:
    """Attach recent prediction-direction stability fields to each row."""
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for record in records:
        key = (
            str(record.get("condition_ref") or ""),
            str(record.get("side_label") or ""),
            str(record.get("window") or ""),
        )
        grouped.setdefault(key, []).append(record)

    for group_records in grouped.values():
        group_records.sort(key=lambda record: str(record.get("observation_time") or ""))
        for index, record in enumerate(group_records):
            sample = group_records[max(0, index - STABILITY_LOOKBACK_RECORDS + 1) : index + 1]
            current_direction = str(record.get("predicted_direction") or "flat")
            sample_count = len(sample)
            agreement_count = sum(
                1
                for sample_record in sample
                if str(sample_record.get("predicted_direction") or "flat") == current_direction
            )
            streak_count = 0
            for sample_record in reversed(sample):
                if str(sample_record.get("predicted_direction") or "flat") != current_direction:
                    break
                streak_count += 1
            timestamps = [
                parsed
                for parsed in (_parse_datetime(sample_record.get("observation_time")) for sample_record in sample)
                if parsed is not None
            ]
            span_hours = (
                (max(timestamps) - min(timestamps)).total_seconds() / 3600.0
                if len(timestamps) >= 2
                else None
            )
            agreement_pct = _pct(agreement_count / sample_count) if sample_count else 0.0
            stability_available = sample_count >= STABILITY_MIN_OBSERVATIONS
            is_stable = (
                current_direction != "flat"
                and stability_available
                and agreement_pct >= STABILITY_MIN_AGREEMENT_PCT
            )
            record["prediction_stability_lookback_records"] = STABILITY_LOOKBACK_RECORDS
            record["prediction_stability_min_observations"] = STABILITY_MIN_OBSERVATIONS
            record["prediction_stability_min_agreement_pct"] = STABILITY_MIN_AGREEMENT_PCT
            record["prediction_stability_sample_count"] = sample_count
            record["prediction_stability_agreement_count"] = agreement_count
            record["prediction_stability_agreement_pct"] = agreement_pct
            record["prediction_stability_streak_count"] = streak_count
            record["prediction_stability_span_hours"] = _round(span_hours, 4) if span_hours is not None else None
            record["prediction_stability_available"] = stability_available
            record["prediction_stability_is_stable"] = is_stable


def _accuracy(records: list[dict[str, Any]]) -> float:
    """Return classifier accuracy percentage for records."""
    return _pct(sum(1 for record in records if record["correct"]) / len(records)) if records else 0.0


def _baseline_accuracy(records: list[dict[str, Any]]) -> float:
    """Return momentum baseline accuracy percentage for records."""
    return _pct(sum(1 for record in records if record["baseline_correct"]) / len(records)) if records else 0.0


def _macro_f1(records: list[dict[str, Any]]) -> float:
    """Return macro F1 without requiring sklearn metrics serialization."""
    scores: list[float] = []
    for label in LABELS:
        true_positive = sum(
            1 for record in records if record["actual_direction"] == label and record["predicted_direction"] == label
        )
        false_positive = sum(
            1 for record in records if record["actual_direction"] != label and record["predicted_direction"] == label
        )
        false_negative = sum(
            1 for record in records if record["actual_direction"] == label and record["predicted_direction"] != label
        )
        precision = _safe_ratio(true_positive, true_positive + false_positive)
        recall = _safe_ratio(true_positive, true_positive + false_negative)
        scores.append(_safe_ratio(2.0 * precision * recall, precision + recall))
    return _round(sum(scores) / len(scores), 6)


def _calibration_bins(records: list[dict[str, Any]], bin_count: int = 10) -> list[dict[str, Any]]:
    """Return confidence-bin reliability rows."""
    rows: list[dict[str, Any]] = []
    for bin_index in range(bin_count):
        low = bin_index / bin_count
        high = (bin_index + 1) / bin_count
        if bin_index == bin_count - 1:
            bucket = [record for record in records if low <= float(record["confidence"]) <= high]
        else:
            bucket = [record for record in records if low <= float(record["confidence"]) < high]
        if not bucket:
            rows.append(
                {
                    "confidence_min": _round(low, 4),
                    "confidence_max": _round(high, 4),
                    "row_count": 0,
                    "average_confidence_pct": 0.0,
                    "accuracy_pct": 0.0,
                    "calibration_gap_pct": 0.0,
                }
            )
            continue
        average_confidence = sum(float(record["confidence"]) for record in bucket) / len(bucket)
        accuracy = sum(1 for record in bucket if record["correct"]) / len(bucket)
        rows.append(
            {
                "confidence_min": _round(low, 4),
                "confidence_max": _round(high, 4),
                "row_count": len(bucket),
                "average_confidence_pct": _pct(average_confidence),
                "accuracy_pct": _pct(accuracy),
                "calibration_gap_pct": _pct(abs(average_confidence - accuracy)),
            }
        )
    return rows


def _calibration_metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return probability-quality metrics for classifier confidence."""
    if not records:
        return {
            "row_count": 0,
            "average_confidence": 0.0,
            "average_confidence_pct": 0.0,
            "accuracy_pct": 0.0,
            "expected_calibration_error": 0.0,
            "expected_calibration_error_pct": 0.0,
            "mean_class_brier_score": 0.0,
            "bins": _calibration_bins(records),
        }

    brier_total = 0.0
    for record in records:
        probabilities = record.get("probabilities") or {}
        for label in LABELS:
            expected = 1.0 if str(record["actual_direction"]) == label else 0.0
            brier_total += (float(probabilities.get(label, 0.0)) - expected) ** 2
    bins = _calibration_bins(records)
    expected_calibration_error = sum(
        (int(row["row_count"]) / len(records)) * (float(row["calibration_gap_pct"]) / 100.0)
        for row in bins
    )
    average_confidence = sum(float(record["confidence"]) for record in records) / len(records)
    return {
        "row_count": len(records),
        "average_confidence": _round(average_confidence, 6),
        "average_confidence_pct": _pct(average_confidence),
        "accuracy_pct": _accuracy(records),
        "expected_calibration_error": _round(expected_calibration_error, 6),
        "expected_calibration_error_pct": _pct(expected_calibration_error),
        "mean_class_brier_score": _round(brier_total / (len(records) * len(LABELS)), 6),
        "bins": bins,
    }


def _compact_calibration_diagnostics(
    records: list[dict[str, Any]],
    *,
    key: str,
    min_rows: int,
) -> list[dict[str, Any]]:
    """Return compact reliability diagnostics by one segment key."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(key) or "unknown"), []).append(record)
    rows: list[dict[str, Any]] = []
    for group_name, group_records in grouped.items():
        if len(group_records) < min_rows:
            continue
        display_records = [
            record
            for record in group_records
            if float(record["confidence"]) >= DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD
        ]
        movement_records = [
            record
            for record in display_records
            if str(record["predicted_direction"]) != "flat"
        ]
        calibration = _calibration_metrics(group_records)
        display_calibration = _calibration_metrics(display_records)
        rows.append(
            {
                "group": group_name,
                "row_count": len(group_records),
                "condition_count": len({record["condition_ref"] for record in group_records}),
                "direction_counts": dict(sorted(Counter(str(row["actual_direction"]) for row in group_records).items())),
                "accuracy_pct": _accuracy(group_records),
                "average_confidence_pct": calibration["average_confidence_pct"],
                "expected_calibration_error_pct": calibration["expected_calibration_error_pct"],
                "mean_class_brier_score": calibration["mean_class_brier_score"],
                "display_count": len(display_records),
                "display_accuracy_pct": _accuracy(display_records),
                "display_expected_calibration_error_pct": display_calibration["expected_calibration_error_pct"],
                "display_movement_signal_count": len(movement_records),
                "display_movement_signal_precision_pct": _accuracy(movement_records),
            }
        )
    rows.sort(
        key=lambda item: (
            -float(item["expected_calibration_error_pct"]),
            -int(item["row_count"]),
            str(item["group"]),
        )
    )
    return rows


def _coverage_table(records: list[dict[str, Any]], thresholds: tuple[float, ...]) -> list[dict[str, Any]]:
    """Return confidence-threshold coverage/precision rows."""
    rows: list[dict[str, Any]] = []
    nonflat_total = sum(1 for record in records if record["nonflat_actual"])
    for threshold in thresholds:
        accepted = [record for record in records if float(record["confidence"]) >= threshold]
        accepted_nonflat = [record for record in accepted if record["nonflat_actual"]]
        movement_accepted = [record for record in accepted if str(record["predicted_direction"]) != "flat"]
        rows.append(
            {
                "confidence_threshold": _round(threshold, 4),
                "accepted_count": len(accepted),
                "coverage_pct": _pct(len(accepted) / len(records)) if records else 0.0,
                "accuracy_pct": _accuracy(accepted),
                "nonflat_accepted_count": len(accepted_nonflat),
                "nonflat_coverage_pct": _pct(len(accepted_nonflat) / nonflat_total) if nonflat_total else 0.0,
                "nonflat_accuracy_pct": _accuracy(accepted_nonflat),
                "movement_signal_count": len(movement_accepted),
                "movement_signal_coverage_pct": _pct(len(movement_accepted) / len(records)) if records else 0.0,
                "movement_signal_precision_pct": _accuracy(movement_accepted),
                "movement_signal_nonflat_capture_pct": _pct(
                    sum(1 for record in movement_accepted if record["correct"] and record["nonflat_actual"]) / nonflat_total
                )
                if nonflat_total
                else 0.0,
                "up_precision_pct": _precision_for_label(accepted, "up"),
                "down_precision_pct": _precision_for_label(accepted, "down"),
            }
        )
    return rows


def _precision_for_label(records: list[dict[str, Any]], label: str) -> float:
    """Return precision for a predicted label."""
    predicted = [record for record in records if record["predicted_direction"] == label]
    if not predicted:
        return 0.0
    return _pct(sum(1 for record in predicted if record["actual_direction"] == label) / len(predicted))


def _threshold_row(
    records: list[dict[str, Any]],
    thresholds: tuple[float, ...],
    threshold: float,
) -> dict[str, Any]:
    """Return one coverage row for a requested threshold."""
    table = _coverage_table(records, thresholds)
    selected = next(
        (row for row in table if float(row["confidence_threshold"]) == _round(threshold, 4)),
        table[0] if table else {},
    )
    return dict(selected)


def _recommended_threshold(
    records: list[dict[str, Any]],
    thresholds: tuple[float, ...],
    *,
    min_coverage: float = DEFAULT_MIN_THRESHOLD_COVERAGE,
) -> dict[str, Any]:
    """Choose a practical threshold for up/down signals in one row group."""
    if not records:
        return {"confidence_threshold": DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD}
    coverage_rows = _coverage_table(records, thresholds)
    nonflat_total = sum(1 for record in records if record["nonflat_actual"])
    minimum_accepted_count = max(8, int(len(records) * min_coverage))
    minimum_movement_signal_count = max(20, int(nonflat_total * min_coverage))
    viable = [
        row
        for row in coverage_rows
        if int(row["accepted_count"]) >= minimum_accepted_count
        and int(row["movement_signal_count"]) >= minimum_movement_signal_count
    ]
    if not viable:
        viable = [row for row in coverage_rows if int(row["movement_signal_count"]) >= 3] or coverage_rows
    selected = max(
        viable,
        key=lambda row: (
            float(row["movement_signal_precision_pct"]),
            float(row["nonflat_accuracy_pct"]),
            float(row["movement_signal_nonflat_capture_pct"]),
            -float(row["confidence_threshold"]),
        ),
    )
    selected = dict(selected)
    selected["selection_rule"] = "maximize_movement_precision_with_minimum_coverage"
    selected["minimum_accepted_count"] = minimum_accepted_count
    selected["minimum_movement_signal_count"] = minimum_movement_signal_count
    return selected


def _confusion_matrix(records: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """Return actual-label by predicted-label counts."""
    matrix = {actual: {predicted: 0 for predicted in LABELS} for actual in LABELS}
    for record in records:
        matrix[str(record["actual_direction"])][str(record["predicted_direction"])] += 1
    return matrix


def _summary(records: list[dict[str, Any]], thresholds: tuple[float, ...]) -> dict[str, Any]:
    """Return compact classifier metrics for a record group."""
    nonflat_records = [record for record in records if record["nonflat_actual"]]
    display_records = [
        record for record in records if float(record["confidence"]) >= DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD
    ]
    display_movement_records = [
        record for record in display_records if str(record["predicted_direction"]) != "flat"
    ]
    recommended_threshold = _recommended_threshold(records, thresholds)
    return {
        "row_count": len(records),
        "condition_count": len({record["condition_ref"] for record in records}),
        "direction_counts": dict(sorted(Counter(str(record["actual_direction"]) for record in records).items())),
        "prediction_counts": dict(sorted(Counter(str(record["predicted_direction"]) for record in records).items())),
        "accuracy_pct": _accuracy(records),
        "baseline_accuracy_pct": _baseline_accuracy(records),
        "accuracy_delta_vs_baseline_pts": _round(_accuracy(records) - _baseline_accuracy(records), 4),
        "nonflat_accuracy_pct": _accuracy(nonflat_records),
        "nonflat_baseline_accuracy_pct": _baseline_accuracy(nonflat_records),
        "macro_f1": _macro_f1(records),
        "average_confidence": _round(
            sum(float(record["confidence"]) for record in records) / len(records)
        )
        if records
        else 0.0,
        "calibration": _calibration_metrics(records),
        "display_confidence_threshold": DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD,
        "display_coverage_pct": _pct(len(display_records) / len(records)) if records else 0.0,
        "display_accuracy_pct": _accuracy(display_records),
        "display_nonflat_accuracy_pct": _accuracy([record for record in display_records if record["nonflat_actual"]]),
        "display_calibration": _calibration_metrics(display_records),
        "display_movement_signal_count": len(display_movement_records),
        "display_movement_signal_coverage_pct": _pct(len(display_movement_records) / len(records)) if records else 0.0,
        "display_movement_signal_precision_pct": _accuracy(display_movement_records),
        "recommended_confidence_threshold": recommended_threshold,
        "coverage": _coverage_table(records, thresholds),
        "confusion_matrix": _confusion_matrix(records),
    }


def _group_summaries(
    records: list[dict[str, Any]],
    *,
    key: str,
    thresholds: tuple[float, ...],
    min_rows: int,
) -> list[dict[str, Any]]:
    """Return grouped classifier summaries."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(key) or "unknown"), []).append(record)
    rows = [
        {"group": group_name, **_summary(group_records, thresholds)}
        for group_name, group_records in grouped.items()
        if len(group_records) >= min_rows
    ]
    rows.sort(key=lambda item: (-int(item["row_count"]), str(item["group"])))
    return rows


def _group_threshold_recommendations(
    records: list[dict[str, Any]],
    *,
    key: str,
    thresholds: tuple[float, ...],
    min_rows: int,
) -> list[dict[str, Any]]:
    """Return recommended confidence thresholds by group."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(key) or "unknown"), []).append(record)
    rows: list[dict[str, Any]] = []
    for group_name, group_records in grouped.items():
        if len(group_records) < min_rows:
            continue
        recommendation = _recommended_threshold(group_records, thresholds)
        rows.append(
            {
                "group": group_name,
                "row_count": len(group_records),
                "direction_counts": dict(sorted(Counter(str(row["actual_direction"]) for row in group_records).items())),
                **recommendation,
            }
        )
    rows.sort(
        key=lambda item: (
            -float(item.get("movement_signal_precision_pct", 0.0)),
            -int(item["row_count"]),
            str(item["group"]),
        )
    )
    return rows


def _apply_category_thresholds(
    records: list[dict[str, Any]],
    thresholds: tuple[float, ...],
    *,
    min_rows: int,
) -> list[dict[str, Any]]:
    """Annotate records with event-category recommended thresholds."""
    recommendations = _group_threshold_recommendations(
        records,
        key="event_category",
        thresholds=thresholds,
        min_rows=min_rows,
    )
    by_category = {
        str(row["group"]): float(row["confidence_threshold"])
        for row in recommendations
    }
    fallback = float(_recommended_threshold(records, thresholds).get("confidence_threshold", DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD))
    for record in records:
        threshold = by_category.get(str(record.get("event_category")), fallback)
        record["category_recommended_confidence_threshold"] = _round(threshold, 4)
        record["accepted_at_category_threshold"] = float(record["confidence"]) >= threshold
        record["movement_signal_at_category_threshold"] = (
            bool(record["accepted_at_category_threshold"]) and str(record["predicted_direction"]) != "flat"
        )
    return records


def _movement_signal_records(records: list[dict[str, Any]], predicate: Any) -> list[dict[str, Any]]:
    """Return up/down records accepted by one signal rule."""
    return [
        record
        for record in records
        if str(record["predicted_direction"]) != "flat" and predicate(record)
    ]


def _whale_pressure_matches_prediction(record: dict[str, Any]) -> bool:
    """Return whether recent whale pressure has the same sign as the predicted move."""
    pressure_key = (
        "whale_recent_net_pressure_12h"
        if str(record.get("window")) == "12h"
        else "whale_recent_net_pressure_24h"
    )
    pressure = float(record.get(pressure_key) or 0.0)
    predicted_direction = str(record.get("predicted_direction"))
    return (predicted_direction == "up" and pressure > 0) or (predicted_direction == "down" and pressure < 0)


def _signal_rule_payload(
    *,
    rule_id: str,
    label: str,
    description: str,
    records: list[dict[str, Any]],
    accepted: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return compact validation metrics for one dashboard signal rule."""
    nonflat_total = sum(1 for record in records if record["nonflat_actual"])
    correct_nonflat = sum(1 for record in accepted if record["correct"] and record["nonflat_actual"])
    direction_counts = Counter(str(record["predicted_direction"]) for record in accepted)
    average_confidence = (
        sum(float(record["confidence"]) for record in accepted) / len(accepted)
        if accepted
        else 0.0
    )
    return {
        "rule_id": rule_id,
        "label": label,
        "description": description,
        "accepted_count": len(accepted),
        "coverage_pct": _pct(len(accepted) / len(records)) if records else 0.0,
        "movement_signal_precision_pct": _accuracy(accepted),
        "nonflat_capture_pct": _pct(correct_nonflat / nonflat_total) if nonflat_total else 0.0,
        "false_signal_count": sum(1 for record in accepted if not record["correct"]),
        "average_confidence_pct": _pct(average_confidence),
        "up_signal_count": int(direction_counts.get("up", 0)),
        "down_signal_count": int(direction_counts.get("down", 0)),
    }


def _signal_rule_backtests(
    records: list[dict[str, Any]],
    thresholds: tuple[float, ...],
) -> list[dict[str, Any]]:
    """Evaluate guarded directional signal rules on out-of-sample rows."""
    recommended_threshold = float(
        _recommended_threshold(records, thresholds).get("confidence_threshold", DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD)
    )
    rule_specs = (
        (
            "display_confidence_movement",
            "Display confidence up/down",
            f"Predicted up/down with confidence >= {DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD:.2f}.",
            lambda record: float(record["confidence"]) >= DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD,
        ),
        (
            "recommended_confidence_movement",
            "Recommended confidence up/down",
            f"Predicted up/down with confidence >= {recommended_threshold:.2f}.",
            lambda record: float(record["confidence"]) >= recommended_threshold,
        ),
        (
            "category_threshold_movement",
            "Category threshold up/down",
            "Predicted up/down and passed the event-category confidence threshold.",
            lambda record: bool(record.get("movement_signal_at_category_threshold")),
        ),
        (
            "category_threshold_momentum_agrees",
            "Category + momentum agrees",
            "Category threshold passed and the price-momentum baseline predicts the same direction.",
            lambda record: bool(record.get("movement_signal_at_category_threshold"))
            and str(record.get("baseline_direction")) == str(record.get("predicted_direction")),
        ),
        (
            "category_threshold_whale_agrees",
            "Category + whale pressure agrees",
            "Category threshold passed and recent whale net pressure has the same sign as the prediction.",
            lambda record: bool(record.get("movement_signal_at_category_threshold"))
            and _whale_pressure_matches_prediction(record),
        ),
        (
            "category_threshold_momentum_or_whale_agrees",
            "Category + trend or whale agrees",
            "Category threshold passed and either momentum or recent whale pressure agrees with the prediction.",
            lambda record: bool(record.get("movement_signal_at_category_threshold"))
            and (
                str(record.get("baseline_direction")) == str(record.get("predicted_direction"))
                or _whale_pressure_matches_prediction(record)
            ),
        ),
    )
    return [
        _signal_rule_payload(
            rule_id=rule_id,
            label=label,
            description=description,
            records=records,
            accepted=_movement_signal_records(records, predicate),
        )
        for rule_id, label, description, predicate in rule_specs
    ]


def _diagnostic_lookup(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Return diagnostics keyed by group."""
    return {str(row["group"]): row for row in rows}


def _movement_signal_payload(
    *,
    records: list[dict[str, Any]],
    accepted: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return precision/coverage metrics for accepted up/down rows."""
    nonflat_total = sum(1 for record in records if record["nonflat_actual"])
    direction_counts = Counter(str(record["predicted_direction"]) for record in accepted)
    average_confidence = (
        sum(float(record["confidence"]) for record in accepted) / len(accepted)
        if accepted
        else 0.0
    )
    return {
        "accepted_count": len(accepted),
        "coverage_pct": _pct(len(accepted) / len(records)) if records else 0.0,
        "movement_signal_precision_pct": _accuracy(accepted),
        "nonflat_capture_pct": _pct(
            sum(1 for record in accepted if record["correct"] and record["nonflat_actual"]) / nonflat_total
        )
        if nonflat_total
        else 0.0,
        "false_signal_count": sum(1 for record in accepted if not record["correct"]),
        "average_confidence_pct": _pct(average_confidence),
        "up_signal_count": int(direction_counts.get("up", 0)),
        "down_signal_count": int(direction_counts.get("down", 0)),
    }


def _tier_payload(
    *,
    records: list[dict[str, Any]],
    tier: str,
    label: str,
    description: str,
) -> dict[str, Any]:
    """Return metrics for one assigned signal tier."""
    tier_records = [record for record in records if str(record.get("signal_tier")) == tier]
    movement_records = [record for record in tier_records if str(record["predicted_direction"]) != "flat"]
    payload = _movement_signal_payload(records=records, accepted=movement_records)
    payload.update(
        {
            "tier": tier,
            "label": label,
            "description": description,
            "row_count": len(tier_records),
            "row_coverage_pct": _pct(len(tier_records) / len(records)) if records else 0.0,
            "movement_signal_count": len(movement_records),
        }
    )
    return payload


def _display_signal_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return surfaced Strong/Watch movement records."""
    return [
        record
        for record in records
        if str(record.get("signal_tier")) in {"strong", "watch"}
        and str(record.get("predicted_direction")) != "flat"
    ]


def _stability_payload(
    *,
    records: list[dict[str, Any]],
    accepted: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return movement metrics plus stability counts for an accepted set."""
    payload = _movement_signal_payload(records=records, accepted=accepted)
    stable = [record for record in accepted if bool(record.get("prediction_stability_is_stable"))]
    unstable = [record for record in accepted if not bool(record.get("prediction_stability_is_stable"))]
    payload.update(
        {
            "stable_signal_count": len(stable),
            "stable_signal_precision_pct": _accuracy(stable),
            "unstable_signal_count": len(unstable),
            "unstable_signal_precision_pct": _accuracy(unstable),
        }
    )
    return payload


def _prediction_stability_diagnostics(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return aggregate stability diagnostics for the dashboard and report."""
    movement_records = [record for record in records if str(record.get("predicted_direction")) != "flat"]
    stable_movement_records = [
        record for record in movement_records if bool(record.get("prediction_stability_is_stable"))
    ]
    surfaced = _display_signal_records(records)
    stable_surfaced = [record for record in surfaced if bool(record.get("prediction_stability_is_stable"))]
    unstable_surfaced = [record for record in surfaced if not bool(record.get("prediction_stability_is_stable"))]
    by_tier: list[dict[str, Any]] = []
    for tier in ("strong", "watch", "abstain"):
        tier_records = [record for record in records if str(record.get("signal_tier")) == tier]
        tier_movement = [record for record in tier_records if str(record.get("predicted_direction")) != "flat"]
        by_tier.append(
            {
                "tier": tier,
                "label": tier.capitalize(),
                "row_count": len(tier_records),
                "movement_signal_count": len(tier_movement),
                **_stability_payload(records=records, accepted=tier_movement),
            }
        )
    return {
        "lookback_records": STABILITY_LOOKBACK_RECORDS,
        "min_observations": STABILITY_MIN_OBSERVATIONS,
        "min_agreement_pct": STABILITY_MIN_AGREEMENT_PCT,
        "row_count": len(records),
        "rows_with_stability_history_count": sum(
            1 for record in records if bool(record.get("prediction_stability_available"))
        ),
        "stable_prediction_count": sum(
            1 for record in records if bool(record.get("prediction_stability_is_stable"))
        ),
        "stable_prediction_rate_pct": _pct(
            sum(1 for record in records if bool(record.get("prediction_stability_is_stable"))) / len(records)
        )
        if records
        else 0.0,
        "internal_movement_signal_count": len(movement_records),
        "stable_internal_movement_signal_count": len(stable_movement_records),
        "stable_internal_movement_precision_pct": _accuracy(stable_movement_records),
        "surfaced_signal_count": len(surfaced),
        "stable_surfaced_signal_count": len(stable_surfaced),
        "stable_surfaced_signal_precision_pct": _accuracy(stable_surfaced),
        "unstable_surfaced_signal_count": len(unstable_surfaced),
        "unstable_surfaced_signal_precision_pct": _accuracy(unstable_surfaced),
        "by_signal_tier": by_tier,
    }


def _time_to_close_signal_backtests(
    records: list[dict[str, Any]],
    *,
    thresholds: tuple[float, ...],
    min_rows: int,
) -> list[dict[str, Any]]:
    """Return tier and stability signal metrics for each time-to-close bucket."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get("time_to_close_bucket") or "unknown"), []).append(record)
    rows: list[dict[str, Any]] = []
    for group_name, group_records in grouped.items():
        if len(group_records) < min_rows:
            continue
        surfaced = _display_signal_records(group_records)
        stable_surfaced = [
            record for record in surfaced if bool(record.get("prediction_stability_is_stable"))
        ]
        unstable_surfaced = [
            record for record in surfaced if not bool(record.get("prediction_stability_is_stable"))
        ]
        bucket_policy = next(
            (
                {
                    "allowed": bool(record.get("watch_time_to_close_bucket_allowed")),
                    "reason": record.get("watch_time_to_close_bucket_reason"),
                    "signal_count": int(record.get("watch_time_to_close_bucket_signal_count") or 0),
                    "precision_pct": _round(float(record.get("watch_time_to_close_bucket_precision_pct") or 0.0), 4),
                }
                for record in group_records
                if record.get("watch_time_to_close_bucket_reason") != "no_watch_candidates"
            ),
            {
                "allowed": False,
                "reason": "no_watch_candidates",
                "signal_count": 0,
                "precision_pct": 0.0,
            },
        )
        rows.append(
            {
                "group": group_name,
                "row_count": len(group_records),
                "condition_count": len({record["condition_ref"] for record in group_records}),
                "direction_counts": dict(
                    sorted(Counter(str(record["actual_direction"]) for record in group_records).items())
                ),
                "recommended_confidence_threshold": _recommended_threshold(group_records, thresholds),
                "surfaced_signals": _movement_signal_payload(records=group_records, accepted=surfaced),
                "stable_surfaced_signals": _movement_signal_payload(
                    records=group_records,
                    accepted=stable_surfaced,
                ),
                "unstable_surfaced_signals": _movement_signal_payload(
                    records=group_records,
                    accepted=unstable_surfaced,
                ),
                "watch_time_bucket_gate": bucket_policy,
                "signal_tiers": [
                    _tier_payload(
                        records=group_records,
                        tier="strong",
                        label="Strong",
                        description=TIER_DESCRIPTIONS["strong"],
                    ),
                    _tier_payload(
                        records=group_records,
                        tier="watch",
                        label="Watch",
                        description=TIER_DESCRIPTIONS["watch"],
                    ),
                    _tier_payload(
                        records=group_records,
                        tier="abstain",
                        label="Abstain",
                        description=TIER_DESCRIPTIONS["abstain"],
                    ),
                ],
            }
        )
    order = {bucket: index for index, bucket in enumerate(TIME_TO_CLOSE_BUCKET_ORDER)}
    rows.sort(key=lambda row: (order.get(str(row["group"]), len(order)), str(row["group"])))
    return rows


def _annotate_reliability_fields(
    records: list[dict[str, Any]],
    category_diagnostics: list[dict[str, Any]],
    time_diagnostics: list[dict[str, Any]],
) -> None:
    """Attach category and time-to-close reliability diagnostics to each record."""
    category_by_group = _diagnostic_lookup(category_diagnostics)
    time_by_group = _diagnostic_lookup(time_diagnostics)
    for record in records:
        category = category_by_group.get(str(record.get("event_category") or "unknown"))
        time_bucket = time_by_group.get(str(record.get("time_to_close_bucket") or "unknown"))
        warnings: list[str] = []
        if category:
            record["category_ece_pct"] = _round(float(category["expected_calibration_error_pct"]), 4)
            record["category_reliability_row_count"] = int(category["row_count"])
        else:
            record["category_ece_pct"] = None
            record["category_reliability_row_count"] = 0
            warnings.append("category reliability sparse")
        if time_bucket:
            record["time_to_close_ece_pct"] = _round(float(time_bucket["expected_calibration_error_pct"]), 4)
            record["time_to_close_reliability_row_count"] = int(time_bucket["row_count"])
        else:
            record["time_to_close_ece_pct"] = None
            record["time_to_close_reliability_row_count"] = 0
            warnings.append("time-to-close reliability sparse")
        record["reliability_warnings"] = warnings


def _mismatch_lookup(
    records: list[dict[str, Any]],
    *,
    key: str,
    min_rows: int,
) -> dict[str, dict[str, Any]]:
    """Return mismatch diagnostics keyed by a segment value."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(key) or "unknown"), []).append(record)
    return {
        group: _mismatch_group_payload(records=group_records, segment=key, group=group)
        for group, group_records in grouped.items()
        if len(group_records) >= min_rows
    }


def _annotate_mismatch_fields(records: list[dict[str, Any]], *, min_rows: int) -> None:
    """Attach historical validation mismatch rates to each row's category and time slice."""
    category_mismatch = _mismatch_lookup(records, key="event_category", min_rows=min_rows)
    time_mismatch = _mismatch_lookup(records, key="time_to_close_bucket", min_rows=min_rows)
    for record in records:
        category = category_mismatch.get(str(record.get("event_category") or "unknown"))
        time_bucket = time_mismatch.get(str(record.get("time_to_close_bucket") or "unknown"))
        if category:
            record["category_internal_movement_mismatch_rate_pct"] = _round(
                float(category["internal_movement_mismatch_rate_pct"]), 4
            )
            record["category_mismatch_row_count"] = int(category["row_count"])
        else:
            record["category_internal_movement_mismatch_rate_pct"] = None
            record["category_mismatch_row_count"] = 0
        if time_bucket:
            record["time_to_close_internal_movement_mismatch_rate_pct"] = _round(
                float(time_bucket["internal_movement_mismatch_rate_pct"]), 4
            )
            record["time_to_close_mismatch_row_count"] = int(time_bucket["row_count"])
        else:
            record["time_to_close_internal_movement_mismatch_rate_pct"] = None
            record["time_to_close_mismatch_row_count"] = 0


def _watch_support_accepts(record: dict[str, Any], rule: dict[str, Any]) -> bool:
    """Return whether the row has the support evidence required by a Watch rule."""
    support_mode = str(rule.get("support_mode") or "explicit")
    has_whale = bool(record.get("whale_signal_available"))
    has_stability = bool(record.get("prediction_stability_is_stable"))
    if support_mode == "whale":
        return has_whale
    if support_mode == "stability":
        return has_stability
    if support_mode == "whale_or_stability":
        return has_whale or has_stability
    if bool(rule.get("require_whale_signal")) and not has_whale:
        return False
    if bool(rule.get("require_trusted_whale_signal")) and not bool(record.get("trusted_whale_signal_available")):
        return False
    if bool(rule.get("require_stable_prediction")) and not has_stability:
        return False
    return True


def _crypto_watch_gate_accepts(record: dict[str, Any]) -> bool:
    """Return whether a crypto row has enough pressure quality for Watch tier."""
    if not bool(record.get("is_crypto")):
        return True
    if str(record.get("predicted_direction") or "flat") == "flat":
        return False
    quality = str(record.get("crypto_pressure_quality") or "unknown")
    if quality in CRYPTO_WATCH_BLOCKED_PRESSURE_QUALITIES:
        return False
    if bool(record.get("crypto_signal_conflict")):
        return False
    return True


def _watch_bucket_policy(
    *,
    records: list[dict[str, Any]],
    watch_records: list[dict[str, Any]],
    min_signal_count: int = WATCH_TIME_BUCKET_MIN_SIGNAL_COUNT,
    min_precision_pct: float = WATCH_TIME_BUCKET_MIN_PRECISION_PCT,
) -> dict[str, Any]:
    """Return time-to-close buckets where Watch is allowed to surface."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in watch_records:
        grouped.setdefault(str(record.get("time_to_close_bucket") or "unknown"), []).append(record)

    rows: list[dict[str, Any]] = []
    allowed: list[str] = []
    for bucket_name in TIME_TO_CLOSE_BUCKET_ORDER:
        bucket_records = grouped.get(bucket_name, [])
        if not bucket_records:
            continue
        metrics = _movement_signal_payload(records=records, accepted=bucket_records)
        signal_count = int(metrics["accepted_count"])
        precision = float(metrics["movement_signal_precision_pct"])
        if signal_count < min_signal_count:
            allowed_bucket = False
            reason = "too_sparse"
        elif precision < min_precision_pct:
            allowed_bucket = False
            reason = "precision_below_minimum"
        else:
            allowed_bucket = True
            reason = None
            allowed.append(bucket_name)
        rows.append(
            {
                "group": bucket_name,
                "allowed": allowed_bucket,
                "reason": reason,
                "minimum_signal_count": int(min_signal_count),
                "minimum_precision_pct": _round(float(min_precision_pct), 4),
                **metrics,
            }
        )
    extra_buckets = sorted(set(grouped) - set(TIME_TO_CLOSE_BUCKET_ORDER))
    for bucket_name in extra_buckets:
        bucket_records = grouped[bucket_name]
        metrics = _movement_signal_payload(records=records, accepted=bucket_records)
        signal_count = int(metrics["accepted_count"])
        precision = float(metrics["movement_signal_precision_pct"])
        allowed_bucket = signal_count >= min_signal_count and precision >= min_precision_pct
        if allowed_bucket:
            allowed.append(bucket_name)
        rows.append(
            {
                "group": bucket_name,
                "allowed": allowed_bucket,
                "reason": None if allowed_bucket else "too_sparse"
                if signal_count < min_signal_count
                else "precision_below_minimum",
                "minimum_signal_count": int(min_signal_count),
                "minimum_precision_pct": _round(float(min_precision_pct), 4),
                **metrics,
            }
        )
    blocked = [row for row in rows if not bool(row["allowed"])]
    return {
        "enabled": True,
        "minimum_signal_count": int(min_signal_count),
        "minimum_precision_pct": _round(float(min_precision_pct), 4),
        "allowed_buckets": allowed,
        "blocked_buckets": blocked,
        "bucket_metrics": rows,
    }


def _watch_rule_accepts(record: dict[str, Any], rule: dict[str, Any]) -> bool:
    """Return whether one record passes the selected Watch rule."""
    if str(record["predicted_direction"]) == "flat":
        return False
    if not _watch_support_accepts(record, rule):
        return False
    if bool(rule.get("apply_crypto_pressure_quality_gate")) and not _crypto_watch_gate_accepts(record):
        return False
    allowed_buckets = rule.get("allowed_time_to_close_buckets")
    if allowed_buckets is not None and str(record.get("time_to_close_bucket") or "unknown") not in {
        str(bucket) for bucket in allowed_buckets
    }:
        return False
    if str(rule.get("rule_id")) == "fallback_category_threshold":
        return bool(record.get("movement_signal_at_category_threshold"))
    category_ece = record.get("category_ece_pct")
    time_ece = record.get("time_to_close_ece_pct")
    category_mismatch = record.get("category_internal_movement_mismatch_rate_pct")
    time_mismatch = record.get("time_to_close_internal_movement_mismatch_rate_pct")
    if category_ece is None or time_ece is None or category_mismatch is None or time_mismatch is None:
        return False
    return (
        float(record["confidence"]) >= float(rule["confidence_threshold"])
        and float(category_ece) <= float(rule["max_category_ece_pct"])
        and float(time_ece) <= float(rule["max_time_to_close_ece_pct"])
        and float(category_mismatch) <= float(rule["max_category_mismatch_pct"])
        and float(time_mismatch) <= float(rule["max_time_to_close_mismatch_pct"])
        and int(record.get("category_reliability_row_count") or 0) >= int(rule["min_segment_rows"])
        and int(record.get("time_to_close_reliability_row_count") or 0) >= int(rule["min_segment_rows"])
        and int(record.get("category_mismatch_row_count") or 0) >= int(rule["min_segment_rows"])
        and int(record.get("time_to_close_mismatch_row_count") or 0) >= int(rule["min_segment_rows"])
    )


def _watch_candidate_records(records: list[dict[str, Any]], candidate: dict[str, Any]) -> list[dict[str, Any]]:
    """Return rows accepted by a candidate Watch rule."""
    return [record for record in records if _watch_rule_accepts(record, candidate)]


def _strong_records(records: list[dict[str, Any]], threshold: float) -> list[dict[str, Any]]:
    """Return Strong tier rows under the strict recommended confidence threshold."""
    return [
        record
        for record in records
        if str(record["predicted_direction"]) != "flat" and float(record["confidence"]) >= threshold
    ]


def _selected_watch_rule(
    records: list[dict[str, Any]],
    thresholds: tuple[float, ...],
    *,
    strong_threshold: float,
) -> dict[str, Any]:
    """Select the highest-coverage Watch rule that keeps acceptable movement precision."""
    strong_rows = _strong_records(records, strong_threshold)
    strong_ids = {id(record) for record in strong_rows}
    best: dict[str, Any] | None = None
    candidates_tested = 0
    for threshold in thresholds:
        if float(threshold) >= strong_threshold:
            continue
        for category_ece_limit in WATCH_ECE_LIMITS:
            for time_ece_limit in WATCH_ECE_LIMITS:
                for min_segment_rows in WATCH_MIN_SEGMENT_ROWS:
                    for category_mismatch_limit in WATCH_MISMATCH_LIMITS:
                        for time_mismatch_limit in WATCH_MISMATCH_LIMITS:
                            for require_trusted_whale_signal in (False,):
                                for support_mode in WATCH_SUPPORT_MODES:
                                    support_phrase = {
                                        "whale": "requiring whale-signal evidence",
                                        "stability": "requiring stable recent direction",
                                        "whale_or_stability": "requiring whale-signal evidence or stable recent direction",
                                    }[support_mode]
                                    candidate = {
                                        "rule_id": "optimized_watch_support_sweep",
                                        "label": "Watch support reliability sweep",
                                        "description": (
                                            "Predicted up/down below Strong threshold, "
                                            f"{support_phrase}, and filtering by confidence, calibration, "
                                            "historical mismatch slices, and crypto pressure quality."
                                        ),
                                        "confidence_threshold": _round(float(threshold), 4),
                                        "max_category_ece_pct": None
                                        if category_ece_limit == float("inf")
                                        else category_ece_limit,
                                        "max_time_to_close_ece_pct": None
                                        if time_ece_limit == float("inf")
                                        else time_ece_limit,
                                        "max_category_mismatch_pct": None
                                        if category_mismatch_limit == float("inf")
                                        else category_mismatch_limit,
                                        "max_time_to_close_mismatch_pct": None
                                        if time_mismatch_limit == float("inf")
                                        else time_mismatch_limit,
                                        "min_segment_rows": int(min_segment_rows),
                                        "support_mode": support_mode,
                                        "require_whale_signal": support_mode == "whale",
                                        "require_trusted_whale_signal": bool(require_trusted_whale_signal),
                                        "require_stable_prediction": support_mode == "stability",
                                        "apply_crypto_pressure_quality_gate": True,
                                    }
                                    evaluation_rule = {
                                        **candidate,
                                        "max_category_ece_pct": 1_000_000.0
                                        if category_ece_limit == float("inf")
                                        else category_ece_limit,
                                        "max_time_to_close_ece_pct": 1_000_000.0
                                        if time_ece_limit == float("inf")
                                        else time_ece_limit,
                                        "max_category_mismatch_pct": 1_000_000.0
                                        if category_mismatch_limit == float("inf")
                                        else category_mismatch_limit,
                                        "max_time_to_close_mismatch_pct": 1_000_000.0
                                        if time_mismatch_limit == float("inf")
                                        else time_mismatch_limit,
                                    }
                                    watch_only = [
                                        record
                                        for record in _watch_candidate_records(records, evaluation_rule)
                                        if id(record) not in strong_ids
                                    ]
                                    bucket_policy = _watch_bucket_policy(records=records, watch_records=watch_only)
                                    gated_watch_only = [
                                        record
                                        for record in watch_only
                                        if str(record.get("time_to_close_bucket") or "unknown")
                                        in set(bucket_policy["allowed_buckets"])
                                    ]
                                    gated_usable = list(
                                        {id(record): record for record in [*strong_rows, *gated_watch_only]}.values()
                                    )
                                    raw_watch_metrics = _movement_signal_payload(records=records, accepted=watch_only)
                                    watch_metrics = _movement_signal_payload(records=records, accepted=gated_watch_only)
                                    usable_metrics = _movement_signal_payload(records=records, accepted=gated_usable)
                                    candidates_tested += 1
                                    if not gated_watch_only:
                                        continue
                                    if float(watch_metrics["movement_signal_precision_pct"]) < DEFAULT_WATCH_MIN_PRECISION_PCT:
                                        continue
                                    if len(gated_usable) < len(strong_rows):
                                        continue
                                    row = {
                                        **candidate,
                                        "allowed_time_to_close_buckets": bucket_policy["allowed_buckets"],
                                        "time_to_close_bucket_policy": bucket_policy,
                                        "fallback_used": False,
                                        "fallback_reason": None,
                                        "candidates_tested": candidates_tested,
                                        "raw_watch_only": raw_watch_metrics,
                                        "watch_only": watch_metrics,
                                        "watch_or_better": usable_metrics,
                                        **usable_metrics,
                                    }
                                    if best is None or (
                                        int(row["accepted_count"]),
                                        float(row["movement_signal_precision_pct"]),
                                        float(row["nonflat_capture_pct"]),
                                        {"whale": 2, "stability": 1, "whale_or_stability": 0}.get(
                                            str(row.get("support_mode")), 0
                                        ),
                                        -float(row["confidence_threshold"]),
                                    ) > (
                                        int(best["accepted_count"]),
                                        float(best["movement_signal_precision_pct"]),
                                        float(best["nonflat_capture_pct"]),
                                        {"whale": 2, "stability": 1, "whale_or_stability": 0}.get(
                                            str(best.get("support_mode")), 0
                                        ),
                                        -float(best["confidence_threshold"]),
                                    ):
                                        best = row
    if best:
        best["candidates_tested"] = candidates_tested
        return best

    fallback_rule = {
        "rule_id": "fallback_category_threshold",
        "label": "Fallback category threshold with whale or stability support",
        "description": (
            "No optimized Watch rule met the precision target; use category-threshold movement signals "
            "only when whale-signal evidence or stable recent direction is available; crypto rows still "
            "must pass the pressure-quality gate."
        ),
        "confidence_threshold": None,
        "max_category_ece_pct": None,
        "max_time_to_close_ece_pct": None,
        "max_category_mismatch_pct": None,
        "max_time_to_close_mismatch_pct": None,
        "min_segment_rows": 0,
        "support_mode": "whale_or_stability",
        "require_whale_signal": False,
        "require_trusted_whale_signal": False,
        "require_stable_prediction": False,
        "apply_crypto_pressure_quality_gate": True,
    }
    fallback_watch_only = [
        record
        for record in _watch_candidate_records(records, fallback_rule)
        if id(record) not in strong_ids
    ]
    bucket_policy = _watch_bucket_policy(records=records, watch_records=fallback_watch_only)
    gated_fallback_watch_only = [
        record
        for record in fallback_watch_only
        if str(record.get("time_to_close_bucket") or "unknown") in set(bucket_policy["allowed_buckets"])
    ]
    usable = list({id(record): record for record in [*strong_rows, *gated_fallback_watch_only]}.values())
    return {
        **fallback_rule,
        "allowed_time_to_close_buckets": bucket_policy["allowed_buckets"],
        "time_to_close_bucket_policy": bucket_policy,
        "fallback_used": True,
        "fallback_reason": "no_optimized_rule_met_watch_precision",
        "candidates_tested": candidates_tested,
        "raw_watch_only": _movement_signal_payload(records=records, accepted=fallback_watch_only),
        "watch_only": _movement_signal_payload(records=records, accepted=gated_fallback_watch_only),
        "watch_or_better": _movement_signal_payload(records=records, accepted=usable),
        **_movement_signal_payload(records=records, accepted=usable),
    }


def _apply_signal_tiers(
    records: list[dict[str, Any]],
    thresholds: tuple[float, ...],
    *,
    category_diagnostics: list[dict[str, Any]],
    time_diagnostics: list[dict[str, Any]],
    min_rows: int,
) -> dict[str, Any]:
    """Annotate records with Strong/Watch/Abstain signal tiers."""
    _annotate_prediction_stability(records)
    _annotate_reliability_fields(records, category_diagnostics, time_diagnostics)
    _annotate_mismatch_fields(records, min_rows=min_rows)
    recommended = _recommended_threshold(records, thresholds)
    strong_threshold = float(recommended.get("confidence_threshold", DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD))
    watch_rule = _selected_watch_rule(records, thresholds, strong_threshold=strong_threshold)
    strong_rows = _strong_records(records, strong_threshold)
    strong_ids = {id(record) for record in strong_rows}
    bucket_policy_rows = {
        str(row.get("group")): row
        for row in watch_rule.get("time_to_close_bucket_policy", {}).get("bucket_metrics", [])
    }
    for record in records:
        warnings = list(record.get("reliability_warnings") or [])
        bucket_name = str(record.get("time_to_close_bucket") or "unknown")
        bucket_policy = bucket_policy_rows.get(bucket_name)
        record["watch_time_to_close_bucket_allowed"] = bool(bucket_policy.get("allowed")) if bucket_policy else False
        record["watch_time_to_close_bucket_reason"] = (
            bucket_policy.get("reason") if bucket_policy else "no_watch_candidates"
        )
        record["watch_time_to_close_bucket_signal_count"] = (
            int(bucket_policy.get("accepted_count") or 0) if bucket_policy else 0
        )
        record["watch_time_to_close_bucket_precision_pct"] = (
            _round(float(bucket_policy.get("movement_signal_precision_pct") or 0.0), 4)
            if bucket_policy
            else 0.0
        )
        if str(record["predicted_direction"]) == "flat":
            record["signal_tier"] = "abstain"
            record["signal_tier_reason"] = "flat_prediction"
        elif id(record) in strong_ids:
            record["signal_tier"] = "strong"
            record["signal_tier_reason"] = f"confidence >= strict threshold {strong_threshold:.2f}"
        elif _watch_rule_accepts(
            record,
            {
                **watch_rule,
                "max_category_ece_pct": 1_000_000.0
                if watch_rule.get("max_category_ece_pct") is None
                else watch_rule["max_category_ece_pct"],
                "max_time_to_close_ece_pct": 1_000_000.0
                if watch_rule.get("max_time_to_close_ece_pct") is None
                else watch_rule["max_time_to_close_ece_pct"],
                "max_category_mismatch_pct": 1_000_000.0
                if watch_rule.get("max_category_mismatch_pct") is None
                else watch_rule["max_category_mismatch_pct"],
                "max_time_to_close_mismatch_pct": 1_000_000.0
                if watch_rule.get("max_time_to_close_mismatch_pct") is None
                else watch_rule["max_time_to_close_mismatch_pct"],
            },
        ):
            record["signal_tier"] = "watch"
            record["signal_tier_reason"] = (
                "passed fallback category-threshold rule with whale or stability support"
                if watch_rule.get("fallback_used")
                else f"passed optimized Watch rule with {str(watch_rule.get('support_mode', 'support')).replace('_', ' ')} support"
            )
        else:
            record["signal_tier"] = "abstain"
            record["signal_tier_reason"] = "did not pass Strong or Watch rule"
        category_ece = record.get("category_ece_pct")
        time_ece = record.get("time_to_close_ece_pct")
        if category_ece is not None and watch_rule.get("max_category_ece_pct") is not None:
            if float(category_ece) > float(watch_rule["max_category_ece_pct"]):
                warnings.append("category calibration weak")
        if time_ece is not None and watch_rule.get("max_time_to_close_ece_pct") is not None:
            if float(time_ece) > float(watch_rule["max_time_to_close_ece_pct"]):
                warnings.append("time-to-close calibration weak")
        if record["signal_tier"] == "abstain" and str(record["predicted_direction"]) != "flat":
            if float(record["confidence"]) < float(watch_rule.get("confidence_threshold") or strong_threshold):
                warnings.append("below Watch confidence")
            support_mode = str(watch_rule.get("support_mode") or "explicit")
            has_watch_support = _watch_support_accepts(record, watch_rule)
            if not has_watch_support:
                if support_mode == "whale_or_stability":
                    warnings.append("no Watch whale or stability support")
                elif support_mode == "stability":
                    warnings.append("unstable recent prediction direction")
                else:
                    warnings.append("no Watch whale signal")
            if bool(watch_rule.get("apply_crypto_pressure_quality_gate")) and not _crypto_watch_gate_accepts(record):
                warnings.append("crypto pressure-quality gate failed")
            elif record.get("watch_time_to_close_bucket_reason") == "too_sparse":
                warnings.append("Watch time bucket too sparse")
            elif record.get("watch_time_to_close_bucket_reason") == "precision_below_minimum":
                warnings.append("Watch time bucket below precision minimum")
            elif record.get("watch_time_to_close_bucket_reason") == "no_watch_candidates":
                warnings.append("no Watch candidates in time bucket")
            if watch_rule.get("require_trusted_whale_signal") and not bool(
                record.get("trusted_whale_signal_available")
            ):
                warnings.append("no trusted Watch whale signal")
            category_mismatch = record.get("category_internal_movement_mismatch_rate_pct")
            time_mismatch = record.get("time_to_close_internal_movement_mismatch_rate_pct")
            if category_mismatch is not None and watch_rule.get("max_category_mismatch_pct") is not None:
                if float(category_mismatch) > float(watch_rule["max_category_mismatch_pct"]):
                    warnings.append("category mismatch slice weak")
            if time_mismatch is not None and watch_rule.get("max_time_to_close_mismatch_pct") is not None:
                if float(time_mismatch) > float(watch_rule["max_time_to_close_mismatch_pct"]):
                    warnings.append("time-to-close mismatch slice weak")
        record["reliability_warnings"] = sorted(set(warnings))

    signal_tier_backtests = [
        _tier_payload(
            records=records,
            tier="strong",
            label="Strong",
            description=TIER_DESCRIPTIONS["strong"],
        ),
        _tier_payload(
            records=records,
            tier="watch",
            label="Watch",
            description=(
                "Fallback category-threshold up/down signals below Strong with whale-signal evidence because no optimized Watch rule met target precision; stability is monitored separately."
                if watch_rule.get("fallback_used")
                else TIER_DESCRIPTIONS["watch"]
            ),
        ),
        _tier_payload(
            records=records,
            tier="abstain",
            label="Abstain",
            description=TIER_DESCRIPTIONS["abstain"],
        ),
    ]
    return {
        "signal_tier_policy": {
            "tier_descriptions": TIER_DESCRIPTIONS,
            "strong_confidence_threshold": _round(strong_threshold, 4),
            "strong_min_precision_pct": DEFAULT_STRONG_MIN_PRECISION_PCT,
            "watch_min_precision_pct": DEFAULT_WATCH_MIN_PRECISION_PCT,
            "watch_candidate_confidence_thresholds": [
                threshold for threshold in thresholds if float(threshold) < strong_threshold
            ],
            "watch_ece_limits_pct": [
                None if value == float("inf") else value for value in WATCH_ECE_LIMITS
            ],
            "watch_mismatch_limits_pct": [
                None if value == float("inf") else value for value in WATCH_MISMATCH_LIMITS
            ],
            "watch_min_segment_rows": list(WATCH_MIN_SEGMENT_ROWS),
            "watch_support_modes_tested": list(WATCH_SUPPORT_MODES),
            "watch_selected_support_mode": watch_rule.get("support_mode"),
            "watch_requires_whale_signal": bool(watch_rule.get("require_whale_signal")),
            "watch_time_bucket_gate_enabled": True,
            "watch_time_bucket_min_signal_count": WATCH_TIME_BUCKET_MIN_SIGNAL_COUNT,
            "watch_time_bucket_min_precision_pct": WATCH_TIME_BUCKET_MIN_PRECISION_PCT,
            "watch_allowed_time_to_close_buckets": watch_rule.get("allowed_time_to_close_buckets", []),
            "watch_crypto_pressure_quality_gate_enabled": bool(
                watch_rule.get("apply_crypto_pressure_quality_gate")
            ),
            "watch_crypto_blocked_pressure_qualities": list(CRYPTO_WATCH_BLOCKED_PRESSURE_QUALITIES),
            "watch_stability_checked": True,
            "watch_requires_stable_prediction": bool(watch_rule.get("require_stable_prediction")),
            "watch_fallback_allowed": True,
            "stability_lookback_records": STABILITY_LOOKBACK_RECORDS,
            "stability_min_observations": STABILITY_MIN_OBSERVATIONS,
            "stability_min_agreement_pct": STABILITY_MIN_AGREEMENT_PCT,
            "tier_precedence": ["strong", "watch", "abstain"],
        },
        "selected_watch_rule": watch_rule,
        "signal_tier_backtests": signal_tier_backtests,
    }


def _signal_tier_guardrail_checks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return pass/fail checks for signal-tier report quality."""
    checks: list[dict[str, Any]] = []
    for window_name, window in payload.get("windows", {}).items():
        tiers = {str(row.get("tier")): row for row in window.get("signal_tier_backtests", [])}
        strong = tiers.get("strong", {})
        strong_precision = float(strong.get("movement_signal_precision_pct") or 0.0)
        strong_signal_count = int(strong.get("movement_signal_count") or 0)
        checks.append(
            {
                "name": "strong_precision_minimum",
                "window": window_name,
                "ok": strong_signal_count > 0 and strong_precision >= DEFAULT_STRONG_MIN_PRECISION_PCT,
                "minimum_precision_pct": DEFAULT_STRONG_MIN_PRECISION_PCT,
                "precision_pct": _round(strong_precision, 4),
                "signal_count": strong_signal_count,
            }
        )

        selected_watch_rule = window.get("selected_watch_rule", {})
        checks.append(
            {
                "name": "watch_stability_checked",
                "window": window_name,
                "ok": bool(window.get("signal_tier_policy", {}).get("watch_stability_checked"))
                and "support_mode" in selected_watch_rule,
                "requires_stable_prediction": bool(selected_watch_rule.get("require_stable_prediction")),
                "support_mode": selected_watch_rule.get("support_mode"),
                "fallback_used": bool(selected_watch_rule.get("fallback_used")),
                "label": selected_watch_rule.get("label"),
                "rule_id": selected_watch_rule.get("rule_id"),
            }
        )
        checks.append(
            {
                "name": "watch_time_bucket_gate_present",
                "window": window_name,
                "ok": bool(window.get("signal_tier_policy", {}).get("watch_time_bucket_gate_enabled"))
                and bool(selected_watch_rule.get("time_to_close_bucket_policy"))
                and bool(selected_watch_rule.get("allowed_time_to_close_buckets")),
                "allowed_time_to_close_buckets": selected_watch_rule.get("allowed_time_to_close_buckets", []),
                "fallback_used": bool(selected_watch_rule.get("fallback_used")),
                "label": selected_watch_rule.get("label"),
                "rule_id": selected_watch_rule.get("rule_id"),
            }
        )
        fallback_used = bool(selected_watch_rule.get("fallback_used"))
        watch_only = selected_watch_rule.get("watch_only", {})
        watch_precision = float(watch_only.get("movement_signal_precision_pct") or 0.0)
        watch_signal_count = int(watch_only.get("accepted_count") or 0)
        if fallback_used:
            label = str(selected_watch_rule.get("label") or "").lower()
            rule_id = str(selected_watch_rule.get("rule_id") or "").lower()
            description = str(selected_watch_rule.get("description") or "").lower()
            watch_tier_description = str(tiers.get("watch", {}).get("description") or "").lower()
            fallback_labeled = (
                bool(selected_watch_rule.get("fallback_reason"))
                and ("fallback" in label or "fallback" in rule_id)
                and ("no optimized" in description or "fallback" in watch_tier_description)
            )
            checks.append(
                {
                    "name": "watch_fallback_labeled",
                    "window": window_name,
                    "ok": fallback_labeled,
                    "fallback_used": True,
                    "fallback_reason": selected_watch_rule.get("fallback_reason"),
                    "label": selected_watch_rule.get("label"),
                    "rule_id": selected_watch_rule.get("rule_id"),
                }
            )
        else:
            checks.append(
                {
                    "name": "optimized_watch_precision_minimum",
                    "window": window_name,
                    "ok": watch_signal_count > 0 and watch_precision >= DEFAULT_WATCH_MIN_PRECISION_PCT,
                    "minimum_precision_pct": DEFAULT_WATCH_MIN_PRECISION_PCT,
                    "precision_pct": _round(watch_precision, 4),
                    "signal_count": watch_signal_count,
                }
            )
    return checks


def _assert_signal_tier_guardrails(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Fail report generation when signal-tier guarantees are violated."""
    checks = _signal_tier_guardrail_checks(payload)
    failures = [check for check in checks if not check["ok"]]
    if failures:
        details = "; ".join(
            f"{check['window']} {check['name']}"
            for check in failures
        )
        raise RuntimeError(f"Signal tier guardrail failure: {details}")
    return checks


def _mismatch_group_payload(
    *,
    records: list[dict[str, Any]],
    segment: str,
    group: str,
) -> dict[str, Any]:
    """Return mismatch diagnostics for one segment group."""
    row_count = len(records)
    mismatches = [record for record in records if not record["correct"]]
    actual_nonflat = [record for record in records if str(record["actual_direction"]) != "flat"]
    actual_nonflat_misses = [record for record in actual_nonflat if not record["correct"]]
    internal_movement = [record for record in records if str(record["predicted_direction"]) != "flat"]
    internal_movement_misses = [record for record in internal_movement if not record["correct"]]
    surfaced = [
        record
        for record in internal_movement
        if str(record.get("signal_tier")) in {"strong", "watch"}
    ]
    surfaced_misses = [record for record in surfaced if not record["correct"]]
    opposite_direction_misses = [
        record
        for record in mismatches
        if {str(record["predicted_direction"]), str(record["actual_direction"])} == {"up", "down"}
    ]
    flat_prediction_nonflat_misses = [
        record
        for record in actual_nonflat_misses
        if str(record["predicted_direction"]) == "flat"
    ]
    top_confusions = Counter(
        f"{record['predicted_direction']} -> {record['actual_direction']}"
        for record in mismatches
    ).most_common(4)
    return {
        "segment": segment,
        "group": group,
        "row_count": row_count,
        "mismatch_count": len(mismatches),
        "mismatch_rate_pct": _pct(len(mismatches) / row_count) if row_count else 0.0,
        "actual_nonflat_count": len(actual_nonflat),
        "actual_nonflat_miss_count": len(actual_nonflat_misses),
        "actual_nonflat_miss_rate_pct": _pct(len(actual_nonflat_misses) / len(actual_nonflat))
        if actual_nonflat
        else 0.0,
        "internal_movement_signal_count": len(internal_movement),
        "internal_movement_mismatch_count": len(internal_movement_misses),
        "internal_movement_mismatch_rate_pct": _pct(len(internal_movement_misses) / len(internal_movement))
        if internal_movement
        else 0.0,
        "surfaced_signal_count": len(surfaced),
        "surfaced_signal_mismatch_count": len(surfaced_misses),
        "surfaced_signal_mismatch_rate_pct": _pct(len(surfaced_misses) / len(surfaced))
        if surfaced
        else 0.0,
        "opposite_direction_miss_count": len(opposite_direction_misses),
        "flat_prediction_nonflat_miss_count": len(flat_prediction_nonflat_misses),
        "abstain_row_count": sum(1 for record in records if str(record.get("signal_tier")) == "abstain"),
        "top_confusions": [
            {
                "confusion": confusion,
                "count": count,
            }
            for confusion, count in top_confusions
        ],
    }


def _mismatch_rows(
    records: list[dict[str, Any]],
    *,
    key: str,
    segment: str,
    min_rows: int,
) -> list[dict[str, Any]]:
    """Return sorted mismatch rows for a grouped segment."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record.get(key) or "unknown"), []).append(record)
    rows = [
        _mismatch_group_payload(records=group_records, segment=segment, group=group)
        for group, group_records in grouped.items()
        if len(group_records) >= min_rows
    ]
    return sorted(
        rows,
        key=lambda row: (
            int(row["actual_nonflat_miss_count"]),
            int(row["internal_movement_mismatch_count"]),
            float(row["actual_nonflat_miss_rate_pct"]),
            int(row["row_count"]),
        ),
        reverse=True,
    )


def _mismatch_diagnostics(records: list[dict[str, Any]], *, min_rows: int) -> dict[str, list[dict[str, Any]]]:
    """Return mismatch diagnostics by the dashboard's main reliability slices."""
    return {
        "by_signal_tier": _mismatch_rows(
            records,
            key="signal_tier",
            segment="Signal tier",
            min_rows=1,
        ),
        "by_event_category": _mismatch_rows(
            records,
            key="event_category",
            segment="Category",
            min_rows=min_rows,
        ),
        "by_time_to_close": _mismatch_rows(
            records,
            key="time_to_close_bucket",
            segment="Time to close",
            min_rows=min_rows,
        ),
    }


def _crypto_direction_diagnostic_row(
    *,
    segment: str,
    segment_type: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return crypto-specific direction and pressure-quality diagnostics."""
    movement = [record for record in records if str(record.get("predicted_direction")) != "flat"]
    surfaced = [
        record
        for record in movement
        if str(record.get("signal_tier")) in {"strong", "watch"}
    ]
    actual_nonflat = [record for record in records if str(record.get("actual_direction")) != "flat"]
    pressure_directional = [
        record
        for record in records
        if str(record.get("crypto_pressure_direction") or "neutral") in {"up", "down"}
    ]
    pressure_actual_aligned = [
        record
        for record in pressure_directional
        if str(record.get("actual_direction")) in {"up", "down"}
        and str(record.get("crypto_pressure_direction")) == str(record.get("actual_direction"))
    ]
    pressure_prediction_aligned = [
        record
        for record in pressure_directional
        if str(record.get("predicted_direction")) in {"up", "down"}
        and str(record.get("crypto_pressure_direction")) == str(record.get("predicted_direction"))
    ]
    flat_on_move_misses = [
        record
        for record in actual_nonflat
        if str(record.get("predicted_direction")) == "flat"
    ]
    opposite_misses = [
        record
        for record in records
        if str(record.get("actual_direction")) in {"up", "down"}
        and str(record.get("predicted_direction")) in {"up", "down"}
        and str(record.get("actual_direction")) != str(record.get("predicted_direction"))
    ]
    return {
        "segment": segment,
        "segment_type": segment_type,
        "row_count": len(records),
        "condition_count": len({record["condition_ref"] for record in records}),
        "actual_direction_counts": dict(sorted(Counter(str(record.get("actual_direction")) for record in records).items())),
        "predicted_direction_counts": dict(
            sorted(Counter(str(record.get("predicted_direction")) for record in records).items())
        ),
        "signal_tier_counts": dict(sorted(Counter(str(record.get("signal_tier")) for record in records).items())),
        "pressure_quality_counts": dict(
            sorted(Counter(str(record.get("crypto_pressure_quality") or "unknown") for record in records).items())
        ),
        "pressure_direction_counts": dict(
            sorted(Counter(str(record.get("crypto_pressure_direction") or "neutral") for record in records).items())
        ),
        "volatility_bucket_counts": dict(
            sorted(Counter(str(record.get("crypto_volatility_bucket") or "unknown") for record in records).items())
        ),
        "accuracy_pct": _accuracy(records),
        "nonflat_accuracy_pct": _accuracy(actual_nonflat),
        "internal_movement_signal_count": len(movement),
        "internal_movement_signal_precision_pct": _accuracy(movement),
        "surfaced_signal_count": len(surfaced),
        "surfaced_signal_precision_pct": _accuracy(surfaced),
        "false_surfaced_signal_count": sum(1 for record in surfaced if not record["correct"]),
        "strong_signal_count": sum(1 for record in surfaced if str(record.get("signal_tier")) == "strong"),
        "watch_signal_count": sum(1 for record in surfaced if str(record.get("signal_tier")) == "watch"),
        "abstain_row_count": sum(1 for record in records if str(record.get("signal_tier")) == "abstain"),
        "actual_nonflat_count": len(actual_nonflat),
        "flat_on_move_miss_count": len(flat_on_move_misses),
        "opposite_direction_miss_count": len(opposite_misses),
        "crypto_signal_conflict_count": sum(1 for record in records if bool(record.get("crypto_signal_conflict"))),
        "crypto_signal_conflict_pct": _pct(
            sum(1 for record in records if bool(record.get("crypto_signal_conflict"))) / len(records)
        )
        if records
        else 0.0,
        "pressure_actual_alignment_pct": _pct(len(pressure_actual_aligned) / len(pressure_directional))
        if pressure_directional
        else 0.0,
        "pressure_prediction_alignment_pct": _pct(len(pressure_prediction_aligned) / len(pressure_directional))
        if pressure_directional
        else 0.0,
        "up_precision_pct": _precision_for_label(records, "up"),
        "down_precision_pct": _precision_for_label(records, "down"),
    }


def _crypto_direction_diagnostics(records: list[dict[str, Any]], *, min_rows: int) -> dict[str, Any]:
    """Return crypto direction diagnostics by pressure, volatility, asset, and time slices."""
    crypto_records = [record for record in records if bool(record.get("is_crypto"))]
    if not crypto_records:
        return {
            "row_count": 0,
            "summary": None,
            "by_pressure_quality": [],
            "by_volatility_bucket": [],
            "by_time_to_close_bucket": [],
            "by_crypto_asset": [],
            "by_signal_tier": [],
        }

    def grouped_rows(key: str, *, minimum_rows: int = min_rows) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in crypto_records:
            grouped.setdefault(str(record.get(key) or "unknown"), []).append(record)
        rows = [
            _crypto_direction_diagnostic_row(
                segment=segment,
                segment_type=key,
                records=group_records,
            )
            for segment, group_records in grouped.items()
            if len(group_records) >= minimum_rows
        ]
        return sorted(
            rows,
            key=lambda row: (
                -int(row["row_count"]),
                -int(row["crypto_signal_conflict_count"]),
                str(row["segment"]),
            ),
        )

    return {
        "row_count": len(crypto_records),
        "policy": {
            "watch_crypto_pressure_quality_gate_enabled": True,
            "watch_crypto_blocked_pressure_qualities": list(CRYPTO_WATCH_BLOCKED_PRESSURE_QUALITIES),
            "strong_tier_exempt": True,
        },
        "summary": _crypto_direction_diagnostic_row(
            segment="crypto",
            segment_type="event_category",
            records=crypto_records,
        ),
        "by_pressure_quality": grouped_rows("crypto_pressure_quality", minimum_rows=1),
        "by_volatility_bucket": grouped_rows("crypto_volatility_bucket", minimum_rows=1),
        "by_time_to_close_bucket": grouped_rows("time_to_close_bucket", minimum_rows=1),
        "by_crypto_asset": grouped_rows("crypto_asset", minimum_rows=1),
        "by_signal_tier": grouped_rows("signal_tier", minimum_rows=1),
    }


def _case_payload(record: dict[str, Any]) -> dict[str, Any]:
    """Return dashboard-safe example row."""
    keys = (
        "window",
        "condition_ref",
        "market_slug",
        "question",
        "side_label",
        "observation_time",
        "event_category",
        "market_family",
        "research_focus",
        "hours_to_close",
        "time_to_close_bucket",
        "current_odds_pct",
        "actual_future_odds_pct",
        "actual_delta_pts",
        "actual_direction",
        "predicted_direction",
        "baseline_direction",
        "confidence",
        "margin",
        "probabilities",
        "correct",
        "baseline_correct",
        "nonflat_actual",
        "trend_delta_2h_pts",
        "trend_delta_6h_pts",
        "whale_recent_net_pressure_12h",
        "whale_recent_net_pressure_24h",
        "trusted_whale_recent_net_pressure",
        "whale_recent_trade_count",
        "trusted_whale_recent_trade_count",
        "whale_recent_activity_available",
        "trusted_whale_recent_activity_available",
        "whale_position_reconstruction_available",
        "trusted_whale_position_reconstruction_available",
        "whale_holding_profit_available",
        "trusted_whale_holding_profit_available",
        "whale_signal_available",
        "trusted_whale_signal_available",
        "category_recommended_confidence_threshold",
        "accepted_at_category_threshold",
        "movement_signal_at_category_threshold",
        "signal_tier",
        "signal_tier_reason",
        "reliability_warnings",
        "category_ece_pct",
        "time_to_close_ece_pct",
        "category_internal_movement_mismatch_rate_pct",
        "time_to_close_internal_movement_mismatch_rate_pct",
        "prediction_stability_lookback_records",
        "prediction_stability_min_observations",
        "prediction_stability_min_agreement_pct",
        "prediction_stability_sample_count",
        "prediction_stability_agreement_count",
        "prediction_stability_agreement_pct",
        "prediction_stability_streak_count",
        "prediction_stability_span_hours",
        "prediction_stability_available",
        "prediction_stability_is_stable",
        "watch_time_to_close_bucket_allowed",
        "watch_time_to_close_bucket_reason",
        "watch_time_to_close_bucket_signal_count",
        "watch_time_to_close_bucket_precision_pct",
        "is_crypto",
        "crypto_asset",
        "crypto_volatility_bucket",
        "crypto_pressure_source",
        "crypto_pressure_direction",
        "crypto_pressure_quality",
        "crypto_signal_conflict",
        "crypto_price_signal_conflict",
        "crypto_pressure_prediction_aligned",
        "crypto_pressure_price_aligned",
    )
    payload = {key: record.get(key) for key in keys}
    payload["accepted_at_display_threshold"] = float(record["confidence"]) >= DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD
    return payload


def _signal_tier_index(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Return compact per-record tier rows that other ML reports can join against."""
    index: dict[str, list[dict[str, Any]]] = {window: [] for window in PREDICTION_WINDOWS}
    for record in records:
        window_name = str(record.get("window") or "")
        if window_name not in index:
            index[window_name] = []
        index[window_name].append(
            {
                "window": window_name,
                "condition_ref": str(record.get("condition_ref") or ""),
                "market_slug": str(record.get("market_slug") or ""),
                "side_label": str(record.get("side_label") or ""),
                "observation_time": str(record.get("observation_time") or ""),
                "signal_tier": str(record.get("signal_tier") or "abstain"),
                "signal_tier_reason": str(record.get("signal_tier_reason") or ""),
                "predicted_direction": str(record.get("predicted_direction") or "flat"),
                "confidence": _round(float(record.get("confidence") or 0.0), 6),
                "category_ece_pct": record.get("category_ece_pct"),
                "time_to_close_ece_pct": record.get("time_to_close_ece_pct"),
                "reliability_warnings": list(record.get("reliability_warnings") or []),
                "is_crypto": bool(record.get("is_crypto")),
                "crypto_asset": record.get("crypto_asset"),
                "crypto_pressure_quality": record.get("crypto_pressure_quality"),
                "crypto_signal_conflict": bool(record.get("crypto_signal_conflict")),
            }
        )
    return index


def _example_cases(records: list[dict[str, Any]], limit: int) -> dict[str, list[dict[str, Any]]]:
    """Return dashboard case slices for filters and diagnosis."""
    confident = [
        record for record in records if float(record["confidence"]) >= DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD
    ]
    high_confidence_correct = sorted(
        [record for record in confident if record["correct"]],
        key=lambda record: (float(record["confidence"]), abs(float(record["actual_delta_pts"]))),
        reverse=True,
    )[:limit]
    high_confidence_misses = sorted(
        [record for record in confident if not record["correct"]],
        key=lambda record: (float(record["confidence"]), abs(float(record["actual_delta_pts"]))),
        reverse=True,
    )[:limit]
    nonflat_correct = sorted(
        [record for record in records if record["nonflat_actual"] and record["correct"]],
        key=lambda record: (float(record["confidence"]), abs(float(record["actual_delta_pts"]))),
        reverse=True,
    )[:limit]
    nonflat_misses = sorted(
        [record for record in records if record["nonflat_actual"] and not record["correct"]],
        key=lambda record: (float(record["confidence"]), abs(float(record["actual_delta_pts"]))),
        reverse=True,
    )[:limit]
    abstained_nonflat = sorted(
        [
            record
            for record in records
            if record["nonflat_actual"] and float(record["confidence"]) < DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD
        ],
        key=lambda record: abs(float(record["actual_delta_pts"])),
        reverse=True,
    )[:limit]
    strong_signals = sorted(
        [record for record in records if str(record.get("signal_tier")) == "strong"],
        key=lambda record: (float(record["confidence"]), abs(float(record["actual_delta_pts"]))),
        reverse=True,
    )[:limit]
    watch_signals = sorted(
        [record for record in records if str(record.get("signal_tier")) == "watch"],
        key=lambda record: (float(record["confidence"]), abs(float(record["actual_delta_pts"]))),
        reverse=True,
    )[:limit]
    abstained_signals = sorted(
        [record for record in records if str(record.get("signal_tier")) == "abstain"],
        key=lambda record: (bool(record["nonflat_actual"]), abs(float(record["actual_delta_pts"]))),
        reverse=True,
    )[:limit]
    by_category: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_category.setdefault(str(record["event_category"]), []).append(record)
    diverse: list[dict[str, Any]] = []
    for _, group_records in sorted(by_category.items()):
        diverse.append(
            sorted(
                group_records,
                key=lambda record: (
                    bool(record["nonflat_actual"]),
                    bool(record["correct"]),
                    float(record["confidence"]),
                    abs(float(record["actual_delta_pts"])),
                ),
                reverse=True,
            )[0]
        )
    diverse.sort(key=lambda record: (str(record["window"]), -abs(float(record["actual_delta_pts"]))))
    return {
        "high_confidence_correct": [_case_payload(record) for record in high_confidence_correct],
        "high_confidence_misses": [_case_payload(record) for record in high_confidence_misses],
        "nonflat_correct": [_case_payload(record) for record in nonflat_correct],
        "nonflat_misses": [_case_payload(record) for record in nonflat_misses],
        "abstained_nonflat": [_case_payload(record) for record in abstained_nonflat],
        "diverse_market_examples": [_case_payload(record) for record in diverse[:limit]],
        "strong_signals": [_case_payload(record) for record in strong_signals],
        "watch_signals": [_case_payload(record) for record in watch_signals],
        "abstained_signals": [_case_payload(record) for record in abstained_signals],
    }


def _feature_health(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return whether the key improvement feature groups are populated."""
    total = len(rows)
    return {
        "row_count": total,
        "time_series_price_rows_pct": _pct(
            sum(1 for row in rows if _safe_number(row.get("trend_observed_2h")) >= 0.5) / total
        )
        if total
        else 0.0,
        "recent_whale_activity_rows_pct": _pct(
            sum(
                1
                for row in rows
                if _safe_number(row.get("whale_side_recent_trade_count_12h"))
                or _safe_number(row.get("whale_side_recent_trade_count_24h"))
            )
            / total
        )
        if total
        else 0.0,
        "entry_exit_reconstruction_rows_pct": _pct(
            sum(
                1
                for row in rows
                if _safe_number(row.get("whale_position_reconstruction_available"))
                or _safe_number(row.get("trusted_whale_position_reconstruction_available"))
            )
            / total
        )
        if total
        else 0.0,
        "holding_profit_rows_pct": _pct(
            sum(
                1
                for row in rows
                if _safe_number(row.get("whale_holding_profit_available"))
                or _safe_number(row.get("trusted_whale_holding_profit_available"))
            )
            / total
        )
        if total
        else 0.0,
    }


def _feasibility_note(payload: dict[str, Any]) -> str:
    """Return a practical feasibility interpretation."""
    nonflat_scores = [
        float(window["summary"]["display_nonflat_accuracy_pct"])
        for window in payload["windows"].values()
        if window["summary"]["display_nonflat_accuracy_pct"]
    ]
    if nonflat_scores and min(nonflat_scores) >= 55.0:
        return (
            "High-confidence trend classification is feasible as a guarded dashboard signal, "
            "but it should still abstain on low-confidence rows."
        )
    return (
        "Non-deterministic pre-close trend prediction remains limited. The classifier should be used "
        "as an abstaining research signal, not as an always-on directional forecast."
    )


def _write_markdown(payload: dict[str, Any], output_path: Path) -> None:
    """Write a concise classifier report."""
    lines = [
        "# ML Trend Direction Classifier",
        "",
        f"Generated: `{payload['generated_at']}`",
        f"Dataset: `{payload['dataset_path']}`",
        f"Regime: `{payload['regime']}`",
        f"Calibration: `{payload.get('calibration_method', 'none')}`",
        "",
        "## Summary",
        "",
        payload["feasibility_note"],
        "",
        "| Window | Rows | Accuracy | Baseline | Non-flat accuracy | ECE | Brier | Display coverage | Display accuracy | Up/down precision | Recommended threshold | Recommended up/down precision |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for window_name, window in payload["windows"].items():
        summary = window["summary"]
        lines.append(
            "| {window} | {rows} | {accuracy}% | {baseline}% | {nonflat}% | {ece}% | {brier} | {coverage}% | {display_accuracy}% | {movement_precision}% | {recommended_threshold}% | {recommended_precision}% |".format(
                window=window_name,
                rows=summary["row_count"],
                accuracy=summary["accuracy_pct"],
                baseline=summary["baseline_accuracy_pct"],
                nonflat=summary["nonflat_accuracy_pct"],
                ece=summary["calibration"]["expected_calibration_error_pct"],
                brier=summary["calibration"]["mean_class_brier_score"],
                coverage=summary["display_coverage_pct"],
                display_accuracy=summary["display_accuracy_pct"],
                movement_precision=summary["display_movement_signal_precision_pct"],
                recommended_threshold=_pct(summary["recommended_confidence_threshold"]["confidence_threshold"]),
                recommended_precision=summary["recommended_confidence_threshold"]["movement_signal_precision_pct"],
            )
        )
    lines.extend(
        [
            "",
            "## Signal Tier Guardrails",
            "",
            "| Window | Check | Status | Minimum | Actual | Signals |",
            "| --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    for check in payload.get("signal_tier_guardrails", []):
        minimum = check.get("minimum_precision_pct")
        actual = check.get("precision_pct")
        lines.append(
            "| {window} | {name} | {status} | {minimum} | {actual} | {signals} |".format(
                window=check.get("window"),
                name=str(check.get("name", "")).replace("_", " "),
                status="pass" if check.get("ok") else "fail",
                minimum=f"{minimum}%" if isinstance(minimum, (int, float)) else "N/A",
                actual=f"{actual}%" if isinstance(actual, (int, float)) else "N/A",
                signals=check.get("signal_count", "N/A"),
            )
        )
    lines.extend(
        [
            "",
            "## Signal Tiers",
            "",
            "| Window | Tier | Description | Rows | Movement signals | Precision | Row coverage | Non-flat capture | False signals |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in payload["windows"].items():
        for tier in window.get("signal_tier_backtests", []):
            lines.append(
                "| {window} | {tier} | {description} | {rows} | {signals} | {precision}% | {coverage}% | {capture}% | {false_signals} |".format(
                    window=window_name,
                    tier=tier["label"],
                    description=tier.get("description", ""),
                    rows=tier["row_count"],
                    signals=tier["movement_signal_count"],
                    precision=tier["movement_signal_precision_pct"],
                    coverage=tier["row_coverage_pct"],
                    capture=tier["nonflat_capture_pct"],
                    false_signals=tier["false_signal_count"],
                )
            )
    lines.extend(
        [
            "",
            "| Window | Selected Watch rule | Watch-or-better signals | Watch-or-better precision | Watch-only signals | Raw Watch candidates | Watch-only precision | Support mode | Allowed buckets | Mismatch limits | Fallback |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- |",
        ]
    )
    for window_name, window in payload["windows"].items():
        rule = window.get("selected_watch_rule", {})
        watch_only = rule.get("watch_only", {})
        raw_watch_only = rule.get("raw_watch_only", {})
        lines.append(
            "| {window} | {label} | {signals} | {precision}% | {watch_signals} | {raw_watch_signals} | {watch_precision}% | {support} | {allowed_buckets} | {mismatch} | {fallback} |".format(
                window=window_name,
                label=rule.get("label", "Watch rule"),
                signals=rule.get("accepted_count", 0),
                precision=rule.get("movement_signal_precision_pct", 0.0),
                watch_signals=watch_only.get("accepted_count", 0),
                raw_watch_signals=raw_watch_only.get("accepted_count", watch_only.get("accepted_count", 0)),
                watch_precision=watch_only.get("movement_signal_precision_pct", 0.0),
                support=str(rule.get("support_mode") or "explicit").replace("_", " "),
                allowed_buckets=", ".join(rule.get("allowed_time_to_close_buckets", [])) or "none",
                mismatch=(
                    "category <= {category}; time <= {time}".format(
                        category=rule.get("max_category_mismatch_pct")
                        if rule.get("max_category_mismatch_pct") is not None
                        else "unbounded",
                        time=rule.get("max_time_to_close_mismatch_pct")
                        if rule.get("max_time_to_close_mismatch_pct") is not None
                        else "unbounded",
                    )
                ),
                fallback="yes" if rule.get("fallback_used") else "no",
            )
        )
    lines.extend(
        [
            "",
            "## Signal Stability",
            "",
            "| Window | Lookback | Min agreement | Stable internal signals | Stable internal precision | Stable surfaced signals | Stable surfaced precision | Unstable surfaced signals | Unstable surfaced precision |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in payload["windows"].items():
        diagnostics = window.get("stability_diagnostics", {})
        lines.append(
            "| {window} | {lookback} | {agreement}% | {stable_internal}/{internal} | {stable_internal_precision}% | {stable_surfaced}/{surfaced} | {stable_surfaced_precision}% | {unstable_surfaced} | {unstable_surfaced_precision}% |".format(
                window=window_name,
                lookback=diagnostics.get("lookback_records", "N/A"),
                agreement=diagnostics.get("min_agreement_pct", "N/A"),
                stable_internal=diagnostics.get("stable_internal_movement_signal_count", "N/A"),
                internal=diagnostics.get("internal_movement_signal_count", "N/A"),
                stable_internal_precision=diagnostics.get("stable_internal_movement_precision_pct", "N/A"),
                stable_surfaced=diagnostics.get("stable_surfaced_signal_count", "N/A"),
                surfaced=diagnostics.get("surfaced_signal_count", "N/A"),
                stable_surfaced_precision=diagnostics.get("stable_surfaced_signal_precision_pct", "N/A"),
                unstable_surfaced=diagnostics.get("unstable_surfaced_signal_count", "N/A"),
                unstable_surfaced_precision=diagnostics.get("unstable_surfaced_signal_precision_pct", "N/A"),
            )
        )
    lines.extend(
        [
            "",
            "## Time-To-Close Signal Backtests",
            "",
            "| Window | Bucket | Watch gate | Rows | Recommended threshold | Strong signals | Watch signals | Surfaced precision | Stable surfaced precision | Unstable surfaced precision |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in payload["windows"].items():
        for row in window.get("time_to_close_signal_backtests", []):
            tiers = {str(tier.get("tier")): tier for tier in row.get("signal_tiers", [])}
            recommendation = row.get("recommended_confidence_threshold", {})
            surfaced = row.get("surfaced_signals", {})
            stable = row.get("stable_surfaced_signals", {})
            unstable = row.get("unstable_surfaced_signals", {})
            gate = row.get("watch_time_bucket_gate", {})
            gate_label = "allowed" if gate.get("allowed") else str(gate.get("reason") or "blocked").replace("_", " ")
            lines.append(
                "| {window} | {bucket} | {gate} | {rows} | {threshold}% | {strong} | {watch} | {surface_precision}% | {stable_precision}% | {unstable_precision}% |".format(
                    window=window_name,
                    bucket=row["group"],
                    gate=gate_label,
                    rows=row["row_count"],
                    threshold=_pct(recommendation.get("confidence_threshold", 0.0)),
                    strong=tiers.get("strong", {}).get("movement_signal_count", 0),
                    watch=tiers.get("watch", {}).get("movement_signal_count", 0),
                    surface_precision=surfaced.get("movement_signal_precision_pct", 0.0),
                    stable_precision=stable.get("movement_signal_precision_pct", 0.0),
                    unstable_precision=unstable.get("movement_signal_precision_pct", 0.0),
                )
            )
    lines.extend(
        [
            "",
            "## Crypto Direction Diagnostics",
            "",
            "| Window | Segment | Rows | Internal precision | Surfaced precision | Watch | Conflicts | Pressure alignment | Flat-on-move misses |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in payload["windows"].items():
        diagnostics = window.get("crypto_direction_diagnostics", {})
        rows = [diagnostics.get("summary")] if diagnostics.get("summary") else []
        rows.extend(diagnostics.get("by_pressure_quality", [])[:5])
        rows.extend(diagnostics.get("by_crypto_asset", [])[:5])
        for row in rows:
            lines.append(
                "| {window} | {segment_type}: {segment} | {rows} | {internal}% | {surfaced}% | {watch} | {conflicts} | {pressure}% | {flat_misses} |".format(
                    window=window_name,
                    segment_type=row.get("segment_type"),
                    segment=row.get("segment"),
                    rows=row.get("row_count", 0),
                    internal=row.get("internal_movement_signal_precision_pct", 0.0),
                    surfaced=row.get("surfaced_signal_precision_pct", 0.0),
                    watch=row.get("watch_signal_count", 0),
                    conflicts=row.get("crypto_signal_conflict_count", 0),
                    pressure=row.get("pressure_actual_alignment_pct", 0.0),
                    flat_misses=row.get("flat_on_move_miss_count", 0),
                )
            )
    lines.extend(
        [
            "",
            "## Guarded Signal Rule Backtests",
            "",
            "| Window | Rule | Signals | Precision | Coverage | Non-flat capture | False signals |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in payload["windows"].items():
        for rule in window.get("signal_rule_backtests", []):
            lines.append(
                "| {window} | {label} | {signals} | {precision}% | {coverage}% | {capture}% | {false_signals} |".format(
                    window=window_name,
                    label=rule["label"],
                    signals=rule["accepted_count"],
                    precision=rule["movement_signal_precision_pct"],
                    coverage=rule["coverage_pct"],
                    capture=rule["nonflat_capture_pct"],
                    false_signals=rule["false_signal_count"],
                )
            )
    lines.extend(
        [
            "",
            "## Weakest Calibration Slices",
            "",
            "| Window | Segment | Group | Rows | ECE | Brier | Display precision |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in payload["windows"].items():
        for segment_name, key in (
            ("category", "calibration_by_event_category"),
            ("time-to-close", "calibration_by_time_to_close"),
        ):
            for row in window.get(key, [])[:5]:
                lines.append(
                    "| {window} | {segment} | {group} | {rows} | {ece}% | {brier} | {precision}% |".format(
                        window=window_name,
                        segment=segment_name,
                        group=row["group"],
                        rows=row["row_count"],
                        ece=row["expected_calibration_error_pct"],
                        brier=row["mean_class_brier_score"],
                        precision=row["display_movement_signal_precision_pct"],
                    )
                )
    lines.extend(
        [
            "",
            "## Prediction Mismatch Hotspots",
            "",
            "| Window | Segment | Group | Non-flat misses | Non-flat miss rate | Internal up/down false rate | Surfaced false rate | Opposite misses | Flat-on-move misses |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for window_name, window in payload["windows"].items():
        diagnostics = window.get("mismatch_diagnostics", {})
        rows = [
            *diagnostics.get("by_signal_tier", []),
            *diagnostics.get("by_event_category", [])[:5],
            *diagnostics.get("by_time_to_close", [])[:5],
        ]
        for row in rows:
            lines.append(
                "| {window} | {segment} | {group} | {misses}/{actual_nonflat} | {miss_rate}% | {internal_rate}% | {surfaced_rate}% | {opposite} | {flat_misses} |".format(
                    window=window_name,
                    segment=row["segment"],
                    group=row["group"],
                    misses=row["actual_nonflat_miss_count"],
                    actual_nonflat=row["actual_nonflat_count"],
                    miss_rate=row["actual_nonflat_miss_rate_pct"],
                    internal_rate=row["internal_movement_mismatch_rate_pct"],
                    surfaced_rate=row["surfaced_signal_mismatch_rate_pct"],
                    opposite=row["opposite_direction_miss_count"],
                    flat_misses=row["flat_prediction_nonflat_miss_count"],
                )
            )
    lines.extend(
        [
            "",
            "## Implemented Improvements",
            "",
        ]
    )
    for item in payload["implemented_steps"]:
        lines.append(f"- {item}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def evaluate_trend_direction_classifier(
    *,
    dataset_path: Path,
    output_json_path: Path,
    output_markdown_path: Path,
    direction_threshold: float,
    confidence_thresholds: tuple[float, ...],
    min_group_rows: int,
    case_limit: int,
    random_state: int,
    calibration_method: str,
) -> dict[str, Any]:
    """Evaluate the trend classifier and write JSON/Markdown reports."""
    regime_rows = _filter_rows_by_regime(_load_training_rows(dataset_path), REGIME_TRADE_COVERED)
    rows = [row for row in regime_rows if not _is_sports_market(row)]
    excluded_physical_sports_rows = len(regime_rows) - len(rows)
    rows, feature_columns = _prepare_rows(rows, direction_threshold)
    windows: dict[str, Any] = {}
    all_records: list[dict[str, Any]] = []
    for window_name in PREDICTION_WINDOWS:
        records, folds = _prediction_records(
            rows=rows,
            feature_columns=feature_columns,
            window_name=window_name,
            direction_threshold=direction_threshold,
            random_state=random_state,
            calibration_method=calibration_method,
        )
        records = _apply_category_thresholds(records, confidence_thresholds, min_rows=min_group_rows)
        calibration_by_event_category = _compact_calibration_diagnostics(
            records,
            key="event_category",
            min_rows=min_group_rows,
        )
        calibration_by_time_to_close = _compact_calibration_diagnostics(
            records,
            key="time_to_close_bucket",
            min_rows=min_group_rows,
        )
        tier_payload = _apply_signal_tiers(
            records,
            confidence_thresholds,
            category_diagnostics=calibration_by_event_category,
            time_diagnostics=calibration_by_time_to_close,
            min_rows=min_group_rows,
        )
        all_records.extend(records)
        windows[window_name] = {
            "summary": _summary(records, confidence_thresholds),
            "folds": folds,
            "calibration_status_counts": dict(sorted(Counter(str(fold["calibration_status"]) for fold in folds).items())),
            "event_category_thresholds": _group_threshold_recommendations(
                records,
                key="event_category",
                thresholds=confidence_thresholds,
                min_rows=min_group_rows,
            ),
            "market_family_thresholds": _group_threshold_recommendations(
                records,
                key="market_family",
                thresholds=confidence_thresholds,
                min_rows=min_group_rows,
            ),
            "calibration_by_event_category": calibration_by_event_category,
            "calibration_by_time_to_close": calibration_by_time_to_close,
            "signal_rule_backtests": _signal_rule_backtests(records, confidence_thresholds),
            "signal_tier_policy": tier_payload["signal_tier_policy"],
            "selected_watch_rule": tier_payload["selected_watch_rule"],
            "signal_tier_backtests": tier_payload["signal_tier_backtests"],
            "stability_diagnostics": _prediction_stability_diagnostics(records),
            "time_to_close_signal_backtests": _time_to_close_signal_backtests(
                records,
                thresholds=confidence_thresholds,
                min_rows=min_group_rows,
            ),
            "mismatch_diagnostics": _mismatch_diagnostics(records, min_rows=min_group_rows),
            "crypto_direction_diagnostics": _crypto_direction_diagnostics(records, min_rows=min_group_rows),
            "by_event_category": _group_summaries(
                records,
                key="event_category",
                thresholds=confidence_thresholds,
                min_rows=min_group_rows,
            ),
            "by_market_family": _group_summaries(
                records,
                key="market_family",
                thresholds=confidence_thresholds,
                min_rows=min_group_rows,
            ),
            "by_research_focus": _group_summaries(
                records,
                key="research_focus",
                thresholds=confidence_thresholds,
                min_rows=min_group_rows,
            ),
        }
    payload: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "regime": REGIME_TRADE_COVERED,
        "excluded_physical_sports_rows": excluded_physical_sports_rows,
        "market_scope_note": "Physical sports are excluded; esports and video-game markets remain in scope.",
        "task": "trend_direction_classification",
        "direction_threshold_pts": _pct(direction_threshold),
        "display_confidence_threshold": DEFAULT_DISPLAY_CONFIDENCE_THRESHOLD,
        "confidence_thresholds": list(confidence_thresholds),
        "calibration_method": calibration_method,
        "feature_count": len(feature_columns),
        "feature_groups": {
            "market_regimes": "event category, market family, and research focus one-hot features plus grouped metrics",
            "direction_classifier": "random forest multiclass classifier for down/flat/up movement",
            "probability_calibration": "sigmoid or isotonic post-fit calibration plus ECE/Brier reliability diagnostics",
            "time_series": "1h, 2h, 3h, 6h, 12h, and 24h price trend deltas, slopes, acceleration, and coverage flags",
            "short_term_interactions": "trend consistency, reversal, close-pressure, and whale-pressure acceleration features",
            "category_whale_trust": "category-specific whale pressure and realized-strategy interaction features",
            "entry_exit_reconstruction": "entry, partial/full exit, unmatched sell, holding-time, and profit availability features",
            "confidence_abstention": "coverage, movement-only precision, and category thresholds are reported across confidence thresholds",
            "segmented_calibration": "event-category and time-to-close reliability diagnostics identify where calibrated confidence is weak",
            "signal_rule_backtests": "guarded up/down decision rules compare confidence-only, category-threshold, momentum-agreement, and whale-agreement filters",
            "signal_tiers": "Strong, Watch, and Abstain tiers separate high-precision signals from broader reliability-filtered watch rows",
            "signal_stability": "recent same-market predictions are checked for repeated direction agreement before Watch signals are surfaced",
            "time_to_close_backtests": "Strong, Watch, Abstain, and stable surfaced-signal performance is reported by time-to-close bucket",
            "crypto_pressure_quality": "crypto Watch-tier signals are diagnosed and gated by trusted/raw whale pressure quality, volatility, asset, and time-to-close slices",
        },
        "implemented_steps": [
            "Separated validation by event category, market family, and research-focus regime.",
            "Added direct up/flat/down trend classification for 12h and 24h windows.",
            "Added confidence scoring and abstention metrics at multiple thresholds.",
            "Added price time-series momentum, volatility-style absolute moves, slope, acceleration, reversal, close-pressure, and trend coverage features.",
            "Added category-specific whale pressure and trusted-whale interaction features.",
            "Added short-term whale pressure acceleration and trend-by-whale interaction features for up/down movement.",
            "Used reconstructed entry/exit, holding-time, profit, and unmatched-sell quality features instead of treating all sells as clean exits.",
            "Evaluated coverage, accepted-row accuracy, non-flat accuracy, movement-only precision, and up/down precision by confidence threshold.",
            "Recommended category-specific confidence thresholds so the dashboard can require stronger evidence in weaker market regimes.",
            "Calibrated classifier probabilities and reported reliability diagnostics so dashboard confidence thresholds are easier to interpret.",
            "Added event-category and time-to-close calibration diagnostics to identify weaker reliability slices.",
            "Backtested guarded signal rules that require category thresholds plus optional momentum or whale-pressure agreement.",
            "Added Strong, Watch, and Abstain signal tiers with an optimized Watch rule that targets broader coverage at acceptable precision.",
            "Added recent prediction-stability diagnostics and require stable direction for Watch-tier signals.",
            "Added time-to-close bucket backtests for surfaced tiers and stable surfaced signals.",
            "Added crypto pressure-quality diagnostics and block weak/conflicting crypto Watch-tier rows while leaving Strong signals unchanged.",
        ],
        "feature_health": _feature_health(rows),
        "windows": windows,
        "signal_tier_index": _signal_tier_index(all_records),
        "cases": _example_cases(all_records, case_limit),
    }
    payload["feasibility_note"] = _feasibility_note(payload)
    payload["signal_tier_guardrails"] = _assert_signal_tier_guardrails(payload)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(payload, output_markdown_path)
    return payload


def _parse_thresholds(value: str) -> tuple[float, ...]:
    """Parse comma-separated confidence thresholds."""
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description="Evaluate up/flat/down trend classification.")
    parser.add_argument("--dataset-path", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--output-json-path", default=str(DEFAULT_OUTPUT_JSON_PATH))
    parser.add_argument("--output-markdown-path", default=str(DEFAULT_OUTPUT_MARKDOWN_PATH))
    parser.add_argument("--direction-threshold", type=float, default=DEFAULT_DIRECTION_THRESHOLD)
    parser.add_argument(
        "--confidence-thresholds",
        default=",".join(str(value) for value in DEFAULT_CONFIDENCE_THRESHOLDS),
    )
    parser.add_argument("--min-group-rows", type=int, default=20)
    parser.add_argument("--case-limit", type=int, default=12)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument(
        "--calibration-method",
        choices=("none", "sigmoid", "isotonic"),
        default=DEFAULT_CALIBRATION_METHOD,
        help="Probability calibration strategy for classifier confidence values.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    payload = evaluate_trend_direction_classifier(
        dataset_path=Path(args.dataset_path),
        output_json_path=Path(args.output_json_path),
        output_markdown_path=Path(args.output_markdown_path),
        direction_threshold=float(args.direction_threshold),
        confidence_thresholds=_parse_thresholds(str(args.confidence_thresholds)),
        min_group_rows=int(args.min_group_rows),
        case_limit=int(args.case_limit),
        random_state=int(args.random_state),
        calibration_method=str(args.calibration_method),
    )
    print(
        json.dumps(
            {
                "output_json_path": str(args.output_json_path),
                "output_markdown_path": str(args.output_markdown_path),
                "calibration_method": payload["calibration_method"],
                "feasibility_note": payload["feasibility_note"],
                "feature_health": payload["feature_health"],
                "windows": {
                    window: payload["windows"][window]["summary"]
                    for window in PREDICTION_WINDOWS
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
