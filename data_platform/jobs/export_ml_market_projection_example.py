"""Export one market-level residual movement prediction example for dashboard review."""

from __future__ import annotations

import argparse
from bisect import bisect_right
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_platform.ml.market_baseline_model import (
    GROUP_KEY_COLUMN,
    PRICE_BASELINE_FEATURE_COLUMNS,
    REGIME_TRADE_COVERED,
    WHALE_FEATURE_COLUMNS,
    WHALE_ONLY_FEATURE_COLUMNS,
    _build_estimator,
    _build_rolling_splits,
    _feature_matrix,
    _filter_rows_by_regime,
    _load_training_rows,
    _market_family_segment,
    _movement_task_for_window,
    _predict_estimator,
    _residual_model_profile_spec,
    _residual_estimator_params,
    _research_focus_segment,
    _targets,
    _training_correlation_feature_selection,
)
from data_platform.services.market_scope import is_physical_sports_market


DEFAULT_DATASET_PATH = Path("data_platform/runtime/ml/resolved_market_snapshot_features_backfilled_second.csv")
DEFAULT_COMPARISON_PATH = Path(
    "data_platform/runtime/ml/final_week10_11_residual_model_comparison_polymarket_trade_covered.json"
)
DEFAULT_OUTPUT_PATH = Path("data_platform/ml/EXAMPLE_MARKET_PROJECTION_RIDGE_TRADE_COVERED.json")
PREDICTION_WINDOWS = ("12h", "24h")
TREND_LOOKBACK_HOURS = (1, 2, 3, 6, 12, 24)
TREND_OVERLAY_MIN_ABS_2H_MOVE = 0.05
TREND_OVERLAY_MODEL_PARAMS = {
    "max_iter": 120,
    "learning_rate": 0.04,
    "max_leaf_nodes": 15,
    "l2_regularization": 2.0,
    "random_state": 42,
}
PAIR_NORMALIZATION_EPSILON = 1e-9
TREND_BASE_FEATURE_COLUMNS = (
    "last_price_side",
    "price_baseline",
    "hours_to_close",
    "horizon_hours",
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
    "whale_side_weighted_net_pressure",
    "top_whale_side_notional_share",
    "whale_vs_crowd_side_net_notional_gap",
    "trusted_whale_vs_crowd_side_net_notional_gap",
    "first_whale_trade_age_side_hours",
    "last_whale_trade_age_side_hours",
    "whale_side_avg_trades_per_active_day",
    "whale_side_entry_trade_count",
    "whale_side_exit_trade_count",
    "whale_side_avg_holding_hours",
    "whale_side_realized_roi",
    "whale_side_recent_trade_count_12h",
    "whale_side_recent_weighted_net_pressure_12h",
    "whale_side_recent_trade_count_24h",
    "whale_side_recent_weighted_net_pressure_24h",
    "trusted_whale_side_entry_trade_count",
    "trusted_whale_side_exit_trade_count",
    "trusted_whale_side_avg_holding_hours",
    "trusted_whale_side_realized_roi",
    "family_crypto_updown",
    "current_above_even",
    "current_extreme",
)


def _parse_selected_config(value: str) -> tuple[float, int]:
    """Return selector threshold and feature cap from a config name."""
    match = re.fullmatch(r"corr_([0-9.]+)_max_(\d+)", value)
    if not match:
        raise RuntimeError(f"Unsupported residual config format: {value}")
    return float(match.group(1)), int(match.group(2))


def _row_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    """Return a stable row key across 12h/24h prediction passes."""
    return (
        str(row["condition_ref"]),
        str(row["market_slug"]),
        str(row["side_label"]),
        str(row["observation_time"]),
    )


def _pair_group_key(row: dict[str, Any]) -> tuple[str, str]:
    """Return the binary-market pair key for one observation cutoff."""
    return (str(row["condition_ref"]), str(row["observation_time"]))


def _safe_float(row: dict[str, Any], column: str) -> float:
    """Return a numeric row value with empty-safe coercion."""
    return float(row.get(column) or 0.0)


def _current_odds(row: dict[str, Any]) -> float:
    """Return the row's current side odds as a probability."""
    return _safe_float(row, "last_price_side") or _safe_float(row, "price_baseline")


def _clip_probability(value: float) -> float:
    """Clip model output into probability bounds."""
    return min(1.0, max(0.0, float(value)))


def _round(value: float, digits: int = 6) -> float:
    """Round floats for stable dashboard payloads."""
    return round(float(value), digits)


def _pct(value: float) -> float:
    """Convert probability movement to percentage-point units."""
    return _round(float(value) * 100.0, 4)


def _parse_time(value: str) -> datetime:
    """Parse ISO datetime strings from the feature CSV."""
    return datetime.fromisoformat(str(value))


def _pretty_label(value: str) -> str:
    """Return a compact dashboard label for a feature name."""
    return value.replace("trusted_whale_side_", "trusted ").replace("whale_side_", "whale ").replace("_", " ")


def _is_crypto_updown(row: dict[str, Any]) -> bool:
    """Return whether a row belongs to the short crypto up/down family."""
    text = f"{row.get('market_slug', '')} {row.get('question', '')}".lower()
    return any(
        marker in text
        for marker in (
            "bitcoin up or down",
            "ethereum up or down",
            "solana up or down",
            "xrp up or down",
            "btc-updown",
            "eth-updown",
            "sol-updown",
            "xrp-updown",
        )
    )


def _enrich_trend_features(rows: list[dict[str, Any]]) -> None:
    """Add point-in-time price trend features used by the experimental overlay."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((str(row["condition_ref"]), str(row["side_label"])), []).append(row)

    for group_rows in grouped.values():
        group_rows.sort(key=lambda row: str(row["observation_time"]))
        observation_hours = [
            _parse_time(str(row["observation_time"])).timestamp() / 3600.0
            for row in group_rows
        ]
        prices = [_current_odds(row) for row in group_rows]

        for index, row in enumerate(group_rows):
            current_time = observation_hours[index]
            current_price = prices[index]
            for lookback_hours in TREND_LOOKBACK_HOURS:
                prior_index = bisect_right(observation_hours, current_time - lookback_hours) - 1
                if prior_index >= 0:
                    prior_price = prices[prior_index]
                    elapsed_hours = max(current_time - observation_hours[prior_index], 1e-9)
                    observed = 1.0
                else:
                    prior_price = current_price
                    elapsed_hours = 1.0
                    observed = 0.0
                delta = current_price - prior_price
                row[f"trend_delta_{lookback_hours}h"] = delta
                row[f"trend_abs_delta_{lookback_hours}h"] = abs(delta)
                row[f"trend_slope_{lookback_hours}h"] = delta / elapsed_hours
                row[f"trend_observed_{lookback_hours}h"] = observed

            day_start_index = bisect_right(observation_hours, current_time - 24.0) - 1
            row["trend_acceleration_6h"] = row["trend_slope_1h"] - row["trend_slope_6h"]
            row["trend_acceleration_24h"] = row["trend_slope_6h"] - row["trend_slope_24h"]
            row["trend_points_24h"] = index - day_start_index
            row["family_crypto_updown"] = 1.0 if _is_crypto_updown(row) else 0.0
            row["current_above_even"] = 1.0 if current_price >= 0.5 else 0.0
            row["current_extreme"] = abs(current_price - 0.5) * 2.0


def _trend_feature_columns(window_name: str) -> tuple[str, ...]:
    """Return trend-aware model feature columns for one prediction window."""
    trend_columns = tuple(
        column
        for lookback_hours in TREND_LOOKBACK_HOURS
        for column in (
            f"trend_delta_{lookback_hours}h",
            f"trend_abs_delta_{lookback_hours}h",
            f"trend_slope_{lookback_hours}h",
            f"trend_observed_{lookback_hours}h",
        )
    )
    return (
        *TREND_BASE_FEATURE_COLUMNS,
        *trend_columns,
        "trend_acceleration_6h",
        "trend_acceleration_24h",
        "trend_points_24h",
        f"future_window_reaches_resolution_{window_name}",
    )


def _selected_configs(comparison_path: Path) -> dict[str, str]:
    """Read selected residual configs from the model-family comparison artifact."""
    return {
        window: str(spec.get("selected_config") or "")
        for window, spec in _selected_prediction_specs(comparison_path).items()
    }


def _selected_prediction_specs(comparison_path: Path) -> dict[str, dict[str, str]]:
    """Read selected residual model specs from the model-family comparison artifact."""
    payload = json.loads(comparison_path.read_text(encoding="utf-8"))
    recommendations = payload.get("recommendation", {}).get("window_recommendations", {})
    if isinstance(recommendations, dict) and recommendations:
        specs: dict[str, dict[str, str]] = {}
        for window in PREDICTION_WINDOWS:
            recommendation = recommendations.get(window, {}) if isinstance(recommendations.get(window, {}), dict) else {}
            selected_config = str(recommendation.get("selected_config") or "")
            if not selected_config:
                continue
            specs[window] = {
                "selected_config": selected_config,
                "estimator_profile": str(recommendation.get("estimator_profile") or recommendation.get("estimator_type") or "ridge"),
                "estimator_type": str(recommendation.get("estimator_type") or recommendation.get("estimator_profile") or "ridge"),
            }
        if specs:
            return specs

    ridge = payload.get("models", {}).get("ridge", {})
    return {
        window: {
            "selected_config": str(ridge.get("windows", {}).get(window, {}).get("selected_config") or ""),
            "estimator_profile": "ridge",
            "estimator_type": "ridge",
        }
        for window in PREDICTION_WINDOWS
    }


def _predict_rolling_rows(
    *,
    rows: list[dict[str, Any]],
    selected_configs: dict[str, str] | None = None,
    selected_specs: dict[str, dict[str, str]] | None = None,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    """Return out-of-sample rolling predictions for the selected residual configs."""
    splits, _, _ = _build_rolling_splits(rows)
    predictions: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    resolved_specs = selected_specs or {
        window: {
            "selected_config": str(config),
            "estimator_profile": "ridge",
            "estimator_type": "ridge",
        }
        for window, config in (selected_configs or {}).items()
    }

    for window_name in PREDICTION_WINDOWS:
        spec = resolved_specs.get(window_name, {})
        selected_config = str(spec.get("selected_config") or "")
        if not selected_config:
            continue
        window_hours = int(window_name.removesuffix("h"))
        task = _movement_task_for_window(window_hours)
        estimator_profile = str(spec.get("estimator_profile") or spec.get("estimator_type") or "ridge")
        model_spec = _residual_model_profile_spec(estimator_profile)
        estimator_type = str(model_spec["estimator_type"])
        estimator_params = _residual_estimator_params(estimator_type, dict(model_spec.get("estimator_params") or {}))
        min_abs_correlation, max_selected_whale_features = _parse_selected_config(selected_config)

        for split in splits:
            train_rows = split["train_rows"]
            test_rows = split["test_rows"]
            y_train = _targets(train_rows, task)
            y_test = _targets(test_rows, task)

            price_model = _build_estimator(task, estimator_type, 42, estimator_params=estimator_params)
            price_model.fit(_feature_matrix(train_rows, PRICE_BASELINE_FEATURE_COLUMNS), y_train)
            price_train_predictions = _predict_estimator(
                price_model,
                _feature_matrix(train_rows, PRICE_BASELINE_FEATURE_COLUMNS),
            )
            price_test_predictions = _predict_estimator(
                price_model,
                _feature_matrix(test_rows, PRICE_BASELINE_FEATURE_COLUMNS),
            )
            residual_train_targets = [
                float(target) - float(prediction)
                for target, prediction in zip(y_train, price_train_predictions, strict=True)
            ]
            residual_columns, feature_selection = _training_correlation_feature_selection(
                train_rows=train_rows,
                feature_columns=WHALE_ONLY_FEATURE_COLUMNS,
                task=task,
                target_values=residual_train_targets,
                min_abs_correlation=min_abs_correlation,
                max_selected_whale_features=max_selected_whale_features,
            )
            residual_model = _build_estimator(task, estimator_type, 42, estimator_params=estimator_params)
            residual_model.fit(_feature_matrix(train_rows, residual_columns), residual_train_targets)
            residual_test_predictions = _predict_estimator(
                residual_model,
                _feature_matrix(test_rows, residual_columns),
            )

            for row, actual_delta, price_delta, residual_delta in zip(
                test_rows,
                y_test,
                price_test_predictions,
                residual_test_predictions,
                strict=True,
            ):
                corrected_delta = float(price_delta) + float(residual_delta)
                key = _row_key(row)
                predictions.setdefault(key, {"row": row, "windows": {}})["windows"][window_name] = {
                    "actual_delta": float(actual_delta),
                    "price_delta": float(price_delta),
                    "residual_delta": float(residual_delta),
                    "corrected_delta": corrected_delta,
                    "price_abs_error": abs(float(actual_delta) - float(price_delta)),
                    "corrected_abs_error": abs(float(actual_delta) - corrected_delta),
                    "error_improvement": abs(float(actual_delta) - float(price_delta))
                    - abs(float(actual_delta) - corrected_delta),
                    "fold_index": split["fold_index"],
                    "selected_config": selected_config,
                    "estimator_profile": estimator_profile,
                    "estimator_type": estimator_type,
                    "selected_feature_columns": list(residual_columns),
                    "selected_whale_features": feature_selection.get("selected_whale_features", []),
                    "residual_model": residual_model,
                }
    return predictions


def _candidate_score(item: dict[str, Any]) -> tuple[float, ...] | None:
    """Return a deterministic ranking score for dashboard example selection."""
    row = item["row"]
    if _is_sports_market(row):
        return None

    windows = item["windows"]
    if any(window not in windows for window in PREDICTION_WINDOWS):
        return None
    if _safe_float(row, "future_price_observed_12h") < 0.5 or _safe_float(row, "future_price_observed_24h") < 0.5:
        return None

    current_odds = _safe_float(row, "last_price_side") or _safe_float(row, "price_baseline")
    if not 0.05 <= current_odds <= 0.95:
        return None

    whale_activity = sum(
        abs(_safe_float(row, column))
        for column in (
            "whale_side_weighted_net_pressure",
            "whale_side_buy_notional_share",
            "whale_side_sell_notional_share",
            "whale_side_trade_share",
            "whale_distinct_users",
            "trusted_whale_distinct_users",
            "top_whale_side_notional_share",
        )
    )
    whale_entry_signal = (
        _safe_float(row, "whale_side_entry_trade_count")
        + _safe_float(row, "trusted_whale_side_entry_trade_count")
        + _safe_float(row, "trusted_whale_side_recent_entry_trade_count_12h")
        + _safe_float(row, "trusted_whale_side_recent_entry_trade_count_24h")
    )
    whale_buy_pressure = _safe_float(row, "whale_side_weighted_buy_pressure")
    if whale_activity <= 0 or whale_entry_signal + whale_buy_pressure <= 0:
        return None

    improvement = sum(float(windows[window]["error_improvement"]) for window in PREDICTION_WINDOWS)
    improves_both_windows = all(float(windows[window]["error_improvement"]) > 0 for window in PREDICTION_WINDOWS)
    improves_any_window = any(float(windows[window]["error_improvement"]) > 0 for window in PREDICTION_WINDOWS)
    actual_movement = sum(abs(float(windows[window]["actual_delta"])) for window in PREDICTION_WINDOWS)
    if actual_movement < 0.005:
        return None

    return (
        1.0 if improves_both_windows else 0.0,
        1.0 if improves_any_window else 0.0,
        improvement,
        min(whale_entry_signal, 5000.0),
        min(whale_buy_pressure, 5000.0),
        min(whale_activity, 5000.0),
        actual_movement,
        -abs(current_odds - 0.5),
    )


def _is_sports_market(row: dict[str, Any]) -> bool:
    """Return whether a row belongs to an excluded physical sports market."""
    return is_physical_sports_market(
        [row.get(column) for column in ("market_slug", "question", "event_title", "event_slug")],
        category=row.get("event_category"),
    )


def _select_example(predictions: dict[tuple[str, str, str, str], dict[str, Any]]) -> dict[str, Any]:
    """Select one real out-of-sample market row for dashboard display."""
    candidates = [
        (score, key, item)
        for key, item in predictions.items()
        if (score := _candidate_score(item)) is not None
    ]
    if not candidates:
        raise RuntimeError("No out-of-sample market example with whale entry activity was found.")
    candidates.sort(key=lambda entry: (entry[0], entry[1]), reverse=True)
    return candidates[0][2]


def _trend_matrix(rows: list[dict[str, Any]], feature_columns: tuple[str, ...]) -> list[list[float]]:
    """Return a numeric matrix for trend-aware models."""
    return [[_safe_float(row, column) for column in feature_columns] for row in rows]


def _rmse(y_true: list[float], predictions: list[float]) -> float:
    """Return root mean squared error."""
    if not y_true:
        return 0.0
    return math.sqrt(
        sum((float(actual) - float(prediction)) ** 2 for actual, prediction in zip(y_true, predictions, strict=True))
        / len(y_true)
    )


def _mae(y_true: list[float], predictions: list[float]) -> float:
    """Return mean absolute error."""
    if not y_true:
        return 0.0
    return sum(
        abs(float(actual) - float(prediction))
        for actual, prediction in zip(y_true, predictions, strict=True)
    ) / len(y_true)


def _pair_side_consistency_summary(
    records: list[dict[str, Any]],
    prediction_key: str,
) -> dict[str, Any]:
    """Return pair-normalization diagnostics for binary market side predictions."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(_pair_group_key(record["row"]), []).append(record)

    actuals: list[float] = []
    raw_predictions: list[float] = []
    normalized_predictions: list[float] = []
    pair_sum_errors: list[float] = []
    skipped_pair_count = 0

    for pair_records in grouped.values():
        side_labels = {str(record["row"].get("side_label")) for record in pair_records}
        if len(pair_records) != 2 or len(side_labels) != 2:
            skipped_pair_count += 1
            continue
        raw_sum = sum(float(record[prediction_key]) for record in pair_records)
        if raw_sum <= PAIR_NORMALIZATION_EPSILON:
            skipped_pair_count += 1
            continue

        pair_sum_errors.append(abs(raw_sum - 1.0))
        for record in pair_records:
            actuals.append(float(record["actual"]))
            raw_prediction = float(record[prediction_key])
            raw_predictions.append(raw_prediction)
            normalized_predictions.append(_clip_probability(raw_prediction / raw_sum))

    raw_rmse = _rmse(actuals, raw_predictions)
    normalized_rmse = _rmse(actuals, normalized_predictions)
    raw_mae = _mae(actuals, raw_predictions)
    normalized_mae = _mae(actuals, normalized_predictions)
    return {
        "paired_row_count": len(actuals),
        "pair_count": len(actuals) // 2,
        "skipped_pair_count": skipped_pair_count,
        "raw_rmse_pts": _pct(raw_rmse),
        "pair_normalized_rmse_pts": _pct(normalized_rmse),
        "pair_normalized_rmse_delta_vs_raw_pts": _pct(normalized_rmse - raw_rmse),
        "raw_mae_pts": _pct(raw_mae),
        "pair_normalized_mae_pts": _pct(normalized_mae),
        "pair_normalized_mae_delta_vs_raw_pts": _pct(normalized_mae - raw_mae),
        "raw_pair_sum_mae_pts": _pct(sum(pair_sum_errors) / len(pair_sum_errors)) if pair_sum_errors else 0.0,
        "raw_pair_sum_max_abs_error_pts": _pct(max(pair_sum_errors)) if pair_sum_errors else 0.0,
    }


def _segment_breakdown(records: list[dict[str, Any]], segment_key: str) -> list[dict[str, Any]]:
    """Return residual/trend/hybrid metrics grouped by a market segment key."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(str(record[segment_key]), []).append(record)

    rows: list[dict[str, Any]] = []
    for segment_name, segment_records in sorted(grouped.items()):
        actuals = [float(record["actual"]) for record in segment_records]
        residual_predictions = [float(record["residual_prediction"]) for record in segment_records]
        trend_predictions = [float(record["trend_prediction"]) for record in segment_records]
        hybrid_predictions = [float(record["hybrid_prediction"]) for record in segment_records]
        residual_rmse = _rmse(actuals, residual_predictions)
        trend_rmse = _rmse(actuals, trend_predictions)
        hybrid_rmse = _rmse(actuals, hybrid_predictions)
        rows.append(
            {
                "segment": segment_name,
                "row_count": len(segment_records),
                "overlay_row_count": sum(1 for record in segment_records if bool(record["overlay_applies"])),
                "residual_rmse_pts": _pct(residual_rmse),
                "trend_head_rmse_pts": _pct(trend_rmse),
                "hybrid_rmse_pts": _pct(hybrid_rmse),
                "hybrid_rmse_delta_vs_residual_pts": _pct(hybrid_rmse - residual_rmse),
                "residual_mae_pts": _pct(_mae(actuals, residual_predictions)),
                "trend_head_mae_pts": _pct(_mae(actuals, trend_predictions)),
                "hybrid_mae_pts": _pct(_mae(actuals, hybrid_predictions)),
            }
        )
    return sorted(rows, key=lambda item: (-int(item["row_count"]), str(item["segment"])))


def _trend_overlay_applies(row: dict[str, Any], window_name: str) -> bool:
    """Return whether the experimental trend/outcome overlay should replace the residual model."""
    return (
        _safe_float(row, "family_crypto_updown") >= 0.5
        and _safe_float(row, f"future_window_reaches_resolution_{window_name}") >= 0.5
        and abs(_safe_float(row, "trend_delta_2h")) >= TREND_OVERLAY_MIN_ABS_2H_MOVE
    )


def _evaluate_trend_aware_overlay(
    *,
    rows: list[dict[str, Any]],
    residual_predictions: dict[tuple[str, str, str, str], dict[str, Any]],
    example: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate a trend/outcome overlay against current residual predictions."""
    from sklearn.ensemble import HistGradientBoostingRegressor

    splits, _, _ = _build_rolling_splits(rows)
    example_key = _row_key(example["row"])
    windows: dict[str, Any] = {}
    example_predictions: dict[str, Any] = {}

    for window_name in PREDICTION_WINDOWS:
        feature_columns = _trend_feature_columns(window_name)
        target_column = f"future_price_side_{window_name}"
        y_all: list[float] = []
        residual_all: list[float] = []
        trend_head_all: list[float] = []
        hybrid_all: list[float] = []
        fold_rows: list[dict[str, Any]] = []
        evaluation_records: list[dict[str, Any]] = []
        overlay_cases: list[dict[str, Any]] = []
        overlay_count = 0

        for split in splits:
            train_rows = split["train_rows"]
            test_rows = split["test_rows"]
            model = HistGradientBoostingRegressor(**TREND_OVERLAY_MODEL_PARAMS)
            model.fit(
                _trend_matrix(train_rows, feature_columns),
                [_safe_float(row, target_column) for row in train_rows],
            )
            trend_predictions = [
                _clip_probability(value)
                for value in model.predict(_trend_matrix(test_rows, feature_columns))
            ]
            y_fold: list[float] = []
            residual_fold: list[float] = []
            trend_fold: list[float] = []
            hybrid_fold: list[float] = []
            fold_overlay_count = 0

            for row, trend_prediction in zip(test_rows, trend_predictions, strict=True):
                key = _row_key(row)
                residual_window = residual_predictions.get(key, {}).get("windows", {}).get(window_name)
                if residual_window is None:
                    continue
                actual = _safe_float(row, target_column)
                residual_prediction = _clip_probability(
                    _current_odds(row) + float(residual_window["corrected_delta"])
                )
                overlay_applies = _trend_overlay_applies(row, window_name)
                hybrid_prediction = trend_prediction if overlay_applies else residual_prediction
                evaluation_records.append(
                    {
                        "row": row,
                        "actual": actual,
                        "residual_prediction": residual_prediction,
                        "trend_prediction": trend_prediction,
                        "hybrid_prediction": hybrid_prediction,
                        "overlay_applies": overlay_applies,
                        "market_family": _market_family_segment(row),
                        "research_focus": _research_focus_segment(row),
                    }
                )
                if overlay_applies:
                    fold_overlay_count += 1
                    residual_error = abs(actual - residual_prediction)
                    trend_error = abs(actual - trend_prediction)
                    overlay_cases.append(
                        {
                            "market_slug": str(row["market_slug"]),
                            "question": str(row["question"]),
                            "side_label": str(row["side_label"]),
                            "observation_time": str(row["observation_time"]),
                            "current_odds_pct": _pct(_current_odds(row)),
                            "actual_future_odds_pct": _pct(actual),
                            "residual_predicted_odds_pct": _pct(residual_prediction),
                            "trend_head_predicted_odds_pct": _pct(trend_prediction),
                            "error_improvement_vs_residual_pts": _pct(residual_error - trend_error),
                            "market_family": _market_family_segment(row),
                            "research_focus_segment": _research_focus_segment(row),
                            "fold_index": split["fold_index"],
                            "hours_to_close": _round(_safe_float(row, "hours_to_close"), 2),
                            "trend_delta_2h_pts": _pct(_safe_float(row, "trend_delta_2h")),
                            "whale_side_notional_share_pct": _pct(_safe_float(row, "whale_side_notional_share")),
                        }
                    )
                y_fold.append(actual)
                residual_fold.append(residual_prediction)
                trend_fold.append(trend_prediction)
                hybrid_fold.append(hybrid_prediction)

                if key == example_key:
                    residual_error = abs(actual - residual_prediction)
                    trend_error = abs(actual - trend_prediction)
                    hybrid_error = abs(actual - hybrid_prediction)
                    example_predictions[window_name] = {
                        "actual_future_odds_pct": _pct(actual),
                        "residual_predicted_odds_pct": _pct(residual_prediction),
                        "trend_head_predicted_odds_pct": _pct(trend_prediction),
                        "hybrid_predicted_odds_pct": _pct(hybrid_prediction),
                        "trend_head_abs_error_pts": _pct(trend_error),
                        "hybrid_abs_error_pts": _pct(hybrid_error),
                        "hybrid_error_improvement_vs_residual_pts": _pct(residual_error - hybrid_error),
                        "overlay_applied": overlay_applies,
                    }

            y_all.extend(y_fold)
            residual_all.extend(residual_fold)
            trend_head_all.extend(trend_fold)
            hybrid_all.extend(hybrid_fold)
            overlay_count += fold_overlay_count
            fold_rows.append(
                {
                    "fold_index": split["fold_index"],
                    "row_count": len(y_fold),
                    "overlay_row_count": fold_overlay_count,
                    "residual_rmse_pts": _pct(_rmse(y_fold, residual_fold)),
                    "trend_head_rmse_pts": _pct(_rmse(y_fold, trend_fold)),
                    "hybrid_rmse_pts": _pct(_rmse(y_fold, hybrid_fold)),
                    "hybrid_rmse_delta_vs_residual_pts": _pct(_rmse(y_fold, hybrid_fold) - _rmse(y_fold, residual_fold)),
                }
            )

        windows[window_name] = {
            "row_count": len(y_all),
            "overlay_row_count": overlay_count,
            "feature_count": len(feature_columns),
            "residual_rmse_pts": _pct(_rmse(y_all, residual_all)),
            "trend_head_rmse_pts": _pct(_rmse(y_all, trend_head_all)),
            "hybrid_rmse_pts": _pct(_rmse(y_all, hybrid_all)),
            "hybrid_rmse_delta_vs_residual_pts": _pct(_rmse(y_all, hybrid_all) - _rmse(y_all, residual_all)),
            "residual_mae_pts": _pct(_mae(y_all, residual_all)),
            "trend_head_mae_pts": _pct(_mae(y_all, trend_head_all)),
            "hybrid_mae_pts": _pct(_mae(y_all, hybrid_all)),
            "hybrid_mae_delta_vs_residual_pts": _pct(_mae(y_all, hybrid_all) - _mae(y_all, residual_all)),
            "folds": fold_rows,
            "pair_side_consistency": {
                "residual": _pair_side_consistency_summary(evaluation_records, "residual_prediction"),
                "trend_head": _pair_side_consistency_summary(evaluation_records, "trend_prediction"),
                "hybrid": _pair_side_consistency_summary(evaluation_records, "hybrid_prediction"),
            },
            "market_family_breakdown": _segment_breakdown(evaluation_records, "market_family"),
            "research_focus_breakdown": _segment_breakdown(evaluation_records, "research_focus"),
            "overlay_case_samples": {
                "best_improvements": sorted(
                    overlay_cases,
                    key=lambda item: float(item["error_improvement_vs_residual_pts"]),
                    reverse=True,
                )[:8],
                "largest_regressions": sorted(
                    overlay_cases,
                    key=lambda item: float(item["error_improvement_vs_residual_pts"]),
                )[:8],
                "all_overlay_cases": sorted(
                    overlay_cases,
                    key=lambda item: (
                        str(item["market_slug"]),
                        str(item["side_label"]),
                        str(item["observation_time"]),
                    ),
                ),
            },
        }

    return {
        "available": True,
        "name": "trend_resolution_overlay",
        "status": "experimental_overlay_only",
        "base_model": "selected residual movement model",
        "overlay_model": "HistGradientBoostingRegressor",
        "overlay_gate": {
            "market_family": "crypto_updown",
            "requires_resolution_inside_prediction_window": True,
            "min_abs_2h_trend_delta": TREND_OVERLAY_MIN_ABS_2H_MOVE,
        },
        "model_params": TREND_OVERLAY_MODEL_PARAMS,
        "windows": windows,
        "example_predictions": example_predictions,
        "interpretation": (
            "The overlay follows abrupt near-resolution crypto trends better on the selected example, "
            "but does not yet beat the selected residual model on aggregate RMSE."
        ),
        "caveats": [
            "Experimental overlay only; not a default production model.",
            "Uses known market end timing to detect whether the prediction window reaches resolution.",
            "The overlay is intentionally gated to crypto up/down rows because aggregate use worsens RMSE.",
        ],
    }


def _history_points(rows: list[dict[str, Any]], example_row: dict[str, Any]) -> list[dict[str, Any]]:
    """Return actual historical odds points for the selected condition side."""
    observation_time = _parse_time(str(example_row["observation_time"]))
    side_rows = [
        row
        for row in rows
        if str(row[GROUP_KEY_COLUMN]) == str(example_row[GROUP_KEY_COLUMN])
        and str(row["side_label"]) == str(example_row["side_label"])
    ]
    points: list[dict[str, Any]] = []
    for row in sorted(side_rows, key=lambda item: str(item["observation_time"])):
        row_time = _parse_time(str(row["observation_time"]))
        relative_hour = (row_time - observation_time).total_seconds() / 3600.0
        if -24.0 <= relative_hour <= 0.0:
            points.append(
                {
                    "relative_hour": _round(relative_hour, 2),
                    "odds_pct": _pct(_safe_float(row, "last_price_side") or _safe_float(row, "price_baseline")),
                    "source": "actual_observed",
                }
            )
    return points


def _ridge_contributions(prediction: dict[str, Any], row: dict[str, Any]) -> list[dict[str, Any]]:
    """Return top single-row Ridge residual feature contributions in percentage points."""
    model = prediction["residual_model"]
    named_steps = getattr(model, "named_steps", {})
    scaler = named_steps.get("standardscaler")
    ridge = named_steps.get("ridge")
    if scaler is None or ridge is None:
        return []

    columns = list(prediction["selected_feature_columns"])
    values = [_safe_float(row, column) for column in columns]
    means = list(getattr(scaler, "mean_", []))
    scales = list(getattr(scaler, "scale_", []))
    coefficients = list(getattr(ridge, "coef_", []))
    contributions = []
    whale_columns = set(WHALE_FEATURE_COLUMNS)
    for column, value, mean, scale, coefficient in zip(columns, values, means, scales, coefficients, strict=True):
        if column not in whale_columns:
            continue
        scaled_value = 0.0 if float(scale) == 0.0 else (float(value) - float(mean)) / float(scale)
        impact = float(coefficient) * scaled_value
        contributions.append(
            {
                "feature": column,
                "label": _pretty_label(column),
                "impact_pts": _pct(impact),
                "raw_value": _round(value),
            }
        )
    contributions.sort(key=lambda item: abs(float(item["impact_pts"])), reverse=True)
    return contributions[:8]


def _flow_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Return compact whale-flow rows from real side-level feature values."""
    return [
        {
            "label": "All side flow",
            "buy_notional": _round(_safe_float(row, "side_buy_notional"), 4),
            "sell_notional": _round(_safe_float(row, "side_sell_notional"), 4),
            "net_notional": _round(_safe_float(row, "side_net_notional"), 4),
        },
        {
            "label": "Whale pressure",
            "buy_notional": _round(_safe_float(row, "whale_side_weighted_buy_pressure"), 4),
            "sell_notional": _round(_safe_float(row, "whale_side_weighted_sell_pressure"), 4),
            "net_notional": _round(_safe_float(row, "whale_side_weighted_net_pressure"), 4),
        },
        {
            "label": "Trusted pressure",
            "buy_notional": _round(_safe_float(row, "trusted_whale_side_weighted_buy_pressure"), 4),
            "sell_notional": _round(_safe_float(row, "trusted_whale_side_weighted_sell_pressure"), 4),
            "net_notional": _round(_safe_float(row, "trusted_whale_side_weighted_net_pressure"), 4),
        },
    ]


def _behavior_summary(row: dict[str, Any]) -> list[dict[str, str]]:
    """Return dashboard-ready behavior summary values from the selected row."""
    return [
        {"label": "Whale users", "value": str(int(_safe_float(row, "whale_distinct_users")))},
        {"label": "Trusted whale users", "value": str(int(_safe_float(row, "trusted_whale_distinct_users")))},
        {"label": "Whale trade share", "value": f"{_pct(_safe_float(row, 'whale_side_trade_share')):.1f}%"},
        {"label": "Whale notional share", "value": f"{_pct(_safe_float(row, 'whale_side_notional_share')):.1f}%"},
        {"label": "Whale entries", "value": str(int(_safe_float(row, "whale_side_entry_trade_count")))},
        {"label": "Whale exits", "value": str(int(_safe_float(row, "whale_side_exit_trade_count")))},
        {"label": "Whale holding time", "value": f"{_round(_safe_float(row, 'whale_side_avg_holding_hours'), 1)}h"},
        {"label": "Whale ROI", "value": f"{_pct(_safe_float(row, 'whale_side_realized_roi')):.1f}%"},
        {"label": "Trusted entries", "value": str(int(_safe_float(row, "trusted_whale_side_entry_trade_count")))},
        {"label": "Trusted exits", "value": str(int(_safe_float(row, "trusted_whale_side_exit_trade_count")))},
        {"label": "Trusted holding time", "value": f"{_round(_safe_float(row, 'trusted_whale_side_avg_holding_hours'), 1)}h"},
        {"label": "Trusted ROI", "value": f"{_pct(_safe_float(row, 'trusted_whale_side_realized_roi')):.1f}%"},
    ]


def _whale_entry_signal(row: dict[str, Any]) -> dict[str, Any]:
    """Return the whale entry trigger represented by this prediction row."""
    whale_entries = int(_safe_float(row, "whale_side_entry_trade_count"))
    trusted_entries = int(_safe_float(row, "trusted_whale_side_entry_trade_count"))
    whale_exits = int(_safe_float(row, "whale_side_exit_trade_count"))
    trusted_exits = int(_safe_float(row, "trusted_whale_side_exit_trade_count"))
    whale_buy_pressure = _safe_float(row, "whale_side_weighted_buy_pressure")
    whale_sell_pressure = _safe_float(row, "whale_side_weighted_sell_pressure")
    recent_12h_entries = int(_safe_float(row, "whale_side_recent_entry_trade_count_12h"))
    recent_24h_entries = int(_safe_float(row, "whale_side_recent_entry_trade_count_24h"))
    recent_12h_exits = int(_safe_float(row, "whale_side_recent_exit_trade_count_12h"))
    recent_24h_exits = int(_safe_float(row, "whale_side_recent_exit_trade_count_24h"))
    recent_12h_net_pressure = _safe_float(row, "whale_side_recent_weighted_net_pressure_12h")
    recent_24h_net_pressure = _safe_float(row, "whale_side_recent_weighted_net_pressure_24h")
    recent_12h_trusted_entries = int(_safe_float(row, "trusted_whale_side_recent_entry_trade_count_12h"))
    recent_24h_trusted_entries = int(_safe_float(row, "trusted_whale_side_recent_entry_trade_count_24h"))

    return {
        "relative_hour": 0.0,
        "timestamp": str(row["observation_time"]),
        "odds_pct": _pct(_current_odds(row)),
        "side_label": str(row["side_label"]),
        "entry_trade_count": whale_entries,
        "exit_trade_count": whale_exits,
        "net_entry_count": whale_entries - whale_exits,
        "trusted_entry_trade_count": trusted_entries,
        "trusted_exit_trade_count": trusted_exits,
        "trusted_net_entry_count": trusted_entries - trusted_exits,
        "recent_entry_trade_count_12h": recent_12h_entries,
        "recent_entry_trade_count_24h": recent_24h_entries,
        "recent_exit_trade_count_12h": recent_12h_exits,
        "recent_exit_trade_count_24h": recent_24h_exits,
        "recent_net_entry_count_12h": recent_12h_entries - recent_12h_exits,
        "recent_net_entry_count_24h": recent_24h_entries - recent_24h_exits,
        "trusted_recent_entry_trade_count_12h": recent_12h_trusted_entries,
        "trusted_recent_entry_trade_count_24h": recent_24h_trusted_entries,
        "weighted_buy_pressure": _round(whale_buy_pressure, 4),
        "weighted_sell_pressure": _round(whale_sell_pressure, 4),
        "weighted_net_pressure": _round(_safe_float(row, "whale_side_weighted_net_pressure"), 4),
        "recent_weighted_net_pressure_12h": _round(recent_12h_net_pressure, 4),
        "recent_weighted_net_pressure_24h": _round(recent_24h_net_pressure, 4),
        "whale_notional_share_pct": _pct(_safe_float(row, "whale_side_notional_share")),
        "trusted_whale_notional_share_pct": _pct(_safe_float(row, "trusted_whale_side_notional_share")),
        "derived_from": "prediction_snapshot",
        "note": (
            "The feature dataset stores reconstructed whale entry/exit behavior and weighted whale pressure "
            "at the model observation time, not the exact first whale-entry timestamp."
        ),
    }


def _build_payload(
    *,
    rows: list[dict[str, Any]],
    selected_specs: dict[str, dict[str, str]],
    example: dict[str, Any],
    trend_overlay: dict[str, Any],
    dataset_path: Path,
    comparison_path: Path,
) -> dict[str, Any]:
    """Build the JSON artifact consumed by the ML dashboard page."""
    row = example["row"]
    windows = example["windows"]
    current_odds = _safe_float(row, "last_price_side") or _safe_float(row, "price_baseline")
    window_payload: dict[str, Any] = {}
    for window in PREDICTION_WINDOWS:
        prediction = windows[window]
        future_column = f"future_price_side_{window}"
        rounded_error_improvement_pts = _pct(prediction["error_improvement"])
        window_payload[window] = {
            "selected_config": prediction["selected_config"],
            "estimator_profile": prediction["estimator_profile"],
            "estimator_type": prediction["estimator_type"],
            "fold_index": prediction["fold_index"],
            "actual_future_odds_pct": _pct(_safe_float(row, future_column)),
            "actual_delta_pts": _pct(prediction["actual_delta"]),
            "price_only_predicted_odds_pct": _pct(current_odds + prediction["price_delta"]),
            "price_only_delta_pts": _pct(prediction["price_delta"]),
            "whale_adjusted_predicted_odds_pct": _pct(current_odds + prediction["corrected_delta"]),
            "whale_adjusted_delta_pts": _pct(prediction["corrected_delta"]),
            "whale_impact_pts": _pct(prediction["corrected_delta"] - prediction["price_delta"]),
            "price_only_abs_error_pts": _pct(prediction["price_abs_error"]),
            "whale_adjusted_abs_error_pts": _pct(prediction["corrected_abs_error"]),
            "error_improvement_pts": rounded_error_improvement_pts,
            "improved": bool(rounded_error_improvement_pts > 0),
        }

    contribution_window = "12h"
    return {
        "available": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(dataset_path),
        "comparison_path": str(comparison_path),
        "selection_method": (
            "Deterministically selected from rolling out-of-sample rows with observed 12h/24h future prices, "
            "nonzero whale activity, no physical sports markets, and the strongest whale-correction improvement score."
        ),
        "estimator": "mixed_by_window",
        "selected_configs": {window: spec["selected_config"] for window, spec in selected_specs.items()},
        "selected_model_specs": selected_specs,
        "market": {
            "condition_ref": str(row["condition_ref"]),
            "market_slug": str(row["market_slug"]),
            "question": str(row["question"]),
            "side_label": str(row["side_label"]),
            "opposite_side_label": str(row["opposite_side_label"]),
            "observation_time": str(row["observation_time"]),
            "market_end_time": row["market_end_time"].isoformat(),
            "hours_to_close": _round(_safe_float(row, "hours_to_close"), 2),
            "current_odds_pct": _pct(current_odds),
            "winning_outcome_label": str(row.get("winning_outcome_label") or ""),
            "resolution_source": str(row.get("resolution_source") or ""),
            "resolution_time": str(row.get("resolution_time") or ""),
        },
        "windows": window_payload,
        "odds_series": _history_points(rows, row),
        "whale_entry_signal": _whale_entry_signal(row),
        "whale_flow": _flow_rows(row),
        "contribution_window": contribution_window,
        "contributions": _ridge_contributions(windows[contribution_window], row),
        "behavior_summary": _behavior_summary(row),
        "trend_aware_overlay": trend_overlay,
        "caveats": [
            "This is one selected out-of-sample row, not the aggregate model result.",
            "The movement model predicts price movement and can underpredict abrupt resolution jumps.",
            "This selected row has whale activity; trusted-whale activity may be zero depending on the available row.",
            "Use the aggregate rolling RMSE section for overall model performance.",
        ],
    }


def export_example(dataset_path: Path, comparison_path: Path, output_path: Path) -> dict[str, Any]:
    """Export the dashboard market projection example artifact."""
    selected_specs = _selected_prediction_specs(comparison_path)
    if any(window not in selected_specs or not selected_specs[window].get("selected_config") for window in PREDICTION_WINDOWS):
        raise RuntimeError("Could not resolve selected residual configs for both 12h and 24h windows.")
    regime_rows = _filter_rows_by_regime(_load_training_rows(dataset_path), REGIME_TRADE_COVERED)
    rows = [row for row in regime_rows if not _is_sports_market(row)]
    excluded_physical_sports_rows = len(regime_rows) - len(rows)
    _enrich_trend_features(rows)
    predictions = _predict_rolling_rows(rows=rows, selected_specs=selected_specs)
    example = _select_example(predictions)
    trend_overlay = _evaluate_trend_aware_overlay(
        rows=rows,
        residual_predictions=predictions,
        example=example,
    )
    payload = _build_payload(
        rows=rows,
        selected_specs=selected_specs,
        example=example,
        trend_overlay=trend_overlay,
        dataset_path=dataset_path,
        comparison_path=comparison_path,
    )
    payload["excluded_physical_sports_rows"] = excluded_physical_sports_rows
    payload["market_scope_note"] = "Physical sports are excluded; esports and video-game markets remain in scope."
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Export a real market-level ML projection example.")
    parser.add_argument("--dataset-path", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--comparison-path", default=str(DEFAULT_COMPARISON_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    payload = export_example(
        dataset_path=Path(args.dataset_path),
        comparison_path=Path(args.comparison_path),
        output_path=Path(args.output_path),
    )
    print(
        json.dumps(
            {
                "output_path": str(args.output_path),
                "market_slug": payload["market"]["market_slug"],
                "side_label": payload["market"]["side_label"],
                "windows": payload["windows"],
                "trend_aware_overlay": payload["trend_aware_overlay"]["windows"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
